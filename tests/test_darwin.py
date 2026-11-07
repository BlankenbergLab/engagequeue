"""macOS group ownership and the recorder's durable release handshake."""

import ctypes
import errno
import os
import signal
import sys
import time
from types import SimpleNamespace

import pytest
from test_backends_native import await_record, finish, launch, prepare

from engagequeue import _platform
from engagequeue.backends import darwin, native
from engagequeue.backends.processes import ProcessOwnershipError, alive, boot_identity, identity, signal_owned
from engagequeue.errors import UnsupportedBackend


def libproc_adapter(monkeypatch, implementation):
    class Query:
        def __call__(self, selector, pgid, buffer, size):
            assert selector == 2 and pgid == 101
            return implementation(buffer, size)

    monkeypatch.setattr(darwin.ctypes, "CDLL", lambda *args, **kwargs: SimpleNamespace(proc_listpids=Query()))
    return darwin._Libproc()


def test_libproc_uses_byte_counts_and_retries_a_full_snapshot(monkeypatch):
    sizes = []

    def query(buffer, size):
        sizes.append(size)
        if len(sizes) == 1:
            return size
        buffer[0], buffer[1] = 101, 102
        return 2 * ctypes.sizeof(ctypes.c_int)

    adapter = libproc_adapter(monkeypatch, query)
    assert adapter.group_pids(101) == [101, 102]
    assert sizes == [256 * ctypes.sizeof(ctypes.c_int), 512 * ctypes.sizeof(ctypes.c_int)]


@pytest.mark.parametrize("count", [-1, 0, 1, 1025])
def test_libproc_failed_or_malformed_byte_count_is_uncertain(monkeypatch, count):
    adapter = libproc_adapter(monkeypatch, lambda buffer, size: count)
    with pytest.raises(ProcessOwnershipError, match="complete.*snapshot"):
        adapter.group_pids(101)


@pytest.mark.parametrize("pids", [[101, 0], [101, -1], [101, 101]])
def test_libproc_invalid_pid_list_is_uncertain(monkeypatch, pids):
    def query(buffer, size):
        buffer[0], buffer[1] = pids
        return len(pids) * ctypes.sizeof(ctypes.c_int)

    with pytest.raises(ProcessOwnershipError, match="Malformed"):
        libproc_adapter(monkeypatch, query).group_pids(101)


def test_libproc_perpetually_truncated_snapshot_fails_with_bounded_allocation(monkeypatch):
    sizes = []

    def query(buffer, size):
        sizes.append(size)
        return size

    with pytest.raises(ProcessOwnershipError, match="remained truncated"):
        libproc_adapter(monkeypatch, query).group_pids(101)
    assert sizes[-1] == 262144 * ctypes.sizeof(ctypes.c_int)
    assert len(sizes) == 11


@pytest.mark.parametrize("pids", [[], [os.getpid() + 1]])
def test_capability_check_rejects_a_snapshot_missing_its_live_owner(monkeypatch, pids):
    monkeypatch.setattr(darwin, "_libproc", lambda: SimpleNamespace(group_pids=lambda pgid: pids))
    with pytest.raises(UnsupportedBackend, match="process-group inspection"):
        darwin.require_available()


class FakeStore:
    def __init__(self):
        self.record = {
            "worker_token": "token",
            "worker_boot": boot_identity(),
            "command_pid": 101,
            "command_birth": 101.0,
            "execution_granted": True,
            "command_started": 1.0,
            "execution_finished": None,
            "execution_returncode": 7,
        }
        self.updates = []

    def get(self, job_id):
        assert job_id == "job"
        return dict(self.record)

    def update(self, job_id, **fields):
        assert job_id == "job"
        self.updates.append(fields)
        self.record.update(fields)


@pytest.fixture
def group(monkeypatch):
    store = FakeStore()
    control = {"pids": [101, 102], "live": {101, 102}, "groups": {}, "signals": [], "clock": 0.0, "sleeps": []}
    process = SimpleNamespace(pid=101, returncode=None)
    process.poll = lambda: process.returncode

    def alive(record):
        return record["pid"] in control["live"] and record["birth"] == float(record["pid"])

    def getpgid(pid):
        if pid not in control["live"]:
            raise ProcessLookupError(errno.ESRCH, "exited")
        return control["groups"].get(pid, 101)

    def killpg(pgid, signum):
        assert pgid == 101 and signum == 0, "Group probes must never send a signal"
        if not control["pids"]:
            raise ProcessLookupError(errno.ESRCH, "group absent")

    def sleep(interval):
        control["clock"] += interval
        control["sleeps"].append(interval)

    monkeypatch.setattr(darwin, "identity", lambda pid: {"pid": pid, "birth": float(pid)})
    monkeypatch.setattr(darwin, "alive", alive)
    monkeypatch.setattr(darwin.os, "getpgid", getpgid)
    monkeypatch.setattr(darwin.os, "killpg", killpg)
    monkeypatch.setattr(darwin, "time", SimpleNamespace(monotonic=lambda: control["clock"], sleep=sleep))
    monkeypatch.setattr(
        darwin.psutil, "Process", lambda pid: SimpleNamespace(uids=lambda: SimpleNamespace(real=os.getuid()))
    )
    monkeypatch.setattr(darwin, "_libproc", lambda: SimpleNamespace(group_pids=lambda pgid: list(control["pids"])))
    monkeypatch.setattr(
        darwin,
        "signal_owned",
        lambda record, signum: control["signals"].append((record["pid"], signum)) or True,
    )
    launcher = darwin.DarwinLauncher(store, "job", "token")
    launcher.process = process
    launcher.pgid = process.pid
    return launcher, store, control


def test_recorder_stays_until_other_group_members_finish(group):
    launcher, store, control = group
    store.record["execution_finished"] = 2.0
    assert len(launcher.processes()) == 2
    assert not store.record.get("command_exit_granted")
    assert not launcher.quiescent()
    control["pids"] = [101]
    control["live"] = {101}
    assert launcher.processes() == [{"pid": 101, "birth": 101.0}]
    assert store.record["command_exit_granted"]
    assert store.record["execution_returncode"] == 7
    launcher.process.returncode = 7
    control["pids"] = []
    control["live"] = set()
    assert launcher.processes() == []
    assert launcher.quiescent()


def test_group_probe_prevents_completion_if_a_snapshot_missed_a_writer(group):
    launcher, store, control = group
    store.record["execution_finished"] = 2.0
    control["pids"] = [101]
    launcher.processes()
    assert store.record["command_exit_granted"]
    launcher.process.returncode = 7
    control["pids"] = [102]
    control["live"] = {102}
    with pytest.raises(ProcessOwnershipError, match="remains populated"):
        launcher.quiescent()
    assert 1.0 <= control["clock"] < 1.1
    assert not control["signals"]


def test_reaped_authorized_anchor_waits_for_transient_kernel_group_cleanup(group, monkeypatch):
    launcher, store, control = group
    store.record["execution_finished"] = 2.0
    control["pids"] = [101]
    launcher.processes()
    assert store.record["command_exit_granted"]
    launcher.process.returncode = 7
    probes = []

    def kernel_group(pgid, signum):
        assert pgid == 101 and signum == 0
        probes.append(control["clock"])
        if len(probes) >= 3:
            raise ProcessLookupError(errno.ESRCH, "kernel group cleanup finished")

    monkeypatch.setattr(darwin.os, "killpg", kernel_group)
    assert launcher.processes() == []
    assert len(probes) == 3 and control["sleeps"] == [0.05, 0.05]
    assert launcher.quiescent()
    assert not control["signals"]


def test_postreap_group_wait_stops_when_durable_ownership_changes(group, monkeypatch):
    launcher, store, control = group
    store.record["execution_finished"] = 2.0
    control["pids"] = [101]
    launcher.processes()
    launcher.process.returncode = 7

    def revoke_ownership(interval):
        control["clock"] += interval
        store.record["worker_token"] = "another-worker"

    monkeypatch.setattr(darwin.time, "sleep", revoke_ownership)
    with pytest.raises(ProcessOwnershipError, match="durable ownership changed"):
        launcher.processes()
    assert control["clock"] == 0.05 and not control["signals"]


def test_authorized_recorder_exit_waits_for_reaping_before_group_quiescence(group):
    launcher, store, control = group
    store.record["execution_finished"] = 2.0
    control["pids"] = [101]
    launcher.processes()
    assert store.record["command_exit_granted"]
    # libproc/psutil can see the anchor exiting before Popen's nonblocking
    # wait can reap it. This is a wait, never evidence that writers stopped.
    control["live"] = set()
    control["pids"] = []
    assert launcher.processes() == [launcher.anchor]
    assert not launcher.quiescent()
    assert store.record["execution_returncode"] == 7
    launcher.process.returncode = 7
    assert launcher.processes() == []
    assert launcher.quiescent()


def test_authorized_exit_still_requires_an_absent_group_after_reaping(group):
    launcher, store, control = group
    store.record["execution_finished"] = 2.0
    control["pids"] = [101]
    launcher.processes()
    control["live"] = {102}
    control["pids"] = [102]
    assert launcher.processes() == [launcher.anchor]
    launcher.process.returncode = 7
    with pytest.raises(ProcessOwnershipError, match="remains populated"):
        launcher.processes()


def test_released_recorder_is_not_inspected_while_awaiting_reaping(group, monkeypatch):
    launcher, store, control = group
    store.record["execution_finished"] = 2.0
    control["pids"] = [101]
    launcher.processes()
    assert store.record["command_exit_granted"]

    def forbidden(*args, **kwargs):
        pytest.fail("A released recorder must only be polled until reaping")

    monkeypatch.setattr(darwin, "_libproc", forbidden)
    monkeypatch.setattr(darwin, "alive", forbidden)
    monkeypatch.setattr(darwin, "identity", forbidden)
    monkeypatch.setattr(darwin.psutil, "Process", forbidden)
    monkeypatch.setattr(darwin.os, "getpgid", forbidden)
    control["pids"] = []
    assert launcher.processes() == [launcher.anchor]
    assert not launcher.quiescent()
    launcher.process.returncode = 7
    assert launcher.processes() == []
    assert launcher.quiescent()


@pytest.mark.parametrize("failure", ["permission", "snapshot", "member"])
def test_pre_exit_grant_ownership_errors_are_not_deferred(group, monkeypatch, failure):
    launcher, store, control = group
    launcher.processes()
    assert not store.record.get("command_exit_granted")
    if failure == "permission":
        monkeypatch.setattr(darwin, "alive", lambda record: None)
    elif failure == "snapshot":

        def incomplete(pgid):
            raise ProcessOwnershipError("Incomplete snapshot")

        monkeypatch.setattr(darwin, "_libproc", lambda: SimpleNamespace(group_pids=incomplete))
    else:
        control["pids"] = [101, 102]
        control["groups"][102] = 999
    with pytest.raises(ProcessOwnershipError):
        launcher.processes()


def test_process_group_inspection_error_includes_its_original_cause(group, monkeypatch):
    launcher, store, control = group

    def incomplete(pgid):
        raise ProcessOwnershipError("Snapshot diagnostic detail")

    monkeypatch.setattr(darwin, "_libproc", lambda: SimpleNamespace(group_pids=incomplete))
    with pytest.raises(ProcessOwnershipError, match="Snapshot diagnostic detail"):
        launcher.processes()


def test_unexpected_anchor_loss_cannot_confirm_quiescence(group):
    launcher, store, control = group
    launcher.processes()
    launcher.process.returncode = -signal.SIGKILL
    control["pids"] = []
    control["live"] = set()
    with pytest.raises(ProcessOwnershipError, match="without.*release grant"):
        launcher.processes()


def test_child_failure_before_execution_grant_can_release_unstarted_capacity(group):
    launcher, store, control = group
    store.record.update(execution_granted=False, command_started=None)
    launcher.process.returncode = 125
    control["pids"] = []
    control["live"] = set()
    assert launcher.processes() == []
    assert launcher.quiescent()


def test_cancellation_does_not_kill_the_postgrant_recorder_anchor(group):
    launcher, store, control = group
    launcher.signal(signal.SIGKILL)
    assert control["signals"] == [(102, signal.SIGKILL)]
    assert not store.record.get("command_exit_granted")


def test_pregrant_term_can_interrupt_the_private_recorder_gate(group):
    launcher, store, control = group
    store.record.update(execution_granted=False, command_started=None)
    control["pids"] = [101]
    launcher.signal(signal.SIGTERM)
    assert control["signals"] == [(101, signal.SIGTERM)]


def test_missing_live_anchor_is_not_an_empty_process_group(group):
    launcher, store, control = group
    store.record["execution_finished"] = 2.0
    control["pids"] = [102]
    with pytest.raises(ProcessOwnershipError):
        launcher.processes()
    assert not store.record.get("command_exit_granted")


def test_observed_process_group_escape_remains_uncertain(group):
    launcher, store, control = group
    launcher.processes()
    control["groups"][102] = 999
    control["pids"] = [101]
    with pytest.raises(ProcessOwnershipError):
        launcher.processes()
    assert not store.record.get("command_exit_granted")


def test_exited_previous_member_is_not_an_inspection_error(group):
    launcher, store, control = group
    launcher.processes()
    control["live"] = {101}
    control["pids"] = [101]
    store.record["execution_finished"] = 2.0
    launcher.processes()
    assert store.record["command_exit_granted"]


def test_recorder_waits_for_its_durable_exit_grant_without_rewriting_result(monkeypatch):
    store = FakeStore()
    store.record["execution_finished"] = 2.0
    sleeps = []

    def sleep(interval):
        sleeps.append(interval)
        store.record["command_exit_granted"] = True

    monkeypatch.setattr(darwin.time, "sleep", sleep)
    assert darwin.wait_for_exit_grant(store, "job", "token", {"pid": 101, "birth": 101.0})
    assert sleeps == [0.05]
    assert store.record["execution_returncode"] == 7
    assert store.record["execution_finished"] == 2.0
    assert store.updates == []


def test_stale_exit_grant_does_not_adopt_a_different_recorder():
    store = FakeStore()
    store.record.update(command_exit_granted=True, execution_finished=2.0)
    assert not darwin.wait_for_exit_grant(store, "job", "old-token", {"pid": 101, "birth": 101.0})
    assert not darwin.wait_for_exit_grant(store, "job", "token", {"pid": 101, "birth": 99.0})


@pytest.mark.parametrize("platform", ["linux", "darwin"])
@pytest.mark.parametrize("cancellation", ["request", "signal"])
def test_cancellation_after_execution_grant_before_payload_spawn_retains_darwin_anchor(
    tmp_path, monkeypatch, platform, cancellation
):
    store = FakeStore()
    original_boot = store.record["worker_boot"]
    store.record.update(
        command_pid=None, command_birth=None, command_started=None, cancel_requested=cancellation == "request"
    )
    grants = []
    monkeypatch.setattr(native, "_store", lambda state_dir: store)
    monkeypatch.setattr(native, "boot_identity", lambda: original_boot)
    monkeypatch.setattr(_platform, "system_name", lambda: platform)
    monkeypatch.setattr(native, "identity", lambda pid: {"pid": 101, "birth": 101.0})

    def register_handler(signum, handler):
        if cancellation == "signal":
            handler(signal.SIGTERM, None)

    monkeypatch.setattr(native.signal, "signal", register_handler)

    def forbidden(*args, **kwargs):
        pytest.fail("A canceled command cannot spawn its payload")

    def release(store, job_id, token, command):
        assert store.record["execution_granted"]
        assert store.record["execution_finished"] is not None
        assert store.record["execution_returncode"] == 143
        assert command == {"pid": 101, "birth": 101.0}
        grants.append(command)
        store.update(job_id, command_exit_granted=True)
        return True

    monkeypatch.setattr(native.subprocess, "Popen", forbidden)
    monkeypatch.setattr(darwin, "wait_for_exit_grant", release)
    assert native.child(str(tmp_path), "job", "token") == 143
    assert bool(grants) is (platform == "darwin")
    assert store.record["execution_returncode"] == 143
    assert store.record["command_started"] is None


@pytest.mark.parametrize("failure", ["token", "boot", "unreadable", "signal", "group-probe"])
def test_uncertain_ownership_never_confirms_writer_termination(group, monkeypatch, failure):
    launcher, store, control = group
    launcher.processes()
    if failure == "token":
        store.record["worker_token"] = "another-worker"
    elif failure == "boot":
        store.record["worker_boot"] = {"boot_id": "another-boot", "pid_namespace": "0"}
    elif failure == "unreadable":
        monkeypatch.setattr(darwin, "alive", lambda record: None)
    elif failure == "signal":
        monkeypatch.setattr(darwin, "signal_owned", lambda record, signum: False)
    else:
        store.record.update(command_exit_granted=True, execution_finished=2.0)
        launcher.release_requested = True
        launcher.process.returncode = 7

        def inaccessible(pgid, signum):
            raise PermissionError(errno.EPERM, "denied")

        monkeypatch.setattr(darwin.os, "killpg", inaccessible)
    with pytest.raises(ProcessOwnershipError):
        launcher.signal(signal.SIGKILL)
    assert not control["signals"]


def test_supervisor_retains_capacity_when_darwin_group_inspection_is_unavailable(tmp_path, monkeypatch):
    store, row = prepare(tmp_path, "print('must not execute')")
    original_boot = boot_identity()

    class UncertainLauncher:
        def __init__(self, store, job_id, token):
            pass

        def start(self, argv):
            pass

        def processes(self):
            raise ProcessOwnershipError("Cannot obtain a complete macOS process-group snapshot")

        def signal(self, signum):
            pytest.fail("Uncertain ownership cannot authorize a signal")

        def quiescent(self):
            pytest.fail("An unavailable snapshot cannot establish quiescence")

    monkeypatch.setattr(_platform, "system_name", lambda: "darwin")
    monkeypatch.setattr(native, "boot_identity", lambda: original_boot)
    monkeypatch.setattr(darwin, "DarwinLauncher", UncertainLauncher)
    assert native.supervise(str(store.root), row["id"], row["worker_token"]) == 125
    current = store.get(row["id"])
    assert current["state"] == "LOST" and not current["writers_stopped"], current.get("reason")
    assert "complete macOS process-group snapshot" in current["reason"]
    store.create({**row, "id": "next-job", "key": "next-key", "state": "QUEUED"})
    assert store.reserve_next("default") is None


MACOS = pytest.mark.skipif(sys.platform != "darwin", reason="Requires the real macOS libproc and process groups")


@MACOS
@pytest.mark.parametrize("returncode", [0, 7])
def test_macos_foreground_subprocesses_finish_without_linux_waitid(tmp_path, returncode):
    code = (
        "import subprocess,sys; "
        "subprocess.run([sys.executable,'-c',\"print('nested child')\"],check=True); "
        f"print('foreground'); sys.exit({returncode})"
    )
    store, row = prepare(tmp_path, code)
    bootstrap = """
import os,sys
from engagequeue.backends import native
from engagequeue.worker import main
def forbidden(*args,**kwargs):
    raise AssertionError('Linux child accounting must not run on macOS')
os.waitid=forbidden
native.become_subreaper=forbidden
native.children_exist=forbidden
sys.exit(main())
"""
    process = launch(store, row, bootstrap=bootstrap)
    current = await_record(store, row)
    finish(process)
    assert current["state"] == ("SUCCEEDED" if returncode == 0 else "FAILED"), current.get("reason")
    assert current["returncode"] == returncode and current["writers_stopped"], current.get("reason")
    assert current["command_exit_granted"]
    assert current["backend_info"]["strategy"] == "darwin-process-group"
    assert (tmp_path / "stdout").read_text() == "nested child\nforeground\n"


@MACOS
def test_macos_orphan_in_the_command_group_retains_the_recorder_and_exit_result(tmp_path):
    code = """
import os,time
from pathlib import Path
if os.fork() == 0:
    Path('orphan-ready').touch()
    while not Path('release-orphan').exists(): time.sleep(.01)
    print('orphan writer finished',flush=True)
    os._exit(0)
os._exit(7)
"""
    store, row = prepare(tmp_path, code)
    process = launch(store, row)
    try:
        current = await_record(
            store,
            row,
            lambda record: record.get("execution_finished") is not None and (tmp_path / "orphan-ready").exists(),
        )
        assert current["state"] == "RUNNING" and not current["writers_stopped"], current.get("reason")
        assert current["execution_returncode"] == 7 and not current.get("command_exit_granted")
        anchor = {"pid": current["command_pid"], "birth": current["command_birth"]}
        assert alive(anchor) is True
        (tmp_path / "release-orphan").touch()
        current = await_record(store, row)
        assert current["state"] == "FAILED" and current["returncode"] == 7, current.get("reason")
        assert current["writers_stopped"] and current["command_exit_granted"]
        assert alive(anchor) is False
        assert (tmp_path / "stdout").read_text() == "orphan writer finished\n"
    finally:
        (tmp_path / "release-orphan").touch()
        finish(process)


@MACOS
def test_macos_cancel_kills_an_orphan_writer_before_releasing_the_anchor(tmp_path):
    code = """
import os,signal,time
from pathlib import Path
if os.fork() == 0:
    signal.signal(signal.SIGTERM,signal.SIG_IGN)
    Path('writer.pid').write_text(str(os.getpid()))
    while True:
        print('still writing',flush=True)
        time.sleep(.02)
os._exit(0)
"""
    store, row = prepare(tmp_path, code)
    process = launch(store, row)
    writer = None
    try:
        current = await_record(
            store,
            row,
            lambda record: record.get("execution_finished") is not None and (tmp_path / "writer.pid").exists(),
        )
        anchor = {"pid": current["command_pid"], "birth": current["command_birth"]}
        writer = identity(int((tmp_path / "writer.pid").read_text()))
        assert os.getpgid(writer["pid"]) == anchor["pid"]
        start = time.monotonic()
        store.request_cancel(row["id"])
        current = await_record(store, row)
        assert current["state"] == "CANCELLED" and current["writers_stopped"], current.get("reason")
        assert current["command_exit_granted"] and time.monotonic() - start >= 0.1
        assert alive(writer) is False and alive(anchor) is False
    finally:
        if writer is not None:
            signal_owned(writer, signal.SIGKILL)
        finish(process)


@MACOS
def test_macos_anchor_loss_is_lost_and_preserves_the_reservation(tmp_path):
    store, row = prepare(tmp_path, "import time; time.sleep(30)")
    process = launch(store, row)
    payload = None
    try:
        current = await_record(store, row, lambda record: record.get("payload_pid") is not None)
        anchor = {"pid": current["command_pid"], "birth": current["command_birth"]}
        payload = {"pid": current["payload_pid"], "birth": current["payload_birth"]}
        assert signal_owned(anchor, signal.SIGKILL)
        current = await_record(store, row)
        assert current["state"] == "LOST" and not current["writers_stopped"], current.get("reason")
        assert "Execution ownership requires reconciliation" in current["reason"]
        assert alive(payload) is True
        assert not current.get("command_exit_granted")
        store.create({**row, "id": "next-job", "key": "next-key", "state": "QUEUED"})
        assert store.reserve_next("default") is None
    finally:
        if payload is not None:
            signal_owned(payload, signal.SIGKILL)
        finish(process)

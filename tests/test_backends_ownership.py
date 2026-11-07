"""Ownership boundaries and optional systemd result retention."""

import json
import os
import signal
import subprocess
import time
from types import SimpleNamespace

import pytest
from runtime_prerequisites import require_systemd
from test_backends_native import await_record, finish, launch, prepare

from engagequeue.backends.containers import ContainerOwnershipError, ContainerUnavailable, OwnedContainer
from engagequeue.backends.native import resources_stopped, supervise
from engagequeue.backends.processes import boot_identity, identity, signal_owned
from engagequeue.backends.systemd import Unit


def configuration(**kwargs):
    return {
        "runtime": "docker",
        "name": "owned-example",
        "label_key": "engagequeue.attempt",
        "label_value": "unique-attempt",
        "executable": "docker",
        **kwargs,
    }


def docker_data(label="unique-attempt", identifier="a" * 64, running=True):
    return [
        {
            "Id": identifier,
            "Name": "/owned-example",
            "State": {"Running": running},
            "Config": {"Labels": {"engagequeue.attempt": label}},
        }
    ]


def test_foreign_container_is_never_signalled(monkeypatch):
    calls = []

    def execute(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0, stdout=json.dumps(docker_data(label="another-job")), stderr="")

    monkeypatch.setattr(subprocess, "run", execute)
    with pytest.raises(ContainerOwnershipError):
        OwnedContainer(configuration()).stop(force=True)
    assert len(calls) == 1 and "inspect" in calls[0]


def test_owned_container_pins_id_and_verifies_every_stop(monkeypatch):
    calls = []
    data = docker_data()

    def execute(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0, stdout=json.dumps(data) if "inspect" in command else "", stderr="")

    monkeypatch.setattr(subprocess, "run", execute)
    container = OwnedContainer(configuration(executable="sudo -n docker", host="unix:///run/docker.sock"))
    assert container.running()
    container.stop(force=True)
    assert calls[-1] == ["sudo", "-n", "docker", "--host", "unix:///run/docker.sock", "kill", "--signal=KILL", "a" * 64]
    data[:] = docker_data(identifier="b" * 64)
    with pytest.raises(ContainerOwnershipError):
        container.stop(force=True)
    assert sum("kill" in item for item in calls) == 1


def test_container_missing_is_distinct_from_runtime_unavailable(monkeypatch):
    result = SimpleNamespace(returncode=1, stdout="", stderr="Error: No such object: owned-example")
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: result)
    assert not OwnedContainer(configuration()).running()
    result.stderr = "Cannot connect to Docker daemon"
    with pytest.raises(ContainerUnavailable):
        OwnedContainer(configuration()).running()


def test_wrong_container_id_file_never_authorizes_stop(tmp_path, monkeypatch):
    cid = tmp_path / "cid"
    cid.write_text("not-a-container-id")
    monkeypatch.setattr(
        subprocess, "run", lambda *args, **kwargs: pytest.fail("Invalid ID must fail before contacting runtime")
    )
    with pytest.raises(ContainerOwnershipError):
        OwnedContainer(configuration(cid_file=str(cid))).stop()


def test_reused_process_id_is_never_signalled():
    me = identity(os.getpid())
    me["birth"] -= 1000
    assert signal_owned(me, signal.SIGKILL) is False


def test_native_uncertain_descendants_require_explicit_reconciliation():
    record = {
        "spec": {},
        "worker_boot": boot_identity(),
        "backend": "native",
        "command_started": time.time(),
        "execution_granted": True,
        "command_members": [],
    }
    assert resources_stopped(record) is None
    record["worker_boot"] = {**boot_identity(), "boot_id": "previous-boot"}
    assert resources_stopped(record) is True


def test_resources_stop_does_not_signal_unowned_pid_or_container(monkeypatch):
    me = identity(os.getpid())
    record = {"spec": {}, "backend": "native", "worker_boot": boot_identity(), "command_members": [me]}
    assert resources_stopped(record) is False
    record["writers_stopped"] = True
    assert resources_stopped(record) is True


def test_systemd_start_retains_unit_results_and_uses_explicit_resources(monkeypatch):
    calls = []
    monkeypatch.setattr("engagequeue.backends.systemd.shutil.which", lambda name: "/usr/bin/" + name)
    monkeypatch.setattr(
        Unit, "_run", lambda self, *args: calls.append(args) or SimpleNamespace(returncode=0, stdout="", stderr="")
    )
    unit = Unit("example", "token", {"timeout": 30, "memory_mb": 64, "cpus": 2}, 0.2)
    unit.start(["/explicit/python", "-m", "engagequeue.worker"])
    command = calls[-1]
    assert "--collect" not in command
    assert "RemainAfterExit=yes" in command and "ExitType=cgroup" in command
    assert "MemoryMax=67108864" in command and "CPUQuota=200.0%" in command
    assert "KillMode=control-group" in command
    assert not any(arg.startswith("--setenv") for arg in command)
    assert not any("stop" in call for call in calls)


def test_very_short_systemd_result_is_committed_before_unit_cleanup(tmp_path, monkeypatch):
    store, row = prepare(tmp_path, "pass", backend="systemd")
    cleaned = []

    class FastUnit:
        def __init__(self, *args):
            self.name = "fake.service"

        def start(self, argv):
            store.transition(row["id"], "RUNNING", started_at=time.time())
            store.update(row["id"], execution_returncode=0, execution_finished=time.time(), execution_granted=True)

        def processes(self):
            return []

        def show(self):
            return {"Result": "success"}

        def quiescent(self):
            return True

        def cleanup(self):
            current = store.get(row["id"])
            assert current["state"] == "SUCCEEDED" and current["writers_stopped"]
            cleaned.append(True)

    monkeypatch.setattr("engagequeue.backends.systemd.Unit", FastUnit)
    assert supervise(str(store.root), row["id"], row["worker_token"], systemd=True) == 0
    assert cleaned == [True]


def test_live_systemd_user_unit(tmp_path):
    require_systemd()
    store, row = prepare(tmp_path, "print('systemd verified')", backend="systemd")
    process = launch(store, row)
    current = await_record(store, row, timeout=20)
    finish(process)
    assert current["state"] == "SUCCEEDED", current["reason"]
    assert (tmp_path / "stdout").read_text() == "systemd verified\n"
    assert current["writers_stopped"]


@pytest.mark.parametrize("substate,expected", [("running", False), ("start", False), ("exited", True), ("dead", True)])
def test_systemd_empty_cgroup_only_confirms_retained_terminal_unit(monkeypatch, substate, expected):
    unit = Unit("unit", "token", {}, 0.1)
    monkeypatch.setattr(
        unit,
        "show",
        lambda: {"ControlGroup": "", "LoadState": "loaded", "SubState": substate, "TasksCurrent": "[not set]"},
    )
    assert unit.quiescent() is expected


@pytest.mark.parametrize(
    "message",
    [
        "error during connect: dial tcp: lookup docker.invalid: no such host",
        "docker: command not found",
        "Error: No such object: a-different-job",
        "Cannot connect to Docker daemon: socket not found",
    ],
)
def test_runtime_and_dns_errors_never_prove_container_absence(monkeypatch, message):
    result = SimpleNamespace(returncode=1, stdout="", stderr=message)
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: result)
    with pytest.raises(ContainerUnavailable):
        OwnedContainer(configuration()).running()


@pytest.mark.parametrize(
    "message", ["Error: No such object: owned-example", "Error response from daemon: No such container: owned-example"]
)
def test_only_exact_daemon_absence_of_queried_container_is_accepted(monkeypatch, message):
    result = SimpleNamespace(returncode=1, stdout="[]", stderr=message)
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: result)
    assert not OwnedContainer(configuration()).running()


@pytest.mark.parametrize("platform", ["linux", "darwin"])
def test_exception_cannot_publish_stopped_writers_before_kernel_quiescence(tmp_path, monkeypatch, platform):
    store, row = prepare(tmp_path, "pass")
    original_boot = boot_identity()
    monkeypatch.setattr("engagequeue.backends.native.boot_identity", lambda: original_boot)
    monkeypatch.setattr("engagequeue._platform.system_name", lambda: platform)

    class FailingLauncher:
        def __init__(self, *args):
            self.calls = 0

        def start(self, argv):
            pass

        def processes(self):
            self.calls += 1
            if self.calls == 1:
                raise OSError("controller evidence write failed")
            # An empty snapshot cannot stand in for the launcher's independent
            # proof of process-group, child, or cgroup quiescence.
            return []

        def quiescent(self):
            return False

        def cleanup(self):
            pytest.fail("Unknown live writers cannot authorize cleanup")

    monkeypatch.setattr("engagequeue.backends.native.NativeLauncher", FailingLauncher)
    monkeypatch.setattr("engagequeue.backends.darwin.DarwinLauncher", FailingLauncher)
    assert supervise(str(store.root), row["id"], row["worker_token"]) == 125
    result = store.get(row["id"])
    assert result["state"] == "LOST" and not result["writers_stopped"]

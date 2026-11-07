"""Real isolated workers verify ownership, durable exits, and cancellation."""

import hashlib
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from engagequeue.backends.processes import alive, identity
from engagequeue.models import JobSpec, PoolConfig
from engagequeue.store import Store

ROOT = Path(__file__).resolve().parents[1]


def prepare(tmp_path, code, *, backend="native", grace=0.15, **kwargs):
    root = tmp_path / "state"
    store = Store(root)
    store.ensure_pool("default", PoolConfig(backend=backend, cancel_grace=grace))
    spec = JobSpec(
        argv=[sys.executable, "-c", code],
        cwd=str(tmp_path),
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
        stdout=str(tmp_path / "stdout"),
        stderr=str(tmp_path / "stderr"),
        **kwargs,
    ).to_dict()
    job_id = str(uuid.uuid4())
    store.create(
        {
            "id": job_id,
            "key": job_id,
            "digest": hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest(),
            "state": "QUEUED",
            "pool": "default",
            "backend": backend,
            "cancel_grace": grace,
            "created_at": time.time(),
            "spec": spec,
        }
    )
    row = store.reserve_next("default")
    return store, row


def launch(store, row, *, bootstrap=None):
    args = [
        sys.executable,
        "-m",
        "engagequeue.worker",
        "--state-dir",
        str(store.root),
        "--job-id",
        row["id"],
        "--token",
        row["worker_token"],
    ]
    if bootstrap:
        args = [sys.executable, "-c", bootstrap, *args[3:]]
    return subprocess.Popen(
        args,
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )


def await_record(
    store, row, predicate=lambda r: r["state"] in ("SUCCEEDED", "FAILED", "CANCELLED", "LOST"), timeout=12
):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = store.get(row["id"])
        if predicate(current):
            return current
        time.sleep(0.025)
    current = store.get(row["id"])
    pytest.fail(f"Worker did not reach expected state: {current['state']}; {current.get('reason')}")


def finish(process):
    _, stderr = process.communicate(timeout=5)
    assert process.returncode is not None, stderr.decode()


@pytest.mark.parametrize(
    "code,expected,state",
    [
        ("print('exact output')", 0, "SUCCEEDED"),
        ("import sys; print('failure detail',file=sys.stderr); sys.exit(7)", 7, "FAILED"),
    ],
)
def test_real_command_exit_and_output_survive_worker_collection(tmp_path, code, expected, state):
    store, row = prepare(tmp_path, code)
    process = launch(store, row)
    current = await_record(store, row)
    finish(process)
    assert current["state"] == state and current["returncode"] == expected, current.get("reason")
    assert current["writers_stopped"]
    assert (tmp_path / "stdout").read_text() == ("exact output\n" if expected == 0 else "")
    assert (tmp_path / "stderr").read_text() == ("failure detail\n" if expected else "")
    assert store.database.stat().st_mode & 0o777 == 0o600


@pytest.mark.skipif(sys.platform != "linux", reason="Linux subreaper contains session-escaping descendants")
def test_daemonized_double_fork_writer_keeps_job_active(tmp_path):
    code = """
import os,time
from pathlib import Path
if os.fork() == 0:
    os.setsid()
    if os.fork() == 0:
        time.sleep(.35)
        Path('daemon-finished').write_text('complete')
        os._exit(0)
    os._exit(0)
os._exit(0)
"""
    store, row = prepare(tmp_path, code)
    process = launch(store, row)
    current = await_record(store, row)
    finish(process)
    assert current["state"] == "SUCCEEDED", current.get("reason")
    assert (tmp_path / "daemon-finished").read_text() == "complete"


@pytest.mark.skipif(sys.platform != "linux", reason="Linux subreaper contains session-escaping descendants")
def test_cancel_terminates_setsid_descendant_and_waits_for_writers(tmp_path):
    code = """
import os,time
from pathlib import Path
Path('parent.pid').write_text(str(os.getpid()))
if os.fork() == 0:
    os.setsid()
    Path('daemon.pid').write_text(str(os.getpid()))
    while True: time.sleep(.01)
while True: time.sleep(.01)
"""
    store, row = prepare(tmp_path, code)
    process = launch(store, row)
    deadline = time.monotonic() + 8
    while not (tmp_path / "daemon.pid").exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    records = [identity(int((tmp_path / name).read_text())) for name in ("parent.pid", "daemon.pid")]
    store.request_cancel(row["id"])
    current = await_record(store, row)
    finish(process)
    assert current["state"] == "CANCELLED" and current["writers_stopped"], current.get("reason")
    assert all(alive(item) is False for item in records)


def test_ignored_term_escalates_after_grace(tmp_path):
    code = "import signal,time; from pathlib import Path; signal.signal(signal.SIGTERM,signal.SIG_IGN); Path('ready').touch(); time.sleep(30)"
    store, row = prepare(tmp_path, code)
    process = launch(store, row)
    deadline = time.monotonic() + 8
    while not (tmp_path / "ready").exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert (tmp_path / "ready").exists()
    start = time.monotonic()
    store.request_cancel(row["id"])
    assert store.get(row["id"])["state"] == "STOPPING"
    current = await_record(store, row)
    finish(process)
    assert current["state"] == "CANCELLED" and time.monotonic() - start >= 0.1, current.get("reason")


@pytest.mark.parametrize("limit,expected", [({"timeout": 0.25}, 124), ({"output_limit": 128}, 153)])
def test_execution_limits_stop_writers(tmp_path, limit, expected):
    store, row = prepare(tmp_path, "import time; print('x'*4096,flush=True); time.sleep(30)", **limit)
    process = launch(store, row)
    current = await_record(store, row)
    finish(process)
    assert current["state"] == "FAILED" and current["returncode"] == expected, current.get("reason")
    assert current["writers_stopped"]


def test_native_rejects_requested_hard_limits_before_execution(tmp_path):
    store, row = prepare(
        tmp_path, "from pathlib import Path; Path('should-not-run').touch()", memory_mb=16, require_hard_limits=True
    )
    process = launch(store, row)
    current = await_record(store, row)
    finish(process)
    assert current["state"] == "FAILED" and current["returncode"] == 125, current.get("reason")
    assert not (tmp_path / "should-not-run").exists()


def test_duplicate_worker_does_not_repeat_command(tmp_path):
    store, row = prepare(
        tmp_path,
        "from pathlib import Path; import time; f=Path('count'); f.write_text(f.read_text()+'x' if f.exists() else 'x'); time.sleep(.2)",
    )
    first = launch(store, row)
    await_record(store, row, lambda record: record["worker_pid"] is not None)
    second = launch(store, row)
    current = await_record(store, row)
    finish(first)
    finish(second)
    assert current["state"] == "SUCCEEDED", current.get("reason")
    assert current["writers_stopped"]
    assert (tmp_path / "count").read_text() == "x"


def test_dead_child_before_registration_fails_and_frees_capacity(tmp_path):
    store, row = prepare(tmp_path, "raise RuntimeError('must not execute')")
    bootstrap = """
import subprocess,sys
from engagequeue.backends.native import NativeLauncher
from engagequeue.backends.darwin import DarwinLauncher
from engagequeue.worker import main
def start(self,argv):
    self.process=subprocess.Popen([sys.executable,'-c','pass'],start_new_session=True)
    self.pgid=self.process.pid
NativeLauncher.start=start
DarwinLauncher.start=start
sys.exit(main())
"""
    process = launch(store, row, bootstrap=bootstrap)
    current = await_record(store, row, timeout=4)
    finish(process)
    assert current["state"] == "FAILED" and "before registration" in current["reason"], current.get("reason")
    assert current["writers_stopped"] and not current["command_pid"]


def test_systemd_unavailable_is_explicit_and_releases_unlaunched_job(tmp_path):
    store, row = prepare(tmp_path, "print('must not run')", backend="systemd")
    bootstrap = """
import sys
from engagequeue.backends.systemd import Unit,SystemdUnavailable
from engagequeue.worker import main
def start(self,argv): raise SystemdUnavailable('no user manager')
Unit.start=start
sys.exit(main())
"""
    process = launch(store, row, bootstrap=bootstrap)
    current = await_record(store, row)
    finish(process)
    assert current["state"] == "FAILED" and current["writers_stopped"], current.get("reason")
    assert "no user manager" in current["reason"]


@pytest.mark.skipif(sys.platform != "linux", reason="Requires Linux subreaper/kernel child accounting")
def test_kernel_quiescence_blocks_completion_when_process_snapshot_misses_daemon(tmp_path):
    """A deliberate empty /proc snapshot cannot stand in for kernel ECHILD."""
    code = """
import os,time
from pathlib import Path
if os.fork() == 0:
    os.setsid()
    if os.fork() == 0:
        Path('daemon-ready').touch()
        while not Path('release-daemon').exists(): time.sleep(.01)
        Path('daemon-finished').touch()
        os._exit(0)
    os._exit(0)
os._exit(0)
"""
    store, row = prepare(tmp_path, code, timeout=10)
    bootstrap = """
import sys
from engagequeue.backends.native import NativeLauncher
from engagequeue.worker import main
NativeLauncher.processes=lambda self: []
sys.exit(main())
"""
    process = launch(store, row, bootstrap=bootstrap)
    await_record(store, row, lambda value: value.get("execution_finished") is not None)
    assert (tmp_path / "daemon-ready").exists()
    assert store.get(row["id"])["state"] == "RUNNING"
    assert not (tmp_path / "daemon-finished").exists()
    (tmp_path / "release-daemon").touch()
    current = await_record(store, row)
    finish(process)
    assert current["state"] == "SUCCEEDED" and current["writers_stopped"], current.get("reason")
    assert (tmp_path / "daemon-finished").exists()


@pytest.mark.skipif(sys.platform != "linux", reason="Requires Linux subreaper/kernel child accounting")
def test_concurrent_double_fork_writers_all_finish_before_terminal(tmp_path):
    code = """
import os,time
from pathlib import Path
if os.fork() == 0:
    os.setsid()
    if os.fork() == 0:
        time.sleep(.05)
        Path('daemon-finished').touch()
        os._exit(0)
    os._exit(0)
os._exit(0)
"""
    attempts = []
    for index in range(12):
        directory = tmp_path / str(index)
        directory.mkdir()
        store, row = prepare(directory, code)
        attempts.append((directory, store, row, launch(store, row)))
    for directory, store, row, process in attempts:
        current = await_record(store, row)
        finish(process)
        assert current["state"] == "SUCCEEDED" and current["writers_stopped"], current.get("reason")
        assert (directory / "daemon-finished").exists()

"""A failed supervisor launch must not block other accepted executions."""

import os
import sqlite3
import subprocess
import time
from pathlib import Path

import pytest

from engagequeue.models import JobSpec, PoolConfig
from engagequeue.runtime import Runtime
from engagequeue.store import Store


def _queued_runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[1] / "src"))
    store = Store(tmp_path / "state")
    store.ensure_pool("default", PoolConfig(1))
    for identifier in ("bad", "good"):
        spec = JobSpec(
            ["/usr/bin/true"],
            str(tmp_path),
            env={},
            stdout=str(tmp_path / (identifier + ".stdout")),
            stderr=str(tmp_path / (identifier + ".stderr")),
        ).to_dict()
        store.create(
            {
                "id": identifier,
                "key": identifier,
                "digest": identifier,
                "spec": spec,
                "pool": "default",
                "backend": "native",
                "cancel_grace": 0.1,
                "created_at": time.time(),
                "state": "QUEUED",
            }
        )
    return Runtime(str(store.root))


def _finish_worker(runtime, job_id):
    process = runtime.children[job_id]
    try:
        assert process.wait(timeout=15) == 0
        assert runtime.store.get(job_id)["state"] == "SUCCEEDED"
    finally:
        if process.poll() is None:
            runtime.store.request_cancel(job_id)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@pytest.mark.parametrize("failure", ["directory", "log", "spawn"])
def test_launch_failure_releases_capacity_for_next_job(tmp_path, monkeypatch, failure):
    runtime = _queued_runtime(tmp_path, monkeypatch)
    record = runtime.store.reserve_next("default")
    assert record["id"] == "bad"
    directory = runtime.root / "jobs" / record["id"]
    if failure == "directory":
        directory.parent.mkdir()
        directory.write_text("This caller-owned path is not a directory")
    elif failure == "log":
        (directory / "supervisor.log").mkdir(parents=True)

    def fail_spawn(*args, **kwargs):
        raise OSError("simulated process launch failure")

    with monkeypatch.context() as patch:
        if failure == "spawn":
            patch.setattr("engagequeue.runtime.subprocess.Popen", fail_spawn)
        runtime.launch(record)

    failed = runtime.store.get("bad")
    assert failed["state"] == "FAILED"
    assert failed["writers_stopped"] is True
    assert failed["returncode"] == 70
    assert failed["reason"].startswith("Unable to start supervisor:")
    assert "bad" not in runtime.children
    next_record = runtime.store.reserve_next("default")
    assert next_record["id"] == "good"
    runtime.launch(next_record)
    _finish_worker(runtime, "good")


def test_failure_publication_database_error_propagates(tmp_path, monkeypatch):
    runtime = _queued_runtime(tmp_path, monkeypatch)
    record = runtime.store.reserve_next("default")
    (runtime.root / "jobs" / record["id"] / "supervisor.log").mkdir(parents=True)
    failure = sqlite3.OperationalError("cannot persist launch failure")

    def fail_transition(*args, **kwargs):
        raise failure

    monkeypatch.setattr(runtime.store, "transition", fail_transition)
    with pytest.raises(sqlite3.OperationalError) as raised:
        runtime.launch(record)
    assert raised.value is failure
    assert runtime.store.get(record["id"])["state"] == "STARTING"
    assert not runtime.children


def test_log_close_error_does_not_fail_a_started_worker(tmp_path, monkeypatch):
    runtime = _queued_runtime(tmp_path, monkeypatch)
    record = runtime.store.reserve_next("default")
    original_fdopen = os.fdopen

    class FailingClose:
        def __init__(self, descriptor, mode):
            self.output = original_fdopen(descriptor, mode)

        def __enter__(self):
            return self.output

        def __exit__(self, *args):
            self.output.close()
            raise OSError("simulated log close failure after spawn")

    with monkeypatch.context() as patch:
        patch.setattr("engagequeue.runtime.os.fdopen", FailingClose)
        runtime.launch(record)
    assert record["id"] in runtime.children
    _finish_worker(runtime, record["id"])

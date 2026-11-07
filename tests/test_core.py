"""Public API and transactional scheduling/recovery acceptance tests."""

from __future__ import annotations

import concurrent.futures
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import psutil
import pytest

from engagequeue import (
    AdmissionClosed,
    ContainerSpec,
    Executor,
    JobSpec,
    JobState,
    LostJobError,
    PoolConfig,
    RuntimeUnavailable,
    SubmissionConflict,
)
from engagequeue.models import capacity
from engagequeue.store import Store


def await_file(path, timeout=15):
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() > deadline:
            raise AssertionError(f"Expected artifact was not created: {path}")
        time.sleep(0.05)


@pytest.fixture
def executor(tmp_path):
    client = Executor(tmp_path / "state")
    yield client
    # Cancel accepted work before stopping its runtime; leave other test roots
    # untouched and never depend on a global service or process-name match.
    for status in client.list():
        if not status.terminal:
            try:
                client.cancel(status.id, wait=True, timeout=8)
            except (LostJobError, TimeoutError):
                pass
    try:
        client.shutdown(wait=True, timeout=8)
    except TimeoutError:
        runtime = client.store.get_meta("runtime", {})
        if runtime.get("pid"):
            process = psutil.Process(runtime["pid"])
            if abs(process.create_time() - runtime["birth"]) < 0.01:
                process.terminate()
    client.close()


def test_real_command_success_and_user_workspace_pruning(executor, tmp_path):
    output = tmp_path / "caller-output"
    job = executor.submit(
        JobSpec(
            [
                sys.executable,
                "-c",
                "from pathlib import Path; Path('caller-output').write_text('exact result'); print('captured')",
            ],
            str(tmp_path),
        ),
        key="success",
    )
    result = executor.wait(job.id, 15)
    assert result.state is JobState.SUCCEEDED
    assert result.returncode == 0 and result.writers_stopped
    assert output.read_text() == "exact result"
    assert Path(executor.store.get(job.id)["spec"]["stdout"]).read_text() == "captured\n"
    assert executor.prune(job.id) == 1
    assert output.read_text() == "exact result"
    assert executor.lookup("success").id == job.id
    assert executor.status(job.id).state is JobState.SUCCEEDED
    assert executor.list() == []
    assert executor.prune(job.id) == 0


def test_lost_submission_response_does_not_repeat_command(executor, tmp_path, monkeypatch):
    spec = JobSpec(
        [
            sys.executable,
            "-c",
            "from pathlib import Path; p=Path('count'); p.write_text(p.read_text()+'one\\n' if p.exists() else 'one\\n')",
        ],
        str(tmp_path),
    )
    accepted = executor.submit(spec, key="accepted-before-response")
    # Different inherited process environment after a controller restart must
    # not change the explicit submission identity.
    monkeypatch.setenv("TRANSIENT_CONTROLLER_PID", "a new controller")
    retried = executor.submit(spec, key="accepted-before-response")
    assert retried.id == accepted.id
    executor.wait(accepted.id, 15)
    assert (tmp_path / "count").read_text() == "one\n"
    with pytest.raises(SubmissionConflict):
        executor.submit(JobSpec(["/usr/bin/false"], str(tmp_path)), key="accepted-before-response")


def test_close_disconnects_only_and_all_client_death_preserves_queue(tmp_path):
    code = r"""
import json,os,sys
from pathlib import Path
from engagequeue import Executor,JobSpec,PoolConfig
root=Path(sys.argv[1]); client=Executor(root/'state',pools={'batch':PoolConfig(1)})
first=client.submit(JobSpec([sys.executable,'-c',"import time; from pathlib import Path; Path('started').touch();\nwhile not Path('release').exists(): time.sleep(.05)"],str(root),pool='batch'),key='first')
second=client.submit(JobSpec([sys.executable,'-c',"from pathlib import Path; Path('continued-without-clients').touch()"],str(root),pool='batch'),key='second')
(root/'jobs.json').write_text(json.dumps([first.id,second.id])); client.close(); os._exit(0)
"""
    parent = subprocess.run([sys.executable, "-c", code, str(tmp_path)], check=True, timeout=20)
    assert parent.returncode == 0
    await_file(tmp_path / "started")
    (tmp_path / "release").touch()
    # Do not open another Executor until the queued command has started. This
    # proves admission survives every embedding client's lifetime.
    await_file(tmp_path / "continued-without-clients")
    with Executor(tmp_path / "state") as client:
        ids = json.loads((tmp_path / "jobs.json").read_text())
        assert all(client.wait(identifier, 15).state is JobState.SUCCEEDED for identifier in ids)
        client.shutdown(wait=True, timeout=8)


def test_coordinator_restart_adopts_worker_without_relaunch(executor, tmp_path):
    job = executor.submit(
        JobSpec(
            [
                sys.executable,
                "-c",
                "import time; from pathlib import Path; Path('execution').write_text('one'); Path('started').touch();\nwhile not Path('release').exists(): time.sleep(.05)",
            ],
            str(tmp_path),
        ),
        key="coordinator-restart",
    )
    await_file(tmp_path / "started")
    record = executor.store.get(job.id)
    worker = record["worker_pid"]
    runtime = executor.store.get_meta("runtime")
    os.kill(runtime["pid"], signal.SIGKILL)
    deadline = time.monotonic() + 5
    while psutil.pid_exists(runtime["pid"]):
        try:
            if psutil.Process(runtime["pid"]).status() == psutil.STATUS_ZOMBIE:
                break
        except psutil.NoSuchProcess:
            break
        if time.monotonic() > deadline:
            pytest.fail("Coordinator did not exit")
        time.sleep(0.05)
    assert executor.status(job.id).state is JobState.RUNNING
    assert executor.store.get(job.id)["worker_pid"] == worker
    assert executor.store.get_meta("runtime")["pid"] != runtime["pid"]
    (tmp_path / "release").touch()
    assert executor.wait(job.id, 15).state is JobState.SUCCEEDED
    assert (tmp_path / "execution").read_text() == "one"


def test_fifo_capacity_is_shared_across_independent_clients(executor, tmp_path):
    executor.configure_pool("bounded", PoolConfig(1))
    with Executor(executor.state_dir, pools={"other": PoolConfig(None)}) as another:
        assert set(another.store.pools()) == {"default", "bounded", "other"}
        first = executor.submit(
            JobSpec(
                [
                    sys.executable,
                    "-c",
                    "import time; from pathlib import Path; Path('first-started').touch();\nwhile not Path('release').exists(): time.sleep(.05)",
                ],
                str(tmp_path),
                pool="bounded",
            ),
            key="fifo-first",
        )
        await_file(tmp_path / "first-started")
        second = another.submit(
            JobSpec(
                [sys.executable, "-c", "from pathlib import Path; Path('second-started').touch()"],
                str(tmp_path),
                pool="bounded",
            ),
            key="fifo-second",
        )
        time.sleep(0.3)
        assert another.status(second.id).state is JobState.QUEUED
        assert not (tmp_path / "second-started").exists()
        (tmp_path / "release").touch()
        assert executor.wait(first.id, 15).state is JobState.SUCCEEDED
        assert another.wait(second.id, 15).state is JobState.SUCCEEDED


def test_queued_cancel_and_drain_are_explicit(executor, tmp_path):
    executor.drain()
    with pytest.raises(AdmissionClosed):
        executor.submit(JobSpec(["/usr/bin/true"], str(tmp_path)), key="closed-admission")
    executor.resume()
    executor.configure_pool("blocked", PoolConfig(1))
    first = executor.submit(
        JobSpec([sys.executable, "-c", "import time; time.sleep(30)"], str(tmp_path), pool="blocked"),
        key="blocked-first",
    )
    deadline = time.monotonic() + 15
    while executor.status(first.id).state is not JobState.RUNNING:
        assert time.monotonic() < deadline
        time.sleep(0.05)
    queued = executor.submit(
        JobSpec(
            [sys.executable, "-c", "from pathlib import Path; Path('must-not-execute').touch()"],
            str(tmp_path),
            pool="blocked",
        ),
        key="blocked-second",
    )
    assert executor.cancel(queued.id, wait=True, timeout=5).state is JobState.CANCELLED
    assert not (tmp_path / "must-not-execute").exists()
    assert executor.cancel(first.id, wait=True, timeout=10).state is JobState.CANCELLED


def test_environment_inspection_is_redacted(executor, tmp_path):
    job = executor.submit(
        JobSpec(["/usr/bin/true"], str(tmp_path), env={"TEST_SECRET": "do-not-print"}), key="redaction"
    )
    executor.wait(job.id, 15)
    report = executor.inspect(job.id)
    assert report["spec"]["env"]["TEST_SECRET"] == "<redacted>"
    assert "do-not-print" not in json.dumps(report)
    assert (Path(executor.state_dir) / "state.sqlite3").stat().st_mode & 0o777 == 0o600
    assert Path(executor.state_dir).stat().st_mode & 0o777 == 0o700


def test_job_spec_multiple_containers_round_trip(tmp_path):
    first = ContainerSpec(name="tool", label_value="attempt")
    second = ContainerSpec(name="metadata", label_value="attempt")
    spec = JobSpec(["/usr/bin/true"], str(tmp_path), container=first, containers=(second,))
    restored = JobSpec.from_dict(spec.to_dict())
    assert restored == spec
    assert restored.containers == (second,)


@pytest.mark.parametrize("value,expected", [(None, 0), (0, 0), ("0", 0), (1, 1), ("50", 50)])
def test_pool_capacity_normalization(value, expected):
    assert capacity(value) == expected


@pytest.mark.parametrize("value", [-1, True, False, "-1", "unlimited", 1.5, float("nan")])
def test_pool_capacity_invalid_values_fail(value):
    with pytest.raises(ValueError):
        PoolConfig(value)


def test_store_concurrent_idempotent_acceptance(tmp_path):
    root = tmp_path / "state"
    store = Store(root)
    store.ensure_pool("default", PoolConfig())
    record = {
        "id": "one",
        "key": "stable-key",
        "digest": "exact-spec",
        "state": "QUEUED",
        "pool": "default",
        "created_at": time.time(),
        "spec": {},
        "backend": "native",
        "cancel_grace": 10,
    }

    def accept(_):
        return Store(root).create(record)[1]

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        accepted = list(pool.map(accept, range(20)))
    assert accepted.count(True) == 1
    assert len(store.list()) == 1


def test_store_requires_writer_proof_and_holds_uncertain_capacity(tmp_path):
    store = Store(tmp_path / "state")
    store.ensure_pool("default", PoolConfig(1))
    for identifier in ("first", "second"):
        store.create(
            {
                "id": identifier,
                "key": identifier,
                "digest": identifier,
                "state": "QUEUED",
                "pool": "default",
                "created_at": time.time(),
                "spec": {},
                "backend": "native",
                "cancel_grace": 10,
            }
        )
    first = store.reserve_next("default")
    assert first["id"] == "first"
    assert store.transition("first", "LOST", reason="Ownership unknown")
    assert store.reserve_next("default") is None
    with pytest.raises(ValueError, match="writer termination"):
        store.transition("first", "FAILED")
    with pytest.raises(ValueError, match="terminal"):
        store.delete_terminal("first")
    assert store.transition("first", "FAILED", writers_stopped=True)
    assert store.reserve_next("default")["id"] == "second"


def test_preloaded_handle_is_usable_after_fork(executor, tmp_path):
    identifier = os.fork()
    if identifier == 0:
        try:
            executor.configure_pool("fork-child", PoolConfig(2))
            executor.close()
            os._exit(0)
        except BaseException:
            os._exit(1)
    _, status = os.waitpid(identifier, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    assert executor.store.pool("fork-child")["max_jobs"] == 2
    # Closing the inherited child handle did not close its parent copy.
    job = executor.submit(JobSpec(["/usr/bin/true"], str(tmp_path)), key="after-fork")
    assert executor.wait(job.id, 15).state is JobState.SUCCEEDED


def test_implicit_clients_adopt_persisted_interpreter_without_borrowing_caller_prefix(tmp_path, monkeypatch):
    original = Executor(tmp_path / "state")
    accepted_python = original.runtime_python
    monkeypatch.setattr("engagequeue.executor.sys.prefix", str(tmp_path / "another-client-env"))
    attached = Executor(tmp_path / "state")
    assert attached.runtime_python == accepted_python
    assert attached.store.get_meta("runtime_python") == accepted_python
    attached.close()
    alternative = tmp_path / "another-runtime-python"
    alternative.symlink_to(accepted_python)
    with pytest.raises(ValueError, match="different runtime_python"):
        Executor(tmp_path / "state", runtime_python=str(alternative))
    original.close()


def test_pruned_acceptance_receipt_prevents_late_retry(executor, tmp_path):
    spec = JobSpec(
        [
            sys.executable,
            "-c",
            "from pathlib import Path; p=Path('count'); p.write_text(p.read_text()+'one\\n' if p.exists() else 'one\\n')",
        ],
        str(tmp_path),
        env={"PRIVATE_VALUE": "remove-after-prune"},
    )
    job = executor.submit(spec, key="retained-receipt")
    executor.wait(job.id, 15)
    assert executor.prune(job.id) == 1
    assert "remove-after-prune" not in json.dumps(executor.store.get(job.id))
    assert not (Path(executor.state_dir) / "jobs" / job.id).exists()
    assert executor.submit(spec, key="retained-receipt").id == job.id
    assert (tmp_path / "count").read_text() == "one\n"
    with pytest.raises(SubmissionConflict):
        executor.submit(JobSpec(["/usr/bin/false"], str(tmp_path)), key="retained-receipt")


def uncertain_record(executor, tmp_path, *, live_payload=False):
    spec = JobSpec(["/usr/bin/true"], str(tmp_path)).to_dict()
    record = {
        "id": "uncertain",
        "key": "uncertain",
        "digest": "uncertain",
        "state": "QUEUED",
        "pool": "default",
        "created_at": time.time(),
        "spec": spec,
        "backend": "native",
        "cancel_grace": 10,
    }
    executor.store.create(record)
    executor.store.reserve_next("default")
    executor.store.update("uncertain", execution_granted=True, command_started=time.time())
    if live_payload:
        executor.store.update("uncertain", payload_pid=os.getpid(), payload_birth=psutil.Process().create_time())
    executor.store.transition("uncertain", "LOST", reason="Supervision ownership lost")


def test_unknown_native_ownership_requires_audited_operator_attestation(executor, tmp_path):
    uncertain_record(executor, tmp_path)
    with pytest.raises(ValueError, match="confirm_stopped"):
        executor.reconcile("uncertain")
    with pytest.raises(ValueError, match="operator reason"):
        executor.reconcile("uncertain", confirm_stopped=True, reason=" ")
    result = executor.reconcile(
        "uncertain", confirm_stopped=True, reason="Checked original execution namespace and stopped escaped descendants"
    )
    assert result.state is JobState.FAILED and result.writers_stopped
    record = executor.store.get("uncertain")
    assert record["resource_verification"] == "operator_attested"
    assert executor.store.events("uncertain")[-1]["details"]["resource_verification"] == "operator_attested"


def test_operator_attestation_cannot_override_a_known_live_writer(executor, tmp_path):
    uncertain_record(executor, tmp_path, live_payload=True)
    with pytest.raises(LostJobError, match="still present"):
        executor.reconcile("uncertain", confirm_stopped=True, reason="Attempted override")
    assert executor.store.get("uncertain")["state"] == "LOST"
    # The known process is this test process; remove its deliberately injected
    # identity before explicitly resolving the fixture.
    executor.store.update("uncertain", payload_pid=None, payload_birth=None)
    executor.reconcile(
        "uncertain", confirm_stopped=True, reason="Removed test-owned identity after proving no execution was started"
    )


def test_fresh_runtime_ignores_unrelated_pythonpath(executor, tmp_path, monkeypatch):
    poisoned = tmp_path / "unrelated-app-source"
    (poisoned / "engagequeue").mkdir(parents=True)
    (poisoned / "engagequeue/__init__.py").write_text("raise RuntimeError('wrong package')")
    monkeypatch.setenv("PYTHONPATH", str(poisoned))
    job = executor.submit(JobSpec(["/usr/bin/true"], str(tmp_path)), key="source-bootstrap")
    assert executor.wait(job.id, 15).state is JobState.SUCCEEDED
    source = executor.store.get_meta("runtime_source")
    assert source is None or Path(source).name == "src"


def test_attach_only_uses_external_lock_without_client_pid_or_interpreter(executor, tmp_path, monkeypatch):
    executor.ensure_runtime()
    stored_python = executor.store.get_meta("runtime_python")
    # Simulate an embedding container whose PID namespace and interpreter
    # filesystem differ while the execution host keeps its actual runtime lock.
    with monkeypatch.context() as patch:
        patch.setattr("engagequeue.executor.pid_namespace", lambda: -123)
        patch.setattr(
            "engagequeue.executor.python_interpreter",
            lambda *_: (_ for _ in ()).throw(AssertionError("Client must not validate host Python")),
        )
        with Executor(executor.state_dir, attach_only=True) as client:
            client.ensure_runtime()
            job = client.submit(JobSpec(["/usr/bin/true"], str(tmp_path)), key="external-host-client")
            assert client.wait(job.id, 15).state is JobState.SUCCEEDED
            assert client.runtime_python == stored_python
    executor.shutdown(wait=True, timeout=8)
    with Executor(executor.state_dir, attach_only=True) as offline:
        with pytest.raises(RuntimeUnavailable, match="External runtime is not ready"):
            offline.ensure_runtime()
        # Receipt lookup precedes new-job checks; closed admission is rejected
        # before preparing a new execution or probing its external runtime.
        with pytest.raises(AdmissionClosed, match="Execution admission is closed"):
            offline.submit(JobSpec(["/usr/bin/true"], str(tmp_path)), key="must-not-accept-offline")
        assert offline.lookup("must-not-accept-offline") is None
        offline.store.put_meta("admission_open", True)
        with pytest.raises(RuntimeUnavailable, match="External runtime is not ready"):
            offline.submit(JobSpec(["/usr/bin/true"], str(tmp_path)), key="open-but-offline")
        assert offline.lookup("open-but-offline") is None


@pytest.mark.parametrize("value", [0, -1, True, float("nan"), float("inf")])
def test_runtime_poll_interval_rejects_invalid_values(tmp_path, value):
    with pytest.raises(ValueError, match="poll_interval"):
        Executor(tmp_path / "invalid-state", poll_interval=value)


def test_prune_preserves_caller_workspace_under_private_job_directory(executor):
    import uuid

    key = "caller-chosen-private-workspace"
    identifier = str(uuid.uuid5(uuid.UUID(executor.store.get_meta("runtime_id")), key))
    workspace = Path(executor.state_dir) / "jobs" / identifier
    workspace.mkdir(parents=True)
    job = executor.submit(
        JobSpec(
            [sys.executable, "-c", "from pathlib import Path; Path('caller-data').write_text('preserve')"],
            str(workspace),
        ),
        key=key,
    )
    executor.wait(job.id, 15)
    assert executor.prune(job.id) == 1
    assert (workspace / "caller-data").read_text() == "preserve"


def test_future_state_schema_is_rejected_before_table_creation(tmp_path):
    import sqlite3

    root = tmp_path / "future"
    root.mkdir()
    path = root / "state.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT)")
        connection.execute("INSERT INTO meta VALUES('schema','99')")
    with pytest.raises(RuntimeError, match="Unsupported EngageQueue state schema"):
        Store(root)
    with sqlite3.connect(path) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert tables == {"meta"}


def test_cancellation_committed_before_stale_success_wins_transaction(tmp_path):
    store = Store(tmp_path / "state")
    store.ensure_pool("default", PoolConfig(1))
    record = {
        "id": "racing",
        "key": "racing",
        "digest": "racing",
        "state": "QUEUED",
        "pool": "default",
        "created_at": time.time(),
        "spec": {},
        "backend": "native",
        "cancel_grace": 10,
    }
    store.create(record)
    store.reserve_next("default")
    assert store.transition("racing", "RUNNING")
    earlier_worker_snapshot = store.get("racing")
    assert not earlier_worker_snapshot["cancel_requested"]
    assert store.request_cancel("racing")["state"] == "STOPPING"
    # The worker completed its command but selected success using the earlier
    # snapshot. SQLite's terminal transaction must honor the newer cancel.
    assert store.transition("racing", "SUCCEEDED", returncode=0, writers_stopped=True)
    final = store.get("racing")
    assert final["state"] == "CANCELLED" and final["returncode"] == 143
    assert final["writers_stopped"] is True
    assert store.events("racing")[-1]["details"]["to"] == "CANCELLED"


def test_idempotent_cancel_after_committed_success_preserves_success(tmp_path):
    store = Store(tmp_path / "state")
    store.ensure_pool("default", PoolConfig(1))
    record = {
        "id": "done",
        "key": "done",
        "digest": "done",
        "state": "QUEUED",
        "pool": "default",
        "created_at": time.time(),
        "spec": {},
        "backend": "native",
        "cancel_grace": 10,
    }
    store.create(record)
    store.reserve_next("default")
    store.transition("done", "RUNNING")
    store.transition("done", "SUCCEEDED", returncode=0, writers_stopped=True)
    assert store.request_cancel("done")["state"] == "SUCCEEDED"
    assert store.get("done")["returncode"] == 0


def test_explicit_job_environment_does_not_inherit_application_secrets(executor, tmp_path, monkeypatch):
    monkeypatch.setenv("APPLICATION_DATABASE_PASSWORD", "must-stay-in-controller")
    job = executor.submit(
        JobSpec(
            [sys.executable, "-c", "import json,os; print(json.dumps(dict(os.environ)))"],
            str(tmp_path),
            env={"PUBLIC_SETTING": "explicit-value"},
        ),
        key="exact-job-environment",
    )
    assert executor.wait(job.id, 15).state is JobState.SUCCEEDED
    record = executor.store.get(job.id)
    assert record["spec"]["env"] == {"PUBLIC_SETTING": "explicit-value"}
    actual = json.loads(Path(record["spec"]["stdout"]).read_text())
    assert actual["PUBLIC_SETTING"] == "explicit-value"
    assert "APPLICATION_DATABASE_PASSWORD" not in actual

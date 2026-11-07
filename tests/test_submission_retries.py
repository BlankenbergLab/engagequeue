"""Receipt adoption and explicit environment intent remain stable across retries."""

import concurrent.futures
import json
import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from engagequeue import Executor, JobSpec, JobState, PoolConfig, SubmissionConflict, UnsupportedBackend


@pytest.fixture
def client(tmp_path, monkeypatch):
    executor = Executor(tmp_path / "state")
    monkeypatch.setattr(executor, "ensure_runtime", lambda: None)
    yield executor
    executor.close()


@pytest.mark.parametrize("overrides", [None, {}])
def test_empty_overrides_preserve_legacy_serialization(tmp_path, overrides):
    plain = JobSpec(["/bin/true"], str(tmp_path), env={})
    specified = JobSpec(["/bin/true"], str(tmp_path), env={}, env_overrides=overrides)
    assert specified.to_dict() == plain.to_dict()
    assert "env_overrides" not in specified.to_dict()
    assert specified.env == {}


def test_overrides_are_copied_and_round_trip(tmp_path):
    values = {"SETTING": "accepted"}
    spec = JobSpec(["/bin/true"], str(tmp_path), env_overrides=values)
    values["SETTING"] = "mutated"
    assert spec.env_overrides == {"SETTING": "accepted"}
    assert JobSpec.from_dict(spec.to_dict()).to_dict() == spec.to_dict()


@pytest.mark.parametrize("invalid", [[], {"": "x"}, {"A=B": "x"}, {"A\x00": "x"}, {"A": 1}, {"A": "x\x00"}])
def test_invalid_overrides_are_rejected(tmp_path, invalid):
    with pytest.raises(ValueError):
        JobSpec(["/bin/true"], str(tmp_path), env_overrides=invalid)


@pytest.mark.parametrize("environment", [{}, {"EXPLICIT": "only"}])
def test_overrides_cannot_change_explicit_environment_semantics(tmp_path, environment):
    with pytest.raises(ValueError, match="cannot be combined"):
        JobSpec(["/bin/true"], str(tmp_path), env=environment, env_overrides={"OVERRIDE": "x"})


def test_environment_is_captured_once_and_inspection_redacts_it(client, tmp_path, monkeypatch):
    monkeypatch.setenv("AMBIENT_SETTING", "first")
    spec = JobSpec(["/bin/true"], str(tmp_path), env_overrides={"OVERRIDE": "private-override"})
    job = client.submit(spec, key="environment-intent")
    monkeypatch.setenv("AMBIENT_SETTING", "second")
    assert client.submit(spec, key="environment-intent").id == job.id
    stored = client.store.get(job.id)
    assert stored["spec"]["env"]["AMBIENT_SETTING"] == "first"
    assert stored["spec"]["env"]["OVERRIDE"] == "private-override"
    assert "env_overrides" not in stored["spec"]
    assert set(client.inspect(job.id)["spec"]["env"].values()) == {"<redacted>"}
    changed = JobSpec(["/bin/true"], str(tmp_path), env_overrides={"OVERRIDE": "changed"})
    with pytest.raises(SubmissionConflict):
        client.submit(changed, key="environment-intent")


def test_explicit_empty_environment_still_does_not_inherit(client, tmp_path, monkeypatch):
    monkeypatch.setenv("PRIVATE_PARENT", "must-not-inherit")
    job = client.submit(JobSpec(["/bin/true"], str(tmp_path), env={}, env_overrides={}), key="explicit-empty")
    assert client.store.get(job.id)["spec"]["env"] == {}


@pytest.mark.parametrize("pruned", [False, True])
def test_terminal_receipt_precedes_workspace_pool_and_runtime_checks(client, tmp_path, monkeypatch, pruned):
    # This receipt was accepted on a Linux execution host. Later adoption must
    # not repeat the original host's backend capability checks.
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    client.configure_pool("limited", PoolConfig(backend="systemd"))
    spec = JobSpec(
        ["/bin/true"],
        str(workspace),
        pool="limited",
        cpus=1,
        require_hard_limits=True,
        env_overrides={"SECRET": "original-private-value"},
    )
    with monkeypatch.context() as linux:
        linux.setattr("engagequeue._platform.system_name", lambda: "linux")
        job = client.submit(spec, key="receipt-first")
    assert client.store.transition(job.id, "FAILED", writers_stopped=True, returncode=70)
    if pruned:
        assert client.prune(job.id) == 1
        assert "original-private-value" not in json.dumps(client.store.get(job.id))
    before = client.store.get(job.id)
    workspace.rmdir()
    client.configure_pool("limited", PoolConfig(backend="native"))
    with Executor(client.state_dir, attach_only=True) as offline:
        monkeypatch.setattr(offline, "ensure_runtime", lambda: pytest.fail("Terminal adoption must not start work"))
        assert offline.submit(spec, key="receipt-first").state == JobState.FAILED
        changed = JobSpec(["/bin/false"], str(workspace), pool="limited", cpus=1, require_hard_limits=True)
        with pytest.raises(SubmissionConflict):
            offline.submit(changed, key="receipt-first")
    assert client.store.get(job.id) == before


@pytest.mark.parametrize("failure", ["workspace", "pool", "hard-limits"])
def test_invalid_new_attempt_is_not_accepted(client, tmp_path, failure):
    options = {"cwd": str(tmp_path)}
    expected = ValueError
    if failure == "workspace":
        options["cwd"] = str(tmp_path / "missing")
    elif failure == "pool":
        options["pool"] = "missing"
        expected = KeyError
    else:
        options.update(cpus=1, require_hard_limits=True)
        expected = UnsupportedBackend
    with pytest.raises(expected):
        client.submit(JobSpec(["/bin/true"], **options), key="invalid-new")
    assert client.lookup("invalid-new") is None
    assert client.list() == []
    with client.store.transaction() as connection:
        assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0


def test_concurrent_identical_submissions_prepare_only_once(client, tmp_path, monkeypatch):
    barrier = threading.Barrier(4)
    lock = threading.Lock()
    validations = 0
    original = Path.is_dir

    def is_dir(path):
        nonlocal validations
        if path == tmp_path:
            with lock:
                validations += 1
        return original(path)

    monkeypatch.setattr(Path, "is_dir", is_dir)
    spec = JobSpec(["/bin/true"], str(tmp_path), env_overrides={"EXPLICIT": "same"})

    def submit():
        barrier.wait(timeout=10)
        return client.submit(spec, key="concurrent-prepare").id

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as threads:
        identifiers = list(threads.map(lambda _: submit(), range(4)))
    assert len(set(identifiers)) == 1
    assert validations == 1
    assert len([event for event in client.store.events(identifiers[0]) if event["event"] == "submitted"]) == 1


def test_cli_override_retry_ignores_ambient_changes_and_keeps_last_override(tmp_path):
    root = tmp_path / "state"
    counter = tmp_path / "executions"
    command = [
        sys.executable,
        "-m",
        "engagequeue",
        "--state-dir",
        str(root),
        "submit",
        "--key",
        "cli-overrides",
        "--cwd",
        str(tmp_path),
        "--env",
        "EXPLICIT=first",
        "--env",
        "EXPLICIT=last",
        "--",
        sys.executable,
        "-c",
        "import json, os; from pathlib import Path; "
        f"p=Path({str(counter)!r}); p.write_text(p.read_text()+'x' if p.exists() else 'x'); "
        "print(json.dumps({name: os.environ[name] for name in ('EXPLICIT', 'AMBIENT_SETTING')}))",
    ]
    environment = dict(os.environ, AMBIENT_SETTING="accepted")
    with Executor(root) as executor:
        try:
            first = subprocess.run(command, env=environment, capture_output=True, text=True, check=True, timeout=20)
            identifier = first.stdout.strip()
            assert executor.wait(identifier, timeout=15).state == JobState.SUCCEEDED
            environment["AMBIENT_SETTING"] = "changed"
            retry = subprocess.run(command, env=environment, capture_output=True, text=True, check=True, timeout=20)
            assert retry.stdout.strip() == identifier
            output = Path(executor.store.get(identifier)["spec"]["stdout"])
            assert json.loads(output.read_text()) == {"EXPLICIT": "last", "AMBIENT_SETTING": "accepted"}
            assert counter.read_text() == "x"
        finally:
            executor.shutdown(wait=True, timeout=15)

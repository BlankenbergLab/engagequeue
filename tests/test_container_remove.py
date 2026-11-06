"""Opt-in cleanup never infers ownership or removes an active container."""

import json
import os
import subprocess
import uuid
from types import SimpleNamespace

import pytest
from runtime_prerequisites import require_docker_image
from test_container_stop import docker_info

from engagequeue import ContainerSpec, Executor, JobSpec, JobState, PoolConfig
from engagequeue.backends.containers import ContainerOwnershipError, ContainerUnavailable, OwnedContainer
from engagequeue.backends.native import _finalize_containers, _stop_container_requests
from engagequeue.store import Store


def owned(**overrides):
    config = ContainerSpec("owned", "attempt", remove_on_exit=True).to_dict()
    config.update(overrides)
    container = OwnedContainer(config)
    container.pinned_id = "a" * 64
    return container


def data(**state):
    return {
        "Id": "a" * 64,
        "Name": "/owned",
        "Config": {"Labels": {"engagequeue.attempt": "attempt"}},
        "State": {
            "Running": False,
            "Restarting": False,
            "Status": "exited",
            "ExitCode": 137,
            "OOMKilled": False,
            **state,
        },
    }


def test_removal_uses_only_verified_pinned_id_without_force(monkeypatch):
    container = owned()
    existing = data()
    calls = []

    def execute(*args):
        nonlocal existing
        calls.append(args)
        if args[0] == "rm":
            existing = None
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if existing is None:
            return SimpleNamespace(returncode=1, stdout="[]", stderr="Error: No such object: " + args[1])
        return SimpleNamespace(returncode=0, stdout=json.dumps([existing]), stderr="")

    monkeypatch.setattr(container, "_execute", execute)
    container.remove()
    assert calls == [("inspect", "a" * 64), ("rm", "a" * 64), ("inspect", "a" * 64)]


@pytest.mark.parametrize(
    "problem",
    ["unrequested", "unpinned", "running", "restarting", "unknown_state", "foreign_label", "different_id", "runtime"],
)
def test_unsafe_or_unrequested_removal_never_calls_rm(monkeypatch, problem):
    container = owned()
    result = data()
    if problem == "unrequested":
        container.configuration.pop("remove_on_exit")
    elif problem == "unpinned":
        container.pinned_id = None
    elif problem == "running":
        result["State"]["Running"] = True
    elif problem == "restarting":
        result["State"]["Restarting"] = True
    elif problem == "unknown_state":
        result["State"].pop("Running")
    elif problem == "foreign_label":
        result["Config"]["Labels"]["engagequeue.attempt"] = "foreign"
    elif problem == "different_id":
        result["Id"] = "b" * 64
    calls = []

    def execute(*args):
        calls.append(args)
        if problem == "runtime":
            return SimpleNamespace(returncode=1, stdout="", stderr="dial tcp: lookup docker.invalid: no such host")
        return SimpleNamespace(returncode=0, stdout=json.dumps([result]), stderr="")

    monkeypatch.setattr(container, "_execute", execute)
    with pytest.raises(ContainerUnavailable if problem == "runtime" else ContainerOwnershipError):
        container.remove()
    assert all(args[0] == "inspect" for args in calls)


def test_container_restart_racing_rm_is_not_forced(monkeypatch):
    container = owned()
    result = data()
    calls = []

    def execute(*args):
        calls.append(args)
        if args[0] == "rm":
            result["State"]["Running"] = True
            result["State"]["Status"] = "running"
            return SimpleNamespace(returncode=1, stdout="", stderr="container is running")
        return SimpleNamespace(returncode=0, stdout=json.dumps([result]), stderr="")

    monkeypatch.setattr(container, "_execute", execute)
    with pytest.raises(ContainerUnavailable, match="not confirmed"):
        container.remove()
    assert [args for args in calls if args[0] == "rm"] == [("rm", "a" * 64)]
    assert not any("--force" in args or "-f" in args or args[0] == "kill" for args in calls)


@pytest.fixture
def stopped_job(tmp_path):
    store = Store(tmp_path / "state")
    spec = JobSpec(["/bin/true"], str(tmp_path), container=ContainerSpec("owned", "attempt", remove_on_exit=True))
    store.create(
        {
            "id": "job",
            "key": "frozen",
            "digest": "test",
            "state": "RUNNING",
            "pool": "default",
            "created_at": 1.0,
            "spec": spec.to_dict(),
            "cancel_grace": 0.1,
            "cancel_requested": False,
            "writers_stopped": False,
        }
    )
    return store


def test_final_state_and_receipt_are_durable_before_removal(stopped_job, monkeypatch):
    store = stopped_job
    container = owned()
    result = data(Running=True, Status="running")
    calls = []

    def execute(*args):
        nonlocal result
        calls.append(args)
        if args[0] == "kill":
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if args[0] == "rm":
            row = store.get("job")
            assert row["state"] == "RUNNING"  # Whole-job terminal publication follows cleanup.
            assert row["container_results"]["owned"]["exit_code"] == 137
            receipt = json.loads(open(store.container_stop_receipt_path("job", "owned")).read())
            assert receipt["status"] == "stopped" and receipt["exit_code"] == 137 and receipt["oom_killed"] is False
            result = None
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if result is None:
            return SimpleNamespace(returncode=1, stdout="[]", stderr="Error: No such object: " + args[1])
        return SimpleNamespace(returncode=0, stdout=json.dumps([result]), stderr="")

    monkeypatch.setattr(container, "_execute", execute)
    store.request_container_stop("job", "owned")
    _stop_container_requests(store, "job", store.get("job"), [container])
    result["State"].update(Running=False, Status="exited")
    _finalize_containers(store, "job", [container])
    assert store.get("job")["container_cleanup"]["owned"]["status"] == "removed"
    assert any(args[0] == "rm" for args in calls)


def test_unknown_other_container_retains_all_cleanup(stopped_job, monkeypatch):
    first = owned()
    second = owned(name="other")
    first.remove = lambda: pytest.fail("No removal before all ownership is known")
    monkeypatch.setattr(first, "inspect", lambda: data())
    monkeypatch.setattr(second, "inspect", lambda: (_ for _ in ()).throw(ContainerUnavailable("runtime unavailable")))
    with pytest.raises(ContainerUnavailable):
        _finalize_containers(stopped_job, "job", [first, second])
    assert "container_cleanup" not in stopped_job.get("job")


@pytest.mark.parametrize("backend", ["native", "systemd"])
@pytest.mark.parametrize("exit_code", [0, 7])
def test_real_ordinary_completion_cleans_opted_in_container_after_capturing_result(tmp_path, backend, exit_code):
    image = require_docker_image(backend)
    token = uuid.uuid4().hex
    name = "engagequeue-remove-" + token
    executor = Executor(tmp_path / "state", pools={"default": PoolConfig(1, backend, 0.5)})
    handle = executor.submit(
        JobSpec(
            [
                "docker",
                "run",
                "--name",
                name,
                "--label",
                "engagequeue.attempt=" + token,
                image,
                "sh",
                "-c",
                f"printf captured; exit {exit_code}",
            ],
            str(tmp_path),
            container=ContainerSpec(name, token, remove_on_exit=True),
        ),
        key="ordinary-remove",
    )
    try:
        result = executor.wait(handle.id, timeout=25)
        assert result.state == (JobState.SUCCEEDED if exit_code == 0 else JobState.FAILED)
        assert result.returncode == exit_code and result.writers_stopped
        assert docker_info(name) is None
        row = executor.inspect(handle.id)
        assert row["container_results"][name]["exit_code"] == exit_code
        assert row["container_results"][name]["oom_killed"] is False
        assert row["container_cleanup"][name]["status"] == "removed"
        assert not os.path.exists(executor.container_stop_receipt_path(handle.id, name))
    finally:
        if not executor.status(handle.id).terminal:
            executor.cancel(handle.id, wait=True, timeout=20)
        executor.shutdown(wait=True, timeout=15)
        executor.close()
        info = docker_info(name)
        if info and info.get("Config", {}).get("Labels", {}).get("engagequeue.attempt") == token:
            subprocess.run(["docker", "rm", "-f", info["Id"]], capture_output=True, timeout=10)

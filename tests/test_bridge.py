"""The managed Linux bridge preserves native admission and workspace policy."""

import fcntl
import json
import os
import signal
import stat
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import psutil
import pytest

from engagequeue import ContainerSpec, JobSpec, PoolConfig, __version__
from engagequeue.bridge import CONFIGURATION, ManagedLinuxExecutor, handle_request
from engagequeue.errors import RuntimeUnavailable
from engagequeue.executor import boot_identity, pid_namespace
from engagequeue.store import Store

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="The managed executor bridge runs inside Linux")


def _request(method, args=None, **fields):
    return {"protocol": 1, "method": method, "args": {} if args is None else args, **fields}


def _ok(request, root):
    reply = handle_request(request, root)
    assert reply["protocol"] == 1 and reply["ok"], reply
    return reply["result"]


@pytest.fixture
def managed(tmp_path, monkeypatch):
    root = (tmp_path / "state").resolve()
    workspace = (tmp_path / "workspace").resolve()
    workspace.mkdir()
    configuration = {
        "instance_id": str(uuid.uuid4()),
        "state_dir": str(root),
        "workspaces": [str(workspace)],
        "docker_jobs": False,
    }
    # Most tests exercise durable acceptance only, without starting a scheduler.
    monkeypatch.setattr(ManagedLinuxExecutor, "ensure_runtime", lambda *args, **kwargs: None)
    _ok(_request("initialize", {"config": configuration}), root)
    return root, workspace, configuration


def test_info_precedes_initialization_without_creating_state(tmp_path):
    root = (tmp_path / "state").resolve()
    result = _ok(_request("info"), root)
    assert result == {
        "protocol": 1,
        "version": __version__,
        "platform": "linux",
        "instance_id": None,
        "runtime_python": sys.executable,
        "uid": os.getuid(),
    }
    assert not root.exists()


@pytest.mark.parametrize(
    "message",
    [
        None,
        [],
        {},
        _request("info", protocol=True),
        _request("info", protocol=2),
        _request("_check"),
        _request("close"),
        _request("wait"),
        _request("info", []),
        _request("info", {"unexpected": True}),
        _request("info", attach_only="false"),
        _request("info", extra=True),
    ],
)
def test_protocol_rejects_unversioned_or_unlisted_operations(tmp_path, message):
    root = (tmp_path / "state").resolve()
    reply = handle_request(message, root)
    assert reply["protocol"] == 1 and reply["ok"] is False
    assert reply["error"]["type"] == "ValueError"
    assert not root.exists()


def test_initialize_custom_default_pool_is_private_and_does_not_start_scheduler(tmp_path):
    root = (tmp_path / "state").resolve()
    configuration = {
        "instance_id": str(uuid.uuid4()),
        "state_dir": str(root),
        "workspaces": [str(tmp_path.resolve())],
        "docker_jobs": False,
    }
    result = _ok(_request("initialize", {"config": configuration}, pools={"default": {"max_jobs": 3}}), root)
    assert result["instance_id"] == configuration["instance_id"]
    assert stat.S_IMODE((root / CONFIGURATION).stat().st_mode) == 0o600
    assert json.loads((root / CONFIGURATION).read_text()) == configuration
    store = Store(root)
    assert store.pool("default")["max_jobs"] == 3
    assert not store.get_meta("runtime")
    assert _ok(_request("runtime_state"), root) == {"running": False, "active": 0, "queued": 0}


@pytest.mark.parametrize("problem", ["instance", "state", "root-workspace", "relative-workspace", "boolean"])
def test_initialize_rejects_invalid_configuration(managed, problem):
    root, _, configuration = managed
    changed = dict(configuration)
    if problem == "instance":
        changed["instance_id"] = "invalid"
    elif problem == "state":
        changed["state_dir"] = "/somewhere/else"
    elif problem == "root-workspace":
        changed["workspaces"] = ["/"]
    elif problem == "relative-workspace":
        changed["workspaces"] = ["relative"]
    else:
        changed["docker_jobs"] = 1
    reply = handle_request(_request("initialize", {"config": changed}), root)
    assert reply["ok"] is False and reply["error"]["type"] == "ValueError"
    assert json.loads((root / CONFIGURATION).read_text()) == configuration


def test_initialize_cannot_adopt_another_instance(managed):
    root, _, configuration = managed
    changed = {**configuration, "instance_id": str(uuid.uuid4())}
    reply = handle_request(_request("initialize", {"config": changed}), root)
    assert reply["ok"] is False and reply["error"]["type"] == "RuntimeUnavailable"


def test_attach_only_cannot_initialize(managed):
    root, _, configuration = managed
    reply = handle_request(_request("initialize", {"config": configuration}, attach_only=True), root)
    assert reply["ok"] is False and reply["error"]["type"] == "RuntimeUnavailable"


def test_reconfiguration_retains_valid_queued_jobs_and_rejects_removed_mount(managed):
    root, workspace, configuration = managed
    spec = JobSpec(["/bin/true"], str(workspace))
    handle = _ok(_request("submit", {"spec": spec.to_dict(), "key": "queued"}), root)
    changed = {**configuration, "docker_jobs": True}
    _ok(_request("initialize", {"config": changed}), root)
    reply = handle_request(_request("initialize", {"config": {**changed, "workspaces": []}}), root)
    assert reply["ok"] is False and reply["error"]["type"] == "ValueError"
    assert json.loads((root / CONFIGURATION).read_text()) == changed
    assert Store(root).get(handle["id"])["state"] == "QUEUED"


def test_reconfiguration_is_blocked_by_active_or_uncertain_work(managed):
    root, workspace, configuration = managed
    handle = _ok(_request("submit", {"spec": JobSpec(["/bin/true"], str(workspace)).to_dict()}), root)
    store = Store(root)
    store.reserve_next("default")
    store.transition(handle["id"], "LOST")
    reply = handle_request(_request("initialize", {"config": {**configuration, "docker_jobs": True}}), root)
    assert reply["ok"] is False and reply["error"]["type"] == "RuntimeUnavailable"
    assert json.loads((root / CONFIGURATION).read_text()) == configuration


@pytest.mark.parametrize("field", ["cwd", "stdout", "stderr", "output_paths", "cid_file"])
def test_new_acceptance_rejects_paths_outside_mounts(managed, tmp_path, field):
    root, workspace, configuration = managed
    outside = (tmp_path / "outside").resolve()
    outside.mkdir()
    options = {"cwd": str(workspace)}
    if field == "cwd":
        options["cwd"] = str(outside)
    elif field == "output_paths":
        options[field] = [str(outside / "output")]
    elif field == "cid_file":
        _ok(_request("initialize", {"config": {**configuration, "docker_jobs": True}}), root)
        options["container"] = ContainerSpec("owned", "attempt", cid_file=str(outside / "cid"))
    else:
        options[field] = str(outside / "output")
    reply = handle_request(_request("submit", {"spec": JobSpec(["/bin/true"], **options).to_dict()}), root)
    assert reply["ok"] is False and reply["error"]["type"] == "ValueError"
    assert Store(root).list() == []


def test_workspace_symlinks_cannot_escape_mount_policy(managed, tmp_path):
    root, workspace, _ = managed
    outside = (tmp_path / "outside").resolve()
    outside.mkdir()
    (workspace / "escape").symlink_to(outside, target_is_directory=True)
    spec = JobSpec(["/bin/true"], str(workspace), stdout=str(workspace / "escape" / "output"))
    reply = handle_request(_request("submit", {"spec": spec.to_dict()}), root)
    assert reply["ok"] is False and reply["error"]["type"] == "ValueError"
    assert Store(root).list() == []


def test_workspace_ancestor_does_not_expose_private_state(managed):
    root, workspace, configuration = managed
    changed = {**configuration, "workspaces": [str(workspace.parent)]}
    _ok(_request("initialize", {"config": changed}), root)
    spec = JobSpec(["/bin/true"], str(workspace), stdout=str(root / "state.sqlite3"))
    reply = handle_request(_request("submit", {"spec": spec.to_dict()}), root)
    assert reply["ok"] is False and reply["error"]["type"] == "ValueError"
    jobs = root / "jobs" / "caller-workspace"
    jobs.mkdir(parents=True)
    _ok(_request("submit", {"spec": JobSpec(["/bin/true"], str(jobs)).to_dict()}), root)


def test_owned_containers_require_explicit_opt_in(managed):
    root, workspace, _ = managed
    spec = JobSpec(["/bin/true"], str(workspace), container=ContainerSpec("owned", "attempt"))
    reply = handle_request(_request("submit", {"spec": spec.to_dict()}), root)
    assert reply["ok"] is False and reply["error"]["type"] == "UnsupportedBackend"
    assert Store(root).list() == []


@pytest.mark.parametrize("method", ["initialize", "configure_pool"])
def test_managed_pool_configuration_rejects_systemd(managed, method):
    root, _, configuration = managed
    pool = PoolConfig(backend="systemd").to_dict()
    request = (
        _request("initialize", {"config": configuration}, pools={"systemd": pool})
        if method == "initialize"
        else _request("configure_pool", {"name": "systemd", "config": pool})
    )
    reply = handle_request(request, root)
    assert reply["ok"] is False and reply["error"]["type"] == "UnsupportedBackend"
    assert "systemd" not in Store(root).pools()


def test_pruned_receipt_adoption_precedes_changed_path_policy(managed):
    root, workspace, configuration = managed
    spec = JobSpec(["/bin/true"], str(workspace), env={"SECRET": "never-export"})
    request = _request("submit", {"spec": spec.to_dict(), "key": "durable-key"})
    handle = _ok(request, root)
    Store(root).request_cancel(handle["id"])
    assert _ok(_request("prune", {"job_id": handle["id"]}), root) == 1
    workspace.rmdir()
    _ok(_request("initialize", {"config": {**configuration, "workspaces": []}}), root)
    adopted = _ok({**request, "attach_only": True}, root)
    assert adopted["id"] == handle["id"] and adopted["state"] == "CANCELLED"
    assert "never-export" not in json.dumps(_ok(_request("inspect", {"job_id": handle["id"]}), root))
    changed = JobSpec(["/bin/false"], str(workspace))
    conflict = handle_request(_request("submit", {"spec": changed.to_dict(), "key": "durable-key"}), root)
    assert conflict["ok"] is False and conflict["error"]["type"] == "SubmissionConflict"


def test_bridge_preserves_environment_snapshot_and_inspection_redaction(managed, monkeypatch):
    root, workspace, _ = managed
    monkeypatch.setenv("BRIDGE_CAPTURE", "first-secret")
    spec = JobSpec(["/bin/true"], str(workspace), env_overrides={"EXPLICIT": "chosen"})
    request = _request("submit", {"spec": spec.to_dict(), "key": "environment"})
    handle = _ok(request, root)
    monkeypatch.setenv("BRIDGE_CAPTURE", "second-secret")
    assert _ok(request, root)["id"] == handle["id"]
    accepted = Store(root).get(handle["id"])["spec"]["env"]
    assert accepted["BRIDGE_CAPTURE"] == "first-secret" and accepted["EXPLICIT"] == "chosen"
    inspected = _ok(_request("inspect", {"job_id": handle["id"]}), root)
    assert inspected["spec"]["env"]["BRIDGE_CAPTURE"] == "<redacted>"
    assert "first-secret" not in json.dumps(inspected)


@pytest.mark.parametrize("method", ["cancel", "shutdown"])
def test_bridge_disallows_waiting_operations(managed, method):
    root, _, _ = managed
    reply = handle_request(_request(method, {"wait": True}), root)
    assert reply["ok"] is False and reply["error"]["type"] == "ValueError"


@pytest.mark.parametrize(
    ("payload", "success"), [("not JSON", False), ('{"protocol":NaN}', False), (json.dumps(_request("info")), True)]
)
def test_stdin_stdout_protocol_emits_one_json_response(tmp_path, payload, success):
    root = (tmp_path / "state").resolve()
    result = subprocess.run(
        [sys.executable, "-m", "engagequeue.bridge", "--state-dir", str(root)],
        input=payload,
        text=True,
        capture_output=True,
        check=True,
        timeout=10,
    )
    response = json.loads(result.stdout)
    assert response["protocol"] == 1
    assert response["ok"] is success
    assert len(result.stdout.splitlines()) == 1
    assert not root.exists()


def test_default_container_keeper_creates_writable_home_and_exits_on_term(tmp_path):
    home = tmp_path / "home"
    process = subprocess.Popen(
        [sys.executable, "-m", "engagequeue.managed_runtime"],
        env={**os.environ, "HOME": str(home)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 10
        while not home.exists():
            assert process.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.02)
        assert home.stat().st_uid == os.getuid()
        assert stat.S_IMODE(home.stat().st_mode) == 0o700
        while True:
            status = dict(line.split(":", 1) for line in Path(f"/proc/{process.pid}/status").read_text().splitlines())
            if int(status["SigCgt"].strip(), 16) & (1 << (signal.SIGTERM - 1)):
                break
            assert time.monotonic() < deadline
            time.sleep(0.02)
        process.terminate()
        assert process.wait(timeout=5) == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.communicate(timeout=5)


def _foreign_runtime(root):
    identity = {
        "boot_id": boot_identity(),
        "pid_namespace": pid_namespace() + 1,
        "pid": os.getpid(),
        "birth": psutil.Process().create_time(),
    }
    Store(root).put_meta("runtime", identity)
    return identity


def test_managed_namespace_restart_requires_free_lock_and_ignores_reused_pid(managed, monkeypatch):
    root, _, configuration = managed
    identity = _foreign_runtime(root)
    (root / "runtime.lock").touch(mode=0o600)

    def reject_stale_pid_check(self):
        raise AssertionError("A foreign namespace PID must not be treated as current ownership")

    monkeypatch.setattr("engagequeue.bridge.Executor._runtime_running", reject_stale_pid_check)
    with ManagedLinuxExecutor(root, managed_configuration=configuration) as executor:
        assert executor._runtime_running() is False
    assert Store(root).get_meta("runtime") == identity


def test_managed_namespace_restart_refuses_missing_runtime_lock(managed):
    root, _, configuration = managed
    identity = _foreign_runtime(root)
    with ManagedLinuxExecutor(root, managed_configuration=configuration) as executor:
        with pytest.raises(RuntimeUnavailable, match="Cannot verify.*runtime lock"):
            executor._runtime_running()
    assert not (root / "runtime.lock").exists()
    assert Store(root).get_meta("runtime") == identity


def test_managed_namespace_restart_refuses_held_runtime_lock(managed):
    root, _, configuration = managed
    identity = _foreign_runtime(root)
    with (root / "runtime.lock").open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        with ManagedLinuxExecutor(root, managed_configuration=configuration) as executor:
            with pytest.raises(RuntimeUnavailable, match="runtime lock is still held"):
                executor._runtime_running()
    assert Store(root).get_meta("runtime") == identity


def test_managed_namespace_restart_waits_for_current_runtime_identity(managed):
    root, _, configuration = managed
    identity = _foreign_runtime(root)
    changed = {**identity, "pid_namespace": pid_namespace()}
    publish = threading.Event()

    def publish_identity():
        assert publish.wait(5)
        time.sleep(0.1)
        Store(root).put_meta("runtime", changed)

    thread = threading.Thread(target=publish_identity)
    with (root / "runtime.lock").open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        thread.start()
        try:
            with ManagedLinuxExecutor(root, managed_configuration=configuration) as executor:
                publish.set()
                assert executor._runtime_running() is True
        finally:
            publish.set()
            thread.join(timeout=5)
    assert not thread.is_alive()
    assert Store(root).get_meta("runtime") == changed

"""Private, versioned JSON bridge to the executor inside a managed Linux container."""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import sys
import time
import uuid
from pathlib import Path

from .errors import RuntimeUnavailable, UnsupportedBackend
from .executor import Executor, boot_identity, pid_namespace
from .models import JobSpec, PoolConfig
from .store import Store

PROTOCOL = 1
CONFIGURATION = "managed-runtime.json"
ACTIVE = ("STARTING", "RUNNING", "STOPPING", "LOST")
METHODS = frozenset(
    (
        "info",
        "initialize",
        "submit",
        "lookup",
        "status",
        "list",
        "cancel",
        "stop_container",
        "container_stop_receipt_path",
        "inspect",
        "configure_pool",
        "drain",
        "resume",
        "shutdown",
        "prune",
        "reconcile",
        "ensure_runtime",
        "runtime_state",
    )
)


def _state_root(state_dir: str | Path) -> Path:
    directory = Path(state_dir)
    if not directory.is_absolute() or directory.resolve() == Path("/"):
        raise ValueError("The bridge requires a private absolute state directory")
    return directory.resolve()


def _configuration(value: dict, root: Path) -> dict:
    if not isinstance(value, dict) or set(value) != {"instance_id", "state_dir", "workspaces", "docker_jobs"}:
        raise ValueError("Invalid managed runtime configuration")
    if not isinstance(value["instance_id"], str):
        raise ValueError("Managed instance_id must be a UUID string")
    identifier = str(uuid.UUID(value["instance_id"]))
    if not isinstance(value["state_dir"], str) or _state_root(value["state_dir"]) != root:
        raise ValueError("Managed runtime state directory does not match the bridge")
    if not isinstance(value["workspaces"], list) or not isinstance(value["docker_jobs"], bool):
        raise ValueError("Managed workspaces must be a list and docker_jobs must be a boolean")
    workspaces = []
    for item in value["workspaces"]:
        if not isinstance(item, str) or not Path(item).is_absolute():
            raise ValueError("Managed workspace roots must be absolute paths")
        workspace = Path(item).resolve()
        if str(workspace) != item or workspace == Path("/") or workspace.is_relative_to(root):
            raise ValueError("Managed workspace roots must be canonical paths outside the private state directory")
        if item in workspaces:
            raise ValueError("Managed workspace roots must be unique")
        workspaces.append(item)
    return {
        "instance_id": identifier,
        "state_dir": str(root),
        "workspaces": workspaces,
        "docker_jobs": value["docker_jobs"],
    }


def _read_configuration(root: Path, *, optional: bool = False) -> dict | None:
    try:
        value = json.loads((root / CONFIGURATION).read_text())
    except FileNotFoundError:
        if optional:
            return None
        raise RuntimeUnavailable("Initialize this managed runtime before using its executor") from None
    return _configuration(value, root)


@contextlib.contextmanager
def _configuration_lock(root: Path, *, initialize: bool = False):
    if initialize:
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.stat().st_uid != os.getuid():
        raise PermissionError("Managed runtime state must belong to its execution user")
    descriptor = os.open(root / "managed-config.lock", os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(descriptor, "a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX if initialize else fcntl.LOCK_SH)
        yield


def _write_configuration(root: Path, configuration: dict) -> None:
    temporary = root / ("managed-runtime-" + uuid.uuid4().hex + ".tmp")
    try:
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            json.dump(configuration, stream, sort_keys=True, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, root / CONFIGURATION)
        descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def _validate_specification(spec: JobSpec, configuration: dict) -> None:
    root = Path(configuration["state_dir"])
    jobs = root / "jobs"
    workspaces = [Path(value) for value in configuration["workspaces"]]
    containers = list(spec.containers)
    if spec.container is not None:
        containers.append(spec.container)
    if containers and not configuration["docker_jobs"]:
        raise UnsupportedBackend("Owned Docker containers require docker_jobs=True for this managed runtime")
    paths = [spec.cwd, spec.stdout, spec.stderr, *spec.output_paths, *(item.cid_file for item in containers)]
    for value in paths:
        if value is None:
            continue
        path = Path(value).resolve()
        if path.is_relative_to(jobs):
            continue
        if path.is_relative_to(root) or not any(path.is_relative_to(workspace) for workspace in workspaces):
            raise ValueError(f"Job path is outside the managed workspace mounts and jobs directory: {value}")


def _pool(configuration: dict) -> PoolConfig:
    if not isinstance(configuration, dict):
        raise TypeError("Pool configuration must be an object")
    result = PoolConfig(**configuration)
    if result.backend != "native":
        raise UnsupportedBackend("Managed Linux runtimes support only the native backend")
    return result


def _pools(value: dict | None) -> dict | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise TypeError("pools must be an object or null")
    return {name: _pool(configuration) for name, configuration in value.items()}


class ManagedLinuxExecutor(Executor):
    def __init__(self, state_dir, *, managed_configuration, **kwargs):
        self.managed_configuration = managed_configuration
        super().__init__(state_dir, **kwargs)

    def _runtime_running(self):
        deadline = time.monotonic() + 1.0
        while True:
            runtime = self.store.get_meta("runtime", {})
            if (
                runtime.get("boot_id") != boot_identity()
                or runtime.get("pid_namespace") in (None, pid_namespace())
                or not runtime.get("pid")
            ):
                return super()._runtime_running()
            # A stopped/restarted owned container receives a new PID namespace.
            # Old numeric PIDs may now belong to unrelated processes. Only the
            # shared coordinator lock can prove that its old runtime is gone.
            try:
                descriptor = os.open(self.store.root / "runtime.lock", os.O_RDWR)
            except OSError as exc:
                raise RuntimeUnavailable("Cannot verify the previous managed coordinator's runtime lock") from exc
            with os.fdopen(descriptor, "a") as lock:
                try:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    pass
                else:
                    return False
            if time.monotonic() >= deadline:
                raise RuntimeUnavailable("The previous managed coordinator's runtime lock is still held")
            # A newly elected runtime can hold the lock briefly before it has
            # committed this namespace's identity. Allow that handoff to finish.
            time.sleep(0.05)

    def _validate_new_submission(self, spec, pool):
        super()._validate_new_submission(spec, pool)
        if pool["backend"] != "native":
            raise UnsupportedBackend("Managed Linux runtimes support only the native backend")
        _validate_specification(spec, self.managed_configuration)


def _info(root: Path) -> dict:
    from . import __version__

    configuration = _read_configuration(root, optional=True)
    return {
        "version": __version__,
        "protocol": PROTOCOL,
        "platform": sys.platform,
        "instance_id": configuration["instance_id"] if configuration is not None else None,
        "runtime_python": sys.executable,
        "uid": os.getuid(),
    }


def _initialize(root: Path, request: dict) -> dict:
    if request.get("attach_only", False):
        raise RuntimeUnavailable("An attach-only client cannot initialize a managed runtime")
    args = request["args"]
    if set(args) != {"config"}:
        raise ValueError("initialize requires only a config object")
    configuration = _configuration(args["config"], root)
    previous = _read_configuration(root, optional=True)
    if previous is not None and previous["instance_id"] != configuration["instance_id"]:
        raise RuntimeUnavailable("The managed runtime belongs to a different instance")
    if previous != configuration:
        store = Store(root)
        if store.list(states=ACTIVE):
            raise RuntimeUnavailable("Managed runtime configuration cannot change while active or uncertain jobs exist")
        for record in store.list(states=("QUEUED",)):
            if record["backend"] != "native":
                raise UnsupportedBackend("Queued jobs require an unsupported managed runtime backend")
            _validate_specification(JobSpec.from_dict(record["spec"]), configuration)
    with ManagedLinuxExecutor(root, managed_configuration=configuration, pools=_pools(request.get("pools"))):
        if previous != configuration:
            _write_configuration(root, configuration)
    return _info(root)


def _result(value):
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, list):
        return [_result(item) for item in value]
    return value


def _dispatch(root: Path, request: dict):
    if not isinstance(request, dict) or set(request) - {"protocol", "method", "args", "attach_only", "pools"}:
        raise ValueError("Bridge requests must be protocol objects")
    if type(request.get("protocol")) is not int or request["protocol"] != PROTOCOL:
        raise ValueError("Unsupported bridge protocol")
    method, args = request.get("method"), request.get("args")
    if not isinstance(method, str) or method not in METHODS:
        raise ValueError("Unsupported bridge method")
    if not isinstance(args, dict) or not isinstance(request.get("attach_only", False), bool):
        raise ValueError("Bridge args must be an object and attach_only must be a boolean")
    if method == "info":
        if args:
            raise ValueError("info does not accept arguments")
        return _info(root)
    if sys.platform != "linux":
        raise UnsupportedBackend("The managed executor bridge requires Linux")
    with _configuration_lock(root, initialize=method == "initialize"):
        if method == "initialize":
            return _initialize(root, request)
        configuration = _read_configuration(root)
        with ManagedLinuxExecutor(
            root,
            managed_configuration=configuration,
            pools=_pools(request.get("pools")),
            attach_only=request.get("attach_only", False),
        ) as executor:
            if method == "submit":
                if set(args) - {"spec", "key"} or "spec" not in args or not isinstance(args["spec"], dict):
                    raise ValueError("submit requires a spec object and optional key")
                return _result(executor.submit(JobSpec.from_dict(args["spec"]), key=args.get("key")))
            if method == "configure_pool":
                if set(args) != {"name", "config"}:
                    raise ValueError("configure_pool requires name and config")
                return executor.configure_pool(args["name"], _pool(args["config"]))
            if method == "runtime_state":
                if args:
                    raise ValueError("runtime_state does not accept arguments")
                return {
                    "running": executor._runtime_running(),
                    "active": len(executor.store.list(states=ACTIVE)),
                    "queued": len(executor.store.list(states=("QUEUED",))),
                }
            if method in ("cancel", "shutdown") and args.get("wait", False):
                raise ValueError("Managed clients must poll instead of blocking the bridge")
            return _result(getattr(executor, method)(**args))


def handle_request(request: dict, state_dir: str | Path) -> dict:
    try:
        result = _dispatch(_state_root(state_dir), request)
        return {"protocol": PROTOCOL, "ok": True, "result": result}
    except Exception as exc:
        return {"protocol": PROTOCOL, "ok": False, "error": {"type": type(exc).__name__, "message": str(exc)}}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    args = parser.parse_args(argv)
    os.umask(0o077)

    def invalid_constant(value):
        raise ValueError(f"Non-finite JSON value is unsupported: {value}")

    try:
        request = json.loads(sys.stdin.read(), parse_constant=invalid_constant)
        response = handle_request(request, args.state_dir)
    except Exception as exc:
        response = {"protocol": PROTOCOL, "ok": False, "error": {"type": type(exc).__name__, "message": str(exc)}}
    print(json.dumps(response, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

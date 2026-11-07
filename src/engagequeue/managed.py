"""Mac/Linux clients for an explicitly owned Docker execution host.

Only the bridge inside Linux opens the queue database. Host clients keep a
private resource manifest and share the jobs directory for outputs/receipts.
"""

from __future__ import annotations

import contextlib
import csv
import email.parser
import fcntl
import hashlib
import io
import json
import os
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
import zipfile
from pathlib import Path

from . import errors
from .models import JobHandle, JobSpec, JobState, JobStatus, PoolConfig, positive_number

PROTOCOL = 1
PYTHON = "/opt/engagequeue/.conda/engagequeue/bin/python"
LABEL = "org.engagequeue.instance"
MARKER = "managed.json"
_EXCEPTIONS = {
    cls.__name__: cls
    for cls in (
        errors.AdmissionClosed,
        errors.SubmissionConflict,
        errors.RuntimeUnavailable,
        errors.UnsupportedBackend,
        errors.LostJobError,
        ValueError,
        KeyError,
        TimeoutError,
        PermissionError,
        FileNotFoundError,
        RuntimeError,
    )
}


def _version():
    from . import __version__

    return __version__


def _pool(config):
    if not isinstance(config, PoolConfig):
        return dict(config)
    result = config.to_dict()
    if getattr(config, "_automatic_capacity", False):
        result["max_jobs"] = "*"
    return result


def _write_json(path: Path, value):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=".managed-", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(value, stream, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if temporary:
            temporary.unlink(missing_ok=True)


def _mount(kind, source, destination):
    stream = io.StringIO()
    csv.writer(stream, lineterminator="").writerow(["type=" + kind, "src=" + str(source), "dst=" + str(destination)])
    return stream.getvalue()


class DockerExecutor:
    """Control Linux execution through Docker, preserving the local job API.

    Call initialize(workspaces=[...]) explicitly once. Image builds and other
    Docker operations never occur merely from construction or close().
    """

    def __init__(self, state_dir, pools=None, poll_interval=0.2, attach_only=False):
        if not isinstance(attach_only, bool):
            raise ValueError("attach_only must be a boolean")
        self.root = Path(state_dir).expanduser().resolve()
        self.state_dir = str(self.root)
        self.poll_interval = positive_number(poll_interval, "poll_interval")
        self.attach_only = attach_only
        self._pools = {name: _pool(config) for name, config in pools.items()} if pools is not None else None
        self._closed = False

    def _check(self):
        if self._closed:
            raise RuntimeError("Executor handle is closed")

    def _directory(self):
        if self.root == Path("/"):
            raise ValueError("The filesystem root cannot be managed execution state")
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.root.stat().st_uid != os.getuid():
            raise PermissionError("Managed state must be owned by the current user")
        self.root.chmod(0o700)
        if (self.root / "state.sqlite3").exists():
            raise ValueError("Existing local state cannot be converted into managed Docker state")

    @contextlib.contextmanager
    def _locked(self):
        self._check()
        self._directory()
        descriptor = os.open(self.root / "managed.lock", os.O_CREAT | os.O_RDWR, 0o600)
        with os.fdopen(descriptor, "a") as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            yield

    def _load(self):
        self._check()
        path = self.root / MARKER
        try:
            if path.is_symlink() or self.root.stat().st_uid != os.getuid() or path.stat().st_uid != os.getuid():
                raise PermissionError("Managed configuration must be privately owned and cannot be a symlink")
            profile = json.loads(path.read_text())
        except FileNotFoundError as exc:
            raise errors.RuntimeUnavailable("Initialize managed Docker state with workspaces before using it") from exc
        if (
            profile.get("schema") != 1
            or profile.get("state_dir") != self.state_dir
            or profile.get("owner_uid") != os.getuid()
        ):
            raise errors.RuntimeUnavailable("Managed state format, owner, or absolute location differs")
        try:
            uuid.UUID(profile["instance_id"])
        except (KeyError, ValueError, TypeError) as exc:
            raise errors.RuntimeUnavailable("Invalid managed instance identity") from exc
        return profile

    def _save(self, profile):
        _write_json(self.root / MARKER, profile)

    def _docker(self, profile, *args, timeout=60, input=None):
        command = ["docker"]
        if profile.get("docker_context"):
            command.extend(["--context", profile["docker_context"]])
        try:
            result = subprocess.run([*command, *args], input=input, text=True, capture_output=True, timeout=timeout)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise errors.RuntimeUnavailable("Docker is unavailable or its response timed out") from exc
        if result.returncode:
            raise errors.RuntimeUnavailable("Docker command failed: " + result.stderr.strip()[:1500])
        return result.stdout

    def _daemon(self, profile, *, initializing=False):
        context = json.loads(self._docker(profile, "context", "inspect"))[0]
        endpoint = context.get("Endpoints", {}).get("docker", {}).get("Host", "")
        if not endpoint.startswith("unix://"):
            raise errors.UnsupportedBackend("Managed execution requires a local Unix-socket Docker context")
        info = json.loads(self._docker(profile, "info", "--format", "{{json .}}"))
        architecture = {"x86_64": "amd64", "aarch64": "arm64"}.get(info.get("Architecture"), info.get("Architecture"))
        if info.get("OSType") != "linux" or architecture not in ("amd64", "arm64") or not info.get("ID"):
            raise errors.UnsupportedBackend("A Linux amd64 or arm64 Docker daemon is required")
        observed = {"daemon_id": info["ID"], "endpoint": endpoint, "architecture": architecture}
        if not initializing and any(profile.get(key) != value for key, value in observed.items()):
            raise errors.RuntimeUnavailable("Docker daemon or architecture differs from this managed instance")
        return observed

    def _inspect(self, profile, kind, name):
        # Enumerating exact identities distinguishes absence from daemon or
        # authorization failures without parsing localized Docker error text.
        if kind == "container":
            ids = self._docker(profile, "container", "ls", "--all", "--no-trunc", "--format", "{{.ID}}")
            if name not in ids.splitlines():
                return None
        else:
            names = self._docker(profile, "volume", "ls", "--format", "{{.Name}}")
            if name not in names.splitlines():
                return None
        return json.loads(self._docker(profile, kind, "inspect", name))[0]

    def _volume(self, profile):
        data = self._inspect(profile, "volume", profile["volume"])
        if data is None or (data.get("Labels") or {}).get(LABEL) != profile["instance_id"]:
            raise errors.RuntimeUnavailable("Managed state volume is missing or its ownership differs")
        return data

    def _container(self, profile):
        identifier = profile.get("container_id")
        data = self._inspect(profile, "container", identifier) if identifier else None
        if data is None:
            raise errors.RuntimeUnavailable("Managed container is missing; initialize explicitly to recover it")
        labels = data.get("Config", {}).get("Labels") or {}
        mounts = {item["Destination"]: item for item in data.get("Mounts", [])}
        state = mounts.get(self.state_dir, {})
        jobs = mounts.get(str(self.root / "jobs"), {})
        expected_user = f"{profile['owner_uid']}:{profile['owner_gid']}"
        if (
            data.get("Id") != identifier
            or labels.get(LABEL) != profile["instance_id"]
            or data.get("Image") != profile["image_id"]
            or data.get("Config", {}).get("User") != expected_user
            or data.get("HostConfig", {}).get("Privileged")
            or data.get("HostConfig", {}).get("PidMode")
            or state.get("Type") != "volume"
            or state.get("Name") != profile["volume"]
            or not state.get("RW")
            or jobs.get("Type") != "bind"
            or jobs.get("Source") != str(self.root / "jobs")
            or not jobs.get("RW")
        ):
            raise errors.RuntimeUnavailable("Managed container ownership, image, user, or mounts differ")
        for path in profile["workspaces"]:
            mount = mounts.get(path, {})
            if mount.get("Type") != "bind" or mount.get("Source") != path or not mount.get("RW"):
                raise errors.RuntimeUnavailable("Managed workspace mounts differ")
        socket = mounts.get("/var/run/docker.sock")
        if bool(socket) != profile["docker_jobs"] or (
            socket and (socket.get("Type") != "bind" or socket.get("Source") != "/var/run/docker.sock")
        ):
            raise errors.RuntimeUnavailable("Managed Docker-job access differs")
        allowed = {self.state_dir, str(self.root / "jobs"), *profile["workspaces"]}
        if profile["docker_jobs"]:
            allowed.add("/var/run/docker.sock")
        if set(mounts) != allowed:
            raise errors.RuntimeUnavailable("Managed container has unexpected mounts")
        return data

    def _raw_rpc(self, profile, method, args=None, *, attach_only=None):
        request = {
            "protocol": PROTOCOL,
            "method": method,
            "args": args or {},
            "attach_only": self.attach_only if attach_only is None else attach_only,
            "pools": self._pools,
        }
        output = self._docker(
            profile,
            "exec",
            "-i",
            profile["container_id"],
            PYTHON,
            "-m",
            "engagequeue.bridge",
            "--state-dir",
            self.state_dir,
            input=json.dumps(request, allow_nan=False),
        )
        try:
            response = json.loads(output)
            if (
                type(response["protocol"]) is not int
                or response["protocol"] != PROTOCOL
                or not isinstance(response["ok"], bool)
            ):
                raise ValueError()
            if response["ok"]:
                result = response["result"]
            else:
                failure = response["error"]
                if not isinstance(failure["type"], str) or not isinstance(failure["message"], str):
                    raise ValueError()
        except (ValueError, TypeError, KeyError) as exc:
            raise errors.RuntimeUnavailable("Invalid managed bridge response") from exc
        if not response["ok"]:
            exception = _EXCEPTIONS.get(failure["type"], errors.RuntimeUnavailable)
            raise exception(failure["message"])
        if method not in ("info", "runtime_state"):
            self._pools = None
        return result

    def _connect(self, profile, *, start=True):
        self._daemon(profile)
        self._volume(profile)
        data = self._container(profile)
        if not data.get("State", {}).get("Running"):
            if self.attach_only or not start:
                raise errors.RuntimeUnavailable("Managed container is stopped")
            self._docker(profile, "start", profile["container_id"])
        if profile.get("stopped_cleanly"):
            profile["stopped_cleanly"] = False
            self._save(profile)
        info = self._raw_rpc(profile, "info")
        if (
            not isinstance(info, dict)
            or info.get("version") != _version()
            or info.get("protocol") != PROTOCOL
            or info.get("platform") != "linux"
            or info.get("instance_id") != profile["instance_id"]
            or info.get("uid") != os.getuid()
        ):
            raise errors.RuntimeUnavailable("Managed runtime version, protocol, or identity differs")

    def _rpc(self, method, args=None, *, start=True):
        self._check()
        with self._locked():
            profile = self._load()
            self._connect(profile, start=start)
            return self._raw_rpc(profile, method, args)

    def _wheel(self, supplied):
        cache = self.root / ".cache" / "runtime"
        cache.mkdir(mode=0o700, parents=True, exist_ok=True)
        if supplied is None:
            try:
                with urllib.request.urlopen(
                    f"https://pypi.org/pypi/engagequeue/{_version()}/json", timeout=30
                ) as response:
                    files = json.load(response)["urls"]
                artifact = next(item for item in files if item["filename"].endswith("-py3-none-any.whl"))
                path = cache / artifact["filename"]
                if not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest() != artifact["digests"]["sha256"]:
                    with urllib.request.urlopen(artifact["url"], timeout=60) as response:
                        content = response.read()
                    if hashlib.sha256(content).hexdigest() != artifact["digests"]["sha256"]:
                        raise ValueError("Downloaded runtime wheel hash differs")
                    path.write_bytes(content)
            except (OSError, ValueError, KeyError, StopIteration) as exc:
                raise errors.RuntimeUnavailable(
                    "Cannot obtain this version's runtime wheel; build a wheel and pass wheel= or --runtime-wheel"
                ) from exc
        else:
            path = Path(supplied).expanduser().resolve()
        try:
            with zipfile.ZipFile(path) as archive:
                names = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
                if len(names) != 1 or "engagequeue/bridge.py" not in archive.namelist():
                    raise ValueError("Runtime wheel must contain the managed bridge")
                metadata = email.parser.BytesParser().parsebytes(archive.read(names[0]))
                if metadata["Name"] != "engagequeue" or metadata["Version"] != _version():
                    raise ValueError("Runtime wheel package/version differs from this client")
        except (OSError, zipfile.BadZipFile) as exc:
            raise ValueError("Cannot read runtime wheel") from exc
        return path, hashlib.sha256(path.read_bytes()).hexdigest()

    def _image(self, profile, image, wheel):
        if image:
            # Explicit image selection can pull once at initialization, never
            # silently change a running instance during a status request.
            try:
                data = json.loads(self._docker(profile, "image", "inspect", image))[0]
            except errors.RuntimeUnavailable:
                self._docker(profile, "pull", image, timeout=1800)
                data = json.loads(self._docker(profile, "image", "inspect", image))[0]
            return data["Id"], None
        path, digest = self._wheel(wheel)
        recipe = Path(__file__).with_name("runtime_image") / "Dockerfile"
        fingerprint = hashlib.sha256(
            recipe.read_bytes() + digest.encode() + profile["architecture"].encode()
        ).hexdigest()
        tag = "engagequeue-runtime:" + fingerprint[:24]
        try:
            data = json.loads(self._docker(profile, "image", "inspect", tag))[0]
        except errors.RuntimeUnavailable:
            with tempfile.TemporaryDirectory(prefix="build-", dir=self.root / ".cache" / "runtime") as directory:
                context = Path(directory)
                (context / "wheels").mkdir()
                shutil.copy2(path, context / "wheels" / path.name)
                shutil.copy2(recipe, context / "Dockerfile")
                try:
                    output = self._docker(
                        profile,
                        "build",
                        "--platform",
                        "linux/" + profile["architecture"],
                        "-t",
                        tag,
                        str(context),
                        timeout=1800,
                    )
                except errors.RuntimeUnavailable as exc:
                    raise errors.RuntimeUnavailable(f"Runtime image build failed: {exc}") from exc
                (self.root / "runtime-build.log").write_text(output)
            data = json.loads(self._docker(profile, "image", "inspect", tag))[0]
        return data["Id"], digest

    def initialize(self, *, workspaces=None, image=None, wheel=None, docker_context=None, docker_jobs=None):
        if self.attach_only:
            raise errors.RuntimeUnavailable("An attach-only client cannot initialize managed resources")
        if image is not None and wheel is not None:
            raise ValueError("Specify an image or a runtime wheel, not both")
        if docker_jobs is not None and not isinstance(docker_jobs, bool):
            raise ValueError("docker_jobs must be a boolean")
        with self._locked():
            exists = (self.root / MARKER).exists()
            previous = self._load() if exists else None
            profile = (
                dict(previous)
                if previous
                else {
                    "schema": 1,
                    "instance_id": str(uuid.uuid4()),
                    "owner_uid": os.getuid(),
                    "owner_gid": os.getgid(),
                    "state_dir": self.state_dir,
                    "container_id": None,
                }
            )
            if workspaces is None:
                workspaces = profile.get("workspaces")
            if isinstance(workspaces, (str, bytes)):
                raise ValueError("workspaces must be a sequence of directory paths")
            if not workspaces:
                raise ValueError("Provide at least one persistent workspace root")
            roots = sorted({str(Path(path).expanduser().resolve()) for path in workspaces})
            for raw in roots:
                path = Path(raw)
                if not path.is_dir() or path == Path("/") or path.is_relative_to(self.root):
                    raise ValueError(
                        "Workspace roots must be existing directories outside managed state and cannot be /"
                    )
            # Redundant nested mounts obscure validation and require no access
            # that their containing workspace has not already granted.
            roots = [
                raw for raw in roots if not any(raw != other and Path(raw).is_relative_to(other) for other in roots)
            ]
            context = docker_context or profile.get("docker_context")
            if context is None:
                if os.environ.get("DOCKER_HOST"):
                    raise ValueError("Select an explicit local Docker context when DOCKER_HOST is set")
                context = self._docker({}, "context", "show").strip()
            profile.update(docker_context=context)
            observed = self._daemon(profile, initializing=True)
            if previous and any(previous.get(key) != value for key, value in observed.items()):
                raise errors.RuntimeUnavailable("Refusing to move managed state to another Docker daemon")
            profile.update(observed)
            enabled = profile.get("docker_jobs", False) if docker_jobs is None else docker_jobs
            profile.update(workspaces=roots, docker_jobs=enabled)
            profile["volume"] = "engagequeue-" + uuid.UUID(profile["instance_id"]).hex
            jobs = self.root / "jobs"
            if jobs.is_symlink():
                raise ValueError("Managed jobs directory cannot be a symlink")
            jobs.mkdir(mode=0o700, exist_ok=True)
            jobs.chmod(0o700)
            if not previous:
                # Reserve the mode before an image build. A failed build stays
                # explicitly recoverable and cannot become a native database.
                descriptor = os.open(self.root / "initialization.lock", os.O_CREAT | os.O_RDWR, 0o600)
                with os.fdopen(descriptor, "a") as election:
                    fcntl.flock(election.fileno(), fcntl.LOCK_EX)
                    if (self.root / "state.sqlite3").exists():
                        raise ValueError("Existing local state cannot be converted into managed Docker state")
                    self._save(profile)
            if image is not None or wheel is not None or not profile.get("image_id"):
                profile["image_id"], profile["wheel_sha256"] = self._image(profile, image, wheel)
            changed = bool(previous) and any(
                profile.get(key) != previous.get(key) for key in ("workspaces", "docker_jobs", "image_id")
            )
            if changed and previous.get("container_id") and not previous.get("stopped_cleanly"):
                raise errors.RuntimeUnavailable("Stop the managed runtime cleanly before changing its image or mounts")
            old = None
            if previous and previous.get("container_id"):
                if self._inspect(previous, "container", previous["container_id"]) is not None:
                    old = self._container(previous)
                if changed and old and old.get("State", {}).get("Running"):
                    raise errors.RuntimeUnavailable("Managed container must be stopped before reconfiguration")
            if not previous or changed or not previous.get("image_id"):
                output = self._docker(
                    profile,
                    "run",
                    "--rm",
                    "-i",
                    "--user",
                    f"{profile['owner_uid']}:{profile['owner_gid']}",
                    "--label",
                    LABEL + "=" + profile["instance_id"],
                    "--entrypoint",
                    PYTHON,
                    profile["image_id"],
                    "-m",
                    "engagequeue.bridge",
                    "--state-dir",
                    "/tmp/engagequeue-image-check",
                    input=json.dumps({"protocol": PROTOCOL, "method": "info", "args": {}}),
                )
                try:
                    probe = json.loads(output)
                    info = probe["result"]
                    if (
                        type(probe["protocol"]) is not int
                        or probe["protocol"] != PROTOCOL
                        or probe["ok"] is not True
                        or info["version"] != _version()
                        or info["protocol"] != PROTOCOL
                        or info["platform"] != "linux"
                    ):
                        raise ValueError()
                except (ValueError, KeyError, TypeError) as exc:
                    raise errors.RuntimeUnavailable("Selected runtime image does not match this client") from exc
            volume = self._inspect(profile, "volume", profile["volume"])
            if volume is None:
                if previous and (previous.get("container_id") or previous.get("volume_created")):
                    raise errors.RuntimeUnavailable(
                        "Managed state volume disappeared; refusing to create empty replacement"
                    )
                self._docker(
                    profile, "volume", "create", "--label", LABEL + "=" + profile["instance_id"], profile["volume"]
                )
            self._volume(profile)
            profile["volume_created"] = True
            if changed and old:
                self._docker(profile, "rm", previous["container_id"])
                profile["container_id"] = None
            if previous and previous.get("container_id") and old is None:
                # Explicit recovery retains the volume; accepted attempts keep
                # their namespace evidence and are never automatically replayed.
                profile["container_id"] = None
            # Checkpoint removal before creating a replacement. A failed create
            # can be retried without an old ID paired with new image/mount data.
            self._save(profile)
            if not profile.get("container_id"):
                owner = f"{profile['owner_uid']}:{profile['owner_gid']}"
                self._docker(
                    profile,
                    "run",
                    "--rm",
                    "--user",
                    "0:0",
                    "--label",
                    LABEL + "=" + profile["instance_id"],
                    "--mount",
                    f"type=volume,src={profile['volume']},dst=/state",
                    "--entrypoint",
                    PYTHON,
                    profile["image_id"],
                    "-c",
                    "import os,sys; os.chown('/state',int(sys.argv[1]),int(sys.argv[2])); os.chmod('/state',0o700)",
                    str(profile["owner_uid"]),
                    str(profile["owner_gid"]),
                )
                args = [
                    "create",
                    "--init",
                    "--restart",
                    "unless-stopped",
                    "--label",
                    LABEL + "=" + profile["instance_id"],
                    "--user",
                    owner,
                    "--env",
                    "HOME=/tmp/engagequeue",
                    "--mount",
                    _mount("volume", profile["volume"], self.state_dir),
                    "--mount",
                    _mount("bind", jobs, jobs),
                ]
                for path in roots:
                    args.extend(["--mount", _mount("bind", path, path)])
                if enabled:
                    args.extend(["--mount", "type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock"])
                    group = self._docker(
                        profile,
                        "run",
                        "--rm",
                        "--user",
                        "0:0",
                        "--label",
                        LABEL + "=" + profile["instance_id"],
                        "--mount",
                        "type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock",
                        "--entrypoint",
                        PYTHON,
                        profile["image_id"],
                        "-c",
                        "import os; print(os.stat('/var/run/docker.sock').st_gid)",
                    ).strip()
                    if not group.isdigit():
                        raise errors.RuntimeUnavailable("Cannot determine Docker socket group")
                    args.extend(["--group-add", group])
                args.extend(["--entrypoint", PYTHON, profile["image_id"], "-m", "engagequeue.managed_runtime"])
                profile["container_id"] = self._docker(profile, *args).strip()
                self._save(profile)
            data = self._container(profile)
            if not data.get("State", {}).get("Running"):
                self._docker(profile, "start", profile["container_id"])
            info = self._raw_rpc(profile, "info")
            if (
                not isinstance(info, dict)
                or info.get("version") != _version()
                or info.get("protocol") != PROTOCOL
                or info.get("platform") != "linux"
            ):
                raise errors.RuntimeUnavailable("Selected image does not match this client version and bridge protocol")
            configuration = {key: profile[key] for key in ("instance_id", "state_dir", "workspaces", "docker_jobs")}
            self._raw_rpc(profile, "initialize", {"config": configuration}, attach_only=False)
            self._raw_rpc(profile, "resume", attach_only=False)
            profile["stopped_cleanly"] = False
            self._save(profile)
        return self.runtime_info()

    def ensure_runtime(self, timeout=15):
        self._rpc("ensure_runtime", {"timeout": timeout})

    def start(self):
        if self.attach_only:
            raise errors.RuntimeUnavailable("An attach-only client cannot start a managed runtime")
        self.resume()
        return self.runtime_info()

    def runtime_info(self):
        with self._locked():
            profile = self._load()
            self._daemon(profile)
            self._volume(profile)
            data = self._container(profile)
            running = bool(data.get("State", {}).get("Running"))
            result = {**profile, "container_running": running}
            result["runtime"] = self._raw_rpc(profile, "runtime_state") if running else {"running": False}
            return result

    def submit(self, spec, key=None):
        if not isinstance(spec, JobSpec):
            raise TypeError("submit requires a JobSpec")
        # Generate before transport so a caller can always retain/retry the
        # stable key even when the bridge accepted but its response was lost.
        key = str(uuid.uuid4()) if key is None else key
        result = self._rpc("submit", {"spec": spec.to_dict(), "key": key})
        return JobHandle(result["id"], result["key"], JobState(result["state"]))

    def lookup(self, key):
        result = self._rpc("lookup", {"key": key})
        return JobHandle(result["id"], result["key"], JobState(result["state"])) if result is not None else None

    @staticmethod
    def _status(result):
        return JobStatus.from_record(result)

    def status(self, job_id):
        return self._status(self._rpc("status", {"job_id": job_id}))

    def list(self, pool=None):
        return [self._status(result) for result in self._rpc("list", {"pool": pool})]

    def inspect(self, job_id):
        return self._rpc("inspect", {"job_id": job_id})

    def wait(self, job_id, timeout=None, *, raise_on_lost=True):
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            result = self.status(job_id)
            if result.terminal:
                return result
            if raise_on_lost and result.needs_reconciliation:
                detail = f": {result.reason}" if result.reason else ""
                raise errors.LostJobError(f"Job {job_id} has uncertain ownership; inspect and reconcile it{detail}")
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"Job {job_id} has not reached a confirmed terminal state")
            time.sleep(min(self.poll_interval, 0.25))

    def cancel(self, job_id, wait=False, timeout=None):
        result = self._status(self._rpc("cancel", {"job_id": job_id}))
        return self.wait(job_id, timeout) if wait else result

    def stop_container(self, job_id, name):
        return self._status(self._rpc("stop_container", {"job_id": job_id, "name": name}))

    def container_stop_receipt_path(self, job_id, name):
        return self._rpc("container_stop_receipt_path", {"job_id": job_id, "name": name}, start=False)

    def configure_pool(self, name, config):
        self._rpc("configure_pool", {"name": name, "config": _pool(config)})

    def drain(self):
        self._rpc("drain")

    def resume(self):
        if self.attach_only:
            raise errors.RuntimeUnavailable("An attach-only client cannot resume a managed runtime")
        self._rpc("resume")

    def shutdown(self, wait=False, timeout=None):
        self._rpc("shutdown")
        if wait:
            deadline = None if timeout is None else time.monotonic() + timeout
            while self._rpc("runtime_state")["running"]:
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError("Runtime is still waiting for active or uncertain jobs")
                time.sleep(self.poll_interval)

    def stop(self, timeout=120):
        if self.attach_only:
            raise errors.RuntimeUnavailable("An attach-only client cannot stop a managed runtime")
        with self._locked():
            profile = self._load()
            self._connect(profile, start=False)
            # Hold the host election lock through shutdown and Docker stop, so
            # another Mac client cannot resume admission between those steps.
            self._raw_rpc(profile, "shutdown")
            deadline = None if timeout is None else time.monotonic() + timeout
            while True:
                state = self._raw_rpc(profile, "runtime_state")
                if not state["running"] and not state["active"]:
                    break
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError("Active or uncertain jobs prevent stopping the managed container")
                time.sleep(self.poll_interval)
            self._docker(profile, "stop", "--time", "30", profile["container_id"])
            profile["stopped_cleanly"] = True
            self._save(profile)

    def reconcile(self, job_id, *, confirm_stopped=False, reason="Operator confirmed resources stopped"):
        return self._status(
            self._rpc("reconcile", {"job_id": job_id, "confirm_stopped": confirm_stopped, "reason": reason})
        )

    def prune(self, job_id=None, before=None):
        return self._rpc("prune", {"job_id": job_id, "before": before})

    def close(self):
        self._closed = True

    def __enter__(self):
        self._check()
        return self

    def __exit__(self, *args):
        self.close()

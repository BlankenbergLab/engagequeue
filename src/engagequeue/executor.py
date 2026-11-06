"""Importable control API shared by applications and the command line."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Mapping

import psutil

from .errors import LostJobError, RuntimeUnavailable, UnsupportedBackend
from .models import JobHandle, JobSpec, JobState, JobStatus, PoolConfig, positive_number
from .store import Store, dump


def boot_identity() -> str:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        return str(psutil.boot_time())


def pid_namespace() -> int:
    return Path("/proc/self/ns/pid").stat().st_ino


def process_alive(pid: int | None, birth: float | None) -> bool | None:
    if pid is None or birth is None:
        return False
    try:
        process = psutil.Process(pid)
        return abs(process.create_time() - birth) < 0.01 and process.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False
    except (psutil.AccessDenied, OSError):
        return None


def python_interpreter(explicit: str | None = None) -> str:
    # sys.executable can be uwsgi rather than Python in an embedded interpreter.
    candidate = Path(explicit) if explicit else Path(sys.prefix) / "bin" / "python"
    if not candidate.is_absolute():
        raise ValueError("runtime_python must be an absolute Python executable path")
    if not candidate.is_file() or not os.access(candidate, os.X_OK):
        raise RuntimeUnavailable(
            "Configure runtime_python with an executable Python environment containing EngageQueue"
        )
    return str(candidate)


class Executor:
    """Control a detached single-host executor without owning its lifetime.

    Constructing or importing an Executor starts no threads or processes. The
    runtime attaches lazily on submission or recovery. ``close()`` disconnects
    this client only; explicit runtime administration handles draining/stopping.
    """

    def __init__(
        self,
        state_dir: str | Path,
        pools: Mapping[str, PoolConfig | Mapping[str, Any]] | None = None,
        runtime_python: str | None = None,
        poll_interval: float = 0.2,
        attach_only: bool = False,
    ):
        if not isinstance(attach_only, bool):
            raise ValueError("attach_only must be a boolean")
        self.attach_only = attach_only
        if attach_only and not (Path(state_dir).expanduser().resolve() / "state.sqlite3").is_file():
            raise RuntimeUnavailable("External execution state is unavailable; start its standalone runtime first")
        self.store = Store(state_dir)
        self.state_dir = str(self.store.root)
        self.poll_interval = positive_number(poll_interval, "poll_interval")
        existing_python = self.store.get_meta("runtime_python")
        if attach_only:
            # The execution host's interpreter need not exist inside an
            # embedding container; that host retains execution ownership.
            self.runtime_python = existing_python
        else:
            self.runtime_python = python_interpreter(runtime_python or existing_python)
            if (
                runtime_python is not None
                and existing_python is not None
                and (os.path.abspath(existing_python) != os.path.abspath(self.runtime_python))
            ):
                raise ValueError(
                    "Shared execution state uses a different runtime_python; drain it before changing environments"
                )
            self.store.put_meta("runtime_python", self.runtime_python)
            if existing_python is None:
                package_root = Path(__file__).resolve().parent.parent
                source_root = (
                    str(package_root)
                    if not {"site-packages", "dist-packages"}.intersection(package_root.parts)
                    else None
                )
                self.store.put_meta("runtime_source", source_root)
        if self.store.get_meta("poll_interval") is None:
            self.store.put_meta("poll_interval", self.poll_interval)
        if pools is not None:
            for name, config in pools.items():
                self.store.ensure_pool(name, config if isinstance(config, PoolConfig) else PoolConfig(**config))
        if not self.store.pools():
            self.store.ensure_pool("default", PoolConfig())
        self._closed = False
        self._owner_pid = os.getpid()

    def _check(self) -> None:
        if self._closed:
            raise RuntimeError("Executor handle is closed")
        # Store connections are operation-scoped; recording the new PID makes
        # the fork boundary explicit without inherited socket/database handles.
        if self._owner_pid != os.getpid():
            self._owner_pid = os.getpid()

    def _runtime_running(self) -> bool:
        runtime = self.store.get_meta("runtime", {})
        if runtime.get("boot_id") != boot_identity():
            return False
        if self.attach_only:
            heartbeat = max(self.store.get_meta("runtime_heartbeat", 0), runtime.get("started_at", 0))
            if time.time() - heartbeat > 10:
                return False
            try:
                descriptor = os.open(self.store.root / "runtime.lock", os.O_RDWR)
            except FileNotFoundError:
                return False
            with os.fdopen(descriptor, "a") as lock:
                try:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return True
                return False
        if runtime.get("pid_namespace") != pid_namespace():
            if runtime.get("pid"):
                raise RuntimeUnavailable(
                    "Execution state belongs to another PID namespace; use attach_only=True with its standalone runtime"
                )
            return False
        return process_alive(runtime.get("pid"), runtime.get("birth")) is True

    def ensure_runtime(self, timeout: float = 15) -> None:
        self._check()
        if self.attach_only:
            if not self._runtime_running():
                raise RuntimeUnavailable("External runtime is not ready; start engagequeue serve on the execution host")
            return
        if self.store.get_meta("shutdown_requested", False):
            return
        if self._runtime_running():
            return
        descriptor = os.open(self.store.root / "startup.lock", os.O_CREAT | os.O_RDWR, 0o600)
        with os.fdopen(descriptor, "a") as startup:
            fcntl.flock(startup.fileno(), fcntl.LOCK_EX)
            if self._runtime_running():
                return
            env = os.environ.copy()
            env.pop("PYTHONHOME", None)
            env.pop("PYTHONPATH", None)
            if source_root := self.store.get_meta("runtime_source"):
                env["PYTHONPATH"] = source_root
            descriptor = os.open(self.store.root / "runtime.log", os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
            with os.fdopen(descriptor, "ab") as output:
                process = subprocess.Popen(
                    [self.runtime_python, "-m", "engagequeue.runtime", "--state-dir", self.state_dir],
                    cwd=self.state_dir,
                    env=env,
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=output,
                    start_new_session=True,
                    close_fds=True,
                )
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if self._runtime_running():
                    return
                if process.poll() is not None:
                    raise RuntimeUnavailable(f"Runtime startup failed; inspect {self.store.root / 'runtime.log'}")
                time.sleep(0.05)
            raise RuntimeUnavailable(f"Runtime did not become ready; inspect {self.store.root / 'runtime.log'}")

    def submit(self, spec: JobSpec, key: str | None = None) -> JobHandle:
        self._check()
        if not isinstance(spec, JobSpec):
            raise TypeError("submit requires a JobSpec")
        if key is None:
            key = str(uuid.uuid4())
        if not isinstance(key, str) or not key or "\x00" in key:
            raise ValueError("Submission key must be a nonempty string without NUL")
        namespace = uuid.UUID(self.store.get_meta("runtime_id"))
        identifier = str(uuid.uuid5(namespace, key))
        directory = self.store.root / "jobs" / identifier
        accepted = spec.to_dict()
        accepted["stdout"] = accepted["stdout"] or str(directory / "stdout")
        accepted["stderr"] = accepted["stderr"] or str(directory / "stderr")
        digest = hashlib.sha256(dump(accepted).encode()).hexdigest()
        record = {
            "id": identifier,
            "key": key,
            "digest": digest,
            "spec": accepted,
            "pool": spec.pool,
            "created_at": time.time(),
            "state": JobState.QUEUED.value,
            "cancel_requested": False,
            "writers_stopped": False,
        }

        def prepare(connection):
            # Existing receipts must be adopted before checking current host
            # resources or policy. Store calls this only for a new key while
            # holding its acceptance transaction, including for racing clients.
            if self.attach_only:
                self.ensure_runtime()
            if not Path(spec.cwd).is_dir():
                raise ValueError("Job working directory must exist")
            row = connection.execute("SELECT config FROM pools WHERE name=?", (spec.pool,)).fetchone()
            if row is None:
                raise KeyError(f"Unknown execution pool {spec.pool!r}")
            pool = json.loads(row[0])
            if spec.require_hard_limits and pool["backend"] == "native" and (spec.cpus or spec.memory_mb):
                raise UnsupportedBackend(
                    "Native process supervision does not provide hard cgroup CPU/memory limits; select systemd"
                )
            execution = dict(accepted)
            overrides = execution.pop("env_overrides", {})
            # Capture inherited values only on first acceptance. The digest
            # above describes explicit intent, not changing caller ambience.
            execution["env"] = dict(os.environ) if execution["env"] is None else dict(execution["env"])
            execution["env"].update(overrides)
            return {**record, "spec": execution, "backend": pool["backend"], "cancel_grace": pool["cancel_grace"]}

        result, created = self.store.create(record, prepare=prepare)
        if created:
            directory.mkdir(parents=True, mode=0o700, exist_ok=True)
            directory.chmod(0o700)
        if result["state"] not in ("SUCCEEDED", "FAILED", "CANCELLED"):
            self.ensure_runtime()
        return JobHandle(result["id"], result["key"], JobState(result["state"]))

    def lookup(self, key: str) -> JobHandle | None:
        self._check()
        record = self.store.lookup(key)
        if record is None:
            return None
        return JobHandle(record["id"], record["key"], JobState(record["state"]))

    def status(self, job_id: str) -> JobStatus:
        self._check()
        record = self.store.get(job_id)
        if record["state"] not in ("SUCCEEDED", "FAILED", "CANCELLED"):
            self.ensure_runtime()
            record = self.store.get(job_id)
        return JobStatus.from_record(record)

    def wait(self, job_id: str, timeout: float | None = None, *, raise_on_lost: bool = True) -> JobStatus:
        self._check()
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            result = self.status(job_id)
            if result.terminal:
                return result
            if raise_on_lost and result.needs_reconciliation:
                raise LostJobError(f"Job {job_id} has uncertain ownership; inspect and reconcile it")
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"Job {job_id} has not reached a confirmed terminal state")
            time.sleep(min(self.poll_interval, 0.25))

    def cancel(self, job_id: str, wait: bool = False, timeout: float | None = None) -> JobStatus:
        self._check()
        record = self.store.request_cancel(job_id)
        if record["state"] not in ("SUCCEEDED", "FAILED", "CANCELLED"):
            self.ensure_runtime()
        return self.wait(job_id, timeout) if wait else JobStatus.from_record(self.store.get(job_id))

    def stop_container(self, job_id: str, name: str) -> JobStatus:
        """Request graceful stop of one declared container, leaving its shell alive.

        The durable request can precede container creation. The worker verifies
        its label and immutable ID, sends TERM, and enforces the pool's grace
        period before KILL. Normal execution, output collection and later
        metadata containers continue; this does not cancel the accepted job.
        """
        self._check()
        record = self.store.request_container_stop(job_id, name)
        if not self.attach_only and record["state"] not in ("SUCCEEDED", "FAILED", "CANCELLED"):
            self.ensure_runtime()
        return JobStatus.from_record(self.store.get(job_id))

    def container_stop_receipt_path(self, job_id: str, name: str) -> str:
        """Return a private receipt path without creating files or starting work.

        A receipt is written only after a verified signal RPC succeeds. It may
        appear shortly after the container's waiting shell resumes. Final exit
        and OOM fields are None when auto-removal prevents their observation.
        """
        self._check()
        return self.store.container_stop_receipt_path(job_id, name)

    def list(self, pool: str | None = None) -> list[JobStatus]:
        self._check()
        return [JobStatus.from_record(record) for record in self.store.list(pool=pool)]

    def inspect(self, job_id: str) -> dict[str, Any]:
        self._check()
        record = self.store.get(job_id)
        record["spec"]["env"] = {key: "<redacted>" for key in record["spec"].get("env", {})}
        record["events"] = self.store.events(job_id)
        return record

    def configure_pool(self, name: str, config: PoolConfig) -> None:
        self._check()
        self.store.ensure_pool(name, config, update=True)

    def drain(self) -> None:
        """Pause admission while existing workers finish; retain queued jobs."""
        self._check()
        self.store.put_meta("admission_open", False)

    def resume(self) -> None:
        self._check()
        self.store.put_meta("shutdown_requested", False)
        self.store.put_meta("admission_open", True)
        self.ensure_runtime()

    def shutdown(self, wait: bool = False, timeout: float | None = None) -> None:
        """Drain active work and stop the coordinator, leaving the queue saved."""
        self._check()
        self.drain()
        self.store.put_meta("shutdown_requested", True)
        if wait:
            deadline = None if timeout is None else time.monotonic() + timeout
            while self._runtime_running():
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError("Runtime is still waiting for active or uncertain jobs")
                time.sleep(0.1)

    def reconcile(
        self, job_id: str, *, confirm_stopped: bool = False, reason: str = "Operator confirmed resources stopped"
    ) -> JobStatus:
        self._check()
        record = self.store.get(job_id)
        if self.attach_only and record.get("admitted_pid_namespace") not in (None, pid_namespace()):
            raise RuntimeUnavailable("Ownership reconciliation must run in the original execution namespace")
        if record["state"] != "LOST":
            raise ValueError("Only uncertain LOST executions require reconciliation")
        if not confirm_stopped:
            raise ValueError("Explicit confirm_stopped=True is required after inspecting ownership")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("A nonempty operator reason is required")
        from .backends.native import resources_stopped

        if process_alive(record.get("worker_pid"), record.get("worker_birth")) is not False:
            raise LostJobError("The recorded supervisor is still present or cannot be verified")
        verified = resources_stopped(record)
        if verified is False:
            raise LostJobError("Known owned execution resources are still present")
        self.store.transition(
            job_id,
            "FAILED",
            writers_stopped=True,
            returncode=255,
            reason=reason,
            reconciled_by=os.getuid(),
            reconciled_at=time.time(),
            resource_verification="verified" if verified is True else "operator_attested",
        )
        return JobStatus.from_record(self.store.get(job_id))

    def prune(self, job_id: str | None = None, before: float | None = None) -> int:
        self._check()
        records = [self.store.get(job_id)] if job_id is not None else self.store.list()
        selected = [
            record
            for record in records
            if record["state"] in ("SUCCEEDED", "FAILED", "CANCELLED")
            and not record.get("pruned")
            and (before is None or (record.get("finished_at") or 0) < before)
        ]
        if job_id is not None and not selected:
            if records[0].get("pruned"):
                return 0
            raise ValueError("Only confirmed terminal jobs can be pruned")
        for record in selected:
            directory = self.store.root / "jobs" / record["id"]
            # Preserve any explicitly caller-owned workspace/watch paths even
            # if an application placed them beneath the executor's state tree.
            owned = [record["spec"].get("cwd"), *record["spec"].get("output_paths", [])]
            preserve = any(Path(path).resolve().is_relative_to(directory.resolve()) for path in owned if path)
            self.store.delete_terminal(record["id"])
            if (
                not preserve
                and directory.resolve().parent == (self.store.root / "jobs").resolve()
                and directory.is_dir()
                and not directory.is_symlink()
            ):
                shutil.rmtree(directory)
        return len(selected)

    def close(self) -> None:
        self._closed = True

    def __enter__(self) -> Executor:
        self._check()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

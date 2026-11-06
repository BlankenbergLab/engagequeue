"""Detached native supervision and the durable execution gate.

Each worker is a Linux child subreaper. A separately registered command recorder
persists the actual command's exit even if its original controller disappears.
"""

from __future__ import annotations

import fcntl
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import psutil

from .containers import (
    ContainerOwnershipError,
    ContainerUnavailable,
    OwnedContainer,
    _container_configurations,
    _valid_container_id,
)
from .processes import (
    alive,
    become_subreaper,
    boot_identity,
    children_exist,
    descendants,
    identity,
    reap,
    rss_mb,
    signal_owned,
)


def _store(state_dir):
    from ..store import Store

    return Store(state_dir)


def _root(state_dir, job_id):
    root = Path(state_dir) / "jobs" / job_id
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root.chmod(0o700)
    return root


def _output(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    return os.fdopen(descriptor, "ab", buffering=0)


def child(state_dir: str, job_id: str, token: str) -> int:
    """Register durably, then await the supervisor's one-time execution grant."""
    store = _store(state_dir)
    root = _root(state_dir, job_id)
    with (root / "command.lock").open("a") as lock:
        os.fchmod(lock.fileno(), 0o600)
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 125
        record = store.get(job_id)
        if record["worker_token"] != token or record.get("command_pid"):
            return 125
        if record.get("worker_boot") != boot_identity():
            return 125
        command = identity(os.getpid())
        store.update(job_id, command_pid=command["pid"], command_birth=command["birth"], command_ready=True)
        interrupted = False

        def on_term(signum, frame):
            nonlocal interrupted
            interrupted = True

        signal.signal(signal.SIGTERM, on_term)
        while True:
            record = store.get(job_id)
            if record["worker_token"] != token or record.get("worker_boot") != boot_identity():
                return 125
            if record["cancel_requested"] or interrupted:
                store.update(job_id, execution_returncode=143, execution_finished=time.time())
                return 143
            if record.get("execution_granted"):
                break
            # A child whose worker died before the durable grant cannot execute.
            worker = {"pid": record["worker_pid"], "birth": record["worker_birth"]}
            if alive(worker) is False:
                store.update(
                    job_id,
                    execution_returncode=125,
                    execution_finished=time.time(),
                    execution_failure="Supervisor exited before granting execution",
                )
                return 125
            time.sleep(0.05)
        spec = record["spec"]
        code = 127
        failure = None
        try:
            store.update(job_id, command_started=time.time())
            with _output(spec["stdout"]) as stdout, _output(spec["stderr"]) as stderr:
                process = subprocess.Popen(
                    spec["argv"],
                    cwd=spec["cwd"],
                    env=spec["env"],
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    close_fds=True,
                )
                payload = identity(process.pid)
                store.update(job_id, payload_pid=payload["pid"], payload_birth=payload["birth"])
                code = process.wait()
        except OSError as exc:
            failure = f"Cannot execute command: {type(exc).__name__}: {exc.strerror}"
        finally:
            store.update(job_id, execution_returncode=code, execution_finished=time.time(), execution_failure=failure)
        return code if code >= 0 else 128 - code


def _size(paths):
    seen = set()
    total = 0
    for raw in paths:
        path = Path(raw)
        if path.is_symlink():
            continue
        entries = path.rglob("*") if path.is_dir() else [path]
        for entry in entries:
            try:
                if entry.is_symlink() or not entry.is_file():
                    continue
                stat = entry.stat()
                key = stat.st_dev, stat.st_ino
                if key not in seen:
                    seen.add(key)
                    total += stat.st_size
            except FileNotFoundError:
                continue
    return total


class NativeLauncher:
    def __init__(self):
        self.process = None

    def start(self, argv):
        become_subreaper()
        self.process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, start_new_session=True, close_fds=True)

    def processes(self):
        if self.process:
            self.process.poll()
        result = descendants()
        reap()
        return result

    def signal(self, signum):
        # Enumerating the dedicated subreaper's descendants includes processes
        # that daemonized with setsid/double-fork. No unrelated PGID is inferred.
        for record in reversed(self.processes()):
            signal_owned(record, signum)

    def show(self):
        return {}

    def quiescent(self):
        return not children_exist()

    def cleanup(self):
        self.processes()


def _container_stop_receipt(store, job_id, name):
    record = store.get(job_id)
    request = record.get("container_stop_requests", {}).get(name, {})
    if not request.get("issued_signal"):
        return
    configuration = store._declared_container(record, name)
    receipt = {
        "schema": 1,
        "job_id": job_id,
        "key": record["key"],
        "name": name,
        "container_id": request["container_id"],
        "label_key": configuration["label_key"],
        "label_value": configuration["label_value"],
        "issued_signal": request["issued_signal"],
        "issued_at": request["issued_at"],
        "stopped_at": request.get("stopped_at"),
        "exit_code": request.get("exit_code"),
        "oom_killed": request.get("oom_killed"),
        "job_cancel_requested": record["cancel_requested"],
        "status": "stopped" if request.get("status") == "stopped" else "signal_issued",
    }
    path = Path(store.container_stop_receipt_path(job_id, name))
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=".container-stop-", delete=False) as output:
            temporary = Path(output.name)
            json.dump(receipt, output, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _stop_container_requests(store, job_id, record, containers):
    """Apply sticky, name-scoped intents without signalling the command process."""
    requests = record.get("container_stop_requests", {})
    for container in containers:
        name = container.configuration["name"]
        request = requests.get(name)
        if request is None:
            continue
        pinned = request.get("container_id")
        if pinned:
            if container.pinned_id not in (None, pinned):
                raise ContainerOwnershipError("Requested container's immutable identity changed")
            container.pinned_id = pinned
        data = container.inspect()
        if data is None or not data.get("State", {}).get("Running"):
            # Before first appearance, absence is not acknowledgment: retain
            # the intent so a later service container is still stopped.
            if data is None and not pinned:
                continue
            if request.get("status") != "stopped":
                state = data.get("State", {}) if data else {}
                store.note_container_stop(
                    job_id,
                    name,
                    "container_stop_confirmed",
                    status="stopped",
                    container_id=container.pinned_id,
                    stopped_at=time.time(),
                    exit_code=state.get("ExitCode"),
                    oom_killed=state.get("OOMKilled"),
                )
                _container_stop_receipt(store, job_id, name)
            continue
        if request.get("status") == "stopped":
            raise ContainerOwnershipError("A container restarted after its stop was confirmed")
        signame = None
        if not request.get("term_sent_at"):
            store.note_container_stop(
                job_id,
                name,
                "container_stop_prepared",
                status="stopping",
                container_id=container.pinned_id,
                term_sent_at=time.time(),
            )
            signame = "TERM"
        elif time.time() - request["term_sent_at"] >= record["cancel_grace"] and not request.get("kill_sent_at"):
            store.note_container_stop(job_id, name, "container_stop_kill_prepared", kill_sent_at=time.time())
            signame = "KILL"
        if signame and container.signal(signame):
            store.note_container_stop(
                job_id,
                name,
                "container_stop_signalled" if signame == "TERM" else "container_stop_killed",
                issued_signal=signame,
                issued_at=time.time(),
            )
            _container_stop_receipt(store, job_id, name)


def _finalize_containers(store, job_id, containers):
    """Persist stopped-state evidence before opt-in, non-forced removal.

    The caller has verified command/process or cgroup quiescence. Inspect every
    container before removing any, so unknown ownership retains all resources.
    """
    observations = []
    results = {}
    for container in containers:
        data = container.inspect()
        state = data.get("State", {}) if data else {}
        if data is not None and state.get("Running") is not False:
            raise ContainerOwnershipError("Container writers are not confirmed stopped")
        name = container.configuration["name"]
        results[name] = {
            "container_id": container.pinned_id,
            "absent": data is None,
            "exit_code": state.get("ExitCode"),
            "oom_killed": state.get("OOMKilled"),
        }
        observations.append((container, data))
    store.update(job_id, container_results=results)
    record = store.get(job_id)
    for container, data in observations:
        name = container.configuration["name"]
        request = record.get("container_stop_requests", {}).get(name)
        if request and request.get("status") != "stopped" and container.pinned_id:
            state = data.get("State", {}) if data else {}
            store.note_container_stop(
                job_id,
                name,
                "container_stop_confirmed",
                status="stopped",
                container_id=container.pinned_id,
                stopped_at=time.time(),
                exit_code=state.get("ExitCode"),
                oom_killed=state.get("OOMKilled"),
            )
        if request:
            _container_stop_receipt(store, job_id, name)
    for container, data in observations:
        if not container.configuration.get("remove_on_exit", False):
            continue
        name = container.configuration["name"]
        if not container.pinned_id:
            store.note_container_cleanup(job_id, name, status="not_observed", container_id=None)
            continue
        if data is None:
            store.note_container_cleanup(job_id, name, status="absent", container_id=container.pinned_id)
            continue
        store.note_container_cleanup(job_id, name, status="pending", container_id=container.pinned_id)
        try:
            container.remove()
        except (ContainerOwnershipError, ContainerUnavailable) as exc:
            store.note_container_cleanup(
                job_id,
                name,
                status="unresolved",
                container_id=container.pinned_id,
                reason=f"{type(exc).__name__}: {exc}",
            )
            raise
        store.note_container_cleanup(job_id, name, status="removed", container_id=container.pinned_id)


def supervise(state_dir: str, job_id: str, token: str, *, systemd: bool = False) -> int:
    store = _store(state_dir)
    me = identity(os.getpid())
    if not store.claim_worker(job_id, token, me["pid"], me["birth"]):
        return 0
    _root(state_dir, job_id)
    record = store.get(job_id)
    spec = record["spec"]
    store.update(job_id, worker_boot=boot_identity(), worker_heartbeat=time.time())
    if systemd:
        from .systemd import Unit

        launcher = Unit(job_id, token, spec, record["cancel_grace"])
        store.update(job_id, backend_info={"unit": launcher.name, "token": token})
    else:
        launcher = NativeLauncher()
        if spec.get("require_hard_limits") and (spec.get("cpus") or spec.get("memory_mb")):
            store.transition(
                job_id,
                "FAILED",
                returncode=125,
                reason="Hard CPU/memory limits require the systemd backend or container runtime",
                writers_stopped=True,
            )
            return 125
    containers = [OwnedContainer(configuration) for configuration in _container_configurations(spec)]
    bootstrap = (
        "import runpy,sys; sys.path.insert(0,"
        + repr(str(Path(__file__).resolve().parents[2]))
        + "); runpy.run_module('engagequeue.worker',run_name='__main__')"
    )
    command = [
        sys.executable,
        "-c",
        bootstrap,
        "--state-dir",
        str(state_dir),
        "--job-id",
        job_id,
        "--token",
        token,
        "--child",
    ]
    stopping_at = None
    reason = None
    stop_code = 143
    stopped_by_user = False
    cancel_receipts_written = False
    launched = False
    last_output_check = 0.0
    first = time.monotonic()
    registration_deadline = first + 30
    try:
        if record["cancel_requested"]:
            store.transition(
                job_id, "CANCELLED", returncode=143, reason="Cancelled before command launch", writers_stopped=True
            )
            return 143
        try:
            launcher.start(command)
            launched = True
        except Exception as exc:
            # A failed systemd submission can be uncertain: an RPC may have
            # succeeded before its client timed out. Keep any such unit intact.
            if systemd and getattr(exc, "uncertain", False):
                try:
                    if launcher.show().get("LoadState") != "not-found":
                        store.transition(job_id, "LOST", reason=f"Uncertain systemd launch: {type(exc).__name__}")
                        return 125
                except Exception:
                    store.transition(job_id, "LOST", reason=f"Uncertain systemd launch: {type(exc).__name__}")
                    return 125
            store.transition(
                job_id,
                "FAILED",
                returncode=127,
                reason=f"Supervisor launch failed: {type(exc).__name__}: {exc}",
                writers_stopped=True,
            )
            return 127
        while True:
            current = store.get(job_id)
            if current["worker_token"] != token:
                return 125
            if current.get("worker_boot") != boot_identity():
                store.transition(
                    job_id, "LOST", reason="Execution boot or PID namespace changed; inspect before reconciliation"
                )
                return 125
            processes = launcher.processes()
            store.update(job_id, worker_heartbeat=time.time(), command_members=processes)
            container_running = [container.running() for container in containers]
            # Persist IDs so operator reconciliation can identify a container
            # even when its initial launcher and both controllers have exited.
            store.update(job_id, owned_container_ids=[container.pinned_id for container in containers])
            now = time.monotonic()
            if current["cancel_requested"] and stopping_at is None:
                stopped_by_user, reason = True, "Cancelled by client"
            elif stopping_at is None and spec.get("timeout") and now - first >= spec["timeout"]:
                reason, stop_code = "Execution walltime limit exceeded", 124
            elif (
                stopping_at is None and spec.get("memory_mb") and not systemd and rss_mb(processes) > spec["memory_mb"]
            ):
                reason, stop_code = "Observed process memory limit exceeded", 137
            if stopping_at is None and spec.get("output_limit") is not None and now - last_output_check >= 0.2:
                last_output_check = now
                paths = [spec["stdout"], spec["stderr"], *(spec.get("output_paths") or [])]
                if _size(paths) > spec["output_limit"]:
                    reason, stop_code = "Output size limit exceeded", 153
            if reason is not None and stopping_at is None:
                stopping_at = now
                store.transition(job_id, "STOPPING", reason=reason)
            if stopping_at is not None:
                if current["cancel_requested"] and not cancel_receipts_written:
                    for name in current.get("container_stop_requests", {}):
                        _container_stop_receipt(store, job_id, name)
                    cancel_receipts_written = True
                force = now - stopping_at >= record["cancel_grace"]
                launcher.signal(signal.SIGKILL if force else signal.SIGTERM)
                for container in containers:
                    container.stop(force=force)
            else:
                _stop_container_requests(store, job_id, current, containers)
            if stopping_at is None and current.get("command_ready") and not current.get("execution_granted"):
                command_record = {"pid": current["command_pid"], "birth": current["command_birth"]}
                if alive(command_record) is True:
                    store.update(job_id, execution_granted=True)
                    store.transition(job_id, "RUNNING", started_at=time.time())
            current = store.get(job_id)
            if not processes and not any(container_running) and launcher.quiescent():
                _finalize_containers(store, job_id, containers)
                if current.get("execution_finished") is not None or stopping_at is not None:
                    code = stop_code if stopping_at is not None else current["execution_returncode"]
                    if code is None:
                        code = 125
                    state = "CANCELLED" if stopped_by_user else ("SUCCEEDED" if code == 0 else "FAILED")
                    details = launcher.show() if systemd else {}
                    if systemd and details.get("Result") not in (None, "", "success") and state == "SUCCEEDED":
                        state, code, reason = "FAILED", 125, f"systemd unit failed: {details['Result']}"
                    store.transition(
                        job_id,
                        state,
                        returncode=code,
                        reason=reason or current.get("execution_failure"),
                        writers_stopped=True,
                        finished_at=time.time(),
                    )
                    launcher.cleanup()
                    return int(code)
                if current.get("command_pid"):
                    store.transition(
                        job_id,
                        "FAILED",
                        returncode=125,
                        reason="Command recorder exited without a durable execution result",
                        writers_stopped=True,
                    )
                    launcher.cleanup()
                    return 125
                child_exited = not systemd and launcher.process is not None and launcher.process.poll() is not None
                if systemd:
                    unit_state = launcher.show()
                    child_exited = unit_state.get("SubState") in ("dead", "failed", "exited")
                if child_exited or now >= registration_deadline:
                    store.transition(
                        job_id,
                        "FAILED",
                        returncode=125,
                        reason="Command child exited before registration",
                        writers_stopped=True,
                    )
                    launcher.cleanup()
                    return 125
            time.sleep(0.05)
    except (ContainerUnavailable, ContainerOwnershipError, psutil.AccessDenied) as exc:
        # Unavailable ownership evidence cannot authorize kill, slot release,
        # or workspace cleanup. Operators have an explicit reconciliation API.
        store.transition(job_id, "LOST", reason=f"Execution ownership requires reconciliation: {exc}")
        return 125
    except Exception as exc:
        try:
            if launched and (
                launcher.processes() or any(container.running() for container in containers) or not launcher.quiescent()
            ):
                store.transition(
                    job_id, "LOST", reason=f"Supervisor failure requires reconciliation: {type(exc).__name__}: {exc}"
                )
            else:
                _finalize_containers(store, job_id, containers)
                store.transition(
                    job_id,
                    "FAILED",
                    returncode=125,
                    reason=f"Supervisor failed: {type(exc).__name__}: {exc}",
                    writers_stopped=True,
                )
                launcher.cleanup()
        except Exception:
            # The durable reservation and worker identity retain uncertainty.
            pass
        return 125


def run_job(state_dir: str, job_id: str, token: str) -> int:
    return supervise(state_dir, job_id, token)


def resources_stopped(record: dict) -> bool | None:
    """Check recorded ownership without signalling or assuming a vanished PID.

    A vanished native supervisor can leave unobserved daemonized descendants.
    That uncertainty is deliberately exposed for explicit operator resolution.
    """
    if record.get("writers_stopped"):
        return True
    current = boot_identity()
    saved = record.get("worker_boot") or {
        "boot_id": record.get("admitted_boot_id"),
        "pid_namespace": record.get("admitted_pid_namespace", record.get("pid_namespace")),
    }
    boot_changed = saved.get("boot_id") is not None and saved["boot_id"] != current["boot_id"]
    if not boot_changed and saved.get("pid_namespace") not in (None, current["pid_namespace"]):
        return None
    configurations = _container_configurations(record["spec"])
    saved_ids = record.get("owned_container_ids")
    valid_ids = (
        isinstance(saved_ids, list)
        and len(saved_ids) == len(configurations)
        and all(value is None or _valid_container_id(value) for value in saved_ids)
    )
    uncertain = "owned_container_ids" in record and not valid_ids
    may_have_started = bool(record.get("execution_granted") or record.get("command_started"))
    for index, configuration in enumerate(configurations):
        container = OwnedContainer(configuration)
        pinned = saved_ids[index] if valid_ids else None
        container.pinned_id = pinned
        try:
            data = container.inspect()
            state = data.get("State") if data is not None else None
            if isinstance(state, dict) and (state.get("Running") is True or state.get("Restarting") is True):
                return False
            if data is None:
                # With no saved immutable ID, name absence cannot rule out a
                # renamed external container after execution may have started.
                if pinned is None and may_have_started:
                    uncertain = True
            elif not isinstance(state, dict) or state.get("Running") is not False:
                uncertain = True
        except (ContainerUnavailable, ContainerOwnershipError):
            uncertain = True
    if not boot_changed:
        records = list(record.get("command_members") or [])
        for prefix in ("worker", "command", "payload"):
            if record.get(prefix + "_pid"):
                records.append({"pid": record[prefix + "_pid"], "birth": record.get(prefix + "_birth")})
        statuses = [alive(item) for item in records]
        if True in statuses:
            return False
        if None in statuses:
            uncertain = True
    if record.get("backend") == "systemd":
        from .systemd import SystemdUnavailable, Unit

        unit = Unit(record["id"], record["worker_token"], record["spec"], record["cancel_grace"])
        try:
            if unit.processes():
                return False
            if not unit.quiescent():
                return False
        except (SystemdUnavailable, OSError, psutil.Error):
            uncertain = True
        return None if uncertain else True
    if boot_changed:
        return None if uncertain else True
    if may_have_started:
        uncertain = True
    return None if uncertain else True

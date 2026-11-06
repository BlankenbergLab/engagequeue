"""Detached scheduler, elected once for each persistent local state directory."""

from __future__ import annotations

import argparse
import fcntl
import logging
import os
import signal
import subprocess
import time

import psutil

from .executor import boot_identity, pid_namespace, process_alive, python_interpreter
from .store import Store

log = logging.getLogger(__name__)
ACTIVE = ("STARTING", "RUNNING", "STOPPING", "LOST")


class Runtime:
    def __init__(self, state_dir: str):
        self.store = Store(state_dir)
        self.root = self.store.root
        self.python = python_interpreter(self.store.get_meta("runtime_python"))
        self.interval = self.store.get_meta("poll_interval", 0.2)
        self.children: dict[str, subprocess.Popen] = {}
        self.stopping = False
        self.boot = boot_identity()
        self.namespace = pid_namespace()

    def _adopt_unregistered_worker(self, record: dict) -> bool:
        token = record.get("worker_token")
        if not token:
            return False
        for process in psutil.process_iter(("pid", "cmdline", "create_time", "uids")):
            try:
                if process.info["uids"].real != os.getuid():
                    continue
                argv = process.info["cmdline"] or []
                expected = (
                    "engagequeue.worker",
                    "--state-dir",
                    str(self.root),
                    "--job-id",
                    record["id"],
                    "--token",
                    token,
                )
                if any(tuple(argv[index : index + len(expected)]) == expected for index in range(len(argv))):
                    return self.store.claim_worker(record["id"], token, process.pid, process.info["create_time"])
            except (psutil.Error, KeyError, TypeError):
                continue
        return False

    def _mark_missing(self, record: dict, reason: str) -> None:
        from .backends.native import resources_stopped

        stopped = resources_stopped(record)
        if stopped is True and not record.get("command_started"):
            self.store.transition(
                record["id"],
                "CANCELLED" if record["cancel_requested"] else "FAILED",
                writers_stopped=True,
                returncode=143 if record["cancel_requested"] else 70,
                reason=reason,
            )
        else:
            self.store.transition(record["id"], "LOST", reason=reason)

    def reconcile(self) -> None:
        for job_id, process in list(self.children.items()):
            returncode = process.poll()
            if returncode is None:
                continue
            del self.children[job_id]
            record = self.store.get(job_id)
            if record["state"] in ("STARTING", "RUNNING", "STOPPING"):
                self._mark_missing(
                    record,
                    f"Supervisor exited before confirmed completion (exit {returncode}); command will not be replayed",
                )
        for record in self.store.list(states=("STARTING", "RUNNING", "STOPPING")):
            if (
                record.get("admitted_pid_namespace", self.namespace) != self.namespace
                and record.get("admitted_boot_id") == self.boot
            ):
                self.store.transition(
                    record["id"],
                    "LOST",
                    reason="Execution PID namespace changed; attach from its execution namespace to reconcile",
                )
                continue
            if record.get("admitted_boot_id", self.boot) != self.boot:
                from .backends.native import resources_stopped

                if resources_stopped(record) is True:
                    self.store.transition(
                        record["id"],
                        "FAILED",
                        writers_stopped=True,
                        returncode=255,
                        reason="Execution host rebooted; accepted commands are not replayed",
                    )
                else:
                    self.store.transition(
                        record["id"], "LOST", reason="Host rebooted and owned external resources require reconciliation"
                    )
                continue
            alive = process_alive(record.get("worker_pid"), record.get("worker_birth"))
            if alive is True or record["id"] in self.children:
                continue
            if record.get("worker_pid") is None and self._adopt_unregistered_worker(record):
                continue
            if time.time() - record.get("admitted_at", record["created_at"]) < 10:
                continue
            self._mark_missing(
                record, "Supervisor ownership was lost; inspect and reconcile without replaying the command"
            )

    def launch(self, record: dict) -> None:
        directory = self.root / "jobs" / record["id"]
        process = None
        try:
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            descriptor = os.open(directory / "supervisor.log", os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
            with os.fdopen(descriptor, "ab") as output:
                process = subprocess.Popen(
                    [
                        self.python,
                        "-m",
                        "engagequeue.worker",
                        "--state-dir",
                        str(self.root),
                        "--job-id",
                        record["id"],
                        "--token",
                        record["worker_token"],
                    ],
                    cwd=directory,
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=output,
                    close_fds=True,
                    start_new_session=True,
                )
                self.children[record["id"]] = process
        except Exception as exc:
            if process is not None:
                # A failed log close must not publish writer termination after
                # the supervisor has already started. Its execution stays owned.
                log.exception("Supervisor for %s started but its log could not be closed", record["id"])
                return
            self.store.transition(
                record["id"],
                "FAILED",
                writers_stopped=True,
                returncode=70,
                reason=f"Unable to start supervisor: {type(exc).__name__}",
            )
            log.exception("Unable to start supervisor for %s", record["id"])

    def run(self) -> None:
        descriptor = os.open(self.root / "runtime.lock", os.O_CREAT | os.O_RDWR, 0o600)
        with os.fdopen(descriptor, "a") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            identity = {
                "pid": os.getpid(),
                "birth": psutil.Process().create_time(),
                "boot_id": self.boot,
                "pid_namespace": self.namespace,
                "started_at": time.time(),
            }
            self.store.put_meta("runtime", identity)
            signal.signal(signal.SIGTERM, lambda *_: setattr(self, "stopping", True))
            signal.signal(signal.SIGINT, lambda *_: setattr(self, "stopping", True))
            try:
                while not self.stopping:
                    self.reconcile()
                    if self.store.get_meta("shutdown_requested", False):
                        if not self.store.list(states=ACTIVE):
                            break
                    else:
                        for name in self.store.pools():
                            while True:
                                record = self.store.reserve_next(
                                    name, admitted_boot_id=self.boot, admitted_pid_namespace=self.namespace
                                )
                                if record is None:
                                    break
                                self.launch(record)
                    self.store.put_meta("runtime_heartbeat", time.time())
                    time.sleep(self.interval)
            finally:
                # Workers own their accepted executions and outlive this loop.
                self.store.put_meta("runtime", {})


def serve(state_dir: str) -> None:
    """Run the elected coordinator in the foreground."""
    Runtime(state_dir).run()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    args = parser.parse_args(argv)
    os.umask(0o077)
    logging.basicConfig(level=logging.INFO)
    Runtime(args.state_dir).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

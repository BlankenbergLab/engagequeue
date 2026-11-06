"""Optional systemd user-unit launcher, retaining results until durable import."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import psutil

from .processes import alive, identity


class SystemdUnavailable(RuntimeError):
    def __init__(self, message: str, *, uncertain: bool = False):
        super().__init__(message)
        self.uncertain = uncertain


class Unit:
    def __init__(self, job_id: str, token: str, spec: dict, grace: float):
        self.name = f"engagequeue-{job_id}.service"
        self.token = token
        self.spec = spec
        self.grace = grace
        self.started = False

    def _run(self, *args: str):
        try:
            return subprocess.run(list(args), capture_output=True, text=True, timeout=20)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise SystemdUnavailable("systemd user manager is unavailable") from exc

    def start(self, argv: list[str]) -> None:
        if not shutil.which("systemd-run") or not shutil.which("systemctl"):
            raise SystemdUnavailable("systemd-run and systemctl are required by this pool")
        available = self._run("systemctl", "--user", "show-environment")
        if available.returncode:
            raise SystemdUnavailable("A functioning systemd user manager is required by this pool")
        properties = [
            "Type=exec",
            "RemainAfterExit=yes",
            "ExitType=cgroup",
            "KillMode=control-group",
            f"TimeoutStopSec={self.grace}",
            f"Description=EngageQueue {self.token}",
        ]
        if self.spec.get("timeout"):
            properties.append(f"RuntimeMaxSec={self.spec['timeout']}")
        if self.spec.get("memory_mb"):
            properties.append(f"MemoryMax={int(self.spec['memory_mb'] * 1024 * 1024)}")
        if self.spec.get("cpus"):
            properties.append(f"CPUQuota={float(self.spec['cpus']) * 100}%")
        command = ["systemd-run", "--user", "--quiet", f"--unit={self.name}"]
        for value in properties:
            command.extend(["--property", value])
        # Only immutable paths/IDs appear in argv. The private state database
        # supplies the job environment; secrets are not systemd-run arguments.
        try:
            result = self._run(*command, *argv)
        except SystemdUnavailable as exc:
            raise SystemdUnavailable("systemd submission response is uncertain", uncertain=True) from exc
        if result.returncode:
            raise SystemdUnavailable("Unable to start the requested systemd unit", uncertain=True)
        self.started = True

    def show(self) -> dict[str, str]:
        result = self._run(
            "systemctl",
            "--user",
            "show",
            self.name,
            "--property=LoadState,ActiveState,SubState,Result,ExecMainCode,ExecMainStatus,ControlGroup,Description,TasksCurrent",
        )
        if result.returncode:
            raise SystemdUnavailable("Unable to query the owned systemd unit")
        data = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        if data.get("LoadState") != "not-found" and data.get("Description") != f"EngageQueue {self.token}":
            raise SystemdUnavailable("systemd unit ownership differs")
        return data

    def processes(self) -> list[dict]:
        data = self.show()
        group = data.get("ControlGroup")
        records = []
        if not group:
            return records
        root = (Path("/sys/fs/cgroup") / group.lstrip("/")).resolve()
        if not root.is_relative_to(Path("/sys/fs/cgroup")):
            raise SystemdUnavailable("Invalid systemd control-group path")
        for path in [root / "cgroup.procs", *root.glob("**/cgroup.procs")]:
            try:
                values = path.read_text().split()
            except FileNotFoundError:
                continue
            for value in values:
                try:
                    record = identity(int(value))
                    if alive(record):
                        records.append(record)
                except psutil.NoSuchProcess:
                    pass
        return records

    def quiescent(self) -> bool:
        data = self.show()
        if data.get("LoadState") == "not-found":
            return True
        group = data.get("ControlGroup")
        if not group and data.get("SubState") in ("exited", "dead", "failed"):
            # With ExitType=cgroup, systemd drops the empty control group after
            # the last descendant exits while RemainAfterExit retains results.
            return True
        if group:
            root = (Path("/sys/fs/cgroup") / group.lstrip("/")).resolve()
            if not root.is_relative_to(Path("/sys/fs/cgroup")):
                raise SystemdUnavailable("Invalid systemd control-group path")
            try:
                events = dict(line.split() for line in (root / "cgroup.events").read_text().splitlines())
                return events.get("populated") == "0"
            except FileNotFoundError:
                pass
        # TasksCurrent is a kernel-backed count, unlike a racy filtered process
        # snapshot. Unknown accounting never authorizes completion or cleanup.
        return data.get("TasksCurrent") == "0"

    def signal(self, signum: int) -> None:
        data = self.show()  # Ownership is rechecked before every signal.
        if data.get("LoadState") == "not-found":
            return
        result = self._run(
            "systemctl",
            "--user",
            "kill",
            "--kill-whom=all",
            f"--signal={signum}",
            self.name,
        )
        if result.returncode and self.processes():
            raise SystemdUnavailable("Unable to signal the owned systemd unit")

    def cleanup(self) -> None:
        # Called only after the coordinator/worker committed a terminal result.
        # No --collect is used during startup, so very short results survive.
        if self.started:
            self._run("systemctl", "--user", "stop", self.name)
            self._run("systemctl", "--user", "reset-failed", self.name)


def run_job(state_dir: str, job_id: str, token: str) -> int:
    from .native import supervise

    return supervise(state_dir, job_id, token, systemd=True)

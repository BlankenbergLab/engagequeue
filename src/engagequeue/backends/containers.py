"""Inspect and stop explicitly owned containers without inferring ownership."""

from __future__ import annotations

import json
import re
import shlex
import subprocess
from pathlib import Path


class ContainerUnavailable(RuntimeError):
    pass


class ContainerOwnershipError(RuntimeError):
    pass


def _container_configurations(spec: dict) -> list[dict]:
    """Use the same stable ordering for supervision and persisted identities."""
    configurations = list(spec.get("containers") or [])
    if spec.get("container"):
        configurations.append(spec["container"])
    unique = {(c["name"], c["label_key"], c["label_value"]): c for c in configurations}
    return list(unique.values())


def _valid_container_id(value) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None


class OwnedContainer:
    def __init__(self, configuration: dict | None):
        self.configuration = configuration
        self.pinned_id = None
        self.observed = False

    def command(self, *args: str) -> list[str]:
        assert self.configuration
        executable = self.configuration.get("executable", "docker")
        result = shlex.split(executable) if isinstance(executable, str) else list(executable)
        if host := self.configuration.get("host"):
            result.extend(["--host", host])
        return [*result, *args]

    def _execute(self, *args: str):
        try:
            return subprocess.run(self.command(*args), capture_output=True, text=True, timeout=15)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ContainerUnavailable("Container runtime is unavailable") from exc

    def inspect(self) -> dict | None:
        if not self.configuration:
            return None
        configuration = self.configuration
        if configuration.get("runtime", "docker") != "docker":
            raise ContainerOwnershipError("Unsupported owned-container runtime")
        if not configuration.get("name") or not configuration.get("label_key") or not configuration.get("label_value"):
            raise ContainerOwnershipError("Container name and ownership label are required")
        if self.pinned_id is not None and not _valid_container_id(self.pinned_id):
            raise ContainerOwnershipError("Invalid pinned container identity")
        candidate = self.pinned_id or configuration["name"]
        cid = None
        cid_file = configuration.get("cid_file")
        if cid_file:
            try:
                cid = Path(cid_file).read_text().strip()
            except FileNotFoundError:
                pass
            except (OSError, UnicodeError) as exc:
                raise ContainerOwnershipError("Cannot read container ID file") from exc
            else:
                if not re.fullmatch(r"[a-f0-9]{12,64}", cid):
                    raise ContainerOwnershipError("Invalid container ID file")
                if self.pinned_id:
                    if not self.pinned_id.startswith(cid):
                        raise ContainerOwnershipError("Container ID file differs from pinned identity")
                else:
                    candidate = cid
        result = self._execute("inspect", candidate)
        if result.returncode:
            # Only the daemon reporting absence of the exact queried identity
            # proves absence. DNS errors ("no such host") and unavailable
            # executables/transports must keep ownership unresolved.
            missing = r"(?:Error(?: response from daemon)?:\s*)?No such (?:object|container):\s*" + re.escape(candidate)
            if re.fullmatch(missing, result.stderr.strip(), re.IGNORECASE):
                return None
            raise ContainerUnavailable("Container inspection failed")
        try:
            rows = json.loads(result.stdout)
            if len(rows) != 1:
                raise ValueError()
            data = rows[0]
            identifier = data["Id"]
            labels = data.get("Config", {}).get("Labels") or {}
            if (
                not _valid_container_id(identifier)
                or data.get("Name", "").lstrip("/") != configuration["name"]
                or labels.get(configuration["label_key"]) != configuration["label_value"]
                or (self.pinned_id and identifier != self.pinned_id)
                or (cid is not None and not identifier.startswith(cid))
            ):
                raise ContainerOwnershipError("Container identity or ownership label differs")
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
            raise ContainerOwnershipError("Malformed container inspection") from exc
        self.pinned_id = identifier
        self.observed = True
        return data

    def running(self) -> bool:
        data = self.inspect()
        return bool(data and data.get("State", {}).get("Running"))

    def remove(self) -> None:
        """Remove only a pinned, verified stopped container, without force."""
        if not self.configuration or not self.configuration.get("remove_on_exit", False):
            raise ContainerOwnershipError("Container removal was not explicitly requested")
        if not self.pinned_id:
            raise ContainerOwnershipError("Container removal requires a verified immutable ID")
        data = self.inspect()
        if data is None:
            return
        state = data.get("State", {})
        if (
            state.get("Running") is not False
            or state.get("Restarting") is not False
            or state.get("Status") not in ("created", "exited", "dead")
        ):
            raise ContainerOwnershipError("Container removal requires a confirmed stopped state")
        result = self._execute("rm", data["Id"])
        remaining = self.inspect()
        if remaining is not None:
            raise ContainerUnavailable("Owned container removal was not confirmed")
        if result.returncode:
            # A verified absent immutable ID can race another owned cleanup.
            return

    def signal(self, signame: str) -> bool:
        """Signal only the verified immutable identity without blocking its shell."""
        if signame not in ("TERM", "KILL"):
            raise ValueError("Owned-container signalling supports TERM and KILL")
        data = self.inspect()
        if not data or not data.get("State", {}).get("Running"):
            return False
        result = self._execute("kill", "--signal=" + signame, data["Id"])
        if result.returncode:
            if self.running():
                raise ContainerUnavailable("Owned container signalling failed")
            return False
        return True

    def stop(self, force: bool = False) -> None:
        data = self.inspect()
        if not data or not data.get("State", {}).get("Running"):
            return
        args = ("kill", "--signal=KILL", data["Id"]) if force else ("stop", "--time=1", data["Id"])
        result = self._execute(*args)
        if result.returncode:
            # An exited/auto-removed owned container can race the stop request.
            if self.running():
                raise ContainerUnavailable("Owned container termination failed")

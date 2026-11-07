"""Stable public job and pool descriptions, independent of any integration."""

from __future__ import annotations

import math
import os
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence


class JobState(str, Enum):
    QUEUED = "QUEUED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    LOST = "LOST"

    @property
    def terminal(self) -> bool:
        return self in TERMINAL_STATES


TERMINAL_STATES = frozenset((JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED))


def capacity(value: int | str | None) -> int:
    if value is None:
        return 0
    if value == "*":
        return os.cpu_count() or 1
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("max_jobs must be a nonnegative integer, None, or '*'")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("max_jobs must be a nonnegative integer, None, or '*'") from exc
    if result < 0 or (isinstance(value, str) and str(result) != value.strip()):
        raise ValueError("max_jobs must be a nonnegative integer, None, or '*'")
    return result


def positive_number(value: Any, name: str, *, allow_zero: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError(f"{name} must be a finite number")
    result = float(value)
    if not math.isfinite(result) or result < 0 or (not allow_zero and result == 0):
        raise ValueError(f"{name} must be {'nonnegative' if allow_zero else 'positive'} and finite")
    return result


def nonnegative_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


def environment_mapping(value: Mapping[str, str], name: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a mapping of strings")
    values = dict(value)
    for key, item in values.items():
        if not isinstance(key, str) or not key or "=" in key or "\x00" in key:
            raise ValueError("environment keys must be nonempty strings without '=' or NUL")
        if not isinstance(item, str) or "\x00" in item:
            raise ValueError("environment values must be strings without NUL")
    return values


@dataclass(frozen=True)
class PoolConfig:
    max_jobs: int | str | None = 1
    backend: str = "native"
    cancel_grace: float = 10.0

    def __post_init__(self) -> None:
        # Preserve execution-host CPU intent for managed clients without
        # changing the existing public normalized value or serialization.
        object.__setattr__(self, "_automatic_capacity", self.max_jobs == "*")
        object.__setattr__(self, "max_jobs", capacity(self.max_jobs))
        if self.backend not in ("native", "systemd"):
            raise ValueError("backend must be 'native' or 'systemd'")
        object.__setattr__(self, "cancel_grace", positive_number(self.cancel_grace, "cancel_grace", allow_zero=True))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ContainerSpec:
    name: str
    label_value: str
    runtime: str = "docker"
    label_key: str = "engagequeue.attempt"
    cid_file: str | None = None
    executable: str = "docker"
    host: str | None = None
    remove_on_exit: bool = False

    def __post_init__(self) -> None:
        if self.runtime != "docker":
            raise ValueError(
                "Owned-container lifecycle currently supports Docker; Apptainer runs as a supervised command"
            )
        if not isinstance(self.remove_on_exit, bool):
            raise ValueError("container remove_on_exit must be a boolean")
        for name in ("name", "label_key", "label_value", "executable"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value or "\x00" in value:
                raise ValueError(f"container {name} must be a nonempty string without NUL")
        if self.cid_file is not None:
            object.__setattr__(self, "cid_file", str(Path(self.cid_file).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        # Keep existing accepted-specification digests stable when cleanup is
        # not requested, including callers using a new client on old state.
        if not self.remove_on_exit:
            result.pop("remove_on_exit")
        return result


@dataclass(frozen=True)
class JobSpec:
    argv: Sequence[str]
    cwd: str
    env: Mapping[str, str] | None = None
    stdout: str | None = None
    stderr: str | None = None
    pool: str = "default"
    timeout: float | None = None
    output_limit: int | None = None
    output_paths: Sequence[str] = field(default_factory=tuple)
    memory_mb: int | None = None
    cpus: int | None = None
    container: ContainerSpec | None = None
    containers: Sequence[ContainerSpec] = field(default_factory=tuple)
    require_hard_limits: bool = False
    env_overrides: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        if isinstance(self.argv, (str, bytes)) or not self.argv:
            raise ValueError("argv must be a nonempty sequence of argument strings")
        argv = tuple(self.argv)
        if any(not isinstance(value, str) or "\x00" in value for value in argv) or not argv[0]:
            raise ValueError("argv must contain argument strings without NUL and a nonempty executable")
        object.__setattr__(self, "argv", argv)
        object.__setattr__(self, "cwd", str(Path(self.cwd).expanduser().resolve()))
        if not isinstance(self.pool, str) or not self.pool or "\x00" in self.pool:
            raise ValueError("pool must be a nonempty name")
        if self.env is not None:
            object.__setattr__(self, "env", environment_mapping(self.env, "env"))
        if self.env_overrides is not None:
            values = environment_mapping(self.env_overrides, "env_overrides")
            object.__setattr__(self, "env_overrides", values or None)
        if self.env is not None and self.env_overrides:
            raise ValueError("env and env_overrides cannot be combined")
        for name in ("stdout", "stderr"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, str(Path(value).expanduser().resolve()))
        if self.timeout is not None:
            object.__setattr__(self, "timeout", positive_number(self.timeout, "timeout"))
        if self.output_limit is not None:
            nonnegative_integer(self.output_limit, "output_limit")
        if isinstance(self.output_paths, (str, bytes)):
            raise ValueError("output_paths must be a sequence of paths")
        object.__setattr__(self, "output_paths", tuple(str(Path(p).expanduser().resolve()) for p in self.output_paths))
        for name in ("memory_mb", "cpus"):
            value = getattr(self, name)
            if value is not None and (nonnegative_integer(value, name) == 0):
                raise ValueError(f"{name} must be positive")
        if self.container is not None and not isinstance(self.container, ContainerSpec):
            raise ValueError("container must be a ContainerSpec")
        if isinstance(self.containers, (str, bytes)) or any(
            not isinstance(item, ContainerSpec) for item in self.containers
        ):
            raise ValueError("containers must be a sequence of ContainerSpec values")
        object.__setattr__(self, "containers", tuple(self.containers))
        if not isinstance(self.require_hard_limits, bool):
            raise ValueError("require_hard_limits must be a boolean")

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["argv"] = list(self.argv)
        result["output_paths"] = list(self.output_paths)
        result["container"] = self.container.to_dict() if self.container is not None else None
        result["containers"] = [item.to_dict() for item in self.containers]
        if not self.env_overrides:
            result.pop("env_overrides")
        return result

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> JobSpec:
        data = dict(value)
        if data.get("container") is not None:
            data["container"] = ContainerSpec(**data["container"])
        data["containers"] = tuple(ContainerSpec(**item) for item in data.get("containers", ()))
        return cls(**data)


@dataclass(frozen=True)
class JobHandle:
    id: str
    key: str
    state: JobState

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "key": self.key, "state": self.state.value}


@dataclass(frozen=True)
class JobStatus:
    id: str
    key: str
    state: JobState
    pool: str
    returncode: int | None
    reason: str | None
    created_at: float
    started_at: float | None = None
    finished_at: float | None = None
    worker_pid: int | None = None
    backend: str = "native"
    writers_stopped: bool = False

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def needs_reconciliation(self) -> bool:
        return self.state == JobState.LOST

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["state"] = self.state.value
        result["terminal"] = self.terminal
        result["needs_reconciliation"] = self.needs_reconciliation
        return result

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> JobStatus:
        fields = {name: record.get(name) for name in cls.__dataclass_fields__}
        fields["state"] = JobState(record["state"])
        fields["writers_stopped"] = bool(record.get("writers_stopped"))
        return cls(**fields)

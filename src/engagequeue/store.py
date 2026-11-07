"""Transactional local execution records.

Connections are operation-scoped. No SQLite connection or thread is inherited
across a preloaded application's fork boundary.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Iterator

from .errors import AdmissionClosed, RuntimeUnavailable, SubmissionConflict
from .models import TERMINAL_STATES, JobState, PoolConfig

SCHEMA_VERSION = 1
TERMINAL_VALUES = tuple(state.value for state in TERMINAL_STATES)
IMMUTABLE_FIELDS = frozenset(("id", "key", "digest", "spec", "pool", "backend", "cancel_grace", "created_at"))
TRANSITIONS = {
    "QUEUED": {"STARTING", "CANCELLED", "FAILED"},
    "STARTING": {"RUNNING", "STOPPING", "FAILED", "CANCELLED", "LOST"},
    "RUNNING": {"STOPPING", "SUCCEEDED", "FAILED", "CANCELLED", "LOST"},
    "STOPPING": {"SUCCEEDED", "FAILED", "CANCELLED", "LOST"},
    "LOST": {"FAILED", "CANCELLED"},
}


def dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class Store:
    def __init__(self, state_dir: str | Path):
        self.root = Path(state_dir).expanduser().resolve()
        if (self.root / "managed.json").exists() or (self.root / "managed.json").is_symlink():
            raise RuntimeUnavailable("Managed Docker state requires DockerExecutor or --execution-mode docker")
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.root.stat().st_uid != os.getuid():
            raise PermissionError("Execution state must be owned by the current user")
        self.root.chmod(0o700)
        self.database = self.root / "state.sqlite3"
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    @contextlib.contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        # A busy timeout cannot serialize competing WAL-mode changes on a new
        # database. Acquire the filesystem lock before either initializer opens
        # SQLite, and retain it until all initialization connections are closed.
        descriptor = os.open(self.root / "initialization.lock", os.O_CREAT | os.O_RDWR, 0o600)
        with os.fdopen(descriptor, "a") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            self._initialize_locked()

    def _initialize_locked(self) -> None:
        # A native initializer may have started just before managed mode was
        # reserved. Recheck while sharing its initialization election lock.
        if (self.root / "managed.json").exists() or (self.root / "managed.json").is_symlink():
            raise RuntimeUnavailable("Managed Docker state requires DockerExecutor or --execution-mode docker")
        descriptor = os.open(self.database, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(descriptor)
        self.database.chmod(0o600)
        connection = self._connect()
        try:
            # Inspect an existing schema before performing any DDL or changing
            # journal mode; an older client must not mutate a newer runtime.
            present = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone()
            if present:
                version = connection.execute("SELECT value FROM meta WHERE key='schema'").fetchone()
                if version is not None and json.loads(version[0]) != SCHEMA_VERSION:
                    raise RuntimeError("Unsupported EngageQueue state schema; use the matching runtime")
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript(
                "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);"
                "CREATE TABLE IF NOT EXISTS pools (name TEXT PRIMARY KEY,config TEXT NOT NULL);"
                "CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY,submission_key TEXT NOT NULL UNIQUE,"
                "digest TEXT NOT NULL,state TEXT NOT NULL,pool TEXT NOT NULL,created_at REAL NOT NULL,record TEXT NOT NULL);"
                "CREATE INDEX IF NOT EXISTS jobs_pool_state ON jobs(pool,state,created_at);"
                "CREATE TABLE IF NOT EXISTS events (sequence INTEGER PRIMARY KEY AUTOINCREMENT,"
                "job_id TEXT,time REAL NOT NULL,event TEXT NOT NULL,details TEXT NOT NULL);"
            )
        finally:
            connection.close()
        with self.transaction() as connection:
            row = connection.execute("SELECT value FROM meta WHERE key='schema'").fetchone()
            if row is not None and json.loads(row[0]) != SCHEMA_VERSION:
                raise RuntimeError("Unsupported EngageQueue state schema; use the matching runtime")
            connection.execute("INSERT OR IGNORE INTO meta VALUES ('schema',?)", (dump(SCHEMA_VERSION),))
            connection.execute("INSERT OR IGNORE INTO meta VALUES ('runtime_id',?)", (dump(str(uuid.uuid4())),))
            connection.execute("INSERT OR IGNORE INTO meta VALUES ('admission_open','true')")
            connection.execute("INSERT OR IGNORE INTO meta VALUES ('shutdown_requested','false')")

    @staticmethod
    def _record(row: sqlite3.Row | None) -> dict[str, Any]:
        if row is None:
            raise KeyError("Unknown EngageQueue job")
        result = json.loads(row["record"])
        for key in (
            "worker_pid",
            "worker_birth",
            "command_pid",
            "command_birth",
            "command_started",
            "execution_returncode",
            "execution_finished",
            "worker_heartbeat",
            "backend_info",
            "started_at",
            "finished_at",
            "returncode",
            "reason",
            "worker_token",
        ):
            result.setdefault(key, None)
        result.setdefault("cancel_requested", False)
        result.setdefault("writers_stopped", False)
        return result

    @staticmethod
    def _write(connection: sqlite3.Connection, record: dict[str, Any]) -> None:
        connection.execute("UPDATE jobs SET state=?,record=? WHERE id=?", (record["state"], dump(record), record["id"]))

    @staticmethod
    def _event(connection: sqlite3.Connection, job_id: str | None, event: str, details: Any) -> None:
        connection.execute(
            "INSERT INTO events(job_id,time,event,details) VALUES (?,?,?,?)",
            (job_id, time.time(), event, dump(details)),
        )

    def get(self, job_id: str) -> dict[str, Any]:
        connection = self._connect()
        try:
            return self._record(connection.execute("SELECT record FROM jobs WHERE id=?", (job_id,)).fetchone())
        finally:
            connection.close()

    def lookup(self, key: str) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            row = connection.execute("SELECT record FROM jobs WHERE submission_key=?", (key,)).fetchone()
            return self._record(row) if row is not None else None
        finally:
            connection.close()

    def list(
        self, *, pool: str | None = None, states: tuple[str, ...] | None = None, include_pruned: bool = False
    ) -> list[dict[str, Any]]:
        statement, parameters = "SELECT record FROM jobs", []
        clauses = []
        if pool is not None:
            clauses.append("pool=?")
            parameters.append(pool)
        if states:
            clauses.append("state IN (" + ",".join("?" for _ in states) + ")")
            parameters.extend(states)
        if clauses:
            statement += " WHERE " + " AND ".join(clauses)
        statement += " ORDER BY created_at,id"
        connection = self._connect()
        try:
            records = [self._record(row) for row in connection.execute(statement, parameters)]
            return records if include_pruned else [record for record in records if not record.get("pruned")]
        finally:
            connection.close()

    def create(
        self,
        record: dict[str, Any],
        *,
        prepare: Callable[[sqlite3.Connection], dict[str, Any]] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Prepare only new records, under the same transaction as acceptance."""
        with self.transaction() as connection:
            previous = connection.execute("SELECT record FROM jobs WHERE submission_key=?", (record["key"],)).fetchone()
            if previous is not None:
                existing = self._record(previous)
                if existing["digest"] != record["digest"]:
                    raise SubmissionConflict("The submission key already identifies a different job specification")
                return existing, False
            admission = connection.execute("SELECT value FROM meta WHERE key='admission_open'").fetchone()
            if admission is not None and not json.loads(admission[0]):
                raise AdmissionClosed("Execution admission is closed; explicitly resume the runtime")
            if prepare is not None:
                record = prepare(connection)
            connection.execute(
                "INSERT INTO jobs VALUES (?,?,?,?,?,?,?)",
                (
                    record["id"],
                    record["key"],
                    record["digest"],
                    record["state"],
                    record["pool"],
                    record["created_at"],
                    dump(record),
                ),
            )
            self._event(connection, record["id"], "submitted", {"pool": record["pool"]})
            return record, True

    def update(self, job_id: str, **fields: Any) -> dict[str, Any]:
        if IMMUTABLE_FIELDS.intersection(fields):
            raise ValueError("Execution identity and accepted specification are immutable")
        if "state" in fields:
            state = fields.pop("state")
            self.transition(job_id, state, **fields)
            return self.get(job_id)
        with self.transaction() as connection:
            record = self._record(connection.execute("SELECT record FROM jobs WHERE id=?", (job_id,)).fetchone())
            record.update(fields)
            self._write(connection, record)
            return record

    def transition(self, job_id: str, state: str | JobState, **fields: Any) -> bool:
        state = JobState(state).value
        if IMMUTABLE_FIELDS.intersection(fields):
            raise ValueError("Execution identity and accepted specification are immutable")
        with self.transaction() as connection:
            record = self._record(connection.execute("SELECT record FROM jobs WHERE id=?", (job_id,)).fetchone())
            old = record["state"]
            if old != state and state not in TRANSITIONS.get(old, set()):
                return False
            # Cancellation and result publication race across processes. A
            # worker's earlier snapshot cannot overwrite a cancellation that
            # committed before this terminal transaction acquired the lock.
            if state == "SUCCEEDED" and record.get("cancel_requested") and old not in TERMINAL_VALUES:
                state = "CANCELLED"
                fields["returncode"] = 143
                fields["reason"] = "Cancellation was requested before successful completion committed"
            if state in TERMINAL_VALUES and not (fields.get("writers_stopped") or record.get("writers_stopped")):
                raise ValueError("Confirmed writer termination is required for a terminal state")
            record.update(fields)
            record["state"] = state
            if state in TERMINAL_VALUES:
                record.setdefault("finished_at", time.time())
                if record["finished_at"] is None:
                    record["finished_at"] = time.time()
            self._write(connection, record)
            if old != state:
                details = {"from": old, "to": state, "reason": fields.get("reason")}
                for name in ("reconciled_by", "resource_verification"):
                    if name in fields:
                        details[name] = fields[name]
                self._event(connection, job_id, "state", details)
            return True

    def claim_worker(self, job_id: str, token: str, pid: int, birth: float) -> bool:
        with self.transaction() as connection:
            record = self._record(connection.execute("SELECT record FROM jobs WHERE id=?", (job_id,)).fetchone())
            if record["worker_token"] != token or record["state"] not in ("STARTING", "RUNNING", "STOPPING"):
                return False
            if record.get("worker_pid") is not None:
                return record["worker_pid"] == pid and record["worker_birth"] == birth
            record.update(worker_pid=pid, worker_birth=birth, worker_heartbeat=time.time())
            self._write(connection, record)
            self._event(connection, job_id, "worker_claimed", {"pid": pid})
            return True

    def cancel_requested(self, job_id: str) -> bool:
        return bool(self.get(job_id)["cancel_requested"])

    def request_cancel(self, job_id: str) -> dict[str, Any]:
        with self.transaction() as connection:
            record = self._record(connection.execute("SELECT record FROM jobs WHERE id=?", (job_id,)).fetchone())
            if record["state"] in TERMINAL_VALUES:
                return record
            record["cancel_requested"] = True
            if record["state"] == "QUEUED":
                record.update(
                    state="CANCELLED",
                    writers_stopped=True,
                    returncode=143,
                    finished_at=time.time(),
                    reason="Cancelled before admission",
                )
            elif record["state"] != "LOST":
                record["state"] = "STOPPING"
            self._write(connection, record)
            self._event(connection, job_id, "cancel_requested", {})
            return record

    @staticmethod
    def _declared_container(record: dict[str, Any], name: str) -> dict[str, Any]:
        if not isinstance(name, str) or not name or "\x00" in name:
            raise ValueError("A declared container name is required")
        spec = record["spec"]
        configurations = list(spec.get("containers") or [])
        if spec.get("container"):
            configurations.append(spec["container"])
        matching = [configuration for configuration in configurations if configuration["name"] == name]
        if not matching or any(configuration.get("runtime", "docker") != "docker" for configuration in matching):
            raise ValueError("Only a declared Docker container may be stopped")
        if len({dump(configuration) for configuration in matching}) != 1:
            raise ValueError("The declared container name has ambiguous ownership")
        return matching[0]

    def container_stop_receipt_path(self, job_id: str, name: str) -> str:
        record = self.get(job_id)
        self._declared_container(record, name)
        token = hashlib.sha256(name.encode()).hexdigest()
        return str(self.root / "jobs" / record["id"] / ("container-stop-" + token + ".json"))

    def request_container_stop(self, job_id: str, name: str) -> dict[str, Any]:
        """Persist one declared container's stop intent without cancelling its job."""
        with self.transaction() as connection:
            record = self._record(connection.execute("SELECT record FROM jobs WHERE id=?", (job_id,)).fetchone())
            self._declared_container(record, name)
            if record["state"] in TERMINAL_VALUES:
                return record
            requests = record.setdefault("container_stop_requests", {})
            if name not in requests:
                requests[name] = {"requested_at": time.time(), "status": "requested"}
                self._write(connection, record)
                self._event(connection, job_id, "container_stop_requested", {"name": name})
            return record

    def note_container_stop(self, job_id: str, name: str, event: str, **fields: Any) -> dict[str, Any]:
        """Merge a worker observation without overwriting concurrent stop intents."""
        with self.transaction() as connection:
            record = self._record(connection.execute("SELECT record FROM jobs WHERE id=?", (job_id,)).fetchone())
            if record["state"] in TERMINAL_VALUES:
                return record
            request = record.get("container_stop_requests", {}).get(name)
            if request is None:
                raise ValueError("Container stop was not requested")
            if (
                request.get("container_id")
                and fields.get("container_id", request["container_id"]) != request["container_id"]
            ):
                raise ValueError("A stopped container's immutable identity cannot change")
            if any(request.get(key) != value for key, value in fields.items()):
                request.update(fields)
                self._write(connection, record)
                self._event(connection, job_id, event, {"name": name, **fields})
            return record

    def note_container_cleanup(self, job_id: str, name: str, **fields: Any) -> dict[str, Any]:
        """Audit verified cleanup without losing another container's evidence."""
        with self.transaction() as connection:
            record = self._record(connection.execute("SELECT record FROM jobs WHERE id=?", (job_id,)).fetchone())
            if not self._declared_container(record, name).get("remove_on_exit", False):
                raise ValueError("Container cleanup was not explicitly requested")
            item = record.setdefault("container_cleanup", {}).setdefault(name, {})
            if item.get("container_id") and fields.get("container_id", item["container_id"]) != item["container_id"]:
                raise ValueError("Container cleanup cannot change an immutable ID")
            item.update(fields, checked_at=time.time())
            self._write(connection, record)
            self._event(connection, job_id, "container_cleanup", {"name": name, **item})
            return record

    def put_meta(self, key: str, value: Any) -> None:
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO meta VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, dump(value))
            )

    def get_meta(self, key: str, default: Any = None) -> Any:
        connection = self._connect()
        try:
            row = connection.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return json.loads(row[0]) if row is not None else default
        finally:
            connection.close()

    def pools(self) -> dict[str, dict[str, Any]]:
        connection = self._connect()
        try:
            return {
                row[0]: json.loads(row[1]) for row in connection.execute("SELECT name,config FROM pools ORDER BY name")
            }
        finally:
            connection.close()

    def pool(self, name: str) -> dict[str, Any]:
        pools = self.pools()
        if name not in pools:
            raise KeyError(f"Unknown execution pool {name!r}")
        return pools[name]

    def ensure_pool(self, name: str, config: PoolConfig, *, update: bool = False) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("Pool names must be nonempty strings")
        serialized = dump(config.to_dict())
        with self.transaction() as connection:
            existing = connection.execute("SELECT config FROM pools WHERE name=?", (name,)).fetchone()
            if existing is not None and existing[0] != serialized and not update:
                raise ValueError(f"Conflicting configuration for shared pool {name!r}")
            connection.execute(
                "INSERT INTO pools VALUES (?,?) ON CONFLICT(name) DO UPDATE SET config=excluded.config",
                (name, serialized),
            )

    def reserve_next(self, name: str, **execution_fields: Any) -> dict[str, Any] | None:
        with self.transaction() as connection:
            if not self.get_meta_in(connection, "admission_open", True):
                return None
            config_row = connection.execute("SELECT config FROM pools WHERE name=?", (name,)).fetchone()
            if config_row is None:
                raise KeyError(name)
            config = json.loads(config_row[0])
            active = connection.execute(
                "SELECT COUNT(*) FROM jobs WHERE pool=? AND state NOT IN ('QUEUED','SUCCEEDED','FAILED','CANCELLED')",
                (name,),
            ).fetchone()[0]
            if config["max_jobs"] and active >= config["max_jobs"]:
                return None
            row = connection.execute(
                "SELECT record FROM jobs WHERE pool=? AND state='QUEUED' ORDER BY created_at,id LIMIT 1", (name,)
            ).fetchone()
            if row is None:
                return None
            record = self._record(row)
            record.update(state="STARTING", worker_token=str(uuid.uuid4()), admitted_at=time.time(), **execution_fields)
            self._write(connection, record)
            self._event(connection, record["id"], "admitted", {})
            return record

    @staticmethod
    def get_meta_in(connection: sqlite3.Connection, key: str, default: Any) -> Any:
        row = connection.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def events(self, job_id: str) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            return [
                {"sequence": row[0], "time": row[1], "event": row[2], "details": json.loads(row[3])}
                for row in connection.execute(
                    "SELECT sequence,time,event,details FROM events WHERE job_id=? ORDER BY sequence", (job_id,)
                )
            ]
        finally:
            connection.close()

    def delete_terminal(self, job_id: str) -> None:
        with self.transaction() as connection:
            record = self._record(connection.execute("SELECT record FROM jobs WHERE id=?", (job_id,)).fetchone())
            if record["state"] not in TERMINAL_VALUES or not record.get("writers_stopped"):
                raise ValueError("Only confirmed terminal execution records may be pruned")
            # Keep a compact acceptance receipt permanently within this state
            # root. Otherwise an old retried key could execute again after a
            # seemingly harmless diagnostics prune.
            keep = (
                "id",
                "key",
                "digest",
                "state",
                "pool",
                "backend",
                "created_at",
                "started_at",
                "finished_at",
                "returncode",
                "reason",
                "writers_stopped",
            )
            receipt = {name: record.get(name) for name in keep}
            receipt.update(spec={}, pruned=True)
            connection.execute("DELETE FROM events WHERE job_id=?", (job_id,))
            self._write(connection, receipt)

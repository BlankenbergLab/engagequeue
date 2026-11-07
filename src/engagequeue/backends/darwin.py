"""macOS supervision for jobs which remain in their command process group."""

from __future__ import annotations

import ctypes
import errno
import os
import subprocess
import time
from functools import lru_cache

import psutil

from ..errors import UnsupportedBackend
from .processes import ProcessOwnershipError, alive, boot_identity, identity, signal_owned


class _Libproc:
    """Small, bounded adapter for the kernel's filtered process-group snapshot."""

    def __init__(self):
        try:
            self.library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
            self.query = self.library.proc_listpids
            self.query.argtypes = [ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_int]
            self.query.restype = ctypes.c_int
        except (AttributeError, OSError) as exc:
            raise ProcessOwnershipError(f"macOS process-group inspection is unavailable: {exc}") from exc

    def group_pids(self, pgid: int) -> list[int]:
        capacity = 256
        while capacity <= 262144:
            buffer = (ctypes.c_int * capacity)()
            size = ctypes.sizeof(buffer)
            ctypes.set_errno(0)
            # PROC_PGRP_ONLY=2. This API returns bytes, unlike the convenience
            # proc_listpgrppids wrapper, whose result is a count of PIDs.
            count = self.query(2, pgid, buffer, size)
            if count <= 0 or count > size or count % ctypes.sizeof(ctypes.c_int):
                raise ProcessOwnershipError(
                    "Cannot obtain a complete macOS process-group snapshot "
                    f"(returned {count} bytes, capacity {size} bytes, errno {ctypes.get_errno()})"
                )
            if count == size:
                capacity *= 2
                continue
            values = list(buffer[: count // ctypes.sizeof(ctypes.c_int)])
            if any(value <= 0 for value in values) or len(set(values)) != len(values):
                raise ProcessOwnershipError("Malformed macOS process-group snapshot")
            return values
        raise ProcessOwnershipError("macOS process-group snapshot remained truncated")


@lru_cache(maxsize=1)
def _libproc() -> _Libproc:
    return _Libproc()


def require_available() -> None:
    try:
        if os.getpid() not in _libproc().group_pids(os.getpgrp()):
            raise ProcessOwnershipError("The macOS process-group snapshot omitted its live owner")
    except (OSError, ProcessOwnershipError) as exc:
        raise UnsupportedBackend("macOS native execution requires working process-group inspection") from exc


def _group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        raise ProcessOwnershipError(f"Cannot verify macOS process-group quiescence: {exc}") from exc


def wait_for_exit_grant(store, job_id: str, token: str, command: dict) -> bool:
    """Retain the recorder as the group's identity anchor after payload exit."""
    while True:
        current = store.get(job_id)
        if (
            current["worker_token"] != token
            or current.get("worker_boot") != boot_identity()
            or current.get("command_pid") != command["pid"]
            or current.get("command_birth") != command["birth"]
        ):
            return False
        if current.get("command_exit_granted") and current.get("execution_finished") is not None:
            return True
        time.sleep(0.05)


class DarwinLauncher:
    def __init__(self, store, job_id: str, token: str):
        self.store = store
        self.job_id = job_id
        self.token = token
        self.process = None
        self.pgid = None
        self.anchor = None
        self.known = []
        self.release_requested = False

    def start(self, argv):
        require_available()
        # Popen is the final fallible operation here. Identity registration and
        # metadata writes happen only once the caller knows a child was launched.
        self.process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, start_new_session=True, close_fds=True)
        self.pgid = self.process.pid

    def _current(self):
        current = self.store.get(self.job_id)
        if current["worker_token"] != self.token or current.get("worker_boot") != boot_identity():
            raise ProcessOwnershipError("The macOS supervisor's durable ownership changed")
        return current

    def _verify_anchor(self):
        try:
            status = alive(self.anchor)
            if status is not True:
                raise ProcessOwnershipError("The macOS process-group identity anchor is unavailable")
            process = psutil.Process(self.anchor["pid"])
            if process.uids().real != os.getuid() or os.getpgid(self.anchor["pid"]) != self.pgid:
                raise ProcessOwnershipError("The macOS process-group identity anchor changed")
        except (OSError, psutil.Error) as exc:
            raise ProcessOwnershipError(f"Cannot verify the macOS process-group identity anchor: {exc}") from exc

    def _exit_authorized(self, current):
        return (
            self.release_requested
            and current.get("command_exit_granted")
            and current.get("execution_finished") is not None
            and self.anchor is not None
            and current.get("command_pid") == self.anchor["pid"]
            and current.get("command_birth") == self.anchor["birth"]
        )

    def _after_anchor_exit(self, current):
        if self.process.poll() is None:
            raise ProcessOwnershipError("Cannot confirm macOS recorder termination")
        may_have_started = current.get("execution_granted") or current.get("command_started")
        if may_have_started and not self._exit_authorized(current):
            raise ProcessOwnershipError("The macOS process-group anchor exited without its durable release grant")
        deadline = time.monotonic() + 1.0
        while _group_exists(self.pgid):
            # Kernel group cleanup may lag recorder reaping. Retain capacity
            # during a bounded grace period, without adopting or signalling a
            # group whose identity anchor is gone. Only ESRCH proves the end.
            if not self._exit_authorized(self._current()) or time.monotonic() >= deadline:
                raise ProcessOwnershipError("The macOS process group remains populated after recorder exit")
            time.sleep(0.05)
        return []

    def processes(self):
        if self.process is None:
            return []
        current = self._current()
        if self.process.poll() is not None:
            return self._after_anchor_exit(current)
        if self._exit_authorized(current) and self.known == [self.anchor]:
            # After its durable release the recorder cannot launch more work.
            # Its unreaped Popen child still anchors the PID; retain capacity
            # without querying a process whose kernel metadata is disappearing.
            # Reaping plus the kernel group probe below remain mandatory.
            return [self.anchor]
        try:
            if self.anchor is None:
                self.anchor = identity(self.process.pid)
                self.store.update(
                    self.job_id,
                    backend_info={"strategy": "darwin-process-group", "pgid": self.pgid, "anchor": self.anchor},
                )
            # The live unreaped Popen child anchors this group ID against reuse.
            self._verify_anchor()
            for previous in self.known:
                status = alive(previous)
                if status is None:
                    raise ProcessOwnershipError("Cannot verify a previously observed macOS group member")
                if status:
                    try:
                        group = os.getpgid(previous["pid"])
                    except ProcessLookupError:
                        continue
                    if group != self.pgid and alive(previous) is not False:
                        raise ProcessOwnershipError("An observed command process escaped its macOS process group")
            pids = _libproc().group_pids(self.pgid)
            if self.anchor["pid"] not in pids:
                raise ProcessOwnershipError("The macOS process-group snapshot omitted its live anchor")
            result = []
            for pid in pids:
                try:
                    member = identity(pid)
                    status = alive(member)
                    if status is None:
                        raise ProcessOwnershipError("Cannot verify a macOS process-group member")
                    if not status:
                        continue
                    if psutil.Process(pid).uids().real != os.getuid() or os.getpgid(pid) != self.pgid:
                        raise ProcessOwnershipError("A macOS process-group member's ownership changed")
                    result.append(member)
                except (ProcessLookupError, psutil.NoSuchProcess):
                    continue
            self._verify_anchor()
        except (OSError, ProcessOwnershipError, psutil.Error) as exc:
            if self.process.poll() is not None:
                return self._after_anchor_exit(current)
            raise ProcessOwnershipError(
                f"Cannot inspect the owned macOS process group: {type(exc).__name__}: {exc}"
            ) from exc
        if not any(member == self.anchor for member in result):
            if self.process.poll() is not None:
                return self._after_anchor_exit(current)
            raise ProcessOwnershipError("The macOS process-group anchor was not verified in its snapshot")
        self.known = result
        if (
            not self.release_requested
            and current.get("execution_finished") is not None
            and current.get("command_pid") == self.anchor["pid"]
            and current.get("command_birth") == self.anchor["birth"]
            and len(result) == 1
        ):
            self.store.update(self.job_id, command_exit_granted=True)
            self.release_requested = True
        return result

    def signal(self, signum):
        members = self.processes()
        current = self._current()
        for member in reversed(members):
            if member == self.anchor:
                # Before the execution grant, TERM interrupts the private
                # recorder's gate. Once execution could start, keep it alive
                # until all other writers have stopped and its release is durable.
                if self.release_requested or current.get("execution_granted") or current.get("command_started"):
                    continue
            self._verify_anchor()
            try:
                if os.getpgid(member["pid"]) != self.pgid:
                    raise ProcessOwnershipError("A macOS signal target left its owned process group")
            except ProcessLookupError:
                continue
            if not signal_owned(member, signum) and alive(member) is not False:
                raise ProcessOwnershipError("Cannot signal a verified macOS process-group member")

    def show(self):
        return {}

    def quiescent(self):
        if self.process is None:
            return True
        if self.process.poll() is None:
            return False
        self._after_anchor_exit(self._current())
        return True

    def cleanup(self):
        if self.process is not None:
            self.process.poll()

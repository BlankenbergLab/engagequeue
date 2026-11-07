"""Process ownership checks and Linux descendant-containment primitives."""

from __future__ import annotations

import ctypes
import os
import signal

import psutil

from .. import _platform
from ..errors import UnsupportedBackend


class ProcessOwnershipError(RuntimeError):
    """Native process containment can no longer be verified."""


def boot_identity() -> dict[str, str]:
    """Use boot and PID namespace, not a mutable hostname, to qualify PIDs."""
    return _platform.ownership_identity()


def identity(pid: int) -> dict[str, float | int]:
    process = psutil.Process(pid)
    return {"pid": process.pid, "birth": process.create_time()}


def alive(record: dict | None) -> bool | None:
    if not record:
        return False
    try:
        process = psutil.Process(int(record["pid"]))
        if abs(process.create_time() - float(record["birth"])) > 0.005:
            return False
        return process.status() not in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD)
    except psutil.NoSuchProcess:
        return False
    except (psutil.AccessDenied, KeyError, TypeError, ValueError):
        return None


def signal_owned(record: dict, signum: int) -> bool:
    """A reused PID never grants authority to signal its new process."""
    if alive(record) is not True:
        return False
    try:
        process = psutil.Process(int(record["pid"]))
        if process.uids().real != os.getuid():
            return False
        # Recheck immediately before signalling. Linux pidfds close the
        # remaining identity-check/signal race when available.
        pidfd = None
        try:
            if hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"):
                pidfd = os.pidfd_open(process.pid)
                if alive(record) is not True:
                    return False
                signal.pidfd_send_signal(pidfd, signum)
            else:
                if alive(record) is not True:
                    return False
                process.send_signal(signum)
            return True
        finally:
            if pidfd is not None:
                os.close(pidfd)
    except (ProcessLookupError, psutil.NoSuchProcess):
        return False


def become_subreaper() -> None:
    """Adopt double-forked descendants even after they create new sessions."""
    if _platform.system_name() != "linux":
        raise UnsupportedBackend("Child subreaping is available only on Linux")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # Linux PR_SET_CHILD_SUBREAPER
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def descendants() -> list[dict]:
    records = []
    for process in psutil.Process().children(recursive=True):
        try:
            record = identity(process.pid)
            if alive(record):
                records.append(record)
        except psutil.NoSuchProcess:
            pass
    return records


def rss_mb(records: list[dict]) -> float:
    total = 0
    for record in records:
        try:
            if alive(record):
                total += psutil.Process(int(record["pid"])).memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return total / (1024 * 1024)


def reap() -> None:
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
            if pid == 0:
                return
        except ChildProcessError:
            return


def children_exist() -> bool:
    """Ask the kernel whether any child still exists after reaping zombies.

    A /proc descendant snapshot is not proof of quiescence: a process can fork
    and exit while that snapshot is being read. As a subreaper, absence of all
    waitable/live children is the definitive end of our native process tree.
    """
    reap()
    try:
        os.waitid(os.P_ALL, 0, os.WEXITED | os.WNOHANG | os.WNOWAIT)
    except ChildProcessError:
        return False
    return True

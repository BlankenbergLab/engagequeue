"""Host ownership identity shared by clients, schedulers, and supervisors."""

from __future__ import annotations

import ctypes
import os
import sys
import uuid
from functools import lru_cache
from pathlib import Path

import psutil

from .errors import RuntimeUnavailable, UnsupportedBackend


def system_name() -> str:
    return sys.platform


@lru_cache(maxsize=1)
def _darwin_boot_id() -> str:
    # Wall-clock-derived boot times can move when the clock is adjusted. The
    # kernel's boot-session UUID remains stable for this process's lifetime.
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        query = libc.sysctlbyname
        query.argtypes = [
            ctypes.c_char_p,
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.c_void_p,
            ctypes.c_size_t,
        ]
        query.restype = ctypes.c_int
        buffer = ctypes.create_string_buffer(128)
        size = ctypes.c_size_t(len(buffer))
        if query(b"kern.bootsessionuuid", buffer, ctypes.byref(size), None, 0) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        identifier = str(uuid.UUID(buffer.value.decode("ascii")))
    except (AttributeError, OSError, UnicodeError, ValueError) as exc:
        raise RuntimeUnavailable("Cannot verify the macOS kernel boot-session identity") from exc
    return "darwin:" + identifier


def boot_id() -> str:
    if system_name() == "darwin":
        return _darwin_boot_id()
    if system_name() == "linux":
        try:
            return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        except OSError:
            # Retain the Linux client's existing fallback and persisted format.
            return str(psutil.boot_time())
    raise UnsupportedBackend("EngageQueue execution supports Linux and macOS")


def pid_namespace() -> int:
    if system_name() == "darwin":
        # macOS has no Linux PID namespaces. A fixed host-domain marker is
        # qualified by the Darwin boot-session identity rather than a hostname.
        return 0
    if system_name() == "linux":
        return Path("/proc/self/ns/pid").stat().st_ino
    raise UnsupportedBackend("EngageQueue execution supports Linux and macOS")


def ownership_identity() -> dict[str, str]:
    return {"boot_id": boot_id(), "pid_namespace": str(pid_namespace())}


def require_native() -> None:
    if system_name() == "linux":
        return
    if system_name() == "darwin":
        from .backends.darwin import require_available

        boot_id()
        require_available()
        return
    raise UnsupportedBackend("EngageQueue native execution supports Linux and macOS")

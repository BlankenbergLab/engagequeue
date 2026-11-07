"""Private Linux container keeper; scheduling remains explicitly controlled."""

from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from pathlib import Path


def initialize_volume(state_dir: str, uid: int, gid: int) -> None:
    if os.geteuid() != 0:
        raise PermissionError("Managed volume initialization requires the container's root user")
    if uid < 0 or gid < 0:
        raise ValueError("Managed user and group IDs must be nonnegative")
    directory = Path(state_dir)
    if not directory.is_absolute() or directory.resolve() == Path("/"):
        raise ValueError("A private absolute state directory is required")
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    os.chown(directory, uid, gid)


def keepalive() -> None:
    if sys.platform != "linux":
        raise RuntimeError("The managed runtime keeper requires Linux")
    home = Path(os.environ.get("HOME", "/tmp/engagequeue"))
    home.mkdir(mode=0o700, parents=True, exist_ok=True)
    if home.stat().st_uid != os.getuid():
        raise PermissionError("The managed runtime home must belong to its user")
    home.chmod(0o700)
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    previous = {number: signal.signal(number, stop) for number in (signal.SIGTERM, signal.SIGINT)}
    try:
        while not stopping:
            # Docker exec children can become PID 1's children after their
            # client exits. Reap them when no separate init process is present.
            while True:
                try:
                    child, _ = os.waitpid(-1, os.WNOHANG)
                except ChildProcessError:
                    break
                if child == 0:
                    break
            time.sleep(0.5)
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("keepalive")
    initialize = commands.add_parser("initialize-volume")
    initialize.add_argument("--state-dir", required=True)
    initialize.add_argument("--uid", required=True, type=int)
    initialize.add_argument("--gid", required=True, type=int)
    args = parser.parse_args(argv)
    os.umask(0o077)
    if args.command == "initialize-volume":
        initialize_volume(args.state_dir, args.uid, args.gid)
    else:
        keepalive()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

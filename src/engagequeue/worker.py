"""Private executable entrypoint for detached supervisors and gated commands."""

from __future__ import annotations

import argparse
import sys


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--child", action="store_true")
    args = parser.parse_args(argv)
    if args.child:
        from .backends.native import child

        return child(args.state_dir, args.job_id, args.token)
    from .store import Store

    store = Store(args.state_dir)
    backend = store.get(args.job_id)["backend"]
    if backend == "systemd":
        from .backends.systemd import run_job
    elif backend == "native":
        from .backends.native import run_job
    else:
        raise ValueError(f"Unknown execution backend {backend!r}")
    return run_job(args.state_dir, args.job_id, args.token)


if __name__ == "__main__":
    sys.exit(main())

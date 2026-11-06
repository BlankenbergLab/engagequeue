"""Command-line frontend for the public EngageQueue Python API."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from . import ContainerSpec, Executor, JobSpec, PoolConfig
from .errors import EngageQueueError


def _emit(value, *, structured=False):
    if hasattr(value, "to_dict"):
        value = value.to_dict()
    elif isinstance(value, list):
        value = [item.to_dict() if hasattr(item, "to_dict") else item for item in value]
    if structured or not isinstance(value, (str, int, float)):
        print(json.dumps(value, sort_keys=True))
    else:
        print(value)


def parser():
    result = argparse.ArgumentParser(description="Local jobs. Make it so.")
    result.add_argument("--version", action="version", version="engagequeue 0.1.0")
    result.add_argument("--state-dir", default=os.environ.get("ENGAGEQUEUE_STATE_DIR"))
    result.add_argument(
        "--attach-only", action="store_true", help="Attach to an externally managed runtime without starting processes"
    )
    result.add_argument("--runtime-python", help="Python interpreter used by the independent runtime")
    commands = result.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init", help="Create or attach to a configured execution pool")
    init.add_argument("--pool", default="default")
    init.add_argument("--max-jobs", default="1", help="Positive cap, 0 for unbounded, or '*' for CPU count")
    init.add_argument("--backend", choices=("native", "systemd"), default="native")
    init.add_argument("--cancel-grace", type=float, default=10)
    submit = commands.add_parser("submit", help="Submit an argument vector without an implicit shell")
    submit.add_argument("--key", help="Stable idempotency key; identical retries return the same job")
    submit.add_argument("--pool", default="default")
    submit.add_argument("--cwd", default=os.getcwd())
    submit.add_argument("--stdout")
    submit.add_argument("--stderr")
    submit.add_argument("--env", action="append", default=[], metavar="NAME=VALUE")
    submit.add_argument("--timeout", type=float)
    submit.add_argument("--output-limit", type=int, metavar="BYTES")
    submit.add_argument("--output-path", action="append", default=[])
    submit.add_argument("--memory-mb", type=int)
    submit.add_argument("--cpus", type=int)
    submit.add_argument("--require-hard-limits", action="store_true")
    submit.add_argument("--container-spec", help="JSON file describing an owned Docker container")
    submit.add_argument("argv", nargs=argparse.REMAINDER)
    for name in ("status", "wait", "cancel", "inspect", "reconcile"):
        command = commands.add_parser(name)
        command.add_argument("job_id")
        if name in ("wait", "cancel"):
            command.add_argument("--timeout", type=float)
        if name == "cancel":
            command.add_argument(
                "--wait", action="store_true", help="Wait for confirmed termination of all owned writers"
            )
        if name == "reconcile":
            command.add_argument("--confirm-stopped", action="store_true", required=True)
            command.add_argument("--reason", required=True)
    stop_container = commands.add_parser(
        "stop-container", help="Gracefully stop one owned Docker container while its accepted command continues"
    )
    stop_container.add_argument("job_id")
    stop_container.add_argument("name", help="Container name declared in the accepted job specification")
    lookup = commands.add_parser("lookup", help="Find a job by its submission key")
    lookup.add_argument("key")
    listing = commands.add_parser("list")
    listing.add_argument("--pool")
    pruning = commands.add_parser("prune", help="Remove terminal executor records, preserving caller workspaces")
    pruning.add_argument("--job-id")
    pruning.add_argument("--before", type=float, help="Only records finished before this Unix timestamp")
    runtime = commands.add_parser("runtime", help="Explicit runtime lifecycle controls")
    runtime.add_argument("action", choices=("serve", "drain", "resume", "shutdown"))
    runtime.add_argument("--wait", action="store_true")
    runtime.add_argument("--timeout", type=float)
    for command in commands.choices.values():
        command.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    return result


def main(argv=None):
    command_parser = parser()
    args = command_parser.parse_args(argv)
    if not args.state_dir:
        command_parser.error("provide --state-dir or ENGAGEQUEUE_STATE_DIR")
    try:
        pools = None
        if args.command == "init":
            pools = {args.pool: PoolConfig(args.max_jobs, args.backend, args.cancel_grace)}
        if args.command == "runtime" and args.action == "serve":
            from .runtime import serve

            return serve(args.state_dir)
        with Executor(
            args.state_dir, pools=pools, runtime_python=args.runtime_python, attach_only=args.attach_only
        ) as executor:
            if args.command == "init":
                value = {
                    "state_dir": str(Path(args.state_dir).resolve()),
                    "pool": args.pool,
                    **pools[args.pool].to_dict(),
                }
            elif args.command == "submit":
                arguments = args.argv[1:] if args.argv and args.argv[0] == "--" else args.argv
                if not arguments:
                    raise ValueError("submit requires an executable and arguments after '--'")
                environment_overrides = None
                if args.env:
                    environment_overrides = {}
                    for item in args.env:
                        name, separator, contents = item.partition("=")
                        if not separator:
                            raise ValueError("--env requires NAME=VALUE")
                        environment_overrides[name] = contents
                containers = ()
                if args.container_spec:
                    descriptions = json.loads(Path(args.container_spec).read_text())
                    if not isinstance(descriptions, list):
                        descriptions = [descriptions]
                    containers = tuple(ContainerSpec(**item) for item in descriptions)
                job = executor.submit(
                    JobSpec(
                        argv=arguments,
                        cwd=args.cwd,
                        env_overrides=environment_overrides,
                        stdout=args.stdout,
                        stderr=args.stderr,
                        pool=args.pool,
                        timeout=args.timeout,
                        output_limit=args.output_limit,
                        output_paths=args.output_path,
                        memory_mb=args.memory_mb,
                        cpus=args.cpus,
                        require_hard_limits=args.require_hard_limits,
                        containers=containers,
                    ),
                    key=args.key,
                )
                value = job.to_dict() if args.json else job.id
            elif args.command == "lookup":
                job = executor.lookup(args.key)
                if job is None:
                    print("engagequeue: no execution has this submission key", file=sys.stderr)
                    return 3
                value = job.to_dict() if args.json else job.id
            elif args.command == "status":
                value = executor.status(args.job_id)
            elif args.command == "wait":
                value = executor.wait(args.job_id, timeout=args.timeout)
                _emit(value, structured=args.json)
                return 0 if value.state.value == "SUCCEEDED" else 1
            elif args.command == "cancel":
                value = executor.cancel(args.job_id, wait=args.wait, timeout=args.timeout)
            elif args.command == "stop-container":
                value = executor.stop_container(args.job_id, args.name)
            elif args.command == "list":
                value = executor.list(pool=args.pool)
            elif args.command == "inspect":
                value = executor.inspect(args.job_id)
            elif args.command == "reconcile":
                value = executor.reconcile(args.job_id, confirm_stopped=args.confirm_stopped, reason=args.reason)
            elif args.command == "prune":
                value = executor.prune(job_id=args.job_id, before=args.before)
            elif args.command == "runtime":
                if args.action == "drain":
                    value = executor.drain()
                elif args.action == "resume":
                    value = executor.resume()
                else:
                    value = executor.shutdown(wait=args.wait, timeout=args.timeout)
            else:
                raise ValueError("Unknown command")
            _emit(value, structured=args.json)
        return 0
    except TimeoutError as exc:
        print(f"engagequeue: {exc}", file=sys.stderr)
        return 124
    except (EngageQueueError, ValueError, KeyError, OSError) as exc:
        print(f"engagequeue: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

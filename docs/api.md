# Python API and CLI

The CLI is a frontend for the importable Python API. `Executor` and explicit
`--execution-mode local` run on the local host. The CLI recognizes a saved
managed profile automatically; a state directory without one defaults to local
execution:

```python
from engagequeue import Executor, JobSpec, PoolConfig

with Executor('/ssd/engagequeue', pools={'batch': PoolConfig(max_jobs=4)}) as executor:
    job = executor.submit(
        JobSpec(argv=['bash', '/work/job.sh'], cwd='/work', pool='batch'),
        key='application:job:attempt',
    )
    status = executor.status(job.id)
```

Linux native execution includes subreaper ownership of daemonized descendants.
On macOS, local jobs must remain foreground commands with descendants in their
original process group. Use managed Docker execution for jobs that daemonize or
need the Linux supervision contract. The systemd backend is Linux-only.

## Managed Docker execution

`DockerExecutor` exposes the same submission and job-control methods while
placing the native Linux runtime in a managed Docker container. Execution mode
is separate from `PoolConfig.backend`; managed runtime pools use `native`.

```python
from pathlib import Path
from engagequeue import DockerExecutor, JobSpec

workspace = Path('/Users/me/project').resolve()
wheel = workspace / 'dist/engagequeue-0.1.1-py3-none-any.whl'

with DockerExecutor(workspace / '.engagequeue-runtime') as executor:
    executor.initialize(workspaces=[str(workspace)], wheel=str(wheel))
    job = executor.submit(
        JobSpec(argv=['sh', '-c', 'printf "managed execution\\n"'], cwd=str(workspace)),
        key='managed:example:1',
    )
    result = executor.wait(job.id, timeout=120)
```

`initialize(*, workspaces=None, image=None, wheel=None, docker_context=None,
docker_jobs=None)` prepares and starts the runtime. Supply an existing runtime
image with `image`, or a built wheel with `wheel` to build one. Workspace roots
are required on first initialization and are mounted at the same absolute paths
inside the runtime. Omitted workspace and sibling-Docker settings retain an
existing profile; a new profile defaults to sibling Docker access disabled.
Commands use Linux executables and the runtime image's environment; a Mac
interpreter or host Conda environment is not executable inside Linux.

The constructor accepts `pools=None`, `poll_interval=0.2`, and
`attach_only=False`. A later client uses the saved profile without initializing
it again. `close()` and context-manager exit disconnect that client;
`start()` starts or resumes admission, `stop(timeout=120)` drains running work
and shuts down the owned runtime while retaining its state volume and queued
work, and `runtime_info()` reports the managed runtime profile and status.
Change an initialized profile only after a clean stop.

Use `docker_context` to select a Docker context. Enable `docker_jobs=True` when
workload commands need sibling Docker containers; this gives the runtime access
to the Docker daemon. Continue to declare each owned workload container with
`ContainerSpec` for lifecycle verification.

The equivalent CLI is explicit about placement:

```sh
engagequeue --execution-mode docker --state-dir "$PWD/.engagequeue-runtime" init \
  --workspace "$PWD" --runtime-wheel "$PWD/dist/engagequeue-0.1.1-py3-none-any.whl"
engagequeue --execution-mode docker --state-dir "$PWD/.engagequeue-runtime" submit \
  --key managed:example:1 --cwd "$PWD" -- sh job.sh
engagequeue --execution-mode docker --state-dir "$PWD/.engagequeue-runtime" runtime info --json
engagequeue --execution-mode docker --state-dir "$PWD/.engagequeue-runtime" runtime stop --timeout 120
engagequeue --execution-mode docker --state-dir "$PWD/.engagequeue-runtime" runtime start
```

Repeat `--workspace` for multiple roots. `--image` is the alternative to
`--runtime-wheel`; initialization also accepts `--docker-context` and
`--docker-jobs`. New jobs' working directories, outputs, and container CID files
must be under a declared workspace or the runtime's per-job artifact directory.
Other runtime state paths are reserved. Use `DockerExecutor` or the saved
managed profile consistently for that state directory; explicit local execution
rejects a managed state directory.

## Submission and job control

`submit` accepts argument vectors, an explicit working directory, environment,
stdout/stderr paths, pool, timeout, output-size policy, and optional resource
requirements. Conda installation, image selection, file staging and application
metadata stay with the caller. Prepared scripts may activate their own Conda
environment or run Apptainer.

Each logical execution attempt should have a stable, unique key. An identical
retry returns the same execution. A conflicting specification raises
`SubmissionConflict`. Re-execution requires a new attempt key. Pruning keeps
compact terminal receipts so a delayed retry cannot accidentally execute again.
An identical terminal retry also adopts its receipt after its working directory
has been removed, without requiring a running coordinator. Working-directory,
pool-capability, and runtime-readiness checks apply when accepting new work.

`lookup`, `status`, `list`, `wait`, `cancel`, and `inspect` provide job control.
`close()` and context-manager exit release the client only. `cancel(wait=True)`
waits for confirmed termination; `STOPPING` is not terminal. `LOST` requires
reconciliation and does not authorize output cleanup.

`stop_container(job_id, name)` records a graceful stop request for one Docker
container declared in the accepted specification and immediately returns the
current `JobStatus`. Its supervisor verifies container ownership before acting.
The accepted command continues so a wrapper can finish metadata and output
processing after its service container exits. The request remains durable when
its client disconnects; use `wait(job_id)` to observe eventual job completion.
Terminal executions retain their existing result. Unknown jobs and undeclared
or ambiguous container names are rejected.

```python
with Executor('/ssd/engagequeue') as executor:
    executor.stop_container('JOB_ID', 'owned-interactive-service')
    result = executor.wait('JOB_ID', timeout=120)
```

`container_stop_receipt_path(job_id, name)` returns a deterministic private
receipt path without starting a runtime or creating a file. The supervisor
writes it only after a verified signal RPC succeeds, recording execution and
container ownership, the issued signal, and any observed exit/OOM state.
Auto-removal can leave final exit/OOM fields unknown. Applications own exit
normalization: a matching TERM receipt can explain exit 143; exit 137 requires
verified KILL, a matching observed exit, and known `oom_killed: false`. Preserve
other failures and reject receipts marked with whole-job cancellation.

CLI examples:

```sh
engagequeue --state-dir /ssd/engagequeue status JOB_ID --json
engagequeue --state-dir /ssd/engagequeue lookup application:job:attempt
engagequeue --state-dir /ssd/engagequeue cancel JOB_ID --wait --timeout 120
engagequeue --state-dir /ssd/engagequeue stop-container JOB_ID owned-interactive-service --json
engagequeue --state-dir /ssd/engagequeue inspect JOB_ID --json
```

Use `--env NAME=VALUE` to override inherited environment variables. In the Python
API, pass `JobSpec(..., env_overrides={"NAME": "VALUE"})` for the same behavior.
Inherited variables are captured once when a new execution is accepted; changes
to the submitting client's ambient environment do not change an identical retry.
Changing an explicitly requested override raises `SubmissionConflict` for an
already accepted key. Repeated CLI overrides of one name use the last value.

API-supplied `env` mappings are complete explicit environments and do not inherit
the client's variables; `env={}` supplies an empty environment. A nonempty
`env_overrides` mapping cannot be combined with an explicit `env`. Inspection
redacts the resolved environment values. State is private to its owning account.

`ContainerSpec` describes owned Docker containers separately from the Docker
launcher process. Callers supply a unique name and ownership label matching
their prepared command; optional CID files pin the container identity. Both
single and multiple owned containers are supported. An arbitrary unlabelled
container is never adopted just because it has a familiar name.

For evidence-sensitive services, run Docker with automatic removal disabled
and declare `ContainerSpec(..., remove_on_exit=True)`. The supervisor retains the
owned container while the command and its metadata finish, captures final
exit/OOM evidence, and removes it only after verified command/container
quiescence. Ownership and immutable identity must still match. Cleanup is
opt-in; default `False` is omitted from direct and nested serialized specs, so
new clients preserve legacy accepted digests and submission-key adoption.
Changing cleanup policy on an accepted key raises `SubmissionConflict`.

For an externally managed runtime, `Executor(state_dir, attach_only=True)`
attaches without launching processes. The corresponding CLI flag is
`--attach-only`. This supports containerized clients sharing aligned state and
execution paths with a host runtime.

# Installation

EngageQueue supports Linux and macOS with Python 3.10 or newer. It uses persistent
local state and needs no database server. Choose the runtime placement for your
workload:

| Execution mode | Host | Process ownership | Requirements |
| --- | --- | --- | --- |
| Local native | Linux | Commands and descendants, including daemonized children | Python environment |
| Local native | macOS | Foreground commands and their original process groups | Python environment |
| Local systemd | Linux | Independently owned user units and cgroups | Working systemd user manager |
| Managed Docker | Linux or macOS | Linux native supervision inside an owned runtime container | Docker daemon; Docker Desktop on macOS |

Keep macOS local jobs in the foreground and keep their descendants in the job's
process group. Use managed Docker mode for daemonizing or double-forking jobs
that need Linux subreaper ownership. Systemd is optional and Linux-only.

## Python environment

Install Miniforge for your platform and architecture, then create this project's
environment. On a Mac, use the native Apple Silicon or Intel installer rather
than a different architecture's Python.

```sh
~/miniforge3/bin/conda create -p .conda/engagequeue -c conda-forge python=3.12 pip
.conda/engagequeue/bin/python -m pip install engagequeue
```

For development from a repository checkout, install the source and validation
tools in that environment:

```sh
.conda/engagequeue/bin/python -m pip install -e '.[dev]'
.conda/engagequeue/bin/python -m pytest
```

For an application embedding local execution, install EngageQueue into that
application's own environment. The local runtime uses its recorded Python
interpreter, which must also contain EngageQueue. Other clients can attach from
compatible environments without replacing that interpreter.

## Local execution

Initialize state on a local filesystem and submit a prepared command:

```sh
.conda/engagequeue/bin/engagequeue --state-dir "$PWD/.engagequeue-state" init --max-jobs 4
.conda/engagequeue/bin/engagequeue --state-dir "$PWD/.engagequeue-state" submit \
  --key example:1 --cwd "$PWD" -- sh job.sh
```

An ordinary state directory defaults to local execution; a saved managed Docker
profile is recognized automatically. Use `--execution-mode local` or `docker`
to select a mode explicitly. On Linux, use `--backend systemd` during
pool initialization to select systemd user services. Explicit systemd execution
on an unsupported host fails; it does not silently switch backends. Hard CPU or
memory limits require a capable backend; native process monitoring is not
kernel cgroup enforcement.

## Managed Docker execution

Start Docker Desktop on macOS, or a Docker daemon on Linux. Build the runtime's
wheel from the checkout:

```sh
.conda/engagequeue/bin/python -m build --wheel --no-isolation
.conda/engagequeue/bin/engagequeue --execution-mode docker \
  --state-dir "$PWD/.engagequeue-runtime" init --workspace "$PWD" \
  --runtime-wheel "$PWD/dist/engagequeue-0.1.1-py3-none-any.whl"
.conda/engagequeue/bin/engagequeue --execution-mode docker \
  --state-dir "$PWD/.engagequeue-runtime" submit --key managed:example:1 \
  --cwd "$PWD" -- sh job.sh
```

An existing runtime image can be supplied with `--image` instead of
`--runtime-wheel`. It must contain the compatible EngageQueue runtime and any
Linux commands and dependencies needed by the jobs. Repeat `--workspace` to
declare additional shared roots, and make those directories available to
Docker Desktop's file sharing. Mac host executables and Conda environments do
not become Linux executables through a bind mount.

Runtime state lives in a persistent Docker volume; the host profile and
per-job artifacts live in the supplied state directory. Closing a client leaves
the runtime running. See [operations](operations.md) for start/stop and recovery,
and [the API and CLI](api.md) for `DockerExecutor` and runtime configuration.

## Build distributions

```sh
.conda/engagequeue/bin/python -m build --no-isolation
```

Both the wheel and source archive include the managed runtime image resources.

# Installation

EngageQueue supports Linux and Python 3.10 or newer. It uses a persistent local
state directory and needs no database server. The native backend requires no
systemd. Docker and Apptainer are needed only for workloads using those runtimes.

Create a project-local environment using Miniforge and install a published release:

```sh
~/miniforge3/bin/conda create -p .conda/engagequeue -c conda-forge python=3.12 pip
.conda/engagequeue/bin/python -m pip install engagequeue
```

For development from a repository checkout, install the source and validation tools:

```sh
.conda/engagequeue/bin/python -m pip install -e '.[dev]'
.conda/engagequeue/bin/python -m pytest
```

For an application embedding EngageQueue, install the wheel into that
application's own environment. The executor runtime uses its recorded Python
interpreter, which must also contain EngageQueue. Other clients can attach from
compatible environments without replacing that interpreter.

Initialize persistent execution state on a local filesystem:

```sh
engagequeue --state-dir /ssd/engagequeue init --max-jobs 4
engagequeue --state-dir /ssd/engagequeue submit --key example:1 -- bash job.sh
```

Use `--backend systemd` during pool initialization to select systemd user
services. The selected user must have a working user service manager and the
requested resource controllers. Explicitly requested systemd execution fails
if unavailable; it does not silently use the native backend.

Build distributions with the project's own interpreter:

```sh
.conda/engagequeue/bin/python -m build
```

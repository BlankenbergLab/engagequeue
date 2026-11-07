# Validation

Run tests and checks with this project's local environment:

```sh
.conda/engagequeue/bin/ruff check src tests scripts
.conda/engagequeue/bin/ruff format --check src tests scripts
.conda/engagequeue/bin/python -m pytest -q
```

The CI matrix covers Python 3.10, 3.12 and 3.14 on Linux and on both Intel and
Apple Silicon macOS runners. Each job uses a project-local environment. Tests execute real local
commands and independent schedulers, including client death, queued admission,
submission receipts, cancellation, fork-safe embedding and recovery.
Regression coverage includes concurrent first-time database initialization,
job-specific launch failures, retries after workspace cleanup, and environment
overrides across client restarts. Deterministic Docker/systemd ownership and
reconciliation tests use controlled runtime responses without requiring live
services. Linux subreaper and kernel-child ownership assertions stay Linux-only;
macOS runs the native foreground/process-group contract and platform-specific
tests. Managed Docker end-to-end jobs run separately on Linux AMD64 and ARM64,
using the wheel built from that checkout.

Some integration tests need a real runtime. Live systemd tests skip when
`systemctl`, `systemd-run`, or a functioning user manager is unavailable, including
in minimal CI containers. Systemd is not required for the native test suite.
Docker tests are opt-in and require an installed client, a reachable daemon, and
an already-present local image; unavailable prerequisites cause a skip:

```sh
ENGAGEQUEUE_TEST_DOCKER_IMAGE=alpine:latest \
  .conda/engagequeue/bin/python -m pytest -q tests/test_docker_live.py
```

The Docker cases deliberately launch containers detached from their CLI,
verify that execution capacity remains occupied, then check confirmed
termination under both native and systemd supervision. They only manage
containers bearing their unique test name and ownership label.

Managed-runtime lifecycle tests are a separate opt-in suite. Build a current
wheel first, then run:

```sh
.conda/engagequeue/bin/python -m build --wheel --no-isolation
ENGAGEQUEUE_TEST_MANAGED_DOCKER=1 \
ENGAGEQUEUE_TEST_RUNTIME_WHEEL="$PWD/dist/engagequeue-0.1.1-py3-none-any.whl" \
  .conda/engagequeue/bin/python -m pytest -q tests/test_managed_live.py
```

With this opt-in enabled, Docker must be available; the managed CI jobs fail
rather than silently skipping a missing daemon. They cover the owned runtime's
initialization, workspace policy, durable job execution, stopping, and restart.
On macOS these tests additionally require a running Docker Desktop installation
and shared workspace paths. Ordinary test runs do not start managed Docker
runtimes.

Build a wheel and source archive, install the wheel into a clean project-local
environment, and run an API/CLI smoke test from outside the source directory.
Confirm the import path resolves to the installed package, running commands
complete after the submitting client exits, and stdout/stderr and exit codes
are preserved.

Application acceptance additionally covers Galaxy uploads, metadata sidecars,
standard tools and interactive sessions, and Pulsar's queued_cli recovery and
collection-acknowledgment behavior. Keep private credentials, datasets and
application smoke reports out of source distributions.

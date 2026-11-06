# Validation

Run tests and checks with this project's local environment:

```sh
.conda/engagequeue/bin/ruff check src tests scripts
.conda/engagequeue/bin/ruff format --check src tests scripts
.conda/engagequeue/bin/python -m pytest -q
```

The CI matrix covers Python 3.10, 3.12 and 3.14. Tests execute real local
commands and independent schedulers, including client death, queued admission,
submission receipts, cancellation, fork-safe embedding and recovery.
Regression coverage includes concurrent first-time database initialization,
job-specific launch failures, retries after workspace cleanup, and environment
overrides across client restarts. Deterministic Docker/systemd ownership and
reconciliation tests use controlled runtime responses without requiring live
services.

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

Build a wheel and source archive, install the wheel into a clean project-local
environment, and run an API/CLI smoke test from outside the source directory.
Confirm the import path resolves to the installed package, running commands
complete after the submitting client exits, and stdout/stderr and exit codes
are preserved.

Application acceptance additionally covers Galaxy uploads, metadata sidecars,
standard tools and interactive sessions, and Pulsar's queued_cli recovery and
collection-acknowledgment behavior. Keep private credentials, datasets and
application smoke reports out of source distributions.

# Operation and recovery

One independent scheduler is elected for each persistent state directory.
Its SQLite store coordinates FIFO admission and capacity across Python clients
and CLI processes. A positive pool cap bounds executions; `0`/`None` is
unbounded and `"*"` uses the host CPU count. Different named pools are separate
capacity domains.

Accepted jobs and queued admission continue when all client applications exit.
The runtime and per-job supervisors use separate process sessions. Imports and
client construction do not start workers; submission or recovery starts the
runtime lazily. Process-local handles reopen across fork boundaries.

For managed services, run the runtime outside the embedding application's
shutdown boundary:

```sh
engagequeue --state-dir /ssd/engagequeue runtime serve
```

A separate Supervisor program or systemd user service can manage this command.
Native processes cannot escape an enclosing container or a systemd control
group just by creating a process session. To survive a Galaxy container stop,
place the runtime outside that container and give clients access to its state
and required execution paths. Use `Executor(..., attach_only=True)` or the CLI
`--attach-only` option for clients in another PID namespace. Such clients never
start a coordinator or validate its host-only Python path; the externally
managed runtime must already be healthy. Bind shared paths at the same absolute
locations, on a local filesystem, and use a compatible account. Operator process
reconciliation runs on the execution host, where ownership can be checked.

Runtime controls are explicit:

```sh
engagequeue --state-dir /ssd/engagequeue runtime drain
engagequeue --state-dir /ssd/engagequeue runtime shutdown --wait --timeout 120
engagequeue --state-dir /ssd/engagequeue runtime resume
```

Native supervision uses a Linux subreaper, durable command registration and an
execution gate. PID creation times and host boot identity prevent signalling
reused PIDs. Docker ownership checks remain necessary because a container can
outlive its CLI process or cgroup. Systemd mode uses independently owned user
units and retains exit information until durable completion is committed.
Systemd remains optional: the default native backend runs without it.

A service wrapper can request graceful stop of one of its declared containers:

```sh
engagequeue --state-dir /ssd/engagequeue stop-container JOB_ID owned-service --json
engagequeue --state-dir /ssd/engagequeue wait JOB_ID --timeout 120
```

The first command records durable intent and returns immediately. The runtime
checks the recorded container identity and ownership label before stopping it.
Its surrounding command continues, allowing metadata generation and output
collection to finish under the original execution key. Inspection exposes the
request and its progress. A terminal job keeps its existing result.

When a service may need KILL after its graceful-stop timeout, retain its Docker
container until final exit/OOM evidence is observed. Combine Docker automatic
removal disabled with `ContainerSpec(remove_on_exit=True)`. EngageQueue removes
that container after its full accepted command, metadata and writers are
confirmed stopped, preserving the final receipt first. Default cleanup remains
disabled. Applications such as Galaxy enable this only for their interactive
tool container; metadata and ordinary-container policies remain independent.

Native resource monitoring is not equivalent to kernel cgroup enforcement.
Explicit hard-limit requirements need a capable backend. Walltime and output
limits are monitored independently of the embedding application.

A host reboot ends native processes. On recovery, unstarted work remains queued;
accepted work is reconciled without replaying its command. Lost ownership is
reported explicitly. Inspect and stop possible remaining writers before an
operator resolves an uncertain attempt:

```sh
engagequeue --state-dir /ssd/engagequeue reconcile JOB_ID \
  --confirm-stopped --reason 'Verified stopped through host and runtime inspection'
```

A live verified owned process cannot be released by this operation. When
ownership remains uncertain, explicit operator attestation is recorded in the
audit trail. No automatic repair requires editing internal database records.
Reconciliation checks systemd cgroup quiescence and recorded immutable container
IDs. Renamed containers, conflicting CID files, or unavailable ownership evidence
remain uncertain. A known live resource prevents release even when another
resource cannot be verified.

Pruning removes eligible terminal execution details while preserving submission
receipts. It never removes caller workspaces or application datasets. Drain the
runtime before changing its interpreter or incompatible state format; keep old
workers and environments available until their jobs have finished.


## Upgrading runtime code

Loaded supervisors keep their original code until they finish. Replacing an
installed wheel does not add capabilities to those existing workers. To upgrade,
pause new application submissions, let queued and accepted work finish, and
verify that no queued, active or uncertain executions remain and all terminal
writers are stopped. Shut down that idle runtime with `runtime shutdown --wait`
before installing verified replacement artifacts into its own environment.
When an unreleased artifact keeps the same version, use `--force-reinstall
--no-deps` after verifying its hash and installing its declared dependencies.
Run `runtime resume` afterward: shutdown persists admission/drain flags that
must be cleared before new work can start. Start application clients with the
matching integration code and use newly created workers to validate new
capabilities such as graceful container stop.

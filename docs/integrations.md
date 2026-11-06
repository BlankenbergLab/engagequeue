# Application integrations

EngageQueue's core has no Galaxy or Pulsar imports. Both integrations execute
prepared commands and use the same durable submission and status contract.

## Galaxy

The native `galaxy.jobs.runners.engagequeue:EngageQueueJobRunner` imports the
Python API directly. Galaxy owns dependencies, datasets, metadata, upload
completion, interactive URLs, and final storage collection. EngageQueue owns
execution lifetime, admission, and confirmed process/container termination.

Use separate pools for batch/standalone metadata, uploads, and interactive
sessions. Recoverable uploads use Galaxy's transactional completion receipts;
worker completion alone does not authorize dataset or working-directory
cleanup. Interactive endpoint publication/revocation remains in Galaxy.
For a normal interactive Stop, Galaxy requests graceful termination of the
owned service container through `stop_container`. The same accepted command
then continues its metadata and output processing, including across a Galaxy
restart, before Galaxy collects the results.

For native local destinations, use `outputs_to_working_directory: true` to keep
running outputs on local SSD until Galaxy imports them into permanent storage.
Standalone metadata has no ordinary output datasets and can read existing input
paths directly. Configure Galaxy's framework interpreter and source modules
for metadata commands, independently of a tool's Conda environment.

## Pulsar

The `EngageQueue` job plugin uses Pulsar's existing `queued_cli` manager. Pulsar
continues to prepare scripts, resolve dependencies, stage files, and collect
outputs. Its adapter submits through `engagequeue`, polls durable status, and
waits for verified cancellation. UUID setup directories distinguish attempts;
stable submission keys allow recovery after acceptance but before recording the
returned scheduler ID.

Generic collection acknowledgment is a separate optional Pulsar/Galaxy feature.
Only explicitly opted-in jobs require acknowledgment before staging cleanup.
Legacy clients keep their established cleanup behavior. Executor record
retention is independent of application output import and staging deletion.

Site-specific paths, Slurm provisioning and routing policy belong in deployment
configuration such as `galaxy-ccf`, rather than EngageQueue's core.

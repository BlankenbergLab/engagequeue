# EngageQueue

<img src="https://raw.githubusercontent.com/BlankenbergLab/engagequeue/main/docs/assets/engagequeue-logo.svg" alt="EngageQueue pointing-hand logo" width="256">

**Local jobs. Make it so.**

EngageQueue provides durable, single-host job execution through a Python
library and a command-line interface. Python applications, CLI clients, Galaxy,
and Pulsar can share persistent execution pools without owning the lifetime
of accepted jobs.

The scheduler and job supervisors run independently of application clients.
Closing a Python handle leaves both running jobs and queue admission active.
The default native backend requires no systemd; an optional systemd backend
uses user services and cgroups.

See [installation](https://github.com/BlankenbergLab/engagequeue/blob/main/docs/installation.md),
[the API and CLI](https://github.com/BlankenbergLab/engagequeue/blob/main/docs/api.md),
[operations](https://github.com/BlankenbergLab/engagequeue/blob/main/docs/operations.md),
and [integrations](https://github.com/BlankenbergLab/engagequeue/blob/main/docs/integrations.md).

See [validation](https://github.com/BlankenbergLab/engagequeue/blob/main/docs/validation.md)
for local checks and optional live-runtime tests.

See [branding](https://github.com/BlankenbergLab/engagequeue/blob/main/docs/branding.md)
for the project logo, downloads, and usage guidance, and
[releasing](https://github.com/BlankenbergLab/engagequeue/blob/main/docs/releasing.md)
for PyPI publishing.

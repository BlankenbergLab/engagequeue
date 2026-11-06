# Publishing releases to PyPI

Publishing uses GitHub Actions and PyPI trusted publishing. Configure the PyPI
publisher with these exact values:

| Setting | Value |
| --- | --- |
| PyPI project | `engagequeue` |
| GitHub owner | `BlankenbergLab` |
| GitHub repository | `engagequeue` |
| Workflow filename | `publish.yml` |
| GitHub environment | `pypi` |

The workflow is `.github/workflows/publish.yml`. Its publishing job uses the
GitHub environment named `pypi` and authenticates through OIDC. No PyPI API token,
username, or password is needed. The workflow and environment names must match
the registered publisher; see [PyPI's trusted publishing documentation](https://docs.pypi.org/trusted-publishers/using-a-publisher/).

## Make a release

1. Set the same version in `pyproject.toml`, `src/engagequeue/__init__.py`, and
   the CLI's `--version` definition in `src/engagequeue/cli.py`.
2. Push the reviewed commit and create a tag for that version, such as `v0.1.0`.
   A tag without the `v` prefix is also accepted.
3. Publish a GitHub release for that tag. Publishing the release starts the
   workflow; ordinary pushes, pull requests, and draft releases do not publish.
4. Check the **Publish to PyPI** workflow result and the package on PyPI.

The workflow runs the Python 3.10, 3.12, and 3.14 test matrix, checks the release
tag against source and artifact versions, builds a wheel and source archive,
and checks both with Twine. It installs the wheel in a clean project-local
environment and verifies the CLI and a real native execution. A separate
publishing job then downloads those verified artifacts and uploads them to
PyPI. Only that job has permission to obtain the OIDC publishing token.

PyPI versions are immutable. For a new release, choose a new version; the
workflow does not silently skip existing files or replace a published version.

## Validate locally

Install this project's development dependencies in its project-local environment,
including the explicitly declared `setuptools` backend and `wheel` needed for
non-isolated builds, then run:

```sh
.conda/engagequeue/bin/python scripts/check_release.py --tag v0.1.0
.conda/engagequeue/bin/python -m build --no-isolation
.conda/engagequeue/bin/python scripts/check_release.py --tag v0.1.0 --dist dist
.conda/engagequeue/bin/python -m twine check --strict dist/*
```

Use the intended version in place of `v0.1.0` and a clean distribution directory
so files from an earlier version cannot be uploaded accidentally. Local validation
does not contact PyPI or upload any distributions.

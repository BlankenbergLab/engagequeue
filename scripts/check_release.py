"""Check a release tag and its distributions before publishing EngageQueue."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tarfile
import zipfile
from email.parser import BytesParser
from pathlib import Path, PurePosixPath

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

PROJECT_FILE = Path(__file__).resolve().parents[1] / "pyproject.toml"


def project_version(project_file: str | Path = PROJECT_FILE) -> str:
    with Path(project_file).open("rb") as stream:
        project = tomllib.load(stream)
    try:
        version = project["project"]["version"]
    except (KeyError, TypeError) as exc:
        raise ValueError("pyproject.toml must declare a static project.version") from exc
    if not isinstance(version, str) or not version:
        raise ValueError("project.version must be a nonempty string")
    return version


def _check_metadata(data: bytes, filename: str, version: str) -> None:
    metadata = BytesParser().parsebytes(data, headersonly=True)
    for field, expected in (("Name", "engagequeue"), ("Version", version)):
        values = metadata.get_all(field, [])
        if values != [expected]:
            raise ValueError(f"{filename}: {field} must be {expected!r}, found {values!r}")


def check_distributions(dist: str | Path, version: str) -> None:
    entries = list(Path(dist).iterdir())
    wheels = [entry for entry in entries if entry.name.endswith(".whl")]
    sources = [entry for entry in entries if entry.name.endswith(".tar.gz")]
    if (
        len(entries) != 2
        or len(wheels) != 1
        or len(sources) != 1
        or any(not entry.is_file() or entry.is_symlink() for entry in entries)
    ):
        raise ValueError("Expected exactly one wheel and one .tar.gz sdist, with no other directory entries")

    wheel, source = wheels[0], sources[0]
    try:
        with zipfile.ZipFile(wheel) as archive:
            members = [
                item
                for item in archive.infolist()
                if len(PurePosixPath(item.filename).parts) == 2
                and PurePosixPath(item.filename).parts[0].endswith(".dist-info")
                and PurePosixPath(item.filename).name == "METADATA"
                and not item.is_dir()
            ]
            if len(members) != 1:
                raise ValueError(f"{wheel.name}: expected one top-level .dist-info/METADATA file")
            _check_metadata(archive.read(members[0]), wheel.name, version)
    except (zipfile.BadZipFile, EOFError) as exc:
        raise ValueError(f"Invalid wheel archive {wheel.name}: {exc}") from exc

    try:
        with tarfile.open(source, "r:gz") as archive:
            members = [
                item
                for item in archive.getmembers()
                if len(PurePosixPath(item.name).parts) == 2
                and PurePosixPath(item.name).name == "PKG-INFO"
                and item.isfile()
            ]
            if len(members) != 1:
                raise ValueError(f"{source.name}: expected one root PKG-INFO file")
            with archive.extractfile(members[0]) as stream:
                _check_metadata(stream.read(), source.name, version)
    except (tarfile.TarError, EOFError) as exc:
        raise ValueError(f"Invalid sdist archive {source.name}: {exc}") from exc


def check_release(
    tag: str,
    dist: str | Path | None = None,
    *,
    project_file: str | Path = PROJECT_FILE,
) -> str:
    version = project_version(project_file)
    if tag not in (version, "v" + version):
        raise ValueError(f"Release tag {tag!r} must be {version!r} or {'v' + version!r}")

    import engagequeue

    if engagequeue.__version__ != version:
        raise ValueError(f"Imported EngageQueue version {engagequeue.__version__!r} does not match {version!r}")
    result = subprocess.run(
        [sys.executable, "-m", "engagequeue", "--version"],
        capture_output=True,
        text=True,
        check=True,
        timeout=15,
    )
    expected = "engagequeue " + version
    if result.stdout.strip() != expected:
        raise ValueError(f"CLI version must be {expected!r}, found {result.stdout.strip()!r}")
    if dist is not None:
        check_distributions(dist, version)
    return version


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--dist", type=Path)
    args = parser.parse_args(argv)
    try:
        version = check_release(args.tag, args.dist)
    except (ImportError, OSError, ValueError, subprocess.SubprocessError) as exc:
        parser.exit(1, f"Release check failed: {exc}\n")
    print(f"Release {version} validated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

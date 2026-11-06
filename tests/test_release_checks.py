"""Release validation checks synthetic distributions without publishing."""

import io
import subprocess
import sys
import tarfile
import zipfile

import pytest

import engagequeue
from scripts import check_release as release

VERSION = "1.2.3"


def _metadata(name="engagequeue", version=VERSION):
    return f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n\n".encode()


def _wheel(path, metadata=None):
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(f"engagequeue-{VERSION}.dist-info/METADATA", _metadata() if metadata is None else metadata)


def _sdist(path, metadata=None):
    data = _metadata() if metadata is None else metadata
    with tarfile.open(path, "w:gz") as archive:
        info = tarfile.TarInfo(f"engagequeue-{VERSION}/PKG-INFO")
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))
        # Real setuptools sdists also contain the egg-info copy. Only the
        # distribution root is authoritative for this check.
        info = tarfile.TarInfo(f"engagequeue-{VERSION}/src/engagequeue.egg-info/PKG-INFO")
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))


@pytest.fixture
def distributions(tmp_path):
    dist = tmp_path / "dist"
    dist.mkdir()
    _wheel(dist / f"engagequeue-{VERSION}-py3-none-any.whl")
    _sdist(dist / f"engagequeue-{VERSION}.tar.gz")
    return dist


@pytest.fixture
def project(tmp_path, monkeypatch):
    path = tmp_path / "pyproject.toml"
    path.write_text(f'[project]\nname = "engagequeue"\nversion = "{VERSION}"\n')
    monkeypatch.setattr(engagequeue, "__version__", VERSION)

    def version_command(argv, **kwargs):
        assert argv == [sys.executable, "-m", "engagequeue", "--version"]
        assert kwargs["check"] and kwargs["timeout"] > 0
        return subprocess.CompletedProcess(argv, 0, f"engagequeue {VERSION}\n", "")

    monkeypatch.setattr(release.subprocess, "run", version_command)
    return path


@pytest.mark.parametrize("tag", [VERSION, "v" + VERSION])
def test_release_accepts_matching_versions_and_artifacts(project, distributions, tag):
    assert release.check_release(tag, distributions, project_file=project) == VERSION


def test_release_without_distributions_still_checks_versions(project):
    assert release.check_release(VERSION, project_file=project) == VERSION


def test_current_project_version_matches_real_cli():
    version = release.project_version()
    assert release.check_release("v" + version) == version


def test_dev_install_provides_nonisolated_build_requirements():
    with release.PROJECT_FILE.open("rb") as stream:
        project = release.tomllib.load(stream)
    # CI and publishing both build without an isolated dependency environment.
    # A backend available only in pip's temporary build environment is not enough.
    required = set(project["build-system"]["requires"])
    assert required.issubset(project["project"]["optional-dependencies"]["dev"])


def test_mismatched_tag_is_rejected(project):
    with pytest.raises(ValueError, match="Release tag"):
        release.check_release("v1.2.4", project_file=project)


def test_imported_version_must_match_project(project, monkeypatch):
    monkeypatch.setattr(engagequeue, "__version__", "1.2.4")
    with pytest.raises(ValueError, match="Imported EngageQueue version"):
        release.check_release(VERSION, project_file=project)


def test_cli_version_must_match_project(project, monkeypatch):
    monkeypatch.setattr(
        release.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, "engagequeue 1.2.4\n", ""),
    )
    with pytest.raises(ValueError, match="CLI version"):
        release.check_release(VERSION, project_file=project)


@pytest.mark.parametrize("kind", ["wheel", "sdist"])
def test_invalid_distribution_archive_is_rejected(distributions, kind):
    suffix = ".whl" if kind == "wheel" else ".tar.gz"
    next(distributions.glob("*" + suffix)).write_bytes(b"not an archive")
    with pytest.raises(ValueError, match="Invalid .* archive"):
        release.check_distributions(distributions, VERSION)


@pytest.mark.parametrize("kind", ["wheel", "sdist"])
def test_wrong_distribution_project_is_rejected(distributions, kind):
    if kind == "wheel":
        _wheel(next(distributions.glob("*.whl")), _metadata(name="another-project"))
    else:
        _sdist(next(distributions.glob("*.tar.gz")), _metadata(name="another-project"))
    with pytest.raises(ValueError, match="Name must be 'engagequeue'"):
        release.check_distributions(distributions, VERSION)


@pytest.mark.parametrize("kind", ["wheel", "sdist"])
def test_wheel_and_source_versions_must_agree(distributions, kind):
    if kind == "wheel":
        _wheel(next(distributions.glob("*.whl")), _metadata(version="1.2.4"))
    else:
        _sdist(next(distributions.glob("*.tar.gz")), _metadata(version="1.2.4"))
    with pytest.raises(ValueError, match="Version must be '1.2.3'"):
        release.check_distributions(distributions, VERSION)


@pytest.mark.parametrize("kind", ["wheel", "sdist"])
def test_missing_archive_metadata_is_rejected(distributions, kind):
    if kind == "wheel":
        with zipfile.ZipFile(next(distributions.glob("*.whl")), "w") as archive:
            archive.writestr("module.py", "")
    else:
        with tarfile.open(next(distributions.glob("*.tar.gz")), "w:gz"):
            pass
    with pytest.raises(ValueError, match="expected one"):
        release.check_distributions(distributions, VERSION)


@pytest.mark.parametrize(
    "problem", ["empty", "missing-wheel", "missing-sdist", "extra-wheel", "extra-file", "directory"]
)
def test_wrong_artifact_counts_or_extra_entries_are_rejected(distributions, problem):
    if problem in ("empty", "missing-wheel"):
        next(distributions.glob("*.whl")).unlink()
    if problem in ("empty", "missing-sdist"):
        next(distributions.glob("*.tar.gz")).unlink()
    if problem == "extra-wheel":
        _wheel(distributions / "another.whl")
    elif problem == "extra-file":
        (distributions / "unrelated.txt").write_text("not a distribution")
    elif problem == "directory":
        (distributions / "unrelated").mkdir()
    with pytest.raises(ValueError, match="exactly one wheel"):
        release.check_distributions(distributions, VERSION)


def test_duplicate_metadata_headers_are_rejected(distributions):
    _wheel(next(distributions.glob("*.whl")), _metadata().replace(b"\n\n", b"\nName: another-project\n\n"))
    with pytest.raises(ValueError, match="Name must be"):
        release.check_distributions(distributions, VERSION)


def test_command_failure_is_reported_without_traceback(monkeypatch, capsys):
    def fail(*args):
        raise ValueError("tag disagrees with project.version")

    monkeypatch.setattr(release, "check_release", fail)
    with pytest.raises(SystemExit) as raised:
        release.main(["--tag", VERSION])
    assert raised.value.code == 1
    assert capsys.readouterr().err == "Release check failed: tag disagrees with project.version\n"

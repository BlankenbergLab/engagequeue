"""Optional services must skip cleanly in minimal CI environments."""

import subprocess
from types import SimpleNamespace

import pytest
import runtime_prerequisites as prerequisites


@pytest.fixture
def probes(monkeypatch):
    calls = []
    monkeypatch.setattr(prerequisites.shutil, "which", lambda name: "/usr/bin/" + name)

    def run(command, **options):
        calls.append((command, options))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(prerequisites.subprocess, "run", run)
    return calls


@pytest.mark.parametrize("missing", ["systemctl", "systemd-run"])
def test_missing_systemd_executables_skip_before_probing(monkeypatch, probes, missing):
    monkeypatch.setattr(prerequisites.shutil, "which", lambda name: None if name == missing else "/usr/bin/" + name)
    with pytest.raises(pytest.skip.Exception, match=missing):
        prerequisites.require_systemd()
    assert probes == []


@pytest.mark.parametrize("runtime", ["systemd", "docker"])
@pytest.mark.parametrize("failure", ["nonzero", "missing", "permission", "timeout"])
def test_unavailable_runtime_probe_skips(monkeypatch, probes, runtime, failure):
    monkeypatch.setenv("ENGAGEQUEUE_TEST_DOCKER_IMAGE", "cached:test")

    def run(command, **options):
        if failure == "missing":
            raise FileNotFoundError(command[0])
        if failure == "permission":
            raise PermissionError(command[0])
        if failure == "timeout":
            raise subprocess.TimeoutExpired(command, options["timeout"])
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(prerequisites.subprocess, "run", run)
    with pytest.raises(pytest.skip.Exception):
        if runtime == "systemd":
            prerequisites.require_systemd()
        else:
            prerequisites.require_docker_image()


def test_systemd_probe_has_a_timeout_and_ignores_environment_output(probes):
    prerequisites.require_systemd()
    assert probes == [(["systemctl", "--user", "show-environment"], {"capture_output": True, "timeout": 10})]


def test_docker_requires_explicit_opt_in(monkeypatch, probes):
    monkeypatch.delenv("ENGAGEQUEUE_TEST_DOCKER_IMAGE", raising=False)
    with pytest.raises(pytest.skip.Exception, match="ENGAGEQUEUE_TEST_DOCKER_IMAGE"):
        prerequisites.require_docker_image()
    assert probes == []


def test_missing_docker_client_skips_before_probing(monkeypatch, probes):
    monkeypatch.setenv("ENGAGEQUEUE_TEST_DOCKER_IMAGE", "cached:test")
    monkeypatch.setattr(prerequisites.shutil, "which", lambda name: None)
    with pytest.raises(pytest.skip.Exception, match="docker"):
        prerequisites.require_docker_image()
    assert probes == []


@pytest.mark.parametrize("backend", ["native", "systemd"])
def test_cached_docker_image_probe_never_pulls(monkeypatch, probes, backend):
    monkeypatch.setenv("ENGAGEQUEUE_TEST_DOCKER_IMAGE", "cached:test")
    assert prerequisites.require_docker_image(backend) == "cached:test"
    commands = [command for command, _ in probes]
    assert commands[0] == ["docker", "image", "inspect", "cached:test"]
    assert all(options == {"capture_output": True, "timeout": 10} for _, options in probes)
    assert commands[1:] == ([["systemctl", "--user", "show-environment"]] if backend == "systemd" else [])

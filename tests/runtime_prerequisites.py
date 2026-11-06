"""Bounded prerequisite checks for optional live-runtime tests."""

import os
import shutil
import subprocess

import pytest


def _require_program(name):
    if shutil.which(name) is None:
        pytest.skip(f"{name} is required for this live-runtime test")


def _probe(command, reason):
    try:
        result = subprocess.run(command, capture_output=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        pytest.skip(reason)
    if result.returncode:
        pytest.skip(reason)


def require_systemd():
    for name in ("systemctl", "systemd-run"):
        _require_program(name)
    _probe(["systemctl", "--user", "show-environment"], "No functioning systemd user manager")


def require_docker_image(backend="native"):
    image = os.environ.get("ENGAGEQUEUE_TEST_DOCKER_IMAGE")
    if not image:
        pytest.skip("Set ENGAGEQUEUE_TEST_DOCKER_IMAGE to a cached Docker image")
    _require_program("docker")
    _probe(["docker", "image", "inspect", image], "Docker daemon and cached image are required")
    if backend == "systemd":
        require_systemd()
    return image

"""Opt-in tests against a real Docker engine and locally available image."""

import json
import subprocess
import time
import uuid

import pytest
from runtime_prerequisites import require_docker_image

from engagequeue import ContainerSpec, Executor, JobSpec, JobState, PoolConfig


@pytest.mark.parametrize("backend", ["native", "systemd"])
def test_owned_container_outlives_cli_and_is_confirmed_stopped(tmp_path, backend):
    image = require_docker_image(backend)
    token = uuid.uuid4().hex
    name = "engagequeue-test-" + token
    executor = Executor(tmp_path / "state", pools={"default": PoolConfig(1, backend, 0.5)})
    handle = executor.submit(
        JobSpec(
            argv=[
                "docker",
                "run",
                "--detach",
                "--rm",
                "--name",
                name,
                "--label",
                f"engagequeue.attempt={token}",
                image,
                "sh",
                "-c",
                "sleep 60",
            ],
            cwd=str(tmp_path),
            timeout=30,
            container=ContainerSpec(name=name, label_value=token),
        ),
        key="owned-container",
    )
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        result = subprocess.run(["docker", "inspect", name], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            data = json.loads(result.stdout)[0]
            if data["State"]["Running"]:
                assert data["Config"]["Labels"]["engagequeue.attempt"] == token
                break
        status = executor.status(handle.id)
        assert not status.terminal, executor.inspect(handle.id)
        time.sleep(0.05)
    else:
        pytest.fail("Owned container did not start")
    # docker run --detach exits; the daemon-owned workload must still occupy its slot.
    time.sleep(0.25)
    assert executor.status(handle.id).state == JobState.RUNNING
    stopped = executor.cancel(handle.id, wait=True, timeout=20)
    assert stopped.state == JobState.CANCELLED
    assert stopped.writers_stopped
    result = subprocess.run(["docker", "inspect", name], capture_output=True, text=True, timeout=5)
    assert result.returncode != 0 or not json.loads(result.stdout)[0]["State"]["Running"]
    executor.shutdown(wait=True, timeout=10)
    executor.close()

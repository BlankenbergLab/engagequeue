"""Required opt-in managed Linux tests, run on both Linux architectures in CI."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from engagequeue import ContainerSpec, DockerExecutor, JobSpec, JobState, PoolConfig
from engagequeue.errors import RuntimeUnavailable, UnsupportedBackend
from engagequeue.managed import LABEL

pytestmark = pytest.mark.skipif(
    os.environ.get("ENGAGEQUEUE_TEST_MANAGED_DOCKER") != "1",
    reason="Set ENGAGEQUEUE_TEST_MANAGED_DOCKER=1 for managed Docker",
)


@pytest.fixture(scope="module")
def managed(tmp_path_factory):
    wheel = os.environ.get("ENGAGEQUEUE_TEST_RUNTIME_WHEEL")
    assert wheel and Path(wheel).is_file(), "Set ENGAGEQUEUE_TEST_RUNTIME_WHEEL to the matching built wheel"
    subprocess.run(["docker", "info"], check=True, capture_output=True, timeout=20)
    root = tmp_path_factory.mktemp("managed")
    work = root / "workspace"
    work.mkdir()
    client = DockerExecutor(root / "state", pools={"default": PoolConfig(2, cancel_grace=0.15)})
    profile = None
    initialized = False
    try:
        client.initialize(workspaces=[work], wheel=wheel, docker_jobs=True)
        profile = client._load()
        initialized = True
        yield client, work
    finally:
        if profile is None and (client.root / "managed.json").exists():
            profile = client._load()
        if profile is not None:
            try:
                if initialized:
                    client.start()
                    for status in client.list():
                        if not status.terminal and not status.needs_reconciliation:
                            client.cancel(status.id, wait=True, timeout=20)
                    if not any(status.needs_reconciliation for status in client.list()):
                        client.stop(timeout=30)
            finally:
                # Tests delete only their exact instance-labelled resources.
                context = ["docker", "--context", profile["docker_context"]]
                for kind, identity in (("container", profile["container_id"]), ("volume", profile["volume"])):
                    if not identity:
                        continue
                    probe = subprocess.run(
                        [*context, kind, "inspect", identity], capture_output=True, text=True, timeout=20
                    )
                    if probe.returncode == 0:
                        data = json.loads(probe.stdout)[0]
                        labels = data.get("Config", {}).get("Labels") if kind == "container" else data.get("Labels")
                        assert (labels or {}).get(LABEL) == profile["instance_id"]
                        command = [*context, kind, "rm"]
                        if kind == "container":
                            command.append("--force")
                        subprocess.run([*command, identity], check=True, capture_output=True, timeout=30)
        client.close()


def test_host_api_cli_outputs_receipts_and_receipt_first_retry(managed):
    client, work = managed
    cwd = work / "removable"
    cwd.mkdir()
    spec = JobSpec(["python", "-c", "print('managed output')"], str(cwd))
    job = client.submit(spec, key="durable-retry")
    assert client.wait(job.id, timeout=30).state is JobState.SUCCEEDED
    current = client.inspect(job.id)
    assert Path(current["spec"]["stdout"]).read_text() == "managed output\n"
    assert not (client.root / "state.sqlite3").exists()
    assert (client.root / "managed.json").is_file()
    cwd.rmdir()
    assert client.submit(spec, key="durable-retry").id == job.id
    assert client.lookup("durable-retry").id == job.id
    cli = subprocess.run(
        [sys.executable, "-m", "engagequeue", "--state-dir", client.state_dir, "status", job.id, "--json"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert cli.returncode == 0, cli.stderr
    assert json.loads(cli.stdout)["state"] == "SUCCEEDED"


def test_daemonized_linux_writers_finish_before_completion(managed):
    client, work = managed
    code = """
import os,time
from pathlib import Path
if os.fork()==0:
    os.setsid()
    if os.fork()==0:
        time.sleep(.3)
        Path('daemon-finished').write_text('complete')
        os._exit(0)
    os._exit(0)
os._exit(0)
"""
    job = client.submit(JobSpec(["python", "-c", code], str(work)), key="daemon")
    assert client.wait(job.id, timeout=30).state is JobState.SUCCEEDED
    assert (work / "daemon-finished").read_text() == "complete"


def test_cancel_and_shutdown_resume_preserve_runtime(managed):
    client, work = managed
    job = client.submit(JobSpec(["python", "-c", "import time; time.sleep(60)"], str(work)), key="cancel")
    assert client.cancel(job.id, wait=True, timeout=30).state is JobState.CANCELLED
    identity = client.runtime_info()["container_id"]
    client.stop(timeout=30)
    assert not client.runtime_info()["container_running"]
    client.start()
    assert client.runtime_info()["container_id"] == identity
    another = client.submit(JobSpec(["python", "-c", "pass"], str(work)), key="after-resume")
    assert client.wait(another.id, timeout=30).state is JobState.SUCCEEDED


def test_new_job_outside_workspace_is_rejected(managed, tmp_path):
    client, _ = managed
    with pytest.raises((ValueError, UnsupportedBackend)):
        client.submit(JobSpec(["true"], str(tmp_path)), key="outside-root")


def test_owned_docker_job_receipt_is_host_readable(managed):
    client, work = managed
    profile = client._load()
    name = "engagequeue-managed-service-" + profile["instance_id"]
    token = "owned-" + profile["instance_id"]
    configuration = ContainerSpec(name=name, label_value=token, remove_on_exit=True)
    code = """
import subprocess,sys
subprocess.run(['docker','run','--entrypoint','/opt/engagequeue/.conda/engagequeue/bin/python',
                '--name',sys.argv[1],'--label','engagequeue.attempt='+sys.argv[2],sys.argv[3],
                '-c','import time; time.sleep(120)'],check=False)
"""
    # Reuse the already cached image, avoiding unrelated external test images.
    image = profile["image_id"]
    job = client.submit(
        JobSpec(["python", "-c", code, name, token, image], str(work), container=configuration), key="service"
    )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        record = client.inspect(job.id)
        if record.get("owned_container_ids") and record["owned_container_ids"][0]:
            break
        time.sleep(0.1)
    else:
        pytest.fail("Owned sibling container was not observed")
    receipt = Path(client.container_stop_receipt_path(job.id, name))
    client.stop_container(job.id, name)
    assert client.wait(job.id, timeout=30).state is JobState.SUCCEEDED
    assert json.loads(receipt.read_text())["container_id"]


def test_unexpected_container_restart_never_replays_accepted_job(managed):
    client, work = managed
    code = "from pathlib import Path; import time; p=Path('runs'); p.write_text(p.read_text()+'x' if p.exists() else 'x'); time.sleep(120)"
    job = client.submit(JobSpec(["python", "-c", code], str(work)), key="interrupted")
    deadline = time.monotonic() + 20
    while not (work / "runs").exists() and time.monotonic() < deadline:
        time.sleep(0.1)
    assert (work / "runs").read_text() == "x"
    profile = client._load()
    client._docker(profile, "restart", profile["container_id"])
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        status = client.status(job.id)
        if status.state is JobState.LOST:
            break
        time.sleep(0.2)
    assert status.state is JobState.LOST
    assert (work / "runs").read_text() == "x"
    assert client.submit(JobSpec(["python", "-c", code], str(work)), key="interrupted").id == job.id
    client.reconcile(job.id, confirm_stopped=True, reason="Verified test container restart ended old namespace")
    assert client.status(job.id).writers_stopped


def test_constructor_close_does_not_stop_existing_runtime(managed):
    client, _ = managed
    with DockerExecutor(client.state_dir):
        pass
    assert client.runtime_info()["container_running"]
    with pytest.raises(RuntimeUnavailable, match="different|differs"):
        altered = client._load()
        altered["daemon_id"] = "another"
        client._daemon(altered)

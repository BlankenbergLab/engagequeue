"""Durable service-container stops leave the accepted host shell alive."""

from __future__ import annotations

import json
import os
import shlex
import signal
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import psutil
import pytest
from runtime_prerequisites import require_docker_image

from engagequeue import ContainerSpec, Executor, JobSpec, JobState, PoolConfig
from engagequeue.backends.containers import ContainerOwnershipError, ContainerUnavailable, OwnedContainer
from engagequeue.backends.native import _stop_container_requests
from engagequeue.backends.processes import identity, signal_owned


def eventually(read, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = read()
        if result:
            return result
        time.sleep(0.03)
    pytest.fail("Expected service-container checkpoint was not reached")


@pytest.fixture
def queued(tmp_path):
    client = Executor(tmp_path / "state", pools={"default": PoolConfig(1, "native", 0.1)})
    client.store.put_meta("shutdown_requested", True)
    specs = [ContainerSpec(name=name, label_value="attempt") for name in ("tool", "metadata")]
    handle = client.submit(JobSpec(["/bin/true"], str(tmp_path), containers=specs), key="queued")
    try:
        yield client, handle
    finally:
        if client._closed:
            client = Executor(tmp_path / "state")
        client.cancel(handle.id, wait=True, timeout=10)
        client.shutdown(wait=True, timeout=10)
        client.close()


def test_request_is_durable_idempotent_scoped_and_survives_client_close(queued):
    client, handle = queued
    before = client.inspect(handle.id)
    assert client.stop_container(handle.id, "tool").state == JobState.QUEUED
    first = client.inspect(handle.id)
    client.stop_container(handle.id, "tool")
    assert client.inspect(handle.id)["container_stop_requests"] == first["container_stop_requests"]
    assert list(first["container_stop_requests"]) == ["tool"]
    assert not first["cancel_requested"] and first["state"] == before["state"]
    client.close()
    with Executor(client.state_dir, attach_only=True) as attached:
        attached.stop_container(handle.id, "tool")
        row = attached.inspect(handle.id)
        assert row["container_stop_requests"] == first["container_stop_requests"]
        assert [event["event"] for event in row["events"]].count("container_stop_requested") == 1


def test_invalid_targets_reject_and_terminal_declared_targets_are_noops(queued):
    client, handle = queued
    for name in ("foreign", "", None):
        with pytest.raises(ValueError):
            client.stop_container(handle.id, name)
    with pytest.raises(KeyError):
        client.stop_container("missing-job", "tool")
    assert "container_stop_requests" not in client.inspect(handle.id)
    client.cancel(handle.id, wait=True)
    before = client.inspect(handle.id)
    assert client.stop_container(handle.id, "tool").state == JobState.CANCELLED
    assert client.inspect(handle.id) == before
    with pytest.raises(ValueError):
        client.stop_container(handle.id, "foreign")


def test_request_cancel_race_never_reopens_or_readmits_job(queued):
    client, handle = queued
    with ThreadPoolExecutor(max_workers=2) as threads:
        first = threads.submit(client.stop_container, handle.id, "tool")
        second = threads.submit(client.cancel, handle.id)
        first.result()
        second.result()
    row = client.inspect(handle.id)
    assert row["state"] == "CANCELLED" and row["cancel_requested"] and row["writers_stopped"]
    assert row.get("command_started") is None
    before = row.copy()
    assert client.stop_container(handle.id, "tool").state == JobState.CANCELLED
    assert client.inspect(handle.id) == before


def test_worker_merges_requests_without_overwriting_other_named_intents(queued):
    client, handle = queued
    with ThreadPoolExecutor(max_workers=2) as threads:
        futures = [threads.submit(client.stop_container, handle.id, name) for name in ("tool", "metadata")]
        for future in futures:
            future.result()
    client.store.note_container_stop(handle.id, "tool", "container_stop_signalled", container_id="a" * 64)
    assert set(client.inspect(handle.id)["container_stop_requests"]) == {"tool", "metadata"}
    with pytest.raises(ValueError, match="immutable"):
        client.store.note_container_stop(handle.id, "tool", "container_stop_signalled", container_id="b" * 64)


def test_pending_intent_waits_for_appearance_then_only_signals_named_verified_container(queued, monkeypatch):
    client, handle = queued
    client.stop_container(handle.id, "tool")
    configurations = client.store.get(handle.id)["spec"]["containers"]
    containers = [OwnedContainer(configuration) for configuration in configurations]
    calls = []
    exists, running = False, True

    def execute(container, *args):
        calls.append((container.configuration["name"], args))
        if args[0] == "inspect":
            if not exists:
                return SimpleNamespace(returncode=1, stdout="[]", stderr="Error: No such object: " + args[1])
            return SimpleNamespace(
                returncode=0,
                stderr="",
                stdout=json.dumps(
                    [
                        {
                            "Id": "a" * 64,
                            "Name": "/tool",
                            "Config": {"Labels": {"engagequeue.attempt": "attempt"}},
                            "State": {"Running": running},
                        }
                    ]
                ),
            )
        row = client.store.get(handle.id)
        assert row["container_stop_requests"]["tool"]["container_id"] == "a" * 64
        assert row["container_stop_requests"]["tool"]["term_sent_at"]
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(OwnedContainer, "_execute", execute)

    def apply():
        _stop_container_requests(client.store, handle.id, client.store.get(handle.id), containers)

    apply()
    assert client.store.get(handle.id)["container_stop_requests"]["tool"]["status"] == "requested"
    assert not any(args[0] == "kill" for _, args in calls)
    exists = True
    apply()
    assert ("tool", ("kill", "--signal=TERM", "a" * 64)) in calls
    assert not any(name == "metadata" for name, _ in calls)
    client.store.note_container_stop(handle.id, "tool", "test_clock", term_sent_at=time.time() - 1)
    apply()
    assert ("tool", ("kill", "--signal=KILL", "a" * 64)) in calls
    running = False
    apply()
    row = client.store.get(handle.id)
    assert row["state"] == "QUEUED" and not row["cancel_requested"]
    assert row["container_stop_requests"]["tool"]["status"] == "stopped"
    assert row["container_stop_requests"]["tool"]["stopped_at"]


@pytest.mark.parametrize("problem", ["label", "runtime"])
def test_wrong_ownership_or_runtime_uncertainty_never_acknowledges_stop(queued, monkeypatch, problem):
    client, handle = queued
    client.stop_container(handle.id, "tool")
    container = OwnedContainer(client.store.get(handle.id)["spec"]["containers"][0])
    calls = []

    def execute(*args):
        calls.append(args)
        if problem == "runtime":
            return SimpleNamespace(returncode=1, stdout="", stderr="dial tcp: lookup docker.invalid: no such host")
        return SimpleNamespace(
            returncode=0,
            stderr="",
            stdout=json.dumps(
                [
                    {
                        "Id": "a" * 64,
                        "Name": "/tool",
                        "Config": {"Labels": {"engagequeue.attempt": "foreign"}},
                        "State": {"Running": True},
                    }
                ]
            ),
        )

    monkeypatch.setattr(container, "_execute", execute)
    with pytest.raises(ContainerUnavailable if problem == "runtime" else ContainerOwnershipError):
        _stop_container_requests(client.store, handle.id, client.store.get(handle.id), [container])
    assert all(args[0] == "inspect" for args in calls)
    assert client.store.get(handle.id)["container_stop_requests"]["tool"]["status"] == "requested"


def docker_info(name):
    result = subprocess.run(["docker", "inspect", name], capture_output=True, text=True, timeout=10)
    return json.loads(result.stdout)[0] if not result.returncode else None


@pytest.mark.parametrize("backend", ["native", "systemd"])
@pytest.mark.parametrize("before_appearance,force_kill", [(False, False), (True, False), (False, True)])
def test_real_service_stop_is_durable_and_original_shell_runs_metadata(
    tmp_path, backend, before_appearance, force_kill
):
    image = require_docker_image(backend)
    attempt = uuid.uuid4().hex
    tool, metadata = ["engagequeue-stop-" + attempt + suffix for suffix in ("-tool", "-metadata")]
    signals = tmp_path / "signals"
    signals.mkdir()
    statements = ["printf 'one\\n' >> count", "printf '%s' $$ > shell-pid"]
    if before_appearance:
        statements.append("while [ ! -f allow-containers ]; do sleep .02; done")
    for name, role in [(tool, "tool"), (metadata, "metadata")]:
        trap = (
            "trap '' TERM"
            if force_kill and role == "tool"
            else f"trap 'printf stopped > /signals/{role}-stopped; exit 0' TERM"
        )
        service = trap + f"; printf ready > /signals/{role}-ready; while :; do sleep .05; done"
        statements.append(
            shlex.join(
                [
                    "docker",
                    "run",
                    "-d",
                    "--name",
                    name,
                    "--label",
                    "engagequeue.attempt=" + attempt,
                    "--user",
                    f"{os.getuid()}:{os.getgid()}",
                    "--volume",
                    str(signals) + ":/signals",
                    image,
                    "sh",
                    "-c",
                    service,
                ]
            )
            + " > /dev/null"
        )
    statements.extend(
        [
            "docker wait " + shlex.quote(tool) + " > tool-exit",
            "printf original-shell > metadata-marker",
            "while [ ! -f finish ]; do sleep .02; done",
            "docker stop --time=1 " + shlex.quote(metadata) + " > /dev/null",
        ]
    )
    spec = JobSpec(
        ["/bin/sh", "-c", "; ".join(statements)],
        str(tmp_path),
        containers=[
            ContainerSpec(name=name, label_value=attempt, remove_on_exit=force_kill) for name in (tool, metadata)
        ],
    )
    client = Executor(tmp_path / "state", pools={"default": PoolConfig(1, backend, 1)})
    handle = client.submit(spec, key="service-stop")
    stopped_worker = None
    try:
        eventually(lambda: (tmp_path / "shell-pid").exists())
        if not before_appearance:
            eventually(lambda: (signals / "tool-ready").exists() and (signals / "metadata-ready").exists())
        original = client.inspect(handle.id)
        stopped_worker = identity(original["worker_pid"])
        assert stopped_worker["birth"] == original["worker_birth"]
        # Hold SQLite's writer lock before freezing the worker. Stopping a
        # process in an arbitrary transaction would itself lock out clients.
        with client.store.transaction():
            assert signal_owned(stopped_worker, signal.SIGSTOP)
            eventually(lambda: psutil.Process(stopped_worker["pid"]).status() == psutil.STATUS_STOPPED)
        receipt_path = client.container_stop_receipt_path(handle.id, tool)
        client.close()
        with Executor(tmp_path / "state", attach_only=True) as request_client:
            status = request_client.stop_container(handle.id, tool)
            assert status.state == JobState.RUNNING
            pending = request_client.inspect(handle.id)
            assert pending["container_stop_requests"][tool]["status"] == "requested"
            assert metadata not in pending["container_stop_requests"]
            assert not pending["cancel_requested"]
            assert request_client.container_stop_receipt_path(handle.id, tool) == receipt_path
            assert not os.path.exists(receipt_path)
        # Both submitting/requesting clients are gone before handling the intent.
        assert signal_owned(stopped_worker, signal.SIGCONT)
        stopped_worker = None
        client = Executor(tmp_path / "state", attach_only=True)
        if before_appearance:
            (tmp_path / "allow-containers").touch()
        eventually(lambda: (tmp_path / "metadata-marker").exists())
        eventually(lambda: client.inspect(handle.id)["container_stop_requests"][tool]["status"] == "stopped")
        current = client.inspect(handle.id)
        assert current["state"] == "RUNNING" and not current["cancel_requested"]
        assert current["command_pid"] == original["command_pid"]
        assert current["payload_pid"] == int((tmp_path / "shell-pid").read_text())
        assert (tmp_path / "count").read_text().splitlines() == ["one"]
        assert docker_info(tool)["State"]["Running"] is False
        assert docker_info(metadata)["State"]["Running"] is True
        if not before_appearance and not force_kill:
            assert (signals / "tool-stopped").read_text() == "stopped"
        assert not (signals / "metadata-stopped").exists()
        assert (tmp_path / "metadata-marker").read_text() == "original-shell"
        with open(receipt_path) as stream:
            receipt = json.load(stream)
        assert receipt["schema"] == 1 and receipt["job_id"] == handle.id and receipt["key"] == current["key"]
        assert receipt["name"] == tool and receipt["container_id"] == docker_info(tool)["Id"]
        assert receipt["label_key"] == "engagequeue.attempt" and receipt["label_value"] == attempt
        assert receipt["status"] == "stopped" and receipt["oom_killed"] is False
        assert receipt["job_cancel_requested"] is False and receipt["issued_signal"] in ("TERM", "KILL")
        assert receipt["stopped_at"] >= receipt["issued_at"]
        if force_kill:
            assert receipt["issued_signal"] == "KILL" and receipt["exit_code"] == 137
        elif not before_appearance:
            assert receipt["issued_signal"] == "TERM" and receipt["exit_code"] == 0
        assert not os.path.exists(client.container_stop_receipt_path(handle.id, metadata))
        (tmp_path / "finish").touch()
        final = client.wait(handle.id, timeout=25)
        assert final.state == JobState.SUCCEEDED and final.returncode == 0 and final.writers_stopped
        if force_kill:
            assert docker_info(tool) is None and docker_info(metadata) is None
            cleanup = client.inspect(handle.id)["container_cleanup"]
            assert cleanup[tool]["status"] == cleanup[metadata]["status"] == "removed"
            # Removal cannot erase the authoritative final stop/OOM receipt.
            with open(receipt_path) as stream:
                retained_receipt = json.load(stream)
            assert retained_receipt["exit_code"] == 137 and retained_receipt["oom_killed"] is False
        events = client.inspect(handle.id)["events"]
        assert [event["event"] for event in events].count("container_stop_requested") == 1
        assert any(event["event"] == "container_stop_confirmed" for event in events)
        before = client.inspect(handle.id)
        assert client.stop_container(handle.id, metadata).state == JobState.SUCCEEDED
        assert client.inspect(handle.id) == before
    finally:
        if stopped_worker is not None:
            signal_owned(stopped_worker, signal.SIGCONT)
        if client._closed:
            client = Executor(tmp_path / "state")
        if not client.status(handle.id).terminal:
            client.cancel(handle.id, wait=True, timeout=25)
        client.shutdown(wait=True, timeout=15)
        client.close()
        for name in (tool, metadata):
            info = docker_info(name)
            if info and info.get("Config", {}).get("Labels", {}).get("engagequeue.attempt") == attempt:
                subprocess.run(["docker", "rm", "-f", info["Id"]], capture_output=True, timeout=10)


@pytest.mark.parametrize("backend", ["native", "systemd"])
def test_real_cancel_supersedes_pending_container_stop_without_replaying(tmp_path, backend):
    image = require_docker_image(backend)
    attempt = uuid.uuid4().hex
    names = ["engagequeue-stop-cancel-" + attempt + suffix for suffix in ("-tool", "-metadata")]
    commands = ["printf 'one\\n' >> count"]
    for name in names:
        commands.append(
            shlex.join(
                [
                    "docker",
                    "run",
                    "-d",
                    "--name",
                    name,
                    "--label",
                    "engagequeue.attempt=" + attempt,
                    image,
                    "sh",
                    "-c",
                    "trap 'exit 0' TERM; while :; do sleep .05; done",
                ]
            )
            + " > /dev/null"
        )
    commands.extend(["touch started", "sleep 60", "touch must-not-resurrect"])
    client = Executor(tmp_path / "state", pools={"default": PoolConfig(1, backend, 0.5)})
    handle = client.submit(
        JobSpec(
            ["/bin/sh", "-c", "; ".join(commands)],
            str(tmp_path),
            containers=[ContainerSpec(name=name, label_value=attempt, remove_on_exit=True) for name in names],
        ),
        key="cancel-race",
    )
    stopped_worker = None
    try:
        eventually(lambda: (tmp_path / "started").exists())
        stopped_worker = identity(client.inspect(handle.id)["worker_pid"])
        with client.store.transaction():
            assert signal_owned(stopped_worker, signal.SIGSTOP)
            eventually(lambda: psutil.Process(stopped_worker["pid"]).status() == psutil.STATUS_STOPPED)
        client.stop_container(handle.id, names[0])
        assert client.cancel(handle.id).state == JobState.STOPPING
        assert signal_owned(stopped_worker, signal.SIGCONT)
        stopped_worker = None
        result = client.wait(handle.id, timeout=25)
        assert result.state == JobState.CANCELLED and result.writers_stopped
        assert client.inspect(handle.id)["cancel_requested"]
        assert not os.path.exists(client.container_stop_receipt_path(handle.id, names[0]))
        assert (tmp_path / "count").read_text().splitlines() == ["one"]
        assert not (tmp_path / "must-not-resurrect").exists()
        assert all(docker_info(name) is None for name in names)
        before = client.inspect(handle.id)
        assert client.stop_container(handle.id, names[0]).state == JobState.CANCELLED
        assert client.inspect(handle.id) == before
    finally:
        if stopped_worker is not None:
            signal_owned(stopped_worker, signal.SIGCONT)
        if not client.status(handle.id).terminal:
            client.cancel(handle.id, wait=True, timeout=25)
        client.shutdown(wait=True, timeout=15)
        client.close()
        for name in names:
            info = docker_info(name)
            if info and info.get("Config", {}).get("Labels", {}).get("engagequeue.attempt") == attempt:
                subprocess.run(["docker", "rm", "-f", info["Id"]], capture_output=True, timeout=10)

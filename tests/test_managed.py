"""Host-side transport, ownership, and compatibility boundaries."""

import csv
import json
import os

import pytest

from engagequeue import DockerExecutor, JobSpec, PoolConfig
from engagequeue.errors import RuntimeUnavailable, SubmissionConflict
from engagequeue.managed import LABEL, _mount, _pool
from engagequeue.runtime import Runtime
from engagequeue.store import Store


def test_construction_and_close_never_touch_docker_or_start_jobs(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Construction/close invoked Docker")

    monkeypatch.setattr("engagequeue.managed.subprocess.run", forbidden)
    root = tmp_path / "state"
    with DockerExecutor(root) as client:
        assert not root.exists()
    with pytest.raises(RuntimeError, match="closed"):
        client.list()


@pytest.mark.parametrize("entrypoint", [Store, Runtime])
def test_direct_native_entrypoints_cannot_open_host_managed_state(tmp_path, entrypoint):
    (tmp_path / "managed.json").write_text("{}")
    with pytest.raises(RuntimeUnavailable, match="Managed Docker state"):
        entrypoint(tmp_path)
    assert not (tmp_path / "state.sqlite3").exists()


def test_pool_auto_capacity_preserves_local_serialization_and_remote_intent():
    config = PoolConfig("*")
    assert config.max_jobs == (os.cpu_count() or 1)
    assert set(config.to_dict()) == {"max_jobs", "backend", "cancel_grace"}
    assert _pool(config)["max_jobs"] == "*"
    assert _pool(PoolConfig(3))["max_jobs"] == 3


def test_mount_paths_with_commas_are_csv_escaped():
    assert next(csv.reader([_mount("bind", "/work/a,b", "/work/a,b")])) == [
        "type=bind",
        "src=/work/a,b",
        "dst=/work/a,b",
    ]


def test_rpc_keeps_secrets_in_stdin_and_restores_typed_errors(tmp_path, monkeypatch):
    requests = []
    client = DockerExecutor(tmp_path, pools={"batch": PoolConfig("*")})

    def docker(profile, *args, **kwargs):
        assert "secret-value" not in " ".join(args)
        requests.append(json.loads(kwargs["input"]))
        return json.dumps(
            {
                "protocol": 1,
                "ok": False,
                "error": {"type": "SubmissionConflict", "message": "This key already has a different specification"},
            }
        )

    monkeypatch.setattr(client, "_docker", docker)
    spec = JobSpec(["python", "job.py"], str(tmp_path), env={"TOKEN": "secret-value"})
    with pytest.raises(SubmissionConflict):
        client._raw_rpc({"container_id": "owned"}, "submit", {"spec": spec.to_dict(), "key": "retry-key"})
    assert requests[0]["args"]["key"] == "retry-key"
    assert requests[0]["args"]["spec"]["env"]["TOKEN"] == "secret-value"
    assert requests[0]["pools"]["batch"]["max_jobs"] == "*"


@pytest.mark.parametrize("response", ["not JSON", "{}", "[]", '{"protocol":2,"ok":true}', '{"protocol":1,"ok":false}'])
def test_invalid_bridge_responses_are_transport_failures(tmp_path, monkeypatch, response):
    client = DockerExecutor(tmp_path)
    monkeypatch.setattr(client, "_docker", lambda *a, **k: response)
    with pytest.raises(RuntimeUnavailable, match="Invalid managed bridge"):
        client._raw_rpc({"container_id": "owned"}, "list")


def test_handshake_does_not_consume_pending_pool_configuration(tmp_path, monkeypatch):
    client = DockerExecutor(tmp_path, pools={"batch": PoolConfig(4)})
    monkeypatch.setattr(client, "_docker", lambda *a, **k: '{"protocol":1,"ok":true,"result":{}}')
    client._raw_rpc({"container_id": "owned"}, "info")
    assert client._pools["batch"]["max_jobs"] == 4
    client._raw_rpc({"container_id": "owned"}, "initialize")
    assert client._pools is None


def test_container_adoption_rejects_changed_ownership_and_extra_mounts(tmp_path, monkeypatch):
    client = DockerExecutor(tmp_path)
    profile = {
        "container_id": "owned",
        "instance_id": "instance",
        "image_id": "image",
        "volume": "data",
        "owner_uid": os.getuid(),
        "owner_gid": os.getgid(),
        "workspaces": [],
        "docker_jobs": False,
    }
    data = {
        "Id": "owned",
        "Image": "image",
        "Config": {"Labels": {LABEL: "instance"}, "User": f"{os.getuid()}:{os.getgid()}"},
        "HostConfig": {},
        "Mounts": [
            {"Destination": str(tmp_path), "Type": "volume", "Name": "data", "RW": True},
            {"Destination": str(tmp_path / "jobs"), "Type": "bind", "Source": str(tmp_path / "jobs"), "RW": True},
        ],
    }
    monkeypatch.setattr(client, "_inspect", lambda *a: data)
    assert client._container(profile) == data
    data["Mounts"].append({"Destination": "/unexpected", "Type": "bind", "Source": "/", "RW": True})
    with pytest.raises(RuntimeUnavailable, match="unexpected mounts"):
        client._container(profile)
    data["Mounts"].pop()
    data["Config"]["Labels"][LABEL] = "another-instance"
    with pytest.raises(RuntimeUnavailable, match="ownership"):
        client._container(profile)


def test_stopped_attach_only_client_never_restarts_container(tmp_path, monkeypatch):
    client = DockerExecutor(tmp_path, attach_only=True)
    monkeypatch.setattr(client, "_daemon", lambda *a: None)
    monkeypatch.setattr(client, "_volume", lambda *a: None)
    monkeypatch.setattr(client, "_container", lambda *a: {"State": {"Running": False}})
    monkeypatch.setattr(client, "_docker", lambda *a, **k: pytest.fail("Restarted stopped container"))
    with pytest.raises(RuntimeUnavailable, match="stopped"):
        client._connect({})


def test_managed_state_cannot_replace_local_database(tmp_path):
    Store(tmp_path)
    with pytest.raises(ValueError, match="Existing local state"):
        DockerExecutor(tmp_path).initialize(workspaces=[tmp_path.parent])


def test_managed_profile_relocation_is_rejected(tmp_path):
    client = DockerExecutor(tmp_path)
    client._directory()
    client._save({"schema": 1, "state_dir": "/old/location", "owner_uid": os.getuid()})
    with pytest.raises(RuntimeUnavailable, match="location"):
        client._load()

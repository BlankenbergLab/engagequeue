"""Managed resources retain their identities across retries and partial failure."""

import copy
import csv
import json
import os

import pytest

from engagequeue import DockerExecutor, PoolConfig, errors, managed
from engagequeue.managed import LABEL


class ManagedDocker:
    """Model Docker's persistent resources while exercising host lifecycle code."""

    def __init__(self):
        self.image_id = "sha256:original-image"
        self.volumes = {}
        self.containers = {}
        self.calls = []
        self.requests = []
        self.active = []
        self.runtime_running = False
        self.fail_create = False
        self.fail_stop = False
        self.created = 0

    def info(self, profile):
        return {
            "version": managed._version(),
            "protocol": managed.PROTOCOL,
            "platform": "linux",
            "instance_id": profile["instance_id"],
            "uid": os.getuid(),
        }

    def inspect(self, profile, kind, identifier):
        resources = self.volumes if kind == "volume" else self.containers
        return copy.deepcopy(resources.get(identifier))

    def image(self, profile, image, wheel):
        return self.image_id, "wheel-digest" if wheel is not None else None

    @staticmethod
    def option(args, name):
        return args[args.index(name) + 1]

    def docker(self, profile, *args, **kwargs):
        self.calls.append((args, copy.deepcopy(profile)))
        if args[:2] == ("volume", "create"):
            self.volumes[args[-1]] = {"Labels": {LABEL: profile["instance_id"]}}
            return args[-1]
        if args[0] == "run":
            if kwargs.get("input"):
                return json.dumps({"protocol": 1, "ok": True, "result": self.info(profile)})
            return ""
        if args[0] == "create":
            if self.fail_create:
                raise errors.RuntimeUnavailable("Injected replacement create failure")
            self.created += 1
            identifier = f"owned-container-{self.created}"
            mounts = []
            for index, value in enumerate(args):
                if value != "--mount":
                    continue
                fields = dict(item.split("=", 1) for item in next(csv.reader([args[index + 1]])))
                mounts.append(
                    {
                        "Destination": fields["dst"],
                        "Type": fields["type"],
                        "Name" if fields["type"] == "volume" else "Source": fields["src"],
                        "RW": True,
                    }
                )
            label, value = self.option(args, "--label").split("=", 1)
            self.containers[identifier] = {
                "Id": identifier,
                "Image": args[args.index("--entrypoint") + 2],
                "Config": {"Labels": {label: value}, "User": self.option(args, "--user")},
                "HostConfig": {},
                "Mounts": mounts,
                "State": {"Running": False},
            }
            return identifier
        if args[0] == "start":
            self.containers[args[1]]["State"]["Running"] = True
            return args[1]
        if args[0] == "rm":
            del self.containers[args[1]]
            return args[1]
        if args[0] == "stop":
            if self.fail_stop:
                raise errors.RuntimeUnavailable("Injected Docker stop failure")
            assert not self.runtime_running and not self.active
            self.containers[args[-1]]["State"]["Running"] = False
            return args[-1]
        assert args[0] == "exec", args
        request = json.loads(kwargs["input"])
        self.requests.append(request)
        method = request["method"]
        if method == "info":
            result = self.info(profile)
        elif method == "runtime_state":
            result = {"running": self.runtime_running, "active": self.active}
        else:
            assert method in ("initialize", "resume", "shutdown"), method
            if method == "resume":
                self.runtime_running = True
            elif method == "shutdown":
                self.runtime_running = bool(self.active)
            result = None
        return json.dumps({"protocol": 1, "ok": True, "result": result})


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    client = DockerExecutor(tmp_path / "state")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    docker = ManagedDocker()
    monkeypatch.setattr(client, "_docker", docker.docker)
    monkeypatch.setattr(client, "_inspect", docker.inspect)
    monkeypatch.setattr(client, "_image", docker.image)
    monkeypatch.setattr(
        client,
        "_daemon",
        lambda *args, **kwargs: {
            "daemon_id": "local-daemon",
            "endpoint": "unix:///docker.sock",
            "architecture": "arm64",
        },
    )
    client.initialize(workspaces=[workspace], wheel="same-runtime.whl", docker_context="local")
    return client, docker, workspace


def test_reinitializing_the_same_wheel_and_image_preserves_a_running_container(runtime):
    client, docker, _ = runtime
    original = client._load()
    count = len(docker.calls)
    result = client.initialize(wheel="same-runtime.whl")
    assert result["container_id"] == original["container_id"]
    assert result["image_id"] == original["image_id"] and result["container_running"]
    assert not result["stopped_cleanly"]
    assert docker.created == 1
    assert not any(args[0] in ("rm", "create", "stop", "run") for args, _ in docker.calls[count:])


def test_image_change_requires_a_clean_stop_and_preserves_the_original_manifest(runtime):
    client, docker, _ = runtime
    original = client._load()
    docker.image_id = "sha256:replacement-image"
    with pytest.raises(errors.RuntimeUnavailable, match="Stop.*cleanly"):
        client.initialize(wheel="replacement-runtime.whl")
    assert client._load() == original
    assert original["container_id"] in docker.containers
    assert docker.created == 1


def test_failed_replacement_create_checkpoints_new_configuration_without_the_old_container_id(runtime, tmp_path):
    client, docker, _ = runtime
    original = client._load()
    client.stop(timeout=1)
    assert client._load()["stopped_cleanly"]
    workspace = tmp_path / "new-workspace"
    workspace.mkdir()
    docker.image_id = "sha256:replacement-image"
    docker.fail_create = True
    with pytest.raises(errors.RuntimeUnavailable, match="replacement create failure"):
        client.initialize(workspaces=[workspace], wheel="replacement-runtime.whl")
    checkpoint = client._load()
    assert checkpoint["container_id"] is None
    assert checkpoint["image_id"] == docker.image_id
    assert checkpoint["workspaces"] == [str(workspace)]
    assert checkpoint["volume"] == original["volume"] and checkpoint["volume_created"]
    assert original["container_id"] not in docker.containers
    docker.fail_create = False
    recovered = client.initialize()
    assert recovered["container_id"] != original["container_id"]
    assert recovered["image_id"] == docker.image_id and recovered["container_running"]
    assert recovered["workspaces"] == [str(workspace)]
    assert sum(args[:2] == ("volume", "create") for args, _ in docker.calls) == 1


def test_missing_volume_is_never_recreated_after_a_failed_replacement(runtime):
    client, docker, _ = runtime
    client.stop(timeout=1)
    docker.image_id = "sha256:replacement-image"
    docker.fail_create = True
    with pytest.raises(errors.RuntimeUnavailable, match="create failure"):
        client.initialize(wheel="replacement-runtime.whl")
    profile = client._load()
    assert profile["container_id"] is None and profile["volume_created"]
    del docker.volumes[profile["volume"]]
    count = len(docker.calls)
    docker.fail_create = False
    with pytest.raises(errors.RuntimeUnavailable, match="volume disappeared"):
        client.initialize()
    assert client._load() == profile
    assert not any(args[0] in ("run", "create") or args[:2] == ("volume", "create") for args, _ in docker.calls[count:])


def test_uncertain_jobs_prevent_docker_stop_and_clean_stop_authorization(runtime):
    client, docker, _ = runtime
    docker.active = [{"id": "lost-attempt", "state": "LOST"}]
    with pytest.raises(TimeoutError, match="uncertain jobs"):
        client.stop(timeout=0)
    assert not client._load()["stopped_cleanly"]
    assert not any(args[0] == "stop" for args, _ in docker.calls)


def test_failed_docker_stop_does_not_authorize_image_replacement(runtime):
    client, docker, _ = runtime
    docker.fail_stop = True
    with pytest.raises(errors.RuntimeUnavailable, match="Docker stop failure"):
        client.stop(timeout=1)
    assert not client._load()["stopped_cleanly"]
    docker.image_id = "sha256:replacement-image"
    with pytest.raises(errors.RuntimeUnavailable, match="Stop.*cleanly"):
        client.initialize(wheel="replacement-runtime.whl")
    assert docker.created == 1


@pytest.mark.parametrize("result", [None, [], "not an object", 42])
@pytest.mark.parametrize("operation", ["connect", "initialize"])
def test_malformed_handshake_results_are_typed_transport_errors(runtime, monkeypatch, result, operation):
    client, docker, _ = runtime
    original = client._docker

    def invalid_info(profile, *args, **kwargs):
        if args[0] == "exec" and json.loads(kwargs["input"])["method"] == "info":
            return json.dumps({"protocol": 1, "ok": True, "result": result})
        return original(profile, *args, **kwargs)

    monkeypatch.setattr(client, "_docker", invalid_info)
    with pytest.raises(errors.RuntimeUnavailable, match="(identity differs|does not match)"):
        if operation == "connect":
            client._connect(client._load())
        else:
            client.initialize()
    assert docker.created == 1


@pytest.mark.parametrize(
    "response",
    [
        {"protocol": True, "ok": True, "result": None},
        {"protocol": 1.0, "ok": True, "result": None},
        {"protocol": 1, "ok": 1, "result": None},
        {"protocol": 1, "ok": False, "error": {"type": [], "message": "failure"}},
        {"protocol": 1, "ok": False, "error": {"type": "RuntimeUnavailable", "message": False}},
        {"protocol": 1, "ok": False, "error": None},
    ],
)
def test_malformed_rpc_envelopes_are_typed_transport_errors(tmp_path, monkeypatch, response):
    client = DockerExecutor(tmp_path)
    monkeypatch.setattr(client, "_docker", lambda *args, **kwargs: json.dumps(response))
    with pytest.raises(errors.RuntimeUnavailable, match="Invalid managed bridge"):
        client._raw_rpc({"container_id": "owned"}, "info")


@pytest.mark.parametrize(
    "exception", [errors.AdmissionClosed, errors.UnsupportedBackend, errors.LostJobError, TimeoutError, KeyError]
)
def test_rpc_restores_typed_failures_without_discarding_pending_pools(tmp_path, monkeypatch, exception):
    client = DockerExecutor(tmp_path, pools={"batch": PoolConfig(4)})
    response = {"protocol": 1, "ok": False, "error": {"type": exception.__name__, "message": "precise failure"}}
    monkeypatch.setattr(client, "_docker", lambda *args, **kwargs: json.dumps(response))
    with pytest.raises(exception) as failure:
        client._raw_rpc({"container_id": "owned"}, "initialize")
    assert failure.value.args == ("precise failure",)
    assert client._pools["batch"]["max_jobs"] == 4


def test_handshakes_and_failed_initialization_retain_pending_pools(tmp_path, monkeypatch):
    client = DockerExecutor(tmp_path, pools={"batch": PoolConfig(4)})
    responses = [
        {"protocol": 1, "ok": True, "result": {}},
        {"protocol": 1, "ok": True, "result": {}},
        {"protocol": 1, "ok": False, "error": {"type": "RuntimeUnavailable", "message": "retry initialization"}},
        {"protocol": 1, "ok": True, "result": None},
    ]
    requests = []

    def docker(profile, *args, **kwargs):
        requests.append(json.loads(kwargs["input"]))
        return json.dumps(responses.pop(0))

    monkeypatch.setattr(client, "_docker", docker)
    profile = {"container_id": "owned"}
    client._raw_rpc(profile, "info")
    client._raw_rpc(profile, "runtime_state")
    with pytest.raises(errors.RuntimeUnavailable, match="retry initialization"):
        client._raw_rpc(profile, "initialize")
    assert client._pools["batch"]["max_jobs"] == 4
    client._raw_rpc(profile, "initialize")
    assert client._pools is None
    assert all(request["pools"]["batch"]["max_jobs"] == 4 for request in requests)

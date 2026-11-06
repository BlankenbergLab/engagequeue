"""Only verified container signals authorize application stop-exit receipts."""

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from engagequeue import ContainerSpec, Executor, JobSpec
from engagequeue.backends.containers import ContainerUnavailable, OwnedContainer
from engagequeue.backends.native import _container_stop_receipt, _stop_container_requests
from engagequeue.store import Store


@pytest.fixture
def receipt_job(tmp_path):
    store = Store(tmp_path / "state")
    container = ContainerSpec("owned-service", "receipt-attempt", label_key="test.owner")
    spec = JobSpec(["/bin/true"], str(tmp_path), containers=[container]).to_dict()
    identifier = "receipt-execution"
    store.create(
        {
            "id": identifier,
            "key": "application:receipt:attempt",
            "digest": "fixture-digest",
            "state": "RUNNING",
            "pool": "default",
            "created_at": 1.0,
            "spec": spec,
            "cancel_grace": 10.0,
            "cancel_requested": False,
            "writers_stopped": False,
        }
    )
    return store, identifier, container.to_dict()


class FakeRuntime:
    def __init__(self, configuration):
        self.container = OwnedContainer(configuration)
        self.container._execute = self.execute
        self.exists = True
        self.running = True
        self.exit_code = 143
        self.oom_killed = False
        self.signal_success = True
        self.exit_during_failed_signal = False
        self.calls = []

    def execute(self, *arguments):
        self.calls.append(arguments)
        if arguments[0] == "inspect":
            if not self.exists:
                return SimpleNamespace(returncode=1, stdout="", stderr="Error: No such object: " + arguments[1])
            configuration = self.container.configuration
            return SimpleNamespace(
                returncode=0,
                stderr="",
                stdout=json.dumps(
                    [
                        {
                            "Id": "a" * 64,
                            "Name": "/" + configuration["name"],
                            "Config": {"Labels": {configuration["label_key"]: configuration["label_value"]}},
                            "State": {
                                "Running": self.running,
                                "ExitCode": self.exit_code,
                                "OOMKilled": self.oom_killed,
                            },
                        }
                    ]
                ),
            )
        assert arguments[0] == "kill"
        if self.exit_during_failed_signal:
            self.running = False
        return SimpleNamespace(returncode=0 if self.signal_success else 1, stdout="", stderr="signal failed")

    def apply(self, store, identifier):
        _stop_container_requests(store, identifier, store.get(identifier), [self.container])


def receipt_path(store, identifier):
    return Path(store.container_stop_receipt_path(identifier, "owned-service"))


def test_public_receipt_path_validates_target_without_creating_files_or_runtime(receipt_job, monkeypatch):
    store, identifier, _ = receipt_job
    with Executor(store.root, attach_only=True) as client:
        monkeypatch.setattr(client, "ensure_runtime", lambda: pytest.fail("Path lookup must not start runtime"))
        before = {str(path.relative_to(store.root)) for path in store.root.rglob("*")}
        path = client.container_stop_receipt_path(identifier, "owned-service")
        assert path == client.container_stop_receipt_path(identifier, "owned-service")
        assert path.endswith("container-stop-" + hashlib.sha256(b"owned-service").hexdigest() + ".json")
        for invalid in ("unknown", "", None):
            with pytest.raises(ValueError):
                client.container_stop_receipt_path(identifier, invalid)
        with pytest.raises(KeyError):
            client.container_stop_receipt_path("unknown-job", "owned-service")
        assert {str(path.relative_to(store.root)) for path in store.root.rglob("*")} == before
        assert not receipt_path(store, identifier).exists()


@pytest.mark.parametrize("outcome", ["intent_only", "not_created", "natural_exit", "signal_failed", "exit_race"])
def test_intent_or_unverified_signal_cannot_create_stop_receipt(receipt_job, outcome):
    store, identifier, configuration = receipt_job
    runtime = FakeRuntime(configuration)
    store.request_container_stop(identifier, "owned-service")
    if outcome == "intent_only":
        _container_stop_receipt(store, identifier, "owned-service")
    else:
        runtime.exists = outcome != "not_created"
        runtime.running = outcome != "natural_exit"
        runtime.signal_success = outcome not in ("signal_failed", "exit_race")
        runtime.exit_during_failed_signal = outcome == "exit_race"
        if outcome == "signal_failed":
            with pytest.raises(ContainerUnavailable):
                runtime.apply(store, identifier)
        else:
            runtime.apply(store, identifier)
            if outcome == "exit_race":
                runtime.apply(store, identifier)
    assert not receipt_path(store, identifier).exists()
    assert not store.get(identifier)["cancel_requested"]


def test_verified_signal_receipt_freezes_execution_and_container_ownership(receipt_job):
    store, identifier, configuration = receipt_job
    runtime = FakeRuntime(configuration)
    store.request_container_stop(identifier, "owned-service")
    runtime.apply(store, identifier)
    path = receipt_path(store, identifier)
    receipt = json.loads(path.read_text())
    assert set(receipt) == {
        "schema",
        "job_id",
        "key",
        "name",
        "container_id",
        "label_key",
        "label_value",
        "issued_signal",
        "issued_at",
        "stopped_at",
        "exit_code",
        "oom_killed",
        "job_cancel_requested",
        "status",
    }
    assert {
        key: receipt[key] for key in ("schema", "job_id", "key", "name", "container_id", "label_key", "label_value")
    } == {
        "schema": 1,
        "job_id": identifier,
        "key": "application:receipt:attempt",
        "name": "owned-service",
        "container_id": "a" * 64,
        "label_key": "test.owner",
        "label_value": "receipt-attempt",
    }
    assert receipt["issued_signal"] == "TERM" and receipt["issued_at"] > 0
    assert receipt["status"] == "signal_issued" and not receipt["job_cancel_requested"]
    assert receipt["exit_code"] is None and receipt["oom_killed"] is None and receipt["stopped_at"] is None
    assert path.stat().st_mode & 0o077 == 0


@pytest.mark.parametrize(
    "exit_code,oom_killed,removed", [(143, False, False), (137, True, False), (137, False, False), (None, None, True)]
)
def test_confirmation_preserves_observed_exit_oom_and_unknown_auto_removal(receipt_job, exit_code, oom_killed, removed):
    store, identifier, configuration = receipt_job
    runtime = FakeRuntime(configuration)
    store.request_container_stop(identifier, "owned-service")
    runtime.apply(store, identifier)
    original = json.loads(receipt_path(store, identifier).read_text())
    runtime.running = False
    runtime.exit_code, runtime.oom_killed, runtime.exists = exit_code, oom_killed, not removed
    runtime.apply(store, identifier)
    receipt = json.loads(receipt_path(store, identifier).read_text())
    assert receipt["status"] == "stopped" and receipt["stopped_at"] >= receipt["issued_at"]
    assert receipt["exit_code"] == exit_code and receipt["oom_killed"] is oom_killed
    assert {
        key: receipt[key]
        for key in ("job_id", "key", "name", "container_id", "label_key", "label_value", "issued_signal", "issued_at")
    } == {
        key: original[key]
        for key in ("job_id", "key", "name", "container_id", "label_key", "label_value", "issued_signal", "issued_at")
    }


def test_global_cancellation_invalidates_prior_graceful_stop_receipt(receipt_job):
    store, identifier, configuration = receipt_job
    runtime = FakeRuntime(configuration)
    store.request_container_stop(identifier, "owned-service")
    runtime.apply(store, identifier)
    store.request_cancel(identifier)
    _container_stop_receipt(store, identifier, "owned-service")
    receipt = json.loads(receipt_path(store, identifier).read_text())
    assert receipt["job_cancel_requested"] is True
    assert receipt["issued_signal"] == "TERM" and receipt["container_id"] == "a" * 64

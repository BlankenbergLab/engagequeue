"""Opt-in cleanup must not change legacy accepted specifications or keys."""

import hashlib
import uuid
from pathlib import Path

import pytest

from engagequeue import ContainerSpec, Executor, JobSpec, JobState, SubmissionConflict
from engagequeue.store import dump


def legacy_container():
    # The complete public ContainerSpec dictionary before remove_on_exit.
    return {
        "name": "owned-service",
        "label_value": "attempt-token",
        "runtime": "docker",
        "label_key": "engagequeue.attempt",
        "cid_file": None,
        "executable": "docker",
        "host": None,
    }


def legacy_job(directory, placement):
    # Literal old shape rather than the new dataclass/asdict implementation.
    return {
        "argv": ["/bin/true"],
        "cwd": str(directory.resolve()),
        "env": {},
        "stdout": None,
        "stderr": None,
        "pool": "default",
        "timeout": None,
        "output_limit": None,
        "output_paths": [],
        "memory_mb": None,
        "cpus": None,
        "container": legacy_container() if placement != "plural" else None,
        "containers": [legacy_container()] if placement != "singular" else [],
        "require_hard_limits": False,
    }


def spec(directory, placement, remove_on_exit=False):
    container = ContainerSpec("owned-service", "attempt-token", remove_on_exit=remove_on_exit)
    return JobSpec(
        ["/bin/true"],
        str(directory),
        env={},
        container=container if placement != "plural" else None,
        containers=[container] if placement != "singular" else [],
    )


def test_default_container_dictionary_matches_legacy_shape():
    container = ContainerSpec("owned-service", "attempt-token")
    assert container.remove_on_exit is False
    assert container.to_dict() == legacy_container()


@pytest.mark.parametrize("placement", ["singular", "plural", "both"])
def test_false_omission_preserves_legacy_canonical_bytes_and_roundtrip(tmp_path, placement):
    serialized = spec(tmp_path, placement).to_dict()
    expected = legacy_job(tmp_path, placement)
    assert serialized == expected
    assert dump(serialized).encode() == dump(expected).encode()
    assert "remove_on_exit" not in dump(serialized)
    assert JobSpec.from_dict(expected).to_dict() == expected
    assert JobSpec.from_dict(serialized).to_dict() == expected


@pytest.mark.parametrize("placement", ["singular", "plural", "both"])
def test_true_policy_is_explicit_and_survives_roundtrip(tmp_path, placement):
    serialized = spec(tmp_path, placement, True).to_dict()
    containers = [*serialized["containers"]]
    if serialized["container"] is not None:
        containers.append(serialized["container"])
    assert containers and all(container["remove_on_exit"] is True for container in containers)
    assert JobSpec.from_dict(serialized).to_dict() == serialized
    assert dump(serialized) != dump(legacy_job(tmp_path, placement))


@pytest.mark.parametrize("placement", ["singular", "plural", "both"])
def test_new_client_adopts_legacy_key_and_rejects_changed_cleanup_policy(tmp_path, monkeypatch, placement):
    with Executor(tmp_path / "state") as client:
        key = "legacy:accepted:" + placement
        identifier = str(uuid.uuid5(uuid.UUID(client.store.get_meta("runtime_id")), key))
        accepted = legacy_job(tmp_path, placement)
        accepted["stdout"] = str(Path(client.state_dir) / "jobs" / identifier / "stdout")
        accepted["stderr"] = str(Path(client.state_dir) / "jobs" / identifier / "stderr")
        digest = hashlib.sha256(dump(accepted).encode()).hexdigest()
        client.store.create(
            {
                "id": identifier,
                "key": key,
                "digest": digest,
                "spec": accepted,
                "state": "SUCCEEDED",
                "pool": "default",
                "backend": "native",
                "cancel_grace": 10.0,
                "created_at": 1.0,
                "returncode": 0,
                "cancel_requested": False,
                "writers_stopped": True,
            }
        )
        monkeypatch.setattr(
            client, "ensure_runtime", lambda: pytest.fail("Legacy terminal adoption must not start work")
        )
        before = client.inspect(identifier)
        handle = client.submit(spec(tmp_path, placement), key=key)
        assert handle.id == identifier and handle.state == JobState.SUCCEEDED
        assert client.inspect(identifier) == before
        with pytest.raises(SubmissionConflict):
            client.submit(spec(tmp_path, placement, True), key=key)
        assert client.inspect(identifier) == before


@pytest.mark.parametrize("invalid", [None, 0, 1, "false", []])
def test_cleanup_policy_requires_a_boolean(invalid):
    with pytest.raises(ValueError, match="remove_on_exit"):
        ContainerSpec("owned-service", "attempt-token", remove_on_exit=invalid)

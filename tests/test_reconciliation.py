"""Reconciliation needs durable identity and proof that all writers stopped."""

import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from engagequeue import ContainerSpec, Executor, JobSpec, JobState, LostJobError, PoolConfig
from engagequeue.backends.containers import ContainerOwnershipError, ContainerUnavailable, OwnedContainer
from engagequeue.backends.native import resources_stopped
from engagequeue.backends.processes import boot_identity, identity
from engagequeue.backends.systemd import SystemdUnavailable, Unit
from engagequeue.runtime import Runtime

FIRST = "a" * 64
SECOND = "b" * 64
THIRD = "c" * 64


def configuration(name="owned", **kwargs):
    return ContainerSpec(name, "attempt", label_key="owner", **kwargs).to_dict()


def observation(name="owned", identifier=FIRST, running=False, **changes):
    return {
        "Id": identifier,
        "Name": "/" + name,
        "Config": {"Labels": {"owner": "attempt"}},
        "State": {"Running": running, "Restarting": False},
        **changes,
    }


def daemon(monkeypatch, responses):
    calls = []

    def execute(container, *args):
        assert args[0] == "inspect", "Reconciliation must not signal or remove resources"
        calls.append((dict(container.configuration), args[1]))
        response = responses[args[1]]
        if isinstance(response, Exception):
            raise response
        if response is None:
            return SimpleNamespace(returncode=1, stdout="[]", stderr="Error: No such object: " + args[1])
        return SimpleNamespace(returncode=0, stdout=json.dumps([response]), stderr="")

    monkeypatch.setattr(OwnedContainer, "_execute", execute)
    return calls


def record(*, spec=None, backend="native", started=True, reboot=True, **kwargs):
    boot = boot_identity()
    if reboot:
        boot = {**boot, "boot_id": "previous-boot"}
    return {
        "id": "job",
        "key": "job",
        "digest": "test",
        "state": "RUNNING",
        "pool": "default",
        "created_at": time.time(),
        "backend": backend,
        "worker_token": "worker-token",
        "cancel_grace": 0.1,
        "spec": {} if spec is None else spec,
        "worker_boot": boot,
        "admitted_boot_id": boot["boot_id"],
        "admitted_pid_namespace": int(boot["pid_namespace"]),
        "execution_granted": started,
        "command_started": 1.0 if started else None,
        **kwargs,
    }


def manager(monkeypatch, *, quiescent):
    # Exercise the real Unit methods: an empty process enumeration is not a
    # stopped unit when its state/accounting still reports work.
    monkeypatch.setattr(
        Unit,
        "show",
        lambda self: {
            "LoadState": "loaded",
            "ControlGroup": "",
            "SubState": "exited" if quiescent else "running",
            "TasksCurrent": "0" if quiescent else "1",
        },
    )


@pytest.mark.parametrize("quiescent", [False, True])
def test_systemd_reconciliation_checks_kernel_quiescence(monkeypatch, quiescent):
    manager(monkeypatch, quiescent=quiescent)
    assert resources_stopped(record(backend="systemd", reboot=False)) is quiescent


def test_missing_systemd_unit_confirms_quiescence(monkeypatch):
    monkeypatch.setattr(Unit, "show", lambda self: {"LoadState": "not-found"})
    assert resources_stopped(record(backend="systemd")) is True


def test_systemd_query_failure_remains_unknown(monkeypatch):
    def unavailable(self):
        raise SystemdUnavailable("manager unavailable")

    monkeypatch.setattr(Unit, "show", unavailable)
    assert resources_stopped(record(backend="systemd")) is None


@pytest.mark.parametrize("case", ["absent", "stopped", "running", "renamed", "foreign", "unavailable"])
def test_recovered_container_uses_saved_immutable_id(monkeypatch, case):
    response = observation()
    expected = True
    if case == "absent":
        response = None
    elif case == "running":
        response["State"]["Running"] = True
        expected = False
    elif case == "renamed":
        response["Name"] = "/renamed"
        expected = None
    elif case == "foreign":
        response["Config"]["Labels"]["owner"] = "foreign"
        expected = None
    elif case == "unavailable":
        response = ContainerUnavailable("daemon unavailable")
        expected = None
    calls = daemon(monkeypatch, {FIRST: response})
    current = record(spec={"container": configuration()}, owned_container_ids=[FIRST])
    assert resources_stopped(current) is expected
    assert [target for _, target in calls] == [FIRST]


@pytest.mark.parametrize("placement", ["singular", "plural", "mixed_duplicates"])
def test_saved_ids_align_with_supervisors_unique_configuration_order(monkeypatch, placement):
    first = configuration("first")
    second = configuration("second")
    third = configuration("third")
    if placement == "singular":
        spec = {"container": first}
        identifiers = [FIRST]
        names = ["first"]
    elif placement == "plural":
        spec = {"containers": [first, second]}
        identifiers = [FIRST, SECOND]
        names = ["first", "second"]
    else:
        # Last configuration wins, but duplicate declarations do not shift the
        # persisted identities for the subsequent distinct containers.
        spec = {
            "containers": [first, second, configuration("first", remove_on_exit=True)],
            "container": third,
        }
        identifiers = [FIRST, SECOND, THIRD]
        names = ["first", "second", "third"]
    calls = daemon(
        monkeypatch,
        {identifier: observation(name, identifier) for name, identifier in zip(names, identifiers)},
    )
    assert resources_stopped(record(spec=spec, owned_container_ids=identifiers)) is True
    assert [(config["name"], target) for config, target in calls] == list(zip(names, identifiers))
    if placement == "mixed_duplicates":
        assert calls[0][0]["remove_on_exit"] is True


@pytest.mark.parametrize("pins", [None, "not-a-list", [], [FIRST, SECOND], ["short"], [123]])
def test_malformed_saved_id_mapping_cannot_confirm_stopped(monkeypatch, pins):
    daemon(monkeypatch, {"owned": observation()})
    assert resources_stopped(record(spec={"container": configuration()}, owned_container_ids=pins)) is None


@pytest.mark.parametrize("saved", [False, True])
@pytest.mark.parametrize("started", [False, True])
def test_legacy_or_unobserved_name_absence_does_not_prove_a_started_container_stopped(monkeypatch, saved, started):
    daemon(monkeypatch, {"owned": None})
    current = record(spec={"container": configuration()}, started=started)
    if saved:
        current["owned_container_ids"] = [None]
    assert resources_stopped(current) is (None if started else True)


def test_known_live_container_dominates_an_earlier_unknown_identity(monkeypatch):
    daemon(
        monkeypatch,
        {FIRST: ContainerUnavailable("first unavailable"), SECOND: observation("second", SECOND, running=True)},
    )
    current = record(
        spec={"containers": [configuration(), configuration("second")]}, owned_container_ids=[FIRST, SECOND]
    )
    assert resources_stopped(current) is False


def test_live_cgroup_dominates_unknown_container(monkeypatch):
    daemon(monkeypatch, {FIRST: ContainerUnavailable("daemon unavailable")})
    manager(monkeypatch, quiescent=False)
    current = record(backend="systemd", spec={"container": configuration()}, owned_container_ids=[FIRST])
    assert resources_stopped(current) is False


def test_live_native_process_dominates_unknown_container(monkeypatch):
    daemon(monkeypatch, {FIRST: ContainerUnavailable("daemon unavailable")})
    process = identity(os.getpid())
    current = record(
        reboot=False,
        spec={"container": configuration()},
        owned_container_ids=[FIRST],
        payload_pid=process["pid"],
        payload_birth=process["birth"],
    )
    assert resources_stopped(current) is False


def test_recorded_live_process_dominates_unavailable_systemd_manager(monkeypatch):
    def unavailable(self):
        raise SystemdUnavailable("manager unavailable")

    monkeypatch.setattr(Unit, "show", unavailable)
    process = identity(os.getpid())
    current = record(backend="systemd", reboot=False, payload_pid=process["pid"], payload_birth=process["birth"])
    assert resources_stopped(current) is False


@pytest.mark.parametrize("contents", [None, FIRST, FIRST[:12]])
def test_cid_file_cannot_replace_the_full_pinned_query(tmp_path, monkeypatch, contents):
    cid = tmp_path / "cid"
    if contents is not None:
        cid.write_text(contents)
    container = OwnedContainer(configuration(cid_file=str(cid)))
    container.pinned_id = FIRST
    calls = daemon(monkeypatch, {FIRST: observation()})
    assert container.inspect()["Id"] == FIRST
    assert [target for _, target in calls] == [FIRST]


@pytest.mark.parametrize("contents", ["", "not-an-id", SECOND, SECOND[:12]])
def test_conflicting_or_invalid_cid_never_authorizes_absence(tmp_path, monkeypatch, contents):
    cid = tmp_path / "cid"
    cid.write_text(contents)
    container = OwnedContainer(configuration(cid_file=str(cid)))
    container.pinned_id = FIRST
    # In particular, querying an absent SECOND must not erase evidence of FIRST.
    calls = daemon(monkeypatch, {SECOND: None, SECOND[:12]: None})
    with pytest.raises(ContainerOwnershipError):
        container.inspect()
    assert calls == []


def test_unreadable_cid_file_remains_ownership_uncertainty(tmp_path, monkeypatch):
    cid = tmp_path / "cid"
    original = Path.read_text

    def read(path, *args, **kwargs):
        if path == cid:
            raise PermissionError("unreadable CID")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read)
    container = OwnedContainer(configuration(cid_file=str(cid)))
    container.pinned_id = FIRST
    calls = daemon(monkeypatch, {})
    with pytest.raises(ContainerOwnershipError, match="Cannot read"):
        container.inspect()
    assert calls == []


def test_unpinned_cid_prefix_must_match_the_inspected_id(tmp_path, monkeypatch):
    cid = tmp_path / "cid"
    cid.write_text(FIRST[:12])
    container = OwnedContainer(configuration(cid_file=str(cid)))
    daemon(monkeypatch, {FIRST[:12]: observation(identifier=SECOND)})
    with pytest.raises(ContainerOwnershipError):
        container.inspect()


def accepted_job(client, tmp_path, *, state="LOST", backend="native", containers=(), reboot=True, **kwargs):
    spec = JobSpec(["/bin/true"], str(tmp_path), containers=containers).to_dict()
    current = record(spec=spec, state=state, backend=backend, reboot=reboot, **kwargs)
    client.store.create(current)
    return current


def queued_job(client, tmp_path):
    client.store.create(
        record(
            id="waiting",
            key="waiting",
            state="QUEUED",
            started=False,
            reboot=False,
            spec=JobSpec(["/bin/true"], str(tmp_path)).to_dict(),
        )
    )


def test_public_reconcile_cannot_attest_away_a_populated_systemd_unit(tmp_path, monkeypatch):
    manager(monkeypatch, quiescent=False)
    with Executor(tmp_path / "state", pools={"default": PoolConfig(1, "systemd")}) as client:
        accepted_job(client, tmp_path, backend="systemd", reboot=False)
        queued_job(client, tmp_path)
        with pytest.raises(LostJobError, match="still present"):
            client.reconcile("job", confirm_stopped=True, reason="Attempted override")
        current = client.store.get("job")
        assert current["state"] == "LOST" and not current["writers_stopped"]
        assert client.store.reserve_next("default") is None
        assert client.store.get_meta("runtime") is None


def test_public_reconcile_records_verified_empty_systemd_unit(tmp_path, monkeypatch):
    manager(monkeypatch, quiescent=True)
    with Executor(tmp_path / "state", pools={"default": PoolConfig(1, "systemd")}) as client:
        accepted_job(client, tmp_path, backend="systemd", reboot=False)
        status = client.reconcile("job", confirm_stopped=True, reason="Verified owned cgroup empty")
        assert status.state is JobState.FAILED and status.writers_stopped
        assert client.store.get("job")["resource_verification"] == "verified"


def test_public_reconcile_does_not_skip_a_live_container_after_an_unknown_one(tmp_path, monkeypatch):
    daemon(
        monkeypatch,
        {FIRST: ContainerUnavailable("first unavailable"), SECOND: observation("second", SECOND, running=True)},
    )
    with Executor(tmp_path / "state") as client:
        accepted_job(
            client,
            tmp_path,
            containers=[
                ContainerSpec("owned", "attempt", label_key="owner"),
                ContainerSpec("second", "attempt", label_key="owner"),
            ],
            owned_container_ids=[FIRST, SECOND],
        )
        with pytest.raises(LostJobError, match="still present"):
            client.reconcile("job", confirm_stopped=True, reason="Attempted override")
        assert client.store.get("job")["state"] == "LOST"


@pytest.mark.parametrize("case", ["absent", "stopped", "running", "renamed", "foreign", "unavailable"])
def test_reboot_recovery_does_not_release_unresolved_remote_containers(tmp_path, monkeypatch, case):
    response = observation()
    if case == "absent":
        response = None
    elif case == "running":
        response["State"]["Running"] = True
    elif case == "renamed":
        response["Name"] = "/renamed"
    elif case == "foreign":
        response["Config"]["Labels"]["owner"] = "foreign"
    elif case == "unavailable":
        response = ContainerUnavailable("remote daemon unavailable")
    calls = daemon(monkeypatch, {FIRST: response})
    with Executor(tmp_path / "state") as client:
        accepted_job(
            client,
            tmp_path,
            state="RUNNING",
            containers=[ContainerSpec("owned", "attempt", label_key="owner", host="ssh://remote")],
            owned_container_ids=[FIRST],
        )
        queued_job(client, tmp_path)
        Runtime(str(client.store.root)).reconcile()
        current = client.store.get("job")
        if case in ("absent", "stopped"):
            assert current["state"] == "FAILED" and current["writers_stopped"]
            assert client.store.reserve_next("default")["id"] == "waiting"
        else:
            assert current["state"] == "LOST" and not current["writers_stopped"]
            assert client.store.reserve_next("default") is None
        assert [target for _, target in calls] == [FIRST]
        assert client.store.get_meta("runtime") is None

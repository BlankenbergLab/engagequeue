"""Platform identity and fresh-submission capability checks."""

import ctypes
import errno
from types import SimpleNamespace

import pytest

from engagequeue import Executor, JobSpec, JobState, PoolConfig, UnsupportedBackend, _platform
from engagequeue.backends.processes import boot_identity as process_boot_identity
from engagequeue.errors import RuntimeUnavailable
from engagequeue.executor import boot_identity, pid_namespace


def test_linux_ownership_values_keep_the_existing_wire_format(monkeypatch):
    monkeypatch.setattr(_platform, "system_name", lambda: "linux")
    monkeypatch.setattr(
        _platform,
        "Path",
        lambda path: SimpleNamespace(read_text=lambda: "kernel-boot-id\n", stat=lambda: SimpleNamespace(st_ino=1234)),
    )
    assert boot_identity() == "kernel-boot-id"
    assert pid_namespace() == 1234
    assert process_boot_identity() == {"boot_id": "kernel-boot-id", "pid_namespace": "1234"}


def test_darwin_ownership_identity_does_not_probe_procfs(monkeypatch):
    monkeypatch.setattr(_platform, "system_name", lambda: "darwin")
    monkeypatch.setattr(_platform, "_darwin_boot_id", lambda: "darwin:kernel-uuid")
    monkeypatch.setattr(_platform, "Path", lambda path: pytest.fail("Darwin must not access procfs"))
    assert boot_identity() == "darwin:kernel-uuid"
    assert pid_namespace() == 0
    assert process_boot_identity() == {"boot_id": "darwin:kernel-uuid", "pid_namespace": "0"}


@pytest.mark.parametrize("failure", [None, "denied", "invalid"])
def test_darwin_boot_session_uses_the_kernel_uuid_and_fails_closed(monkeypatch, failure):
    query_calls = []

    class Query:
        def __call__(self, name, buffer, size, new_value, new_size):
            query_calls.append(name)
            if failure == "denied":
                ctypes.set_errno(errno.EPERM)
                return -1
            buffer.value = b"invalid" if failure == "invalid" else b"A1234567-89AB-4CDE-8FAB-0123456789AB"
            return 0

    monkeypatch.setattr(_platform.ctypes, "CDLL", lambda *args, **kwargs: SimpleNamespace(sysctlbyname=Query()))
    monkeypatch.setattr(_platform.psutil, "boot_time", lambda: pytest.fail("No wall-clock fallback on Darwin"))
    _platform._darwin_boot_id.cache_clear()
    try:
        if failure:
            with pytest.raises(RuntimeUnavailable, match="boot-session"):
                _platform._darwin_boot_id()
        else:
            expected = "darwin:a1234567-89ab-4cde-8fab-0123456789ab"
            assert _platform._darwin_boot_id() == expected
            assert _platform._darwin_boot_id() == expected
            assert len(query_calls) == 1
        assert query_calls == [b"kern.bootsessionuuid"]
    finally:
        _platform._darwin_boot_id.cache_clear()


def test_unsupported_native_platform_is_explicit(monkeypatch):
    monkeypatch.setattr(_platform, "system_name", lambda: "unsupported")
    for operation in (_platform.boot_id, _platform.pid_namespace, _platform.require_native):
        with pytest.raises(UnsupportedBackend):
            operation()


def test_systemd_capability_check_runs_after_receipt_adoption(tmp_path, monkeypatch):
    with Executor(tmp_path / "state") as client:
        monkeypatch.setattr(client, "ensure_runtime", lambda: None)
        monkeypatch.setattr(_platform, "require_native", lambda: None)
        spec = JobSpec(["/bin/true"], str(tmp_path))
        accepted = client.submit(spec, key="accepted")
        assert client.store.transition(accepted.id, "FAILED", writers_stopped=True, returncode=70)
        client.configure_pool("default", PoolConfig(backend="systemd"))
        monkeypatch.setattr(_platform, "system_name", lambda: "darwin")
        assert client.submit(spec, key="accepted").state is JobState.FAILED
        with pytest.raises(UnsupportedBackend, match="systemd.*Linux"):
            client.submit(spec, key="fresh")
        assert client.lookup("fresh") is None


def test_submission_policy_hook_does_not_revalidate_receipts(tmp_path, monkeypatch):
    class PolicyExecutor(Executor):
        validations = 0

        def _validate_new_submission(self, spec, pool):
            super()._validate_new_submission(spec, pool)
            self.validations += 1

    with PolicyExecutor(tmp_path / "state") as client:
        monkeypatch.setattr(client, "ensure_runtime", lambda: None)
        monkeypatch.setattr(_platform, "require_native", lambda: None)
        spec = JobSpec(["/bin/true"], str(tmp_path))
        first = client.submit(spec, key="accepted")
        assert client.submit(spec, key="accepted").id == first.id
        assert client.validations == 1

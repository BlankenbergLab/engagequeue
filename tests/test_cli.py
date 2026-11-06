"""Exercise installed CLI/API interoperability and detached execution."""

import json
import subprocess
import sys

from engagequeue import ContainerSpec, Executor, JobSpec, JobState


def cli(root, *arguments, check=True):
    return subprocess.run(
        [sys.executable, "-m", "engagequeue", "--state-dir", str(root), *arguments],
        capture_output=True,
        text=True,
        check=check,
        timeout=30,
    )


def test_cli_and_library_share_one_accepted_execution(tmp_path):
    root = tmp_path / "state"
    counter = tmp_path / "counter"
    cli(root, "init", "--max-jobs", "1")
    arguments = (
        "submit",
        "--key",
        "cli-once",
        "--cwd",
        str(tmp_path),
        "--",
        sys.executable,
        "-c",
        f"from pathlib import Path; Path({str(counter)!r}).write_text('once'); print('hello')",
    )
    identifier = cli(root, *arguments).stdout.strip()
    assert cli(root, *arguments).stdout.strip() == identifier
    with Executor(root) as executor:
        assert executor.lookup("cli-once").id == identifier
        status = executor.wait(identifier, timeout=15)
        assert status.state == JobState.SUCCEEDED
        inspected = executor.inspect(identifier)
    result = json.loads(cli(root, "status", identifier, "--json").stdout)
    assert result["state"] == "SUCCEEDED"
    assert counter.read_text() == "once"
    assert "env" not in inspected or inspected["env"] == "<redacted>"
    cli(root, "runtime", "shutdown", "--wait", "--timeout", "10")


def test_submit_process_exit_leaves_queue_admission_running(tmp_path):
    root = tmp_path / "state"
    gate = tmp_path / "release"
    cli(root, "init", "--max-jobs", "1")
    first = cli(
        root,
        "submit",
        "--key",
        "first",
        "--cwd",
        str(tmp_path),
        "--",
        sys.executable,
        "-c",
        f"import time; from pathlib import Path\nwhile not Path({str(gate)!r}).exists(): time.sleep(.03)",
    ).stdout.strip()
    second = cli(
        root,
        "submit",
        "--key",
        "second",
        "--cwd",
        str(tmp_path),
        "--",
        sys.executable,
        "-c",
        f"from pathlib import Path; Path({str(tmp_path / 'second')!r}).write_text('finished')",
    ).stdout.strip()
    gate.touch()
    # Every submit CLI process has exited. The independent runtime still admits second.
    cli(root, "wait", first, "--timeout", "15", "--json")
    cli(root, "wait", second, "--timeout", "15", "--json")
    assert (tmp_path / "second").read_text() == "finished"
    cli(root, "runtime", "shutdown", "--wait", "--timeout", "10")


def test_bad_command_arguments_fail_without_accepting_work(tmp_path):
    result = cli(tmp_path, "submit", "--key", "invalid", check=False)
    assert result.returncode == 2
    assert "requires an executable" in result.stderr
    with Executor(tmp_path) as executor:
        assert executor.list() == []


def test_import_and_help_have_no_runtime_side_effects(tmp_path):
    result = subprocess.run(
        [sys.executable, "-c", "import engagequeue; import engagequeue.cli"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert list(tmp_path.iterdir()) == []
    help_result = subprocess.run(
        [sys.executable, "-m", "engagequeue", "--help"], cwd=tmp_path, capture_output=True, text=True, timeout=10
    )
    assert help_result.returncode == 0
    assert "Local jobs. Make it so." in help_result.stdout


def test_stop_container_cli_keeps_terminal_receipt_and_rejects_undeclared_names(tmp_path, monkeypatch):
    root = tmp_path / "state"
    # A confirmed prelaunch failure has no live processes. Retain a declared
    # container to exercise the public CLI/API against a persistent receipt.
    with Executor(root) as executor:
        monkeypatch.setattr(executor, "ensure_runtime", lambda: None)
        job = executor.submit(
            JobSpec(["/bin/true"], str(tmp_path), containers=[ContainerSpec("owned-service", "attempt-token")]),
            key="cli-container-stop",
        )
        assert executor.store.transition(job.id, "FAILED", writers_stopped=True, reason="Prelaunch test failure")
        before = executor.inspect(job.id)
    result = cli(root, "stop-container", job.id, "owned-service", "--json")
    assert json.loads(result.stdout)["state"] == "FAILED"
    rejected = cli(root, "stop-container", job.id, "unrelated-service", check=False)
    assert rejected.returncode == 2
    missing = cli(root, "stop-container", "unknown-job", "owned-service", check=False)
    assert missing.returncode == 2
    with Executor(root) as executor:
        after = executor.inspect(job.id)
        assert after == before
        assert not executor._runtime_running()

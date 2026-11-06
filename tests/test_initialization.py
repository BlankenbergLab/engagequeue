"""Fresh processes serialize first use without inheriting SQLite connections."""

import contextlib
import multiprocessing
import sqlite3

import pytest

from engagequeue.store import SCHEMA_VERSION, Store


def _initialize_process(root, output, barrier=None, entered=None, release=None, started=None):
    original_connect = Store._connect

    def connect(self):
        if entered is not None:
            entered.set()
        if release is not None and not release.wait(15):
            raise TimeoutError("Initialization test was not released")
        return original_connect(self)

    Store._connect = connect
    if started is not None:
        started.set()
    try:
        if barrier is not None:
            barrier.wait(15)
        store = Store(root)
        with contextlib.closing(store._connect()) as connection:
            mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        output.put(("ok", store.get_meta("runtime_id"), store.get_meta("schema"), mode))
    except BaseException as exc:
        output.put(("error", type(exc).__name__, str(exc)))


def _stop_processes(processes):
    for process in processes:
        if process.is_alive():
            process.terminate()
        if process.pid is not None:
            process.join(5)


def _results(processes, output):
    results = [output.get(timeout=20) for _ in processes]
    for process in processes:
        process.join(10)
        assert process.exitcode == 0
    assert all(result[0] == "ok" for result in results), results
    return results


def test_concurrent_first_use_from_fresh_processes(tmp_path):
    context = multiprocessing.get_context("spawn")
    output = context.Queue()
    barrier = context.Barrier(6)
    processes = [
        context.Process(target=_initialize_process, args=(str(tmp_path / "state"), output, barrier)) for _ in range(6)
    ]
    try:
        for process in processes:
            process.start()
        results = _results(processes, output)
        assert len({result[1] for result in results}) == 1
        assert {result[2] for result in results} == {SCHEMA_VERSION}
        assert {result[3] for result in results} == {"wal"}
    finally:
        _stop_processes(processes)
        output.close()
        output.join_thread()


def test_initialization_lock_precedes_every_sqlite_connection(tmp_path):
    context = multiprocessing.get_context("spawn")
    output = context.Queue()
    first_entered, second_entered = context.Event(), context.Event()
    release, second_started = context.Event(), context.Event()
    root = str(tmp_path / "state")
    first = context.Process(target=_initialize_process, args=(root, output, None, first_entered, release))
    second = context.Process(
        target=_initialize_process, args=(root, output, None, second_entered, None, second_started)
    )
    processes = [first, second]
    try:
        first.start()
        assert first_entered.wait(15)
        second.start()
        assert second_started.wait(15)
        # The first process is paused before opening SQLite. A competing
        # constructor must wait at the filesystem lock, not open another reader.
        assert not second_entered.wait(0.5)
        release.set()
        results = _results(processes, output)
        assert second_entered.is_set()
        assert results[0][1] == results[1][1]
    finally:
        release.set()
        _stop_processes(processes)
        output.close()
        output.join_thread()


def test_initialization_failure_propagates_and_releases_lock(tmp_path, monkeypatch):
    root = tmp_path / "state"
    failure = sqlite3.OperationalError("simulated initialization failure")

    def fail_connect(self):
        raise failure

    with monkeypatch.context() as patch:
        patch.setattr(Store, "_connect", fail_connect)
        with pytest.raises(sqlite3.OperationalError) as raised:
            Store(root)
        assert raised.value is failure

    context = multiprocessing.get_context("spawn")
    output = context.Queue()
    process = context.Process(target=_initialize_process, args=(str(root), output))
    try:
        process.start()
        result = _results([process], output)[0]
        assert result[2:] == (SCHEMA_VERSION, "wal")
    finally:
        _stop_processes([process])
        output.close()
        output.join_thread()

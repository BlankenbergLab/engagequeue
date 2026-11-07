"""Managed lifecycle operations serialize against other host clients."""

import concurrent.futures
import threading
import time
from types import SimpleNamespace

from engagequeue import DockerExecutor


def test_stop_keeps_other_clients_from_resuming_between_drain_and_container_stop(tmp_path, monkeypatch):
    import engagequeue.managed as managed

    stopping = DockerExecutor(tmp_path)
    resuming = DockerExecutor(tmp_path)
    profile = {"container_id": "owned"}
    sleeping = threading.Event()
    release = threading.Event()
    attempting_resume = threading.Event()
    resumed = threading.Event()
    calls = []
    polls = 0

    def sleep(_):
        sleeping.set()
        assert release.wait(timeout=5), "Test did not release the draining runtime"

    def raw_rpc(_, method, args=None, **kwargs):
        nonlocal polls
        if method == "runtime_state":
            polls += 1
            return {"running": polls == 1, "active": []}
        calls.append(method)
        if method == "resume":
            assert "docker-stop" in calls
            resumed.set()

    def docker(_, *args, **kwargs):
        assert args[:1] == ("stop",)
        calls.append("docker-stop")

    for client in (stopping, resuming):
        monkeypatch.setattr(client, "_load", lambda: profile)
        monkeypatch.setattr(client, "_connect", lambda *a, **k: None)
        monkeypatch.setattr(client, "_raw_rpc", raw_rpc)
        monkeypatch.setattr(client, "_save", lambda *a: None)
    monkeypatch.setattr(stopping, "_docker", docker)
    monkeypatch.setattr(managed, "time", SimpleNamespace(monotonic=time.monotonic, sleep=sleep))

    def resume():
        attempting_resume.set()
        resuming.resume()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as threads:
        stop = threads.submit(stopping.stop, timeout=5)
        assert sleeping.wait(timeout=5)
        retry = threads.submit(resume)
        try:
            assert attempting_resume.wait(timeout=5)
            assert not resumed.wait(timeout=0.1)
        finally:
            release.set()
        stop.result(timeout=5)
        retry.result(timeout=5)
    assert resumed.is_set()
    assert calls == ["shutdown", "docker-stop", "resume"]

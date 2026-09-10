"""In-process jobd tests. Stub jobs finish in <1s. Hard 5s waits.

``CRYOBT_JOB_STUB=1`` keeps the queue/cancel tests off ``run_backtest``.
The real runner is covered in test_runner_real.py and test_acceptance_two_jobs.py.
"""
from __future__ import annotations

import time

import pytest

from backtester.job.api import JobSpec, JobStore
from backtester.job.supervisor import Supervisor


def _spec() -> JobSpec:
    return JobSpec(strategy="stub", param_grid={"x": [0]}, source="test")


def _wait(pred, *, timeout=5.0, interval=0.05, msg="condition"):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return
        time.sleep(interval)
    raise TimeoutError(f"{msg} not met in {timeout:.1f}s")


@pytest.fixture
def sup(tmp_path, monkeypatch):
    monkeypatch.setenv("CRYOBT_JOBS", str(tmp_path))
    monkeypatch.setenv("CRYOBT_JOB_STUB", "1")
    monkeypatch.setenv("CRYOBT_JOB_STUB_SECS", "0.4")
    s = Supervisor(tmp_path, stub=True)
    yield s
    s.kill_remaining()


def test_j1_enqueue_starts_running(sup):
    resp = sup.enqueue(_spec())
    job_id = resp["job_id"]
    assert (sup.root / job_id / "spec.json").is_file()
    _wait(
        lambda: (sup.store.get(job_id) or type("V", (), {"state": ""})).state == "running",
        msg="job running",
    )
    sup.reap()


def test_q1_second_job_stays_queued(sup):
    a = sup.enqueue(_spec())["job_id"]
    _wait(lambda: sup.store.get(a).state == "running", msg="A running")
    b = sup.enqueue(_spec())["job_id"]
    assert b in sup.queued
    assert b not in sup.running
    assert a in sup.running


def test_j4_cancel_queued_never_starts(sup):
    a = sup.enqueue(_spec())["job_id"]
    _wait(lambda: a in sup.running, msg="A running")
    b = sup.enqueue(_spec())["job_id"]
    resp = sup.cancel(b)
    assert resp["state"] == "cancelled"
    assert b not in sup.queued
    time.sleep(0.2)
    sup.reap()
    assert sup.store.get(b).state == "cancelled"
    assert b not in sup.running


def test_cancel_running_promotes_queued(sup, monkeypatch):
    """Cancel the active job must free the slot and start the next queued one."""
    monkeypatch.setenv("CRYOBT_JOB_STUB_SECS", "5")
    a = sup.enqueue(_spec())["job_id"]
    _wait(lambda: a in sup.running, msg="A running")
    b = sup.enqueue(_spec())["job_id"]
    assert b in sup.queued
    resp = sup.cancel(a)
    assert resp["state"] == "cancel_requested"
    assert a not in sup.running
    assert b in sup.running
    assert b not in sup.queued
    assert (sup.store.get(b) or type("V", (), {"state": ""})).state == "running"
    assert (sup.store.get(a) or type("V", (), {"state": ""})).state == "cancelled"


def test_j3_spec_immutable(sup):
    spec = JobSpec(strategy="stub", param_grid={"k": [1, 2, 3]}, source="test")
    job_id = sup.enqueue(spec)["job_id"]
    got = sup.store.get(job_id)
    assert got.spec.param_grid == {"k": [1, 2, 3]}
    with pytest.raises(FileExistsError):
        sup.store.write_spec(job_id, spec)


def test_q2_concurrency_two_running(sup):
    sup.set_concurrency(2)
    a = sup.enqueue(_spec())["job_id"]
    b = sup.enqueue(_spec())["job_id"]
    _wait(lambda: a in sup.running and b in sup.running, msg="both running")


def test_stub_finishes_done(sup):
    job_id = sup.enqueue(_spec())["job_id"]

    def ready():
        sup.reap()
        view = sup.store.get(job_id)
        return view is not None and view.state in ("done", "cancelled")

    _wait(ready, timeout=5.0, msg="stub job terminal")
    assert sup.store.get(job_id).state == "done"


def test_q3_store_snapshot_no_socket(sup):
    job_id = sup.enqueue(_spec())["job_id"]
    snap = JobStore(sup.root).read_snapshot()
    ids = [v.job_id for v in snap.running + snap.queued + snap.recent]
    assert job_id in ids


def test_j5_queue_client_over_inprocess_socket(tmp_path, monkeypatch):
    """Socket path without spawning a second jobd process."""
    import threading

    from backtester.job.api import QueueClient
    from backtester.job.supervisor import rpc, sock_path

    monkeypatch.setenv("CRYOBT_JOBS", str(tmp_path))
    monkeypatch.setenv("CRYOBT_JOB_STUB", "1")
    monkeypatch.setenv("CRYOBT_JOB_STUB_SECS", "0.4")
    s = Supervisor(tmp_path, stub=True)
    t = threading.Thread(target=lambda: s.serve_forever(idle_sec=8), daemon=True)
    t.start()
    try:
        _wait(lambda: sock_path(tmp_path).exists(), timeout=2.0, msg="queue.sock")
        _wait(lambda: rpc(tmp_path, {"op": "ping"}, timeout=0.3).get("ok"), timeout=2.0, msg="ping")
        client = QueueClient(tmp_path)
        assert client.ping() is True
        view = client.enqueue(_spec())
        assert view.job_id
        snap = client.snapshot()
        ids = [v.job_id for v in snap.running + snap.queued]
        assert view.job_id in ids
        rpc(tmp_path, {"op": "shutdown"}, timeout=1.0)
    finally:
        s._stop = True
        s.kill_remaining()
        t.join(timeout=2.0)

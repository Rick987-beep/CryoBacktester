"""E2E: two real job_smoke jobs through jobd (not the sleep stub).

Uses the in-repo tiny parquet helper. Hard 30s waits. Always shut down jobd.
"""
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest

from backtester.job.api import JobSpec, JobStore, QueueClient
from backtester.job.supervisor import rpc
from tests.test_engine_workers_parity import _write_tiny_replay


def _wait(pred, *, timeout=20.0, interval=0.05, msg="condition"):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return
        time.sleep(interval)
    raise TimeoutError(f"{msg} not met in {timeout:.1f}s")


def _alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
        return False


def _ppid(pid: int) -> int | None:
    try:
        out = subprocess.check_output(
            ["ps", "-o", "ppid=", "-p", str(pid)], text=True, timeout=2,
        )
        return int(out.strip())
    except (OSError, ValueError, subprocess.CalledProcessError):
        return None


def _spec(opt: str, spot: str) -> JobSpec:
    return JobSpec(
        strategy="job_smoke",
        param_grid={"x": list(range(8))},
        account_size=100_000.0,
        requested_inner_workers=1,
        options_path=opt,
        spot_path=spot,
        source="test",
    )


@pytest.fixture
def jobd_env(tmp_path, monkeypatch):
    monkeypatch.setenv("CRYOBT_JOBS", str(tmp_path))
    monkeypatch.delenv("CRYOBT_JOB_STUB", raising=False)
    monkeypatch.setenv("CRYOBT_JOBD_IDLE_SEC", "30")
    monkeypatch.setenv("CRYOBT_JOB_SMOKE_HOLD", "1.2")
    mkt = tmp_path / "mkt"
    mkt.mkdir()
    opt, spot = _write_tiny_replay(mkt)
    yield tmp_path, opt, spot, QueueClient(tmp_path)
    try:
        rpc(tmp_path, {"op": "shutdown"}, timeout=1.0)
    except OSError:
        pass
    time.sleep(0.2)


def test_e2e_two_jobs_queue_done_cancel(jobd_env):
    root, opt, spot, client = jobd_env
    store = JobStore(root)
    pytest_pid = os.getpid()

    a = client.enqueue(_spec(opt, spot))
    _wait(lambda: store.get(a.job_id).state == "running", msg="A running")
    view_a = store.get(a.job_id)
    assert view_a.pid and _alive(view_a.pid)
    parent = _ppid(view_a.pid)
    assert parent != pytest_pid, "job runner must not be a child of pytest"

    b = client.enqueue(_spec(opt, spot))
    snap = client.snapshot()
    running_ids = [v.job_id for v in snap.running]
    queued_ids = [v.job_id for v in snap.queued]
    assert running_ids == [a.job_id]
    assert queued_ids == [b.job_id]
    assert snap.queued[0].queue_position is not None

    _wait(lambda: store.get(a.job_id).state == "done", timeout=20.0, msg="A done")
    done_a = store.get(a.job_id)
    assert done_a.bundle_path
    assert Path(done_a.bundle_path).is_dir()
    _wait(lambda: not _alive(done_a.pid), timeout=5.0, msg="A process gone")

    _wait(lambda: store.get(b.job_id).state == "running", timeout=10.0, msg="B running")
    client.cancel(b.job_id)
    _wait(lambda: store.get(b.job_id).state == "cancelled", timeout=10.0, msg="B cancelled")
    done_b = store.get(b.job_id)
    assert done_b.bundle_path is None
    snap3 = client.snapshot()
    assert snap3.running == []
    assert snap3.queued == []
    _wait(lambda: not _alive(done_b.pid), timeout=5.0, msg="B process gone")


def test_e2e_cancel_queued_never_starts(jobd_env):
    root, opt, spot, client = jobd_env
    store = JobStore(root)
    a = client.enqueue(_spec(opt, spot))
    _wait(lambda: store.get(a.job_id).state == "running", msg="A running")
    b = client.enqueue(_spec(opt, spot))
    client.cancel(b.job_id)
    _wait(lambda: store.get(b.job_id).state == "cancelled", msg="B cancelled")
    assert store.get(b.job_id).pid is None
    assert not (root / b.job_id / "runtime.json").exists()
    _wait(lambda: store.get(a.job_id).state == "done", timeout=20.0, msg="A still finishes")
    assert store.get(a.job_id).state == "done"

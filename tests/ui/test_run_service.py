"""RunService enqueues on jobd; shutdown_all does not kill jobs.

Human GUI recipe: README Research UI → “GUI job acceptance”.
"""
from __future__ import annotations

import time

import pytest

from backtester.job.api import JobStore
from backtester.job.supervisor import rpc
from backtester.ui.services.cache_service import ResultCache
from backtester.ui.services.run_service import RunService
from tests.test_engine_workers_parity import _write_tiny_replay


def _wait(pred, *, timeout=20.0, interval=0.05, msg="condition"):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return
        time.sleep(interval)
    raise TimeoutError(f"{msg} not met in {timeout:.1f}s")


@pytest.fixture
def job_run_service(tmp_path, sqlite_store, monkeypatch):
    monkeypatch.setenv("CRYOBT_JOB_SMOKE_HOLD", "1.2")
    mkt = tmp_path / "mkt"
    mkt.mkdir()
    opt, spot = _write_tiny_replay(mkt)
    cache = ResultCache(sqlite_store, max_unpinned=2)
    svc = RunService(sqlite_store, cache, jobs_root=tmp_path / "jobs")
    yield svc, str(opt), str(spot), tmp_path / "jobs"
    try:
        rpc(tmp_path / "jobs", {"op": "shutdown"}, timeout=1.0)
    except OSError:
        pass
    time.sleep(0.15)


def test_submit_enqueues_and_survives_shutdown_all(job_run_service):
    """G0: shutdown_all must not cancel a jobd job (GUI quit path)."""
    svc, opt, spot, jobs_root = job_run_service
    handle = svc.submit(
        "job_smoke",
        {"x": list(range(8))},
        (None, None),
        account_size=100_000.0,
        options_path=opt,
        spot_path=spot,
        workers=1,
    )
    assert handle.job_id
    _wait(handle.is_alive, timeout=8, msg="job queued or running")

    svc.shutdown_all(timeout_s=0.4)
    assert handle.is_alive(), "shutdown_all killed the job (G0 regression)"

    store = JobStore(jobs_root)
    view = store.get(handle.job_id)
    assert view is not None
    assert view.state in ("queued", "running")

    svc.cancel(handle)
    _wait(lambda: not handle.is_alive(), timeout=8, msg="cancel")
    view = store.get(handle.job_id)
    assert view is not None
    assert view.state == "cancelled"


def test_submit_tail_await_registers_bundle(job_run_service):
    """Progress lines appear; done job registers into the UI store."""
    svc, opt, spot, jobs_root = job_run_service
    handle = svc.submit(
        "job_smoke",
        {"x": [0, 1, 2]},
        (None, None),
        account_size=100_000.0,
        options_path=opt,
        spot_path=spot,
        workers=1,
    )
    lines = []
    deadline = time.monotonic() + 20
    while handle.is_alive() and time.monotonic() < deadline:
        lines.extend(svc.tail_progress(handle))
        time.sleep(0.05)
    lines.extend(svc.tail_progress(handle))
    statuses = [l.get("status") for l in lines if "status" in l]
    phases = [l.get("phase") for l in lines if "phase" in l]
    assert handle.exit_code() == 0
    assert "done" in statuses or phases, f"no progress: {lines[:8]}"

    run_id = svc.await_result(handle)
    assert run_id is not None
    assert svc._store.get_run(run_id) is not None


def test_cancel_queued_never_starts(job_run_service):
    svc, opt, spot, jobs_root = job_run_service
    a = svc.submit(
        "job_smoke",
        {"x": list(range(8))},
        (None, None),
        options_path=opt,
        spot_path=spot,
        workers=1,
    )
    b = svc.submit(
        "job_smoke",
        {"x": [0, 1]},
        (None, None),
        options_path=opt,
        spot_path=spot,
        workers=1,
    )
    _wait(lambda: a.is_alive() and b.is_queued(), timeout=8, msg="A running, B queued")
    svc.cancel(b)
    _wait(lambda: not b.is_alive(), timeout=8, msg="B cancelled")
    store = JobStore(jobs_root)
    vb = store.get(b.job_id)
    assert vb is not None
    assert vb.state == "cancelled"
    runtime = store.job_dir(b.job_id) / "runtime.json"
    assert not runtime.exists(), "queued cancel must never start"
    # A still in flight until we cancel it (hold is 1.2s; may already be done)
    if a.is_alive():
        svc.cancel(a)


def test_cancel_running_starts_queued_successor(job_run_service):
    """Cancel job A while B is queued — B must leave the queue and run."""
    svc, opt, spot, jobs_root = job_run_service
    a = svc.submit(
        "job_smoke",
        {"x": list(range(8))},
        (None, None),
        options_path=opt,
        spot_path=spot,
        workers=1,
    )
    b = svc.submit(
        "job_smoke",
        {"x": list(range(8))},
        (None, None),
        options_path=opt,
        spot_path=spot,
        workers=1,
    )
    _wait(lambda: a.is_alive() and b.is_queued(), timeout=8, msg="A running, B queued")
    svc.cancel(a)
    _wait(lambda: not a.is_alive(), timeout=8, msg="A cancelled")
    store = JobStore(jobs_root)
    assert store.get(a.job_id).state == "cancelled"
    _wait(
        lambda: store.get(b.job_id).state in ("running", "done"),
        timeout=10,
        msg="B promoted after A cancel",
    )
    assert store.get(b.job_id).state in ("running", "done")
    if b.is_alive():
        svc.cancel(b)


def test_adopt_in_flight_reattaches(job_run_service):
    svc, opt, spot, jobs_root = job_run_service
    handle = svc.submit(
        "job_smoke",
        {"x": list(range(8))},
        (None, None),
        options_path=opt,
        spot_path=spot,
        workers=1,
    )
    _wait(handle.is_alive, timeout=8, msg="job in flight")
    adopted = svc.adopt_in_flight()
    assert adopted is not None
    assert adopted.job_id == handle.job_id
    svc.cancel(handle)


def test_import_finished_jobs_after_done(job_run_service):
    svc, opt, spot, jobs_root = job_run_service
    handle = svc.submit(
        "job_smoke",
        {"x": [0, 1]},
        (None, None),
        options_path=opt,
        spot_path=spot,
        workers=1,
    )
    _wait(lambda: not handle.is_alive(), timeout=20, msg="job done")
    ids = svc.import_finished_jobs()
    assert ids, "done job should register into the UI store"


def test_running_worker_count_includes_jobs(job_run_service):
    svc, opt, spot, _ = job_run_service
    assert svc.running_worker_count() == 0
    handle = svc.submit(
        "job_smoke",
        {"x": list(range(8))},
        (None, None),
        options_path=opt,
        spot_path=spot,
        workers=1,
    )
    _wait(lambda: svc.running_worker_count() >= 1, timeout=8, msg="count")
    svc.shutdown_all()
    assert svc.running_worker_count() >= 1
    svc.cancel(handle)

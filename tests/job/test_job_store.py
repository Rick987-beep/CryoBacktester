"""JobStore file protocol — no supervisor, no socket."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from backtester.job.api import (
    HEARTBEAT_STALE_SEC,
    JobSpec,
    JobStore,
    QueueClient,
)


def _spec(**kwargs) -> JobSpec:
    base = dict(
        strategy="blueprint_howto",
        param_grid={"dummy": [0, 1]},
        date_from="2025-10-01",
        date_to="2025-10-08",
        account_size=100_000.0,
        requested_inner_workers=1,
        source="test",
    )
    base.update(kwargs)
    return JobSpec(**base)


def test_spec_roundtrip(tmp_path):
    store = JobStore(tmp_path)
    spec = _spec()
    store.write_spec("abc123", spec, submitted_at="2026-09-09T10:00:00Z")
    view = store.get("abc123")
    assert view is not None
    assert view.spec.strategy == "blueprint_howto"
    assert view.spec.param_grid == {"dummy": [0, 1]}
    assert view.spec.requested_inner_workers == 1
    assert view.submitted_at == "2026-09-09T10:00:00Z"
    assert view.state == "queued"


def test_spec_immutable(tmp_path):
    store = JobStore(tmp_path)
    store.write_spec("abc123", _spec())
    with pytest.raises(FileExistsError):
        store.write_spec("abc123", _spec(param_grid={"dummy": [9]}))
    view = store.get("abc123")
    assert view.spec.param_grid == {"dummy": [0, 1]}


def test_stale_heartbeat_marks_dead(tmp_path):
    store = JobStore(tmp_path)
    store.write_spec("job1", _spec())
    stale = (datetime.now(timezone.utc) - timedelta(seconds=HEARTBEAT_STALE_SEC + 30))
    store.write_status(
        "job1",
        {
            "state": "running",
            "pid": 999,
            "phase": "backtesting",
            "heartbeat_ts": stale.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )
    view = store.get("job1")
    assert view.state == "dead"
    assert view.pid == 999


def test_fresh_heartbeat_stays_running(tmp_path):
    store = JobStore(tmp_path)
    store.write_spec("job1", _spec())
    store.write_status(
        "job1",
        {
            "state": "running",
            "pid": 42,
            "phase": "loading_data",
            "heartbeat_ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )
    assert store.get("job1").state == "running"


def test_read_snapshot_from_dirs_no_socket(tmp_path):
    store = JobStore(tmp_path)
    store.write_spec("run_a", _spec(strategy="a"), submitted_at="2026-09-09T10:00:00Z")
    store.write_spec("wait_b", _spec(strategy="b"), submitted_at="2026-09-09T10:01:00Z")
    store.write_spec("done_c", _spec(strategy="c"), submitted_at="2026-09-09T09:00:00Z")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    store.write_status("run_a", {"state": "running", "pid": 1, "heartbeat_ts": now})
    store.write_status("wait_b", {"state": "queued"})
    store.write_status("done_c", {"state": "done"})
    store.write_result("done_c", {"state": "done", "bundle_path": "/tmp/c.bundle"})
    store.write_queue(concurrency=1, queued=["wait_b"], running=["run_a"], supervisor_pid=None)

    snap = store.read_snapshot()
    assert snap.concurrency == 1
    assert snap.supervisor_pid is None
    assert [v.job_id for v in snap.running] == ["run_a"]
    assert [v.job_id for v in snap.queued] == ["wait_b"]
    assert snap.queued[0].queue_position == 1
    assert [v.job_id for v in snap.recent] == ["done_c"]
    assert snap.recent[0].bundle_path == "/tmp/c.bundle"
    assert not (tmp_path / "queue.sock").exists()


def test_queue_client_snapshot_without_jobd(tmp_path):
    store = JobStore(tmp_path)
    store.write_spec("x", _spec())
    client = QueueClient(tmp_path)
    assert client.ping() is False
    snap = client.snapshot()
    assert len(snap.queued) + len(snap.running) + len(snap.recent) >= 1
    with pytest.raises(NotImplementedError):
        client.enqueue(_spec())

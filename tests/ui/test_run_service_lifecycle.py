"""
tests/ui/test_run_service_lifecycle.py — Default-CI lifecycle tests for RunService.

Uses dummy subprocesses (no parquet, not slow_ui) to verify process-group
spawn, shutdown_all TERM→KILL, and running_worker_count.
"""
from __future__ import annotations

import os
import sys
import time

import pytest

from backtester.ui.services.cache_service import ResultCache
from backtester.ui.services.run_service import RunService


# Sleep forever until signalled (obeys SIGTERM by default).
_SLEEP_CMD = [sys.executable, "-c", "import time; time.sleep(3600)"]


@pytest.fixture
def run_service(sqlite_store):
    cache = ResultCache(sqlite_store, max_unpinned=2)
    return RunService(sqlite_store, cache)


def test_shutdown_all_reaps_sleeping_worker(run_service):
    handle = run_service.submit_cmd(_SLEEP_CMD)
    assert handle.is_alive()
    assert run_service.running_worker_count() == 1

    run_service.shutdown_all(timeout_s=2.0)

    deadline = time.monotonic() + 3
    while handle.is_alive() and time.monotonic() < deadline:
        time.sleep(0.05)

    assert not handle.is_alive()
    assert run_service.running_worker_count() == 0


@pytest.mark.skipif(os.name != "posix", reason="process groups are POSIX-only")
def test_spawn_uses_new_session(run_service):
    handle = run_service.submit_cmd(_SLEEP_CMD)
    try:
        parent_pgid = os.getpgid(0)
        child_pgid = os.getpgid(handle.pid)
        assert child_pgid == handle.pid, "child should be session/group leader"
        assert child_pgid != parent_pgid
    finally:
        run_service.shutdown_all(timeout_s=2.0)


@pytest.mark.skipif(os.name != "posix", reason="SIGTERM ignore + killpg is POSIX")
def test_shutdown_all_kill_fallback_for_stubborn_worker(run_service, tmp_path):
    ready = tmp_path / "ready"
    stubborn = [
        sys.executable,
        "-c",
        (
            "import signal, sys, time; "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            f"open({str(ready)!r}, 'w').close(); "
            "time.sleep(3600)"
        ),
    ]
    handle = run_service.submit_cmd(stubborn)
    # Wait until SIGTERM is ignored (avoids race with default handler)
    deadline = time.monotonic() + 5
    while not ready.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert ready.exists(), "stubborn child never became ready"
    assert handle.is_alive()

    t0 = time.monotonic()
    run_service.shutdown_all(timeout_s=0.4)
    elapsed = time.monotonic() - t0

    deadline = time.monotonic() + 2
    while handle.is_alive() and time.monotonic() < deadline:
        time.sleep(0.05)

    assert not handle.is_alive(), "stubborn worker should be SIGKILL'd"
    assert elapsed >= 0.3, "should wait for TERM timeout before KILL"
    assert run_service.running_worker_count() == 0


def test_cancel_uses_same_stop_path(run_service):
    handle = run_service.submit_cmd(_SLEEP_CMD)
    run_service.cancel(handle)

    deadline = time.monotonic() + 3
    while handle.is_alive() and time.monotonic() < deadline:
        time.sleep(0.05)

    assert not handle.is_alive()
    assert run_service.running_worker_count() == 0

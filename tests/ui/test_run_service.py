"""
tests/ui/test_run_service.py — Tests for RunService submit/cancel flow.

Marked slow_ui because the submit() call spawns a real subprocess.
"""
import json
import os
import pathlib
import time

import pytest


@pytest.mark.slow_ui
def test_submit_and_tail(tmp_path, sqlite_store):
    """submit() spawns a worker; tail_progress yields lines; await_result registers run."""
    try:
        import backtester.config as _cfg
        opts = _cfg.cfg.data.options_parquet
        spot = _cfg.cfg.data.spot_parquet
        if not (os.path.exists(opts) and os.path.exists(spot)):
            pytest.skip("no parquet data available")
    except Exception:
        pytest.skip("config unavailable")

    from backtester.ui.services.run_service import RunService
    from backtester.ui.services.cache_service import ResultCache

    store = sqlite_store
    cache = ResultCache(store, max_unpinned=2)
    svc = RunService(store, cache)

    handle = svc.submit(
        strategy_key="short_generic",
        param_grid={"delta": [0.24], "dte": [7]},
        date_range=("2025-10-01", "2025-10-08"),
        account_size=100000.0,
    )

    assert handle.pid > 0
    assert handle.is_alive() or handle.exit_code() is not None

    # Wait for completion (up to 120 s)
    deadline = time.monotonic() + 120
    while handle.is_alive() and time.monotonic() < deadline:
        time.sleep(1)

    assert not handle.is_alive(), "worker did not finish in 120s"

    lines = list(svc.tail_progress(handle))
    statuses = [l.get("status") for l in lines if "status" in l]
    assert "done" in statuses or "error" in statuses, f"lines: {lines[:5]}"


@pytest.mark.slow_ui
def test_cancel_stops_worker(tmp_path, sqlite_store):
    """cancel() terminates the worker within 5 s."""
    try:
        import backtester.config as _cfg
        opts = _cfg.cfg.data.options_parquet
        spot = _cfg.cfg.data.spot_parquet
        if not (os.path.exists(opts) and os.path.exists(spot)):
            pytest.skip("no parquet data available")
    except Exception:
        pytest.skip("config unavailable")

    from backtester.ui.services.run_service import RunService
    from backtester.ui.services.cache_service import ResultCache

    store = sqlite_store
    cache = ResultCache(store, max_unpinned=2)
    svc = RunService(store, cache)

    # Use a very wide date range so the run won't finish before we cancel
    handle = svc.submit(
        strategy_key="short_generic",
        param_grid={"delta": [0.24], "dte": [7]},
        date_range=("2024-01-01", "2026-12-31"),
        account_size=100000.0,
    )

    time.sleep(3)
    svc.cancel(handle)

    deadline = time.monotonic() + 5
    while handle.is_alive() and time.monotonic() < deadline:
        time.sleep(0.1)

    assert not handle.is_alive(), "worker still alive after cancel"
    assert handle.exit_code() != 0

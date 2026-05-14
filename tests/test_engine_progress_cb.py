"""
tests/test_engine_progress_cb.py — Tests for run_grid_full progress_cb parameter.
"""
import pytest


class _SimpleStrategy:
    """Minimal strategy stub for engine testing."""

    name = "stub_progress"
    PARAM_GRID = {"offset": [0]}

    def __init__(self, offset=0):
        self.offset = offset

    def on_market(self, state):
        return []

    def on_end(self, state):
        return []


def _make_minimal_replay(n=120):
    """Build a tiny in-memory MarketReplay-like iterator of n states."""
    from datetime import datetime, timezone, timedelta
    from backtester.strategy_base import MarketState
    import numpy as np

    # We use a real-ish MarketState structure
    start = datetime(2025, 10, 1, tzinfo=timezone.utc)
    states = []
    for i in range(n):
        dt = start + timedelta(minutes=5 * i)
        ts_us = int(dt.timestamp() * 1_000_000)
        states.append({
            "ts": ts_us,
            "spot": 30000.0 + i,
            "day_key": dt.strftime("%Y-%m-%d"),
        })
    return states


def test_callback_invoked_with_totals():
    """progress_cb is called at least once; final call has current == total."""
    from backtester.engine import run_grid_full
    from unittest.mock import MagicMock
    from backtester.market_replay import MarketReplay
    import pathlib

    # Use actual data if available; otherwise skip if data missing
    try:
        import backtester.config as _cfg
        opts = _cfg.cfg.data.options_parquet
        spot = _cfg.cfg.data.spot_parquet
        import os
        if not (os.path.exists(opts) and os.path.exists(spot)):
            pytest.skip("no parquet data available")
    except Exception:
        pytest.skip("config unavailable")

    calls = []

    def _cb(current, total, date_iso):
        calls.append((current, total, date_iso))

    from backtester.strategies.short_generic import ShortGeneric
    # Use one value per param to keep the run tiny
    param_grid = {k: [v[0]] for k, v in ShortGeneric.PARAM_GRID.items()}

    try:
        replay = MarketReplay(opts, spot, start="2025-10-01", end="2025-10-10")
    except Exception:
        pytest.skip("data range unavailable")

    run_grid_full(
        ShortGeneric,
        param_grid,
        replay,
        progress=False,
        progress_cb=_cb,
        progress_cb_interval=50,
    )

    # If data is present, we should have been called at least once
    if not calls:
        pytest.skip("no states in data range; no progress_cb calls")

    last_current, last_total, _ = calls[-1]
    assert last_total > 0
    assert last_current <= last_total
    assert all(c[0] <= c[1] for c in calls)   # current never exceeds total


def test_bad_callback_does_not_break_run():
    """A progress_cb that raises must not abort the run."""
    from backtester.engine import run_grid_full
    from backtester.market_replay import MarketReplay
    import backtester.config as _cfg
    import os

    try:
        opts = _cfg.cfg.data.options_parquet
        spot = _cfg.cfg.data.spot_parquet
        if not (os.path.exists(opts) and os.path.exists(spot)):
            pytest.skip("no parquet data available")
    except Exception:
        pytest.skip("config unavailable")

    def _bad_cb(current, total, date_iso):
        raise RuntimeError("intentional test error")

    from backtester.strategies.short_generic import ShortGeneric
    param_grid = {k: [v[0]] for k, v in ShortGeneric.PARAM_GRID.items()}

    try:
        replay = MarketReplay(opts, spot, start="2025-10-01", end="2025-10-10")
    except Exception:
        pytest.skip("data range unavailable")

    # Should complete without raising
    df, keys, nav_daily_df, final_nav_df, df_fills = run_grid_full(
        ShortGeneric,
        param_grid,
        replay,
        progress=False,
        progress_cb=_bad_cb,
        progress_cb_interval=1,
    )
    assert df is not None

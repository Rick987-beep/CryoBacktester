"""A1/A4: combo-shard spawn matches workers=1; uneven shards; child errors.

Production thresholds stay 8/8. These tests monkeypatch
``_effective_inner_workers`` so a tiny parquet fixture can hit workers=2/3/4.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from backtester.core.engine import run_grid_full
from backtester.core.market_replay import MarketReplay
from backtester.core.strategy_base import Trade


class _ParamPnlStrategy:
    """Module-level (picklable). PnL = param x so combo_idx remapping is visible."""

    name = "param_pnl"

    def configure(self, params):
        self.x = int(params["x"])

    def on_market_state(self, state):
        return []

    def on_end(self, state):
        return [
            Trade(
                entry_time=state.dt,
                exit_time=state.dt,
                entry_spot=float(state.spot),
                exit_spot=float(state.spot),
                entry_price_usd=0.0,
                exit_price_usd=0.0,
                fees=0.0,
                pnl=float(self.x),
                triggered=False,
                exit_reason="end",
                exit_hour=0,
                entry_date=state.dt.strftime("%Y-%m-%d"),
                side="close",
            )
        ]

    def reset(self):
        pass

    def describe_params(self):
        return {"x": self.x}


class _BoomStrategy:
    """Raises on combo x=5 so one spawn child fails."""

    name = "boom_shard"

    def configure(self, params):
        self.x = int(params["x"])

    def on_market_state(self, state):
        if self.x == 5:
            raise RuntimeError("intentional shard failure")
        return []

    def on_end(self, state):
        return []

    def reset(self):
        pass

    def describe_params(self):
        return {"x": self.x}


def _write_tiny_replay(tmp_path, n_ticks=8):
    start = datetime(2025, 10, 1, 12, 0, tzinfo=timezone.utc)
    opt_rows = []
    spot_rows = []
    for i in range(n_ticks):
        dt = start + timedelta(minutes=5 * i)
        ts = int(dt.timestamp() * 1_000_000)
        opt_rows.append(
            {
                "timestamp": ts,
                "expiry": "3OCT25",
                "strike": 100_000.0,
                "is_call": True,
                "bid_price": 0.01,
                "ask_price": 0.012,
                "mark_price": 0.011,
                "mark_iv": 50.0,
                "delta": 0.25,
            }
        )
        px = 100_000.0 + i
        spot_rows.append(
            {
                "timestamp": ts,
                "open": px,
                "high": px + 10,
                "low": px - 10,
                "close": px,
            }
        )
    opt_path = tmp_path / "options.parquet"
    spot_path = tmp_path / "spot.parquet"
    pd.DataFrame(opt_rows).to_parquet(opt_path, index=False)
    pd.DataFrame(spot_rows).to_parquet(spot_path, index=False)
    return str(opt_path), str(spot_path)


def _sorted(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = [c for c in ("combo_idx", "entry_time", "pnl", "date") if c in df.columns]
    return df.sort_values(cols, kind="mergesort").reset_index(drop=True)


def _assert_same_grid(ref, got):
    df1, keys1, nav1, fin1, fills1 = ref
    df2, keys2, nav2, fin2, fills2 = got
    assert keys1 == keys2
    pd.testing.assert_frame_equal(_sorted(df1), _sorted(df2), check_dtype=False)
    pd.testing.assert_frame_equal(_sorted(fin1), _sorted(fin2), check_dtype=False)
    pd.testing.assert_frame_equal(_sorted(nav1), _sorted(nav2), check_dtype=False)
    if fills1 is None or fills1.empty:
        assert fills2 is None or fills2.empty
    else:
        pd.testing.assert_frame_equal(_sorted(fills1), _sorted(fills2), check_dtype=False)


@pytest.mark.parametrize("n_combos,n_workers", [(16, 2), (10, 3), (10, 4), (17, 2)])
def test_spawn_matches_single_process(tmp_path, monkeypatch, n_combos, n_workers):
    import backtester.core.engine as eng

    opt, spot = _write_tiny_replay(tmp_path)
    grid = {"x": list(range(n_combos))}
    ref = run_grid_full(
        _ParamPnlStrategy, grid, MarketReplay(opt, spot), progress=False, workers=1,
    )
    monkeypatch.setattr(eng, "_effective_inner_workers", lambda *a, **k: n_workers)
    got = run_grid_full(
        _ParamPnlStrategy, grid, MarketReplay(opt, spot), progress=False, workers=n_workers,
    )
    _assert_same_grid(ref, got)
    df2, keys2, *_ = got
    assert len(keys2) == n_combos
    assert list(df2["pnl"].astype(int)) == list(range(n_combos))
    assert list(df2["combo_idx"].astype(int)) == list(range(n_combos))


def test_child_raise_propagates_no_partial(tmp_path, monkeypatch):
    """A4: a crashing shard fails the parent; nothing is returned to merge."""
    import backtester.core.engine as eng

    opt, spot = _write_tiny_replay(tmp_path)
    monkeypatch.setattr(eng, "_effective_inner_workers", lambda *a, **k: 2)
    with pytest.raises(Exception, match="intentional shard failure"):
        run_grid_full(
            _BoomStrategy,
            {"x": list(range(16))},
            MarketReplay(opt, spot),
            progress=False,
            workers=2,
        )

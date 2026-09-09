"""A1: spawn duplicate-load path matches workers=1 on a tiny parquet replay."""
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


def test_spawn_two_workers_matches_single_process(tmp_path, monkeypatch):
    opt, spot = _write_tiny_replay(tmp_path)
    grid = {"x": list(range(16))}

    replay_1 = MarketReplay(opt, spot)
    df1, keys1, nav1, fin1, fills1 = run_grid_full(
        _ParamPnlStrategy, grid, replay_1, progress=False, workers=1,
    )

    import backtester.core.engine as eng

    monkeypatch.setattr(eng, "_effective_inner_workers", lambda *a, **k: 2)
    replay_2 = MarketReplay(opt, spot)
    df2, keys2, nav2, fin2, fills2 = run_grid_full(
        _ParamPnlStrategy, grid, replay_2, progress=False, workers=2,
    )

    assert keys1 == keys2
    assert [k[0][1] for k in keys2] == list(range(16))
    pd.testing.assert_frame_equal(_sorted(df1), _sorted(df2), check_dtype=False)
    pd.testing.assert_frame_equal(_sorted(fin1), _sorted(fin2), check_dtype=False)
    pd.testing.assert_frame_equal(_sorted(nav1), _sorted(nav2), check_dtype=False)
    assert list(df2["pnl"].astype(int)) == list(range(16))
    assert list(df2["combo_idx"].astype(int)) == list(range(16))


def test_uneven_shards_remap_combo_idx(tmp_path, monkeypatch):
    """17 combos / 2 workers: leftover combo stays in global index order."""
    import backtester.core.engine as eng

    opt, spot = _write_tiny_replay(tmp_path)
    grid = {"x": list(range(17))}
    replay_1 = MarketReplay(opt, spot)
    df1, keys1, _nav1, fin1, _f1 = run_grid_full(
        _ParamPnlStrategy, grid, replay_1, progress=False, workers=1,
    )
    monkeypatch.setattr(eng, "_effective_inner_workers", lambda *a, **k: 2)
    replay_2 = MarketReplay(opt, spot)
    df2, keys2, _nav2, fin2, _f2 = run_grid_full(
        _ParamPnlStrategy, grid, replay_2, progress=False, workers=2,
    )
    assert keys1 == keys2
    assert len(keys2) == 17
    pd.testing.assert_frame_equal(_sorted(df1), _sorted(df2), check_dtype=False)
    pd.testing.assert_frame_equal(_sorted(fin1), _sorted(fin2), check_dtype=False)
    assert int(df2["pnl"].astype(int).sum()) == sum(range(17))


def test_below_min_combos_stays_single_process(tmp_path, monkeypatch):
    """A2: 7 combos never spawn even when the caller asks for 4 workers."""
    import backtester.core.engine as eng
    from types import SimpleNamespace

    opt, spot = _write_tiny_replay(tmp_path)
    replay = MarketReplay(opt, spot)

    def boom(*_a, **_k):
        raise AssertionError("Pool must not be constructed below min_combos_parallel")

    monkeypatch.setattr(
        eng.multiprocessing,
        "get_context",
        lambda *_a, **_k: SimpleNamespace(Pool=boom),
    )
    df, keys, *_ = run_grid_full(
        _ParamPnlStrategy,
        {"x": list(range(7))},
        replay,
        progress=False,
        workers=4,
    )
    assert len(keys) == 7
    assert len(df) == 7

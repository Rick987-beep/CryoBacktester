"""Short-window and full-grid multicore checks against the run-777 fixture.

Skipped unless CRYOBT_RUN_777=1 (needs real parquet + kline cache).
Short window: 2026-07-25 → 2026-07-31. Full acceptance: DATE_RANGE_777.
"""
from __future__ import annotations

import json
import os
from functools import reduce
from operator import mul
from pathlib import Path

import pandas as pd
import pytest

from tests.tudysho_777 import (
    ACCOUNT_SIZE_777,
    DATE_RANGE_777,
    N_COMBOS_777,
    PARAM_GRID_777,
    STRATEGY_777,
)

SHORT_RANGE = ("2026-07-25", "2026-07-31")


def _need_777():
    if os.environ.get("CRYOBT_RUN_777") != "1":
        pytest.skip("set CRYOBT_RUN_777=1 to run the tudysho-777 worker checks")


def test_777_grid_cartesian():
    n = reduce(mul, (max(len(v), 1) for v in PARAM_GRID_777.values()), 1)
    assert n == N_COMBOS_777


def _sorted_trades(df):
    cols = [c for c in ("combo_idx", "entry_time", "pnl") if c in df.columns]
    return df.sort_values(cols, kind="mergesort").reset_index(drop=True)


def test_777_short_window_workers4_matches_1(tmp_path):
    _need_777()
    from backtester.run import run_backtest

    runs = tmp_path / "runs"
    n_workers = int(os.environ.get("CRYOBT_777_WORKERS", "4"))
    b1 = run_backtest(
        STRATEGY_777,
        PARAM_GRID_777,
        SHORT_RANGE,
        ACCOUNT_SIZE_777,
        str(runs / "w1"),
        progress_cb=None,
        source="cli",
        workers=1,
    )
    progress = []

    def _cb(current, total, day):
        progress.append((int(current), int(total), str(day)))

    b4 = run_backtest(
        STRATEGY_777,
        PARAM_GRID_777,
        SHORT_RANGE,
        ACCOUNT_SIZE_777,
        str(runs / "w4"),
        progress_cb=_cb,
        source="cli",
        workers=n_workers,
    )

    m1 = json.loads((Path(b1) / "meta.json").read_text())
    m4 = json.loads((Path(b4) / "meta.json").read_text())
    assert m1["n_combos"] == N_COMBOS_777
    assert m4["n_combos"] == N_COMBOS_777
    assert m4["n_trades"] == m1["n_trades"]
    effective = int(m4.get("grid_workers_effective") or 0)
    assert effective > 1, m4
    assert progress, "multicore run must forward progress_cb from shard 0"
    assert progress[-1][0] == progress[-1][1]

    t1df = _sorted_trades(pd.read_parquet(Path(b1) / "trade_log.parquet"))
    t4df = _sorted_trades(pd.read_parquet(Path(b4) / "trade_log.parquet"))
    assert len(t1df) == len(t4df)
    pd.testing.assert_frame_equal(
        t1df[["combo_idx", "pnl"]],
        t4df[["combo_idx", "pnl"]],
        check_dtype=False,
        rtol=1e-5,
        atol=1e-4,
    )


def test_777_full_workers4_matches_1(tmp_path):
    _need_777()
    if os.environ.get("CRYOBT_RUN_777_FULL") != "1":
        pytest.skip("set CRYOBT_RUN_777_FULL=1 for the full May–Jul acceptance")
    from backtester.run import run_backtest

    runs = tmp_path / "runs"
    n_workers = int(os.environ.get("CRYOBT_777_WORKERS", "4"))
    progress = []

    def _cb(current, total, day):
        progress.append((int(current), int(total), str(day)))

    b4 = run_backtest(
        STRATEGY_777,
        PARAM_GRID_777,
        DATE_RANGE_777,
        ACCOUNT_SIZE_777,
        str(runs / "w4"),
        progress_cb=_cb,
        source="cli",
        workers=n_workers,
    )
    b1 = run_backtest(
        STRATEGY_777,
        PARAM_GRID_777,
        DATE_RANGE_777,
        ACCOUNT_SIZE_777,
        str(runs / "w1"),
        progress_cb=None,
        source="cli",
        workers=1,
    )
    m1 = json.loads((Path(b1) / "meta.json").read_text())
    m4 = json.loads((Path(b4) / "meta.json").read_text())
    assert m1["n_combos"] == N_COMBOS_777
    assert m4["n_combos"] == N_COMBOS_777
    assert m4["n_trades"] == m1["n_trades"]
    assert int(m4.get("grid_workers_effective") or 0) > 1, m4
    assert progress and progress[-1][0] == progress[-1][1]
    t1df = _sorted_trades(pd.read_parquet(Path(b1) / "trade_log.parquet"))
    t4df = _sorted_trades(pd.read_parquet(Path(b4) / "trade_log.parquet"))
    pd.testing.assert_frame_equal(
        t1df[["combo_idx", "pnl"]],
        t4df[["combo_idx", "pnl"]],
        check_dtype=False,
        rtol=1e-5,
        atol=1e-4,
    )

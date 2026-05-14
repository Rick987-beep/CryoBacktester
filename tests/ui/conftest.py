"""
tests/ui/conftest.py — Shared fixtures for all UI tests.

tiny_grid_result  — a real GridResult from 3 combos × ~15-day synthetic trade log.
tmp_bundle_dir    — tmp_path/bundles, monkeypatched as bundles_root.
tmp_state_dir     — tmp_path/ui_state, monkeypatched as state_dir.
sqlite_store      — a StoreService bound to a throwaway DB.
"""
import os
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone


def _make_tiny_grid_result():
    """Build a synthetic GridResult with 3 combos over 15 trading days."""
    from backtester.results import GridResult

    # 3 combos: delta=[0.20, 0.25, 0.30], dte=[1]
    param_grid = {"delta": [0.20, 0.25, 0.30], "dte": [1]}
    keys = [
        (("delta", 0.20), ("dte", 1)),
        (("delta", 0.25), ("dte", 1)),
        (("delta", 0.30), ("dte", 1)),
    ]

    # Build synthetic trade log: 2 trades per combo per week for 3 weeks
    rng = np.random.default_rng(42)
    start = datetime(2025, 10, 1, tzinfo=timezone.utc)
    rows = []
    nav_rows = []
    for combo_idx, key in enumerate(keys):
        nav = 10000.0
        for week in range(3):
            for day_offset in [1, 3]:
                entry_dt = start + timedelta(weeks=week, days=day_offset, hours=9)
                exit_dt = entry_dt + timedelta(hours=6)
                pnl = float(rng.uniform(-80, 120))
                rows.append({
                    "combo_idx": combo_idx,
                    "entry_time": entry_dt,
                    "exit_time": exit_dt,
                    "entry_spot": 30000.0,
                    "exit_spot": 30100.0,
                    "entry_price_usd": 50.0,
                    "exit_price_usd": 50.0 - pnl,
                    "fees": 2.0,
                    "pnl": pnl,
                    "triggered": pnl > 0,
                    "exit_reason": "tp" if pnl > 0 else "sl",
                    "exit_hour": 6,
                    "entry_date": entry_dt.strftime("%Y-%m-%d"),
                    "status": 0,
                })
        # Build daily NAV rows
        for day_i in range(21):
            dt = start + timedelta(days=day_i)
            ds = dt.strftime("%Y-%m-%d")
            nav += float(rng.uniform(-30, 40))
            nav_rows.append({
                "combo_idx": combo_idx,
                "date": ds,
                "nav_low": nav - 10,
                "nav_high": nav + 10,
                "nav_close": nav,
            })

    df = pd.DataFrame(rows)
    df["combo_idx"] = df["combo_idx"].astype("int16")
    df["pnl"] = df["pnl"].astype("float32")

    nav_daily_df = pd.DataFrame(nav_rows)
    nav_daily_df["combo_idx"] = nav_daily_df["combo_idx"].astype("int16")

    final_rows = []
    for combo_idx in range(3):
        subset = df[df["combo_idx"] == combo_idx]
        final_rows.append({
            "combo_idx": combo_idx,
            "final_nav": 10000.0 + float(subset["pnl"].sum()),
            "realized_pnl": float(subset["pnl"].sum()),
            "open_pnl": 0.0,
        })
    final_nav_df = pd.DataFrame(final_rows)
    final_nav_df["combo_idx"] = final_nav_df["combo_idx"].astype("int16")

    date_from = start.strftime("%Y-%m-%d")
    date_to = (start + timedelta(days=20)).strftime("%Y-%m-%d")

    return GridResult(
        df, keys, nav_daily_df, final_nav_df,
        param_grid=param_grid,
        account_size=10000.0,
        date_range=(date_from, date_to),
        df_fills=None,
    )


@pytest.fixture(scope="session")
def tiny_grid_result():
    """A real GridResult with 3 combos, built once per test session."""
    return _make_tiny_grid_result()


@pytest.fixture
def tmp_bundle_dir(tmp_path):
    return tmp_path / "bundles"


@pytest.fixture
def tmp_state_dir(tmp_path):
    return tmp_path / "ui_state"


@pytest.fixture
def sqlite_store(tmp_bundle_dir, tmp_state_dir):
    from backtester.ui.services.store_service import StoreService
    return StoreService(tmp_state_dir, tmp_bundle_dir)

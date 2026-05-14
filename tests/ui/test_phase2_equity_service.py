"""
tests/ui/test_phase2_equity_service.py — equity_service unit tests.
"""
import pytest


def test_returns_top_n_eq_unchanged(tiny_grid_result):
    """Keys in top_n_eq are returned directly (same dict object)."""
    from backtester.ui.services.equity_service import equity_for_key
    result = tiny_grid_result
    # All 3 combos are in top_n_eq (default top_n_report >= 3)
    for key in result.top_n_eq:
        eq = equity_for_key(result, key)
        assert eq is result.top_n_eq[key], "Expected the same object for top-N key"


def test_computes_for_non_top_n(tmp_path, monkeypatch):
    """Key NOT in top_n_eq: equity is computed on demand and matches direct call."""
    # Build a GridResult with top_n_report = 1 so only the best combo is pre-computed.
    from backtester.config import cfg
    monkeypatch.setattr(cfg.simulation, "top_n_report", 1)

    # Rebuild with patched config (conftest fixture is session-scoped; build fresh here)
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta, timezone
    from backtester.results import GridResult, equity_metrics

    rng = np.random.default_rng(99)
    start = datetime(2025, 10, 1, tzinfo=timezone.utc)
    param_grid = {"delta": [0.20, 0.25], "dte": [1]}
    keys = [
        (("delta", 0.20), ("dte", 1)),
        (("delta", 0.25), ("dte", 1)),
    ]
    rows = []
    nav_rows = []
    for ci, _ in enumerate(keys):
        for week in range(2):
            for day in [1, 3]:
                entry_dt = start + timedelta(weeks=week, days=day, hours=9)
                exit_dt = entry_dt + timedelta(hours=4)
                pnl = float(rng.uniform(-50, 100))
                rows.append(dict(
                    combo_idx=ci, entry_time=entry_dt, exit_time=exit_dt,
                    entry_spot=30000.0, exit_spot=30050.0, entry_price_usd=40.0,
                    exit_price_usd=40.0 - pnl, fees=1.0, pnl=pnl,
                    triggered=pnl > 0, exit_reason="tp" if pnl > 0 else "sl",
                    exit_hour=6, entry_date=entry_dt.strftime("%Y-%m-%d"), status=0,
                ))
        for di in range(14):
            dt = start + timedelta(days=di)
            nav = 10000 + float(rng.uniform(-20, 30)) * di
            nav_rows.append(dict(combo_idx=ci, date=dt.strftime("%Y-%m-%d"),
                                 nav_low=nav - 5, nav_high=nav + 5, nav_close=nav))

    df = pd.DataFrame(rows)
    df["combo_idx"] = df["combo_idx"].astype("int16")
    nav_daily_df = pd.DataFrame(nav_rows)
    nav_daily_df["combo_idx"] = nav_daily_df["combo_idx"].astype("int16")
    final_nav_df = pd.DataFrame([
        {"combo_idx": ci, "final_nav": 10000.0, "realized_pnl": 0.0, "open_pnl": 0.0}
        for ci in range(2)
    ])
    final_nav_df["combo_idx"] = final_nav_df["combo_idx"].astype("int16")

    result = GridResult(
        df, keys, nav_daily_df, final_nav_df,
        param_grid=param_grid, account_size=10000.0,
        date_range=("2025-10-01", "2025-10-14"),
    )

    # Restore top_n_report so config stays valid for other tests
    monkeypatch.undo()

    assert len(result.top_n_eq) == 1, f"Expected 1 pre-computed equity, got {len(result.top_n_eq)}"

    # The non-top key
    non_top_key = [k for k in keys if k not in result.top_n_eq][0]

    from backtester.ui.services.equity_service import equity_for_key

    eq = equity_for_key(result, non_top_key)

    assert eq is not None
    assert "daily" in eq
    assert "sortino" in eq
    assert "calmar" in eq

    # Must match a direct equity_metrics() call
    cidx = result.key_to_idx[non_top_key]
    df_c = df[df["combo_idx"] == cidx]
    nav_c = nav_daily_df[nav_daily_df["combo_idx"] == cidx]
    direct_eq = equity_metrics(
        df_c, capital=10000.0, nav_daily_combo=nav_c,
        date_from="2025-10-01", date_to="2025-10-14",
    )
    assert eq["total_pnl"] == direct_eq["total_pnl"]
    assert eq["sortino"] == direct_eq["sortino"]


def test_caches_on_result(tiny_grid_result, monkeypatch):
    """Second call for the same non-top key must not re-enter equity_metrics."""
    import backtester.results as _results_mod

    call_count = {"n": 0}
    _original = _results_mod.equity_metrics

    def _counted(*args, **kwargs):
        call_count["n"] += 1
        return _original(*args, **kwargs)

    monkeypatch.setattr(_results_mod, "equity_metrics", _counted)

    from backtester.ui.services.equity_service import equity_for_key
    result = tiny_grid_result

    # Use a key that IS in top_n_eq — should not call equity_metrics at all
    key = next(iter(result.top_n_eq))
    if hasattr(result, "_lazy_eq") and key in result._lazy_eq:
        del result._lazy_eq[key]

    eq1 = equity_for_key(result, key)
    eq2 = equity_for_key(result, key)
    assert call_count["n"] == 0, "top-N key should never invoke equity_metrics"
    assert eq1 is eq2

"""tests/ui/test_ann_return.py — Annualized return (CAGR) metric."""
import numpy as np
import pandas as pd
import pytest

from backtester.results import equity_metrics


def test_equity_metrics_ann_return_matches_cagr_formula():
    """ann_return == (final/capital)^(365/n_days) - 1."""
    capital = 10_000.0
    # 365 calendar days of flat +$10/day → final = 10000 + 3650
    dates = pd.date_range("2024-01-01", periods=365, freq="D")
    rows = []
    for i, d in enumerate(dates):
        nav = capital + (i + 1) * 10.0
        rows.append({
            "date": d.strftime("%Y-%m-%d"),
            "nav_close": nav,
            "nav_low": nav,
            "nav_high": nav,
            "realized_close": nav - capital,
        })
    nav_df = pd.DataFrame(rows)
    trades = pd.DataFrame(columns=["pnl", "entry_date", "exit_date"])

    eq = equity_metrics(
        trades, capital=capital, nav_daily_combo=nav_df,
        date_from="2024-01-01", date_to="2024-12-30",
    )
    assert eq is not None
    final_eq = capital + 365 * 10.0
    expected = (final_eq / capital) ** (365.0 / 365.0) - 1.0
    assert eq["ann_return"] == pytest.approx(expected, rel=1e-9)
    assert eq["ann_return"] == pytest.approx(0.365, rel=1e-9)


def test_all_combo_stats_includes_ann_return(tiny_grid_result):
    """Every combo in all_stats has ann_return; not used in scoring."""
    result = tiny_grid_result
    for key, stats in result.all_stats.items():
        assert "ann_return" in stats
        assert isinstance(stats["ann_return"], float)
        assert np.isfinite(stats["ann_return"])

    # Scoring unchanged: score dict keys match combos; no weight for ann_return
    assert set(result.scores.keys()) == set(result.all_stats.keys())


def test_grid_dataframe_ann_return_between_pnl_and_sharpe(tiny_grid_result):
    from backtester.ui.views.grid_view import _FIXED_DISPLAY_COLS, _grid_dataframe

    assert _FIXED_DISPLAY_COLS.index("ann_return") == _FIXED_DISPLAY_COLS.index("total_pnl") + 1
    assert _FIXED_DISPLAY_COLS.index("sharpe") == _FIXED_DISPLAY_COLS.index("ann_return") + 1

    df, _ = _grid_dataframe(tiny_grid_result)
    assert "ann_return" in df.columns
    # Stored as percent units for Tabulator "%" formatter
    key = tiny_grid_result.ranked[0][0]
    expected_pct = round(float(tiny_grid_result.all_stats[key]["ann_return"]) * 100, 1)
    assert float(df.iloc[0]["ann_return"]) == pytest.approx(expected_pct)


def test_detail_stats_labels_order():
    from backtester.ui.views.detail_view import _STATS_LABELS

    keys = list(_STATS_LABELS.keys())
    assert keys[keys.index("total_pnl") + 1] == "ann_return"
    assert keys[keys.index("ann_return") + 1] == "sharpe"
    assert _STATS_LABELS["ann_return"] == "Annualized Return"


def test_favourites_schema_migration_adds_ann_return(tmp_path):
    """Existing DBs without ann_return get the column on StoreService init."""
    import sqlite3
    from backtester.ui.services.store_service import StoreService

    state_dir = tmp_path / "state"
    bundles = tmp_path / "bundles"
    state_dir.mkdir()
    bundles.mkdir()
    db = state_dir / "ui_state.db"

    # Simulate a pre-migration favourites table
    con = sqlite3.connect(str(db))
    con.execute("""
        CREATE TABLE favourites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            combo_hash TEXT NOT NULL,
            combo_key_json TEXT NOT NULL,
            name TEXT NOT NULL DEFAULT '',
            strategy TEXT NOT NULL DEFAULT '',
            note TEXT NOT NULL DEFAULT '',
            score REAL,
            sharpe REAL,
            total_pnl REAL,
            params_str TEXT NOT NULL DEFAULT '',
            added_at TEXT NOT NULL,
            UNIQUE(run_id, combo_hash)
        )
    """)
    con.commit()
    con.close()

    store = StoreService(state_dir, bundles)
    con = sqlite3.connect(str(db))
    cols = {row[1] for row in con.execute("PRAGMA table_info(favourites)")}
    con.close()
    assert "ann_return" in cols

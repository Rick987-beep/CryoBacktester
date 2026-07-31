"""
tests/ui/test_detail_view.py — Detail view unit tests.
"""
import pytest
import panel as pn


def test_stats_card_contains_key_metrics(tiny_grid_result):
    """Stats card HTML for the best combo contains Sharpe, Total PnL, Max DD."""
    from backtester.ui.views.detail_view import _stats_card_html, _N_METRIC_ROWS
    result = tiny_grid_result
    key = result.best_key
    stats = result.all_stats[key]
    eq = result.top_n_eq.get(key)
    rank = 1

    html = _stats_card_html(stats, eq, key, rank)
    assert "Sharpe" in html
    assert "Total PnL" in html
    assert "Annualized Return" in html
    assert "Max DD" in html
    assert "Parameters" in html
    assert "Performance Metrics" in html
    assert "Params" not in html.replace("Parameters", "")
    assert html.count("<tr>") == _N_METRIC_ROWS + 1  # header + body rows
    # Ann return shown as xx.x%
    assert "%" in html


def test_trades_table_filtered_to_combo(tiny_grid_result):
    """_trades_df returns rows equal to trades for that combo only."""
    from backtester.ui.views.detail_view import _trades_df
    result = tiny_grid_result

    for combo_idx in range(3):
        df_t = _trades_df(result, combo_idx)
        expected = int((result.df["combo_idx"] == combo_idx).sum())
        assert len(df_t) == expected, (
            f"combo_idx={combo_idx}: expected {expected} rows, got {len(df_t)}"
        )


def test_trades_df_has_derived_columns(tiny_grid_result):
    """_trades_df adds days_held and pnl_pct derived columns."""
    from backtester.ui.views.detail_view import _trades_df
    df_t = _trades_df(tiny_grid_result, 0)
    assert "days_held" in df_t.columns
    assert "pnl_pct" in df_t.columns


def test_trades_df_includes_comment_when_present():
    """Fills view shows comment when column exists; omits when absent (old bundles)."""
    import pandas as pd
    from types import SimpleNamespace
    from backtester.ui.views.detail_view import _trades_df, _FILLS_COLS

    assert "comment" in _FILLS_COLS

    base = {
        "combo_idx": [0, 0],
        "ts": pd.to_datetime(["2025-07-28 17:15", "2025-07-28 17:15"]),
        "trade_idx": [1, 2],
        "event": ["open", "open"],
        "contract": ["BTC-29JUL25-121000-C", "BTC-29JUL25-114000-P"],
        "side": ["sell", "sell"],
        "qty": [5.1, 5.1],
        "amount_usd": [100.0, 100.0],
        "balance_usd": [100_000.0, 100_100.0],
        "fee_usd": [1.0, 1.0],
        "spot": [118_000.0, 118_000.0],
        "exit_reason": ["", ""],
    }
    with_comment = pd.DataFrame({**base, "comment": ["macro delay 2h", "macro delay 2h"]})
    result = SimpleNamespace(df_fills=with_comment, df=None)
    out = _trades_df(result, 0)
    assert "comment" in out.columns
    assert list(out["comment"]) == ["macro delay 2h", "macro delay 2h"]

    # Old fills.parquet without comment — must not crash; column omitted
    old = pd.DataFrame(base)
    result_old = SimpleNamespace(df_fills=old, df=None)
    out_old = _trades_df(result_old, 0)
    assert "comment" not in out_old.columns
    assert len(out_old) == 2


def test_detail_view_builds_without_error(tiny_grid_result, sqlite_store):
    """build_detail_view returns a Panel Column without exceptions."""
    pn.extension("tabulator", "plotly", sizing_mode="stretch_width")
    from backtester.ui.state import AppState
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.views.detail_view import build_detail_view

    state = AppState()
    cache = ResultCache(sqlite_store, max_unpinned=5)
    bundle_path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="tiny_test", runtime_s=0.1, source="test"
    )
    run_id = sqlite_store.register_bundle(bundle_path)
    cache.get(run_id)  # warm cache

    view = build_detail_view(state, cache)
    assert isinstance(view, pn.Column)

    # Trigger a render by setting the active run + combo
    state.active_run_id = run_id
    state.active_combo_key = tiny_grid_result.best_key
    # Should not raise

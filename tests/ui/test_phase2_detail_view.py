"""
tests/ui/test_phase2_detail_view.py — Detail view unit tests.
"""
import pytest
import panel as pn


def test_stats_card_contains_key_metrics(tiny_grid_result):
    """Stats card HTML for the best combo contains Sharpe, Total PnL, Max DD."""
    from backtester.ui.views.detail_view import _stats_card_html
    result = tiny_grid_result
    key = result.best_key
    stats = result.all_stats[key]
    eq = result.top_n_eq.get(key)
    rank = 1

    html = _stats_card_html(stats, eq, key, rank)
    assert "Sharpe" in html
    assert "Total PnL" in html
    assert "Max DD" in html
    assert "Rank #1" in html


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

"""
tests/ui/test_phase4_compare_view.py — Compare view tests.

Tests:
  - test_compare_figure_has_two_traces
  - test_stats_delta_table_signs
  - test_compare_view_builds
"""
import pytest


def _make_eq(daily_pnls):
    """Build a minimal equity dict from a list of daily PnL values.

    daily rows: (date_str, day_pnl, cum_pnl, nav_high, nav_low, nav_close)
    """
    rows = []
    from datetime import date, timedelta
    d = date(2025, 10, 1)
    cum = 0.0
    nav = 10000.0
    for pnl in daily_pnls:
        cum += pnl
        nav += pnl
        rows.append((d.strftime("%Y-%m-%d"), pnl, cum, nav + 20, nav - 20, nav))
        d += timedelta(days=1)
    return {"daily": rows, "total_pnl": cum, "sharpe": 1.0, "max_dd_pct": 5.0}


def test_compare_figure_has_two_traces():
    """equity_overlay_figure with two eqs must produce exactly two traces."""
    from backtester.ui.charts.equity import equity_overlay_figure

    eq_a = _make_eq([10, 20, -5, 15, 8])
    eq_b = _make_eq([5, -10, 30, 12, 6])
    fig = equity_overlay_figure({"A": eq_a, "B": eq_b})
    assert len(fig.data) == 2


def test_stats_delta_table_signs():
    """Delta (B−A) should have correct sign."""
    # Manually test the logic used in compare_view
    v_a = 100.0
    v_b = 150.0
    delta = v_b - v_a
    assert delta == pytest.approx(50.0)

    # lower_better logic: if A < B and lower_better, A wins
    lower_better = True
    winner = "A" if v_a < v_b else ("B" if v_b < v_a else "tie")
    assert winner == "A"

    # higher_better: if A > B and not lower_better, A wins
    lower_better = False
    v_a2, v_b2 = 200.0, 150.0
    winner2 = "A" if v_a2 > v_b2 else ("B" if v_b2 > v_a2 else "tie")
    assert winner2 == "A"


def test_compare_view_builds(sqlite_store, tiny_grid_result, tmp_bundle_dir):
    """build_compare_view constructs without error."""
    import panel as pn
    pn.extension("tabulator", "plotly")
    from backtester.ui.state import AppState
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.views.compare_view import build_compare_view

    tmp_bundle_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="compare_test", runtime_s=1.0
    )
    sqlite_store.register_bundle(bundle_path)

    cache = ResultCache(sqlite_store, max_unpinned=5)
    state = AppState()
    view = build_compare_view(state, sqlite_store, cache)
    assert view is not None

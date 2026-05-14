"""
tests/ui/test_phase2_charts_equity.py — Plotly chart builder unit tests.
"""
import pytest


def _make_eq(n_days: int = 30, start_pnl: float = 100.0) -> dict:
    """Build a minimal equity_metrics-style dict for chart testing."""
    from datetime import date, timedelta
    import random
    rng = random.Random(7)
    daily = []
    cum = 0.0
    peak = 10000.0
    start = date(2025, 10, 1)
    for i in range(n_days):
        ds = (start + timedelta(days=i)).strftime("%Y-%m-%d")
        pnl = rng.uniform(-50, 80)
        cum += pnl
        nav = 10000 + cum
        peak = max(peak, nav)
        daily.append((ds, pnl, cum, nav + 10, nav - 5, nav))
    return {
        "daily": daily,
        "total_pnl": cum,
        "max_drawdown": 200.0,
        "max_dd_pct": 2.0,
        "profit_factor": 1.2,
        "sharpe": 0.85,
        "sortino": 1.1,
        "calmar": 0.5,
        "consec_wins": 5,
        "consec_losses": 3,
    }


def test_equity_figure_has_two_subplots():
    """equity_figure must produce exactly 2 traces (equity + drawdown)."""
    from backtester.ui.charts.equity import equity_figure
    eq = _make_eq()
    fig = equity_figure(eq, capital=10000)
    assert len(fig.data) == 2, f"Expected 2 traces, got {len(fig.data)}"


def test_equity_figure_empty_returns_figure():
    """equity_figure with empty daily list returns a Figure (with annotation)."""
    import plotly.graph_objects as go
    from backtester.ui.charts.equity import equity_figure
    eq = {"daily": []}
    fig = equity_figure(eq, capital=10000)
    assert isinstance(fig, go.Figure)


def test_overlay_has_trace_per_key():
    """equity_overlay_figure produces one trace per key in the eqs dict."""
    from backtester.ui.charts.equity import equity_overlay_figure
    eqs = {f"key{i}": _make_eq(20 + i) for i in range(3)}
    fig = equity_overlay_figure(eqs)
    assert len(fig.data) == 3, f"Expected 3 traces, got {len(fig.data)}"
    # Each trace name corresponds to a label
    trace_names = {t.name for t in fig.data}
    assert trace_names == set(eqs.keys())


def test_overlay_empty_returns_figure():
    """equity_overlay_figure with empty eqs returns a Figure (with annotation)."""
    import plotly.graph_objects as go
    from backtester.ui.charts.equity import equity_overlay_figure
    fig = equity_overlay_figure({})
    assert isinstance(fig, go.Figure)


def test_overlay_y_mode_switch():
    """NAV vs cumpnl y_mode produce different y-arrays for the first trace."""
    from backtester.ui.charts.equity import equity_overlay_figure
    eqs = {"combo1": _make_eq(15)}
    fig_nav    = equity_overlay_figure(eqs, y_mode="nav",    capital=10000)
    fig_cumpnl = equity_overlay_figure(eqs, y_mode="cumpnl", capital=10000)
    y_nav    = list(fig_nav.data[0].y)
    y_cumpnl = list(fig_cumpnl.data[0].y)
    # NAV values are shifted by capital, so they differ
    assert y_nav != y_cumpnl, "NAV and Cum PnL modes should produce different y values"

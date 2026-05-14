"""
charts/equity.py — Plotly equity curve builders (shared by detail + overlay views).

Two public functions:

    equity_figure(eq, title=None) -> go.Figure
        Single combo equity + underwater drawdown (two subplots).

    equity_overlay_figure(eqs, y_mode="nav") -> go.Figure
        Multi-combo overlay (one trace per combo, shared x-axis).
"""
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Constants ─────────────────────────────────────────────────────────────────

_EQUITY_LINE_COLOR = "#2563eb"
_DD_FILL_COLOR = "rgba(220,38,38,0.3)"
_DD_LINE_COLOR = "#dc2626"
_ZERO_LINE_COLOR = "#6b7280"

# eq["daily"] is a list of tuples:
# (date_str, pnl_d, cum_pnl, high, low, close_nav)
# index:  0          1        2      3    4   5


def _daily_to_arrays(daily: list, capital: float, y_mode: str):
    """Extract dates and y-values (NAV or cumulative PnL) from a daily tuple list."""
    dates = [row[0] for row in daily]
    if y_mode == "nav":
        nav_close = [row[5] for row in daily]
        # Use actual nav_close if the series has more than one distinct value
        # (the fallback path in equity_metrics stores capital+cum at index 5)
        if len(set(nav_close)) > 1:
            y = nav_close
        else:
            y = [capital + row[2] for row in daily]
    else:
        y = [row[2] for row in daily]   # cumulative PnL
    return dates, y


def _daily_close_series(daily: list, capital: float) -> list[float]:
    """Return the close NAV series used for drawdown computation."""
    nav_close = [row[5] for row in daily]
    if len(set(nav_close)) > 1:
        return nav_close
    return [capital + row[2] for row in daily]


def _drawdown_series(daily: list, capital: float) -> list[float]:
    """Underwater drawdown % (negative) at each daily close."""
    closes = _daily_close_series(daily, capital)
    peak = capital
    dd = []
    for close in closes:
        peak = max(peak, close)
        dd.append(-100.0 * (peak - close) / peak if peak > 0 else 0.0)
    return dd


# ── Single-combo figure ───────────────────────────────────────────────────────

def equity_figure(eq: dict, title: str | None = None, capital: float = 10000,
                  y_mode: str = "nav") -> go.Figure:
    """Build a two-subplot figure: equity (top) + underwater drawdown (bottom).

    Args:
        eq:       equity_metrics() result dict with a "daily" list.
        title:    Optional figure title.
        capital:  Account size (for NAV baseline).
        y_mode:   "nav" | "cumpnl"
    """
    daily = eq.get("daily", [])
    if not daily:
        fig = go.Figure()
        fig.add_annotation(text="No equity data", showarrow=False)
        return fig

    dates, y = _daily_to_arrays(daily, capital, y_mode)
    dd = _drawdown_series(daily, capital)

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        row_heights=[0.7, 0.3],
        vertical_spacing=0.04,
    )

    # Equity trace
    fig.add_trace(
        go.Scatter(
            x=dates, y=y,
            mode="lines",
            name="Equity",
            line=dict(color=_EQUITY_LINE_COLOR, width=1.5),
            hovertemplate="%{x}: %{y:,.0f}<extra></extra>",
        ),
        row=1, col=1,
    )

    # Max-DD annotation
    if dd:
        max_dd_idx = min(range(len(dd)), key=lambda i: dd[i])
        fig.add_annotation(
            x=dates[max_dd_idx],
            y=y[max_dd_idx],
            text=f"Max DD {dd[max_dd_idx]:.1f}%",
            showarrow=True,
            arrowhead=2,
            arrowcolor=_DD_LINE_COLOR,
            font=dict(color=_DD_LINE_COLOR, size=10),
            row=1, col=1,
        )

    # Drawdown trace (underwater, filled)
    fig.add_trace(
        go.Scatter(
            x=dates, y=dd,
            mode="lines",
            name="Drawdown %",
            fill="tozeroy",
            line=dict(color=_DD_LINE_COLOR, width=1),
            fillcolor=_DD_FILL_COLOR,
            hovertemplate="%{x}: %{y:.1f}%<extra></extra>",
        ),
        row=2, col=1,
    )

    fig.update_layout(
        title=title,
        height=420,
        showlegend=False,
        margin=dict(l=50, r=20, t=40 if title else 20, b=30),
        hovermode="x unified",
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    fig.update_xaxes(showgrid=True, gridcolor="#e5e7eb")
    fig.update_yaxes(showgrid=True, gridcolor="#e5e7eb")
    y_label = "NAV ($)" if y_mode == "nav" else "Cum PnL ($)"
    fig.update_yaxes(title_text=y_label, row=1, col=1)
    fig.update_yaxes(title_text="DD %", row=2, col=1)

    return fig


# ── Multi-combo overlay figure ────────────────────────────────────────────────

_OVERLAY_COLORS = [
    "#2563eb", "#16a34a", "#dc2626", "#d97706", "#7c3aed",
    "#0891b2", "#be185d", "#65a30d", "#ea580c", "#4f46e5",
]


def equity_overlay_figure(eqs: dict, y_mode: str = "nav",
                          capital: float = 10000) -> go.Figure:
    """Overlay equity curves for multiple combos.

    Args:
        eqs:    {label_str: eq_dict}  — label is used as the legend entry.
        y_mode: "nav" | "cumpnl"
    """
    if not eqs:
        fig = go.Figure()
        fig.add_annotation(text="Select combos from the Results Grid tab.",
                           showarrow=False, font=dict(size=14))
        return fig

    fig = go.Figure()
    for i, (label, eq) in enumerate(eqs.items()):
        daily = eq.get("daily", []) if eq else []
        if not daily:
            continue
        dates, y = _daily_to_arrays(daily, capital, y_mode)
        color = _OVERLAY_COLORS[i % len(_OVERLAY_COLORS)]
        fig.add_trace(
            go.Scatter(
                x=dates, y=y,
                mode="lines",
                name=label,
                line=dict(color=color, width=1.5),
                hovertemplate=f"{label}: %{{y:,.0f}}<extra></extra>",
            )
        )

    y_label = "NAV ($)" if y_mode == "nav" else "Cum PnL ($)"
    fig.update_layout(
        height=400,
        margin=dict(l=50, r=20, t=20, b=30),
        hovermode="x unified",
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(orientation="v", x=1.01, y=1),
    )
    fig.update_xaxes(showgrid=True, gridcolor="#e5e7eb")
    fig.update_yaxes(showgrid=True, gridcolor="#e5e7eb", title_text=y_label)
    return fig

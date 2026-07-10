"""Square performance infographic: monthly bars, equity/drawdown, metric tiles."""
from __future__ import annotations

import math
from datetime import datetime
from typing import Any

import pandas as pd

# Marketing palette (matches run146 / marketing_report.html)
C_BG = "#070d18"
C_PANEL = "#0f1a2b"
C_PANEL_BORDER = "#1c2d44"
C_GRID = "#1e2d45"
C_EQUITY = "#3b9eff"
C_EQUITY_GLOW = "rgba(59,158,255,0.22)"
C_POS = "#3b9eff"
C_NEG = "#ff5c6c"
C_DD = "#ff5c6c"
C_DD_FILL = "rgba(255,92,108,0.32)"
C_TEXT = "#edf2f9"
C_MUTED = "#8fa3bc"
C_GOLD = "#c9a84c"
FONT_DIN = (
    '"DIN Next LT Pro", "DIN Next", "DIN Alternate", Barlow, '
    '"Helvetica Neue", Helvetica, Arial, sans-serif'
)

W = H = 1080
PAD = 44
CARD_TITLE_FS = 20


def _monthly_compounded_returns(daily_df: pd.DataFrame, capital: float) -> pd.Series:
    """Month-end NAV to month-end NAV compounded return (%)."""
    df = daily_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M")
    month_end = df.groupby("month", sort=True)["nav_close"].last()
    ret = month_end.pct_change() * 100.0
    ret.iloc[0] = (month_end.iloc[0] / capital - 1.0) * 100.0
    return ret


def _month_label(period) -> str:
    dt = period.to_timestamp()
    if period.month == 6 and period.year == 2025:
        return "Jun'25"
    if period.month == 6 and period.year == 2026:
        return "Jun'26"
    return dt.strftime("%b")


def _nice_step(span: float, n_ticks: int = 4) -> float:
    raw = span / n_ticks
    mag = 10 ** math.floor(math.log10(max(raw, 1e-9)))
    for f in (1, 2, 2.5, 5, 10):
        if raw <= f * mag:
            return f * mag
    return 10 * mag


def _panel(x: float, y: float, w: float, h: float, rx: float = 12) -> str:
    return (
        f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" '
        f'fill="{C_PANEL}" stroke="{C_PANEL_BORDER}" stroke-width="1" rx="{rx}"/>'
    )


def _month_x_positions(daily_df: pd.DataFrame, x0: float, pw: float) -> list[tuple[Any, float]]:
    """Return (period, x_center) for each month in the daily series."""
    df = daily_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M")
    n = len(df)
    positions: list[tuple[Any, float]] = []
    for period, idx in df.groupby("month", sort=True).groups.items():
        center_i = (min(idx) + max(idx)) / 2.0
        positions.append((period, x0 + center_i / max(n - 1, 1) * pw))
    return positions


def _monthly_bars_svg(
    monthly: pd.Series,
    x: float,
    y: float,
    w: float,
    h: float,
) -> list[str]:
    parts: list[str] = []
    axis_fs = 15
    month_fs = 14
    left_margin = 62
    right_margin = 14
    title_h = 46
    xaxis_h = 34
    title_y = y + 32
    parts.append(
        f'<text x="{x + 16}" y="{title_y}" fill="{C_MUTED}" '
        f'font-family=\'{FONT_DIN}\' font-size="{CARD_TITLE_FS}" font-weight="600" '
        f'letter-spacing="0.14em">MONTHLY PERFORMANCE</text>'
    )

    chart_x = x + left_margin
    chart_y = y + title_h
    chart_w = w - left_margin - right_margin
    chart_h = h - title_h - xaxis_h
    base_y = chart_y + chart_h
    label_x = x + 18
    month_y = base_y + 14

    vals = monthly.values.astype(float)
    y_max = max(float(max(vals)), 1.0) * 1.12
    bar_w = chart_w / max(len(monthly), 1) * 0.62
    gap = chart_w / max(len(monthly), 1)

    parts.append(
        f'<line x1="{chart_x}" y1="{base_y:.1f}" x2="{chart_x + chart_w}" y2="{base_y:.1f}" '
        f'stroke="{C_GRID}" stroke-width="1"/>'
    )

    step = _nice_step(y_max)
    t = 0.0
    while t <= y_max + step * 0.01:
        yy = base_y - (t / y_max) * chart_h
        parts.append(
            f'<line x1="{chart_x}" y1="{yy:.1f}" x2="{chart_x + chart_w}" y2="{yy:.1f}" '
            f'stroke="{C_GRID}" stroke-width="1" stroke-dasharray="3,5" opacity="0.55"/>'
        )
        label = "0%" if t == 0 else f"+{t:.0f}%"
        label_y = yy - 4 if t == 0 else yy + 5
        parts.append(
            f'<text x="{label_x:.1f}" y="{label_y:.1f}" text-anchor="start" fill="{C_MUTED}" '
            f'font-family=\'{FONT_DIN}\' font-size="{axis_fs}">{label}</text>'
        )
        t += step

    for i, (period, val) in enumerate(monthly.items()):
        cx = chart_x + gap * i + gap * 0.5
        bar_h = max(val / y_max * chart_h, 1.0)
        bx = cx - bar_w / 2
        y0 = base_y - bar_h
        parts.append(
            f'<rect x="{bx:.1f}" y="{y0:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" '
            f'fill="{C_POS}" rx="3" opacity="0.92"/>'
        )
        parts.append(
            f'<text x="{cx:.1f}" y="{month_y:.1f}" text-anchor="middle" '
            f'fill="{C_MUTED}" font-family=\'{FONT_DIN}\' font-size="{month_fs}">'
            f'{_month_label(period)}</text>'
        )

    return parts


def _return_highlight_svg(
    total_return_pct: float,
    daily_df: pd.DataFrame,
    capital: float,
    x: float,
    y: float,
    w: float,
    h: float,
) -> list[str]:
    parts: list[str] = []
    parts.append(
        f'<text x="{x + w / 2:.1f}" y="{y + 40}" text-anchor="middle" fill="{C_MUTED}" '
        f'font-family=\'{FONT_DIN}\' font-size="{CARD_TITLE_FS}" font-weight="600" '
        f'letter-spacing="0.1em">TOTAL RETURN 12 MONTHS</text>'
    )
    ret_label = f"+{total_return_pct:.1f}%"
    parts.append(
        f'<text x="{x + w / 2:.1f}" y="{y + 108}" text-anchor="middle" fill="{C_EQUITY}" '
        f'font-family=\'{FONT_DIN}\' font-size="56" font-weight="700">{ret_label}</text>'
    )

    # Sparkline from month-end NAV
    df = daily_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M")
    nav = df.groupby("month")["nav_close"].last()
    ret_pct = (nav / capital - 1.0) * 100.0

    sx0 = x + 24
    sy0 = y + 130
    sw = w - 48
    sh = h - 150
    n = len(ret_pct)
    if n < 2:
        return parts

    y_min = min(float(ret_pct.min()), 0.0)
    y_max = max(float(ret_pct.max()), 1.0) * 1.05

    def px(i: int) -> float:
        return sx0 + i / (n - 1) * sw

    def py(v: float) -> float:
        return sy0 + sh - (v - y_min) / (y_max - y_min) * sh

    pts = " ".join(f"{px(i):.1f},{py(float(ret_pct.iloc[i])):.1f}" for i in range(n))
    base = py(0.0)
    fill_pts = f"{px(0):.1f},{base:.1f} {pts} {px(n - 1):.1f},{base:.1f}"
    parts.append(f'<polygon points="{fill_pts}" fill="{C_EQUITY_GLOW}"/>')
    parts.append(
        f'<polyline points="{pts}" fill="none" stroke="{C_EQUITY}" stroke-width="2.5" '
        f'stroke-linejoin="round"/>'
    )
    parts.append(
        f'<circle cx="{px(n - 1):.1f}" cy="{py(float(ret_pct.iloc[-1])):.1f}" r="4" fill="{C_EQUITY}"/>'
    )
    return parts


def _equity_dd_svg(
    daily_df: pd.DataFrame,
    capital: float,
    x: float,
    y: float,
    w: float,
    h: float,
) -> list[str]:
    parts: list[str] = []
    axis_fs = 15
    month_fs = 14
    left_margin = 62
    right_margin = 14
    parts.append(
        f'<text x="{x + 16}" y="{y + 32}" fill="{C_MUTED}" '
        f'font-family=\'{FONT_DIN}\' font-size="{CARD_TITLE_FS}" font-weight="600" '
        f'letter-spacing="0.14em">EQUITY &amp; DRAWDOWN</text>'
    )

    df = daily_df.copy()
    ret_pct = df["return_pct"].astype(float).tolist()
    dd_pct = df["drawdown_pct"].astype(float).tolist()
    n = len(ret_pct)
    if n < 2:
        return parts

    mt, mb = 42, 28
    gap = 10
    chart_x = x + left_margin
    chart_w = w - left_margin - right_margin
    label_x = x + 18
    total_chart_h = h - mt - mb
    eq_h = int(total_chart_h * 0.64)
    dd_h = total_chart_h - eq_h - gap
    y0_eq = y + mt
    y0_dd = y0_eq + eq_h + gap
    x_bottom = y0_dd + dd_h

    y_max = max(max(ret_pct), 1.0) * 1.08
    dd_min = min(dd_pct)
    dd_span = max(abs(dd_min), 0.5) * 1.12

    def sx(i: int) -> float:
        return chart_x + i / max(n - 1, 1) * chart_w

    def sy_eq(v: float) -> float:
        return y0_eq + eq_h - (v / y_max) * eq_h

    def sy_dd(v: float) -> float:
        return y0_dd + (-v / dd_span) * dd_h

    # Equity y-axis (0% to max return)
    eq_base = sy_eq(0.0)
    parts.append(
        f'<line x1="{chart_x}" y1="{eq_base:.1f}" x2="{chart_x + chart_w}" y2="{eq_base:.1f}" '
        f'stroke="{C_GRID}" stroke-width="1"/>'
    )
    eq_step = _nice_step(y_max)
    t = 0.0
    while t <= y_max + eq_step * 0.01:
        yy = sy_eq(t)
        parts.append(
            f'<line x1="{chart_x}" y1="{yy:.1f}" x2="{chart_x + chart_w}" y2="{yy:.1f}" '
            f'stroke="{C_GRID}" stroke-width="1" stroke-dasharray="3,5" opacity="0.55"/>'
        )
        label = "0%" if t == 0 else f"+{t:.0f}%"
        label_y = yy - 4 if t == 0 else yy + 5
        parts.append(
            f'<text x="{label_x:.1f}" y="{label_y:.1f}" text-anchor="start" fill="{C_MUTED}" '
            f'font-family=\'{FONT_DIN}\' font-size="{axis_fs}">{label}</text>'
        )
        t += eq_step

    eq_pts = " ".join(f"{sx(i):.1f},{sy_eq(ret_pct[i]):.1f}" for i in range(n))
    parts.append(
        f'<polygon points="{sx(0):.1f},{eq_base:.1f} {eq_pts} {sx(n - 1):.1f},{eq_base:.1f}" '
        f'fill="{C_EQUITY_GLOW}"/>'
    )
    parts.append(
        f'<polyline points="{eq_pts}" fill="none" stroke="{C_EQUITY}" stroke-width="2" '
        f'stroke-linejoin="round"/>'
    )

    # Drawdown y-axis (0% at top, negative below)
    zero_dd = sy_dd(0.0)
    parts.append(
        f'<line x1="{chart_x}" y1="{zero_dd:.1f}" x2="{chart_x + chart_w}" y2="{zero_dd:.1f}" '
        f'stroke="{C_GRID}" stroke-width="1"/>'
    )
    dd_step = _nice_step(dd_span)
    t = dd_step
    while t <= dd_span + dd_step * 0.01:
        yy = sy_dd(-t)
        parts.append(
            f'<line x1="{chart_x}" y1="{yy:.1f}" x2="{chart_x + chart_w}" y2="{yy:.1f}" '
            f'stroke="{C_GRID}" stroke-width="1" stroke-dasharray="3,5" opacity="0.55"/>'
        )
        parts.append(
            f'<text x="{label_x:.1f}" y="{yy + 5:.1f}" text-anchor="start" fill="{C_MUTED}" '
            f'font-family=\'{FONT_DIN}\' font-size="{axis_fs}">-{t:.0f}%</text>'
        )
        t += dd_step

    dd_pts = " ".join(f"{sx(i):.1f},{sy_dd(dd_pct[i]):.1f}" for i in range(n))
    parts.append(
        f'<polygon points="{sx(0):.1f},{zero_dd:.1f} {dd_pts} {sx(n - 1):.1f},{zero_dd:.1f}" '
        f'fill="{C_DD_FILL}"/>'
    )
    parts.append(
        f'<polyline points="{dd_pts}" fill="none" stroke="{C_DD}" stroke-width="1.5"/>'
    )

    # X-axis month labels (aligned to month centres)
    month_positions = _month_x_positions(daily_df, chart_x, chart_w)
    for period, cx in month_positions:
        parts.append(
            f'<text x="{cx:.1f}" y="{x_bottom + 22}" text-anchor="middle" '
            f'fill="{C_MUTED}" font-family=\'{FONT_DIN}\' font-size="{month_fs}">'
            f'{_month_label(period)}</text>'
        )

    return parts


def _metric_tile(
    x: float,
    y: float,
    w: float,
    h: float,
    label: str,
    value: str,
    desc: str,
) -> list[str]:
    parts = [
        _panel(x, y, w, h, rx=10),
        f'<text x="{x + w / 2:.1f}" y="{y + 30}" text-anchor="middle" fill="{C_GOLD}" '
        f'font-family=\'{FONT_DIN}\' font-size="15" font-weight="600" '
        f'letter-spacing="0.1em">{label}</text>',
        f'<text x="{x + w / 2:.1f}" y="{y + 68}" text-anchor="middle" fill="{C_TEXT}" '
        f'font-family=\'{FONT_DIN}\' font-size="32" font-weight="700">{value}</text>',
        f'<text x="{x + w / 2:.1f}" y="{y + 90}" text-anchor="middle" fill="{C_MUTED}" '
        f'font-family=\'{FONT_DIN}\' font-size="10">{desc}</text>',
    ]
    return parts


def performance_square_svg(
    stats: dict[str, Any],
    daily_df: pd.DataFrame,
    capital: float,
) -> str:
    monthly = _monthly_compounded_returns(daily_df, capital)
    expectancy_pct = stats["avg_pnl_usd"] / capital * 100.0

    parts: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}">',
        "<defs>",
        "<style>",
        "@import url('https://fonts.googleapis.com/css2?family=Barlow:wght@400;500;600;700&amp;display=swap');",
        "</style>",
        f'<radialGradient id="bgGrad" cx="50%" cy="0%" r="80%">'
        f'<stop offset="0%" stop-color="#152238"/><stop offset="55%" stop-color="{C_BG}"/>'
        f"</radialGradient>",
        "</defs>",
        f'<rect width="{W}" height="{H}" fill="url(#bgGrad)"/>',
        # Header
        f'<text x="{PAD}" y="{PAD + 14}" fill="{C_GOLD}" font-family=\'{FONT_DIN}\' '
        f'font-size="11" font-weight="600" letter-spacing="0.2em">'
        f"SIMULATED TRACK RECORD · JUN 2025 TO JUN 2026</text>",
        f'<text x="{PAD}" y="{PAD + 52}" fill="{C_TEXT}" font-family=\'{FONT_DIN}\' '
        f'font-size="34" font-weight="700">Short Gamma Engine</text>',
        f'<text x="{PAD}" y="{PAD + 82}" fill="{C_MUTED}" font-family=\'{FONT_DIN}\' '
        f'font-size="16" font-weight="500">Performance Overview</text>',
    ]

    # Top row panels
    top_y = 148
    top_h = 268
    left_w = 520
    right_w = W - PAD * 2 - left_w - 16
    parts.append(_panel(PAD, top_y, left_w, top_h))
    parts.extend(_monthly_bars_svg(monthly, PAD, top_y, left_w, top_h))

    right_x = PAD + left_w + 16
    parts.append(_panel(right_x, top_y, right_w, top_h))
    parts.extend(
        _return_highlight_svg(
            float(stats["total_return_pct"]), daily_df, capital,
            right_x, top_y, right_w, top_h,
        )
    )

    # Equity + drawdown
    eq_y = top_y + top_h + 16
    eq_h = 400
    eq_w = W - PAD * 2
    parts.append(_panel(PAD, eq_y, eq_w, eq_h))
    parts.extend(_equity_dd_svg(daily_df, capital, PAD, eq_y, eq_w, eq_h))

    # Metric tiles
    tiles_y = eq_y + eq_h + 12
    tile_h = 108
    tile_gap = 10
    tile_w = (eq_w - tile_gap * 5) / 6
    metrics = [
        ("SHARPE", f"{stats['sharpe']:.2f}", "Return per unit of volatility"),
        ("SORTINO", f"{stats['sortino']:.1f}", "Return per unit of downside vol"),
        ("CALMAR", f"{stats['calmar']:.1f}", "CAGR divided by max drawdown"),
        ("MAX DD", f"-{stats['max_dd_pct']:.1f}%", "Peak-to-trough decline"),
        ("EXPECTANCY", f"+{expectancy_pct:.2f}%", "Average return per trade"),
        ("PROFIT FACTOR", f"{stats['profit_factor']:.1f}x", "Gross wins over gross losses"),
    ]
    for i, (label, value, desc) in enumerate(metrics):
        tx = PAD + i * (tile_w + tile_gap)
        parts.extend(_metric_tile(tx, tiles_y, tile_w, tile_h, label, value, desc))

    # Footer
    parts.append(
        f'<text x="{W / 2:.1f}" y="{H - 18}" text-anchor="middle" fill="#5c708a" '
        f'font-family=\'{FONT_DIN}\' font-size="10">'
        f"Simulated performance. Past results are not indicative of future returns."
        f"</text>"
    )
    parts.append("</svg>")
    return "\n".join(parts)

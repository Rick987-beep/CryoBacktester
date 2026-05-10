#!/usr/bin/env python3
"""
reporting_charts.py — SVG chart generators for the backtester HTML report.

All functions are pure: they take plain Python data structures and return
self-contained SVG strings. No GridResult, no pandas, no external CSS.

Public API:
    equity_chart_svg(daily_rows, capital, width, height)
    fan_chart_svg(curves, capital, width, height)
    histogram_svg(pnl_values, highlight_pnl, n_bins, width, height)
    marginal_bar_chart_svg(sensitivity_rows, param_name, width, height)
    sparkline_svg(points, width, height)
"""
import math


# ── Shared helpers ───────────────────────────────────────────────

def _fmt_val(v):
    if isinstance(v, float) and v != int(v):
        # Use :g to preserve all significant digits (e.g. 0.0002 stays 0.0002,
        # not 0.00 as with :.2f). Trailing zeros are stripped automatically.
        return f"{v:g}"
    return str(int(v) if isinstance(v, float) else v)


def _fmt_pnl(v):
    return f"${v:,.0f}"


def _nice_step(span, n_ticks=6):
    raw = span / n_ticks
    mag = 10 ** math.floor(math.log10(max(raw, 1e-9)))
    for f in (1, 2, 2.5, 5, 10):
        if raw <= f * mag:
            return f * mag
    return 10 * mag


# ── Sparkline ────────────────────────────────────────────────────

def sparkline_svg(points, width=300, height=40):
    if not points or len(points) < 2:
        return ""
    ymin, ymax = min(points), max(points)
    if ymax == ymin:
        ymax = ymin + 1
    n = len(points)
    coords = [
        f"{i / (n-1) * width:.1f},"
        f"{height - (y - ymin) / (ymax - ymin) * (height-4) - 2:.1f}"
        for i, y in enumerate(points)
    ]
    zero_y = height - (0 - ymin) / (ymax - ymin) * (height-4) - 2
    return (
        f'<svg width="{width}" height="{height}" style="vertical-align:middle">'
        f'<line x1="0" y1="{zero_y:.1f}" x2="{width}" y2="{zero_y:.1f}" '
        f'stroke="#ccc" stroke-width="1" stroke-dasharray="4,3"/>'
        f'<polyline points="{" ".join(coords)}" '
        f'fill="none" stroke="#1565C0" stroke-width="2"/>'
        f'</svg>'
    )


# ── Single-combo equity curve ────────────────────────────────────

def equity_chart_svg(daily_rows, capital=10000, width=860, height=260):
    """Full equity curve SVG with labelled dollar Y-axis and day-number X-axis.

    daily_rows: list of (date_str, day_pnl, cum_pnl, high, low, close)
    Returns a self-contained <svg> string.
    """
    if not daily_rows or len(daily_rows) < 2:
        return ""

    ml, mr, mt, mb = 80, 20, 18, 36   # margins: left, right, top, bottom
    pw = width - ml - mr
    ph = height - mt - mb

    eq_vals = [row[5] for row in daily_rows]  # close (NAV at end of day)
    hi_vals  = [row[3] for row in daily_rows]  # intraday high
    lo_vals  = [row[4] for row in daily_rows]  # intraday low
    # Prepend Day 0 = initial capital so x-axis starts at 0
    plot_vals = [capital] + eq_vals
    plot_hi   = [capital] + hi_vals
    plot_lo   = [capital] + lo_vals
    n_pts = len(plot_vals)
    # Include full intraday range and capital baseline in y-axis bounds
    y_min = min(min(plot_lo), capital)
    y_max = max(max(plot_hi), capital)
    y_range = max(y_max - y_min, 1.0)
    y_lo = y_min - y_range * 0.05
    y_hi = y_max + y_range * 0.05

    step = _nice_step(y_hi - y_lo)
    first_tick = math.ceil(y_lo / step) * step
    y_ticks = []
    t = first_tick
    while t <= y_hi + step * 0.01:
        y_ticks.append(t)
        t += step

    def sx(i):    # point index (0 = Day 0) → pixel x
        return ml + i / max(n_pts - 1, 1) * pw

    def sy(v):    # equity value → pixel y
        return mt + (1.0 - (v - y_lo) / (y_hi - y_lo)) * ph

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'style="display:block;font-family:-apple-system,BlinkMacSystemFont,sans-serif;'
        f'font-size:11px;color:#333">'
    ]

    # Clip path for plot area (prevents fill/line overflow into margins)
    parts.append(
        f'<defs><clipPath id="plot-clip">'
        f'<rect x="{ml}" y="{mt}" width="{pw}" height="{ph}"/>'
        f'</clipPath></defs>'
    )

    # Plot area background
    parts.append(
        f'<rect x="{ml}" y="{mt}" width="{pw}" height="{ph}" '
        f'fill="#fafafa" stroke="#ddd" stroke-width="1"/>'
    )

    # Y-axis gridlines + labels
    for tick in y_ticks:
        py = sy(tick)
        if mt - 1 <= py <= mt + ph + 1:
            parts.append(
                f'<line x1="{ml}" y1="{py:.1f}" x2="{ml+pw}" y2="{py:.1f}" '
                f'stroke="#e0e0e0" stroke-width="1"/>'
            )
            label = f"${tick:,.0f}"
            parts.append(
                f'<text x="{ml-6}" y="{py+4:.1f}" text-anchor="end" fill="#666">{label}</text>'
            )

    # Capital / zero-gain baseline (dashed)
    py_cap = sy(capital)
    if mt <= py_cap <= mt + ph:
        parts.append(
            f'<line x1="{ml}" y1="{py_cap:.1f}" x2="{ml+pw}" y2="{py_cap:.1f}" '
            f'stroke="#999" stroke-width="1" stroke-dasharray="6,4"/>'
        )
        parts.append(
            f'<text x="{ml+4}" y="{py_cap-4:.1f}" fill="#888" font-size="10">'
            f'start ${capital:,.0f}</text>'
        )

    # X-axis tick labels (day number, spread evenly, ~8 labels max)
    x_step = max(1, round(n_pts / 8))
    for i in range(n_pts):
        if i == 0 or i == n_pts - 1 or i % x_step == 0:
            px = sx(i)
            parts.append(
                f'<line x1="{px:.1f}" y1="{mt+ph}" x2="{px:.1f}" y2="{mt+ph+4}" '
                f'stroke="#aaa" stroke-width="1"/>'
            )
            parts.append(
                f'<text x="{px:.1f}" y="{mt+ph+16}" text-anchor="middle" fill="#666">'
                f'Day {i}</text>'
            )

    # Axis lines
    parts.append(
        f'<line x1="{ml}" y1="{mt}" x2="{ml}" y2="{mt+ph}" stroke="#aaa" stroke-width="1"/>'
    )
    parts.append(
        f'<line x1="{ml}" y1="{mt+ph}" x2="{ml+pw}" y2="{mt+ph}" stroke="#aaa" stroke-width="1"/>'
    )

    # Axis titles
    parts.append(
        f'<text transform="rotate(-90)" x="-{mt+ph//2}" y="14" '
        f'text-anchor="middle" fill="#555" font-size="11">Equity (USD)</text>'
    )
    parts.append(
        f'<text x="{ml + pw // 2}" y="{height - 2}" '
        f'text-anchor="middle" fill="#555" font-size="11">Day #</text>'
    )

    # Intraday high/low band (shaded, clipped to plot bounds)
    band_fwd = " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(plot_hi))
    band_rev = " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in reversed(list(enumerate(plot_lo))))
    parts.append(
        f'<polygon points="{band_fwd} {band_rev}" fill="#1565C0" fill-opacity="0.08" '
        f'clip-path="url(#plot-clip)"/>'
    )

    # Fill under curve (light blue area, clipped to plot bounds)
    fill_pts = (
        f"{sx(0):.1f},{sy(capital):.1f} "
        + " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(plot_vals))
        + f" {sx(n_pts-1):.1f},{sy(capital):.1f}"
    )
    parts.append(
        f'<polygon points="{fill_pts}" fill="#1565C0" fill-opacity="0.07" clip-path="url(#plot-clip)"/>'
    )

    # Equity curve line
    line_pts = " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(plot_vals))
    parts.append(
        f'<polyline points="{line_pts}" fill="none" stroke="#1565C0" '
        f'stroke-width="2" stroke-linejoin="round" clip-path="url(#plot-clip)"/>'
    )

    # Final dot
    parts.append(
        f'<circle cx="{sx(n_pts-1):.1f}" cy="{sy(plot_vals[-1]):.1f}" r="3" '
        f'fill="#1565C0"/>'
    )

    parts.append("</svg>")
    return "\n".join(parts)


# ── Performance fan chart ────────────────────────────────────────

def _lerp_color(c1, c2, t):
    """Linearly interpolate between two '#rrggbb' hex colors."""
    r1, g1, b1 = int(c1[1:3], 16), int(c1[3:5], 16), int(c1[5:7], 16)
    r2, g2, b2 = int(c2[1:3], 16), int(c2[3:5], 16), int(c2[5:7], 16)
    return (f"#{int(r1+(r2-r1)*t):02x}"
            f"{int(g1+(g2-g1)*t):02x}"
            f"{int(b1+(b2-b1)*t):02x}")


def _rank_style(rank, n_curves):
    """Return (color_hex, opacity, stroke_width) for a rank (1 = best)."""
    if rank == 1:
        return "#1b5e20", 1.0, 2.5
    if rank <= 5:                                   # top-tier greens
        t = (rank - 2) / max(3.0, 1)
        return _lerp_color("#43a047", "#a5d6a7", t), 0.85, 1.5
    if rank <= 12:                                  # mid-tier ambers
        t = (rank - 6) / max(6.0, 1)
        return _lerp_color("#fb8c00", "#ffe082", t), 0.65, 1.0
    t = (rank - 13) / max(float(n_curves - 13), 1.0)   # bottom-tier reds
    return _lerp_color("#e53935", "#ffcdd2", t), 0.45, 0.8


def fan_chart_svg(curves, capital=10000, width=920, height=340):
    """Performance fan — all top-N equity curves in one SVG.

    Three layers (bottom to top):
      1. Shaded envelope band  — min/max range across all combos
      2. Non-winner curves     — rank 20→2, green/amber/red gradient
      3. Winner curve          — bold dark-green, final PnL label

    curves: list of (rank, total_pnl, eq_values, tooltip)
    """
    if not curves or len(curves[0][2]) < 2:
        return ""

    n_curves = len(curves)
    n_days   = len(curves[0][2])

    ml, mr, mt, mb = 80, 30, 20, 42
    pw = width - ml - mr
    ph = height - mt - mb

    # Axis range — include starting capital in bounds
    all_vals = [v for _, _, eq, _ in curves for v in eq] + [float(capital)]
    y_min, y_max = min(all_vals), max(all_vals)
    y_range = max(y_max - y_min, 1.0)
    y_lo = y_min - y_range * 0.06
    y_hi = y_max + y_range * 0.08

    step       = _nice_step(y_hi - y_lo)
    first_tick = math.ceil(y_lo / step) * step
    y_ticks    = []
    t = first_tick
    while t <= y_hi + step * 0.01:
        y_ticks.append(t)
        t += step

    def sx(i):  return ml + i / max(n_days - 1, 1) * pw
    def sy(v):  return mt + (1.0 - (v - y_lo) / (y_hi - y_lo)) * ph

    p = []
    p.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'style="display:block;font-family:-apple-system,BlinkMacSystemFont,sans-serif;'
        f'font-size:11px">'
    )

    # Plot background
    p.append(f'<rect x="{ml}" y="{mt}" width="{pw}" height="{ph}" '
             f'fill="#fafafa" stroke="#ddd" stroke-width="1"/>')

    # Y-axis gridlines + labels
    for tick in y_ticks:
        py = sy(tick)
        if mt - 1 <= py <= mt + ph + 1:
            p.append(f'<line x1="{ml}" y1="{py:.1f}" x2="{ml+pw}" y2="{py:.1f}" '
                     f'stroke="#ececec" stroke-width="1"/>')
            p.append(f'<text x="{ml-6}" y="{py+4:.1f}" text-anchor="end" '
                     f'fill="#777">${tick:,.0f}</text>')

    # Capital baseline (dashed)
    py_cap = sy(capital)
    if mt <= py_cap <= mt + ph:
        p.append(f'<line x1="{ml}" y1="{py_cap:.1f}" x2="{ml+pw}" y2="{py_cap:.1f}" '
                 f'stroke="#bbb" stroke-width="1" stroke-dasharray="5,4"/>')
        p.append(f'<text x="{ml+4}" y="{py_cap-4:.1f}" fill="#aaa" font-size="10">'
                 f'start ${capital:,.0f}</text>')

    # X-axis ticks
    x_step = max(1, round(n_days / 8))
    for i in range(n_days):
        if i == 0 or i == n_days - 1 or i % x_step == 0:
            px = sx(i)
            p.append(f'<line x1="{px:.1f}" y1="{mt+ph}" x2="{px:.1f}" y2="{mt+ph+4}" '
                     f'stroke="#aaa" stroke-width="1"/>')
            p.append(f'<text x="{px:.1f}" y="{mt+ph+16}" text-anchor="middle" fill="#777">'
                     f'Day {i+1}</text>')

    # ── Layer 1: Envelope band ───────────────────────────────────
    env_top     = [max(c[2][i] for c in curves) for i in range(n_days)]
    env_bot     = [min(c[2][i] for c in curves) for i in range(n_days)]
    top_pts     = " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(env_top))
    bot_pts_rev = " ".join(f"{sx(i):.1f},{sy(v):.1f}"
                           for i, v in reversed(list(enumerate(env_bot))))
    p.append(f'<polygon points="{top_pts} {bot_pts_rev}" '
             f'fill="#bbdefb" fill-opacity="0.35" stroke="none"/>')
    p.append(f'<polyline points="{top_pts}" fill="none" '
             f'stroke="#90caf9" stroke-width="0.8" stroke-opacity="0.6"/>')
    p.append(f'<polyline points="{" ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(env_bot))}" '
             f'fill="none" stroke="#90caf9" stroke-width="0.8" stroke-opacity="0.6"/>')

    # ── Layer 2: Non-winner curves (worst → best order so best sits on top) ──
    for rank, total_pnl, eq, tooltip in reversed(curves[1:]):
        color, opacity, sw = _rank_style(rank, n_curves)
        pts = " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(eq))
        # Hit area
        p.append(f'<polyline points="{pts}" fill="none" stroke="#000" '
                 f'stroke-width="12" stroke-opacity="0" stroke-linejoin="round">'
                 f'<title>{tooltip}</title></polyline>')
        # Visible line
        p.append(f'<polyline points="{pts}" fill="none" stroke="{color}" '
                 f'stroke-width="{sw}" stroke-opacity="{opacity}" stroke-linejoin="round"'
                 f' pointer-events="none"/>')

    # ── Layer 3: Winner ──────────────────────────────────────────
    w_rank, w_pnl, w_eq, w_tip = curves[0]
    w_pts = " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(w_eq))
    # Hit area
    p.append(f'<polyline points="{w_pts}" fill="none" stroke="#000" '
             f'stroke-width="12" stroke-opacity="0" stroke-linejoin="round">'
             f'<title>{w_tip}</title></polyline>')
    # Visible line
    p.append(f'<polyline points="{w_pts}" fill="none" stroke="#1b5e20" '
             f'stroke-width="2.5" stroke-linejoin="round" pointer-events="none"/>')
    wx, wy = sx(n_days - 1), sy(w_eq[-1])
    p.append(f'<circle cx="{wx:.1f}" cy="{wy:.1f}" r="4" fill="#1b5e20" pointer-events="none"/>')
    # Label centered above the final dot, with white backing rect for legibility
    sign = "+" if w_pnl >= 0 else ""
    lbl_text = f"{sign}{_fmt_pnl(w_pnl)}"
    lbl_w, lbl_h = 64, 16
    p.append(f'<rect x="{wx - lbl_w/2:.1f}" y="{wy - 26:.1f}" width="{lbl_w}" height="{lbl_h}" '
             f'fill="white" fill-opacity="0.85" rx="3" pointer-events="none"/>')
    p.append(f'<text x="{wx:.1f}" y="{wy - 14:.1f}" text-anchor="middle" fill="#1b5e20" '
             f'font-weight="bold" font-size="11" pointer-events="none">{lbl_text}</text>')

    # ── Axis lines + titles ──────────────────────────────────────
    p.append(f'<line x1="{ml}" y1="{mt}" x2="{ml}" y2="{mt+ph}" stroke="#aaa" stroke-width="1"/>')
    p.append(f'<line x1="{ml}" y1="{mt+ph}" x2="{ml+pw}" y2="{mt+ph}" stroke="#aaa" stroke-width="1"/>')
    p.append(f'<text transform="rotate(-90)" x="-{mt+ph//2}" y="14" '
             f'text-anchor="middle" fill="#555" font-size="11">Equity (USD)</text>')
    p.append(f'<text x="{ml + pw//2}" y="{height-4}" '
             f'text-anchor="middle" fill="#555" font-size="11">Day #</text>')

    # ── Legend (top-left inside plot area) ───────────────────────
    lx, ly = ml + 10, mt + 14
    legend = [
        ("#1b5e20", 1.0,  2.5, False, "#1 Winner"),
        ("#43a047", 0.85, 1.5, False, "Rank 2\u20135"),
        ("#fb8c00", 0.65, 1.0, False, "Rank 6\u201312"),
        ("#e53935", 0.45, 0.8, False, "Rank 13\u201320"),
        ("#bbdefb", 0.35, 0,   True,  "Min/Max band"),
    ]
    leg_h = len(legend) * 16 + 8
    p.append(f'<rect x="{lx-4}" y="{ly-12}" width="138" height="{leg_h}" '
             f'fill="white" fill-opacity="0.88" rx="3" stroke="#ddd" stroke-width="0.5"/>')
    for j, (color, op, sw, is_fill, lbl) in enumerate(legend):
        yj = ly + j * 16
        if is_fill:
            p.append(f'<rect x="{lx}" y="{yj-6}" width="18" height="9" '
                     f'fill="{color}" fill-opacity="{op}" stroke="#90caf9" stroke-width="0.5"/>')
        else:
            p.append(f'<line x1="{lx}" y1="{yj}" x2="{lx+18}" y2="{yj}" '
                     f'stroke="{color}" stroke-width="{sw}" stroke-opacity="{op}"/>')
            if sw > 2:
                p.append(f'<circle cx="{lx+9}" cy="{yj}" r="2.5" fill="{color}"/>')
        p.append(f'<text x="{lx+24}" y="{yj+4}" fill="#444" font-size="11">{lbl}</text>')

    p.append("</svg>")
    return "\n".join(p)


# ── Robustness: PnL distribution histogram ───────────────────────

def histogram_svg(pnl_values, highlight_pnl=None, n_bins=20, width=700, height=200):
    """Bar histogram of all combo PnLs with an optional vertical highlight marker.

    pnl_values    — list of floats (one per combo)
    highlight_pnl — if given, draw a vertical marker at this PnL (the live combo)
    """
    if not pnl_values:
        return ""

    pnl_min = min(pnl_values)
    pnl_max = max(pnl_values)
    if pnl_max == pnl_min:
        pnl_max = pnl_min + 1

    bin_w = (pnl_max - pnl_min) / n_bins
    bins = [0] * n_bins
    for v in pnl_values:
        idx = min(int((v - pnl_min) / bin_w), n_bins - 1)
        bins[idx] += 1

    max_count = max(bins) if bins else 1

    ml, mr, mt, mb = 50, 20, 14, 36
    pw = width - ml - mr
    ph = height - mt - mb
    bar_gap = 1
    bar_w = pw / n_bins

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'style="display:block;font-family:-apple-system,BlinkMacSystemFont,sans-serif;font-size:11px">'
    ]

    # Background
    parts.append(f'<rect x="{ml}" y="{mt}" width="{pw}" height="{ph}" fill="#fafafa" stroke="#ddd" stroke-width="1"/>')

    # Bars
    for i, count in enumerate(bins):
        x = ml + i * bar_w
        bar_h = (count / max_count) * ph if max_count else 0
        bin_centre = pnl_min + (i + 0.5) * bin_w
        color = "#4caf50" if bin_centre >= 0 else "#e53935"
        if bar_h > 0:
            parts.append(
                f'<rect x="{x + bar_gap:.1f}" y="{mt + ph - bar_h:.1f}" '
                f'width="{bar_w - bar_gap * 2:.1f}" height="{bar_h:.1f}" '
                f'fill="{color}" fill-opacity="0.75">'
                f'<title>{count} combos  ~${bin_centre:,.0f}</title></rect>'
            )

    # Y-axis count labels (just 0 and max)
    parts.append(f'<text x="{ml - 4}" y="{mt + ph}" text-anchor="end" fill="#888">0</text>')
    parts.append(f'<text x="{ml - 4}" y="{mt + 8}" text-anchor="end" fill="#888">{max_count}</text>')

    # X-axis ticks (min, 0 if in range, max)
    def _sx(v):
        return ml + (v - pnl_min) / (pnl_max - pnl_min) * pw

    for label_v in [pnl_min, pnl_max]:
        px = _sx(label_v)
        parts.append(f'<line x1="{px:.1f}" y1="{mt+ph}" x2="{px:.1f}" y2="{mt+ph+4}" stroke="#aaa" stroke-width="1"/>')
        parts.append(f'<text x="{px:.1f}" y="{mt+ph+16}" text-anchor="middle" fill="#666">${label_v:,.0f}</text>')
    if pnl_min < 0 < pnl_max:
        px0 = _sx(0)
        parts.append(f'<line x1="{px0:.1f}" y1="{mt}" x2="{px0:.1f}" y2="{mt+ph}" stroke="#999" stroke-width="1" stroke-dasharray="4,3"/>')
        parts.append(f'<text x="{px0:.1f}" y="{mt+ph+16}" text-anchor="middle" fill="#666">$0</text>')

    # Highlight marker (live combo)
    if highlight_pnl is not None and pnl_min <= highlight_pnl <= pnl_max:
        hx = _sx(highlight_pnl)
        parts.append(f'<line x1="{hx:.1f}" y1="{mt}" x2="{hx:.1f}" y2="{mt+ph}" stroke="#1565C0" stroke-width="2" stroke-dasharray="5,3"/>')
        parts.append(f'<text x="{hx:.1f}" y="{mt - 2}" text-anchor="middle" fill="#1565C0" font-weight="bold" font-size="10">live: ${highlight_pnl:,.0f}</text>')

    # Axis lines
    parts.append(f'<line x1="{ml}" y1="{mt}" x2="{ml}" y2="{mt+ph}" stroke="#aaa" stroke-width="1"/>')
    parts.append(f'<line x1="{ml}" y1="{mt+ph}" x2="{ml+pw}" y2="{mt+ph}" stroke="#aaa" stroke-width="1"/>')
    parts.append(f'<text transform="rotate(-90)" x="-{mt+ph//2}" y="14" text-anchor="middle" fill="#555" font-size="11">Combos</text>')
    parts.append(f'<text x="{ml+pw//2}" y="{height-2}" text-anchor="middle" fill="#555" font-size="11">Total PnL</text>')

    parts.append("</svg>")
    return "\n".join(parts)


# ── Robustness: per-parameter marginal bar chart ─────────────────

def marginal_bar_chart_svg(sensitivity_rows, param_name, width=340, height=180):
    """Horizontal bar chart for one parameter's marginal PnL.

    sensitivity_rows — list of (value, mean_pnl, p10, p90) for each param value.
    Shows mean PnL as a bar, with a p10–p90 error band.
    """
    if not sensitivity_rows:
        return ""

    ml, mr, mt, mb = 80, 30, 14, 20
    pw = width - ml - mr
    ph = height - mt - mb

    all_lo = [r[2] for r in sensitivity_rows]  # p10
    all_hi = [r[3] for r in sensitivity_rows]  # p90
    all_mean = [r[1] for r in sensitivity_rows]

    y_lo = min(min(all_lo), 0) * 1.1 if min(all_lo) < 0 else 0
    y_hi = max(max(all_hi), 0) * 1.1 if max(all_hi) > 0 else 1
    if y_hi == y_lo:
        y_hi = y_lo + 1

    n = len(sensitivity_rows)
    bar_h_px = ph / n * 0.6
    gap_h_px = ph / n

    def _sx(v):  # value → pixel x
        return ml + (v - y_lo) / (y_hi - y_lo) * pw

    def _sy(i):  # row index → pixel y centre
        return mt + (i + 0.5) * gap_h_px

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'style="display:block;font-family:-apple-system,BlinkMacSystemFont,sans-serif;font-size:10px">'
    ]
    parts.append(f'<rect x="{ml}" y="{mt}" width="{pw}" height="{ph}" fill="#fafafa" stroke="#ddd" stroke-width="1"/>')

    # Zero line
    x0 = _sx(0)
    if ml <= x0 <= ml + pw:
        parts.append(f'<line x1="{x0:.1f}" y1="{mt}" x2="{x0:.1f}" y2="{mt+ph}" stroke="#bbb" stroke-width="1" stroke-dasharray="4,3"/>')

    for i, (val, mean_v, p10_v, p90_v) in enumerate(sensitivity_rows):
        cy = _sy(i)
        x_mean = _sx(mean_v)
        x_zero = _sx(0)
        color = "#4caf50" if mean_v >= 0 else "#e53935"

        # Bar from 0 to mean
        bar_x = min(x_zero, x_mean)
        bar_w_px = abs(x_mean - x_zero)
        parts.append(
            f'<rect x="{bar_x:.1f}" y="{cy - bar_h_px/2:.1f}" '
            f'width="{bar_w_px:.1f}" height="{bar_h_px:.1f}" '
            f'fill="{color}" fill-opacity="0.72"/>'
        )

        # p10-p90 error band
        x_p10 = _sx(p10_v)
        x_p90 = _sx(p90_v)
        parts.append(
            f'<line x1="{x_p10:.1f}" y1="{cy:.1f}" x2="{x_p90:.1f}" y2="{cy:.1f}" '
            f'stroke="#555" stroke-width="1.5" opacity="0.5"/>'
        )
        parts.append(f'<line x1="{x_p10:.1f}" y1="{cy-4:.1f}" x2="{x_p10:.1f}" y2="{cy+4:.1f}" stroke="#555" stroke-width="1" opacity="0.5"/>')
        parts.append(f'<line x1="{x_p90:.1f}" y1="{cy-4:.1f}" x2="{x_p90:.1f}" y2="{cy+4:.1f}" stroke="#555" stroke-width="1" opacity="0.5"/>')

        # Y-axis label (param value)
        parts.append(f'<text x="{ml-4}" y="{cy+3:.1f}" text-anchor="end" fill="#444">{_fmt_val(val)}</text>')

        # Mean value label at end of bar
        lbl_x = x_mean + (5 if mean_v >= 0 else -5)
        anchor = "start" if mean_v >= 0 else "end"
        parts.append(f'<text x="{lbl_x:.1f}" y="{cy+3:.1f}" text-anchor="{anchor}" fill="{color}" font-weight="600">${mean_v:,.0f}</text>')

    # Axis
    parts.append(f'<line x1="{ml}" y1="{mt}" x2="{ml}" y2="{mt+ph}" stroke="#aaa" stroke-width="1"/>')
    parts.append(f'<line x1="{ml}" y1="{mt+ph}" x2="{ml+pw}" y2="{mt+ph}" stroke="#aaa" stroke-width="1"/>')

    # Title
    label = param_name.replace("_", " ").title()
    parts.append(f'<text x="{ml+pw//2}" y="{mt-2}" text-anchor="middle" fill="#333" font-size="11" font-weight="600">{label}</text>')

    parts.append("</svg>")
    return "\n".join(parts)

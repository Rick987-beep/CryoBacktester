#!/usr/bin/env python3
"""
reporting_v2.py — Strategy-agnostic self-contained HTML report generator.

Receives a fully pre-computed GridResult and renders it into a single-file
HTML report with no external dependencies. Does zero analysis or recomputation
— all metrics, equity curves, and fan-chart data are read directly from the
GridResult attributes supplied by results.py.

Report sections:
  • Risk summary bar — key metrics for the best combo at a glance
  • Best-combo box — parameters, all scoring metrics, Sortino, Calmar
  • Fan chart — equity curves for the top-N combos, shaded intraday band
  • Leaderboard — top-N combos ranked by composite score with all metrics
  • Heatmaps — auto-generated for every 2D parameter pair
  • Trade log — every entry/exit for the best combo

Usage:
    from backtester.reporting_v2 import generate_html
    html = generate_html(result, strategy_name=..., n_intervals=..., runtime_s=...)
    with open('report.html', 'w') as f:
        f.write(html)
"""
import math
import pandas as pd
from datetime import datetime
from itertools import combinations

from backtester.config import cfg
from backtester.results import GridResult
from backtester.reporting_charts import (
    equity_chart_svg as _equity_chart_svg,
    fan_chart_svg as _fan_chart_svg,
    histogram_svg as _histogram_svg,
    marginal_bar_chart_svg as _marginal_bar_chart_svg,
    sparkline_svg as _sparkline_svg,
)


# ── Heatmap helpers ──────────────────────────────────────────────

def _build_heatmap_data(df, keys, pa, pb):
    """Pool trades by (pa_val, pb_val) and compute cell metrics.

    Cells aggregate across all other parameters, so trade counts are
    balanced and no single thin combo can distort the picture.

    Returns:
        grid_pnl  — {(a,b): total_pnl}
        grid_wr   — {(a,b): win_rate_pct}
        grid_n    — {(a,b): trade_count}
        a_vals, b_vals — sorted unique axis values
    """
    if df.empty:
        return {}, {}, {}, [], []

    mapping = pd.DataFrame({
        "combo_idx": pd.array(range(len(keys)), dtype=df["combo_idx"].dtype),
        "pa_val":    [dict(k).get(pa) for k in keys],
        "pb_val":    [dict(k).get(pb) for k in keys],
    })
    merged = df.merge(mapping, on="combo_idx")

    grp = merged.groupby(["pa_val", "pb_val"])
    grid_pnl = grp["pnl"].sum().to_dict()
    grid_n   = grp["pnl"].count().to_dict()
    wins     = (merged["pnl"] > 0).groupby([merged["pa_val"], merged["pb_val"]]).sum()
    grid_wr  = (wins / grp["pnl"].count() * 100).to_dict()

    a_vals = sorted(set(k[0] for k in grid_pnl))
    b_vals = sorted(set(k[1] for k in grid_pnl))
    return grid_pnl, grid_wr, grid_n, a_vals, b_vals



def _select_pairs(result, heatmap_pairs_override=None):
    """Return (pa, pb) pairs to render in heatmaps.

    Priority:
      1. Caller-supplied override (e.g. strategy HEATMAP_PAIRS)
      2. result.heatmap_pairs — pre-ranked by PnL spread in results.py
    """
    if heatmap_pairs_override:
        all_pairs = list(combinations(sorted(result.param_names), 2))
        valid = [tuple(p) for p in heatmap_pairs_override
                 if tuple(sorted(p)) in [tuple(sorted(x)) for x in all_pairs]]
        if valid:
            return valid
    return list(result.heatmap_pairs)


# ── Formatting helpers ───────────────────────────────────────────

def _fmt_val(v):
    if isinstance(v, float) and v != int(v):
        # Use :g to preserve all significant digits (e.g. 0.0002 stays 0.0002,
        # not 0.00 as with :.2f). Trailing zeros are stripped automatically.
        return f"{v:g}"
    return str(int(v) if isinstance(v, float) else v)


def _fmt_pnl(v):
    return f"${v:,.0f}"


def _param_label(name):
    return name.replace("_", " ").title()


def _pnl_class(v):
    if v > 0: return "pos"
    if v < 0: return "neg"
    return ""


def _heatmap_color(val, vmin, vmax):
    if vmin == vmax:
        return "#f0f0f0"
    t = (val - vmin) / (vmax - vmin)
    if t < 0.5:
        r, g = 255, int(255 * t * 2)
    else:
        r, g = int(255 * (2 - t * 2)), 255
    return f"rgb({r},{g},80)"


def _robustness_section_html(result, highlight_key=None):
    """Render the full Robustness section as an HTML string.

    highlight_key — param tuple for the 'live' combo to mark in charts/table.
                    If None, no highlight is applied.
    """
    parts = []
    parts.append("<h2>Robustness Analysis</h2>")
    parts.append(
        "<p>Distribution of results across <em>all</em> parameter combinations tested. "
        "A flat plateau (low fragility, high % profitable) indicates the edge is real "
        "across the region — not an isolated in-sample spike.</p>"
    )

    # ── Summary card ─────────────────────────────────────────────
    n_combos = len(result.pnl_all)
    pct_pos = result.pct_profitable * 100
    frag = result.fragility_score
    frag_color = "#2e7d32" if frag < 1.5 else ("#fb8c00" if frag < 3.0 else "#c62828")
    pct_color = "#2e7d32" if pct_pos >= 90 else ("#fb8c00" if pct_pos >= 70 else "#c62828")

    # Monotonicity summary: average |ρ| across continuous params
    mono_vals = list(result.monotonicity.values())
    avg_mono = sum(abs(v) for v in mono_vals) / len(mono_vals) if mono_vals else 0.0
    mono_color = "#2e7d32" if avg_mono >= 0.7 else ("#fb8c00" if avg_mono >= 0.4 else "#c62828")
    mono_tip = "smoothly monotone" if avg_mono >= 0.7 else ("mixed" if avg_mono >= 0.4 else "non-monotone / noisy")

    parts.append(f"""<div class="grid-info" style="display:flex;gap:36px;flex-wrap:wrap;align-items:flex-start">
  <div>
    <div class="metric-label">Combos tested</div>
    <div class="metric-value">{n_combos:,}</div>
  </div>
  <div>
    <div class="metric-label">% Profitable</div>
    <div class="metric-value" style="color:{pct_color}">{pct_pos:.0f}%</div>
  </div>
  <div>
    <div class="metric-label">Median PnL</div>
    <div class="metric-value">{_fmt_pnl(result.median_pnl)}</div>
  </div>
  <div>
    <div class="metric-label">P10 PnL</div>
    <div class="metric-value {_pnl_class(result.p10_pnl)}">{_fmt_pnl(result.p10_pnl)}</div>
  </div>
  <div>
    <div class="metric-label">P90 PnL</div>
    <div class="metric-value {_pnl_class(result.p90_pnl)}">{_fmt_pnl(result.p90_pnl)}</div>
  </div>
  <div>
    <div class="metric-label">IQR (P25&ndash;P75)</div>
    <div class="metric-value">{_fmt_pnl(result.pnl_iqr)}</div>
  </div>
  <div title="(max PnL − min PnL) / |median PnL|. Lower = flatter plateau = more robust.">
    <div class="metric-label">Fragility score &#9432;</div>
    <div class="metric-value" style="color:{frag_color}">{frag:.2f}</div>
  </div>
  <div title="Mean |Spearman ρ| across marginal param curves. Higher = smoother hill.">
    <div class="metric-label">Avg monotonicity &#9432;</div>
    <div class="metric-value" style="color:{mono_color}">{avg_mono:.2f} <span style="font-size:12px;color:#888">({mono_tip})</span></div>
  </div>
</div>""")

    # ── PnL distribution histogram ───────────────────────────────
    pnl_values = [pnl for _, pnl in result.pnl_all]
    highlight_pnl = None
    if highlight_key is not None and highlight_key in dict(result.pnl_all):
        highlight_pnl = dict(result.pnl_all)[highlight_key]

    parts.append("<h3>PnL Distribution — All Combos</h3>")
    parts.append(
        "<p style=\"color:#555;font-size:13px;margin:4px 0 8px\">"
        "Green bars = profitable combos. Red = losing. "
        "Blue dashed line = live/target combo (if applicable).</p>"
    )
    parts.append(_histogram_svg(pnl_values, highlight_pnl=highlight_pnl))

    # ── Per-parameter marginal charts ────────────────────────────
    if result.param_sensitivity:
        parts.append("<h3>Per-Parameter Marginal Sensitivity</h3>")
        parts.append(
            "<p style=\"color:#555;font-size:13px;margin:4px 0 12px\">"
            "Average PnL (bar) across all combos sharing each parameter value. "
            "Whiskers show P10&ndash;P90 range. Spearman &rho; measures smoothness of the hill.</p>"
        )
        parts.append('<div style="display:flex;flex-wrap:wrap;gap:24px;margin-bottom:16px">')
        for param, rows in sorted(result.param_sensitivity.items()):
            rho = result.monotonicity.get(param, 0.0)
            rho_color = "#2e7d32" if abs(rho) >= 0.7 else ("#fb8c00" if abs(rho) >= 0.4 else "#c62828")
            parts.append('<div>')
            parts.append(_marginal_bar_chart_svg(rows, param))
            parts.append(
                f'<div style="text-align:center;font-size:11px;color:{rho_color};margin-top:2px">'
                f'Spearman &rho; = {rho:+.2f}</div>'
            )
            parts.append('</div>')
        parts.append('</div>')

    # ── Compact all-combos sortable table ────────────────────────
    parts.append("<h3>All Combos</h3>")
    parts.append(
        "<p style=\"color:#555;font-size:13px;margin:4px 0 8px\">"
        "Click any column header to sort. "
        + (f"Highlighted row = live/target combo." if highlight_key is not None else "")
        + "</p>"
    )

    # Inline JS for sort
    parts.append("""<script>
function sortRobTable(col, th) {
  var tbl = document.getElementById('rob-table');
  var tbody = tbl.querySelector('tbody');
  var rows = Array.from(tbody.querySelectorAll('tr'));
  var asc = th.dataset.asc === '1';
  rows.sort(function(a, b) {
    var av = a.cells[col].dataset.v || a.cells[col].textContent.replace(/[$,%]/g,'').trim();
    var bv = b.cells[col].dataset.v || b.cells[col].textContent.replace(/[$,%]/g,'').trim();
    var an = parseFloat(av), bn = parseFloat(bv);
    if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
    return asc ? av.localeCompare(bv) : bv.localeCompare(av);
  });
  rows.forEach(function(r){ tbody.appendChild(r); });
  th.dataset.asc = asc ? '0' : '1';
}
</script>""")

    # Build header
    param_names = result.param_names
    all_items = sorted(result.all_stats.items(),
                       key=lambda kv: result.scores.get(kv[0], 0.0), reverse=True)

    hdr = '<thead><tr>'
    hdr += f'<th onclick="sortRobTable(0,this)" data-asc="0" style="cursor:pointer">#</th>'
    col = 1
    for p in param_names:
        hdr += f'<th onclick="sortRobTable({col},this)" data-asc="0" style="cursor:pointer">{_param_label(p)}</th>'
        col += 1
    for lbl in ["PnL", "Win%", "Sharpe", "MaxDD%", "PF", "Score"]:
        hdr += f'<th onclick="sortRobTable({col},this)" data-asc="0" style="cursor:pointer">{lbl}</th>'
        col += 1
    hdr += '</tr></thead>'

    parts.append('<div class="hm-wrap"><table id="rob-table" style="font-size:12px">')
    parts.append(hdr)
    parts.append('<tbody>')
    for rank, (key, s) in enumerate(all_items, 1):
        params = dict(key)
        score = result.scores.get(key, 0.0)
        pnl_cls = _pnl_class(s["total_pnl"])
        pf_str = f'{s["profit_factor"]:.2f}' if s["profit_factor"] < 100 else "99+"
        is_highlight = (key == highlight_key)
        row_style = ' style="background:#e3f2fd;font-weight:600"' if is_highlight else ''
        row = f'<tr{row_style}>'
        row += f'<td data-v="{rank}">{rank}</td>'
        for p in param_names:
            row += f'<td data-v="{params[p]}">{_fmt_val(params[p])}</td>'
        row += (
            f'<td class="{pnl_cls}" data-v="{s["total_pnl"]:.0f}">{_fmt_pnl(s["total_pnl"])}</td>'
            f'<td data-v="{s["win_rate"]*100:.0f}">{s["win_rate"]*100:.0f}%</td>'
            f'<td data-v="{s["sharpe"]:.2f}">{s["sharpe"]:.2f}</td>'
            f'<td data-v="{s["max_dd_pct"]:.1f}">{s["max_dd_pct"]:.1f}%</td>'
            f'<td data-v="{s["profit_factor"]:.2f}">{pf_str}</td>'
            f'<td data-v="{score:.3f}">{score:.3f}</td>'
        )
        row += '</tr>'
        parts.append(row)
    parts.append('</tbody></table></div>')

    return "\n".join(parts)


# ── CSS ──────────────────────────────────────────────────────────

CSS = """
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
       max-width: 1500px; margin: 20px auto; padding: 0 20px; background: #fafafa; color: #222; }
h1 { border-bottom: 3px solid #333; padding-bottom: 10px; }
h2 { margin-top: 36px; color: #333; border-bottom: 1px solid #ddd; padding-bottom: 6px; }
h3 { margin-top: 20px; color: #555; }
h4 { margin: 0 0 6px; font-size: 13px; color: #555; font-weight: 600; }
.meta { background: #e8eaf6; padding: 12px 18px; border-radius: 6px; margin: 16px 0;
        display: flex; gap: 28px; flex-wrap: wrap; font-size: 14px; }
.meta b { color: #333; }
.best-box { background: #e8f5e9; border: 2px solid #4caf50; border-radius: 8px;
            padding: 18px 24px; margin: 16px 0; }
.best-box.negative { background: #fff3e0; border-color: #ff9800; }
.best-box h3 { margin: 0 0 10px; color: #2e7d32; }
.best-box.negative h3 { color: #e65100; }
.best-box .params { font-size: 17px; font-weight: 700; color: #00695c; margin: 8px 0; }
.best-box.negative .params { color: #bf360c; }
.metric { display: inline-block; margin: 4px 20px 4px 0; }
.metric-label { color: #666; font-size: 12px; }
.metric-value { font-size: 16px; font-weight: 600; }
.grid-info { background: #f5f5f5; border: 1px solid #ddd; border-radius: 6px;
             padding: 12px 18px; margin: 10px 0; font-size: 13px; }
.grid-info code { background: #e0e0e0; padding: 1px 5px; border-radius: 3px; }
table { border-collapse: collapse; font-size: 13px; margin: 10px 0 24px; }
th, td { padding: 5px 8px; text-align: right; border: 1px solid #ccc; white-space: nowrap; }
th { background: #333; color: #fff; font-weight: 600; position: sticky; top: 0; }
th:first-child, td:first-child { text-align: left; }
.pos { color: #2e7d32; font-weight: 600; }
.neg { color: #c62828; }
.empty { color: #bbb; background: #f8f8f8; }
.hm-wrap { overflow-x: auto; margin: 4px 0 12px; }
.hm-label { text-align: left !important; font-weight: 600; background: #f0f0f0 !important;
             color: #333 !important; min-width: 60px; }
.hm-pair { display: flex; gap: 32px; flex-wrap: wrap; align-items: flex-start;
           margin-bottom: 28px; }
.hm-pair > div { flex: 0 0 auto; }
.eq-bar { display: inline-block; height: 14px; border-radius: 2px; vertical-align: middle; }
.eq-pos { background: #4caf50; }
.eq-neg { background: #e53935; }
"""


# ── Walk-Forward section ─────────────────────────────────────────

def _wfo_section_html(wfo_result, account_size):
    """Render the Walk-Forward Validation section as an HTML string."""
    from backtester.walk_forward import WFOResult  # local import to avoid circularity

    parts = []
    parts.append("<h2>Walk-Forward Validation</h2>")

    n_win = len(wfo_result.windows)
    n_oos_win = sum(1 for w in wfo_result.windows if w.oos_win)
    wr_color = "#2e7d32" if wfo_result.oos_win_rate >= 0.67 else (
        "#fb8c00" if wfo_result.oos_win_rate >= 0.34 else "#c62828")
    total_pnl_cls = "pos" if wfo_result.oos_total_pnl >= 0 else "neg"
    avg_sh_color = "#2e7d32" if wfo_result.oos_avg_sharpe >= 1.0 else (
        "#fb8c00" if wfo_result.oos_avg_sharpe >= 0.3 else "#c62828")

    parts.append(
        f'<p style="color:#555;font-size:13px;margin:4px 0 10px">'
        f'Parameters optimised on the in-sample (IS) period each window; '
        f'best IS combo run frozen on out-of-sample (OOS).  '
        f'IS={wfo_result.is_days}d / OOS={wfo_result.oos_days}d / step={wfo_result.step_days}d.</p>'
    )

    parts.append('<div class="grid-info">')
    parts.append(
        f'<span class="metric"><span class="metric-label">Windows</span><br>'
        f'<span class="metric-value">{n_win}</span></span>'
    )
    parts.append(
        f'<span class="metric"><span class="metric-label">OOS Win Rate</span><br>'
        f'<span class="metric-value" style="color:{wr_color}">'
        f'{n_oos_win}/{n_win} ({wfo_result.oos_win_rate*100:.0f}%)</span></span>'
    )
    parts.append(
        f'<span class="metric"><span class="metric-label">OOS Total PnL</span><br>'
        f'<span class="metric-value {total_pnl_cls}">'
        f'${wfo_result.oos_total_pnl:+,.0f}</span></span>'
    )
    parts.append(
        f'<span class="metric"><span class="metric-label">Avg OOS Sharpe</span><br>'
        f'<span class="metric-value" style="color:{avg_sh_color}">'
        f'{wfo_result.oos_avg_sharpe:.2f}</span></span>'
    )
    parts.append('</div>')

    # OOS stitched equity chart
    if wfo_result.oos_equity:
        chart = _equity_chart_svg(wfo_result.oos_equity, capital=account_size)
        parts.append(
            f'<h3 style="margin-top:18px;margin-bottom:4px">Stitched OOS Equity Curve</h3>'
            f'<p style="color:#555;font-size:13px;margin:0 0 6px">Each segment uses the '
            f'IS-optimised combo, run frozen on the OOS period.</p>'
        )
        parts.append(f'<div style="margin-bottom:14px">{chart}</div>')

    # Per-window table
    parts.append('<h3 style="margin-top:18px">Per-Window Results</h3>')
    # Determine param names from first window
    param_names_wfo = sorted(wfo_result.windows[0].best_params.keys()) if wfo_result.windows else []
    parts.append('<div class="hm-wrap"><table>')
    hdr = (
        '<tr><th>#</th>'
        '<th>IS Period</th><th>OOS Period</th>'
    )
    for p in param_names_wfo:
        hdr += f'<th>{_param_label(p)}</th>'
    hdr += (
        '<th>IS PnL</th><th>IS Trades</th><th>IS Sharpe</th>'
        '<th>OOS PnL</th><th>OOS Trades</th><th>OOS Sharpe</th>'
        '<th>Result</th></tr>'
    )
    parts.append(hdr)

    for w in wfo_result.windows:
        is_cls = _pnl_class(w.is_pnl)
        oos_cls = _pnl_class(w.oos_pnl)
        badge = (
            '<span style="color:#2e7d32;font-weight:600">&#x2713; Win</span>'
            if w.oos_win else
            '<span style="color:#c62828;font-weight:600">&#x2717; Loss</span>'
        )
        row = (
            f'<tr>'
            f'<td>{w.idx}</td>'
            f'<td style="white-space:nowrap">{w.is_start}<br><small>to {w.is_end}</small></td>'
            f'<td style="white-space:nowrap">{w.oos_start}<br><small>to {w.oos_end}</small></td>'
        )
        for p in param_names_wfo:
            row += f'<td>{_fmt_val(w.best_params.get(p))}</td>'
        row += (
            f'<td class="{is_cls}">${w.is_pnl:+,.0f}</td>'
            f'<td>{w.is_n_trades}</td>'
            f'<td>{w.is_sharpe:.2f}</td>'
            f'<td class="{oos_cls}">${w.oos_pnl:+,.0f}</td>'
            f'<td>{w.oos_n_trades}</td>'
            f'<td>{w.oos_sharpe:.2f}</td>'
            f'<td>{badge}</td>'
            f'</tr>'
        )
        parts.append(row)

    parts.append('</table></div>')
    return "\n".join(parts)


# ── HTML Report ──────────────────────────────────────────────────

def generate_html(strategy_name, result, n_intervals, runtime_s,
                  strategy_description="", qty=1, heatmap_pairs=None,
                  robustness=False, wfo_result=None):
    """Generate a self-contained HTML backtest report.

    Args:
        strategy_name:        Strategy.name string
        result:               GridResult from backtester.results
        n_intervals:          number of 5-min market states processed
        runtime_s:            grid execution time in seconds
        strategy_description: Short prose description shown near the top
        qty:                  Contracts per trade (default 1)
        heatmap_pairs:        Optional list of (pa, pb) tuples to pin;
                              falls back to auto-selection by PnL spread
        robustness:           If True, include the robustness analysis section
                              (distribution histogram, marginal charts, all-combos
                              table). Off by default for fast discovery runs.
        wfo_result:           Optional WFOResult from walk_forward.run_walk_forward.
                              When provided, a Walk-Forward Validation section is
                              appended before the trade log.

    Returns:
        Complete self-contained HTML string.
    """
    # ── Unpack from GridResult ────────────────────────────────────
    df           = result.df
    keys         = result.keys
    param_grid   = result.param_grid
    account_size = result.account_size
    date_range   = result.date_range
    all_stats    = result.all_stats
    scores       = result.scores
    ranked       = result.ranked
    total_trades = result.total_trades
    param_names  = result.param_names
    best_key     = result.best_key
    best_stats   = result.best_stats
    df_best      = result.df_best
    best_eq      = result.best_eq
    best_final_nav = result.best_final_nav
    best_params  = result.best_params
    top_n_eq     = result.top_n_eq

    title = strategy_name.replace("_", " ").title()
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    is_negative = best_stats and best_stats["total_pnl"] < 0

    parts = []

    # ── Head ─────────────────────────────────────────────────────
    desc_html = (
        f'\n<div class="grid-info" style="margin-top:12px">'
        f'<b>Strategy:</b> {strategy_description}</div>'
        if strategy_description else ""
    )
    parts.append(f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Backtest: {title}</title>
<style>{CSS}</style>
</head><body>
<h1>Backtest Report &mdash; {title}</h1>{desc_html}
<div class="meta">
  <span><b>Generated:</b> {now}</span>
  <span><b>Data:</b> {date_range[0]} to {date_range[1]}</span>
  <span><b>Intervals:</b> {n_intervals:,}</span>
  <span><b>Combos:</b> {len(all_stats):,}</span>
  <span><b>Trades:</b> {total_trades:,}</span>
  <span><b>Runtime:</b> {runtime_s:.1f}s</span>
  <span><b>Account:</b> ${account_size:,} / {qty} contract{"s" if qty != 1 else ""}</span>
</div>""")

    # ── Risk summary bar ─────────────────────────────────────────
    if best_eq:
        _eq = best_eq
        _pf = f'{_eq["profit_factor"]:.2f}' if _eq["profit_factor"] < 100 else "99+"
        _bs = best_stats or {}
        parts.append(f"""<div class="grid-info">
  <b>Best Combo &mdash; Risk Summary:</b> &nbsp;
    Max DD: {_fmt_pnl(_eq["max_drawdown"])} ({_eq["max_dd_pct"]:.1f}%) &nbsp;|&nbsp;
  Sharpe: {_eq["sharpe"]:.2f} &nbsp;|&nbsp;
  Sortino: {_eq["sortino"]:.2f} &nbsp;|&nbsp;
  Calmar: {_eq["calmar"]:.2f} &nbsp;|&nbsp;
  Profit Factor: {_pf} &nbsp;|&nbsp;
  Consec Wins: {_eq["consec_wins"]} &nbsp;|&nbsp;
  Consec Losses: {_eq["consec_losses"]} &nbsp;|&nbsp;
  R&sup2;: {_bs.get("r_squared", 0):.2f} &nbsp;|&nbsp;
  Omega: {_bs.get("omega", 0):.2f} &nbsp;|&nbsp;
  Ulcer: {_bs.get("ulcer", 0):.1f} &nbsp;|&nbsp;
  Consistency: {_bs.get("consistency", 0)*100:.0f}%
</div>""")

    # ── Best combo box ───────────────────────────────────────────
    if best_stats:
        neg_cls = " negative" if is_negative else ""
        param_str = " &nbsp;|&nbsp; ".join(
            f"{_param_label(p)}={_fmt_val(best_params[p])}" for p in param_names)
        parts.append(f"""
<h2>Best Combo</h2>
<div class="best-box{neg_cls}">
  <h3>{"Best Result (all combos negative)" if is_negative else "Top Performing Configuration"}</h3>
  <div class="params">{param_str}</div>""")

        pnl_cls = _pnl_class(best_stats["total_pnl"])
        metrics_html = [
            ("Total PnL", f'<span class="{pnl_cls}">{_fmt_pnl(best_stats["total_pnl"])}</span>'),
            ("Trades", str(best_stats["n"])),
            ("Avg PnL", _fmt_pnl(best_stats["avg_pnl"])),
            ("Win Rate", f'{best_stats["win_rate"]*100:.0f}%'),
        ]
        if best_final_nav is not None:
            metrics_html.append(("Final NAV", _fmt_pnl(best_final_nav)))
        if best_eq:
            metrics_html.extend([
                ("Max DD", f'{_fmt_pnl(best_eq["max_drawdown"])} ({best_eq["max_dd_pct"]:.1f}%)'),
                ("Sharpe", f'{best_eq["sharpe"]:.2f}'),
                ("Sortino", f'{best_eq["sortino"]:.2f}'),
                ("Calmar", f'{best_eq["calmar"]:.2f}'),
                ("Profit Factor", f'{best_eq["profit_factor"]:.2f}'),
                ("Consec Wins", str(best_eq["consec_wins"])),
                ("Consec Losses", str(best_eq["consec_losses"])),
                ("R\u00b2", f'{best_stats.get("r_squared", 0):.2f}'),
                ("Omega", f'{best_stats.get("omega", 0):.2f}'),
                ("Ulcer Index", f'{best_stats.get("ulcer", 0):.1f}'),
                ("Consistency", f'{best_stats.get("consistency", 0)*100:.0f}%'),
            ])
        if robustness and result.dsr is not None:
            dsr = result.dsr
            dsr_color = "#2e7d32" if dsr >= 0.95 else ("#fb8c00" if dsr >= 0.70 else "#c62828")
            n_trials = len(result.keys)
            dsr_tip = (
                f"title=\"Deflated Sharpe Ratio: probability that the best-combo Sharpe "
                f"is genuinely positive after correcting for {n_trials:,} combos tested "
                f"and non-normality of returns (Bailey &amp; L\\u00f3pez de Prado 2014). "
                f"\\u2265 0.95 = strong; &lt; 0.50 = likely noise.\""
            )
            metrics_html.append((
                f'DSR <span {dsr_tip} style="cursor:help">\u2139</span>',
                f'<span style="color:{dsr_color}">{dsr:.2f}</span>',
            ))

        for label, val in metrics_html:
            parts.append(
                f'  <span class="metric">'
                f'<span class="metric-label">{label}</span><br>'
                f'<span class="metric-value">{val}</span></span>'
            )

        if best_eq and best_eq["daily"]:
            chart_svg = _equity_chart_svg(best_eq["daily"], capital=account_size)
            parts.append(
                f'<div style="margin-top:14px">{chart_svg}</div>')

        parts.append("</div>")

    # ── Parameter grid info ──────────────────────────────────────
    parts.append('<h2>Parameter Grid</h2><div class="grid-info">')
    for p in param_names:
        vals = param_grid[p]
        parts.append(
            f'<b>{_param_label(p)}:</b> <code>{vals}</code> ({len(vals)} values)<br>')
    n_combos = 1
    for v in param_grid.values():
        n_combos *= len(v)
    parts.append(f'<b>Total combos:</b> {n_combos:,}</div>')

    # ── Top 20 combos table ──────────────────────────────────────
    top_n = min(20, len(ranked))
    parts.append(f'<h2>Top {top_n} Combos</h2>')
    _sc = cfg.scoring
    # Recency description line
    if _sc.recency_pct > 0.0:
        _r_window = f'{_sc.recency_pct*100:.0f}% of range'
        _r_blend  = f'{_sc.recency_weight*100:.0f}%'
        _r_gate   = (
            f'gate Sharpe&nbsp;&ge;&nbsp;{_sc.recency_gate_sharpe:.2f}'
            if _sc.recency_gate_enabled else 'gate off'
        )
        _recency_str = (
            f' &nbsp;|&nbsp; '
            f'<b>Recency</b>: window&nbsp;=&nbsp;{_r_window} &middot; '
            f'blend&nbsp;=&nbsp;{_r_blend} &middot; '
            f'{_r_gate} &middot; '
            f'min&nbsp;active&nbsp;days&nbsp;=&nbsp;{_sc.recency_min_trades}'
        )
    else:
        _recency_str = ' &nbsp;|&nbsp; <b>Recency</b>: off'
    parts.append(
        f'<p style="color:#555;font-size:13px;margin:4px 0 8px">'
        f'Ranked by composite score &mdash; '
        f'<b>R&sup2;</b> {_sc.w_r_squared*100:.0f}% &middot; '
        f'<b>Sharpe</b> {_sc.w_sharpe*100:.0f}% &middot; '
        f'<b>PnL</b> {_sc.w_pnl*100:.0f}% &middot; '
        f'<b>Max&nbsp;DD</b> {_sc.w_max_dd*100:.0f}% (&#x2193;&nbsp;better) &middot; '
        f'<b>Omega</b> {_sc.w_omega*100:.0f}% &middot; '
        f'<b>Ulcer</b> {_sc.w_ulcer*100:.0f}% (&#x2193;&nbsp;better) &middot; '
        f'<b>Consistency</b> {_sc.w_consistency*100:.0f}% &middot; '
        f'<b>Profit&nbsp;Factor</b> {_sc.w_profit_factor*100:.0f}% &nbsp;|&nbsp; '
        f'min trades: {_sc.min_trades}'
        f'{_recency_str}'
        f'</p>'
    )
    parts.append('<div class="hm-wrap"><table>')
    hdr = "<tr><th>#</th>"
    for p in param_names:
        hdr += f"<th>{_param_label(p)}</th>"
    hdr += ("<th>Trades</th><th>Total PnL</th><th>Avg PnL</th><th>Med PnL</th>"
            "<th>Win%</th><th>Max Win</th><th>Max Loss</th>"
            "<th>Max DD</th><th>Sharpe</th><th>Sortino</th><th>Calmar</th><th>PF</th>"
            "<th>R&sup2;</th><th>Omega</th><th>Ulcer</th><th>Consist</th>"
            "<th>Score</th></tr>")
    parts.append(hdr)
    for rank, (key, s) in enumerate(ranked[:top_n], 1):
        params = dict(key)
        pnl_cls = _pnl_class(s["total_pnl"])
        avg_cls = _pnl_class(s["avg_pnl"])
        pf_str = f'{s["profit_factor"]:.2f}' if s["profit_factor"] < 100 else "99+"
        row = f'<tr><td>{rank}</td>'
        for p in param_names:
            row += f'<td>{_fmt_val(params[p])}</td>'
        _eq_detail = top_n_eq.get(key)
        _sortino_str = f'{_eq_detail["sortino"]:.2f}' if _eq_detail else "&mdash;"
        _calmar_str  = f'{_eq_detail["calmar"]:.2f}'  if _eq_detail else "&mdash;"
        row += (
            f'<td>{s["n"]}</td>'
            f'<td class="{pnl_cls}">{_fmt_pnl(s["total_pnl"])}</td>'
            f'<td class="{avg_cls}">{_fmt_pnl(s["avg_pnl"])}</td>'
            f'<td>{_fmt_pnl(s["median_pnl"])}</td>'
            f'<td>{s["win_rate"]*100:.0f}%</td>'
            f'<td class="pos">{_fmt_pnl(s["max_win"])}</td>'
            f'<td class="neg">{_fmt_pnl(s["max_loss"])}</td>'
            f'<td class="neg">{s["max_dd_pct"]:.1f}%</td>'
            f'<td>{s["sharpe"]:.2f}</td>'
            f'<td>{_sortino_str}</td>'
            f'<td>{_calmar_str}</td>'
            f'<td>{pf_str}</td>'
            f'<td>{s["r_squared"]:.2f}</td>'
            f'<td>{s["omega"]:.2f}</td>'
            f'<td>{s["ulcer"]:.1f}</td>'
            f'<td>{s["consistency"]*100:.0f}%</td>'
            f'<td>{scores[key]:.3f}</td></tr>'
        )
        parts.append(row)
    parts.append("</table></div>")

    # ── Top 5 absolute PnL combos ────────────────────────────────
    top_abs = sorted(all_stats.items(), key=lambda kv: kv[1]["total_pnl"], reverse=True)[:5]
    parts.append('<h2>Top 5 Absolute PnL Combos</h2>')
    parts.append(
        '<p style="color:#555;font-size:13px;margin:4px 0 8px">'
        'Ranked by total PnL only &mdash; no scoring applied.</p>'
    )
    parts.append('<div class="hm-wrap"><table>')
    hdr2 = "<tr><th>#</th>"
    for p in param_names:
        hdr2 += f"<th>{_param_label(p)}</th>"
    hdr2 += ("<th>Trades</th><th>Total PnL</th><th>Avg PnL</th><th>Med PnL</th>"
             "<th>Win%</th><th>Max Win</th><th>Max Loss</th>"
             "<th>Max DD</th><th>Sharpe</th><th>PF</th><th>Score</th></tr>")
    parts.append(hdr2)
    for rank, (key, s) in enumerate(top_abs, 1):
        params = dict(key)
        pnl_cls = _pnl_class(s["total_pnl"])
        avg_cls = _pnl_class(s["avg_pnl"])
        pf_str = f'{s["profit_factor"]:.2f}' if s["profit_factor"] < 100 else "99+"
        score_str = f'{scores[key]:.3f}' if key in scores else "&mdash;"
        row = f'<tr><td>{rank}</td>'
        for p in param_names:
            row += f'<td>{_fmt_val(params[p])}</td>'
        row += (
            f'<td>{s["n"]}</td>'
            f'<td class="{pnl_cls}">{_fmt_pnl(s["total_pnl"])}</td>'
            f'<td class="{avg_cls}">{_fmt_pnl(s["avg_pnl"])}</td>'
            f'<td>{_fmt_pnl(s["median_pnl"])}</td>'
            f'<td>{s["win_rate"]*100:.0f}%</td>'
            f'<td class="pos">{_fmt_pnl(s["max_win"])}</td>'
            f'<td class="neg">{_fmt_pnl(s["max_loss"])}</td>'
            f'<td class="neg">{s["max_dd_pct"]:.1f}%</td>'
            f'<td>{s["sharpe"]:.2f}</td>'
            f'<td>{pf_str}</td>'
            f'<td>{score_str}</td></tr>'
        )
        parts.append(row)
    parts.append("</table></div>")

    # ── Robustness section (opt-in via --robustness flag) ─────────
    if robustness:
        parts.append(_robustness_section_html(result, highlight_key=best_key))

    # ── Performance fan chart ─────────────────────────────────────
    if result.fan_curves:
        fan_top = len(result.fan_curves)
        parts.append(
            f'<h2>Top {fan_top} Equity Curves</h2>'
            f'<p style="color:#555;font-size:13px;margin:4px 0 8px">'
            f'Hover any curve for its parameters and PnL. '
            f'Shaded band = full min&ndash;max range across all {fan_top} combos.</p>'
        )
        parts.append(_fan_chart_svg(result.fan_curves, capital=account_size))

    # ── Parameter sensitivity heatmaps ───────────────────────────
    # Design:
    #  - Cells pool ALL trades from combos sharing (pa_val, pb_val), so no
    #    combo with few trades can distort the cell value.
    #  - Left table: total PnL. Right table: win rate %.
    #  - Auto-selects top-3 most informative pairs by PnL spread.
    #  - Strategy can override with HEATMAP_PAIRS.
    if len(param_names) >= 2:
        parts.append("<h2>Parameter Sensitivity</h2>")
        parts.append(
            "<p>Each cell pools <em>all</em> trades sharing those two parameter "
            "values (marginalised over all other parameters). "
            "<b>Left:</b> Total PnL &nbsp; <b>Right:</b> Win rate. "
            "Pairs ranked by PnL spread — most informative first.</p>"
        )

        selected_pairs = _select_pairs(result, heatmap_pairs_override=heatmap_pairs)

        for pa, pb in selected_pairs:
            grid_pnl, grid_wr, grid_n, a_vals, b_vals = _build_heatmap_data(
                df, keys, pa, pb)
            if not grid_pnl:
                continue

            pnl_vals = list(grid_pnl.values())
            wr_vals = list(grid_wr.values())
            spread = max(pnl_vals) - min(pnl_vals)
            pnl_min, pnl_max = min(pnl_vals), max(pnl_vals)
            wr_min, wr_max = min(wr_vals), max(wr_vals)

            parts.append(
                f'<h3>{_param_label(pa)} &times; {_param_label(pb)} '
                f'<span style="font-size:12px;color:#888;font-weight:normal">'
                f'spread {_fmt_pnl(spread)}</span></h3>'
            )
            parts.append('<div class="hm-pair">')

            # PnL table
            parts.append('<div>')
            parts.append('<h4>Total PnL (pooled trades)</h4>')
            parts.append('<div class="hm-wrap"><table>')
            parts.append(
                f'<tr><th class="hm-label">{_param_label(pa)} \\ {_param_label(pb)}</th>')
            for b in b_vals:
                parts.append(f'<th>{_fmt_val(b)}</th>')
            parts.append('</tr>')
            for a in a_vals:
                parts.append(f'<tr><td class="hm-label">{_fmt_val(a)}</td>')
                for b in b_vals:
                    v = grid_pnl.get((a, b))
                    if v is not None:
                        bg = _heatmap_color(v, pnl_min, pnl_max)
                        cls = _pnl_class(v)
                        n = grid_n.get((a, b), 0)
                        parts.append(
                            f'<td style="background:{bg}" title="{n} trades">'
                            f'<span class="{cls}">{_fmt_pnl(v)}</span></td>')
                    else:
                        parts.append('<td class="empty">&mdash;</td>')
                parts.append('</tr>')
            parts.append('</table></div></div>')

            # Win rate table
            parts.append('<div>')
            parts.append('<h4>Win Rate %</h4>')
            parts.append('<div class="hm-wrap"><table>')
            parts.append(
                f'<tr><th class="hm-label">{_param_label(pa)} \\ {_param_label(pb)}</th>')
            for b in b_vals:
                parts.append(f'<th>{_fmt_val(b)}</th>')
            parts.append('</tr>')
            for a in a_vals:
                parts.append(f'<tr><td class="hm-label">{_fmt_val(a)}</td>')
                for b in b_vals:
                    wr = grid_wr.get((a, b))
                    if wr is not None:
                        bg = _heatmap_color(wr, wr_min, wr_max)
                        parts.append(f'<td style="background:{bg}">{wr:.0f}%</td>')
                    else:
                        parts.append('<td class="empty">&mdash;</td>')
                parts.append('</tr>')
            parts.append('</table></div></div>')

            parts.append('</div>')  # .hm-pair

    # ── Daily equity — best combo ────────────────────────────────
    if best_eq and best_eq["daily"]:
        parts.append("<h2>Daily Equity &mdash; Best Combo</h2>")
        parts.append('<div class="hm-wrap"><table>')
        parts.append(
            '<tr><th style="text-align:left">Date</th>'
            '<th>Day PnL</th><th>Cumulative</th><th>Equity</th>'
            '<th style="min-width:120px">Visual</th></tr>')
        max_abs = max(abs(row[1]) for row in best_eq["daily"]) or 1
        for ds, pnl, cum, high, low, eq in best_eq["daily"]:
            pnl_cls = _pnl_class(pnl)
            cum_cls = _pnl_class(cum)
            bar_w = min(abs(pnl) / max_abs * 100, 100)
            bar_cls = "eq-pos" if pnl >= 0 else "eq-neg"
            sign = "+" if pnl > 0 else ""
            parts.append(
                f'<tr><td style="text-align:left">{ds}</td>'
                f'<td class="{pnl_cls}">{sign}{_fmt_pnl(pnl)}</td>'
                f'<td class="{cum_cls}">{_fmt_pnl(cum)}</td>'
                f'<td>{_fmt_pnl(eq)}</td>'
                f'<td><span class="eq-bar {bar_cls}" '
                f'style="width:{bar_w:.0f}%"></span></td></tr>'
            )
        parts.append("</table></div>")

        eq = best_eq
        _pf2 = f'{eq["profit_factor"]:.2f}' if eq["profit_factor"] < 100 else "99+"
        parts.append(f"""
<div class="grid-info">
  <b>Max Drawdown:</b> {_fmt_pnl(eq["max_drawdown"])} ({eq["max_dd_pct"]:.1f}%) &nbsp;|&nbsp;
  <b>Sharpe:</b> {eq["sharpe"]:.2f} &nbsp;|&nbsp;
  <b>Sortino:</b> {eq["sortino"]:.2f} &nbsp;|&nbsp;
  <b>Calmar:</b> {eq["calmar"]:.2f} &nbsp;|&nbsp;
  <b>Profit Factor:</b> {_pf2} &nbsp;|&nbsp;
  <b>Consec Wins:</b> {eq["consec_wins"]} &nbsp;|&nbsp;
  <b>Consec Losses:</b> {eq["consec_losses"]}
</div>""")

    # ── Walk-Forward Validation section (opt-in via --wfo) ────────
    if wfo_result is not None:
        parts.append(_wfo_section_html(wfo_result, account_size=account_size))

    # ── Trade log — best combo ───────────────────────────────────
    if df_best is not None and not df_best.empty:
        parts.append("<h2>Trade Log &mdash; Best Combo</h2>")
        parts.append(f'<p>{len(df_best)} trades total</p>')
        parts.append('<div class="hm-wrap"><table>')
        parts.append(
            '<tr><th style="text-align:left">Date</th>'
            '<th>Entry Time</th><th>Exit Time</th>'
            '<th>Entry Spot</th><th>Exit Spot</th>'
            '<th>Entry USD</th><th>Exit USD</th>'
            '<th>Fees</th><th>PnL</th><th>Reason</th></tr>')
        for t in df_best.itertuples(index=False):
            pnl_cls = _pnl_class(t.pnl)
            parts.append(
                f'<tr><td style="text-align:left">{t.entry_date}</td>'
                f'<td>{t.entry_time.strftime("%H:%M")}</td>'
                f'<td>{t.exit_time.strftime("%H:%M")}</td>'
                f'<td>${t.entry_spot:,.0f}</td>'
                f'<td>${t.exit_spot:,.0f}</td>'
                f'<td>${t.entry_price_usd:,.2f}</td>'
                f'<td>${t.exit_price_usd:,.2f}</td>'
                f'<td>${t.fees:,.2f}</td>'
                f'<td class="{pnl_cls}">${t.pnl:,.2f}</td>'
                f'<td>{t.exit_reason}</td></tr>'
            )
        parts.append("</table></div>")

    # ── Footer ───────────────────────────────────────────────────
    parts.append(f"""
<div style="margin-top:40px; padding-top:12px; border-top:1px solid #ddd;
            color:#999; font-size:12px;">
  Backtester V2 &mdash; Real Deribit prices via Tardis &mdash;
  Generated {now} &mdash; {runtime_s:.1f}s grid + report
</div>
</body></html>""")

    return "\n".join(parts)

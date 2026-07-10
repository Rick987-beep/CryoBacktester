#!/usr/bin/env python3
"""Extract run 146 / combo c33317c67db1 and build marketing assets."""
from __future__ import annotations

import json
import math
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from backtester.results import equity_metrics

# ── Config ────────────────────────────────────────────────────────────────────

RUN_ID = 146
BUNDLE = Path("backtester/reports/tudysho_20260702_095428.bundle")
BACKTEST_HTML = Path("backtester/reports/tudysho_20260702_095428.html")
COMBO_IDX = 57
COMBO_HASH = "c33317c67db1"
OUT = Path("analysis/run146_marketing")

# Marketing palette
C_BG = "#0b1220"
C_PANEL = "#111b2e"
C_GRID = "#1e2d45"
C_EQUITY = "#3b9eff"
C_EQUITY_GLOW = "rgba(59,158,255,0.18)"
C_DD = "#ff5c6c"
C_DD_FILL = "rgba(255,92,108,0.35)"
C_TEXT = "#e8eef7"
C_MUTED = "#7a8fa8"


def _load_combo_data() -> dict:
    meta = json.loads((BUNDLE / "meta.json").read_text())
    trades = pd.read_parquet(BUNDLE / "trade_log.parquet")
    nav = pd.read_parquet(BUNDLE / "nav_daily.parquet")
    fills_path = BUNDLE / "fills.parquet"

    df = trades[trades.combo_idx == COMBO_IDX].copy()
    nav_c = nav[nav.combo_idx == COMBO_IDX].copy()
    capital = float(meta["account_size"])
    date_from, date_to = meta["date_range"]
    params = dict(meta["keys"][COMBO_IDX])

    eq = equity_metrics(
        df, capital=capital, nav_daily_combo=nav_c,
        date_from=date_from, date_to=date_to,
    )
    assert eq is not None

    total_return_pct = (eq["total_pnl"] / capital) * 100
    win_rate = float((df["pnl"] > 0).mean() * 100)
    n_trades = len(df)

    df["month"] = pd.to_datetime(df["entry_date"]).dt.to_period("M")
    monthly = df.groupby("month")["pnl"].sum()
    positive_months_pct = float((monthly > 0).mean() * 100) if len(monthly) else 0.0

    fills = None
    if fills_path.exists():
        all_fills = pd.read_parquet(fills_path)
        if "combo_idx" in all_fills.columns:
            fills = all_fills[all_fills.combo_idx == COMBO_IDX]

    return {
        "meta": meta,
        "eq": eq,
        "trades": df,
        "nav": nav_c,
        "fills": fills,
        "capital": capital,
        "params": params,
        "stats": {
            "run_id": RUN_ID,
            "combo_idx": COMBO_IDX,
            "combo_hash": COMBO_HASH,
            "strategy": meta["strategy"],
            "date_from": date_from,
            "date_to": date_to,
            "account_size_usd": capital,
            "n_trades": n_trades,
            "total_pnl_usd": round(float(eq["total_pnl"]), 2),
            "total_return_pct": round(total_return_pct, 1),
            "max_dd_pct": round(eq["max_dd_pct"], 1),
            "sharpe": round(eq["sharpe"], 2),
            "sortino": round(eq["sortino"], 2),
            "calmar": round(eq["calmar"], 2),
            "omega": round(eq.get("omega", 0.0), 2),
            "profit_factor": round(eq["profit_factor"], 2),
            "win_rate_pct": round(win_rate, 1),
            "positive_months_pct": round(positive_months_pct, 0),
            "consec_wins_max": eq["consec_wins"],
            "consec_losses_max": eq["consec_losses"],
            "avg_pnl_usd": round(float(df["pnl"].mean()), 2),
            "median_pnl_usd": round(float(df["pnl"].median()), 2),
        },
    }


def _equity_daily_csv(eq: dict, capital: float) -> pd.DataFrame:
    rows = []
    peak = capital
    prev_close = capital
    for row in eq["daily"]:
        date, _pnl, _cum, hi, lo, close = row[0], row[1], row[2], row[3], row[4], row[5]
        hi_f, lo_f, close_f = float(hi), float(lo), float(close)
        peak = max(peak, hi_f)
        ret_pct = 100.0 * (close_f / capital - 1.0)
        dd_pct = -100.0 * (peak - lo_f) / peak if peak > 0 else 0.0
        rows.append({
            "date": date,
            "nav_open": prev_close,
            "nav_high": hi_f,
            "nav_low": lo_f,
            "nav_close": close_f,
            "return_pct": round(ret_pct, 4),
            "drawdown_pct": round(dd_pct, 4),
        })
        prev_close = close_f
    return pd.DataFrame(rows)


def _return_pct_series(daily: list, capital: float) -> tuple[list[str], list[float], list[float], list[float]]:
    dates, ret_pct, hi_pct, lo_pct = [], [], [], []
    for row in daily:
        ds = row[0]
        hi, lo, close = float(row[3]), float(row[4]), float(row[5])
        dates.append(ds)
        ret_pct.append(100.0 * (close / capital - 1.0))
        hi_pct.append(100.0 * (hi / capital - 1.0))
        lo_pct.append(100.0 * (lo / capital - 1.0))
    return dates, ret_pct, hi_pct, lo_pct


def _drawdown_pct(daily: list, capital: float) -> list[float]:
    peak = capital
    dd = []
    for row in daily:
        peak = max(peak, float(row[3]))
        lo = float(row[4])
        dd.append(-100.0 * (peak - lo) / peak if peak > 0 else 0.0)
    return dd


def _nice_step(span: float, n_ticks: int = 5) -> float:
    raw = span / n_ticks
    mag = 10 ** math.floor(math.log10(max(raw, 1e-9)))
    for f in (1, 2, 2.5, 5, 10):
        if raw <= f * mag:
            return f * mag
    return 10 * mag


FONT_SANS = "Helvetica, Arial, sans-serif"
FONT_MONO = "Consolas, Monaco, Courier New, monospace"
# DIN Next is proprietary; Barlow (Google Fonts) is the free DIN-style fallback.
FONT_DIN = (
    '"DIN Next LT Pro", "DIN Next", "DIN Alternate", Barlow, '
    '"Helvetica Neue", Helvetica, Arial, sans-serif'
)


def _svg_font_defs(font_family: str) -> str:
    """Embed Barlow when the stack may need a web fallback (headless PNG export)."""
    if "Barlow" not in font_family:
        return ""
    return (
        "<style>"
        "@import url('https://fonts.googleapis.com/css2?family=Barlow:wght@400;500;600;700&amp;display=swap');"
        "</style>"
    )

_CHROME_PATHS = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
)


def _svg_to_png(svg_path: Path, png_path: Path, width: int, height: int) -> None:
    import subprocess

    chrome = next((p for p in _CHROME_PATHS if Path(p).exists()), None)
    if chrome is None:
        print(f"  PNG skipped ({png_path.name}): Chrome not found")
        return
    url = svg_path.resolve().as_uri()
    subprocess.run(
        [
            chrome, "--headless", "--disable-gpu", "--hide-scrollbars",
            f"--screenshot={png_path.resolve()}",
            f"--window-size={width},{height}",
            url,
        ],
        check=True,
        capture_output=True,
    )


def _fmt_date(ds: str, compact: bool = False) -> str:
    dt = datetime.strptime(ds, "%Y-%m-%d")
    if compact:
        return f"{dt.month}/{dt.strftime('%y')}"
    return dt.strftime("%b %Y")


def equity_chart_svg(
    eq: dict,
    capital: float,
    width: int,
    height: int,
    *,
    title: str | None = None,
    font_scale: float = 1.0,
    show_dd_markers: bool = True,
    date_compact: bool = False,
    font_family: str | None = None,
    eq_section_label: str | None = None,
    dd_section_label: str | None = None,
) -> str:
    daily = eq["daily"]
    if len(daily) < 2:
        return ""

    dates, ret_pct, hi_pct, lo_pct = _return_pct_series(daily, capital)
    dd = _drawdown_pct(daily, capital)
    final_ret = ret_pct[-1]
    ff = font_family or FONT_SANS
    fs = max(10, width // 90) * font_scale
    fs_title = max(13, width // 55) * font_scale
    fs_dd = fs
    label_x = int(fs * 3.2)
    ml = label_x + int(fs * 0.45)
    mr = int(max(width * 0.05, fs * 2.5))
    mt = int(max(height * (0.10 if title else 0.07), fs * 1.2))
    mb = int(max(height * 0.13, fs * 1.85))
    label_pad = int(fs * 0.45)
    base_gap = int(height * 0.03)
    label_row = int(fs * 1.25) if dd_section_label else 0
    gap = max(base_gap, label_row)
    eq_h = int((height - mt - mb - gap) * 0.68)
    dd_h = height - mt - mb - gap - eq_h
    pw = width - ml - mr
    x0, y0_eq = ml, mt
    y0_dd = y0_eq + eq_h + gap

    y_min = min(min(lo_pct), 0.0)
    y_max = max(max(hi_pct), 0.0)
    pad = (y_max - y_min) * 0.08 or 2.0
    y_lo, y_hi = y_min - pad, y_max + pad

    dd_min = min(dd)
    dd_pad = abs(dd_min) * 0.15 or 1.0
    dd_lo, dd_hi = dd_min - dd_pad, 0.0

    n = len(dates)

    def sx(i: int) -> float:
        return x0 + i / max(n - 1, 1) * pw

    def sy_eq(v: float) -> float:
        return y0_eq + (1.0 - (v - y_lo) / (y_hi - y_lo)) * eq_h

    def sy_dd(v: float) -> float:
        return y0_dd + (1.0 - (v - dd_lo) / (dd_hi - dd_lo)) * dd_h

    parts: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'width="{width}" height="{height}">',
        "<defs>",
        f'<linearGradient id="eqGrad" x1="0" y1="0" x2="0" y2="1">'
        f'<stop offset="0%" stop-color="{C_EQUITY}" stop-opacity="0.35"/>'
        f'<stop offset="100%" stop-color="{C_EQUITY}" stop-opacity="0.02"/>'
        f"</linearGradient>",
        f'<filter id="glow" x="-20%" y="-20%" width="140%" height="140%">'
        f'<feGaussianBlur stdDeviation="2" result="b"/>'
        f'<feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>'
        f"</filter>",
        _svg_font_defs(ff),
        "</defs>",
        f'<rect width="{width}" height="{height}" fill="{C_BG}" rx="12"/>',
        f'<rect x="{ml-8}" y="{mt-6}" width="{pw+16}" height="{eq_h+dd_h+gap+12}" '
        f'fill="{C_PANEL}" rx="10" opacity="0.55"/>',
    ]

    if title:
        parts.append(
            f'<text x="{ml}" y="{int(mt*0.55)}" fill="{C_TEXT}" '
            f"font-family='{ff}' font-size=\"{fs_title:.0f}\" "
            f'font-weight="600">{title}</text>'
        )

    if eq_section_label:
        parts.append(
            f'<text x="{x0}" y="{y0_eq + fs*0.55:.0f}" fill="{C_EQUITY}" '
            f"font-family='{ff}' font-size=\"{fs:.0f}\" font-weight=\"600\">"
            f"{eq_section_label}</text>"
        )

    step = _nice_step(y_hi - y_lo)
    t = math.ceil(y_lo / step) * step
    while t <= y_hi:
        y = sy_eq(t)
        parts.append(
            f'<line x1="{x0}" y1="{y:.1f}" x2="{x0+pw}" y2="{y:.1f}" stroke="{C_GRID}" '
            f'stroke-width="{max(1, font_scale)}"/>'
        )
        label = f"+{t:.0f}%" if t > 0 else (f"{t:.0f}%" if t < 0 else "0%")
        parts.append(
            f'<text x="{label_x}" y="{y+fs*0.1:.1f}" text-anchor="end" fill="{C_MUTED}" '
            f"font-family='{ff}' font-size=\"{fs:.0f}\">{label}</text>"
        )
        t += step

    if y_lo <= 0 <= y_hi:
        y0 = sy_eq(0.0)
        parts.append(
            f'<line x1="{x0}" y1="{y0:.1f}" x2="{x0+pw}" y2="{y0:.1f}" '
            f'stroke="{C_MUTED}" stroke-width="1" stroke-dasharray="5,5" opacity="0.6"/>'
        )

    band_hi = " ".join(f"{sx(i):.1f},{sy_eq(hi_pct[i]):.1f}" for i in range(n))
    band_lo = " ".join(f"{sx(i):.1f},{sy_eq(lo_pct[i]):.1f}" for i in range(n))
    parts.append(f'<polygon points="{band_hi} {" ".join(reversed(band_lo.split()))}" fill="{C_EQUITY_GLOW}"/>')

    eq_pts = " ".join(f"{sx(i):.1f},{sy_eq(ret_pct[i]):.1f}" for i in range(n))
    base_y = sy_eq(0.0)
    fill_poly = f"{sx(0):.1f},{base_y:.1f} {eq_pts} {sx(n-1):.1f},{base_y:.1f}"
    parts.append(f'<polygon points="{fill_poly}" fill="url(#eqGrad)"/>')
    parts.append(
        f'<polyline points="{eq_pts}" fill="none" stroke="{C_EQUITY}" stroke-width="{max(2,width//500)}" '
        f'filter="url(#glow)" stroke-linejoin="round"/>'
    )

    ret_label = f"+{final_ret:.1f}%" if final_ret >= 0 else f"{final_ret:.1f}%"
    dot_r = max(4, 4 * font_scale)
    parts.append(
        f'<circle cx="{sx(n-1):.1f}" cy="{sy_eq(final_ret):.1f}" r="{dot_r}" fill="{C_EQUITY}"/>'
    )
    parts.append(
        f'<text x="{min(sx(n-1)-label_pad, x0+pw-fs*0.2):.0f}" y="{max(sy_eq(final_ret)-fs*0.25, mt+fs*0.3):.0f}" '
        f'text-anchor="end" fill="{C_EQUITY}" font-family=\'{ff}\' font-size="{fs:.0f}" '
        f'font-weight="600">{ret_label}</text>'
    )

    max_dd_i = min(range(len(dd)), key=lambda i: dd[i])
    if show_dd_markers:
        parts.append(
            f'<circle cx="{sx(max_dd_i):.1f}" cy="{sy_eq(lo_pct[max_dd_i]):.1f}" r="{dot_r}" fill="{C_DD}"/>'
        )
        parts.append(
            f'<text x="{sx(max_dd_i)+label_pad:.0f}" y="{sy_eq(lo_pct[max_dd_i])-fs*0.25:.0f}" fill="{C_DD}" '
            f"font-family='{ff}' font-size=\"{fs:.0f}\" font-weight=\"600\">"
            f'Max DD {dd[max_dd_i]:.1f}%</text>'
        )

    if dd_section_label:
        dd_label_y = y0_eq + eq_h + int(fs * 0.72) + 4
        parts.append(
            f"<text x=\"{x0}\" y=\"{dd_label_y}\" fill=\"{C_DD}\" "
            f"font-family='{ff}' font-size=\"{fs:.0f}\" font-weight=\"600\">"
            f"{dd_section_label}</text>"
        )
    elif not eq_section_label:
        parts.append(
            f'<text x="{x0}" y="{y0_dd-fs*0.15:.0f}" fill="{C_MUTED}" '
            f"font-family='{ff}' font-size=\"{fs:.0f}\" font-weight=\"500\">Drawdown</text>"
        )

    dd_step = _nice_step(dd_hi - dd_lo)
    t = 0.0
    while t >= dd_lo - dd_step * 0.01:
        y = sy_dd(t)
        parts.append(
            f'<line x1="{x0}" y1="{y:.1f}" x2="{x0+pw}" y2="{y:.1f}" stroke="{C_GRID}" '
            f'stroke-width="{max(1, font_scale)}"/>'
        )
        dd_label = "0%" if abs(t) < 0.05 else f"{t:.0f}%"
        parts.append(
            f'<text x="{label_x}" y="{y+fs*0.1:.1f}" text-anchor="end" fill="{C_MUTED}" '
            f"font-family='{ff}' font-size=\"{fs_dd:.0f}\">{dd_label}</text>"
        )
        t -= dd_step

    dd_pts = " ".join(f"{sx(i):.1f},{sy_dd(dd[i]):.1f}" for i in range(n))
    zero_dd = sy_dd(0.0)
    dd_fill = f"{sx(0):.1f},{zero_dd:.1f} {dd_pts} {sx(n-1):.1f},{zero_dd:.1f}"
    parts.append(f'<polygon points="{dd_fill}" fill="{C_DD_FILL}"/>')
    parts.append(
        f'<polyline points="{dd_pts}" fill="none" stroke="{C_DD}" stroke-width="{max(1.5,width//600)}"/>'
    )
    parts.append(
        f'<line x1="{x0}" y1="{zero_dd:.1f}" x2="{x0+pw}" y2="{zero_dd:.1f}" stroke="{C_MUTED}" '
        f'stroke-width="1" opacity="0.5"/>'
    )

    if show_dd_markers:
        parts.append(
            f'<circle cx="{sx(max_dd_i):.1f}" cy="{sy_dd(dd[max_dd_i]):.1f}" r="{dot_r}" fill="{C_DD}"/>'
        )
        parts.append(
            f'<text x="{sx(max_dd_i)+label_pad:.0f}" y="{sy_dd(dd[max_dd_i])+fs*0.12:.0f}" fill="{C_DD}" '
            f"font-family='{ff}' font-size=\"{fs:.0f}\" font-weight=\"600\">"
            f'{dd[max_dd_i]:.1f}%</text>'
        )

    tick_idx = [0, n // 4, n // 2, 3 * n // 4, n - 1]
    x_date_y = height - int(fs * 0.55)
    for j, i in enumerate(tick_idx):
        if j == 0:
            anchor, x_pos = "start", x0
        elif j == len(tick_idx) - 1:
            anchor, x_pos = "end", x0 + pw
        else:
            anchor, x_pos = "middle", sx(i)
        parts.append(
            f'<text x="{x_pos:.1f}" y="{x_date_y}" text-anchor="{anchor}" fill="{C_MUTED}" '
            f"font-family='{ff}' font-size=\"{fs:.0f}\">"
            f'{_fmt_date(dates[i], compact=date_compact)}</text>'
        )

    parts.append("</svg>")
    return "\n".join(parts)


def _marketing_html(stats: dict, chart_svg: str) -> str:
    ret = stats["total_return_pct"]
    bullets = [
        f"<strong>+{ret:.0f}% net return</strong> over a full year of live-market replay — "
        f"turning disciplined options income into measurable portfolio growth.",
        f"<strong>{stats['win_rate_pct']:.0f}% winning trades</strong> across {stats['n_trades']} "
        f"round-trips, with a profit factor of {stats['profit_factor']:.1f}.",
        f"<strong>Sharpe ratio of {stats['sharpe']:.1f}</strong> — risk-adjusted performance that "
        f"institutional allocators look for, not just headline returns.",
        f"<strong>Maximum drawdown held to {stats['max_dd_pct']:.1f}%</strong> — defined-risk "
        f"options structures designed to cap tail exposure.",
        f"<strong>{stats['positive_months_pct']:.0f}% of months closed positive</strong> — "
        f"consistency that compounds quietly in the background.",
        f"<strong>Sortino {stats['sortino']:.1f} · Calmar {stats['calmar']:.1f}</strong> — "
        f"downside-aware metrics that reward smooth equity, not lucky streaks.",
        f"<strong>Up to {stats['consec_wins_max']} consecutive winning periods</strong> with "
        f"loss streaks capped at {stats['consec_losses_max']} — a profile built for staying invested.",
        "Backtested on <strong>real exchange tick data</strong> with realistic fills, fees, "
        "and intraday mark-to-market — not curve-fit fantasy.",
    ]

    bullet_html = "\n".join(f"        <li>{b}</li>" for b in bullets[:8])

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>tudysho — Performance Overview</title>
<style>
  :root {{
    --bg: #070d18;
    --card: #0f1a2b;
    --text: #edf2f9;
    --muted: #8fa3bc;
    --gold: #c9a84c;
    --blue: #3b9eff;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: Inter, "Segoe UI", system-ui, sans-serif;
    background: radial-gradient(ellipse 120% 80% at 50% -20%, #152238 0%, var(--bg) 55%);
    color: var(--text);
    line-height: 1.65;
    min-height: 100vh;
  }}
  .wrap {{ max-width: 960px; margin: 0 auto; padding: 3rem 1.5rem 4rem; }}
  .eyebrow {{
    font-size: 0.72rem; letter-spacing: 0.22em; text-transform: uppercase;
    color: var(--gold); font-weight: 600; margin-bottom: 1rem;
  }}
  h1 {{
    font-size: clamp(2rem, 5vw, 2.75rem); font-weight: 700;
    letter-spacing: -0.03em; line-height: 1.15; margin-bottom: 0.75rem;
  }}
  .tagline {{
    font-size: 1.15rem; color: var(--muted); max-width: 38rem; margin-bottom: 2.5rem;
  }}
  .chart-card {{
    background: var(--card); border: 1px solid #1c2d44; border-radius: 16px;
    padding: 1rem; margin-bottom: 2.5rem;
    box-shadow: 0 24px 60px rgba(0,0,0,0.35);
  }}
  .chart-card svg {{ width: 100%; height: auto; display: block; border-radius: 10px; }}
  ul {{
    list-style: none; display: grid; gap: 1rem;
  }}
  li {{
    padding-left: 1.4rem; position: relative; color: #c5d3e4; font-size: 1.02rem;
  }}
  li::before {{
    content: ""; position: absolute; left: 0; top: 0.55em;
    width: 7px; height: 7px; border-radius: 50%; background: var(--blue);
    box-shadow: 0 0 10px rgba(59,158,255,0.5);
  }}
  li strong {{ color: var(--text); }}
  footer {{
    margin-top: 3rem; font-size: 0.78rem; color: #5c708a; line-height: 1.5;
    border-top: 1px solid #1a2a40; padding-top: 1.25rem;
  }}
</style>
</head>
<body>
<div class="wrap">
  <div class="eyebrow">Simulated track record · {stats['date_from']} – {stats['date_to']}</div>
  <h1>Steady Growth from<br>Disciplined Options Income</h1>
  <p class="tagline">
    A systematic options approach that prioritises consistency over speculation —
    delivering strong returns with controlled drawdowns, backed by exchange-grade data.
  </p>

  <div class="chart-card">
    {chart_svg}
  </div>

  <ul>
{bullet_html}
  </ul>

  <footer>
    Past simulated performance is not indicative of future results. Figures shown are
    percentage returns from a backtest on historical market data and do not represent
    actual client accounts. Options trading involves substantial risk.
  </footer>
</div>
</body>
</html>
"""


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    source_dir = OUT / "source"
    source_dir.mkdir(exist_ok=True)

    data = _load_combo_data()
    stats = data["stats"]
    eq = data["eq"]
    capital = data["capital"]
    meta = data["meta"]

    # Raw data exports
    data["trades"].to_csv(OUT / "combo_trades.csv", index=False)
    data["nav"].to_csv(OUT / "combo_nav_daily.csv", index=False)
    if data["fills"] is not None:
        data["fills"].to_csv(OUT / "combo_fills.csv", index=False)
    _equity_daily_csv(eq, capital).to_csv(OUT / "equity_daily.csv", index=False)
    (OUT / "stats.json").write_text(json.dumps(stats, indent=2))
    (OUT / "params.json").write_text(json.dumps(data["params"], indent=2))

    provenance = {
        "run_id": RUN_ID,
        "combo_hash": COMBO_HASH,
        "combo_idx": COMBO_IDX,
        "ui_url": f"http://localhost:5007/?tab=Combo+Detail&run={RUN_ID}&combo={COMBO_HASH}",
        "bundle_path": str(BUNDLE.resolve()),
        "backtest_html": str(BACKTEST_HTML.resolve()),
        "strategy": meta["strategy"],
        "created_at": meta.get("created_at"),
        "git_sha": meta.get("git_sha"),
        "git_dirty": meta.get("git_dirty"),
        "runtime_s": meta.get("runtime_s"),
        "n_combos_in_run": meta.get("n_combos"),
        "param_grid": meta.get("param_grid"),
        "date_range": meta.get("date_range"),
        "account_size_usd": meta.get("account_size"),
    }
    (OUT / "metadata.json").write_text(json.dumps(provenance, indent=2))
    shutil.copy2(BUNDLE / "meta.json", source_dir / "bundle_meta.json")
    if BACKTEST_HTML.exists():
        shutil.copy2(BACKTEST_HTML, OUT / "backtest_report.html")

    svg_16x9 = equity_chart_svg(eq, capital, width=1920, height=1080, title="Cumulative Return")
    svg_square = equity_chart_svg(eq, capital, width=1080, height=1080, title="Cumulative Return")

    li_kw = dict(
        title=None,
        font_scale=3.0,
        show_dd_markers=False,
        date_compact=True,
        font_family=FONT_DIN,
        eq_section_label="Trading Performance",
        dd_section_label="Drawdown",
    )
    svg_16x9_li = equity_chart_svg(eq, capital, width=1920, height=1080, **li_kw)
    svg_square_li = equity_chart_svg(eq, capital, width=1080, height=1080, **li_kw)

    (OUT / "equity_chart_16x9.svg").write_text(svg_16x9)
    (OUT / "equity_chart_square.svg").write_text(svg_square)
    (OUT / "equity_chart_16x9_linkedin.svg").write_text(svg_16x9_li)
    (OUT / "equity_chart_square_linkedin.svg").write_text(svg_square_li)
    (OUT / "marketing_report.html").write_text(_marketing_html(stats, svg_16x9))

    _svg_to_png(OUT / "equity_chart_16x9_linkedin.svg",
                OUT / "equity_chart_16x9_linkedin.png", 1600, 900)
    _svg_to_png(OUT / "equity_chart_square_linkedin.svg",
                OUT / "equity_chart_square_linkedin.png", 1080, 1080)

    print(f"Wrote marketing assets to {OUT.resolve()}")
    for p in sorted(OUT.iterdir()):
        print(f"  {p.name}")


if __name__ == "__main__":
    main()

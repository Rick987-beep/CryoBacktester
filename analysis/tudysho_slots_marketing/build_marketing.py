#!/usr/bin/env python3
"""Build marketing package for combined tudysho slots A+B+C."""
from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtester.results import equity_metrics

from analysis.tudysho_slots_marketing.combine_trades import combine_trades, count_position_overlaps
from analysis.tudysho_slots_marketing.combined_intraday_dd import (
    compute_combined_intraday_equity,
    nav_daily_for_equity_metrics,
    trade_level_metrics,
)
from analysis.tudysho_slots_marketing.performance_square import performance_square_svg

OUT = Path(__file__).resolve().parent
HANDOVER = ROOT / "handover/tudysho_cryotrader"

CAPITAL = 100_000.0
DATE_FROM = "2025-06-27"
DATE_TO = "2026-06-27"
# Linear scale on contract count / premium target (PnL and open MTM scale together).
SIZE_SCALE = 0.5

SLOTS = {
    "A": {"combo_hash": "5cd986cf48cd", "label": "Mon–Thu daytime"},
    "B": {"combo_hash": "e2f4ac2b3e69", "label": "Monday early"},
    "C": {"combo_hash": "829e7226cc48", "label": "Friday → Saturday"},
}


def _load_chart_module():
    path = ROOT / "analysis/run146_marketing/build_marketing.py"
    spec = importlib.util.spec_from_file_location("m146", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _stats_from_trades(
    trades: pd.DataFrame,
    eq_mtm: dict,
    eq_close: dict,
    dd_meta: dict,
    trade_metrics: dict,
    overlap_count: int,
) -> dict:
    n_days = len(eq_mtm["daily"])
    years = max(n_days / 365, 1 / 365)
    final_eq = CAPITAL + float(eq_mtm["total_pnl"])
    cagr = (final_eq / CAPITAL) ** (1.0 / years) - 1 if CAPITAL > 0 else 0.0

    trades = trades.copy()
    trades["month"] = pd.to_datetime(trades["entry_date"]).dt.to_period("M")
    monthly = trades.groupby("month")["pnl"].sum()
    positive_months_pct = float((monthly > 0).mean() * 100) if len(monthly) else 0.0

    return {
        "strategy": "tudysho",
        "size_scale": SIZE_SCALE,
        "merge_method": trades["merge_method"].iloc[0] if len(trades) else "union_no_overlap",
        "position_overlaps_excl_end_of_data": overlap_count,
        "date_from": DATE_FROM,
        "date_to": DATE_TO,
        "account_size_usd": CAPITAL,
        "n_trades": int(len(trades)),
        "total_pnl_usd": round(float(trades["pnl"].sum()), 2),
        "total_return_pct": round(float(eq_mtm["total_pnl"]) / CAPITAL * 100, 1),
        "cagr_pct": round(cagr * 100, 1),
        "max_dd_pct": round(eq_mtm["max_dd_pct"], 1),
        "max_dd_pct_close_only": round(eq_close["max_dd_pct"], 1),
        "max_dd_pct_intraday_scaled": dd_meta["max_dd_pct_intraday_scaled"],
        "max_dd_date": dd_meta["max_dd_date"],
        "isolated_slot_max_dd_pct": dd_meta["isolated_slot_max_dd_pct"],
        "sharpe": round(eq_mtm["sharpe"], 2),
        "sharpe_close_only": round(eq_close["sharpe"], 2),
        "sortino": round(eq_mtm["sortino"], 2),
        "sortino_close_only": round(eq_close["sortino"], 2),
        "calmar": round(eq_mtm["calmar"], 2),
        "profit_factor": round(trade_metrics["profit_factor"], 2),
        "win_rate_pct": round(float((trades["pnl"] > 0).mean() * 100), 1),
        "positive_months_pct": round(positive_months_pct, 0),
        "consec_wins_max": trade_metrics["consec_wins"],
        "consec_losses_max": trade_metrics["consec_losses"],
        "avg_pnl_usd": round(float(trades["pnl"].mean()), 2),
        "median_pnl_usd": round(float(trades["pnl"].median()), 2),
        "trades_by_slot": {
            k: int(v) for k, v in trades["slot"].value_counts().items()
        },
        "slots": SLOTS,
        "metrics_basis": "EOD MTM nav_close + intraday hi/lo for DD; trade-level profit factor",
    }


def _marketing_html(stats: dict, chart_svg: str) -> str:
    ret = stats["total_return_pct"]
    bullets = [
        f"<strong>+{ret:.0f}% net return</strong> over twelve months of simulated "
        f"live-market replay on a ${stats['account_size_usd']:,.0f} notional base.",
        f"<strong>{stats['win_rate_pct']:.0f}% hit rate</strong> across {stats['n_trades']} "
        f"round trips, with a trade-level profit factor of {stats['profit_factor']:.1f}.",
        f"<strong>Sharpe ratio of {stats['sharpe']:.1f}</strong> on end-of-day "
        f"mark-to-market returns.",
        f"<strong>Peak drawdown of {stats['max_dd_pct']:.1f}%</strong> on an "
        f"intraday-scaled basis, with explicit exit rules and position limits.",
        f"<strong>{stats['positive_months_pct']:.0f}% of months closed positive</strong> "
        f"in the simulation window.",
        f"<strong>Sortino {stats['sortino']:.1f}</strong> and "
        f"<strong>Calmar {stats['calmar']:.1f}</strong> on the same return series.",
        f"<strong>{stats['consec_wins_max']} consecutive winning trades</strong> at peak; "
        f"maximum loss streak {stats['consec_losses_max']}.",
        "Simulated on <strong>historical exchange tick data</strong> with realistic "
        "fees, fills, and intraday mark-to-market.",
    ]
    bullet_html = "\n".join(f"        <li>{b}</li>" for b in bullets)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Short Gamma Engine | Performance Overview</title>
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
    font-family: "DIN Next LT Pro", "DIN Next", "DIN Alternate", Barlow,
      "Helvetica Neue", Helvetica, Arial, sans-serif;
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
    font-size: 1.15rem; color: var(--muted); max-width: 42rem; margin-bottom: 1.25rem;
  }}
  .strategy {{
    font-size: 1.02rem; color: #c5d3e4; max-width: 42rem; margin-bottom: 2.5rem;
    line-height: 1.7;
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
  <div class="eyebrow">Simulated track record · {stats['date_from']} to {stats['date_to']}</div>
  <h1>Systematic Premium<br>Harvesting Programme</h1>
  <p class="tagline">
    The Short Gamma Engine is a rules-based short-volatility programme designed for
    repeatable carry extraction with institutional risk reporting standards.
  </p>
  <p class="strategy">
    The engine sells options premium when the market is quiet, using a volatility
    regime filter before each entry. Exposure is scheduled around recurring calendar
    structure: intraday timing windows through the week, a Friday entry into the
    weekend volatility cool-down, and a Monday session that captures the post-weekend
    reopen. Sizing, fees, and mark-to-market are applied on exchange-grade historical
    data.
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
    actual client accounts. Short options strategies involve substantial risk of loss.
  </footer>
</div>
</body>
</html>
"""


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    charts = _load_chart_module()

    trades = combine_trades()
    trades["pnl"] = trades["pnl"] * SIZE_SCALE
    overlap_count = count_position_overlaps(trades)
    if overlap_count:
        print(f"WARNING: {overlap_count} position overlap(s) detected (excl. end_of_data)")

    daily_df, dd_meta = compute_combined_intraday_equity(
        trades,
        capital=CAPITAL,
        date_from=DATE_FROM,
        date_to=DATE_TO,
        size_scale=SIZE_SCALE,
    )
    nav_daily = nav_daily_for_equity_metrics(daily_df, CAPITAL)

    eq_mtm = equity_metrics(
        trades,
        capital=CAPITAL,
        nav_daily_combo=nav_daily,
        date_from=DATE_FROM,
        date_to=DATE_TO,
    )
    assert eq_mtm is not None

    eq_close = equity_metrics(
        trades, capital=CAPITAL, date_from=DATE_FROM, date_to=DATE_TO,
    )
    assert eq_close is not None

    trade_metrics = trade_level_metrics(trades)
    stats = _stats_from_trades(
        trades, eq_mtm, eq_close, dd_meta, trade_metrics, overlap_count,
    )

    trades.to_csv(OUT / "combo_trades.csv", index=False)
    daily_df.to_csv(OUT / "equity_daily.csv", index=False)
    (OUT / "stats.json").write_text(json.dumps(stats, indent=2) + "\n")

    schedule = json.loads((HANDOVER / "LIVE_PARAM_SCHEDULE.json").read_text())
    (OUT / "params.json").write_text(json.dumps(schedule, indent=2) + "\n")

    provenance = {
        "package": "tudysho_slots_marketing",
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "size_scale": SIZE_SCALE,
        "merge_method": "union_no_overlap",
        "handover_package": str(HANDOVER.relative_to(ROOT)),
        "slot_sources": {
            slot: {
                **meta,
                "trades_file": (
                    f"handover/tudysho_cryotrader/backtests/trades_slot_"
                    f"{'a_mon_thu' if slot == 'A' else 'b_mon_early' if slot == 'B' else 'c_fri_sat'}.csv"
                ),
            }
            for slot, meta in SLOTS.items()
        },
        "methodology": "METHODOLOGY.md",
    }
    (OUT / "metadata.json").write_text(json.dumps(provenance, indent=2) + "\n")

    svg_16x9 = charts.equity_chart_svg(
        eq_mtm, CAPITAL, width=1920, height=1080,
        title="Short Gamma Engine: Combined Programme",
    )
    svg_square = charts.equity_chart_svg(
        eq_mtm, CAPITAL, width=1080, height=1080,
        title="Short Gamma Engine: Combined Programme",
    )
    li_kw = dict(
        title=None,
        font_scale=3.0,
        show_dd_markers=False,
        date_compact=True,
        font_family=charts.FONT_DIN,
        eq_section_label="Trading Performance",
        dd_section_label="Drawdown",
    )
    svg_16x9_li = charts.equity_chart_svg(eq_mtm, CAPITAL, width=1920, height=1080, **li_kw)
    svg_square_li = charts.equity_chart_svg(eq_mtm, CAPITAL, width=1080, height=1080, **li_kw)

    (OUT / "equity_chart_16x9.svg").write_text(svg_16x9)
    (OUT / "equity_chart_square.svg").write_text(svg_square)
    (OUT / "equity_chart_16x9_linkedin.svg").write_text(svg_16x9_li)
    (OUT / "equity_chart_square_linkedin.svg").write_text(svg_square_li)
    (OUT / "marketing_report.html").write_text(_marketing_html(stats, svg_16x9))

    charts._svg_to_png(
        OUT / "equity_chart_16x9_linkedin.svg",
        OUT / "equity_chart_16x9_linkedin.png",
        1600, 900,
    )
    charts._svg_to_png(
        OUT / "equity_chart_square_linkedin.svg",
        OUT / "equity_chart_square_linkedin.png",
        1080, 1080,
    )

    perf_svg = performance_square_svg(stats, daily_df, CAPITAL)
    (OUT / "performance_square.svg").write_text(perf_svg)
    charts._svg_to_png(
        OUT / "performance_square.svg",
        OUT / "performance_square.png",
        1080, 1080,
    )

    print(f"Wrote marketing assets to {OUT.resolve()}")
    print(
        f"  Trades: {stats['n_trades']}  Return: +{stats['total_return_pct']}%  "
        f"Sharpe: {stats['sharpe']} (MTM; close-only {stats['sharpe_close_only']})  "
        f"Max DD: {stats['max_dd_pct']}%  Calmar: {stats['calmar']}  "
        f"PF: {stats['profit_factor']}  Overlaps: {overlap_count}"
    )


if __name__ == "__main__":
    main()

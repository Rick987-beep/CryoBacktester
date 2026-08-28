#!/usr/bin/env python3
"""Build the Monopteros 7-chapter strategy report (Defined Theta layout)."""
from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path

from agent_commons.analysis.strategy_report import (
    ReportCard,
    ReportEdgeItem,
    ReportStep,
    StrategyReportCopy,
)
from agents.strategy_report.agent import run_strategy_report

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "analysis/marketing/monopteros_strategy_report"
NAV = OUT / "data/equity_daily.csv"
TRADES = OUT / "data/trades.csv"

# Engine metrics from backtester.inspect combo 750 / 812903ec6cc9
ENGINE_LOCK = {
    "sharpe": 8.358940124511719,
    "sortino": 11.282829812542223,
    "calmar": 11.050504409287472,
    "max_dd_pct": 8.399940490722656,
    "win_rate": 0.9621212121212122,
    "omega": 4.462937355041504,
    "ann_return": 0.9282357692718506,
    "total_pnl": 92945.4453125,
    "n": 264,
    "max_loss": -3790.1611328125,
}

BRIEF = """
Product name: Monopteros. Present it as one strategy that sells short-dated
Bitcoin volatility. Do not frame "master combo", "marketing book", or
"three schedules glued into one equity curve" for the investor.
In strategy mechanics only, it is fine to note that three weekday entry
schedules (and their configs) exist inside the strategy.
Short listed Bitcoin strangles, short tenor into the next daily expiry.
Entry is selective: skip sessions when the market looks too turbulent.
Risk controls: premium stop and strike-proximity stop. Sized at 0.4% of NAV
target premium. Naked shorts (no wings). No clocks, deltas, thresholds,
stop multiples, or combo hashes. Most tickets expire (scheduled close;
includes losers).
"""


def build_copy() -> StrategyReportCopy:
    return StrategyReportCopy(
        product_name="Monopteros",
        subtitle="Short-dated selling of Bitcoin volatility premium.",
        eyebrow="Strategy Report · August 2026",
        asset="BTC options (Deribit)",
        position_size="0.4% of NAV target premium",
        firm="Aureas GmbH",
        confidential=True,
        summary_h2="A Disciplined Machine for Harvesting Volatility Premium",
        summary_html="""
<p><strong>Monopteros</strong> sells short-dated listed Bitcoin strangles and
holds them into the next daily expiry. Entry is selective: sessions that look
too turbulent to write are skipped, and idle days are allowed. The strategy
collects overnight theta on naked short premium and keeps a premium stop and a
strike-proximity stop on every ticket so one path cannot run unchecked.</p>
<p>Bitcoin options persistently price implied volatility above the realised move
that follows. Sellers of that insurance collect the spread. Short-dated paper
pays theta quickly and carries overnight gamma. Most tickets simply expire. In
this sample, 254 of 264 did. Expiry is the scheduled close; it includes winning
tickets and most of the 10 losers.</p>
<p>On a $100,000 account from 27 Aug 2025 to 27 Aug 2026 the strategy delivered
<strong>+93.2% total return</strong> ($93,171 ending NAV; CAGR +93.4%) with a maximum
intraday drawdown of <strong>−8.40%</strong> ($14,194). It was profitable on
96.2% of 264 tickets. Sharpe 8.36, Sortino 11.28, Calmar 11.05. Median ticket
about $442. Worst loss −$3,790.</p>
""".strip(),
        mechanics_h2="Strategy Architecture and Execution Framework",
        mechanics_lead_html="""
<p>Monopteros is a short-dated volatility-selling strategy on listed Bitcoin
options. Inside the week it uses three entry schedules with their own configs:
an early Monday ticket, a weekday midday ticket, and a Friday late-morning ticket
into Saturday expiry. On each look the strategy asks whether short-dated paper
is worth writing. When the tape looks too turbulent, the session is skipped.
When the screen is quiet it stays flat. Idle days are allowed. The job is to
sell insurance that already looks expensive enough to carry overnight.</p>
<p>A ticket is a short listed call and put (a strangle) into the next daily
expiry, sized so the target premium is a fixed fraction of account NAV. There is
no wing and no cover: naked short premium with hard exit rules, not a
defined-risk structure. At most one position is open at a time; Monday can still
print two tickets when both the early and midday schedules fire on the same day.</p>
<p>After the open the position is marked until the next daily expiry. There is
no mid-life rehedge and no discretionary overlay. A premium stop and a
strike-proximity stop sit on the ticket from the open. In this sample the median
hold was 15 hours: overnight, into the next daily expiry.</p>
<p>The intended close is expiry. Here 254 of 264 tickets expired; eight premium
stops and one proximity stop fired (one ticket closed at end of data). Expiry
includes winning tickets and most of the 10 losers. Typical winners are a few
hundred dollars of collected premium (median $442). The losers are larger, and
the worst ticket still lost $3,790. That is the cycle: look, write or skip, hold
overnight, expire or stop, then recycle.</p>
""".strip(),
        steps=[
            ReportStep(
                name="Weekday Looks",
                desc=(
                    "Three entry schedules with their own configs: early Monday, "
                    "midday Mon–Thu, Friday late morning."
                ),
            ),
            ReportStep(
                name="Selective Entry",
                desc="Skip sessions that look too turbulent to write. Idle days are allowed.",
            ),
            ReportStep(
                name="Short Strangle",
                desc="Sell a listed call and put into the next daily expiry. Size scales with NAV.",
            ),
            ReportStep(
                name="Hold / Stop",
                desc=(
                    "Mark overnight. Close on premium damage or strike proximity, "
                    "otherwise let it expire."
                ),
            ),
        ],
        cards=[
            ReportCard(
                name="Entry Filter",
                value="Turbulence gate",
                desc=(
                    "Opens when the session looks calm enough to write short-dated paper. "
                    "Quiet or chaotic sessions stay flat."
                ),
                kind="fil",
                icon="📊",
            ),
            ReportCard(
                name="Theta Window",
                value="Next expiry",
                desc=(
                    "Short-dated listed strangles. Theta is earned overnight. "
                    "Median hold in sample: 15 hours."
                ),
                kind="default",
                icon="",
            ),
            ReportCard(
                name="Stops",
                value="Premium + proximity",
                desc=(
                    "A premium stop and a strike-proximity stop sit on every ticket. "
                    "Nine early exits in 264 trades."
                ),
                kind="sl",
                icon="🛑",
            ),
            ReportCard(
                name="Structure",
                value="Naked short",
                desc=(
                    "Call and put sold into the next daily expiry. No wing and no cover; "
                    "exits are the risk control."
                ),
                kind="tp",
                icon="⚡",
            ),
        ],
        performance_h2="",
        performance_lead_html=(
            "<p>Monopteros was replayed on listed BTC options (Deribit) from 27 Aug 2025 "
            "to 27 Aug 2026 on $100,000 starting capital, 264 tickets. The path is a "
            "high-hit-rate overnight theta strategy with a handful of larger losers inside "
            "a moderate drawdown.</p>"
        ),
        edge_h2="Structural Advantages and the Source of Alpha",
        edge_lead_html="""
<p>Short-dated Bitcoin options keep paying writers who are willing to sit through
overnight gamma. The listed market prices that insurance with an implied move that
still tends to run ahead of what actually prints into the next daily expiry.
Monopteros harvests that gap by writing selectively across the week rather than
forcing a single daily clock. The edge is not a clever stop. It is repeated
writing of paper that decays overnight, with exits that keep the rare bad path
from dominating the year.</p>
""".strip(),
        edge_items=[
            ReportEdgeItem(
                title="Weekday rhythm",
                html=(
                    "Three entry schedules with their own configs spread looks across the week, "
                    "so the strategy is not tied to a single session."
                ),
            ),
            ReportEdgeItem(
                title="Selective writing",
                html=(
                    "The strategy skips sessions that look too turbulent to sell overnight gamma. "
                    "Idle days are a feature: no force to trade every clock."
                ),
            ),
            ReportEdgeItem(
                title="Overnight theta on short paper",
                html=(
                    "Tickets target the next daily expiry. Most of the sample’s P&amp;L comes from "
                    "premium that simply expires, not from active scalping."
                ),
            ),
            ReportEdgeItem(
                title="Hard exits on the rare losers",
                html=(
                    "Premium and proximity stops cut tickets that are going wrong. Nine early exits "
                    "in 264 trades; the rest ran to the scheduled close."
                ),
            ),
            ReportEdgeItem(
                title="NAV-fraction sizing",
                html=(
                    "Each open targets a fixed fraction of account NAV as premium, so contract size "
                    "scales with equity without a separate gearing story."
                ),
            ),
        ],
        risks_h2="Risk Factors and Mitigation Framework",
        risks_lead_html=(
            "<p>Monopteros is a naked short-premium strategy. High win rate does not remove "
            "gap risk, overnight gamma, or the fact that a small number of losers dominate "
            "the left tail.</p>"
        ),
        risks_items=[
            (
                "<strong>Naked short gamma:</strong> There is no wing or cover. A fast move through "
                "a strike can print a large red ticket before a stop fills."
            ),
            (
                "<strong>Left-tail concentration:</strong> Ten losers against 254 winners still "
                "include a worst ticket of −$3,790; drawdown reached −8.4% ($14,194)."
            ),
            (
                "<strong>Stop slippage:</strong> Premium and proximity stops assume listed quotes. "
                "In a gap, the exit can be worse than the model mark."
            ),
            (
                "<strong>Sample length:</strong> One year of Deribit history. Regime shifts, fee "
                "changes, or thinner books can change the hit rate and the size of losers."
            ),
            (
                "<strong>Schedule clustering:</strong> Monday can open twice when both weekday "
                "schedules fire. Correlated sessions can stack risk in the same week."
            ),
        ],
        notes_html="",
    )


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    result = asyncio.run(
        run_strategy_report(
            product_name="Monopteros",
            brief=BRIEF,
            nav_path=NAV,
            trades_path=TRADES,
            capital=100_000,
            engine_lock=ENGINE_LOCK,
            copy=build_copy(),
            asset="BTC options (Deribit)",
            position_size="0.4% of NAV target premium",
            firm="Aureas GmbH",
            output_dir=OUT,
            quality_gate="report",
        )
    )
    assert result.output_path is not None
    (OUT / "stats.json").write_text(
        json.dumps(result.pack, indent=2, default=str) + "\n"
    )
    mirror = ROOT / "analysis/marketing/monopteros_strategy_report.html"
    shutil.copyfile(result.output_path, mirror)
    print("HTML:", result.output_path)
    print("mirror:", mirror)
    print("quality:", result.metadata.get("quality_path"))


if __name__ == "__main__":
    main()

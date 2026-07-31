#!/usr/bin/env python3
"""Generate branded HTML report for live vs BT fill comparison."""
from __future__ import annotations

import html
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

PKG = Path(__file__).resolve().parents[1]
DATA = PKG / "data"
META = json.loads((PKG / "metadata.json").read_text())

# Default brand tokens (agent-commons default)
C_PRIMARY = "#2E6B5A"
C_NAVY = "#1A2B3C"
C_BG = "#F5F5F0"
C_SURFACE = "#FFFFFF"
C_TEXT = "#1A1A1A"
C_MUTED = "#6B7280"
C_BORDER = "#E5E7EB"
C_POS = "#059669"
C_NEG = "#DC2626"
C_WARN = "#D97706"


def _fmt(v, digits=2):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    if isinstance(v, (int, float)):
        return f"{v:,.{digits}f}"
    return html.escape(str(v))


def _table(headers: list[str], rows: list[list], right_cols: set[int] | None = None) -> str:
    right_cols = right_cols or set()
    th = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = []
    for row in rows:
        tds = []
        for i, cell in enumerate(row):
            cls = ' class="num"' if i in right_cols else ""
            tds.append(f"<td{cls}>{cell}</td>")
        body.append("<tr>" + "".join(tds) + "</tr>")
    return f'<table><thead><tr>{th}</tr></thead><tbody>{"".join(body)}</tbody></table>'


def pairwise_rows(live: pd.DataFrame, bt: pd.DataFrame) -> list[list]:
    L2 = live[(live.entry_date >= "2026-07-08") & (live.strategy == "tudysho")]
    B2 = bt[bt.entry_date >= "2026-07-08"]
    rows = []
    for ds in sorted(set(L2.entry_date) | set(B2.entry_date)):
        for sched in ("mon_early", "mon_thu", "fri"):
            l = L2[(L2.entry_date == ds) & (L2.schedule == sched)]
            b = B2[(B2.entry_date == ds) & (B2.schedule == sched)]
            if len(l) == 0 and len(b) == 0:
                continue
            if len(l) == 1 and len(b) == 1:
                lr, br = l.iloc[0], b.iloc[0]
                dlt = lr.pnl_usd_per_lot - br.pnl_usd_per_lot
                dlt_s = f'<span class="{"pos" if dlt >= 0 else "neg"}">{dlt:+.2f}</span>'
                rows.append(
                    [
                        ds,
                        sched,
                        f"{int(lr.call)}/{int(lr.put)}",
                        f"{int(br.call)}/{int(br.put)}",
                        _fmt(lr.pnl_usd_per_lot),
                        _fmt(br.pnl_usd_per_lot),
                        dlt_s,
                        f"{lr.entry_utc[11:]} / {br.entry_utc[11:]}",
                    ]
                )
            elif len(l) == 1:
                lr = l.iloc[0]
                rows.append(
                    [
                        ds,
                        sched,
                        f"{int(lr.call)}/{int(lr.put)}",
                        "—",
                        _fmt(lr.pnl_usd_per_lot),
                        "—",
                        "—",
                        f"{lr.entry_utc[11:]} / —",
                    ]
                )
            elif len(b) == 1:
                br = b.iloc[0]
                rows.append(
                    [
                        ds,
                        sched,
                        "—",
                        f"{int(br.call)}/{int(br.put)}",
                        "—",
                        _fmt(br.pnl_usd_per_lot),
                        "—",
                        f"— / {br.entry_utc[11:]}",
                    ]
                )
    return rows


def build_html() -> str:
    live = pd.read_csv(DATA / "live_jul1_17.csv")
    bt = pd.read_csv(DATA / "bt_jul1_17.csv")
    day = pd.read_csv(DATA / "day_by_day.csv")
    fill_gap_path = DATA / "fill_gap.csv"
    fill_gap = pd.read_csv(fill_gap_path) if fill_gap_path.exists() else pd.DataFrame()

    L2 = live[(live.entry_date >= "2026-07-08") & (live.strategy == "tudysho")]
    B2 = bt[bt.entry_date >= "2026-07-08"]

    totals_full = [
        ["Full Jul 1–17", len(live), len(bt), _fmt(live.pnl_usd.sum()), _fmt(bt.pnl_usd.sum()),
         _fmt(live.pnl_usd_per_lot.sum()), _fmt(bt.pnl_usd_per_lot.sum())],
        ["Tudysho-only Jul 8–17", len(L2), len(B2), _fmt(L2.pnl_usd.sum()), _fmt(B2.pnl_usd.sum()),
         _fmt(L2.pnl_usd_per_lot.sum()), _fmt(B2.pnl_usd_per_lot.sum())],
    ]

    day_rows = []
    for _, r in day.iterrows():
        note = html.escape(str(r.notes)) if r.notes else ""
        day_rows.append([
            r.date, r.weekday, int(r.live_n), int(r.bt_n),
            _fmt(r.live_pnl_usd), _fmt(r.bt_pnl_usd),
            _fmt(r.live_pnl_per_lot), _fmt(r.bt_pnl_per_lot), note,
        ])

    fill_rows = []
    for _, r in fill_gap.iterrows():
        live_cell = '<span class="pos">yes</span>' if r.live_filled else '<span class="muted">no</span>'
        openable = f"{int(r.bt_openable_ticks)}/{int(r.total_ticks)}"
        if r.bt_openable_ticks == 0 and r.live_filled:
            openable = f'<span class="warn">{openable}</span>'
        first_ok = r.first_bt_openable_utc
        first_ok_s = first_ok[11:16] if isinstance(first_ok, str) and first_ok else "—"
        fill_rows.append([
            r.date, r.schedule, r.entry_utc[11:16], openable,
            int(r.mark_fallback_ok_ticks), live_cell,
            _fmt(r.at_entry_call_bid, 4), _fmt(r.at_entry_call_mark, 4),
            first_ok_s,
        ])

    generated = datetime.now().strftime("%Y-%m-%d %H:%M")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Eisbach Live vs Backtester — Fill Comparison</title>
<style>
:root {{
  --primary: {C_PRIMARY};
  --navy: {C_NAVY};
  --bg: {C_BG};
  --surface: {C_SURFACE};
  --text: {C_TEXT};
  --muted: {C_MUTED};
  --border: {C_BORDER};
  --pos: {C_POS};
  --neg: {C_NEG};
  --warn: {C_WARN};
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0;
  font-family: "Segoe UI", system-ui, -apple-system, sans-serif;
  background: var(--bg);
  color: var(--text);
  line-height: 1.55;
  font-size: 15px;
}}
header {{
  background: linear-gradient(135deg, var(--navy) 0%, #243447 100%);
  color: #fff;
  padding: 2.5rem 2rem 2rem;
  border-bottom: 4px solid var(--primary);
}}
header h1 {{ margin: 0 0 .35rem; font-size: 1.75rem; font-weight: 600; letter-spacing: -0.02em; }}
header .sub {{ opacity: .85; font-size: .95rem; max-width: 52rem; }}
.wrap {{ max-width: 1100px; margin: 0 auto; padding: 2rem 1.5rem 4rem; }}
section {{ margin-bottom: 2.5rem; }}
h2 {{
  color: var(--navy);
  font-size: 1.25rem;
  margin: 0 0 1rem;
  padding-bottom: .4rem;
  border-bottom: 2px solid var(--primary);
}}
h3 {{ font-size: 1rem; color: var(--navy); margin: 1.5rem 0 .75rem; }}
p {{ margin: 0 0 .85rem; max-width: 72ch; }}
.card {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 1.25rem 1.5rem;
  margin-bottom: 1rem;
  box-shadow: 0 1px 3px rgba(0,0,0,.04);
}}
.card.warn {{ border-left: 4px solid var(--warn); }}
.card.ok {{ border-left: 4px solid var(--primary); }}
ul {{ margin: .5rem 0 1rem; padding-left: 1.25rem; }}
li {{ margin-bottom: .35rem; max-width: 72ch; }}
table {{
  width: 100%;
  border-collapse: collapse;
  font-size: .875rem;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 6px;
  overflow: hidden;
  margin: .75rem 0 1.25rem;
}}
th {{
  background: var(--navy);
  color: #fff;
  text-align: left;
  padding: .55rem .65rem;
  font-weight: 600;
}}
td {{ padding: .45rem .65rem; border-top: 1px solid var(--border); vertical-align: top; }}
tr:nth-child(even) td {{ background: #fafafa; }}
.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
.pos {{ color: var(--pos); font-weight: 600; }}
.neg {{ color: var(--neg); font-weight: 600; }}
.warn {{ color: var(--warn); font-weight: 600; }}
.muted {{ color: var(--muted); }}
.meta-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: .75rem;
  margin: 1rem 0;
}}
.meta-item {{
  background: #f8f9fa;
  border-radius: 6px;
  padding: .65rem .85rem;
  font-size: .85rem;
}}
.meta-item strong {{ display: block; color: var(--navy); font-size: .75rem; text-transform: uppercase; letter-spacing: .04em; }}
code {{ background: #eef2f0; padding: .1em .35em; border-radius: 3px; font-size: .88em; }}
footer {{ margin-top: 3rem; padding-top: 1rem; border-top: 1px solid var(--border); color: var(--muted); font-size: .8rem; }}
</style>
</head>
<body>
<header>
  <div class="wrap" style="padding:0">
    <h1>Eisbach: Live vs Backtester Fill Comparison</h1>
    <p class="sub">Forensic reconciliation of CryoTrader slot-02 (prod) against backtester run 375,
    focusing on trade presence, per-lot PnL, and the bid-vs-mark fill gap on thin OTM wings.</p>
  </div>
</header>
<div class="wrap">

<section>
  <h2>Executive summary</h2>
  <div class="card ok">
    <p>We replicated the live Eisbach configuration in the backtester as <code>tudysho_eisbach</code>,
    then compared Jul 1–17, 2026 trades from prod slot-02 against UI run 375 (combo
    <code>{META['backtest']['combo_hash']}</code>).</p>
    <p><strong>Bottom line:</strong> when both systems trade the same strikes on the same schedule,
    exit quality and $/lot match closely (often within pennies). The main divergence is
    <em>whether a trade happens at all</em> — live fills more often in thin-book conditions because
    it prices off mark when bid is zero; the backtester requires bid &gt; 0 on both legs.</p>
  </div>
</section>

<section>
  <h2>What we did</h2>
  <ol>
    <li><strong>Strategy port.</strong> Created <code>backtester/strategies/tudysho_eisbach.py</code> mirroring
    CryoTrader slot-02: three schedules (Mon–Thu 16:00 NYC, Monday 01:00, Friday 12:00), per-schedule
    delta/min_otm/turbulence/SL/proximity params. Intentionally kept backtester turbulence bucketing and
    $100k sizing (0.8% NAV / 12 per BTC-equity).</li>
    <li><strong>Backtest run.</strong> UI run 375, Jul 1–18 window, combo hash
    <code>{META['backtest']['combo_hash']}</code>, bundle
    <code>{META['backtest']['bundle']}</code>.</li>
    <li><strong>Live blotter pull.</strong> SCP from <code>{META['live']['host']}</code>:
    <code>{META['live']['blotter_path']}</code> (pulled {META['live']['pulled']}).</li>
    <li><strong>Trade alignment.</strong> Normalised both sides to entry-date CSVs with schedule, strikes,
    qty, and $/lot PnL. Built day-by-day presence matrix.</li>
    <li><strong>Fill-gap scan.</strong> Replayed 5-min option snapshots after each schedule entry time;
    counted ticks where BT would open (bid&gt;0 both legs) vs ticks where mark-fallback would allow entry.</li>
  </ol>

  <div class="meta-grid">
    <div class="meta-item"><strong>Window</strong>{META['window']['entry_from']} → {META['window']['entry_to']}</div>
    <div class="meta-item"><strong>Live slot</strong>{META['live']['slot']} (~{META['live']['sizing']['typical_lots']} lots)</div>
    <div class="meta-item"><strong>BT account</strong>${META['backtest']['account_size_usd']:,} (~{META['backtest']['sizing']['typical_lots']} lots)</div>
    <div class="meta-item"><strong>BT run</strong>#{META['backtest']['run_id']}</div>
  </div>
</section>

<section>
  <h2>Caveats — read before interpreting $</h2>
  <div class="card warn">
    <ul>
      <li><strong>Strategy change mid-window.</strong> Live ran <code>short_str_turb_dyn</code> through Jul 7,
      then <code>tudysho</code> Eisbach from Jul 8. Only Jul 8–17 is apples-to-apples on strategy logic.</li>
      <li><strong>Sizing differs.</strong> Live: 0.28% NAV, max 20 lots on ~$285k. BT: 0.8% / 12 on $100k.
      Compare <strong>$/lot</strong> and trade presence, not raw dollar PnL.</li>
      <li><strong>Bid vs mark.</strong> Live <code>_bid_price()</code> falls back to mark when bid is missing
      (<code>min_qty_price_floor=0</code>). BT <code>_bid_acceptable()</code> requires bid &gt; 0.</li>
      <li><strong>Jul 17 BT artifact.</strong> Run ends 2026-07-18 before Saturday 08:00 expiry →
      <code>end_of_data</code> MTM close, not a real strategy loss vs live.</li>
    </ul>
  </div>
</section>

<section>
  <h2>Totals</h2>
  {_table(
    ["Scope", "Live n", "BT n", "Live $", "BT $", "Live $/lot", "BT $/lot"],
    totals_full,
    right_cols={1, 2, 3, 4, 5, 6},
  )}
</section>

<section>
  <h2>Day-by-day</h2>
  {_table(
    ["Date", "WD", "Live#", "BT#", "Live $", "BT $", "Live $/lot", "BT $/lot", "Notes"],
    day_rows,
    right_cols={2, 3, 4, 5, 6, 7},
  )}
</section>

<section>
  <h2>Pairwise matched trades (Jul 8+, by schedule)</h2>
  <p>Rows where live and BT traded the same schedule on the same entry date, or one side skipped.</p>
  {_table(
    ["Date", "Schedule", "Live C/P", "BT C/P", "Live $/lot", "BT $/lot", "Δ $/lot", "Entry UTC (L / BT)"],
    pairwise_rows(live, bt),
    right_cols={4, 5, 6},
  )}
</section>

<section>
  <h2>Fill-gap analysis: why live trades when BT skips</h2>
  <p>At OTM wing strikes the order book is often empty on one side (<code>bid=0</code>) while mark/ask
  still show ~1 Deribit tick (0.0001 BTC). After <code>min_otm_pct=2.6</code> pushes strikes further OTM,
  the call wing is especially prone to zero bids in 5-min snapshots.</p>

  <div class="card">
    <table style="border:none;box-shadow:none;margin:0">
      <tr><td style="border:none;background:transparent;width:140px"><strong>Backtester</strong></td>
          <td style="border:none;background:transparent">Requires <code>bid &gt; 0</code> on both legs → skip or wait.</td></tr>
      <tr><td style="border:none;background:transparent"><strong>Live</strong></td>
          <td style="border:none;background:transparent">Falls back to mark; RFQ/limit can fill at minimum tick.</td></tr>
    </table>
  </div>

  <h3>Snapshot scan after schedule entry</h3>
  {_table(
    ["Date", "Schedule", "Entry", "BT openable", "Mark-fallback ticks", "Live filled",
     "Call bid@entry", "Call mark@entry", "First BT-open UTC"],
    fill_rows,
    right_cols={3, 4},
  ) if len(fill_rows) else '<p class="muted">Run scripts/analyze_fill_gap.py to generate fill_gap.csv</p>'}

  <h3>Key mismatch days</h3>
  <ul>
    <li><strong>Jul 7 &amp; Jul 9 (mon_thu):</strong> turbulence below threshold; BT saw 0 openable ticks
    after entry (call bid=0 post-min_otm). Live still filled via mark path.</li>
    <li><strong>Jul 16 (mon_thu):</strong> live filled 20:00 UTC; BT waited until 23:20 for positive bids →
    different strikes (66000/62500 vs 65500/62000).</li>
    <li><strong>Jul 13 mon_early:</strong> both traded; put strike differs (61500 live vs 62000 BT) →
    $/lot gap ($6.29 vs $16.89).</li>
    <li><strong>Jul 17 fri:</strong> same strikes; live +$22.92/lot at expiry, BT −$4.13/lot from
    <code>end_of_data</code> only.</li>
  </ul>
</section>

<section>
  <h2>Findings for later work</h2>
  <ol>
    <li><strong>Exit logic is sound.</strong> When both trade comparable strikes, $/lot converges — Jul 10, 13 mon_thu,
    14, 15 within pennies. The backtester models expiry PnL correctly.</li>
    <li><strong>Fill model is the gap.</strong> Live likely trades more frequently in thin-wing regimes.
    All live Eisbach opens in this window had combined premium 0.0001–0.0006 BTC (~$6–$37/1x).</li>
    <li><strong>Optional BT parity mode.</strong> Accept mark when bid=0 (optimistic) to approximate live RFQ fills.
    Would recover Jul 9 and reduce Jul 16 timing skew.</li>
    <li><strong>Extend BT date range.</strong> Run through Jul 19+ to avoid Jul 17 <code>end_of_data</code> artifact.</li>
    <li><strong>Pre-Eisbach days (Jul 1–7)</strong> are not comparable: live legacy strat vs BT Eisbach schedules.</li>
  </ol>
</section>

<section>
  <h2>Artifacts in this folder</h2>
  <ul>
    <li><code>data/slot-02.jsonl</code> — raw prod blotter</li>
    <li><code>data/live_jul1_17.csv</code>, <code>data/bt_jul1_17.csv</code> — normalised trades</li>
    <li><code>data/day_by_day.csv</code> — daily presence and PnL</li>
    <li><code>data/fill_gap.csv</code> — snapshot bid/mark scan</li>
    <li><code>scripts/</code> — reproducible extraction and analysis</li>
    <li><code>metadata.json</code> — provenance and run IDs</li>
  </ul>
</section>

<footer>
  Generated {generated} · CryoBacktester analysis package ·
  BT bundle: {html.escape(META['backtest']['bundle'])}
</footer>
</div>
</body>
</html>"""


def main() -> None:
    out = PKG / "report.html"
    out.write_text(build_html(), encoding="utf-8")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()

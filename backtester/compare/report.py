"""Generate summary.md and report.html from comparison run."""
from __future__ import annotations

import html
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from backtester.compare.io_utils import write_json

C_PRIMARY = "#2E6B5A"
C_NAVY = "#1A2B3C"
C_BG = "#F5F5F0"
C_POS = "#059669"
C_NEG = "#DC2626"
C_WARN = "#D97706"


def _fmt(v, digits=2):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    if isinstance(v, (int, float)):
        return f"{v:,.{digits}f}"
    return html.escape(str(v))


def write_summary(manifest: dict, comparison: pd.DataFrame, forensics: pd.DataFrame, out: Path) -> None:
    n_ok = int((comparison.comparability == "OK").sum())
    n_warn = int((comparison.comparability == "WARN").sum())
    n_excl = int((comparison.comparability == "EXCLUDE").sum())
    lines = [
        f"# Livecompare — slot {manifest.get('slot')} — {manifest.get('created_at', '')[:10]}",
        "",
        f"**Window:** last {manifest.get('last_n')} live fills",
        f"**BT:** `{manifest.get('bt_strategy')}` | bundle `{manifest.get('bundle', '')}`",
        "",
        "## Comparability",
        f"- OK: {n_ok} | WARN: {n_warn} | EXCLUDE: {n_excl}",
        "",
        "## Trades",
        "",
        "| Date | Sched | Comp | Live $/lot | BT $/lot | Δ | Notes |",
        "|------|-------|------|----------:|---------:|--:|-------|",
    ]
    for _, r in comparison.iterrows():
        fore = forensics[(forensics.entry_date == r.entry_date) & (forensics.schedule == r.schedule)]
        note = fore.iloc[0]["narrative"] if len(fore) else ""
        dlt = r.get("delta_pnl_per_lot", "")
        dlt_s = f"{dlt:+.2f}" if pd.notna(dlt) and dlt != "" else "—"
        lines.append(
            f"| {r.entry_date} | {r.schedule} | {r.comparability} | "
            f"{_fmt(r.live_pnl_per_lot)} | {_fmt(r.bt_pnl_per_lot) if r.bt_present else '—'} | "
            f"{dlt_s} | {note[:60]} |"
        )
    lines.extend(["", "## Warnings", ""])
    for w in manifest.get("warnings", []):
        lines.append(f"- **{w['code']}** ({w['severity']}): {w['message']}")
    out.write_text("\n".join(lines) + "\n")


def write_report_html(manifest: dict, comparison: pd.DataFrame, forensics: pd.DataFrame, out: Path) -> None:
    cmp_rows = []
    for _, r in comparison.iterrows():
        comp_cls = {"OK": "pos", "WARN": "warn", "EXCLUDE": "neg"}.get(r.comparability, "")
        fore = forensics[(forensics.entry_date == r.entry_date) & (forensics.schedule == r.schedule)]
        narr = fore.iloc[0]["narrative"] if len(fore) else ""
        cmp_rows.append([
            r.entry_date, r.schedule,
            f'<span class="{comp_cls}">{r.comparability}</span>',
            _fmt(r.live_pnl_per_lot),
            _fmt(r.bt_pnl_per_lot) if r.bt_present else "—",
            html.escape(str(r.warning_codes)),
            html.escape(narr[:120]),
        ])

    warn_items = "".join(
        f"<li><strong>{html.escape(w['code'])}</strong> ({w['severity']}): {html.escape(w['message'])}</li>"
        for w in manifest.get("warnings", [])
    )

    body_rows = "".join(
        "<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>" for row in cmp_rows
    )

    html_doc = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/>
<title>Livecompare slot {manifest.get('slot')}</title>
<style>
body{{font-family:system-ui,sans-serif;background:{C_BG};margin:0;padding:2rem;color:#1a1a1a}}
header{{background:{C_NAVY};color:#fff;padding:1.5rem 2rem;border-bottom:4px solid {C_PRIMARY}}}
table{{width:100%;border-collapse:collapse;background:#fff;margin:1rem 0}}
th{{background:{C_NAVY};color:#fff;text-align:left;padding:.5rem}}
td{{padding:.45rem;border-top:1px solid #e5e7eb;font-size:.9rem}}
.pos{{color:{C_POS};font-weight:600}}.neg{{color:{C_NEG};font-weight:600}}.warn{{color:{C_WARN};font-weight:600}}
</style></head><body>
<header><h1>Livecompare — slot {manifest.get('slot')}</h1>
<p>Last {manifest.get('last_n')} live fills vs {html.escape(str(manifest.get('bt_strategy')))}</p></header>
<div style="max-width:1100px;margin:0 auto">
<h2>Warnings</h2><ul>{warn_items}</ul>
<h2>Comparison</h2>
<table><thead><tr><th>Date</th><th>Sched</th><th>Comp</th><th>Live $/lot</th><th>BT $/lot</th><th>Codes</th><th>Forensics</th></tr></thead>
<tbody>{body_rows}</tbody></table>
<p style="color:#666;font-size:.85rem">Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
</div></body></html>"""
    out.write_text(html_doc, encoding="utf-8")

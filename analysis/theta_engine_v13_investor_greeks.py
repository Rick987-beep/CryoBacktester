#!/usr/bin/env python3
"""Scorecard: v13 shorts-only vs full-book D/G.

Reads the latest ``theta_engine_v13_*.bundle`` that contains
``investor_greeks.parquet``. Vega is reported as a meter only.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtester.core.paths import runs_dir
from workspace.strategies.theta_engine._common import BASELINE_DISPLAY

OUT = ROOT / "analysis" / "theta_engine_v13_investor_greeks.json"


def _latest_v13_bundle() -> Path:
    bundles = sorted(
        runs_dir().glob("theta_engine_v13_*.bundle"),
        key=lambda p: p.stat().st_mtime,
    )
    with_sidecar = [p for p in bundles if (p / "investor_greeks.parquet").exists()]
    if not with_sidecar:
        raise SystemExit(
            "No theta_engine_v13 bundle with investor_greeks.parquet under %s"
            % runs_dir()
        )
    return with_sidecar[-1]


def _display(entry: str) -> str:
    return BASELINE_DISPLAY.get(str(entry), str(entry))


def main() -> None:
    bundle = _latest_v13_bundle()
    greeks = pd.read_parquet(bundle / "investor_greeks.parquet")
    nav = pd.read_parquet(bundle / "final_nav.parquet")
    meta = json.loads((bundle / "meta.json").read_text())
    df = greeks.merge(nav[["combo_idx", "final_nav", "realized_pnl"]], on="combo_idx", how="left")
    capital = float(meta.get("account_size") or 0.0)
    df["pnl"] = df["final_nav"] - capital if capital else df["realized_pnl"]

    strike_col = "wing_strike_pct" if "wing_strike_pct" in df.columns else "wing_delta"
    debit_col = "wing_debit_pct" if "wing_debit_pct" in df.columns else "wing_budget_usd"
    sort_cols = ["entry_policy", debit_col, strike_col, "wing_expiry"]
    sort_cols = [c for c in sort_cols if c in df.columns]

    print("bundle:", bundle)
    print("date_range:", meta.get("date_range"))
    print("n_combos:", len(df))
    print()
    print(
        "entry  strike%  debit%  expiry  live  wing%  "
        "sD%  sG%  fD%  fG%  rec%  still%  ind%  pnl"
    )

    rows_out = []
    for _, r in df.sort_values(sort_cols).iterrows():
        entry = r["entry_policy"]
        strike = float(r[strike_col]) if strike_col in r.index else float("nan")
        debit = float(r[debit_col]) if debit_col in r.index else float("nan")
        line = {
            "entry": _display(entry),
            "entry_policy": entry,
            "wing_strike_pct": strike,
            "wing_debit_pct": debit,
            "wing_expiry": str(r.get("wing_expiry", "")),
            "live_bars": int(r["live_bars"]),
            "wing_on_pct": float(r["wing_on_pct"]),
            "short_breach_d_pct": float(r["short_breach_d_pct"]),
            "short_breach_g_pct": float(r["short_breach_g_pct"]),
            "full_breach_d_pct": float(r["full_breach_d_pct"]),
            "full_breach_g_pct": float(r["full_breach_g_pct"]),
            "recovered_pct": float(r["recovered_pct"]),
            "still_breach_pct": float(r["still_breach_pct"]),
            "induced_pct": float(r["induced_pct"]),
            "short_p95_abs_d_pct": float(r["short_p95_abs_d_pct"]),
            "full_p95_abs_d_pct": float(r["full_p95_abs_d_pct"]),
            "short_min_g_pct": float(r["short_min_g_pct"]),
            "full_min_g_pct": float(r["full_min_g_pct"]),
            "mean_abs_v_pct": float(r["mean_abs_v_pct"]),
            "pnl": float(r["pnl"]),
        }
        rows_out.append(line)
        print(
            f"{line['entry']:<12}  {line['wing_strike_pct']:7.2f}  "
            f"{line['wing_debit_pct']:6.2f}  {line['wing_expiry']:<12}  "
            f"{line['live_bars']:>6}  {line['wing_on_pct']:5.1f}  "
            f"{line['short_breach_d_pct']:5.1f}  {line['short_breach_g_pct']:5.1f}  "
            f"{line['full_breach_d_pct']:5.1f}  {line['full_breach_g_pct']:5.1f}  "
            f"{line['recovered_pct']:5.1f}  {line['still_breach_pct']:5.1f}  "
            f"{line['induced_pct']:5.1f}  {line['pnl']:+,.0f}"
        )

    payload = {
        "bundle": str(bundle),
        "date_range": meta.get("date_range"),
        "account_size": capital,
        "combos": rows_out,
    }
    OUT.write_text(json.dumps(payload, indent=2))
    print("\nwrote", OUT)


if __name__ == "__main__":
    main()

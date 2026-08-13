#!/usr/bin/env python3
"""Quantify sticky-wing trading cost/frequency: v10-legacy vs v11-new.

Reads two GUI-run bundles produced by ``theta_engine_v11_gui_runs.py``
(the ``v10_legacy`` and ``v11_new`` wing configurations) and reports, per
Mode C baseline (RichForce16 / Daily15):

* wing round-trips (opens matched to a close via ``open_idx``)
* total wing fees + total wing bid/ask-spread cost (net premium round-trip)
* average / median wing holding time
* total book PnL and short-book-only breach bar count, for context

This is the same style of analysis that flagged v10's sticky-wing thrashing
in the first place — rerun here against v11 to quantify the improvement.

Usage:
    python -m analysis.theta_engine_v11_wing_cost_report \
        --legacy data/runs/theta_engine_v11_<ts_legacy>.bundle \
        --new    data/runs/theta_engine_v11_<ts_new>.bundle

With no args, auto-discovers the two most recent theta_engine_v11 bundles
under ``data/runs`` whose ``param_grid.wing_side_mode`` is ``count``
(legacy) and ``greek`` (new).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtester.core.paths import runs_dir
from workspace.strategies.theta_engine._common import BASELINE_DAILY15, BASELINE_RICHFORCE16

_DISPLAY = {BASELINE_RICHFORCE16: "RichForce16", BASELINE_DAILY15: "Daily15"}


def _load_meta(bundle: Path) -> Dict[str, Any]:
    return json.loads((bundle / "meta.json").read_text())


def _combo_entry_policy(meta: Dict[str, Any], combo_idx: int) -> str:
    keys = meta["keys"][combo_idx]
    for k, v in keys:
        if k == "entry_policy":
            return str(v)
    return "?"


def _auto_discover() -> tuple[Path, Path]:
    root = Path(runs_dir())
    candidates = sorted(
        (p for p in root.glob("theta_engine_v11_*.bundle") if p.is_dir()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    legacy = new = None
    for p in candidates:
        try:
            meta = _load_meta(p)
        except Exception:
            continue
        grid = meta.get("param_grid", {})
        if grid.get("option_hedge_mode") != ["sticky_budget"]:
            continue
        side_mode = grid.get("wing_side_mode")
        if side_mode == ["count"] and legacy is None:
            legacy = p
        elif side_mode == ["greek"] and new is None:
            new = p
        if legacy is not None and new is not None:
            break
    if legacy is None or new is None:
        raise SystemExit(
            "Could not auto-discover both bundles; pass --legacy/--new explicitly. "
            "Run analysis/theta_engine_v11_gui_runs.py first."
        )
    return legacy, new


def _wing_stats(bundle: Path, combo_idx: int) -> Dict[str, Any]:
    fills = pd.read_parquet(bundle / "fills.parquet")
    fills = fills[fills["combo_idx"] == combo_idx]
    wing = fills[fills["comment"].fillna("").str.contains("sticky_wing")]

    opens = wing[wing["event"] == "open"]
    closes = wing[wing["event"] == "close"]
    n_opens = len(opens)
    n_closes = len(closes)

    paired = opens.merge(
        closes, on="open_idx", suffixes=("_open", "_close"), how="inner"
    )
    if not paired.empty:
        hold_minutes = (
            (paired["ts_close"] - paired["ts_open"]).dt.total_seconds() / 60.0
        )
        round_trip_cost = -(paired["amount_usd_open"] + paired["amount_usd_close"])
        round_trip_cost += paired["fee_usd_open"] + paired["fee_usd_close"]
    else:
        hold_minutes = pd.Series(dtype=float)
        round_trip_cost = pd.Series(dtype=float)

    trade_log = pd.read_parquet(bundle / "trade_log.parquet")
    trade_log = trade_log[trade_log["combo_idx"] == combo_idx]
    total_pnl = float(trade_log["pnl"].sum())

    return {
        "n_wing_opens": int(n_opens),
        "n_wing_closes": int(n_closes),
        "n_round_trips": int(len(paired)),
        "total_wing_fees_usd": round(float(wing["fee_usd"].sum()), 2),
        "total_wing_cost_usd": round(float(round_trip_cost.sum()), 2),
        "avg_hold_minutes": round(float(hold_minutes.mean()), 1) if len(hold_minutes) else 0.0,
        "median_hold_minutes": round(float(hold_minutes.median()), 1) if len(hold_minutes) else 0.0,
        "pct_holds_under_15min": (
            round(float((hold_minutes <= 15).mean() * 100.0), 1) if len(hold_minutes) else 0.0
        ),
        "total_book_pnl_usd": round(total_pnl, 2),
    }


def _print_table(rows: List[Dict[str, Any]]) -> None:
    cols = [
        "regime", "entry", "n_wing_opens", "n_round_trips",
        "avg_hold_minutes", "median_hold_minutes", "pct_holds_under_15min",
        "total_wing_fees_usd", "total_wing_cost_usd", "total_book_pnl_usd",
    ]
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    header = "  ".join(c.ljust(widths[c]) for c in cols)
    print(header)
    print("-" * len(header))
    for r in rows:
        print("  ".join(str(r.get(c, "")).ljust(widths[c]) for c in cols))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--legacy", type=str, default=None, help="v10-legacy bundle dir")
    ap.add_argument("--new", type=str, default=None, help="v11-new bundle dir")
    args = ap.parse_args()

    if args.legacy and args.new:
        legacy_bundle, new_bundle = Path(args.legacy), Path(args.new)
    else:
        legacy_bundle, new_bundle = _auto_discover()

    print(f"v10-legacy bundle: {legacy_bundle}")
    print(f"v11-new    bundle: {new_bundle}\n")

    legacy_meta = _load_meta(legacy_bundle)
    new_meta = _load_meta(new_bundle)

    rows: List[Dict[str, Any]] = []
    for combo_idx in range(len(legacy_meta["keys"])):
        ep = _combo_entry_policy(legacy_meta, combo_idx)
        stats = _wing_stats(legacy_bundle, combo_idx)
        rows.append({"regime": "v10_legacy", "entry": _DISPLAY.get(ep, ep), **stats})
    for combo_idx in range(len(new_meta["keys"])):
        ep = _combo_entry_policy(new_meta, combo_idx)
        stats = _wing_stats(new_bundle, combo_idx)
        rows.append({"regime": "v11_new", "entry": _DISPLAY.get(ep, ep), **stats})

    rows.sort(key=lambda r: (r["entry"], r["regime"]))
    _print_table(rows)

    out = ROOT / "analysis" / "theta_engine_v11_wing_cost_report.json"
    out.write_text(json.dumps(rows, indent=2))
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Phase 3 — sticky long options for residual G/V on P1 size + optional perp."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis.theta_engine_v10_phase_lib import (
    CAPITAL,
    base_book_params,
    load_replay,
    run_grid_telemetry,
    scorecard_row,
    write_phase_dir,
)
from workspace.strategies.theta_engine._common import BASELINE_DAILY15, BASELINE_RICHFORCE16
from workspace.strategies.theta_engine.v10 import ThetaEngineV10


def _champion_params() -> dict:
    meta_path = ROOT / "analysis" / "theta_engine_v10_p1" / "meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        ch = meta.get("champion") or {}
        if ch:
            return {
                "qty_per_1btc_equity": [float(ch["qty_per_1btc_equity"])],
                "max_concurrent": [int(ch["max_concurrent"])],
                "greek_limits_mode": ["scale"],
            }
    return {
        "qty_per_1btc_equity": [0.10],
        "max_concurrent": [10],
        "greek_limits_mode": ["scale"],
    }


def main() -> None:
    replay = load_replay()
    ch = _champion_params()
    grid = base_book_params(
        greek_limits_mode=ch["greek_limits_mode"],
        qty_per_1btc_equity=ch["qty_per_1btc_equity"],
        max_concurrent=ch["max_concurrent"],
        entry_policy=[BASELINE_RICHFORCE16, BASELINE_DAILY15],
        perp_delta_hedge=[1],
        perp_deadband_pct=[2.0],
        option_hedge_mode=["none", "sticky_budget"],
    )
    results = run_grid_telemetry(replay, grid)
    scorecard = [
        scorecard_row(
            r["params"], r["tel"],
            pnl=r["pnl"], n_closes=r["n_closes"],
            ann_return=r["ann_return"], max_dd=r["max_dd"], sharpe=r["sharpe"],
        )
        for r in results
    ]
    wing_activity = []
    for r in results:
        s = scorecard_row(r["params"], r["tel"], pnl=r["pnl"], n_closes=r["n_closes"],
                          ann_return=r["ann_return"], max_dd=r["max_dd"], sharpe=r["sharpe"])
        wing_activity.append({
            "entry": s["entry"],
            "option_hedge_mode": s["option_hedge_mode"],
            "wing_adjusts": s["wing_adjusts"],
            "breach_v_pct": s["breach_v_pct"],
            "breach_g_pct": s["breach_g_pct"],
            "breach_d_pct": s["breach_d_pct"],
            "ann_return": s["ann_return"],
            "max_dd_pct": s["max_dd_pct"],
            "pnl": s["pnl"],
        })

    sticky = [s for s in scorecard if s["option_hedge_mode"] == "sticky_budget"]
    plain = [s for s in scorecard if s["option_hedge_mode"] in ("none", "off")]
    v_improved = False
    if sticky and plain:
        v_improved = sum(s["breach_v_pct"] for s in sticky) < sum(
            s["breach_v_pct"] for s in plain
        )

    summary = [
        "# Phase 3 — sticky long options",
        "",
        f"Size from P1: {ch}; perp_delta_hedge=1",
        "",
        f"Vega breach improved with sticky wings: **{v_improved}**",
        "",
        "## Roadmap status",
        "P0–P3 complete. Review scorecards vs 20% ann / 5% DD and investor bands.",
        "",
    ]
    out = write_phase_dir(
        "p3",
        summary_md="\n".join(summary),
        scorecard_rows=scorecard,
        meta={
            "phase": "p3",
            "date_range": list(ThetaEngineV10.DATE_RANGE),
            "account_size": CAPITAL,
            "param_grid": grid,
            "champion_from_p1": ch,
            "vega_improved": v_improved,
        },
        extras={"wing_activity.csv": wing_activity},
    )
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()

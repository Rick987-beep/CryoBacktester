#!/usr/bin/env python3
"""Phase 2 — BTC perp delta overlay on Phase 1 champion (+ naked control)."""

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
                "entry_policy": ch["entry_policy"],
                "qty_per_1btc_equity": [float(ch["qty_per_1btc_equity"])],
                "max_concurrent": [int(ch["max_concurrent"])],
                "greek_limits_mode": ["scale"],
            }
    return {
        "entry_policy": [BASELINE_RICHFORCE16, BASELINE_DAILY15],
        "qty_per_1btc_equity": [0.10],
        "max_concurrent": [10],
        "greek_limits_mode": ["scale"],
    }


def main() -> None:
    replay = load_replay()
    ch = _champion_params()
    # Champion ± perp, both baselines at champion size for comparison
    grid = base_book_params(
        greek_limits_mode=ch["greek_limits_mode"],
        qty_per_1btc_equity=ch["qty_per_1btc_equity"],
        max_concurrent=ch["max_concurrent"],
        entry_policy=[BASELINE_RICHFORCE16, BASELINE_DAILY15],
        perp_delta_hedge=[0, 1],
        perp_deadband_pct=[2.0],
        option_hedge_mode=["none"],
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
    perp_activity = []
    for r in results:
        s = scorecard_row(r["params"], r["tel"], pnl=r["pnl"], n_closes=r["n_closes"],
                          ann_return=r["ann_return"], max_dd=r["max_dd"], sharpe=r["sharpe"])
        perp_activity.append({
            "entry": s["entry"],
            "perp_delta_hedge": s["perp_delta_hedge"],
            "perp_trades": s["perp_trades"],
            "perp_fees": s["perp_fees"],
            "breach_d_pct": s["breach_d_pct"],
            "breach_v_pct": s["breach_v_pct"],
            "ann_return": s["ann_return"],
            "max_dd_pct": s["max_dd_pct"],
            "pnl": s["pnl"],
        })

    with_perp = [s for s in scorecard if int(s["perp_delta_hedge"]) == 1]
    without = [s for s in scorecard if int(s["perp_delta_hedge"]) == 0]
    d_improved = False
    if with_perp and without:
        d_improved = sum(s["breach_d_pct"] for s in with_perp) < sum(
            s["breach_d_pct"] for s in without
        )

    summary = [
        "# Phase 2 — perp delta overlay",
        "",
        f"Champion size from P1: {ch}",
        "",
        f"Delta breach improved with perp: **{d_improved}**",
        "",
        "## Next",
        "Phase 3: sticky_budget wings if V/G still outside bands.",
        "",
    ]
    out = write_phase_dir(
        "p2",
        summary_md="\n".join(summary),
        scorecard_rows=scorecard,
        meta={
            "phase": "p2",
            "date_range": list(ThetaEngineV10.DATE_RANGE),
            "account_size": CAPITAL,
            "param_grid": grid,
            "champion_from_p1": ch,
            "delta_improved": d_improved,
        },
        extras={"perp_activity.csv": perp_activity},
    )
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()

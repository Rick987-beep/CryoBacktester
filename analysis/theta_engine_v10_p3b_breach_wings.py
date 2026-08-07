#!/usr/bin/env python3
"""Phase 3b — breach-gated sticky wings on naked Mode C baselines.

Grid (Axis A × Axis B) vs naked control:
  wing_expiry_mode ∈ {same, next_listed}
  wing_delta ∈ {0.05, 0.10, 0.15}
  wing_trigger ∈ {dg, dgv}

No scale/perp. Episode logic: open once on breach → hold → close when
inside → reopen only on a fresh breach. Theta ignored for wing decisions.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis.theta_engine_v10_phase_lib import (
    CAPITAL,
    base_book_params,
    combos,
    load_replay,
    run_grid_telemetry,
    scorecard_row,
    write_phase_dir,
)
from workspace.strategies.theta_engine._common import BASELINE_DAILY15, BASELINE_RICHFORCE16
from workspace.strategies.theta_engine.v10 import ThetaEngineV10


def main() -> None:
    replay = load_replay()

    control = base_book_params(
        greek_limits_mode=["off"],
        perp_delta_hedge=[0],
        option_hedge_mode=["none"],
        wing_expiry_mode=["same"],
        wing_delta=[0.10],
        wing_trigger=["dg"],
        entry_policy=[BASELINE_RICHFORCE16, BASELINE_DAILY15],
    )
    sticky = base_book_params(
        greek_limits_mode=["off"],
        perp_delta_hedge=[0],
        option_hedge_mode=["sticky_budget"],
        wing_expiry_mode=["same", "next_listed"],
        wing_delta=[0.05, 0.10, 0.15],
        wing_trigger=["dg", "dgv"],
        entry_policy=[BASELINE_RICHFORCE16, BASELINE_DAILY15],
    )
    all_params = combos(control) + combos(sticky)
    results = run_grid_telemetry(replay, param_list=all_params)

    scorecard = [
        scorecard_row(
            r["params"], r["tel"],
            pnl=r["pnl"], n_closes=r["n_closes"],
            ann_return=r["ann_return"], max_dd=r["max_dd"], sharpe=r["sharpe"],
        )
        for r in results
    ]

    wing_activity = [
        {
            "entry": s["entry"],
            "option_hedge_mode": s["option_hedge_mode"],
            "wing_expiry_mode": s["wing_expiry_mode"],
            "wing_delta": s["wing_delta"],
            "wing_trigger": s["wing_trigger"],
            "wing_opens": s["wing_opens"],
            "wing_adjusts": s["wing_adjusts"],
            "wing_premium_paid": s["wing_premium_paid"],
            "breach_d_pct": s["breach_d_pct"],
            "breach_g_pct": s["breach_g_pct"],
            "breach_v_pct": s["breach_v_pct"],
            "ann_return": s["ann_return"],
            "max_dd_pct": s["max_dd_pct"],
            "pnl": s["pnl"],
        }
        for s in scorecard
    ]

    controls = [
        s for s in scorecard
        if s["option_hedge_mode"] in ("none", "off")
    ]
    treated = [s for s in scorecard if s["option_hedge_mode"] == "sticky_budget"]

    def _ok_product(row: dict) -> bool:
        return row["ann_return"] >= 0.18 and row["max_dd_pct"] <= 5.5

    ctrl_by_entry = {c["entry"]: c for c in controls}
    ranked = []
    for s in treated:
        c = ctrl_by_entry.get(s["entry"])
        if not c:
            continue
        dg_ctrl = c["breach_d_pct"] + c["breach_g_pct"]
        dg_s = s["breach_d_pct"] + s["breach_g_pct"]
        ranked.append({
            "entry": s["entry"],
            "wing_expiry_mode": s["wing_expiry_mode"],
            "wing_delta": s["wing_delta"],
            "wing_trigger": s["wing_trigger"],
            "ann_return": s["ann_return"],
            "max_dd_pct": s["max_dd_pct"],
            "pnl": s["pnl"],
            "breach_d_pct": s["breach_d_pct"],
            "breach_g_pct": s["breach_g_pct"],
            "breach_v_pct": s["breach_v_pct"],
            "wing_opens": s["wing_opens"],
            "dg_breach_delta": round(dg_s - dg_ctrl, 3),
            "ann_vs_ctrl": round(s["ann_return"] - c["ann_return"], 4),
            "product_ok": _ok_product(s),
        })
    ranked.sort(key=lambda r: (r["dg_breach_delta"], -r["ann_return"]))
    champion = ranked[0] if ranked else None
    dg_improved = bool(ranked and ranked[0]["dg_breach_delta"] < -1.0)
    product_any = any(r["product_ok"] for r in ranked)

    summary_lines = [
        "# Phase 3b — breach-gated sticky wings (naked Mode C)",
        "",
        "## Goal",
        "Long OTM wings only on D/G (or D/G/V) breach; Axis A expiry×δ; "
        "episode hold (no bar flip); theta ignored for wing decisions.",
        "",
        f"DATE_RANGE={ThetaEngineV10.DATE_RANGE}  capital={CAPITAL:,.0f}",
        "",
        f"Grid: 2 controls + {len(treated)} sticky cells "
        f"(2 expiry × 3 δ × 2 trigger × 2 entries).",
        "",
        "## Naked controls",
    ]
    for c in controls:
        summary_lines.append(
            f"- **{c['entry']}**: ann={c['ann_return']*100:.1f}%  "
            f"maxDD={c['max_dd_pct']:.1f}%  "
            f"D/G/V breach={c['breach_d_pct']:.1f}/{c['breach_g_pct']:.1f}/{c['breach_v_pct']:.1f}%  "
            f"PnL=${c['pnl']:,.0f}"
        )
    summary_lines.extend([
        "",
        f"**DG breach improved (best sticky vs control): {dg_improved}**",
        f"**Any sticky cell near 20%/5% DD: {product_any}**",
        "",
        "## Champion (lowest DG breach vs control, then return)",
    ])
    if champion:
        summary_lines.append(
            f"- **{champion['entry']}** "
            f"expiry={champion['wing_expiry_mode']} δ={champion['wing_delta']} "
            f"trigger={champion['wing_trigger']}: "
            f"ann={champion['ann_return']*100:.1f}% DD={champion['max_dd_pct']:.1f}% "
            f"Δ(D+G)breach={champion['dg_breach_delta']:+.1f}pp "
            f"opens={champion['wing_opens']}"
        )
    else:
        summary_lines.append("- (none)")
    summary_lines.extend(["", "## Top 5 by DG improvement", ""])
    for r in ranked[:5]:
        summary_lines.append(
            f"- {r['entry']} {r['wing_expiry_mode']}/δ{r['wing_delta']}/{r['wing_trigger']}: "
            f"ΔDG={r['dg_breach_delta']:+.1f}pp ann={r['ann_return']*100:.1f}% "
            f"opens={r['wing_opens']}"
        )
    summary_lines.extend([
        "",
        "## Next",
        "Read scorecard; if DG still stuck, combine with P2 perp on champion size.",
        "",
    ])

    out = write_phase_dir(
        "p3b",
        summary_md="\n".join(summary_lines),
        scorecard_rows=scorecard,
        meta={
            "phase": "p3b",
            "date_range": list(ThetaEngineV10.DATE_RANGE),
            "account_size": CAPITAL,
            "n_controls": len(controls),
            "n_sticky": len(treated),
            "champion": champion,
            "dg_improved": dg_improved,
            "product_any": product_any,
            "design": {
                "wing_expiry_mode": ["same", "next_listed"],
                "wing_delta": [0.05, 0.10, 0.15],
                "wing_trigger": ["dg", "dgv"],
                "greek_limits_mode": "off",
                "perp_delta_hedge": 0,
                "episode": "open once / hold / close inside / reopen on fresh breach",
                "theta": "ignored for wing open/size",
            },
        },
        extras={"wing_activity.csv": wing_activity},
    )
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()

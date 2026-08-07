#!/usr/bin/env python3
"""Phase 1 — scale mode × qty × max_concurrent scorecard."""

from __future__ import annotations

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
from workspace.strategies.theta_engine.v10 import ThetaEngineV10


def main() -> None:
    replay = load_replay()
    grid = base_book_params(
        greek_limits_mode=["off", "scale"],
        qty_per_1btc_equity=[0.05, 0.10, 0.15, 0.20],
        max_concurrent=[5, 10, 20],
    )
    print(f"P1 combos: {len(list(__import__('itertools').product(*grid.values())))}")
    results = run_grid_telemetry(replay, grid)

    scorecard = [
        scorecard_row(
            r["params"], r["tel"],
            pnl=r["pnl"], n_closes=r["n_closes"],
            ann_return=r["ann_return"], max_dd=r["max_dd"], sharpe=r["sharpe"],
        )
        for r in results
    ]

    # Champion: scale mode, DD<=5 preferred, then max ann_return; else best ann with DD<=10
    scale_rows = [s for s in scorecard if s["greek_limits_mode"] == "scale"]
    champ = None
    for dd_cap in (5.0, 10.0, 100.0):
        cand = [s for s in scale_rows if s["max_dd_pct"] <= dd_cap]
        if cand:
            champ = max(cand, key=lambda s: (s["ann_return"], -s["breach_v_pct"]))
            break
    if champ is None and scale_rows:
        champ = max(scale_rows, key=lambda s: s["ann_return"])

    control = [s for s in scorecard if s["greek_limits_mode"] == "off" and s["qty_per_1btc_equity"] == 0.2 and s["max_concurrent"] == 20]

    pareto_lines = [
        "# Phase 1 Pareto notes",
        "",
        "Target: ~20% ann, DD ≤ 5%, low vega breach, skips ≈ 0 under `scale`.",
        "",
        "## Champion (scale)",
    ]
    if champ:
        pareto_lines.append(
            f"- **{champ['entry']}** qty={champ['qty_per_1btc_equity']} "
            f"concurrent={champ['max_concurrent']}: "
            f"ann={champ['ann_return']*100:.1f}% DD={champ['max_dd_pct']:.1f}% "
            f"Vbreach={champ['breach_v_pct']:.1f}% skips={champ['skips']}"
        )
    pareto_lines += ["", "## Naked controls (off, qty=0.2, concurrent=20)", ""]
    for s in control:
        pareto_lines.append(
            f"- {s['entry']}: ann={s['ann_return']*100:.1f}% DD={s['max_dd_pct']:.1f}% "
            f"Vbreach={s['breach_v_pct']:.1f}%"
        )

    go = False
    if champ and champ["max_dd_pct"] <= 5 and champ["ann_return"] >= 0.15:
        go = True
    summary = [
        "# Phase 1 — scale & thin",
        "",
        "## Goal",
        "Keep entry calendar; shrink via `scale` + qty/concurrent; minimize skips.",
        "",
        f"DATE_RANGE={ThetaEngineV10.DATE_RANGE}  capital={CAPITAL:,.0f}",
        "",
        f"**Go/no-go vs 20%/5% DD:** {'GO (near target)' if go else 'NO-GO / review Pareto — may need P2 perp for delta'}",
        "",
        "## Champion",
        (pareto_lines[4] if champ else "- none"),
        "",
        "## Next",
        "Phase 2: enable `perp_delta_hedge=1` on champion params.",
        "",
    ]
    out = write_phase_dir(
        "p1",
        summary_md="\n".join(summary),
        scorecard_rows=scorecard,
        meta={
            "phase": "p1",
            "date_range": list(ThetaEngineV10.DATE_RANGE),
            "account_size": CAPITAL,
            "param_grid": grid,
            "champion": champ,
            "go_near_target": go,
        },
        extras={"pareto.md": "\n".join(pareto_lines)},
    )
    print(f"Wrote {out}")
    if champ:
        print("Champion:", champ)


if __name__ == "__main__":
    main()

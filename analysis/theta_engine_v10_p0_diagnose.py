#!/usr/bin/env python3
"""Phase 0 — naked Mode C breach rate vs concurrent opens (n_open)."""

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
    pct,
    run_grid_telemetry,
    scorecard_row,
    write_phase_dir,
)
from workspace.strategies.theta_engine.v10 import ThetaEngineV10


def main() -> None:
    replay = load_replay()
    grid = base_book_params(
        greek_limits_mode=["off"],
        qty_per_1btc_equity=[0.2],
        max_concurrent=[20],
    )
    results = run_grid_telemetry(replay, grid, collect_n_open=True)

    scorecard = []
    breach_rows = []
    for r in results:
        scorecard.append(scorecard_row(
            r["params"], r["tel"],
            pnl=r["pnl"], n_closes=r["n_closes"],
            ann_return=r["ann_return"], max_dd=r["max_dd"], sharpe=r["sharpe"],
        ))
        for bucket, st in sorted(r["n_open_bins"].items()):
            bars = st["bars"]
            breach_rows.append({
                "entry": scorecard[-1]["entry"],
                "n_open_bucket": bucket,
                "bars": bars,
                "breach_any_pct": round(pct(st["breach_any"], bars), 3),
                "breach_v_pct": round(pct(st["breach_v"], bars), 3),
                "breach_d_pct": round(pct(st["breach_d"], bars), 3),
            })

    # Decision hint
    emphasize = "qty_per_1btc_equity"
    for row in breach_rows:
        if row["n_open_bucket"] in ("0", "1") and row["breach_v_pct"] > 20:
            emphasize = "qty_per_1btc_equity (breaches even at low n_open)"
            break
        if row["n_open_bucket"] in ("6-10", "11+") and row["breach_v_pct"] > 50:
            emphasize = "max_concurrent (breaches concentrate at high n_open)"

    lines = [
        "# Phase 0 — scale vs overlap diagnose",
        "",
        "## Goal",
        "Decide whether naked Mode C greek breaches are mostly per-trade size or stacked overlap.",
        "",
        f"DATE_RANGE={ThetaEngineV10.DATE_RANGE}  capital={CAPITAL:,.0f}",
        "",
        "## Scorecard (greek_limits_mode=off)",
        "",
    ]
    for s in scorecard:
        lines.append(
            f"- **{s['entry']}**: ann={s['ann_return']*100:.1f}%  maxDD={s['max_dd_pct']:.1f}%  "
            f"PnL=${s['pnl']:,.0f}  Vbreach={s['breach_v_pct']:.1f}%  Dbreach={s['breach_d_pct']:.1f}%"
        )
    lines += [
        "",
        "## Decision for Phase 1 Pareto read-order",
        f"Emphasize first: **{emphasize}**",
        "",
        "P1 still sweeps both `qty_per_1btc_equity` and `max_concurrent`.",
        "",
        "## Next",
        "Phase 1: `greek_limits_mode=scale` + qty × concurrent grid.",
        "",
    ]
    out = write_phase_dir(
        "p0",
        summary_md="\n".join(lines),
        scorecard_rows=scorecard,
        meta={
            "phase": "p0",
            "date_range": list(ThetaEngineV10.DATE_RANGE),
            "account_size": CAPITAL,
            "param_grid": grid,
            "emphasize": emphasize,
        },
        extras={"breach_by_n_open.csv": breach_rows},
    )
    print(f"Wrote {out}")
    print(f"Emphasize: {emphasize}")


if __name__ == "__main__":
    main()

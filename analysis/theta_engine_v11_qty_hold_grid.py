#!/usr/bin/env python3
"""v11 accumulate discovery: wing budget × add pacing × instrument × entry.

72 combos (dg only — vega trigger is a later pass):

* wing_max_qty:          5 | 10 | 199     — 5 = current cap; 199 ≈ size-to-unbreach
* wing_min_hold_minutes: 0 | 60 | 240     — add immediately vs 1h vs 4h
* wing_delta:            0.10 | 0.20
* wing_expiry_mode:      same | next_listed
* entry_policy:          RichForce16 | Daily15

3*3*2*2*2 = 72.

Locked: sticky_budget, accumulate, trigger=dg, side=greek, delta_mode=fixed,
margin=3, cooldown=60, override=1.5, perp=0, SL=3, max_concurrent=20,
hold_days=0, greek_limits_mode=off.

Date range 2025-04-11 → 2026-08-12.  GUI-visible via run_backtest.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtester.core.config import cfg as _cfg
from backtester.core.paths import runs_dir
from backtester.run import run_backtest
from workspace.strategies.theta_engine._common import BASELINE_DAILY15, BASELINE_RICHFORCE16

_ENTRIES = [BASELINE_RICHFORCE16, BASELINE_DAILY15]
DATE_RANGE = ("2025-04-11", "2026-08-12")


def _grid(**overrides: Any) -> Dict[str, List]:
    g: Dict[str, List] = {
        "delta": [0.25],
        "min_dte": [90],
        "hold_days": [0],
        "stop_loss_pct": [3.0],
        "take_profit_pct": [0.50],
        "max_concurrent": [20],
        "qty_per_1btc_equity": [0.2],
        "launch_accel": [0],
        "launch_size_mult": [1.0],
        "greek_limits_mode": ["off"],
        "perp_delta_hedge": [0],
        "perp_deadband_pct": [2.0],
        "option_hedge_mode": ["sticky_budget"],
        "wing_expiry_mode": ["same", "next_listed"],
        "wing_delta": [0.10, 0.20],
        "wing_trigger": ["dg"],
        "wing_close_margin_pct": [3.0],
        "wing_min_hold_minutes": [0.0, 60.0, 240.0],
        "wing_cooldown_minutes": [60.0],
        "wing_cooldown_override_mult": [1.5],
        "wing_side_mode": ["greek"],
        "wing_delta_mode": ["fixed"],
        "wing_delta_ratio": [0.5],
        "wing_resize_mode": ["accumulate"],
        "wing_max_qty": [5.0, 10.0, 199.0],
        "entry_policy": list(_ENTRIES),
    }
    g.update(overrides)
    return g


def main() -> None:
    account = float(_cfg.simulation.account_size_usd)
    root = str(runs_dir())
    grid = _grid()
    n_combos = 1
    for v in grid.values():
        n_combos *= len(v)

    print(f"=== v11 qty/hold discovery {DATE_RANGE}: {n_combos} combos ===")
    bundle = run_backtest("theta_engine_v11", grid, DATE_RANGE, account, root, source="cli")
    print(f"Bundle: {bundle}")
    print("Open Research UI -> Runs and select the newest theta_engine_v11 bundle.")


if __name__ == "__main__":
    main()

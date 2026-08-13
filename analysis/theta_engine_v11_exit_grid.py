#!/usr/bin/env python3
"""Re-look at stop_loss_pct / max_concurrent / hold_days now that the v11
sticky-wing hedge actually works.

These three knobs were tuned once in v7 (stop_loss_pct, max_concurrent) or
never explored at all (hold_days), then carried forward unchanged through
v8-v11 while the hedge machinery around them was completely rebuilt. This
grid re-tests them with the v11-new hedge locked ON (validated defaults from
run 704: wing_close_margin_pct=3.0, wing_min_hold_minutes=60,
wing_cooldown_minutes=60, wing_side_mode=greek, wing_delta_mode=relative) to
see whether the hedged optimum has moved away from the old unhedged one.

Axes (3 values each, below/at/above current lock):
* stop_loss_pct: 1.5 / 3.0 (current) / 5.0
* max_concurrent: 15 / 20 (current) / 30
* hold_days: 0 (current) / 10 / 20
* entry_policy: RichForce16 / Daily15

3*3*3*2 = 54 combos.

Uses ``backtester.run.run_backtest`` so the artefact lands in ``data/runs/``
(.html + .bundle) and shows up in the Research UI Runs view.
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
from workspace.strategies.theta_engine.v11 import ThetaEngineV11

_ENTRIES = [BASELINE_RICHFORCE16, BASELINE_DAILY15]


def _grid(**overrides: Any) -> Dict[str, List]:
    g: Dict[str, List] = {
        "delta": [0.25],
        "min_dte": [90],
        "hold_days": [0.0, 10.0, 20.0],
        "stop_loss_pct": [1.5, 3.0, 5.0],
        "take_profit_pct": [0.50],
        "max_concurrent": [15, 20, 30],
        "qty_per_1btc_equity": [0.2],
        "launch_accel": [0],
        "launch_size_mult": [1.0],
        "greek_limits_mode": ["off"],
        "perp_delta_hedge": [0],
        "perp_deadband_pct": [2.0],
        "option_hedge_mode": ["sticky_budget"],
        "wing_expiry_mode": ["same"],
        "wing_delta": [0.10],
        "wing_trigger": ["dg"],
        "wing_close_margin_pct": [3.0],
        "wing_min_hold_minutes": [60.0],
        "wing_cooldown_minutes": [60.0],
        "wing_cooldown_override_mult": [1.5],
        "wing_side_mode": ["greek"],
        "wing_delta_mode": ["relative"],
        "wing_delta_ratio": [0.5],
        "entry_policy": list(_ENTRIES),
    }
    g.update(overrides)
    return g


def main() -> None:
    account = float(_cfg.simulation.account_size_usd)
    date_range = ThetaEngineV11.DATE_RANGE
    root = str(runs_dir())

    grid = _grid()
    n_combos = 1
    for v in grid.values():
        n_combos *= len(v)

    print(f"=== v11 exit-knob re-look (SL / max_concurrent / hold_days): {n_combos} combos ===")
    bundle = run_backtest("theta_engine_v11", grid, date_range, account, root, source="cli")
    print(f"Bundle: {bundle}")
    print("Open Research UI -> Runs and select the newest theta_engine_v11 bundle.")
    print("Sort by composite score to see whether 3.0 / 20 / 0 is still the peak.")


if __name__ == "__main__":
    main()

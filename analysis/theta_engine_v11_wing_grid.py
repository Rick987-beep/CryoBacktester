#!/usr/bin/env python3
"""Discovery grid over the new v11 wing knobs, both Mode C baselines.

Answers "what's best?" across the timing (hysteresis) and selection knobs
introduced in the v11 hedge redesign, run as ONE bundle so the Research UI
Runs view's composite-score heatmap ranks every cell directly.

Axes (compact resolution — 2 values each):
* wing_close_margin_pct: 0.0 (legacy, no margin) vs 3.0 (proposed default)
* wing_min_hold_minutes: 0 (legacy) vs 60 (proposed default)
* wing_cooldown_minutes: 0 (legacy) vs 60 (proposed default)
* wing_side_mode:  count (v10 leg-count heuristic) vs greek (net $-Greek driven)
* wing_delta_mode: fixed (absolute wing_delta) vs relative (anchor x ratio)
* entry_policy: RichForce16 vs Daily15 (the two Mode C baselines)

2*2*2*2*2*2 = 64 combos.

Held fixed (not part of the "new" v11 knobs, or secondary/tertiary knobs that
only bite in narrow sub-cases and would just multiply redundant cells):
* option_hedge_mode=sticky_budget, wing_trigger=dg, wing_expiry_mode=same,
  wing_delta=0.10, wing_cooldown_override_mult=1.5, wing_delta_ratio=0.5.

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
        "wing_expiry_mode": ["same"],
        "wing_delta": [0.10],
        "wing_trigger": ["dg"],
        "wing_close_margin_pct": [0.0, 3.0],
        "wing_min_hold_minutes": [0.0, 60.0],
        "wing_cooldown_minutes": [0.0, 60.0],
        "wing_cooldown_override_mult": [1.5],
        "wing_side_mode": ["count", "greek"],
        "wing_delta_mode": ["fixed", "relative"],
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

    print(f"=== v11 wing knob grid: {n_combos} combos ===")
    bundle = run_backtest("theta_engine_v11", grid, date_range, account, root, source="cli")
    print(f"Bundle: {bundle}")
    print("Open Research UI -> Runs and select the newest theta_engine_v11 bundle.")
    print("Sort by composite score to see the best margin/hold/cooldown/side/delta_mode cell.")


if __name__ == "__main__":
    main()

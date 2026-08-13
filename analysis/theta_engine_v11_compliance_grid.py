#!/usr/bin/env python3
"""Greek-compliance discovery grid: wing instrument + perp delta overlay.

Answers whether a better *compliance* recipe exists than the PnL-locked
v11-new defaults (trigger=dg, delta=0.10, expiry=same, perp off), before
those knobs get frozen into v12.

Axes:
* wing_trigger:       dg | dgv          — does targeting vega matter?
* wing_delta:         0.10 | 0.15 | 0.20 — how much hedge we buy
* wing_expiry_mode:   same | next_listed — longer-dated wing = more vega/contract
* perp_delta_hedge:   0 | 1             — cheap delta overlay (now blotter-logged)
* entry_policy:       RichForce16 | Daily15

2*3*2*2*2 = 48 combos.

Locked (already decided from run 704 / 706):
sticky_budget, wing_side_mode=greek, wing_delta_mode=fixed, margin=3,
hold/cooldown=60min, override=1.5, SL=3, max_concurrent=20, hold_days=0,
perp_deadband_pct=2.0.

Date range matches the latest full package (through 2026-08-12).

Uses ``backtester.run.run_backtest`` so the artefact lands in ``data/runs/``
and shows up in the Research UI Runs view.
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
        "perp_delta_hedge": [0, 1],
        "perp_deadband_pct": [2.0],
        "option_hedge_mode": ["sticky_budget"],
        "wing_expiry_mode": ["same", "next_listed"],
        "wing_delta": [0.10, 0.15, 0.20],
        "wing_trigger": ["dg", "dgv"],
        "wing_close_margin_pct": [3.0],
        "wing_min_hold_minutes": [60.0],
        "wing_cooldown_minutes": [60.0],
        "wing_cooldown_override_mult": [1.5],
        "wing_side_mode": ["greek"],
        "wing_delta_mode": ["fixed"],
        "wing_delta_ratio": [0.5],
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

    print(f"=== v11 compliance grid {DATE_RANGE}: {n_combos} combos ===")
    bundle = run_backtest("theta_engine_v11", grid, DATE_RANGE, account, root, source="cli")
    print(f"Bundle: {bundle}")
    print("Open Research UI -> Runs and select the newest theta_engine_v11 bundle.")


if __name__ == "__main__":
    main()

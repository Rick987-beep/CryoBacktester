#!/usr/bin/env python3
"""Re-run of the run-705 exit-knob grid with the date range extended to the
latest available market data (2026-08-12), same strategy/params otherwise.

Identical grid to ``theta_engine_v11_exit_grid.py`` (stop_loss_pct /
max_concurrent / hold_days re-look, v11-new hedge locked ON) — only the
DATE_RANGE end date changes, from 2026-08-01 to 2026-08-12.
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
    root = str(runs_dir())

    grid = _grid()
    n_combos = 1
    for v in grid.values():
        n_combos *= len(v)

    print(f"=== v11 exit-knob re-look, date range {DATE_RANGE}: {n_combos} combos ===")
    bundle = run_backtest("theta_engine_v11", grid, DATE_RANGE, account, root, source="cli")
    print(f"Bundle: {bundle}")
    print("Open Research UI -> Runs and select the newest theta_engine_v11 bundle.")


if __name__ == "__main__":
    main()

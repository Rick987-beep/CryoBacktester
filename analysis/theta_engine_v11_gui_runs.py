#!/usr/bin/env python3
"""Write GUI bundles for the v11 hedge redesign: naked / v10-legacy / v11-new.

Forked from ``theta_engine_v10_p3b_gui_runs.py``.  Three bundles per Mode C
baseline (RichForce16 / Daily15), so the Research UI Runs view can directly
compare:

* **naked**       — ``option_hedge_mode=none`` (no wing at all).
* **v10_legacy**  — sticky wing with all v11 timing/selection knobs pinned to
  their legacy-parity values (``wing_side_mode=count``, ``wing_delta_mode=
  fixed``, margin/hold/cooldown all ``0``) — i.e. v10's exact wing behavior,
  reproduced inside v11 for an apples-to-apples equity-curve/cost comparison.
* **v11_new**     — sticky wing with the new hysteresis + Greek-driven
  selection defaults (``wing_close_margin_pct=3``, ``wing_min_hold_minutes=
  60``, ``wing_cooldown_minutes=60``, ``wing_cooldown_override_mult=1.5``,
  ``wing_side_mode=greek``, ``wing_delta_mode=relative``,
  ``wing_delta_ratio=0.5``).

Uses ``backtester.run.run_backtest`` so artefacts land in ``data/runs/``
(.html + .bundle) and show up in the Research UI Runs view.
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


def _base_book_params(**overrides: Any) -> Dict[str, List]:
    """Mode C book skeleton for v11 (no v8/v9 hedge_delta/hedge_qty_mult leftovers)."""
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
        "option_hedge_mode": ["none"],
        "wing_expiry_mode": ["same"],
        "wing_delta": [0.10],
        "wing_trigger": ["dg"],
        "wing_close_margin_pct": [0.0],
        "wing_min_hold_minutes": [0.0],
        "wing_cooldown_minutes": [0.0],
        "wing_cooldown_override_mult": [1.5],
        "wing_side_mode": ["count"],
        "wing_delta_mode": ["fixed"],
        "wing_delta_ratio": [0.5],
        "entry_policy": list(_ENTRIES),
    }
    g.update(overrides)
    return g


def main() -> None:
    account = float(_cfg.simulation.account_size_usd)
    date_range = ThetaEngineV11.DATE_RANGE
    root = str(runs_dir())

    naked = _base_book_params(
        option_hedge_mode=["none"],
    )
    v10_legacy = _base_book_params(
        option_hedge_mode=["sticky_budget"],
        wing_trigger=["dg"],
        wing_delta=[0.10],
        wing_side_mode=["count"],
        wing_delta_mode=["fixed"],
        wing_close_margin_pct=[0.0],
        wing_min_hold_minutes=[0.0],
        wing_cooldown_minutes=[0.0],
    )
    v11_new = _base_book_params(
        option_hedge_mode=["sticky_budget"],
        wing_trigger=["dg"],
        wing_delta=[0.10],
        wing_side_mode=["greek"],
        wing_delta_mode=["relative"],
        wing_delta_ratio=[0.5],
        wing_close_margin_pct=[3.0],
        wing_min_hold_minutes=[60.0],
        wing_cooldown_minutes=[60.0],
        wing_cooldown_override_mult=[1.5],
    )

    print("=== v11 naked controls (2 combos) ===")
    b1 = run_backtest("theta_engine_v11", naked, date_range, account, root, source="cli")
    print(f"Naked bundle: {b1}")

    print("=== v11 v10-legacy sticky wing (2 combos) ===")
    b2 = run_backtest("theta_engine_v11", v10_legacy, date_range, account, root, source="cli")
    print(f"v10-legacy bundle: {b2}")

    print("=== v11 new hysteresis + Greek-driven wing (2 combos) ===")
    b3 = run_backtest("theta_engine_v11", v11_new, date_range, account, root, source="cli")
    print(f"v11-new bundle: {b3}")

    print("Open Research UI → Runs and select the newest theta_engine_v11 bundles.")


if __name__ == "__main__":
    main()

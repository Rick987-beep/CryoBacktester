#!/usr/bin/env python3
"""Write GUI bundles for P3b: naked Mode C controls + sticky wing grid.

Uses ``backtester.run.run_backtest`` so artefacts land in ``data/runs/``
(.html + .bundle) and show up in the Research UI Runs view.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtester.core.config import cfg as _cfg
from backtester.core.paths import runs_dir
from backtester.run import run_backtest
from analysis.theta_engine_v10_phase_lib import base_book_params
from workspace.strategies.theta_engine._common import BASELINE_DAILY15, BASELINE_RICHFORCE16
from workspace.strategies.theta_engine.v10 import ThetaEngineV10


def main() -> None:
    account = float(_cfg.simulation.account_size_usd)
    date_range = ThetaEngineV10.DATE_RANGE
    root = str(runs_dir())

    naked = base_book_params(
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

    print("=== P3b naked controls (2 combos) ===")
    b1 = run_backtest(
        "theta_engine_v10",
        naked,
        date_range,
        account,
        root,
        source="cli",
    )
    print(f"Naked bundle: {b1}")

    print("=== P3b sticky wings (24 combos) ===")
    b2 = run_backtest(
        "theta_engine_v10",
        sticky,
        date_range,
        account,
        root,
        source="cli",
    )
    print(f"Sticky bundle: {b2}")
    print("Open Research UI → Runs and select the newest theta_engine_v10 bundles.")


if __name__ == "__main__":
    main()

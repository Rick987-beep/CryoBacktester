#!/usr/bin/env python3
"""Run the 4 v8 entry favourites as a normal UI run and star them.

Combos (from staged research)::

    fav_sharpe_rich4_f5_1600  — Sharpe winner
    fav_pnl_daily_1500        — Total PnL winner (daily 15:00)
    fav_mid_rich4_f4_1600     — Near-Sharpe interior (force=4)
    fav_sched_mwf_1600        — Fixed Mon/Wed/Fri schedule

Usage::

    PYTHONPATH=. python analysis/theta_engine_v8_star_favourites.py
"""

from __future__ import annotations

import time
from pathlib import Path

from backtester.core.config import cfg as _cfg
from backtester.core.engine import run_grid_full
from backtester.core.market_replay import MarketReplay
from backtester.core.paths import runs_dir
from backtester.core.results import GridResult
from backtester.reporting.html_report import generate_html
from backtester.ui.services.store_service import StoreService
from workspace.catalog import family_for
from workspace.strategies.theta_engine.v8 import ThetaEngineV8

FAVOURITES = [
    {
        "entry_policy": "fav_sharpe_rich4_f5_1600",
        "name": "Sharpe winner",
        "note": "Sharpe winner — rich VRP≥4, force after 5d, 16:00 UTC",
    },
    {
        "entry_policy": "fav_pnl_daily_1500",
        "name": "PnL winner",
        "note": "PnL winner — daily Mon–Fri clock at 15:00 UTC",
    },
    {
        "entry_policy": "fav_mid_rich4_f4_1600",
        "name": "Near-Sharpe mid",
        "note": "Near-Sharpe interior — rich VRP≥4, force after 4d @ 16:00 (not force edge)",
    },
    {
        "entry_policy": "fav_sched_mwf_1600",
        "name": "MWF schedule",
        "note": "Fixed schedule — Mon/Wed/Fri only at 16:00 UTC",
    },
]


def main() -> int:
    policies = [f["entry_policy"] for f in FAVOURITES]
    param_grid = {
        "delta": [0.25],
        "min_dte": [90],
        "hedge_delta": [0.0],
        "hedge_qty_mult": [0],
        "hold_days": [0],
        "stop_loss_pct": [3.0],
        "take_profit_pct": [0.5],
        "max_concurrent": [20],
        "qty_per_1btc_equity": [0.2],
        "launch_accel": [0],
        "launch_size_mult": [1.0],
        "entry_policy": policies,
    }

    print("=" * 60)
    print("  theta_engine_v8 — star 4 entry favourites")
    print(f"  DATE_RANGE {ThetaEngineV8.DATE_RANGE[0]} → {ThetaEngineV8.DATE_RANGE[1]}")
    print(f"  policies: {policies}")
    print("=" * 60)

    t0 = time.time()
    replay = MarketReplay(
        _cfg.data.options_parquet,
        _cfg.data.spot_parquet,
        start=ThetaEngineV8.DATE_RANGE[0],
        end=ThetaEngineV8.DATE_RANGE[1],
    )
    print(f"  Data: {len(replay._timestamps):,} intervals")

    t1 = time.time()
    df, keys, nav_daily, final_nav, fills = run_grid_full(
        ThetaEngineV8, param_grid, replay
    )
    runtime = time.time() - t1
    account = float(_cfg.simulation.account_size_usd)
    result = GridResult(
        df,
        keys,
        nav_daily,
        final_nav,
        param_grid=param_grid,
        account_size=account,
        date_range=tuple(ThetaEngineV8.DATE_RANGE),
        df_fills=fills,
    )
    print(f"  {len(keys)} combos, {len(df)} trades in {runtime:.1f}s")

    out_dir = Path(runs_dir())
    out_dir.mkdir(parents=True, exist_ok=True)
    html = generate_html(
        strategy_name=ThetaEngineV8.name,
        result=result,
        n_intervals=len(replay._timestamps),
        runtime_s=runtime,
        strategy_description=(
            ThetaEngineV8.DESCRIPTION + " | UI favourites shortlist (4 combos)"
        ),
        robustness=True,
    )
    html_path = out_dir / "theta_engine_v8_entry_favourites.html"
    html_path.write_text(html)
    print(f"  Report: {html_path}")

    ui_state = Path(__file__).resolve().parents[1] / "backtester" / "ui" / "state"
    store = StoreService(ui_state, out_dir)
    bundle = store.write_bundle(
        result,
        strategy="theta_engine_v8",
        runtime_s=runtime,
        source="cli",
        strategy_cls=ThetaEngineV8,
        family=family_for("theta_engine_v8"),
    )
    run_id = store.register_bundle(bundle)
    store.set_label(run_id, "v8 entry favourites (Sharpe/PnL/mid/MWF)")
    store.set_pinned(run_id, True)
    print(f"  Bundle: {bundle}")
    print(f"  Run id: {run_id} (pinned)")

    note_by_policy = {f["entry_policy"]: f for f in FAVOURITES}
    for key in result.keys:
        params = dict(key)
        policy = str(params.get("entry_policy", ""))
        meta = note_by_policy.get(policy)
        if meta is None:
            continue
        stats = result.all_stats.get(key, {})
        if store.get_favourite_by_combo(run_id, key) is not None:
            print(f"  already starred: {policy}")
            continue
        fav_id = store.add_favourite(
            run_id=run_id,
            combo_key=key,
            name=meta["name"],
            note=meta["note"],
            score=float(result.scores.get(key, 0.0)),
            sharpe=float(stats.get("sharpe", 0.0)),
            total_pnl=float(stats.get("total_pnl", 0.0)),
            ann_return=float(stats.get("ann_return", 0.0))
            if stats.get("ann_return") is not None
            else None,
            params_str="  ".join(f"{k}={v}" for k, v in key),
            strategy="theta_engine_v8",
        )
        print(
            f"  ★ fav#{fav_id} {meta['name']}: sharpe={stats.get('sharpe', 0):.2f} "
            f"pnl={stats.get('total_pnl', 0):,.0f} | {meta['note']}"
        )

    print(f"  Total: {time.time() - t0:.1f}s")
    print("Open the UI → Favourites tab (or Runs → pinned 'v8 entry favourites').")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

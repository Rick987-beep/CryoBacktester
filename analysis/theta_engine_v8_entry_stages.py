#!/usr/bin/env python3
"""Staged entry-policy research for theta_engine_v8 (A → B → C1 → C2 → WFO).

Pre-registered look budgets (no post-hoc widening)::

    A   clock times           7
    B   weekday schedules     4   (clock locked from A)
    C1  vrp_min               7   (clock from A; force_after=3)
    C2  force_after_days      4   (vrp from C1)
    WFO candidate vs daily_1430 control (IS picks between 2)

Usage::

    python analysis/theta_engine_v8_entry_stages.py
    python analysis/theta_engine_v8_entry_stages.py --skip-wfo
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from backtester.core.config import cfg as _cfg
from backtester.core.engine import run_grid_full
from backtester.core.market_replay import MarketReplay
from backtester.core.paths import runs_dir
from backtester.core.results import GridResult
from backtester.reporting.html_report import generate_html
from backtester.research.experiment import load_experiment
from backtester.research.walk_forward import run_walk_forward
from workspace.strategies.theta_engine.v8 import (
    ENTRY_POLICIES,
    ENTRY_SCHEDULES,
    ThetaEngineV8,
    parse_entry_schedule,
)

_BOOK_KEYS = (
    "delta",
    "min_dte",
    "hedge_delta",
    "hedge_qty_mult",
    "hold_days",
    "stop_loss_pct",
    "take_profit_pct",
    "max_concurrent",
    "qty_per_1btc_equity",
    "launch_accel",
    "launch_size_mult",
)


def _runs() -> Path:
    return Path(runs_dir())


def _summary_rows(result: GridResult) -> pd.DataFrame:
    rows = []
    for rank, (key, stats) in enumerate(result.ranked, 1):
        params = dict(key)
        rows.append(
            {
                "rank": rank,
                "score": round(float(result.scores.get(key, 0.0)), 4),
                "n_trades": int(stats["n"]),
                "pnl_sum": round(float(stats["total_pnl"]), 1),
                "sharpe": round(float(stats["sharpe"]), 3),
                "max_dd": round(-abs(float(stats["max_dd_pct"])) / 100.0, 4),
                "win_rate": round(float(stats["win_rate"]), 3),
                **{k: params.get(k) for k in sorted(params)},
            }
        )
    return pd.DataFrame(rows)


def _write_stage(
    stem: str,
    result: GridResult,
    runtime_s: float,
    n_intervals: int,
    note: str,
) -> Path:
    out_dir = _runs()
    out_dir.mkdir(parents=True, exist_ok=True)
    html = generate_html(
        strategy_name=ThetaEngineV8.name,
        result=result,
        n_intervals=n_intervals,
        runtime_s=runtime_s,
        strategy_description=f"{ThetaEngineV8.DESCRIPTION} | {note}",
        robustness=True,
        status_labels=getattr(ThetaEngineV8, "TRADE_STATUS", None),
    )
    html_path = out_dir / f"{stem}.html"
    html_path.write_text(html)
    summary = _summary_rows(result)
    summary_path = out_dir / f"{stem}_summary.csv"
    summary.to_csv(summary_path, index=False)
    meta = {
        "stem": stem,
        "note": note,
        "date_range": list(ThetaEngineV8.DATE_RANGE),
        "best_params": result.best_params,
        "best_sharpe": float(result.best_stats["sharpe"]) if result.best_stats else None,
        "best_pnl": float(result.best_stats["total_pnl"]) if result.best_stats else None,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (out_dir / f"{stem}_meta.json").write_text(json.dumps(meta, indent=2, default=str))
    print(f"  Report: {html_path}")
    print(f"  Summary: {summary_path}")
    print(summary.head(min(8, len(summary))).to_string(index=False))
    return summary_path


def _run_grid(
    param_grid: Dict[str, List],
    replay: MarketReplay,
    stem: str,
    note: str,
) -> Tuple[GridResult, Dict[str, Any]]:
    t0 = time.time()
    df, keys, nav_daily, final_nav, fills = run_grid_full(
        ThetaEngineV8, param_grid, replay
    )
    runtime = time.time() - t0
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
    _write_stage(stem, result, runtime, len(replay._timestamps), note)
    return result, dict(result.best_params)


def _patch_exp_best(name: str, updates: Dict[str, Any]) -> Dict[str, List]:
    exp = load_experiment(name)
    best = dict(exp.sensitivity_best)
    best.update(updates)
    exp.sensitivity_best = best
    grid = exp.build_sensitivity_grid()
    print(exp.describe())
    return grid


def _candidate_policy(params: Dict[str, Any]) -> str:
    """Register Stage-C winner as a named policy for the 2-way WFO shortlist."""
    name = "stage_c_winner"
    force = parse_entry_schedule(params.get("force_schedule", "mon_thu"))
    ENTRY_POLICIES[name] = {
        "entry_mode": str(params.get("entry_mode", "rich_or_forced")),
        "entry_time": str(params.get("entry_time", "14:30")),
        "vrp_min": float(params.get("vrp_min", 3.0)),
        "force_after_days": int(params.get("force_after_days", 3)),
        "force_weekdays": force,
        "entry_days": ENTRY_SCHEDULES["mon_fri"],
    }
    return name


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-wfo", action="store_true")
    parser.add_argument(
        "--options",
        default=_cfg.data.options_parquet,
        help="Options parquet path/dir",
    )
    parser.add_argument(
        "--spot",
        default=_cfg.data.spot_parquet,
        help="Spot parquet path/dir",
    )
    args = parser.parse_args(argv)

    print("=" * 60)
    print("  theta_engine_v8 staged entry research")
    print(f"  DATE_RANGE {ThetaEngineV8.DATE_RANGE[0]} → {ThetaEngineV8.DATE_RANGE[1]}")
    print("=" * 60)

    t_load = time.time()
    replay = MarketReplay(
        args.options,
        args.spot,
        start=ThetaEngineV8.DATE_RANGE[0],
        end=ThetaEngineV8.DATE_RANGE[1],
    )
    print(f"  Data loaded: {len(replay._timestamps):,} intervals in {time.time()-t_load:.1f}s")

    chain: Dict[str, Any] = {}

    # ── Stage A ───────────────────────────────────────────────────
    print("\n── Stage A: clock sensitivity ──")
    grid_a = _patch_exp_best("theta_engine_v8_entry_stage_a", {})
    _, best_a = _run_grid(
        grid_a,
        replay,
        "theta_engine_v8_entry_stage_a",
        "Stage A clock sweep (daily mon_fri)",
    )
    clock = str(best_a["entry_time"])
    chain["stage_a_entry_time"] = clock
    print(f"  → lock entry_time={clock}")

    # ── Stage B ───────────────────────────────────────────────────
    print("\n── Stage B: schedule sensitivity ──")
    grid_b = _patch_exp_best(
        "theta_engine_v8_entry_stage_b",
        {"entry_time": clock},
    )
    _, best_b = _run_grid(
        grid_b,
        replay,
        "theta_engine_v8_entry_stage_b",
        f"Stage B schedules (entry_time={clock})",
    )
    schedule = str(best_b["entry_schedule"])
    chain["stage_b_entry_schedule"] = schedule
    print(f"  → Stage B winner schedule={schedule} (informational; C keeps mon_fri rich + mon_thu force)")

    # ── Stage C1 ──────────────────────────────────────────────────
    print("\n── Stage C1: vrp_min sensitivity ──")
    grid_c1 = _patch_exp_best(
        "theta_engine_v8_entry_stage_c_vrp",
        {"entry_time": clock},
    )
    _, best_c1 = _run_grid(
        grid_c1,
        replay,
        "theta_engine_v8_entry_stage_c_vrp",
        f"Stage C1 VRP sweep (entry_time={clock}, force_after=3)",
    )
    vrp = float(best_c1["vrp_min"])
    chain["stage_c1_vrp_min"] = vrp
    print(f"  → lock vrp_min={vrp}")

    # ── Stage C2 ──────────────────────────────────────────────────
    print("\n── Stage C2: force_after_days sensitivity ──")
    grid_c2 = _patch_exp_best(
        "theta_engine_v8_entry_stage_c_force",
        {"entry_time": clock, "vrp_min": vrp},
    )
    _, best_c2 = _run_grid(
        grid_c2,
        replay,
        "theta_engine_v8_entry_stage_c_force",
        f"Stage C2 force gap (entry_time={clock}, vrp_min={vrp})",
    )
    force_after = int(best_c2["force_after_days"])
    chain["stage_c2_force_after_days"] = force_after
    print(f"  → Stage C winner: time={clock} vrp_min={vrp} force_after={force_after}")

    candidate = {
        "entry_mode": "rich_or_forced",
        "entry_time": clock,
        "entry_schedule": "mon_fri",
        "vrp_min": vrp,
        "force_after_days": force_after,
        "force_schedule": "mon_thu",
    }
    for k in _BOOK_KEYS:
        candidate[k] = best_c2[k]
    chain["candidate"] = candidate

    # ── WFO ───────────────────────────────────────────────────────
    if not args.skip_wfo:
        print("\n── WFO: candidate vs daily_1430 ──")
        pol = _candidate_policy(candidate)
        exp_wfo = load_experiment("theta_engine_v8_entry_wfo")
        wfo_grid = {
            k: [candidate[k]] for k in _BOOK_KEYS
        }
        wfo_grid["entry_policy"] = ["daily_1430", pol]
        print(
            f"  Shortlist entry_policy={wfo_grid['entry_policy']}  "
            f"IS={exp_wfo.wfo_is_days}d OOS={exp_wfo.wfo_oos_days}d "
            f"step={exp_wfo.wfo_step_days}d"
        )
        wfo = run_walk_forward(
            strategy_cls=ThetaEngineV8,
            options_path=args.options,
            spot_path=args.spot,
            is_days=exp_wfo.wfo_is_days,
            oos_days=exp_wfo.wfo_oos_days,
            step_days=exp_wfo.wfo_step_days,
            account_size=float(_cfg.simulation.account_size_usd),
            param_grid=wfo_grid,
        )
        wfo_rows = [
            {
                "window": w.idx,
                "is": f"{w.is_start}:{w.is_end}",
                "oos": f"{w.oos_start}:{w.oos_end}",
                "is_best": w.best_params.get("entry_policy"),
                "is_pnl": round(w.is_pnl, 1),
                "is_sharpe": round(w.is_sharpe, 3),
                "oos_pnl": round(w.oos_pnl, 1),
                "oos_sharpe": round(w.oos_sharpe, 3),
                "oos_win": w.oos_win,
                "oos_n_trades": w.oos_n_trades,
            }
            for w in wfo.windows
        ]
        wfo_df = pd.DataFrame(wfo_rows)
        wfo_path = _runs() / "theta_engine_v8_entry_wfo_windows.csv"
        wfo_df.to_csv(wfo_path, index=False)
        chain["wfo"] = {
            "oos_win_rate": wfo.oos_win_rate,
            "oos_total_pnl": wfo.oos_total_pnl,
            "oos_avg_sharpe": wfo.oos_avg_sharpe,
            "n_windows": len(wfo.windows),
            "is_days": wfo.is_days,
            "oos_days": wfo.oos_days,
            "step_days": wfo.step_days,
            "shortlist": wfo_grid["entry_policy"],
            "candidate_policy": pol,
            "candidate_knobs": {
                "entry_time": clock,
                "vrp_min": vrp,
                "force_after_days": force_after,
            },
        }
        print(f"  WFO windows: {wfo_path}")
        print(
            f"  OOS win rate={wfo.oos_win_rate:.0%}  "
            f"OOS PnL={wfo.oos_total_pnl:+,.0f}  "
            f"avg Sharpe={wfo.oos_avg_sharpe:.2f}"
        )
        print(wfo_df.to_string(index=False))

    chain_path = _runs() / "theta_engine_v8_entry_stages_chain.json"
    chain_path.write_text(json.dumps(chain, indent=2, default=str))
    print(f"\n  Chain log: {chain_path}")
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

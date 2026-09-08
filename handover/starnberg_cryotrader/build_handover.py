#!/usr/bin/env python3
"""Regenerate Starnberg → CryoTrader handover exports.

Run from CryoBacktester repo root:
    python handover/starnberg_cryotrader/build_handover.py

Uses ``python -m backtester.inspect`` resolution (no StoreService.load_run).
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtester.inspect.load import read_fills, read_nav, read_trades
from backtester.inspect.resolve import default_store, resolve_combo, resolve_run

OUT = Path(__file__).resolve().parent

# ── Live lock (authoritative) ─────────────────────────────────────────────────

SLOT = "starnberg"
COMBO_HASH = "a1d621a81904"
UI_RUN_ID = 746
BUNDLE = "theta_engine_v18_20260824_143142.bundle"

# Engine-lock metrics from ``inspect combo 746 a1d621a81904 --full``.
ENGINE_LOCK = {
    "sharpe": 4.018637180328369,
    "sortino": 5.0611482530577465,
    "calmar": 7.510489397963374,
    "max_dd_pct": 6.568584442138672,
    "ann_return": 0.4933328628540039,
    "omega": 2.3267910480499268,
}

STOP_BOOK_FULL2_EQ8 = {
    "display": "Full2Eq8",
    "prox_stop": "2@16",
    "prox_stop_pct": 2.0,
    "prox_hours": 16.0,
    "open_pnl_equity_stop_pct": 8.0,
    "stop_loss_pct": 0.0,
}

REFERENCE_SOURCES = (
    ROOT / "workspace/strategies/theta_engine/v18.py",
    ROOT / "workspace/strategies/theta_engine/v17.py",
    ROOT / "workspace/strategies/theta_engine/v14.py",
    ROOT / "workspace/strategies/theta_engine/_common.py",
    ROOT / "workspace/tests/test_theta_engine_v18.py",
    ROOT / "backtester/core/expiry_utils.py",
)


def _live_params(grid_params: dict, meta: dict, bundle_name: str) -> dict:
    stop = STOP_BOOK_FULL2_EQ8
    live = {
        "slot_id": SLOT,
        "product_name": "Starnberg",
        "strategy_id_backtester": "theta_engine_v18",
        "strategy_name_live_suggested": "starnberg",
        "combo_hash": COMBO_HASH,
        "ui_run_id": UI_RUN_ID,
        "run_bundle": bundle_name,
        "date_range": list(meta.get("date_range") or ["2025-08-18", "2026-08-22"]),
        "capital_usd": float(meta.get("account_size") or 100_000),
        **dict(grid_params),
        "entry_policy": "rich_force_2d_1600",
        "entry_policy_display": "RichForce2",
        "entry_time_utc": "16:00",
        "entry_hour_utc": 16,
        "entry_minute_utc": 0,
        "vrp_min": 4.0,
        "force_after_days": 2,
        "force_weekdays": [0, 1, 2, 3, 4],
        "entry_days": [0, 1, 2, 3, 4],
        "stop_display": stop["display"],
        "prox_stop": stop["prox_stop"],
        "prox_stop_pct": stop["prox_stop_pct"],
        "prox_hours": stop["prox_hours"],
        "open_pnl_equity_stop_pct": stop["open_pnl_equity_stop_pct"],
        "take_profit_pct": 0.0,
        "always_hedged": True,
        "wing_equal_qty": True,
        "wing_same_expiry": True,
        "notes": (
            "Marketing lock Starnberg. Always attach 1:1 further-OTM wing. "
            "Entry 16:00 UTC. Weekend cover=skip."
        ),
    }
    return live


def export_slot() -> dict:
    store = default_store()
    run = resolve_run(store, str(UI_RUN_ID))
    combo = resolve_combo(run, COMBO_HASH)
    idx = combo.combo_idx
    params = dict(combo.params)
    live = _live_params(params, run.meta, run.bundle_name)

    trades = read_trades(run, [idx]).copy()
    nav = read_nav(run, [idx]).copy()
    fills = read_fills(run, [idx]).copy()

    pnls = trades["pnl"].astype(float)
    winners = pnls[pnls > 0]
    losers = pnls[pnls < 0]
    capital = float(live["capital_usd"])

    stats = {
        "slot_id": SLOT,
        "label": "Starnberg — RichForce2 16 front + Full2Eq8 + wing 7%",
        "product_name": "Starnberg",
        "combo_hash": COMBO_HASH,
        "combo_idx": idx,
        "ui_run_id": UI_RUN_ID,
        "run_bundle": run.bundle_name,
        "strategy": "theta_engine_v18",
        "date_range": live["date_range"],
        "capital_usd": capital,
        "params_full_grid": params,
        "params_for_live": live,
        "n_trades": int(len(trades)),
        "total_pnl_usd": float(pnls.sum()),
        "total_return_pct": float(pnls.sum() / capital * 100),
        **ENGINE_LOCK,
        "win_rate_pct": float((pnls > 0).mean() * 100),
        "profit_factor": (
            float(winners.sum() / abs(losers.sum()))
            if len(losers) and float(losers.sum()) != 0
            else None
        ),
        "median_pnl_usd": float(pnls.median()),
        "max_win_usd": float(pnls.max()),
        "max_loss_usd": float(pnls.min()),
        "fees_usd": float(trades["fees"].sum()) if "fees" in trades.columns else None,
        "exit_reasons": {
            str(k): int(v) for k, v in trades["exit_reason"].value_counts().items()
        },
        "favourite_note": (
            "1DTE perfection: with long wings, high sharpe, "
            "90% win rate but not higher"
        ),
        "backtest_caveat": (
            "Single combo from run 746. Header Sharpe/Sortino/Calmar/DD from "
            "inspect --full. 180 expiry / 2 strike_proximity_stop. "
            "Expiry includes losers."
        ),
        "marketing_report": (
            "workspace/marketing/ship/starnberg/starnberg_strategyreport_082026.html"
        ),
    }

    params_dir = OUT / "params"
    bt_dir = OUT / "backtests"
    params_dir.mkdir(exist_ok=True)
    bt_dir.mkdir(exist_ok=True)

    (params_dir / f"{SLOT}.json").write_text(
        json.dumps(live, indent=2, sort_keys=True) + "\n"
    )
    (bt_dir / f"stats_{SLOT}.json").write_text(json.dumps(stats, indent=2) + "\n")
    trades.to_csv(bt_dir / f"trades_{SLOT}.csv", index=False)
    nav.to_csv(bt_dir / f"nav_daily_{SLOT}.csv", index=False)
    if len(fills):
        fills.to_csv(bt_dir / f"fills_{SLOT}.csv", index=False)

    return stats


def copy_reference() -> list[str]:
    ref_dir = OUT / "reference"
    ref_dir.mkdir(exist_ok=True)
    copied: list[str] = []
    for src in REFERENCE_SOURCES:
        if src.exists():
            shutil.copy2(src, ref_dir / src.name)
            copied.append(src.relative_to(ROOT).as_posix())
    return copied


def write_schedule() -> None:
    schedule = {
        "product_name": "Starnberg",
        "strategy_name_live": "starnberg",
        "strategy_id_backtester": "theta_engine_v18",
        "implementation_target": (
            "CryoTrader — greenfield strategy module (no existing theta live "
            "strategy). Reuse execution / option-selection helpers from "
            "CryoTrader; behavior from reference/v18.py (+ v17/v14/_common)."
        ),
        "entry_time_semantics": (
            "UTC wall-clock. entry_time_utc=16:00 (same as CryoTrader system time). "
            "See TIMEZONE.md."
        ),
        "system_time_semantics": (
            "CryoTrader loop, logs, and Deribit timestamps run in UTC."
        ),
        "position_rules": {
            "max_concurrent_positions": 1,
            "max_entries_per_utc_day": 1,
            "always_hedged": True,
            "weekend_cover": "skip",
        },
        "position_rules_doc": "POSITION_RULES.md",
        "slots": {
            SLOT: {
                "combo_hash": COMBO_HASH,
                "param_file": f"params/{SLOT}.json",
                "label": "Starnberg (RichForce2 16 front + Full2Eq8 + wing)",
                "reserved": False,
                "live_calendar": {
                    "weekdays_utc": [0, 1, 2, 3, 4],
                    "entry_time_utc": "16:00",
                    "dte": 1,
                    "expiry_note": "Next daily Deribit expiry (~08:00 UTC)",
                },
            }
        },
        "locked_combo": {
            "ui_run_id": UI_RUN_ID,
            "bundle": BUNDLE,
            "combo_hash": COMBO_HASH,
            "book": "rf2_1600",
            "stop_book": "full2_eq8",
            "wing_pct": 0.07,
            "min_width_usd": 2500.0,
        },
        "ignore_grid_params": [
            "alpha_rank",
            "alpha_vrp",
            "vrp_lo",
            "vrp_hi",
            "rank_lo",
            "rank_hi",
            "f_min",
            "rich_mode",
        ],
        "ignore_grid_params_note": (
            "rich_mode discovery closed (run 741). Live uses rich_mode=none; "
            "alpha_* / vrp_* / rank_* / f_min are inert on that path."
        ),
    }
    (OUT / "LIVE_PARAM_SCHEDULE.json").write_text(
        json.dumps(schedule, indent=2) + "\n"
    )


def main() -> None:
    copied = copy_reference()
    print(f"reference copies: {len(copied)}")
    stats = export_slot()
    write_schedule()
    summary = {
        "slots": {SLOT: stats},
        "unique_combos": {COMBO_HASH: [SLOT]},
        "product_name": "Starnberg",
        "marketing_report": stats["marketing_report"],
    }
    (OUT / "backtests" / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n"
    )
    print(
        f"  {SLOT}: {COMBO_HASH} — {stats['n_trades']} trades, "
        f"Sharpe {stats['sharpe']:.2f}, WR {stats['win_rate_pct']:.1f}%"
    )
    print(f"\nHandover package updated: {OUT}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Regenerate Monopteros → CryoTrader handover exports.

Run from CryoBacktester repo root:
    python handover/monopteros_cryotrader/build_handover.py

Uses ``python -m backtester.inspect`` resolution (no StoreService.load_run).
"""
from __future__ import annotations

import json
import shutil
import sys
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtester.inspect.load import read_fills, read_nav, read_trades
from backtester.inspect.resolve import default_store, resolve_combo, resolve_run
from workspace.strategies.tudysho.monopteros import (
    RELEASE_NAME,
    _LIVE_SLOT_REF,
    _SCHEDULE_DEFAULTS,
    _SCHEDULE_IDS,
)

OUT = Path(__file__).resolve().parent

# ── Live lock (authoritative) ─────────────────────────────────────────────────

SLOT = "monopteros"
COMBO_HASH = "7e21b64f1d94"
UI_RUN_ID = 770
BUNDLE = "tudysho_monopteros_20260904_105017.bundle"

# Engine-lock metrics from ``inspect combo 770 7e21b64f1d94 --full``.
ENGINE_LOCK = {
    "sharpe": 4.100724697113037,
    "sortino": 4.7197069659719855,
    "calmar": 6.3733500796569516,
    "max_dd_pct": 11.349560737609863,
    "ann_return": 0.7233473062515259,
    "omega": 2.909510612487793,
}

REFERENCE_SOURCES = (
    ROOT / "workspace/strategies/tudysho/monopteros.py",
    ROOT / "workspace/tests/test_tudysho_monopteros.py",
    ROOT / "backtester/core/market_hours.py",
    ROOT / "backtester/core/expiry_utils.py",
)

SCHEDULE_CALENDAR = {
    "mon_early": {
        "weekdays_nyc": [0],
        "entry_time_nyc": "00:05",
        "dte": 0,
        "expiry_note": "Monday entry → Monday 08:00 UTC expiry (dte=0)",
    },
    "mon_thu": {
        "weekdays_nyc": [0, 1, 2, 3],
        "entry_time_nyc": "14:00",
        "dte": 1,
        "expiry_note": "Next daily Deribit expiry (~08:00 UTC)",
    },
    "fri": {
        "weekdays_nyc": [4],
        "entry_time_nyc": "12:30",
        "dte": 1,
        "expiry_note": "Friday entry → Saturday 08:00 UTC expiry",
    },
}


def _live_params(grid_params: dict, meta: dict, bundle_name: str) -> dict:
    schedules = deepcopy(_SCHEDULE_DEFAULTS)
    for sid, cal in SCHEDULE_CALENDAR.items():
        schedules[sid] = {**schedules[sid], **cal}

    return {
        "slot_id": SLOT,
        "product_name": RELEASE_NAME,
        "strategy_id_backtester": "tudysho_monopteros",
        "strategy_name_live_suggested": "monopteros",
        "combo_hash": COMBO_HASH,
        "ui_run_id": UI_RUN_ID,
        "run_bundle": bundle_name,
        "date_range": list(
            meta.get("date_range") or [None, None]
        ) or ["2025-04-11", "2026-08-31"],
        "capital_usd": float(meta.get("account_size") or 100_000),
        **dict(grid_params),
        "structure": "naked_strangle",
        "take_profit_pct": 0.0,
        "max_concurrent_positions": 1,
        "schedules": schedules,
        "schedule_ids": list(_SCHEDULE_IDS),
        "schedule_source": _LIVE_SLOT_REF,
        "notes": (
            "Marketing lock Monopteros (Aug-2026). One strategy with three NYC "
            "schedules; global sizing 0.5% NAV / max 6 contracts; equity DD stop "
            "5%. Naked shorts (no wings)."
        ),
    }


def export_slot() -> dict:
    store = default_store()
    run = resolve_run(store, str(UI_RUN_ID))
    combo = resolve_combo(run, COMBO_HASH)
    idx = combo.combo_idx
    params = dict(combo.params)
    date_range = [run.date_from, run.date_to]
    meta = dict(run.meta)
    meta.setdefault("date_range", date_range)
    meta.setdefault("account_size", 100_000)
    live = _live_params(params, meta, run.bundle_name)
    live["date_range"] = date_range

    trades = read_trades(run, [idx]).copy()
    nav = read_nav(run, [idx]).copy()
    fills = read_fills(run, [idx]).copy()

    pnls = trades["pnl"].astype(float)
    winners = pnls[pnls > 0]
    losers = pnls[pnls < 0]
    capital = float(live["capital_usd"])

    stats = {
        "slot_id": SLOT,
        "label": (
            "Monopteros — three NYC schedules, 0.5% NAV, equity DD 5%"
        ),
        "product_name": RELEASE_NAME,
        "combo_hash": COMBO_HASH,
        "combo_idx": idx,
        "ui_run_id": UI_RUN_ID,
        "run_bundle": run.bundle_name,
        "strategy": "tudysho_monopteros",
        "date_range": date_range,
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
            "Monopteros master: mon_thu=16dcb4f42c9a (768), "
            "mon_early=b012cb51c397 (764), fri=fd39a836394d (766); "
            "nav=0.5% qty=6 eqDD=5%; window 2025-04-11→2026-08-31."
        ),
        "backtest_caveat": (
            "Single master combo from run 770. Three schedules share one "
            "equity curve. Equity drawdown stop is on every ticket but did "
            "not fire in this sample (376 expiry / 4 proximity / 2 premium SL)."
        ),
        "marketing_report": (
            "workspace/marketing/ship/monopteros/monopteros_strategyreport_082026.html"
        ),
        "schedule_source_combos": {
            "mon_thu": {"combo_hash": "16dcb4f42c9a", "ui_run_id": 768},
            "mon_early": {"combo_hash": "b012cb51c397", "ui_run_id": 764},
            "fri": {"combo_hash": "fd39a836394d", "ui_run_id": 766},
        },
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
        "product_name": RELEASE_NAME,
        "strategy_name_live": "monopteros",
        "strategy_id_backtester": "tudysho_monopteros",
        "implementation_target": (
            "CryoTrader — one strategy module with three NYC schedule windows. "
            "Prefer forking / replacing prior tudysho multi-slot wiring; "
            "behavior from reference/monopteros.py."
        ),
        "entry_time_semantics": (
            "NYC wall-clock (America/New_York, DST-aware). Convert via "
            "to_nyc / to_utc before comparing to system UTC. See TIMEZONE.md."
        ),
        "system_time_semantics": (
            "CryoTrader loop, logs, and Deribit timestamps run in UTC."
        ),
        "position_rules": {
            "max_concurrent_positions": 1,
            "max_entries_per_schedule_per_nyc_day": 1,
            "monday_two_schedules": True,
            "structure": "naked_strangle",
        },
        "position_rules_doc": "POSITION_RULES.md",
        "slots": {
            SLOT: {
                "combo_hash": COMBO_HASH,
                "param_file": f"params/{SLOT}.json",
                "label": "Monopteros (three NYC schedules, one equity curve)",
                "reserved": False,
                "schedules": {
                    sid: {
                        "label": _SCHEDULE_DEFAULTS[sid]["label"],
                        "live_calendar": SCHEDULE_CALENDAR[sid],
                        "trade_params": {
                            k: v
                            for k, v in _SCHEDULE_DEFAULTS[sid].items()
                            if k != "label"
                        },
                    }
                    for sid in _SCHEDULE_IDS
                },
            }
        },
        "locked_combo": {
            "ui_run_id": UI_RUN_ID,
            "bundle": BUNDLE,
            "combo_hash": COMBO_HASH,
            "nav_premium_pct": 0.5,
            "max_qty_per_1btc_equity": 6.0,
            "equity_drawdown_stop_pct": 5.0,
        },
        "schedule_source_combos": {
            "mon_thu": {"combo_hash": "16dcb4f42c9a", "ui_run_id": 768},
            "mon_early": {"combo_hash": "b012cb51c397", "ui_run_id": 764},
            "fri": {"combo_hash": "fd39a836394d", "ui_run_id": 766},
        },
        "ignore_grid_params": [
            "leg_min_price",
            "equity_sl_only_final_hours",
            "equity_sl_except_final_hours",
        ],
        "ignore_grid_params_note": (
            "leg_min_price=0 (off). equity_sl_* hour windows are 0 on this lock; "
            "equity_drawdown_stop_pct=5 applies for the full hold."
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
        "product_name": RELEASE_NAME,
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

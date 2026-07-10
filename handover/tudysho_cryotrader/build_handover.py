#!/usr/bin/env python3
"""Regenerate tudysho CryoTrader handover exports from backtest bundles.

Run from CryoBacktester repo root:
    python handover/tudysho_cryotrader/build_handover.py
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtester.results import equity_metrics
from backtester.ui.services.store_service import key_hash

OUT = Path(__file__).resolve().parent

# ── Live schedule (authoritative) ─────────────────────────────────────────────

SLOTS = {
    "slot_a_mon_thu": {
        "combo_hash": "5cd986cf48cd",
        "bundle": ROOT / "backtester/reports/tudysho_20260706_084941.bundle",
        "ui_run_id": 296,
        "label": "Mon–Thu daytime (next-day expiry)",
        "live_calendar": {
            "weekdays": [0, 1, 2, 3],
            "entry_time_nyc": "16:00",
            "dte": 1,
            "expiry_note": "Entry Mon→Tue 08:00 UTC expiry; Tue→Wed; … Thu→Fri",
        },
        "backtest_caveat": (
            "Mon–Thu grid (trade_mon–thu=1, trade_friday=0). "
            "143 trades across Mon/Tue/Wed/Thu in backtest."
        ),
    },
    "slot_b_mon_early": {
        "combo_hash": "e2f4ac2b3e69",
        "bundle": ROOT / "backtester/reports/tudysho_20260706_140834.bundle",
        "ui_run_id": 306,
        "label": "Monday early hours (same-day expiry)",
        "live_calendar": {
            "weekdays": [0],
            "entry_time_nyc": "1:00",
            "dte": 0,
            "expiry_note": "Monday entry → Monday 08:00 UTC expiry (dte=0)",
        },
        "backtest_caveat": (
            "Monday-only grid (trade_monday=1), entry 01:00 NYC, dte=0. "
            "Does not include Sunday-night entries; see TIMEZONE.md."
        ),
    },
    "slot_c_fri_sat": {
        "combo_hash": "829e7226cc48",
        "bundle": ROOT / "backtester/reports/tudysho_20260706_081343.bundle",
        "ui_run_id": 295,
        "label": "Friday daytime (Saturday morning expiry)",
        "live_calendar": {
            "weekdays": [4],
            "entry_time_nyc": "12:00",
            "dte": 1,
            "expiry_note": "Friday entry → Saturday 08:00 UTC expiry",
        },
        "backtest_caveat": "Friday-only grid (trade_friday=1). Matches live Friday slot.",
    },
    "slot_d_saturday": {
        "combo_hash": None,
        "bundle": None,
        "ui_run_id": None,
        "label": "Saturday (reserved)",
        "live_calendar": None,
        "backtest_caveat": "Placeholder — combo TBD.",
        "reserved": True,
    },
}


def _normalize_keys(meta: dict) -> list[tuple]:
    keys = []
    for key in meta["keys"]:
        if isinstance(key, list) and key and isinstance(key[0], list):
            keys.append(tuple((p[0], p[1]) for p in key))
        else:
            names = sorted(meta["param_grid"].keys())
            keys.append(tuple((n, key[i]) for i, n in enumerate(names)))
    return keys


def export_slot(slot_id: str, cfg: dict) -> dict | None:
    if cfg.get("reserved"):
        return None

    bundle = cfg["bundle"]
    combo_hash = cfg["combo_hash"]
    meta = json.loads((bundle / "meta.json").read_text())
    trades = pd.read_parquet(bundle / "trade_log.parquet")
    nav = pd.read_parquet(bundle / "nav_daily.parquet")
    fills_path = bundle / "fills.parquet"

    keys = _normalize_keys(meta)
    idx = next(i for i, k in enumerate(keys) if key_hash(k) == combo_hash)
    params = dict(keys[idx])
    capital = float(meta["account_size"])
    d_from, d_to = meta["date_range"]

    df = trades[trades.combo_idx == idx].copy()
    nav_c = nav[nav.combo_idx == idx].copy()
    eq = equity_metrics(
        df, capital=capital, nav_daily_combo=nav_c,
        date_from=d_from, date_to=d_to,
    )

    live_params = {k: v for k, v in params.items() if not k.startswith("trade_")}
    live_params["slot_id"] = slot_id
    live_params["combo_hash"] = combo_hash
    if cfg.get("live_calendar"):
        live_params.update(cfg["live_calendar"])

    stats = {
        "slot_id": slot_id,
        "label": cfg["label"],
        "combo_hash": combo_hash,
        "combo_idx": idx,
        "ui_run_id": cfg["ui_run_id"],
        "run_bundle": str(bundle.relative_to(ROOT)),
        "date_range": meta["date_range"],
        "capital_usd": capital,
        "params_full_grid": params,
        "params_for_live": live_params,
        "n_trades": int(len(df)),
        "total_pnl_usd": float(df.pnl.sum()),
        "total_return_pct": float(eq["total_pnl"] / capital * 100),
        "sharpe": float(eq["sharpe"]),
        "max_dd_pct": float(eq["max_dd_pct"]),
        "win_rate_pct": float((df.pnl > 0).mean() * 100),
        "profit_factor": float(eq.get("profit_factor", 0)),
        "exit_reasons": {k: int(v) for k, v in df.exit_reason.value_counts().items()},
        "trades_by_entry_weekday": {
            k: int(v)
            for k, v in pd.to_datetime(df.entry_date).dt.day_name().value_counts().items()
        },
        "backtest_caveat": cfg["backtest_caveat"],
    }

    params_dir = OUT / "params"
    bt_dir = OUT / "backtests"
    params_dir.mkdir(exist_ok=True)
    bt_dir.mkdir(exist_ok=True)

    (params_dir / f"{slot_id}.json").write_text(
        json.dumps(live_params, indent=2, sort_keys=True) + "\n"
    )
    (bt_dir / f"stats_{slot_id}.json").write_text(json.dumps(stats, indent=2) + "\n")
    df.to_csv(bt_dir / f"trades_{slot_id}.csv", index=False)
    nav_c.to_csv(bt_dir / f"nav_daily_{slot_id}.csv", index=False)

    if fills_path.exists():
        fills = pd.read_parquet(fills_path)
        if "combo_idx" in fills.columns:
            fills[fills.combo_idx == idx].to_csv(
                bt_dir / f"fills_{slot_id}.csv", index=False
            )

    return stats


def main() -> None:
    ref_dir = OUT / "reference"
    ref_dir.mkdir(exist_ok=True)
    copied = []
    for src in (
        ROOT / "backtester/strategies/tudysho.py",
        ROOT / "backtester/strategies/tests/test_tudysho.py",
        ROOT / "market_hours.py",
        ROOT / "backtester/expiry_utils.py",
    ):
        if src.exists():
            shutil.copy2(src, ref_dir / src.name)
            copied.append(src.relative_to(ROOT).as_posix())

    source_note = OUT / "reference" / "SOURCE.md"
    if not source_note.exists():
        source_note.write_text(
            "# Reference code\n\nSee build_handover.py — copies strategy sources into this folder.\n"
        )

    # Remove legacy v1 exports
    legacy = [
        OUT / "backtests/run_295_friday_grid",
        OUT / "params/mon_thu_f77e3a873cb7.json",
        OUT / "params/friday_829e7226cc48.json",
    ]
    for p in legacy:
        if p.is_dir():
            shutil.rmtree(p)
        elif p.is_file():
            p.unlink()

    exported = {}
    for slot_id, cfg in SLOTS.items():
        if cfg.get("reserved"):
            continue
        exported[slot_id] = export_slot(slot_id, cfg)
        print(f"  {slot_id}: {cfg['combo_hash']} — {exported[slot_id]['n_trades']} trades")

    schedule = {
        "strategy_name_live": "tudysho",
        "implementation_target": (
            "CryoTrader — fork CryoTrader/strategies/short_str_turb_dyn.py; "
            "one strategy module, slot-based param routing."
        ),
        "entry_time_semantics": "NYC wall-clock (America/New_York, DST-aware). See TIMEZONE.md.",
        "system_time_semantics": (
            "CryoTrader loop, logs, and Deribit timestamps run in UTC. "
            "entry_time is NOT UTC."
        ),
        "position_rules": {
            "max_concurrent_positions": 1,
            "per_slot_max_entries_per_nyc_day": 1,
            "monday_two_trades": (
                "Monday: slot_b (01:00 NYC, dte=0) expires Mon 08:00 UTC, then "
                "slot_a (16:00 NYC, dte=1) may open same NYC Monday — sequential, "
                "not overlapping. See POSITION_RULES.md."
            ),
        },
        "position_rules_doc": "POSITION_RULES.md",
        "slots": {
            slot_id: {
                "combo_hash": cfg["combo_hash"],
                "param_file": f"params/{slot_id}.json",
                "label": cfg["label"],
                "reserved": cfg.get("reserved", False),
                **({"live_calendar": cfg["live_calendar"]} if cfg.get("live_calendar") else {}),
            }
            for slot_id, cfg in SLOTS.items()
        },
        "ignore_grid_params": [
            "trade_monday", "trade_tuesday", "trade_wednesday", "trade_thursday",
            "trade_friday", "trade_saturday", "trade_sunday",
        ],
    }
    (OUT / "LIVE_PARAM_SCHEDULE.json").write_text(
        json.dumps(schedule, indent=2) + "\n"
    )

    summary = {
        "slots": exported,
        "unique_combos": {
            "5cd986cf48cd": ["slot_a_mon_thu"],
            "e2f4ac2b3e69": ["slot_b_mon_early"],
            "829e7226cc48": ["slot_c_fri_sat"],
        },
    }
    (OUT / "backtests" / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"\nHandover package updated: {OUT}")


if __name__ == "__main__":
    main()

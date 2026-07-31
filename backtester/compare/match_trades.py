"""Match live trades to BT trades with comparability codes."""
from __future__ import annotations

from typing import List

import pandas as pd

from backtester.compare.models import Comparability, WarningCode
from backtester.compare.resolve_config import load_strategy_map


def _exit_equivalent(live_exit: str, bt_exit: str) -> bool:
    smap = load_strategy_map()
    mapping = smap.get("exit_label_map", {})
    norm_live = mapping.get(live_exit, live_exit)
    if norm_live == bt_exit:
        return True
    if "mark_sl" in (live_exit or "") and bt_exit == "stop_loss":
        return True
    return live_exit == bt_exit


def _classify_row(lr, br) -> tuple:
    codes: List[str] = []
    comparability = Comparability.OK

    if lr.strategy != "tudysho" and lr.strategy not in ("tudysho_eisbach",):
        codes.append(WarningCode.STRATEGY_MISMATCH.value)
        comparability = Comparability.EXCLUDE

    if lr.config_drift:
        codes.append(WarningCode.CONFIG_DRIFT.value)
        comparability = Comparability.EXCLUDE

    if lr.partial_fill:
        codes.append(WarningCode.PARTIAL_FILL.value)
        comparability = Comparability.WARN

    if br is None:
        codes.append(WarningCode.NO_BT_TRADE.value)
        if comparability == Comparability.OK:
            comparability = Comparability.WARN
        return comparability.value, codes

    if lr.call and br.call and (int(lr.call), int(lr.put)) != (int(br.call), int(br.put)):
        codes.append(WarningCode.STRIKE_MISMATCH.value)
        comparability = Comparability.WARN

    if br.exit == "end_of_data":
        codes.append(WarningCode.DATA_GAP.value)
        comparability = Comparability.EXCLUDE

    if not _exit_equivalent(str(lr.exit or ""), str(br.exit or "")):
        codes.append(WarningCode.EXIT_MISMATCH.value)
        if br.exit != "end_of_data":
            comparability = Comparability.WARN

    codes.append(WarningCode.SIZING_DIFF.value)
    codes.append(WarningCode.FILL_MODEL.value)

    return comparability.value, codes


def match(live: pd.DataFrame, bt: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, lr in live.iterrows():
        cands = bt[(bt.entry_date == lr.entry_date) & (bt.schedule == lr.schedule)]
        if len(cands) == 0:
            cands = bt[bt.entry_date == lr.entry_date]
        br = cands.iloc[0] if len(cands) else None

        comparability, warning_codes = _classify_row(lr, br)

        row = {
            "entry_date": lr.entry_date,
            "schedule": lr.schedule,
            "comparability": comparability,
            "warning_codes": "|".join(sorted(set(warning_codes))),
            "live_entry_utc": lr.entry_utc,
            "live_exit_utc": lr.exit_utc,
            "live_exit": lr.exit,
            "live_call_put": f"{int(lr.call)}/{int(lr.put)}" if lr.call else "?",
            "live_pnl_per_lot": round(lr.pnl_usd_per_lot, 2) if pd.notna(lr.pnl_usd_per_lot) else None,
            "live_partial": bool(lr.partial_fill),
            "live_config_drift": bool(lr.config_drift),
            "bt_entry_utc": br.entry_utc if br is not None else None,
            "bt_exit_utc": br.exit_utc if br is not None else None,
            "bt_exit": br.exit if br is not None else None,
            "bt_call_put": (
                f"{int(br.call)}/{int(br.put)}" if br is not None and br.call else None
            ),
            "bt_pnl_per_lot": (
                round(br.pnl_usd_per_lot, 2) if br is not None and pd.notna(br.pnl_usd_per_lot) else None
            ),
            "bt_present": br is not None,
        }
        if br is not None and lr.pnl_usd_per_lot is not None and br.pnl_usd_per_lot is not None:
            row["delta_pnl_per_lot"] = round(lr.pnl_usd_per_lot - br.pnl_usd_per_lot, 2)
        rows.append(row)
    return pd.DataFrame(rows)

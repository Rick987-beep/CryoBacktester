"""Combine per-slot tudysho backtest trades into one joint trade list."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

HANDOVER = Path(__file__).resolve().parents[2] / "handover/tudysho_cryotrader"
SLOT_FILES = {
    "A": HANDOVER / "backtests/trades_slot_a_mon_thu.csv",
    "B": HANDOVER / "backtests/trades_slot_b_mon_early.csv",
    "C": HANDOVER / "backtests/trades_slot_c_fri_sat.csv",
}

SLOT_META = {
    "A": {"combo_hash": "5cd986cf48cd", "slot_id": "slot_a_mon_thu"},
    "B": {"combo_hash": "e2f4ac2b3e69", "slot_id": "slot_b_mon_early"},
    "C": {"combo_hash": "829e7226cc48", "slot_id": "slot_c_fri_sat"},
}


def _load_slot(slot: str) -> pd.DataFrame:
    df = pd.read_csv(SLOT_FILES[slot], parse_dates=["entry_time", "exit_time"])
    df["entry_date"] = pd.to_datetime(df["entry_date"]).dt.strftime("%Y-%m-%d")
    df["slot"] = slot
    df["combo_hash"] = SLOT_META[slot]["combo_hash"]
    df["slot_id"] = SLOT_META[slot]["slot_id"]
    return df


def combine_trades() -> pd.DataFrame:
    """Union of all slot trades — slots are timed so positions do not overlap.

    Monday: B opens ~01:00 NYC, expires 08:00 UTC; A opens 16:00 NYC same day.
    Thu A expires Fri 08:00 UTC before Fri C opens at 12:00 NYC.
    No deduplication or priority dropping — include every trade from each slot.
    """
    combo = pd.concat(
        [_load_slot(s) for s in ("A", "B", "C")],
        ignore_index=True,
    )
    combo = combo.sort_values("entry_time").reset_index(drop=True)
    combo["merge_method"] = "union_no_overlap"
    return combo


def count_position_overlaps(trades: pd.DataFrame) -> int:
    """Return count of overlapping open intervals (for validation)."""
    normal = trades[~trades["exit_reason"].isin(["end_of_data"])]
    n = 0
    for i in range(len(normal)):
        for j in range(i + 1, len(normal)):
            t1, t2 = normal.iloc[i], normal.iloc[j]
            if t1.entry_time < t2.exit_time and t2.entry_time < t1.exit_time:
                n += 1
    return n

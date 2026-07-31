"""Forensic flags for peculiar live fills."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def build_forensics(live: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, lr in live.iterrows():
        flags = []
        if lr.partial_fill:
            flags.append("partial_or_zero_leg")
        if lr.thin_book:
            flags.append("thin_book_entry")
        exit_s = str(lr.exit or "")
        if "mark_sl" in exit_s or exit_s == "stop_loss":
            flags.append("loss_stop")
        if lr.config_drift:
            flags.append("config_drift")
        if float(lr.pnl_usd_per_lot or 0) < 0:
            flags.append("loss")

        rows.append({
            "entry_date": lr.entry_date,
            "schedule": lr.schedule,
            "flags": "|".join(flags),
            "open_legs": lr.open_legs_json,
            "close_legs": lr.close_legs_json,
            "narrative": _narrative(lr, flags),
        })
    return pd.DataFrame(rows)


def _narrative(lr, flags: list) -> str:
    parts = []
    if "partial_or_zero_leg" in flags:
        legs = json.loads(lr.open_legs_json)
        for leg in legs:
            fq = leg.get("filled_qty") or 0
            tq = leg.get("target_qty") or 0
            if fq < tq * 0.99:
                parts.append(
                    f"{leg.get('symbol')}: filled {fq}/{tq} @ {leg.get('fill_price')}"
                )
    if "loss_stop" in flags:
        parts.append(f"Stopped out via {lr.exit} at {lr.exit_utc} UTC")
    if "config_drift" in flags:
        parts.append("Live trade metadata params differ from current slot TOML")
    return "; ".join(parts) if parts else ""


def write_forensics_jsonl(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for _, row in df.iterrows():
            fh.write(json.dumps(row.to_dict(), default=str) + "\n")

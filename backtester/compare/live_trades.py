"""Load and normalize live trades from blotter JSONL."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional
from zoneinfo import ZoneInfo

import pandas as pd

NYC = ZoneInfo("America/New_York")
UTC = timezone.utc


def parse_strikes(symbols: list) -> tuple:
    call = put = None
    for sym in symbols:
        if not sym:
            continue
        parts = sym.split("-")
        if parts[-1] == "C":
            call = float(parts[2])
        if parts[-1] == "P":
            put = float(parts[2])
    return call, put


def load_blotter_rows(blotter: Path) -> list:
    rows = []
    with blotter.open() as fh:
        for line in fh:
            r = json.loads(line)
            if r.get("opened_at"):
                rows.append(r)
    rows.sort(key=lambda r: r["opened_at"])
    return rows


def select_window(rows: list, last_n: Optional[int], date_from: Optional[str], date_to: Optional[str]) -> list:
    if last_n is not None:
        return rows[-last_n:]
    out = []
    for r in rows:
        opened = datetime.fromtimestamp(r["opened_at"], tz=UTC)
        ds = opened.date().isoformat()
        if date_from and ds < date_from:
            continue
        if date_to and ds > date_to:
            continue
        out.append(r)
    return out


def rows_to_dataframe(rows: list, resolved_slot_params: dict | None = None) -> pd.DataFrame:
    resolved_slot_params = resolved_slot_params or {}
    sched_defaults = {}
    for k, v in resolved_slot_params.items():
        if k.startswith("schedule_") and isinstance(v, dict):
            sched_defaults[k.replace("schedule_", "")] = v

    out = []
    for r in rows:
        opened = datetime.fromtimestamp(r["opened_at"], tz=UTC)
        closed = (
            datetime.fromtimestamp(r["closed_at"], tz=UTC) if r.get("closed_at") else None
        )
        meta = r.get("strategy_metadata") or {}
        sched = meta.get("schedule_id") or (meta.get("schedule_params") or {}).get(
            "schedule_id", "legacy"
        )
        sp = meta.get("schedule_params") or {}
        legs = r.get("open_legs") or []
        symbols = [leg.get("symbol") for leg in legs]
        call, put = parse_strikes(symbols)
        qty = float(legs[0]["qty"]) if legs else None
        spot_c = float(r.get("index_price_at_close") or r.get("index_price_at_open") or 0)
        pnl_btc = float(r.get("realized_pnl") or 0)

        partial = any(
            float(l.get("filled_qty") or 0) < float(l.get("qty") or 0) * 0.99 for l in legs
        ) or any(float(l.get("filled_qty") or 0) == 0 for l in legs)

        thin = all(
            float(l.get("fill_price") or 0) <= 0.0002 for l in legs if l.get("fill_price")
        )

        config_drift = False
        if sched in sched_defaults and sp:
            ref = sched_defaults[sched]
            for key in ("entry_time", "delta", "stop_loss_pct", "min_otm_pct"):
                if key in ref and key in sp and str(ref[key]) != str(sp[key]):
                    config_drift = True

        out.append({
            "entry_date": opened.date().isoformat(),
            "entry_utc": opened.strftime("%Y-%m-%d %H:%M"),
            "entry_nyc": opened.astimezone(NYC).strftime("%Y-%m-%d %H:%M"),
            "exit_utc": closed.strftime("%Y-%m-%d %H:%M") if closed else "",
            "strategy": r["strategy_id"],
            "schedule": sched,
            "exit": r.get("close_trigger"),
            "qty": qty,
            "call": call,
            "put": put,
            "contracts": " | ".join(s for s in symbols if s),
            "spot_open": r.get("index_price_at_open"),
            "spot_close": spot_c,
            "pnl_usd": pnl_btc * spot_c,
            "pnl_usd_per_lot": pnl_btc * spot_c / qty if qty else None,
            "partial_fill": partial,
            "thin_book": thin,
            "config_drift": config_drift,
            "open_legs_json": json.dumps([
                {
                    "symbol": l.get("symbol"),
                    "target_qty": l.get("qty"),
                    "filled_qty": l.get("filled_qty"),
                    "fill_price": l.get("fill_price"),
                }
                for l in legs
            ]),
            "close_legs_json": json.dumps([
                {"symbol": l.get("symbol"), "fill_price": l.get("fill_price")}
                for l in (r.get("close_legs") or [])
            ]),
        })
    return pd.DataFrame(out)

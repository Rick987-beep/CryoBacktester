#!/usr/bin/env python3
"""Scan 5-min snapshots after schedule entry: BT bid>0 vs live mark-fallback path."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
PKG = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtester.core.config import cfg
from backtester.core.expiry_utils import select_expiry
from backtester.core.market_replay import MarketReplay
from backtester.core.option_selection import select_by_delta
from backtester.strategies.tudysho_eisbach import (
    _SCHEDULE_DEFAULTS,
    _apply_min_otm,
    _entry_utc_today,
)

UTC = timezone.utc


def entry_utc_for(day: date, schedule: str) -> datetime:
    noon = datetime(day.year, day.month, day.day, 12, 0, tzinfo=UTC)
    return _entry_utc_today(_SCHEDULE_DEFAULTS[schedule], noon)


def watch_end(entry: datetime, schedule: str) -> datetime:
    params = _SCHEDULE_DEFAULTS[schedule]
    if params["watch_until_utc_midnight"]:
        return datetime.combine(entry.date() + timedelta(days=1), datetime.min.time()).replace(
            tzinfo=UTC
        )
    return datetime.combine(entry.date(), datetime.min.time()).replace(
        tzinfo=UTC, hour=int(params["watch_until_utc_hour"])
    )


def scan_after_entry(day: date, schedule: str) -> dict:
    params = _SCHEDULE_DEFAULTS[schedule]
    dte = int(params["dte"])
    min_otm = float(params["min_otm_pct"])
    delta = float(params["delta"])
    entry = entry_utc_for(day, schedule)
    end = watch_end(entry, schedule)

    day_str = entry.strftime("%Y-%m-%d")
    replay = MarketReplay(
        cfg.data.options_parquet,
        cfg.data.spot_parquet,
        start=day_str,
        end=(entry + timedelta(days=1)).strftime("%Y-%m-%d"),
    )

    openable = blocked = mark_fallback_ok = 0
    first_ok = at_entry = None
    for state in replay:
        if state.dt < entry or state.dt >= end:
            continue
        expiry = select_expiry(state, dte)
        if not expiry:
            blocked += 1
            continue
        chain = state.get_chain(expiry) or []
        calls = [q for q in chain if q.is_call]
        puts = [q for q in chain if not q.is_call]
        call = select_by_delta(calls, +delta)
        put = select_by_delta(puts, -delta)
        if not call or not put:
            blocked += 1
            continue
        if min_otm > 0:
            call = _apply_min_otm(calls, call, state.spot, min_otm, True)
            put = _apply_min_otm(puts, put, state.spot, min_otm, False)
            if not call or not put:
                blocked += 1
                continue

        bid_ok = (call.bid or 0) > 0 and (put.bid or 0) > 0
        mark_ok = ((call.bid or 0) > 0 or (call.mark or 0) > 0) and (
            (put.bid or 0) > 0 or (put.mark or 0) > 0
        )
        rec = dict(
            ts=state.dt,
            call_strike=call.strike,
            call_bid=call.bid,
            call_mark=call.mark,
            put_strike=put.strike,
            put_bid=put.bid,
            put_mark=put.mark,
            bid_ok=bid_ok,
            mark_ok=mark_ok,
        )
        if at_entry is None and state.dt >= entry:
            at_entry = rec
        if bid_ok:
            openable += 1
            if first_ok is None:
                first_ok = rec
        else:
            blocked += 1
            if mark_ok:
                mark_fallback_ok += 1

    total = openable + blocked
    return {
        "date": day.isoformat(),
        "schedule": schedule,
        "entry_utc": entry.strftime("%Y-%m-%d %H:%M"),
        "watch_end_utc": end.strftime("%Y-%m-%d %H:%M"),
        "total_ticks": total,
        "bt_openable_ticks": openable,
        "bt_blocked_ticks": blocked,
        "mark_fallback_ok_ticks": mark_fallback_ok,
        "first_bt_openable_utc": first_ok["ts"].strftime("%Y-%m-%d %H:%M") if first_ok else "",
        "at_entry_call_bid": at_entry["call_bid"] if at_entry else None,
        "at_entry_call_mark": at_entry["call_mark"] if at_entry else None,
        "at_entry_put_bid": at_entry["put_bid"] if at_entry else None,
        "at_entry_put_mark": at_entry["put_mark"] if at_entry else None,
        "at_entry_mark_fallback_ok": at_entry["mark_ok"] if at_entry else None,
    }


def live_filled_on(blotter: Path, day: date, schedule: str) -> bool:
    with blotter.open() as fh:
        for line in fh:
            r = json.loads(line)
            if r.get("strategy_id") != "tudysho":
                continue
            opened = datetime.fromtimestamp(r["opened_at"], tz=UTC)
            if opened.date() != day:
                continue
            meta = r.get("strategy_metadata") or {}
            sched = meta.get("schedule_id") or (meta.get("schedule_params") or {}).get(
                "schedule_id"
            )
            if sched == schedule:
                return True
    return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=PKG / "data/fill_gap.csv")
    ap.add_argument("--blotter", type=Path, default=PKG / "data/slot-02.jsonl")
    args = ap.parse_args()

    scans = [
        (date(2026, 7, 7), "mon_thu"),
        (date(2026, 7, 8), "mon_thu"),
        (date(2026, 7, 9), "mon_thu"),
        (date(2026, 7, 10), "fri"),
        (date(2026, 7, 13), "mon_early"),
        (date(2026, 7, 13), "mon_thu"),
        (date(2026, 7, 14), "mon_thu"),
        (date(2026, 7, 15), "mon_thu"),
        (date(2026, 7, 16), "mon_thu"),
        (date(2026, 7, 17), "fri"),
    ]

    rows = []
    for day, sched in scans:
        row = scan_after_entry(day, sched)
        row["live_filled"] = live_filled_on(args.blotter, day, sched)
        rows.append(row)
        print(
            f"{day} {sched:10s}  openable={row['bt_openable_ticks']:3d}/{row['total_ticks']:3d}  "
            f"mark_fallback={row['mark_fallback_ok_ticks']:3d}  live={row['live_filled']}"
        )

    df = pd.DataFrame(rows)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()

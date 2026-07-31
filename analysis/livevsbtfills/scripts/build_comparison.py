#!/usr/bin/env python3
"""Build live vs BT trade CSVs and day-by-day summary from blotter + BT bundle."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
PKG = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def parse_strikes(symbols: list[str | None]) -> tuple[float | None, float | None]:
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


def load_live(blotter: Path, d_from: date, d_to: date) -> pd.DataFrame:
    rows = []
    with blotter.open() as fh:
        for line in fh:
            r = json.loads(line)
            opened_at = r.get("opened_at")
            if not opened_at:
                continue
            opened = datetime.fromtimestamp(opened_at, tz=timezone.utc)
            if not (d_from <= opened.date() <= d_to):
                continue
            closed = (
                datetime.fromtimestamp(r["closed_at"], tz=timezone.utc)
                if r.get("closed_at")
                else None
            )
            meta_sm = r.get("strategy_metadata") or {}
            sched = meta_sm.get("schedule_id") or (meta_sm.get("schedule_params") or {}).get(
                "schedule_id"
            )
            legs = r.get("open_legs") or []
            symbols = [leg.get("symbol") for leg in legs]
            call, put = parse_strikes(symbols)
            qty = float(legs[0]["qty"]) if legs else None
            spot_c = float(r.get("index_price_at_close") or r.get("index_price_at_open") or 0)
            pnl_btc = float(r.get("realized_pnl") or 0)
            fees_btc = float(r.get("total_fees") or 0)
            rows.append(
                {
                    "entry_date": opened.date().isoformat(),
                    "entry_utc": opened.strftime("%Y-%m-%d %H:%M"),
                    "exit_utc": closed.strftime("%Y-%m-%d %H:%M") if closed else "",
                    "strategy": r["strategy_id"],
                    "schedule": sched or "legacy",
                    "exit": r.get("close_trigger"),
                    "qty": qty,
                    "call": call,
                    "put": put,
                    "contracts": " | ".join(s for s in symbols if s),
                    "spot_open": r.get("index_price_at_open"),
                    "spot_close": spot_c,
                    "pnl_btc": pnl_btc,
                    "pnl_usd": pnl_btc * spot_c,
                    "fees_usd": fees_btc * spot_c,
                    "pnl_usd_per_lot": pnl_btc * spot_c / qty if qty else None,
                }
            )
    return pd.DataFrame(rows)


def infer_schedule(entry_ts: pd.Timestamp) -> str:
    nyc = entry_ts.tz_convert(ZoneInfo("America/New_York"))
    wd = nyc.weekday()
    hhmm = nyc.hour * 60 + nyc.minute
    if wd == 0 and hhmm < 16 * 60:
        return "mon_early"
    if wd in (0, 1, 2, 3) and hhmm >= 16 * 60:
        return "mon_thu"
    if wd == 4 and hhmm >= 12 * 60:
        return "fri"
    return "?"


def load_bt(bundle: Path, d_from: date, d_to: date) -> pd.DataFrame:
    trades = pd.read_parquet(bundle / "trade_log.parquet")
    fills = pd.read_parquet(bundle / "fills.parquet")
    fills["ts"] = pd.to_datetime(fills["ts"], utc=True)
    opens = fills[fills["event"] == "open"]
    closes = fills[fills["event"] == "close"]

    rows = []
    for open_idx, g in opens.groupby("open_idx"):
        g = g.sort_values("ts")
        entry_ts = g["ts"].iloc[0]
        if not (d_from <= entry_ts.date() <= d_to):
            continue
        contracts = list(g["contract"])
        call, put = parse_strikes(contracts)
        qty = float(g["qty"].iloc[0])
        cg = closes[closes["open_idx"] == open_idx]
        exit_ts = cg["ts"].iloc[0] if len(cg) else None
        exit_reason = cg["exit_reason"].iloc[0] if len(cg) else None
        tl = trades[pd.to_datetime(trades["entry_time"], utc=True) == entry_ts]
        if len(tl) == 0:
            ttimes = pd.to_datetime(trades["entry_time"], utc=True)
            tl = trades.loc[[(ttimes - entry_ts).abs().idxmin()]]
        tr = tl.iloc[0]
        sched = infer_schedule(entry_ts)
        pnl = float(tr["pnl"])
        rows.append(
            {
                "entry_date": entry_ts.date().isoformat(),
                "entry_utc": entry_ts.strftime("%Y-%m-%d %H:%M"),
                "exit_utc": exit_ts.strftime("%Y-%m-%d %H:%M") if exit_ts is not None else "",
                "strategy": "tudysho_eisbach",
                "schedule": sched,
                "exit": exit_reason or tr["exit_reason"],
                "qty": qty,
                "call": call,
                "put": put,
                "contracts": " | ".join(contracts),
                "spot_open": float(tr["entry_spot"]),
                "spot_close": float(tr["exit_spot"]),
                "pnl_btc": None,
                "pnl_usd": pnl,
                "fees_usd": float(tr["fees"]),
                "pnl_usd_per_lot": pnl / qty if qty else None,
            }
        )
    return pd.DataFrame(rows)


def build_day_by_day(live: pd.DataFrame, bt: pd.DataFrame, d_from: date, d_to: date) -> pd.DataFrame:
    rows = []
    for d in pd.date_range(d_from.isoformat(), d_to.isoformat()):
        ds = d.strftime("%Y-%m-%d")
        L = live[live.entry_date == ds]
        B = bt[bt.entry_date == ds]
        note: list[str] = []
        if len(L) == 0 and len(B) == 0:
            note.append("flat")
        if len(L) and L.iloc[0].strategy != "tudysho":
            note.append("live still short_str_turb_dyn")
        if len(L) != len(B):
            note.append(f"count L{len(L)}/B{len(B)}")
        if len(L) == 1 and len(B) == 1:
            if (L.iloc[0].call, L.iloc[0].put) != (B.iloc[0].call, B.iloc[0].put):
                note.append(
                    f"strikes L {int(L.iloc[0].call)}/{int(L.iloc[0].put)} "
                    f"vs BT {int(B.iloc[0].call)}/{int(B.iloc[0].put)}"
                )
        if ds in ("2026-07-07", "2026-07-09"):
            note.append("BT skip: min_otm call bid=0 in snapshots")
        if ds == "2026-07-16":
            note.append("BT delayed to 23:20 waiting for positive bids after min_otm")
        if ds == "2026-07-17":
            note.append("BT end_of_data (run ends 2026-07-18); live expired Sat 08:00")
        if ds == "2026-07-06" and len(B) == 1:
            note.append("BT no mon_early (0-DTE call bid=0 after min_otm); live was legacy strat")
        rows.append(
            {
                "date": ds,
                "weekday": d.strftime("%a"),
                "live_n": len(L),
                "bt_n": len(B),
                "live_pnl_usd": round(float(L.pnl_usd.sum()), 2) if len(L) else 0,
                "bt_pnl_usd": round(float(B.pnl_usd.sum()), 2) if len(B) else 0,
                "live_pnl_per_lot": round(float(L.pnl_usd_per_lot.sum()), 2) if len(L) else 0,
                "bt_pnl_per_lot": round(float(B.pnl_usd_per_lot.sum()), 2) if len(B) else 0,
                "notes": "; ".join(note),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d_from", default="2026-07-01")
    ap.add_argument("--to", dest="d_to", default="2026-07-17")
    ap.add_argument("--blotter", type=Path, default=PKG / "data/slot-02.jsonl")
    ap.add_argument(
        "--bundle",
        type=Path,
        default=ROOT / "backtester/reports/tudysho_eisbach_20260720_090214.bundle",
    )
    ap.add_argument("--out", type=Path, default=PKG / "data")
    args = ap.parse_args()

    d_from = date.fromisoformat(args.d_from)
    d_to = date.fromisoformat(args.d_to)
    args.out.mkdir(parents=True, exist_ok=True)

    live = load_live(args.blotter, d_from, d_to)
    bt = load_bt(args.bundle, d_from, d_to)
    day = build_day_by_day(live, bt, d_from, d_to)

    live.to_csv(args.out / "live_jul1_17.csv", index=False)
    bt.to_csv(args.out / "bt_jul1_17.csv", index=False)
    day.to_csv(args.out / "day_by_day.csv", index=False)

    L2 = live[(live.entry_date >= "2026-07-08") & (live.strategy == "tudysho")]
    B2 = bt[bt.entry_date >= "2026-07-08"]
    print(f"Live trades: {len(live)}  BT trades: {len(bt)}")
    print(f"Tudysho-only Jul 8–17: live {len(L2)}  bt {len(B2)}")
    print(f"Wrote CSVs to {args.out}")


if __name__ == "__main__":
    main()

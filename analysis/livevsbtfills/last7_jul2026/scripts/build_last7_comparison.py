#!/usr/bin/env python3
"""Extract last N live fills with forensic detail + compare to BT bundle."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
PKG = Path(__file__).resolve().parents[1]
DATA = PKG / "data"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

NYC = ZoneInfo("America/New_York")
UTC = timezone.utc


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


def load_live_last_n(blotter: Path, n: int) -> pd.DataFrame:
    rows_raw = []
    with blotter.open() as fh:
        for line in fh:
            r = json.loads(line)
            if r.get("opened_at"):
                rows_raw.append(r)
    rows_raw.sort(key=lambda r: r["opened_at"])
    last = rows_raw[-n:]

    rows = []
    for r in last:
        opened = datetime.fromtimestamp(r["opened_at"], tz=UTC)
        closed = (
            datetime.fromtimestamp(r["closed_at"], tz=UTC)
            if r.get("closed_at")
            else None
        )
        meta = r.get("strategy_metadata") or {}
        sched = meta.get("schedule_id") or (meta.get("schedule_params") or {}).get(
            "schedule_id", "legacy"
        )
        sp = meta.get("schedule_params") or {}
        legs = r.get("open_legs") or []
        close_legs = r.get("close_legs") or []
        symbols = [leg.get("symbol") for leg in legs]
        call, put = parse_strikes(symbols)
        qty = float(legs[0]["qty"]) if legs else None
        spot_c = float(r.get("index_price_at_close") or r.get("index_price_at_open") or 0)
        pnl_btc = float(r.get("realized_pnl") or 0)
        fees_btc = float(r.get("total_fees") or 0)

        open_detail = []
        for leg in legs:
            open_detail.append({
                "symbol": leg.get("symbol"),
                "target_qty": leg.get("qty"),
                "filled_qty": leg.get("filled_qty"),
                "fill_price": leg.get("fill_price"),
            })
        close_detail = [
            {"symbol": leg.get("symbol"), "fill_price": leg.get("fill_price")}
            for leg in close_legs
        ]

        combined_open_premium = sum(
            float(l.get("fill_price") or 0) * float(l.get("filled_qty") or 0)
            for l in legs
            if l.get("fill_price") is not None and float(l.get("filled_qty") or 0) > 0
        )
        partial = any(
            float(l.get("filled_qty") or 0) < float(l.get("qty") or 0) * 0.99
            for l in legs
        ) or any(float(l.get("filled_qty") or 0) == 0 for l in legs)

        rows.append({
            "entry_date": opened.date().isoformat(),
            "entry_utc": opened.strftime("%Y-%m-%d %H:%M"),
            "entry_nyc": opened.astimezone(NYC).strftime("%Y-%m-%d %H:%M"),
            "exit_utc": closed.strftime("%Y-%m-%d %H:%M") if closed else "",
            "exit_nyc": closed.astimezone(NYC).strftime("%Y-%m-%d %H:%M") if closed else "",
            "strategy": r["strategy_id"],
            "schedule": sched,
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
            "partial_fill": partial,
            "combined_open_premium_btc": combined_open_premium,
            "open_legs_json": json.dumps(open_detail),
            "close_legs_json": json.dumps(close_detail),
            "live_delta": sp.get("delta"),
            "live_entry_time": sp.get("entry_time"),
            "live_sl_pct": sp.get("stop_loss_pct"),
            "live_min_otm": sp.get("min_otm_pct"),
            "notes": _live_notes(r, legs, partial),
        })
    return pd.DataFrame(rows)


def _live_notes(r: dict, legs: list, partial: bool) -> str:
    notes = []
    if partial:
        notes.append("partial_or_zero_leg_fill")
    trigger = r.get("close_trigger") or ""
    if "mark_sl" in trigger or trigger == "stop_loss":
        notes.append("loss_stop")
    if trigger == "expiry" and float(r.get("realized_pnl") or 0) < 0:
        notes.append("loss_at_expiry")
    if trigger == "expiry":
        zeros = [l for l in (r.get("close_legs") or []) if float(l.get("fill_price") or 0) == 0]
        if zeros:
            notes.append("expired_otm")
    sp = (r.get("strategy_metadata") or {}).get("schedule_params") or {}
    if sp.get("entry_time") == "16:00":
        notes.append("legacy_config_pre_jul21")
    return "; ".join(notes)


def load_bt(bundle: Path, d_from: str, d_to: str) -> pd.DataFrame:
    trades = pd.read_parquet(bundle / "trade_log.parquet")
    fills = pd.read_parquet(bundle / "fills.parquet")
    fills["ts"] = pd.to_datetime(fills["ts"], utc=True)
    opens = fills[fills["event"] == "open"]
    closes = fills[fills["event"] == "close"]

    rows = []
    for open_idx, g in opens.groupby("open_idx"):
        g = g.sort_values("ts")
        entry_ts = g["ts"].iloc[0]
        if not (d_from <= entry_ts.date().isoformat() <= d_to):
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
        nyc = entry_ts.tz_convert(NYC)
        wd = nyc.weekday()
        hhmm = nyc.hour * 60 + nyc.minute
        if wd == 0 and hhmm < 16 * 60:
            sched = "mon_early"
        elif wd in (0, 1, 2, 3) and hhmm >= 12 * 60:
            sched = "mon_thu"
        elif wd == 4 and hhmm >= 12 * 60:
            sched = "fri"
        else:
            sched = "?"
        pnl = float(tr["pnl"])
        rows.append({
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
            "pnl_usd": pnl,
            "fees_usd": float(tr["fees"]),
            "pnl_usd_per_lot": pnl / qty if qty else None,
        })
    return pd.DataFrame(rows)


def match_comparison(live: pd.DataFrame, bt: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, lr in live.iterrows():
        cands = bt[
            (bt.entry_date == lr.entry_date) & (bt.schedule == lr.schedule)
        ]
        if len(cands) == 0:
            cands = bt[bt.entry_date == lr.entry_date]
        br = cands.iloc[0] if len(cands) else None
        row = {
            "entry_date": lr.entry_date,
            "schedule": lr.schedule,
            "live_entry_utc": lr.entry_utc,
            "live_exit_utc": lr.exit_utc,
            "live_exit": lr.exit,
            "live_call_put": f"{int(lr.call)}/{int(lr.put)}" if lr.call else "?",
            "live_pnl_per_lot": round(lr.pnl_usd_per_lot, 2) if lr.pnl_usd_per_lot else None,
            "live_partial": lr.partial_fill,
            "live_notes": lr.notes,
            "bt_entry_utc": br.entry_utc if br is not None else None,
            "bt_exit_utc": br.exit_utc if br is not None else None,
            "bt_exit": br.exit if br is not None else None,
            "bt_call_put": (
                f"{int(br.call)}/{int(br.put)}" if br is not None and br.call else None
            ),
            "bt_pnl_per_lot": (
                round(br.pnl_usd_per_lot, 2) if br is not None and br.pnl_usd_per_lot else None
            ),
            "bt_present": br is not None,
        }
        if br is not None and lr.pnl_usd_per_lot and br.pnl_usd_per_lot:
            row["delta_pnl_per_lot"] = round(lr.pnl_usd_per_lot - br.pnl_usd_per_lot, 2)
        rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=7)
    ap.add_argument("--blotter", type=Path, default=ROOT / "analysis/livevsbtfills/data/slot-02.jsonl")
    ap.add_argument("--bundle", type=Path, default=None)
    ap.add_argument("--from", dest="d_from", default="2026-07-19")
    ap.add_argument("--to", dest="d_to", default="2026-07-29")
    args = ap.parse_args()

    meta_path = PKG / "metadata.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    bundle = args.bundle
    if bundle is None:
        rel = meta.get("backtest", {}).get("bundle")
        if rel:
            bundle = ROOT / rel
    if bundle is None or not bundle.exists():
        print("ERROR: no BT bundle — run scripts/run_slot02_bt.py first", file=sys.stderr)
        sys.exit(1)

    DATA.mkdir(parents=True, exist_ok=True)
    live = load_live_last_n(args.blotter, args.n)
    bt = load_bt(bundle, args.d_from, args.d_to)
    cmp_df = match_comparison(live, bt)

    live.to_csv(DATA / "live_last7.csv", index=False)
    bt.to_csv(DATA / "bt_window.csv", index=False)
    cmp_df.to_csv(DATA / "comparison.csv", index=False)

    print(f"Live last {args.n}: {len(live)} trades")
    print(f"BT window {args.d_from}..{args.d_to}: {len(bt)} trades")
    print(f"Wrote {DATA}/live_last7.csv, bt_window.csv, comparison.csv")
    print()
    for _, r in cmp_df.iterrows():
        flag = " ***" if r.live_notes and "loss" in r.live_notes else ""
        bt_s = "MISSING" if not r.bt_present else f"${r.bt_pnl_per_lot}/lot {r.bt_exit} {r.bt_call_put}"
        print(
            f"{r.entry_date} {r.schedule:8s} live ${r.live_pnl_per_lot}/lot {r.live_exit} "
            f"{r.live_call_put} | BT {bt_s}{flag}"
        )
        if r.live_notes:
            print(f"  notes: {r.live_notes}")


if __name__ == "__main__":
    main()

"""Load BT trades from bundle."""
from __future__ import annotations

from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from backtester.compare.live_trades import parse_strikes

NYC = ZoneInfo("America/New_York")


def infer_schedule(entry_ts: pd.Timestamp) -> str:
    nyc = entry_ts.tz_convert(NYC)
    wd = nyc.weekday()
    hhmm = nyc.hour * 60 + nyc.minute
    if wd == 0 and hhmm < 16 * 60:
        return "mon_early"
    if wd in (0, 1, 2, 3) and hhmm >= 12 * 60:
        return "mon_thu"
    if wd == 4 and hhmm >= 12 * 60:
        return "fri"
    return "?"


def load_bundle(bundle: Path, bt_strategy: str) -> pd.DataFrame:
    trades = pd.read_parquet(bundle / "trade_log.parquet")
    fills = pd.read_parquet(bundle / "fills.parquet")
    fills["ts"] = pd.to_datetime(fills["ts"], utc=True)
    opens = fills[fills["event"] == "open"]
    closes = fills[fills["event"] == "close"]

    rows = []
    for open_idx, g in opens.groupby("open_idx"):
        g = g.sort_values("ts")
        entry_ts = g["ts"].iloc[0]
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
        pnl = float(tr["pnl"])
        rows.append({
            "entry_date": entry_ts.date().isoformat(),
            "entry_utc": entry_ts.strftime("%Y-%m-%d %H:%M"),
            "exit_utc": exit_ts.strftime("%Y-%m-%d %H:%M") if exit_ts is not None else "",
            "strategy": bt_strategy,
            "schedule": infer_schedule(entry_ts),
            "exit": exit_reason or tr["exit_reason"],
            "qty": qty,
            "call": call,
            "put": put,
            "contracts": " | ".join(contracts),
            "spot_open": float(tr["entry_spot"]),
            "spot_close": float(tr["exit_spot"]),
            "pnl_usd": pnl,
            "pnl_usd_per_lot": pnl / qty if qty else None,
        })
    return pd.DataFrame(rows)

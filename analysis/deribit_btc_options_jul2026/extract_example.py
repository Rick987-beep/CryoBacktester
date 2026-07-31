#!/usr/bin/env python3
"""
How to extract Deribit BTC options / spot data from this package.

Dependencies:
    pip install pandas pyarrow

Run from this directory:
    python extract_example.py
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent / "data"


def list_days(prefix: str = "options_") -> list[str]:
    """Return sorted YYYY-MM-DD strings for available complete daily files."""
    days = []
    for path in sorted(DATA_DIR.glob(f"{prefix}????-??-??.parquet")):
        # Skip *_INCOMPLETE.parquet
        if "INCOMPLETE" in path.name:
            continue
        day = path.name.removeprefix(prefix).removesuffix(".parquet")
        days.append(day)
    return days


def load_options_day(day: str) -> pd.DataFrame:
    """Load one UTC day of 5-minute option-chain snapshots."""
    path = DATA_DIR / f"options_{day}.parquet"
    df = pd.read_parquet(path)
    df["dt"] = pd.to_datetime(df["timestamp"], unit="us", utc=True)
    return df


def load_spot_day(day: str) -> pd.DataFrame:
    """Load one UTC day of 1-minute BTC OHLC bars."""
    path = DATA_DIR / f"spot_track_{day}.parquet"
    df = pd.read_parquet(path)
    df["dt"] = pd.to_datetime(df["timestamp"], unit="us", utc=True)
    return df


def filter_chain(
    options: pd.DataFrame,
    *,
    expiry: str | None = None,
    strike: float | None = None,
    calls_only: bool | None = None,
    snapshot_dt: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Common filters for a single chain slice."""
    out = options
    if expiry is not None:
        out = out[out["expiry"] == expiry]
    if strike is not None:
        out = out[out["strike"] == strike]
    if calls_only is True:
        out = out[out["is_call"]]
    elif calls_only is False:
        out = out[~out["is_call"]]
    if snapshot_dt is not None:
        out = out[out["dt"] == pd.Timestamp(snapshot_dt)]
    return out.copy()


def prices_to_usd(options: pd.DataFrame) -> pd.DataFrame:
    """Add USD columns from BTC prices × underlying_price."""
    out = options.copy()
    spot = out["underlying_price"]
    for col in ("bid_price", "ask_price", "mark_price"):
        out[col.replace("_price", "_usd")] = out[col] * spot
    return out


def spot_at_snapshot(spot: pd.DataFrame, snapshot_dt: pd.Timestamp) -> pd.Series | None:
    """Nearest-previous 1-min bar at or before a 5-min options snapshot."""
    ts = pd.Timestamp(snapshot_dt)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    eligible = spot[spot["dt"] <= ts]
    if eligible.empty:
        return None
    return eligible.iloc[-1]


def main() -> None:
    days = list_days()
    print(f"Complete option days available: {days[0]} → {days[-1]} ({len(days)} days)")
    print()

    day = days[0]
    options = load_options_day(day)
    spot = load_spot_day(day)

    print(f"Loaded options_{day}.parquet: {len(options):,} rows")
    print(f"  columns: {list(options.columns)}")
    print(f"  unique 5-min snapshots: {options['dt'].nunique()}")
    print(f"  expiries (sample): {sorted(options['expiry'].unique())[:8]}")
    print()
    print(f"Loaded spot_track_{day}.parquet: {len(spot):,} bars")
    print()

    # Example: one 5-min snapshot, near-ATM calls for the nearest expiry
    first_ts = options["dt"].iloc[0]
    snap = options[options["dt"] == first_ts]
    spot_row = snap["underlying_price"].iloc[0]
    nearest_expiry = sorted(snap["expiry"].unique())[0]
    atm_band = snap[
        (snap["expiry"] == nearest_expiry)
        & (snap["is_call"])
        & (snap["strike"].between(spot_row * 0.98, spot_row * 1.02))
    ].sort_values("strike")

    print(f"Snapshot {first_ts}  |  spot ≈ {spot_row:,.2f} USD  |  expiry {nearest_expiry}")
    print("Near-ATM calls (BTC prices → USD):")
    show = prices_to_usd(atm_band)[
        ["strike", "bid_price", "ask_price", "mark_price", "mark_iv", "delta",
         "bid_usd", "ask_usd", "mark_usd"]
    ]
    print(show.head(8).to_string(index=False))
    print()

    # Align spot track to the same snapshot
    bar = spot_at_snapshot(spot, first_ts)
    if bar is not None:
        print(
            f"Matching 1-min spot bar @ {bar['dt']}: "
            f"O={bar['open']:.1f} H={bar['high']:.1f} "
            f"L={bar['low']:.1f} C={bar['close']:.1f}"
        )

    print()
    print(
        "Reminder: options rows are 5-minute Deribit BTC option-chain snapshots only — "
        "not tick data."
    )


if __name__ == "__main__":
    main()

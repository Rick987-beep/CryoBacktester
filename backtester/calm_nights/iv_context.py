"""ATM IV and rolling IV rank from options snapshot parquets."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from backtester.calm_nights.sessions import decision_time_utc, slot_a_entry_days

logger = logging.getLogger(__name__)


def _options_path(data_dir: Path | str, entry_day: date) -> Path | None:
    root = Path(data_dir)
    p = root / f"options_{entry_day.isoformat()}.parquet"
    return p if p.is_file() else None


def _nearest_snapshot_us(entry_day: date, decision_time_nyc: str) -> int:
    """Microseconds since epoch for nearest 5-min tick at decision NYC."""
    dt_utc = decision_time_utc(entry_day, decision_time_nyc)
    # Snapshots align to 5-min grid; round down to nearest 5 minutes
    minute = (dt_utc.minute // 5) * 5
    snapped = dt_utc.replace(minute=minute, second=0, microsecond=0)
    return int(snapped.timestamp() * 1_000_000)


def _load_chain_at_ts(path: Path, ts_us: int) -> pd.DataFrame:
    table = pq.read_table(
        path,
        columns=["timestamp", "expiry", "strike", "is_call", "mark_iv", "underlying_price"],
    )
    df = table.to_pandas()
    if df.empty:
        return df
    sub = df[df["timestamp"] == ts_us]
    if sub.empty:
        # fallback: nearest timestamp at or before decision
        ts_arr = df["timestamp"].unique()
        ts_arr = ts_arr[ts_arr <= ts_us]
        if len(ts_arr) == 0:
            return pd.DataFrame()
        use_ts = int(ts_arr.max())
        sub = df[df["timestamp"] == use_ts]
    return sub


def atm_iv_dte1_from_chain(chain: pd.DataFrame, entry_day: date) -> float:
    """ATM mark IV (%) for nearest expiry to 1 DTE."""
    if chain.empty:
        return float("nan")

    df = chain.copy()
    # Parse expiry codes like 28MAY26
    exp_raw = df["expiry"].astype(str)
    exp_dt = pd.to_datetime(exp_raw, format="%d%b%y", errors="coerce")
    if exp_dt.isna().all():
        exp_dt = pd.to_datetime(exp_raw, errors="coerce")
    df["dte"] = (exp_dt.dt.date - entry_day).apply(lambda x: x.days if pd.notna(x) else np.nan)
    df = df[df["dte"] > 0]
    if df.empty:
        return float("nan")

    nearest = int(df["dte"].unique()[np.argmin(np.abs(df["dte"].unique() - 1))])
    subset = df[df["dte"] == nearest]

    calls = subset[subset["is_call"]].groupby("strike")["mark_iv"].first()
    puts = subset[~subset["is_call"]].groupby("strike")["mark_iv"].first()
    common = calls.index.intersection(puts.index)
    if common.empty:
        return float(subset["mark_iv"].median())

    diff = (calls.loc[common] - puts.loc[common]).abs()
    atm_strike = diff.idxmin()
    return float((calls.loc[atm_strike] + puts.loc[atm_strike]) / 2.0)


def atm_iv_for_entry_day(
    entry_day: date,
    data_dir: Path,
    decision_time_nyc: str = "12:00",
) -> float:
    path = _options_path(data_dir, entry_day)
    if path is None:
        return float("nan")
    ts_us = _nearest_snapshot_us(entry_day, decision_time_nyc)
    chain = _load_chain_at_ts(path, ts_us)
    return atm_iv_dte1_from_chain(chain, entry_day)


def iv_rolling_rank(series: pd.Series, rolling: int = 60) -> pd.Series:
    """Rolling percentile rank of IV (0–1, lower = calmer vs recent history)."""
    min_periods = min(rolling, max(5, rolling // 4))
    return series.rolling(rolling, min_periods=min_periods).rank(pct=True)


def build_iv_series(
    entry_days: list[date],
    data_dir: Path,
    decision_time_nyc: str = "12:00",
    rolling: int = 60,
) -> pd.DataFrame:
    """ATM IV and rolling rank for slot-A entry days."""
    rows = []
    for d in entry_days:
        iv = atm_iv_for_entry_day(d, data_dir, decision_time_nyc)
        rows.append({"entry_date": d.isoformat(), "atm_iv_dte1": iv})

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["iv_rank_60d"] = iv_rolling_rank(df["atm_iv_dte1"], rolling=rolling)
    df["has_options"] = df["atm_iv_dte1"].notna()
    return df

"""Load normalized option chains from per-day market parquets.

Supports two on-disk layouts under ``data/market/``:

* **Recorder snapshots** — ``options_YYYY-MM-DD.parquet`` with ``timestamp``,
  ``expiry``, ``strike``, ``is_call``, ``mark_iv``, ``delta`` (5-min grid).
* **Tardis ticker dumps** — same filename but ``instrument_name`` +
  ``creation_timestamp`` (single daily scrape; delta synthesized via BS).

``options_BTC_*`` / ``options_ETH_*`` siblings are ignored.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Literal, Optional

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from backtester.core.expiry_utils import parse_expiry_date
from backtester.core.paths import market_data_dir
from backtester.core.pricing import EXPIRY_HOUR_UTC, bs_call_delta, bs_put_delta

logger = logging.getLogger(__name__)

SchemaKind = Literal["recorder", "tardis_ticker", "unknown"]

_INSTRUMENT_RE = re.compile(
    r"^BTC-(\d{1,2}[A-Z]{3}\d{2})-(\d+(?:\.\d+)?)-([CP])$"
)

_NORMALIZED_COLS = (
    "timestamp", "expiry", "strike", "is_call", "mark_iv", "delta",
)


def detect_options_schema(path: Path) -> SchemaKind:
    names = set(pq.read_schema(path).names)
    if {"timestamp", "expiry", "strike", "is_call", "mark_iv", "delta"}.issubset(names):
        return "recorder"
    if {"instrument_name", "mark_iv", "creation_timestamp"}.issubset(names):
        return "tardis_ticker"
    return "unknown"


def options_day_path(options_dir: Path, entry_day: date) -> Optional[Path]:
    """Canonical per-day options file (never ``options_BTC_*``)."""
    path = Path(options_dir) / f"options_{entry_day.isoformat()}.parquet"
    return path if path.is_file() else None


def _snap_ts_us(entry_day: date, hour: int, minute: int) -> int:
    dt_utc = datetime(
        entry_day.year, entry_day.month, entry_day.day,
        hour, minute, tzinfo=timezone.utc,
    )
    snapped_min = (dt_utc.minute // 5) * 5
    snapped = dt_utc.replace(minute=snapped_min, second=0, microsecond=0)
    return int(snapped.timestamp() * 1_000_000)


def _years_to_expiry(
    decision_dt: datetime,
    expiry_code: str,
) -> float:
    exp = parse_expiry_date(expiry_code)
    if exp is None:
        return float("nan")
    exp_dt = exp.replace(hour=EXPIRY_HOUR_UTC, tzinfo=timezone.utc)
    dt = decision_dt if decision_dt.tzinfo else decision_dt.replace(tzinfo=timezone.utc)
    return max((exp_dt - dt).total_seconds(), 0.0) / (365.25 * 24.0 * 3600.0)


def _parse_tardis_instrument(name: str) -> Optional[tuple[str, float, bool]]:
    m = _INSTRUMENT_RE.match(str(name).strip())
    if not m:
        return None
    expiry, strike_s, cp = m.group(1), m.group(2), m.group(3)
    return expiry, float(strike_s), cp == "C"


def _normalize_tardis_df(df: pd.DataFrame, decision_dt: datetime) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    ts_us = int(decision_dt.timestamp() * 1_000_000)
    for rec in df.itertuples(index=False):
        parsed = _parse_tardis_instrument(getattr(rec, "instrument_name", ""))
        if parsed is None:
            continue
        expiry, strike, is_call = parsed
        mark_iv = float(getattr(rec, "mark_iv", np.nan))
        if not np.isfinite(mark_iv) or mark_iv <= 0:
            continue
        spot = float(getattr(rec, "underlying_price", np.nan))
        if not np.isfinite(spot) or spot <= 0:
            continue
        sigma = mark_iv / 100.0
        t_years = _years_to_expiry(decision_dt, expiry)
        if not np.isfinite(t_years) or t_years <= 0:
            continue
        if is_call:
            delta = bs_call_delta(spot, strike, t_years, sigma)
        else:
            delta = bs_put_delta(spot, strike, t_years, sigma)
        rows.append({
            "timestamp": ts_us,
            "expiry": expiry,
            "strike": strike,
            "is_call": is_call,
            "mark_iv": mark_iv,
            "delta": float(delta),
        })
    if not rows:
        return pd.DataFrame(columns=list(_NORMALIZED_COLS))
    return pd.DataFrame(rows)


def _load_recorder_chain(path: Path, ts_us: int) -> pd.DataFrame:
    table = pq.read_table(
        path,
        columns=list(_NORMALIZED_COLS),
    )
    df = table.to_pandas()
    if df.empty:
        return df
    sub = df[df["timestamp"] == ts_us]
    if sub.empty:
        ts_arr = np.sort(df["timestamp"].unique())
        ts_arr = ts_arr[ts_arr <= ts_us]
        if len(ts_arr) == 0:
            return pd.DataFrame(columns=list(_NORMALIZED_COLS))
        use_ts = int(ts_arr[-1])
        sub = df[df["timestamp"] == use_ts]
    return sub.reset_index(drop=True)


def load_chain_at_clock(
    path: Path,
    entry_day: date,
    *,
    entry_hour: int,
    entry_minute: int,
) -> pd.DataFrame:
    """Normalized chain at or before the entry clock on ``entry_day``."""
    kind = detect_options_schema(path)
    decision_dt = datetime(
        entry_day.year, entry_day.month, entry_day.day,
        entry_hour, entry_minute, tzinfo=timezone.utc,
    )
    if kind == "recorder":
        return _load_recorder_chain(path, _snap_ts_us(entry_day, entry_hour, entry_minute))
    if kind == "tardis_ticker":
        df = pq.read_table(path).to_pandas()
        return _normalize_tardis_df(df, decision_dt)
    logger.warning(
        "options_snapshot: unsupported schema in %s (columns=%s)",
        path.name,
        pq.read_schema(path).names[:8],
    )
    return pd.DataFrame(columns=list(_NORMALIZED_COLS))


def _select_expiry_code(chain: pd.DataFrame, entry_day: date, dte: int) -> Optional[str]:
    target = entry_day + timedelta(days=dte)
    for exp in chain["expiry"].astype(str).unique():
        exp_dt = parse_expiry_date(exp)
        if exp_dt is not None and exp_dt.date() == target:
            return exp
    return None


def _iv_by_delta(side_df: pd.DataFrame, target_delta: float) -> float:
    if side_df.empty:
        return float("nan")
    work = side_df[np.isfinite(side_df["delta"]) & (side_df["delta"] != 0.0)]
    if work.empty:
        work = side_df
    if work.empty:
        return float("nan")
    idx = (work["delta"].astype(float) - target_delta).abs().idxmin()
    val = work.loc[idx, "mark_iv"]
    try:
        return float(val)
    except (TypeError, ValueError):
        return float("nan")


def iv_25d_for_entry_day(
    entry_day: date,
    options_dir: Path,
    *,
    entry_hour: int = 16,
    entry_minute: int = 0,
    dte: int = 1,
    delta: float = 0.25,
) -> tuple[float, float]:
    """Call and put 25Δ mark IV (%) on the exact-``dte`` expiry."""
    path = options_day_path(options_dir, entry_day)
    if path is None:
        return float("nan"), float("nan")
    chain = load_chain_at_clock(
        path,
        entry_day,
        entry_hour=entry_hour,
        entry_minute=entry_minute,
    )
    if chain.empty:
        return float("nan"), float("nan")
    expiry = _select_expiry_code(chain, entry_day, dte)
    if expiry is None:
        return float("nan"), float("nan")
    sub = chain[chain["expiry"].astype(str) == expiry]
    call_iv = _iv_by_delta(sub[sub["is_call"]], +abs(delta))
    put_iv = _iv_by_delta(sub[~sub["is_call"]], -abs(delta))
    return call_iv, put_iv

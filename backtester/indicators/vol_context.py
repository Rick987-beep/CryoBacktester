"""DVOL + realized-vol context for short-vol entry gates.

Loads synced Deribit BTC_DVOL from the data plane and builds a daily panel::

    dvol, rv30, vrp (= dvol - rv30), dvol_rank_60

``rv30`` is Parkinson 30-day annualised vol (%) from daily OHLC, shifted by
one calendar day so an intraday decision on date T only sees through T−1.
DVOL is asof-joined (last print with timestamp ≤ decision day).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from backtester.core.paths import dvol_dir

logger = logging.getLogger(__name__)

_PARKINSON_DENOM = 4.0 * np.log(2.0)


def load_dvol_series(root: Optional[Path] = None) -> pd.Series:
    """Load BTC_DVOL daily series (annualised IV %). Index: UTC midnight."""
    base = Path(root) if root is not None else dvol_dir()
    files = sorted(base.rglob("*.parquet"))
    if not files:
        logger.warning("vol_context: no DVOL parquets under %s", base)
        return pd.Series(dtype=float, name="dvol")

    parts: list[pd.DataFrame] = []
    for path in files:
        df = pd.read_parquet(path)
        if df.empty:
            continue
        parts.append(df)
    if not parts:
        return pd.Series(dtype=float, name="dvol")

    raw = pd.concat(parts, axis=0)
    if "value" in raw.columns:
        col = "value"
    elif "dvol" in raw.columns:
        col = "dvol"
    else:
        raise ValueError(f"DVOL parquet missing value/dvol columns: {list(raw.columns)}")

    if not isinstance(raw.index, pd.DatetimeIndex):
        # CryoQuant hive frames are indexed by timestamp
        if "timestamp" in raw.columns:
            raw = raw.set_index("timestamp")
        else:
            raise ValueError("DVOL parquet has no DatetimeIndex or timestamp column")

    s = raw[col].astype(float).sort_index()
    if s.index.tz is None:
        s.index = s.index.tz_localize("UTC")
    else:
        s.index = s.index.tz_convert("UTC")
    s = s[~s.index.duplicated(keep="last")].sort_index()
    s.name = "dvol"
    return s


def parkinson_rv30_daily(ohlc: pd.DataFrame) -> pd.Series:
    """30-day Parkinson RV annualised (%), causal shift(+1) on daily bars."""
    high = ohlc["high"].astype(float)
    low = ohlc["low"].astype(float)
    valid = (high > 0) & (low > 0) & (high >= low)
    park_var = pd.Series(np.nan, index=ohlc.index, dtype=float)
    park_var.loc[valid] = (np.log(high.loc[valid] / low.loc[valid]) ** 2) / _PARKINSON_DENOM
    # Mean daily variance over 30 calendar bars → annualise
    rv = np.sqrt(park_var.rolling(30, min_periods=10).mean() * 365.0) * 100.0
    # Closed-bar only: decision on day T uses RV through T-1
    return rv.shift(1).rename("rv30")


def dvol_rank(series: pd.Series, lookback: int = 60) -> pd.Series:
    min_periods = max(5, lookback // 4)
    return series.rolling(lookback, min_periods=min_periods).rank(pct=True)


def build_vol_context(
    df_raw: pd.DataFrame,
    *,
    dvol_root: Optional[Path] = None,
    rank_lookback: int = 60,
    **_params,
) -> pd.DataFrame:
    """Daily panel indexed by UTC midnight: dvol, rv30, vrp, dvol_rank_60.

    ``df_raw`` should be daily (or resampleable) OHLCV with UTC DatetimeIndex.
    """
    if df_raw.empty:
        return pd.DataFrame(columns=["dvol", "rv30", "vrp", "dvol_rank_60"])

    bars = df_raw.sort_index()
    if bars.index.tz is None:
        bars = bars.copy()
        bars.index = bars.index.tz_localize("UTC")

    # Resample to daily if intraday
    if len(bars) > 1:
        median_gap = bars.index.to_series().diff().median()
        if median_gap is not pd.NaT and median_gap < pd.Timedelta(hours=20):
            daily = bars.resample("1D").agg(
                {"open": "first", "high": "max", "low": "min", "close": "last"}
            ).dropna(subset=["close"])
        else:
            daily = bars[["open", "high", "low", "close"]].copy()
    else:
        daily = bars[["open", "high", "low", "close"]].copy()

    daily.index = daily.index.normalize()

    rv30 = parkinson_rv30_daily(daily)
    dvol = load_dvol_series(dvol_root)
    if not dvol.empty:
        dvol = dvol.copy()
        dvol.index = dvol.index.normalize()
        dvol = dvol[~dvol.index.duplicated(keep="last")].sort_index()

    out = pd.DataFrame(index=daily.index)
    out["rv30"] = rv30.reindex(daily.index)
    if dvol.empty:
        out["dvol"] = np.nan
    else:
        # Union index asof: reindex DVOL onto daily calendar with forward-fill
        # from prior prints (causal: only past/present DVOL values).
        union = dvol.reindex(dvol.index.union(daily.index)).sort_index()
        filled = union.ffill()
        out["dvol"] = filled.reindex(daily.index)

    out["vrp"] = out["dvol"] - out["rv30"]
    out["dvol_rank_60"] = dvol_rank(out["dvol"], lookback=rank_lookback)
    return out


def lookup_vol_context(panel: pd.DataFrame, dt: datetime) -> dict[str, float]:
    """Causal asof row for decision timestamp ``dt`` (UTC)."""
    empty = {
        "dvol": float("nan"),
        "rv30": float("nan"),
        "vrp": float("nan"),
        "dvol_rank_60": float("nan"),
    }
    if panel is None or panel.empty:
        return empty
    ts = pd.Timestamp(dt)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    day = ts.normalize()
    idx = panel.index[panel.index <= day]
    if len(idx) == 0:
        return empty
    row = panel.loc[idx[-1]]
    return {
        "dvol": float(row.get("dvol", np.nan)),
        "rv30": float(row.get("rv30", np.nan)),
        "vrp": float(row.get("vrp", np.nan)),
        "dvol_rank_60": float(row.get("dvol_rank_60", np.nan)),
    }

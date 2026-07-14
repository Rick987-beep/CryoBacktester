"""Per-session extrusion metrics on OHLC bars."""

from __future__ import annotations

import numpy as np
import pandas as pd


def range_extrusion(bars: pd.DataFrame) -> float:
    """(high - low) / open as fraction."""
    if bars.empty:
        return np.nan
    o = float(bars["open"].iloc[0])
    if o <= 0:
        return np.nan
    return (float(bars["high"].max()) - float(bars["low"].min())) / o


def path_extrusion(bars: pd.DataFrame) -> float:
    """max |close - open| / open over session."""
    if bars.empty:
        return np.nan
    o = float(bars["open"].iloc[0])
    if o <= 0:
        return np.nan
    path = (bars["close"] - o).abs().max()
    return float(path) / o


def slice_bars(bars: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """Half-open [start, end) slice on UTC index."""
    idx = bars.index
    if idx.tz is None:
        bars = bars.copy()
        bars.index = bars.index.tz_localize("UTC")
    mask = (bars.index >= start) & (bars.index < end)
    return bars.loc[mask]


def drop_weekend_bars(bars: pd.DataFrame) -> pd.DataFrame:
    """Remove Saturday/Sunday bars (UTC dayofweek 5, 6)."""
    if bars.empty:
        return bars
    return bars[bars.index.dayofweek < 5]


def normalize_extrusion(
    series: pd.Series,
    rolling: int = 60,
    *,
    exclude_weekends: bool = False,
    entry_dates: pd.Series | None = None,
) -> pd.Series:
    """Current value / rolling median of same series."""
    if exclude_weekends and entry_dates is not None:
        weekdays = pd.to_datetime(entry_dates).dt.dayofweek < 4
        baseline = series.where(weekdays)
    else:
        baseline = series
    med = baseline.rolling(rolling, min_periods=10).median()
    return series / med.replace(0, np.nan)


def quiet_tertile_flags(
    normalized: pd.Series,
    q: float = 0.33,
    *,
    rolling: int = 60,
    exclude_weekends: bool = False,
    entry_dates: pd.Series | None = None,
) -> pd.Series:
    """True when normalized extrusion below rolling tertile threshold."""
    if exclude_weekends and entry_dates is not None:
        weekdays = pd.to_datetime(entry_dates).dt.dayofweek < 4
        baseline = normalized.where(weekdays)
    else:
        baseline = normalized
    thresh = baseline.rolling(rolling, min_periods=10).quantile(q)
    return normalized < thresh

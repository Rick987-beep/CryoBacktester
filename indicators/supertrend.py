"""
SuperTrend Indicator — Long-call entry/exit signal on 1h BTC candles.

Faithful Python port of the SuperTrend variant described in
docs/Backtest_Report_SuperTrend_DTE30_Delta05_EN.md (section 4).

NOTE: This implementation uses the report's **2-bar range ATR approximation**
(NOT classical True Range). Replacing it with classical TR will shift signals.

  range[i]      = max(close[i], close[i-1]) − min(close[i], close[i-1])
  atr[i]        = SMA(range, period)              for i >= period
  upper_band[i] = close[i] + multiplier × atr[i]
  lower_band[i] = close[i] − multiplier × atr[i]

Trend state machine (initialized to +1):
  if close[i] > upper_band[i-1]:  trend = +1   (long signal active)
  elif close[i] < lower_band[i-1]: trend = -1  (close long)
  else: trend persists from previous bar.

Public API:
    supertrend(df, period=7, multiplier=3) -> DataFrame
        Returns the input frame's index with columns:
        [range, atr, upper_band, lower_band, trend, flip_up, flip_down]

    latest_signal(df, period=7, multiplier=3) -> dict | None
        Returns dict for the LAST FULLY CLOSED bar of df:
            {bar_ts, trend, flip_up, flip_down}
        Returns None if not enough bars to evaluate the trend state machine.
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# Default parameters (per the backtest report)
DEFAULT_PERIOD = 7
DEFAULT_MULTIPLIER = 3.0


def supertrend(
    df: pd.DataFrame,
    period: int = DEFAULT_PERIOD,
    multiplier: float = DEFAULT_MULTIPLIER,
    strict_first_cycle: bool = False,
) -> pd.DataFrame:
    """
    Compute SuperTrend(period, multiplier) using the report's 2-bar range ATR.

    Args:
        df: DataFrame with columns at least [close]. (high/low not required —
            the report's range only uses close[i] and close[i-1].)
            Index should be a sorted timestamp index, oldest first.
        period: ATR SMA window in bars (default 7).
        multiplier: Band multiplier (default 3.0).

    Returns:
        DataFrame indexed identically to `df` with columns:
            range, atr, upper_band, lower_band, trend, flip_up, flip_down

        - trend is integer +1 / -1. Before the state machine is initialized
          (i < period) trend is +1 by convention (matches the report).
        - flip_up is True at bars where trend transitioned -1 → +1.
        - flip_down is True at bars where trend transitioned +1 → -1.
    """
    if "close" not in df.columns:
        raise ValueError("supertrend(): df must have a 'close' column")

    n = len(df)
    out = pd.DataFrame(index=df.index)

    if n == 0:
        for col in ("range", "atr", "upper_band", "lower_band"):
            out[col] = pd.Series(dtype=float)
        out["trend"] = pd.Series(dtype=int)
        out["flip_up"] = pd.Series(dtype=bool)
        out["flip_down"] = pd.Series(dtype=bool)
        return out

    close = df["close"].astype(float).values
    prev_close = np.concatenate(([close[0]], close[:-1]))
    rng = np.maximum(close, prev_close) - np.minimum(close, prev_close)

    # SMA(period) over range — first (period-1) values are NaN, matching the
    # pseudocode's "if i >= period" guard.
    atr = pd.Series(rng).rolling(window=period, min_periods=period).mean().values

    upper = close + multiplier * atr
    lower = close - multiplier * atr

    trend = np.full(n, 1, dtype=int)  # initial state +1 (per the report)
    # State machine starts evaluating at index `period` (first bar where ATR
    # and the previous bar's bands are well-defined).
    for i in range(1, n):
        prev_upper = upper[i - 1]
        prev_lower = lower[i - 1]
        if np.isnan(prev_upper) or np.isnan(prev_lower):
            trend[i] = trend[i - 1]
            continue
        if close[i] > prev_upper:
            trend[i] = 1
        elif close[i] < prev_lower:
            trend[i] = -1
        else:
            trend[i] = trend[i - 1]

    flip_up = np.zeros(n, dtype=bool)
    flip_down = np.zeros(n, dtype=bool)
    # Only mark flips once the state machine is active (i >= period).
    if n > period:
        for i in range(period, n):
            if trend[i] == 1 and trend[i - 1] == -1:
                flip_up[i] = True
            elif trend[i] == -1 and trend[i - 1] == 1:
                flip_down[i] = True

    if strict_first_cycle:
        # Suppress flip_up on any bar before the trend has been -1 at least once.
        seen_bear = np.zeros(n, dtype=bool)
        for i in range(1, n):
            seen_bear[i] = seen_bear[i - 1] or (trend[i - 1] == -1)
        flip_up = flip_up & seen_bear

    out["range"] = rng
    out["atr"] = atr
    out["upper_band"] = upper
    out["lower_band"] = lower
    out["trend"] = trend
    out["flip_up"] = flip_up
    out["flip_down"] = flip_down
    return out


def latest_signal(
    df: pd.DataFrame,
    period: int = DEFAULT_PERIOD,
    multiplier: float = DEFAULT_MULTIPLIER,
    strict_first_cycle: bool = False,
) -> Optional[dict]:
    """
    Evaluate SuperTrend on the input bars and return a dict for the LAST bar.

    Caller is responsible for passing only fully-closed bars (e.g., resample
    to '1h' on closed minute data).

    Returns:
        {
            "bar_ts":    pd.Timestamp,   # index of last bar
            "trend":     int (+1 / -1),
            "flip_up":   bool,
            "flip_down": bool,
        }
        Or None when df has fewer than `period + 1` rows (state machine
        cannot evaluate).
    """
    if df is None or len(df) <= period:
        return None
    out = supertrend(df, period=period, multiplier=multiplier, strict_first_cycle=strict_first_cycle)
    last = out.iloc[-1]
    return {
        "bar_ts":    out.index[-1],
        "trend":     int(last["trend"]),
        "flip_up":   bool(last["flip_up"]),
        "flip_down": bool(last["flip_down"]),
    }

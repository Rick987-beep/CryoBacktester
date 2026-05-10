"""
Turbulence Indicator — Python port of market_wildness_v2.pine
(PineStrategy workspace)

Composite 0–100 score indicating market turbulence. Traffic light:
  Green  (0–35)  = calm, safe to sell
  Yellow (35–65) = caution
  Red    (65–100) = stay out

Input:  pandas DataFrame of OHLC at 15-minute resolution (any Binance spot symbol)
Output: DataFrame indexed at 1-hour timestamps with columns:
        [composite, vol_score, trend_score, burst_score, decay_score, signal]

Four components (weighted sum):
  A: Parkinson Realized Volatility   weight 0.40
  B: Kaufman Efficiency Ratio        weight 0.25
  C: Burst Detection                 weight 0.15
  D: Decay / Calm Detection          weight 0.20

Reference: /WorkspacePineStrategy/indicators/market_wildness_v2.pine
Spec:      /WorkspacePineStrategy/market_wildness_python_spec.md
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# =============================================================================
# Default Parameters
# =============================================================================

_DEFAULTS = {
    "w_vol":            0.40,
    "w_trend":          0.25,
    "w_burst":          0.15,
    "w_decay":          0.20,
    "thresh_green":     35,
    "thresh_red":       65,
    "vol_smooth":       4,      # SMA window (active 1H bars)
    "vol_lookback":     336,    # percentile lookback (~14 weekdays × 24h)
    "er_period":        6,      # Efficiency Ratio period (active 1H bars)
    "burst_atr_pct":    2.0,    # outsized candle threshold (% of daily ATR)
    "burst_max_ratio":  3.0,    # max/median range ratio cap
    "decay_short":      2,      # short vol SMA window (active 1H bars)
    "decay_long":       6,      # long vol SMA window (active 1H bars)
    "calm_bars_needed": 2,      # consecutive calm bars for full safe signal
}


# =============================================================================
# Internal Helpers
# =============================================================================

def _daily_atr(df_15m: pd.DataFrame, period: int = 14) -> pd.Series:
    """ATR(period) on daily OHLC resampled from 15m data."""
    daily = df_15m.resample("1D").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    ).dropna()
    prev_close = daily["close"].shift(1)
    tr = pd.concat(
        [
            daily["high"] - daily["low"],
            (daily["high"] - prev_close).abs(),
            (daily["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period).mean()


def _parkinson_per_hour(df_15m: pd.DataFrame) -> pd.Series:
    """
    Parkinson realized variance per 1H bar: sum of ln(H/L)^2 over sub-bars.
    Returns a Series indexed by 1H timestamps.
    """
    def _park(group: pd.DataFrame) -> float:
        valid = (group["high"] > 0) & (group["low"] > 0) & (group["high"] >= group["low"])
        if not valid.any():
            return np.nan
        return float(np.sum(np.log(group.loc[valid, "high"] / group.loc[valid, "low"]) ** 2))

    return df_15m.groupby(pd.Grouper(freq="1h")).apply(_park)


def _burst_per_hour(
    df_15m: pd.DataFrame,
    df_1h: pd.DataFrame,
    daily_atr_1h: pd.Series,
    burst_atr_pct: float,
    burst_max_ratio: float,
    is_weekend: pd.Series,
) -> pd.Series:
    """
    Burst score (0–100) per 1H bar from 15m sub-bars.
    NaN on weekend bars.
    """
    scores = {}
    for hour_ts, group in df_15m.groupby(pd.Grouper(freq="1h")):
        if hour_ts not in df_1h.index or len(group) == 0:
            continue

        if is_weekend.get(hour_ts, False):
            scores[hour_ts] = np.nan
            continue

        atr_val = daily_atr_1h.get(hour_ts, np.nan)
        if np.isnan(atr_val) or atr_val <= 0:
            scores[hour_ts] = np.nan
            continue

        ranges = (group["high"] - group["low"]).values
        if len(ranges) == 0:
            scores[hour_ts] = np.nan
            continue

        threshold = atr_val * burst_atr_pct / 100.0
        count = float(np.sum(ranges > threshold))
        max_r = float(np.max(ranges))
        med_r = float(np.median(ranges))

        count_score = min(count / 3.0, 1.0) * 100.0

        ratio_score = 0.0
        if med_r > 0:
            ratio = max_r / med_r
            ratio_score = max(min((ratio - 1.0) / (burst_max_ratio - 1.0), 1.0), 0.0) * 100.0

        scores[hour_ts] = 0.6 * count_score + 0.4 * ratio_score

    return pd.Series(scores).reindex(df_1h.index)


def _calm_streak(is_active: np.ndarray, is_calm: np.ndarray) -> np.ndarray:
    """
    Consecutive calm bar streak, weekday-only. Weekend bars preserve streak
    (do not reset, do not increment). Mirrors Pine `var int calm_streak` logic.
    """
    streak_arr = np.zeros(len(is_active), dtype=int)
    streak = 0
    for i in range(len(is_active)):
        if is_active[i]:
            streak = streak + 1 if is_calm[i] else 0
        # Weekend: leave streak unchanged
        streak_arr[i] = streak
    return streak_arr


# =============================================================================
# Public API
# =============================================================================

def turbulence(
    df_15m: pd.DataFrame,
    exclude_weekends: bool = True,
    **params,
) -> pd.DataFrame:
    """
    Compute the Turbulence composite indicator from 15-minute OHLC data.

    Args:
        df_15m: DataFrame with columns [open, high, low, close] at 15m
                resolution. Index must be a DatetimeIndex (UTC recommended).
        exclude_weekends: Skip Saturday/Sunday from all baseline calculations.
                          Weekend bars output NaN. Default True.
        **params: Override any default parameter (see _DEFAULTS).

    Returns:
        DataFrame indexed by 1H timestamps with columns:
        - composite:   0–100 composite turbulence score
        - vol_score:   Component A (Parkinson volatility percentile)
        - trend_score: Component B (Kaufman Efficiency Ratio × 100)
        - burst_score: Component C (outsized sub-bar detection)
        - decay_score: Component D (short/long vol ratio + calm streak)
        - signal:      'green' | 'yellow' | 'red' | None (weekends)
    """
    cfg = {**_DEFAULTS, **params}

    w_vol   = cfg["w_vol"]
    w_trend = cfg["w_trend"]
    w_burst = cfg["w_burst"]
    w_decay = cfg["w_decay"]
    thresh_green     = cfg["thresh_green"]
    thresh_red       = cfg["thresh_red"]
    vol_smooth       = cfg["vol_smooth"]
    vol_lookback     = cfg["vol_lookback"]
    er_period        = cfg["er_period"]
    burst_atr_pct    = cfg["burst_atr_pct"]
    burst_max_ratio  = cfg["burst_max_ratio"]
    decay_short      = cfg["decay_short"]
    decay_long       = cfg["decay_long"]
    calm_bars_needed = cfg["calm_bars_needed"]

    # -------------------------------------------------------------------------
    # Resample to 1H
    # -------------------------------------------------------------------------
    df_1h = df_15m.resample("1h").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    ).dropna()

    is_weekend_mask = df_1h.index.dayofweek >= 5  # 5=Sat, 6=Sun
    is_weekend = pd.Series(is_weekend_mask, index=df_1h.index)
    is_active  = pd.Series(~is_weekend_mask if exclude_weekends else np.ones(len(df_1h), dtype=bool), index=df_1h.index)

    # -------------------------------------------------------------------------
    # Daily ATR(14) — mapped forward to 1H index
    # -------------------------------------------------------------------------
    daily_atr_series = _daily_atr(df_15m, period=14)
    daily_atr_1h = daily_atr_series.reindex(df_1h.index, method="ffill")

    # -------------------------------------------------------------------------
    # Parkinson raw per 1H bar (from 15m sub-bars)
    # -------------------------------------------------------------------------
    park_raw = _parkinson_per_hour(df_15m).reindex(df_1h.index)

    # Active-bars-only series
    park_active = park_raw[is_active]

    # =========================================================================
    # Component A: Parkinson Realized Volatility (score 0–100)
    # =========================================================================
    # 1. Smooth with SMA(vol_smooth) over active bars
    park_smooth = park_active.rolling(vol_smooth, min_periods=1).mean()

    # 2. Percentile rank within rolling vol_lookback window of active bars
    #    "What % of historical smoothed values are <= current?"
    vol_score_active = park_smooth.rolling(vol_lookback, min_periods=2).apply(
        lambda arr: (arr <= arr[-1]).sum() / len(arr) * 100.0,
        raw=True,
    )
    vol_score = vol_score_active.reindex(df_1h.index)

    logger.debug(
        "Component A: vol_score range [%.1f, %.1f]",
        vol_score.min(), vol_score.max(),
    )

    # =========================================================================
    # Component B: Kaufman Efficiency Ratio (score 0–100)
    # =========================================================================
    # ER = |close_now - close_N_bars_ago| / sum(|step changes over N bars|)
    # Window of er_period + 1 active closes covers er_period steps.
    close_active = df_1h.loc[is_active, "close"]

    def _er(arr: np.ndarray) -> float:
        direction = abs(arr[-1] - arr[0])
        path = float(np.abs(np.diff(arr)).sum())
        return (direction / path * 100.0) if path > 0 else 0.0

    trend_score_active = close_active.rolling(er_period + 1, min_periods=er_period + 1).apply(
        _er, raw=True
    )
    trend_score = trend_score_active.reindex(df_1h.index)

    logger.debug(
        "Component B: trend_score range [%.1f, %.1f]",
        trend_score.min(), trend_score.max(),
    )

    # =========================================================================
    # Component C: Burst Detection (score 0–100)
    # =========================================================================
    burst_score = _burst_per_hour(
        df_15m, df_1h, daily_atr_1h, burst_atr_pct, burst_max_ratio, is_weekend
    )

    logger.debug(
        "Component C: burst_score range [%.1f, %.1f]",
        burst_score.min(), burst_score.max(),
    )

    # =========================================================================
    # Component D: Decay / Calm Detection (score 0–100)
    # =========================================================================

    # D3: Short/long vol ratio from weekday Parkinson array
    park_short_sma = park_active.rolling(decay_short, min_periods=1).mean()
    park_long_sma  = park_active.rolling(decay_long,  min_periods=1).mean()
    vol_ratio = park_short_sma / park_long_sma.replace(0.0, np.nan)
    vol_ratio = vol_ratio.fillna(1.0)  # default to 1.0 when long avg is zero (matches Pine)
    ratio_decay_active = np.clip((vol_ratio - 0.5) / 1.0, 0.0, 1.0) * 100.0
    ratio_decay_score = ratio_decay_active.reindex(df_1h.index)  # NaN on weekends

    # D2: Consecutive calm bars
    # "Calm" = current 1H range < median of weekday ranges in lookback window
    bar_range_1h   = df_1h["high"] - df_1h["low"]
    range_active   = bar_range_1h[is_active]
    median_range   = range_active.rolling(vol_lookback, min_periods=1).median()
    # Forward-fill median over weekends so is_calm can be computed on all bars
    median_range_full = median_range.reindex(df_1h.index, method="ffill")

    is_calm_full = (bar_range_1h < median_range_full).values

    streak_arr = _calm_streak(is_active.values, is_calm_full)
    calm_streak_series = pd.Series(streak_arr, index=df_1h.index)

    calm_score_full = np.maximum(1.0 - (calm_streak_series / calm_bars_needed), 0.0) * 100.0
    if exclude_weekends:
        calm_score_full[is_weekend] = np.nan

    decay_score = 0.5 * ratio_decay_score + 0.5 * calm_score_full

    logger.debug(
        "Component D: decay_score range [%.1f, %.1f]",
        decay_score.min(), decay_score.max(),
    )

    # =========================================================================
    # Composite Score
    # =========================================================================
    # nz() equivalent: fill NaN components with 0 before weighting,
    # then blank out weekend bars (matches Pine `is_active ? ... : na`)
    composite = (
        w_vol   * vol_score.fillna(0.0)
        + w_trend * trend_score.fillna(0.0)
        + w_burst * burst_score.fillna(0.0)
        + w_decay * decay_score.fillna(0.0)
    )
    if exclude_weekends:
        composite[is_weekend] = np.nan

    # =========================================================================
    # Traffic Light Signal
    # =========================================================================
    def _signal(v: float):
        if pd.isna(v):
            return None
        if v >= thresh_red:
            return "red"
        if v >= thresh_green:
            return "yellow"
        return "green"

    signal = composite.map(_signal)

    return pd.DataFrame(
        {
            "composite":   composite,
            "vol_score":   vol_score,
            "trend_score": trend_score,
            "burst_score": burst_score,
            "decay_score": decay_score,
            "signal":      signal,
        },
        index=df_1h.index,
    )


# =============================================================================
# Convenience Entry Point
# =============================================================================

def get_turbulence_now(
    symbol: str = "BTCUSDT",
    exclude_weekends: bool = True,
    lookback_bars: int = 1500,
    **params,
) -> Optional[dict]:
    """
    Fetch the latest 15m klines and return the current turbulence reading.

    Uses indicators.data.fetch_klines() with interval="15m". The indicator
    internally resamples to 1H for all calculations.

    Args:
        symbol:        Binance spot symbol, e.g. "BTCUSDT", "ETHUSDT".
        exclude_weekends: Skip weekends from baseline calculations.
        lookback_bars: Number of 15m bars to fetch (default 1500 ≈ 15 days).
        **params:      Override any turbulence default parameter.

    Returns:
        Dict with keys [composite, vol_score, trend_score, burst_score,
        decay_score, signal] for the most recent completed 1H bar,
        or None if data is unavailable.
    """
    from indicators.data import fetch_klines

    df_15m = fetch_klines(symbol=symbol, interval="15m", lookback_bars=lookback_bars)
    if df_15m is None or df_15m.empty:
        logger.error("get_turbulence_now(%s): no data", symbol)
        return None

    result = turbulence(df_15m, exclude_weekends=exclude_weekends, **params)
    # Use the last non-NaN row (current completed bar)
    latest = result.dropna(subset=["composite"])
    if latest.empty:
        return None

    row = latest.iloc[-1]
    return {
        "timestamp":   row.name.isoformat(),
        "composite":   round(float(row["composite"]), 2),
        "vol_score":   round(float(row["vol_score"]),   2) if pd.notna(row["vol_score"])   else None,
        "trend_score": round(float(row["trend_score"]), 2) if pd.notna(row["trend_score"]) else None,
        "burst_score": round(float(row["burst_score"]), 2) if pd.notna(row["burst_score"]) else None,
        "decay_score": round(float(row["decay_score"]), 2) if pd.notna(row["decay_score"]) else None,
        "signal":      row["signal"],
    }

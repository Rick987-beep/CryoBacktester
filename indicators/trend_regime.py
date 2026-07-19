"""
trend_regime — 3-state BTC trend composite (+1 / 0 / −1).

Port of cryoquant ``trend_regime`` (ADX + EMA stack + Donchian position).
No cryoquant dependency.

Raw regime (same-bar):
  +1: ADX>25 AND EMA20>EMA50>EMA200 AND dc_pos>0.7
  −1: ADX>25 AND EMA20<EMA50<EMA200 AND dc_pos<0.3
   0: otherwise

``compute_trend_regime`` returns a DataFrame with component columns and:
  - ``regime_raw`` — unshifted same-bar regime
  - ``regime``     — ``regime_raw.shift(1)`` (closed-bar safe for strategy use)

Donchian uses prior closes internally (levels known at bar open).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd

# Locked cryoquant defaults
ADX_N = 14
ADX_THRESHOLD = 25.0
EMA_FAST = 20
EMA_MID = 50
EMA_SLOW = 200
DONCHIAN_N = 55
DC_POS_BULL = 0.7
DC_POS_BEAR = 0.3


def _seeded_recursive_ma(s: pd.Series, n: int, alpha: float) -> pd.Series:
    """Pine-faithful SMA-seeded recursive MA (EMA / RMA)."""
    if n < 1:
        raise ValueError("n must be >= 1")
    arr = s.to_numpy(dtype=float)
    out = np.full_like(arr, np.nan)
    if len(arr) < n:
        return pd.Series(out, index=s.index)
    seed_window = arr[:n]
    seed = np.where(np.isnan(seed_window), 0.0, seed_window).sum() / n
    out[n - 1] = seed
    for i in range(n, len(arr)):
        x = arr[i]
        prev = out[i - 1]
        out[i] = prev if np.isnan(x) else alpha * x + (1.0 - alpha) * prev
    return pd.Series(out, index=s.index)


def ema(s: pd.Series, n: int) -> pd.Series:
    """SMA-seeded EMA, alpha = 2/(n+1)."""
    return _seeded_recursive_ma(s, n, 2.0 / (n + 1.0))


def rma(s: pd.Series, n: int) -> pd.Series:
    """Wilder RMA, alpha = 1/n."""
    return _seeded_recursive_ma(s, n, 1.0 / n)


def tr(df: pd.DataFrame) -> pd.Series:
    """True range."""
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_c = close.shift(1)
    a = high - low
    b = (high - prev_c).abs()
    c = (low - prev_c).abs()
    out = pd.concat([a, b, c], axis=1).max(axis=1)
    if len(out):
        out.iloc[0] = high.iloc[0] - low.iloc[0]
    return out


def adx(df: pd.DataFrame, n: int = ADX_N) -> pd.Series:
    """Wilder-smoothed ADX (0–100)."""
    high = df["high"]
    low = df["low"]
    prev_high = high.shift(1)
    prev_low = low.shift(1)

    dm_plus_raw = high - prev_high
    dm_minus_raw = prev_low - low

    dm_plus = np.where(
        (dm_plus_raw > dm_minus_raw) & (dm_plus_raw > 0), dm_plus_raw, 0.0
    )
    dm_minus = np.where(
        (dm_minus_raw > dm_plus_raw) & (dm_minus_raw > 0), dm_minus_raw, 0.0
    )

    dm_plus_s = pd.Series(dm_plus, index=high.index)
    dm_minus_s = pd.Series(dm_minus, index=high.index)
    tr_s = tr(df)

    smooth_tr = rma(tr_s, n)
    smooth_plus = rma(dm_plus_s, n)
    smooth_minus = rma(dm_minus_s, n)

    di_plus = 100.0 * smooth_plus / smooth_tr.replace(0, np.nan)
    di_minus = 100.0 * smooth_minus / smooth_tr.replace(0, np.nan)

    di_sum = (di_plus + di_minus).replace(0, np.nan)
    dx = 100.0 * (di_plus - di_minus).abs() / di_sum
    return rma(dx, n).rename("adx")


def donchian(close: pd.Series, n: int = DONCHIAN_N) -> pd.DataFrame:
    """Donchian of prior closes — levels known at bar open."""
    shifted = close.shift(1)
    upper = shifted.rolling(n, min_periods=n).max()
    lower = shifted.rolling(n, min_periods=n).min()
    mid = (upper + lower) / 2.0
    return pd.DataFrame({"upper": upper, "lower": lower, "mid": mid}, index=close.index)


def _drop_incomplete_last_bar(
    df: pd.DataFrame,
    asof: Optional[datetime] = None,
) -> pd.DataFrame:
    """Drop the last daily bar if it has not fully closed yet (optional)."""
    if df.empty:
        return df
    if asof is None:
        asof = datetime.now(tz=timezone.utc)
    elif asof.tzinfo is None:
        asof = asof.replace(tzinfo=timezone.utc)
    last = df.index[-1]
    bar_close = last + timedelta(days=1)
    if asof < bar_close:
        return df.iloc[:-1]
    return df


def _freeze_incomplete_last_bar(
    out: pd.DataFrame,
    asof: Optional[datetime] = None,
) -> pd.DataFrame:
    """Zero ``regime_raw`` on a still-forming last daily bar; keep the row.

    Keeping the index is required so strategy lookups on calendar day ``D``
    (key = D 00:00) still resolve while bar ``D`` is open.  Only ``regime_raw``
    on that forming bar is cleared so tomorrow's shifted ``regime`` will not
    inherit an incomplete signal.
    """
    if out.empty:
        return out
    if asof is None:
        asof = datetime.now(tz=timezone.utc)
    elif asof.tzinfo is None:
        asof = asof.replace(tzinfo=timezone.utc)
    last = out.index[-1]
    bar_close = last + timedelta(days=1)
    if asof < bar_close:
        out = out.copy()
        out.loc[last, "regime_raw"] = 0.0
        # Recompute shifted regime so last and next semantics stay consistent
        out["regime"] = out["regime_raw"].shift(1).fillna(0.0)
    return out


def compute_trend_regime(
    df: pd.DataFrame,
    *,
    asof: Optional[datetime] = None,
    drop_incomplete: bool = False,
    freeze_incomplete: bool = True,
) -> pd.DataFrame:
    """Compute trend regime and component columns from OHLC.

    Parameters
    ----------
    df:
        OHLC with columns open/high/low/close (volume optional), UTC index.
    asof:
        Wall-clock / replay time used to freeze a still-forming last daily bar.
        Defaults to now (UTC).
    drop_incomplete:
        Deprecated path: drop last bar if still forming (removes lookup key —
        prefer ``freeze_incomplete``).
    freeze_incomplete:
        If True, keep the forming last bar but force its ``regime_raw`` to 0
        and refresh ``regime``.

    Returns
    -------
    DataFrame with columns:
      close, ema_20, ema_50, ema_200, adx_14, dc_upper, dc_lower, dc_pos,
      regime_raw, regime
    ``regime`` is ``regime_raw.shift(1)`` with NaN filled to 0.
    """
    required = {"high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"trend_regime requires columns {sorted(required)}; missing {missing}")

    work = df.copy()
    if drop_incomplete:
        work = _drop_incomplete_last_bar(work, asof=asof)
    if work.empty:
        return pd.DataFrame(
            columns=[
                "close", "ema_20", "ema_50", "ema_200", "adx_14",
                "dc_upper", "dc_lower", "dc_pos", "regime_raw", "regime",
            ]
        )

    close = work["close"].astype(float)
    ema_20 = ema(close, EMA_FAST)
    ema_50 = ema(close, EMA_MID)
    ema_200 = ema(close, EMA_SLOW)
    adx_val = adx(work, ADX_N)
    dc = donchian(close, DONCHIAN_N)
    dc_range = (dc["upper"] - dc["lower"]).replace(0, np.nan)
    dc_pos = (close - dc["lower"]) / dc_range

    trending = adx_val > ADX_THRESHOLD
    bullish = (
        trending
        & (ema_20 > ema_50)
        & (ema_50 > ema_200)
        & (dc_pos > DC_POS_BULL)
    )
    bearish = (
        trending
        & (ema_20 < ema_50)
        & (ema_50 < ema_200)
        & (dc_pos < DC_POS_BEAR)
    )

    regime_raw = pd.Series(0.0, index=work.index, dtype=float)
    valid = (
        ema_20.notna()
        & ema_50.notna()
        & ema_200.notna()
        & adx_val.notna()
        & dc_pos.notna()
    )
    regime_raw[valid & bullish] = 1.0
    regime_raw[valid & bearish] = -1.0

    out = pd.DataFrame(
        {
            "close": close,
            "ema_20": ema_20,
            "ema_50": ema_50,
            "ema_200": ema_200,
            "adx_14": adx_val,
            "dc_upper": dc["upper"],
            "dc_lower": dc["lower"],
            "dc_pos": dc_pos,
            "regime_raw": regime_raw,
            "regime": regime_raw.shift(1).fillna(0.0),
        },
        index=work.index,
    )
    if freeze_incomplete:
        out = _freeze_incomplete_last_bar(out, asof=asof)
    return out

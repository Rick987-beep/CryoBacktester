"""OHLC + Wilder ATR on cached klines (no live fetch)."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def build_ohlc_atr(df_raw: pd.DataFrame, *, period: int = 14, **_params: Any) -> pd.DataFrame:
    """Return OHLC plus ATR(*period*) aligned to *df_raw* index.

    Expects columns ``open`` / ``high`` / ``low`` / ``close`` (Binance kline cache).
    """
    period = int(period)
    if period < 1:
        raise ValueError("atr period must be >= 1, got %r" % period)
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(
            columns=["open", "high", "low", "close", "range", "atr"]
        )

    df = df_raw.copy()
    for col in ("open", "high", "low", "close"):
        if col not in df.columns:
            raise ValueError("ohlc_atr requires column %r" % col)

    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # Wilder ATR: first value = SMA(TR, period), then recursive smooth.
    atr = pd.Series(np.nan, index=df.index, dtype=float)
    if len(tr) >= period:
        seed = float(tr.iloc[:period].mean())
        atr.iloc[period - 1] = seed
        for i in range(period, len(tr)):
            atr.iloc[i] = (atr.iloc[i - 1] * (period - 1) + float(tr.iloc[i])) / period

    out = pd.DataFrame(
        {
            "open": df["open"].astype(float),
            "high": high,
            "low": low,
            "close": close,
            "range": (high - low).astype(float),
            "atr": atr,
        },
        index=df.index,
    )
    logger.debug("ohlc_atr: %d bars period=%d", len(out), period)
    return out


def lookup_ohlc_atr(frame: pd.DataFrame | None, ts) -> dict[str, float] | None:
    """Last bar at or before *ts*; returns open/high/low/close/range/atr or None."""
    if frame is None or getattr(frame, "empty", True):
        return None
    try:
        sub = frame.loc[:ts]
    except Exception:
        return None
    if sub.empty:
        return None
    row = sub.iloc[-1]
    atr = float(row["atr"]) if pd.notna(row["atr"]) else float("nan")
    if not np.isfinite(atr) or atr <= 0:
        return None
    return {
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "range": float(row["range"]),
        "atr": atr,
    }

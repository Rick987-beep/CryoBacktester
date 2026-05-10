"""
backtester/indicators.py — Indicator pre-computation for backtesting.

Strategies declare which indicators they need via the ``indicator_deps``
class attribute. The engine calls ``build_indicators()`` once before the
grid replay starts, and injects the result into every strategy instance
via ``strategy.set_indicators(ind)``.

Usage in a strategy::

    from backtester.indicators import IndicatorDep

    class MyStrategy:
        indicator_deps = [
            IndicatorDep(name="turbulence", symbol="BTCUSDT", interval="15m"),
        ]

        def set_indicators(self, ind):
            self._turbulence = ind.get("turbulence")

        def on_market_state(self, state):
            if self._turbulence is not None:
                hour_ts = state.dt.replace(minute=0, second=0, microsecond=0)
                try:
                    row = self._turbulence.loc[hour_ts]
                    if row["signal"] == "red":
                        return []
                except KeyError:
                    pass
            ...

Adding a new indicator
----------------------
Register it in the ``_BUILDERS`` dict at the bottom of this file:

    _BUILDERS["my_indicator"] = _build_my_indicator

where ``_build_my_indicator(df_raw, **params)`` takes a kline DataFrame
and returns a DataFrame/Series indexed by the relevant timestamps.
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple, Union

import pandas as pd

from indicators.hist_data import load_klines
from indicators.supertrend import supertrend as _supertrend
from indicators.turbulence import turbulence as _turbulence

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

@dataclass
class IndicatorDep:
    """
    Declares one indicator dependency for a strategy.

    Attributes:
        name:     Key used in the ``indicators`` dict passed to
                  ``strategy.set_indicators()``.  Must match a registered
                  builder in ``_BUILDERS``.
        symbol:   Binance spot symbol, e.g. ``"BTCUSDT"``.
        interval: Kline interval required by the indicator, e.g. ``"15m"``.
        params:   Optional keyword arguments forwarded to the builder function.
        warmup_days: Extra history before the backtest start date needed for
                  the indicator's rolling windows to warm up fully.
                  Default 30 days covers Turbulence (14-day lookback).
    """
    name: str
    symbol: str
    interval: str
    params: Dict[str, Any] = field(default_factory=dict)
    warmup_days: int = 30


# ---------------------------------------------------------------------------
# Long-Gamma Regime indicator
# Self-contained: types, whitelist constants, helpers, main function, pairing.
# Spec: backtester/newstrategy/coincall_signal_schedule_bull/STRATEGY_SPEC.md
# ---------------------------------------------------------------------------

DateSet = Union[Set[date], FrozenSet[date]]
PairList = List[Tuple[pd.Timestamp, pd.Timestamp]]

# Authoritative historical whitelists — embedded, no external file required.
# Source: delivered by strategy author, May 2026.
BULL_WHITELIST_DATES: FrozenSet[date] = frozenset(
    date.fromisoformat(d) for d in [
        "2024-10-24", "2024-10-25", "2024-10-26", "2024-10-27", "2024-10-28",
        "2024-10-29", "2024-10-30", "2024-10-31", "2024-11-01", "2024-11-02",
        "2024-11-03", "2024-11-04", "2024-11-05", "2024-11-06", "2024-11-07",
        "2024-11-08", "2024-11-09", "2024-11-10", "2024-11-11", "2024-11-12",
        "2024-11-13", "2024-11-14", "2024-11-15", "2024-11-16", "2024-11-17",
        "2024-11-18", "2024-11-19", "2024-11-20", "2024-11-21", "2024-11-22",
        "2024-11-23", "2024-11-24", "2024-11-25", "2024-11-26", "2024-11-27",
        "2024-11-28", "2024-11-29", "2024-11-30", "2024-12-01", "2024-12-02",
        "2024-12-03", "2024-12-04", "2024-12-05", "2024-12-06", "2024-12-07",
        "2024-12-08", "2024-12-09", "2024-12-10", "2024-12-11", "2024-12-12",
        "2024-12-13", "2024-12-14", "2024-12-15", "2024-12-16", "2025-01-15",
        "2025-01-16", "2025-01-17", "2025-01-18", "2025-01-19", "2025-01-20",
        "2025-01-21", "2025-04-08", "2025-04-09", "2025-04-10", "2025-04-11",
        "2025-04-12", "2025-04-13", "2025-04-14", "2025-04-15", "2025-04-16",
        "2025-04-17", "2025-04-18", "2025-04-19", "2025-04-20", "2025-04-21",
        "2025-04-22", "2025-04-23", "2025-04-24", "2025-04-25", "2025-04-26",
        "2025-04-27", "2025-04-28", "2025-04-29", "2025-04-30", "2025-05-01",
        "2025-05-02", "2025-05-03", "2025-05-04", "2025-05-05", "2025-05-06",
        "2025-05-07", "2025-05-08", "2025-05-09", "2025-05-10", "2025-05-11",
        "2025-05-12", "2025-05-13", "2025-05-14", "2025-05-15", "2025-05-16",
        "2025-05-17", "2025-05-18", "2025-05-19", "2025-05-20", "2025-05-21",
        "2025-05-22", "2025-05-23", "2025-05-24", "2025-05-25", "2025-05-26",
        "2025-05-27", "2025-05-28", "2025-05-29", "2025-07-10", "2025-07-11",
        "2025-07-12", "2025-07-13", "2025-07-14", "2025-07-15", "2025-07-16",
        "2025-07-17", "2025-08-04", "2025-08-05", "2025-08-06", "2025-08-07",
        "2025-08-08", "2025-08-09", "2025-08-10", "2025-08-11", "2025-08-12",
        "2025-08-13", "2025-08-14", "2025-09-08", "2025-09-09", "2025-09-10",
        "2025-09-11", "2025-09-12", "2025-09-13", "2025-09-14", "2025-09-15",
        "2025-09-16", "2025-09-17", "2025-09-18", "2025-09-19", "2025-09-29",
        "2025-09-30", "2025-10-01", "2025-10-02", "2025-10-03", "2025-10-04",
        "2025-10-05", "2026-03-01", "2026-03-02", "2026-03-03", "2026-03-04",
        "2026-03-05", "2026-03-06", "2026-03-07", "2026-03-08", "2026-03-09",
        "2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13", "2026-03-14",
        "2026-03-15", "2026-03-16", "2026-04-02", "2026-04-03", "2026-04-04",
        "2026-04-05", "2026-04-06", "2026-04-07", "2026-04-08", "2026-04-09",
        "2026-04-10", "2026-04-11", "2026-04-12", "2026-04-13", "2026-04-14",
        "2026-04-15", "2026-04-16", "2026-04-17", "2026-04-18", "2026-04-19",
        "2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24",
    ]
)

BEAR_WHITELIST_DATES: FrozenSet[date] = frozenset(
    date.fromisoformat(d) for d in [
        "2024-12-17", "2024-12-18", "2024-12-19", "2024-12-20", "2024-12-21",
        "2024-12-22", "2024-12-23", "2024-12-24", "2024-12-25", "2024-12-26",
        "2024-12-27", "2024-12-28", "2024-12-29", "2024-12-30", "2024-12-31",
        "2025-01-01", "2025-01-22", "2025-01-23", "2025-01-24", "2025-01-25",
        "2025-01-26", "2025-01-27", "2025-01-28", "2025-01-29", "2025-01-30",
        "2025-01-31", "2025-02-01", "2025-02-02", "2025-02-03", "2025-02-04",
        "2025-02-05", "2025-02-06", "2025-02-07", "2025-02-08", "2025-02-09",
        "2025-02-10", "2025-02-11", "2025-02-12", "2025-02-13", "2025-02-14",
        "2025-02-15", "2025-02-16", "2025-02-17", "2025-02-18", "2025-02-19",
        "2025-02-20", "2025-02-21", "2025-02-22", "2025-02-23", "2025-02-24",
        "2025-02-25", "2025-02-26", "2025-02-27", "2025-02-28", "2025-03-01",
        "2025-03-02", "2025-03-03", "2025-03-04", "2025-03-05", "2025-03-06",
        "2025-03-07", "2025-03-08", "2025-03-09", "2025-03-10", "2025-03-11",
        "2025-03-12", "2025-03-13", "2025-03-14", "2025-03-15", "2025-03-16",
        "2025-03-17", "2025-03-18", "2025-03-19", "2025-03-20", "2025-03-21",
        "2025-03-22", "2025-03-23", "2025-03-24", "2025-03-25", "2025-03-26",
        "2025-03-27", "2025-03-28", "2025-03-29", "2025-03-30", "2025-03-31",
        "2025-04-01", "2025-04-02", "2025-04-03", "2025-04-04", "2025-04-05",
        "2025-04-06", "2025-04-07", "2025-06-12", "2025-06-13", "2025-06-14",
        "2025-06-15", "2025-06-16", "2025-06-17", "2025-06-18", "2025-06-19",
        "2025-06-20", "2025-06-21", "2025-06-22", "2025-06-23", "2025-06-24",
        "2025-08-15", "2025-08-16", "2025-08-17", "2025-08-18", "2025-08-19",
        "2025-08-20", "2025-08-21", "2025-08-22", "2025-08-23", "2025-08-24",
        "2025-08-25", "2025-08-26", "2025-08-27", "2025-08-28", "2025-08-29",
        "2025-08-30", "2025-08-31", "2025-09-01", "2025-09-02", "2025-09-22",
        "2025-09-23", "2025-09-24", "2025-09-25", "2025-09-26", "2025-09-27",
        "2025-09-28", "2025-10-06", "2025-10-07", "2025-10-08", "2025-10-09",
        "2025-10-10", "2025-10-11", "2025-10-12", "2025-10-13", "2025-10-14",
        "2025-10-15", "2025-10-16", "2025-10-17", "2025-10-18", "2025-10-19",
        "2025-10-20", "2025-10-21", "2025-10-22", "2025-10-23", "2025-10-24",
        "2025-10-25", "2025-10-26", "2025-10-27", "2025-10-28", "2025-10-29",
        "2025-10-30", "2025-10-31", "2025-11-01", "2025-11-02", "2025-11-03",
        "2025-11-04", "2025-11-05", "2025-11-06", "2025-11-07", "2025-11-08",
        "2025-11-09", "2025-11-10", "2025-11-11", "2025-11-12", "2025-11-13",
        "2025-11-14", "2025-11-15", "2025-11-16", "2025-11-17", "2025-11-18",
        "2025-11-19", "2025-11-20", "2025-11-21", "2026-01-14", "2026-01-15",
        "2026-01-16", "2026-01-17", "2026-01-18", "2026-01-19", "2026-01-20",
        "2026-01-21", "2026-01-22", "2026-01-23", "2026-01-24", "2026-01-25",
        "2026-01-26", "2026-01-27", "2026-01-28", "2026-01-29", "2026-01-30",
        "2026-01-31", "2026-02-01", "2026-02-02", "2026-02-03", "2026-02-04",
        "2026-02-05", "2026-02-06", "2026-02-07", "2026-02-08", "2026-02-09",
        "2026-02-10", "2026-02-11", "2026-02-12", "2026-02-13", "2026-02-14",
        "2026-02-15", "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19",
        "2026-02-20", "2026-02-21", "2026-02-22", "2026-02-23", "2026-02-24",
        "2026-02-25", "2026-02-26", "2026-02-27", "2026-02-28", "2026-03-17",
        "2026-03-18", "2026-03-19", "2026-03-20", "2026-03-21", "2026-03-22",
        "2026-03-23", "2026-03-24", "2026-03-25", "2026-03-26", "2026-03-27",
        "2026-03-28", "2026-03-29", "2026-03-30", "2026-03-31", "2026-04-01",
    ]
)


def _wilder_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0)
    dn = (-delta).clip(lower=0)
    avg_up = up.ewm(com=period - 1, adjust=False, min_periods=period).mean()
    avg_dn = dn.ewm(com=period - 1, adjust=False, min_periods=period).mean()
    rs = avg_up / avg_dn.replace(0, float("nan"))
    rsi = 100.0 - (100.0 / (1.0 + rs))
    rsi = rsi.fillna(close.where(avg_dn == 0).map(lambda _: 100.0))
    return rsi


def _cross_above(fast: pd.Series, slow: pd.Series) -> pd.Series:
    return (fast.shift(1) <= slow.shift(1)) & (fast > slow)


def _cross_below(fast: pd.Series, slow: pd.Series) -> pd.Series:
    return (fast.shift(1) >= slow.shift(1)) & (fast < slow)


def _cross_up_through(series: pd.Series, level: float) -> pd.Series:
    return (series.shift(1) <= level) & (series > level)


def _cross_down_through(series: pd.Series, level: float) -> pd.Series:
    return (series.shift(1) >= level) & (series < level)


def long_gamma_regime(
    df_4h: pd.DataFrame,
    bull_whitelist_dates: Optional[DateSet] = None,
    bear_whitelist_dates: Optional[DateSet] = None,
) -> pd.DataFrame:
    """
    Compute regime and trigger columns on a 4h OHLCV DataFrame.

    Returns a DataFrame with columns:
      sma8, sma21, sma20, sma50, ema8, ema21, rsi14,
      bull_regime, bear_regime,
      ema_cross_up, ema_cross_down, rsi_cross_up55, rsi_cross_dn45,
      bull_armed, bear_armed
    """
    if bull_whitelist_dates is None:
        bull_whitelist_dates = BULL_WHITELIST_DATES
    if bear_whitelist_dates is None:
        bear_whitelist_dates = BEAR_WHITELIST_DATES

    if "close" not in df_4h.columns:
        raise ValueError("long_gamma_regime(): df_4h must have a 'close' column")

    close = df_4h["close"].astype(float)
    out = pd.DataFrame(index=df_4h.index)

    out["sma8"]  = close.rolling(8,  min_periods=8).mean()
    out["sma21"] = close.rolling(21, min_periods=21).mean()
    out["sma20"] = close.rolling(20, min_periods=20).mean()
    out["sma50"] = close.rolling(50, min_periods=50).mean()

    out["ema8"]  = close.ewm(span=8,  adjust=False, min_periods=8).mean()
    out["ema21"] = close.ewm(span=21, adjust=False, min_periods=21).mean()

    out["rsi14"] = _wilder_rsi(close, period=14)

    out["bull_regime"] = out["sma8"] > out["sma21"]
    out["bear_regime"] = out["sma20"] < out["sma50"]

    out["ema_cross_up"]   = _cross_above(out["ema8"], out["ema21"])
    out["ema_cross_down"] = _cross_below(out["ema8"], out["ema21"])
    out["rsi_cross_up55"] = _cross_up_through(out["rsi14"], 55.0)
    out["rsi_cross_dn45"] = _cross_down_through(out["rsi14"], 45.0)

    # Spec §3.1/§4.1: gate uses utc_date(bar_CLOSE); Binance timestamps are
    # bar-open, so bar close = bar_open + 4h.
    close_dates = (out.index + pd.Timedelta(hours=4)).normalize().date
    bull_on_wl = pd.array([d in bull_whitelist_dates for d in close_dates], dtype=bool)
    bear_on_wl = pd.array([d in bear_whitelist_dates for d in close_dates], dtype=bool)
    out["bull_armed"] = bull_on_wl & out["bull_regime"].fillna(False)
    out["bear_armed"] = bear_on_wl & out["bear_regime"].fillna(False)

    return out


def pair_signals(regime_df: pd.DataFrame, mode: str) -> PairList:
    """
    Derive (entry_ts, exit_ts) signal pairs for one sleeve.

    mode: "BULL", "BEAR", or "SIDEWAYS".
    Pairing: fan-out — each entry paired with its next exit; exit not consumed.
    """
    mode = mode.upper()
    if mode == "SIDEWAYS":
        return []
    if mode == "BULL":
        entry_mask = regime_df["ema_cross_up"] & regime_df["bull_armed"]
        exit_mask  = regime_df["ema_cross_down"]
    elif mode == "BEAR":
        entry_mask = regime_df["rsi_cross_up55"] & regime_df["bear_armed"]
        exit_mask  = regime_df["rsi_cross_dn45"]
    else:
        raise ValueError(f"pair_signals(): unknown mode {mode!r}")
    entries = regime_df.index[entry_mask].tolist()
    exits   = regime_df.index[exit_mask].tolist()
    return _pair_first_entry_then_next_exit(entries, exits)


def _pair_first_entry_then_next_exit(
    entries: List[pd.Timestamp],
    exits: List[pd.Timestamp],
) -> PairList:
    pairs: PairList = []
    for ent in entries:
        for ext in exits:
            if ext > ent:
                pairs.append((ent, ext))
                break
    return pairs


# ---------------------------------------------------------------------------
# Builder functions  (one per indicator)
# ---------------------------------------------------------------------------

def _build_turbulence(df_raw: pd.DataFrame, **params) -> pd.DataFrame:
    return _turbulence(df_raw, **params)


def _build_supertrend(df_raw: pd.DataFrame, **params) -> pd.DataFrame:
    return _supertrend(df_raw, **params)


def _build_long_gamma_regime(df_raw: pd.DataFrame, **params) -> pd.DataFrame:
    return long_gamma_regime(df_raw, **params)


# Registry: indicator name → builder function
_BUILDERS: Dict[str, Callable[..., Any]] = {
    "turbulence": _build_turbulence,
    "supertrend": _build_supertrend,
    "long_gamma_regime": _build_long_gamma_regime,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_indicators(
    deps: List[IndicatorDep],
    start: datetime,
    end: datetime,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch/cache klines and compute all declared indicators.

    Called once per grid run, before the replay loop starts.

    Args:
        deps:  List of ``IndicatorDep`` objects declared by the strategy.
        start: First timestamp of the backtest range (tz-aware UTC).
        end:   Last timestamp of the backtest range (tz-aware UTC).

    Returns:
        Dict mapping indicator name → computed DataFrame/Series.
        Passed directly to ``strategy.set_indicators()``.
    """
    result: Dict[str, pd.DataFrame] = {}

    for dep in deps:
        builder = _BUILDERS.get(dep.name)
        if builder is None:
            raise ValueError(
                f"Unknown indicator '{dep.name}'. "
                f"Registered indicators: {sorted(_BUILDERS)}"
            )

        logger.info(
            "build_indicators: loading %s klines for %s (%s → %s, +%dd warmup)",
            dep.interval, dep.symbol, start.date(), end.date(), dep.warmup_days,
        )
        df_raw = load_klines(
            symbol=dep.symbol,
            interval=dep.interval,
            start=start,
            end=end,
            warmup_days=dep.warmup_days,
        )
        logger.info(
            "build_indicators: computing '%s' from %d raw bars",
            dep.name, len(df_raw),
        )
        result[dep.name] = builder(df_raw, **dep.params)
        logger.info(
            "build_indicators: '%s' ready — %d output bars",
            dep.name, len(result[dep.name]),
        )

    return result

"""
indicators/hist_data.py — Persistent historical kline fetcher.

Downloads OHLC klines from the Binance public spot API for an arbitrary date
range and caches the result as a parquet on disk. Repeat calls covering the
same range read from disk with only the missing tail fetched from Binance.

Storage location: $CRYOTRADER_KLINE_DIR (default: indicators/data/ next to this file)
Storage file:     {KLINE_DIR}/{SYMBOL}_{interval}.parquet  (one file per series)

Public API
----------
    from indicators.hist_data import load_klines

    df = load_klines(
        symbol="BTCUSDT",
        interval="15m",
        start=datetime(2025, 11, 1, tzinfo=timezone.utc),
        end=datetime(2026, 4, 21, tzinfo=timezone.utc),
        warmup_days=30,   # extra history prepended for indicator warmup
    )
    # Returns a DataFrame with columns [open, high, low, close, volume]
    # indexed by UTC-aware timestamps (bar open times), oldest first.
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
_MAX_PER_REQUEST = 1000        # Binance hard limit
_REQUEST_PAUSE_S = 0.08        # ~12 req/s — well under 1200 weight/min limit

# Permanent storage for historical klines — next to this module, gitignored.
# Override with CRYOTRADER_KLINE_DIR env var (e.g. for a shared NAS or CI).
KLINE_DIR = Path(
    os.environ.get("CRYOTRADER_KLINE_DIR", str(Path(__file__).parent / "data"))
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_ms(dt: datetime) -> int:
    """Convert a datetime to Binance-style milliseconds since epoch."""
    return int(dt.timestamp() * 1000)


def _parse_raw(raw: list) -> pd.DataFrame:
    """Convert raw Binance kline list to a typed DataFrame."""
    if not raw:
        return pd.DataFrame(
            columns=["open", "high", "low", "close", "volume"],
            index=pd.DatetimeIndex([], name="timestamp", tz="UTC"),
        )
    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_vol", "trades", "taker_base", "taker_quote", "ignore",
    ])
    df = df[["open_time", "open", "high", "low", "close", "volume"]].copy()
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df.set_index("open_time", inplace=True)
    df.index.name = "timestamp"
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    df = df[~df.index.duplicated(keep="last")]
    df.sort_index(inplace=True)
    return df


_FETCH_RETRIES = 3
_FETCH_BACKOFF_S = 1.0  # doubles on each retry: 1s, 2s, 4s


def _fetch_page(symbol: str, interval: str, params: dict) -> list:
    """Fetch one Binance klines page with retry + exponential backoff."""
    last_exc: Optional[Exception] = None
    for attempt in range(1, _FETCH_RETRIES + 1):
        try:
            resp = requests.get(_BINANCE_KLINES_URL, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            if attempt < _FETCH_RETRIES:
                wait = _FETCH_BACKOFF_S * (2 ** (attempt - 1))
                logger.warning(
                    "_fetch_page(%s %s): attempt %d/%d failed (%s) — retrying in %.1fs",
                    symbol, interval, attempt, _FETCH_RETRIES, exc, wait,
                )
                print(
                    f"  [hist_data] WARNING: Binance fetch failed (attempt {attempt}/{_FETCH_RETRIES}): {exc}"
                    f" — retrying in {wait:.0f}s..."
                )
                time.sleep(wait)
            else:
                logger.error(
                    "_fetch_page(%s %s): all %d attempts failed — %s",
                    symbol, interval, _FETCH_RETRIES, exc,
                )
                raise RuntimeError(
                    f"Binance fetch failed after {_FETCH_RETRIES} attempts ({symbol} {interval}): {exc}"
                ) from last_exc
    return []  # unreachable


def _fetch_range(symbol: str, interval: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    """
    Fetch klines from Binance for [start_ms, end_ms] (both inclusive, ms).
    Paginates forward until the range is covered or no more data arrives.
    Each page is retried up to _FETCH_RETRIES times with exponential backoff.
    """
    all_raw = []
    cursor = start_ms

    while cursor <= end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": _MAX_PER_REQUEST,
            "startTime": cursor,
            "endTime": end_ms,
        }
        batch = _fetch_page(symbol, interval, params)

        if not batch:
            break

        all_raw.extend(batch)
        last_open_ms = int(batch[-1][0])
        cursor = last_open_ms + 1  # next page starts after last received bar

        if len(batch) < _MAX_PER_REQUEST:
            break  # reached end of available data

        time.sleep(_REQUEST_PAUSE_S)

    df = _parse_raw(all_raw)
    logger.info(
        "_fetch_range(%s %s): %d bars fetched",
        symbol, interval, len(df),
    )
    return df


def _cache_path(symbol: str, interval: str) -> Path:
    return KLINE_DIR / f"{symbol.upper()}_{interval}.parquet"


def _read_cache(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        # Ensure index is tz-aware UTC (older cache files may be tz-naive)
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        return df
    except Exception as exc:
        logger.warning("Failed to read cache %s: %s — will re-fetch", path, exc)
        return None


def _write_cache(path: Path, df: pd.DataFrame) -> None:
    KLINE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)
    logger.debug("Cache written: %s (%d rows)", path.name, len(df))


def _merge(existing: Optional[pd.DataFrame], new: pd.DataFrame) -> pd.DataFrame:
    if existing is None or existing.empty:
        return new
    if new.empty:
        return existing
    combined = pd.concat([existing, new])
    combined = combined[~combined.index.duplicated(keep="last")]
    combined.sort_index(inplace=True)
    return combined


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_klines(
    symbol: str,
    interval: str,
    start: datetime,
    end: datetime,
    warmup_days: int = 30,
) -> pd.DataFrame:
    """
    Return OHLC klines for [start - warmup_days, end], fetching from Binance
    as needed and caching to disk.

    The extra ``warmup_days`` before ``start`` ensures rolling-window indicators
    have enough history to be valid at the first bar of the backtest range.

    Args:
        symbol:      Binance spot symbol, e.g. "BTCUSDT".
        interval:    Binance interval string, e.g. "15m", "1h", "1d".
        start:       Earliest backtest timestamp (tz-aware UTC).
        end:         Latest backtest timestamp (tz-aware UTC).
        warmup_days: Additional history to prepend for indicator warmup.

    Returns:
        DataFrame with columns [open, high, low, close, volume],
        UTC-aware DatetimeIndex (bar open times), oldest bar first.
    """
    symbol = symbol.upper()
    # Clamp end to "now" so we never request future bars
    now = datetime.now(tz=timezone.utc)
    if end > now:
        end = now

    needed_start = start - timedelta(days=warmup_days)
    path = _cache_path(symbol, interval)
    cached = _read_cache(path)

    fetch_head: Optional[pd.DataFrame] = None
    fetch_tail: Optional[pd.DataFrame] = None

    if cached is None or cached.empty:
        # Cold start — fetch everything
        logger.info("load_klines(%s %s): cold fetch %s → %s", symbol, interval, needed_start.date(), end.date())
        fetch_head = _fetch_range(symbol, interval, _to_ms(needed_start), _to_ms(end))
    else:
        cached_start = cached.index[0]
        cached_end   = cached.index[-1]

        # Need earlier history?
        if cached_start > needed_start + timedelta(hours=1):
            logger.info(
                "load_klines(%s %s): fetching head %s → %s",
                symbol, interval, needed_start.date(), cached_start.date(),
            )
            fetch_head = _fetch_range(
                symbol, interval,
                _to_ms(needed_start),
                _to_ms(cached_start) - 1,
            )

        # Need more recent data?
        if cached_end < end - timedelta(hours=1):
            logger.info(
                "load_klines(%s %s): fetching tail %s → %s",
                symbol, interval, cached_end.date(), end.date(),
            )
            fetch_tail = _fetch_range(
                symbol, interval,
                _to_ms(cached_end) + 1,
                _to_ms(end),
            )

    if fetch_head is not None or fetch_tail is not None:
        merged = _merge(cached, fetch_head if fetch_head is not None else pd.DataFrame())
        merged = _merge(merged, fetch_tail if fetch_tail is not None else pd.DataFrame())
        _write_cache(path, merged)
        cached = merged

    if cached is None or cached.empty:
        raise RuntimeError(
            f"load_klines({symbol} {interval}): no data available for requested range"
        )

    return cached.loc[
        needed_start.strftime("%Y-%m-%d") : end.strftime("%Y-%m-%d %H:%M")
    ].copy()

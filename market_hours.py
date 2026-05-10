"""
market_hours.py — US market time utilities for CryoTrader.

All internal trading logic stays in UTC. Use this module when entry/exit
conditions need to align with US market hours (NYSE), which shift between
EDT (UTC-4, ~Mar–Nov) and EST (UTC-5, ~Nov–Mar) due to DST.

Design rules:
- All public functions accept/return tz-aware datetimes only.
- Inputs that are UTC may be naive — they are treated as UTC.
- The base codebase is untouched; strategies adopt this incrementally.

NYSE holidays are hardcoded for 2024–2026 from the NYSE published schedule:
https://www.nyse.com/markets/hours-calendars
This covers all years needed by the backtester and live strategies.

Requires: zoneinfo (stdlib ≥ 3.9). tzdata pip package acts as fallback on
systems without a system tz database (minimal containers, some CI images).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NYC = ZoneInfo("America/New_York")
UTC = timezone.utc

# NYSE session times (local NYC)
_NYSE_OPEN_LOCAL = time(9, 30)
_NYSE_CLOSE_LOCAL = time(16, 0)


# ---------------------------------------------------------------------------
# Core conversions
# ---------------------------------------------------------------------------

def to_nyc(dt: datetime) -> datetime:
    """Convert a UTC (or tz-aware) datetime to NYC time.

    If ``dt`` is naive it is assumed to be UTC.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(NYC)


def to_utc(dt: datetime) -> datetime:
    """Convert a tz-aware datetime to UTC.

    If ``dt`` is naive it is assumed to already be UTC and returned unchanged
    (with tzinfo set).
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def nyc_now() -> datetime:
    """Return the current wall-clock time in NYC (tz-aware)."""
    return datetime.now(NYC)


def utc_now() -> datetime:
    """Return the current UTC time (tz-aware). Convenience alias."""
    return datetime.now(UTC)


# ---------------------------------------------------------------------------
# NYSE session anchors
# ---------------------------------------------------------------------------

def nyse_open_utc(date: datetime | None = None) -> datetime:
    """Return the NYSE open (09:30 NYC) as a UTC datetime for *date*.

    If *date* is None, uses today in NYC.  If *date* has timezone info the
    **NYC date** is derived from it (important near midnight UTC); if naive
    it is treated as UTC.

    Examples (2026 — EDT, UTC-4):
        nyse_open_utc(datetime(2026, 5, 7, tzinfo=UTC))  →  2026-05-07 13:30 UTC
    Examples (2025-01 — EST, UTC-5):
        nyse_open_utc(datetime(2025, 1, 10, tzinfo=UTC))  →  2025-01-10 14:30 UTC
    """
    nyc_date = _resolve_nyc_date(date)
    open_nyc = datetime.combine(nyc_date, _NYSE_OPEN_LOCAL, tzinfo=NYC)
    return open_nyc.astimezone(UTC)


def nyse_close_utc(date: datetime | None = None) -> datetime:
    """Return the NYSE close (16:00 NYC) as a UTC datetime for *date*.

    Same semantics as :func:`nyse_open_utc`.
    """
    nyc_date = _resolve_nyc_date(date)
    close_nyc = datetime.combine(nyc_date, _NYSE_CLOSE_LOCAL, tzinfo=NYC)
    return close_nyc.astimezone(UTC)


# ---------------------------------------------------------------------------
# Predicate helpers — for use in entry/exit condition logic
# ---------------------------------------------------------------------------

def in_nyc_window(utc_dt: datetime, start_hour: int, end_hour: int) -> bool:
    """Return True if *utc_dt* falls in the half-open interval [start_hour, end_hour) NYC.

    Handles midnight wrap-around (e.g. start_hour=22, end_hour=2).
    *utc_dt* may be naive (treated as UTC) or tz-aware.

    Args:
        utc_dt:     The moment to test (UTC or tz-aware).
        start_hour: Start of window in NYC local hours (0–23).
        end_hour:   End of window in NYC local hours (0–23), exclusive.
    """
    nyc_hour = to_nyc(utc_dt).hour
    if start_hour < end_hour:
        return start_hour <= nyc_hour < end_hour
    elif start_hour > end_hour:  # midnight wrap-around
        return nyc_hour >= start_hour or nyc_hour < end_hour
    else:
        return False  # zero-length window


def is_near_nyse_open(
    utc_dt: datetime,
    before_min: int = 15,
    after_min: int = 30,
) -> bool:
    """Return True if *utc_dt* is within [open - before_min, open + after_min).

    Uses the NYSE open for the NYC date of *utc_dt* (DST-aware).

    Args:
        utc_dt:     The moment to test (UTC or tz-aware).
        before_min: Minutes before the open to start the window.
        after_min:  Minutes after the open to end the window.
    """
    open_utc = nyse_open_utc(utc_dt)
    window_start = open_utc - timedelta(minutes=before_min)
    window_end = open_utc + timedelta(minutes=after_min)
    dt = to_utc(utc_dt)
    return window_start <= dt < window_end


def is_near_nyse_close(
    utc_dt: datetime,
    before_min: int = 15,
    after_min: int = 5,
) -> bool:
    """Return True if *utc_dt* is within [close - before_min, close + after_min).

    Uses the NYSE close for the NYC date of *utc_dt* (DST-aware).
    """
    close_utc = nyse_close_utc(utc_dt)
    window_start = close_utc - timedelta(minutes=before_min)
    window_end = close_utc + timedelta(minutes=after_min)
    dt = to_utc(utc_dt)
    return window_start <= dt < window_end


def nyc_hour(utc_dt: datetime) -> int:
    """Return the NYC local hour (0–23) for *utc_dt*. Convenience one-liner."""
    return to_nyc(utc_dt).hour


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_nyc_date(d: date | datetime | None) -> date:
    """Derive the NYC calendar date from *d*, or use today in NYC."""
    if d is None:
        return datetime.now(NYC).date()
    if isinstance(d, datetime):
        return to_nyc(d).date()
    # plain date object
    return d


# ---------------------------------------------------------------------------
# NYSE market holidays — 2024, 2025, 2026
# Source: https://www.nyse.com/markets/hours-calendars
# Observations applied: if Jul 4 / Jun 19 / Dec 25 / Jan 1 fall on Saturday,
# observed Friday prior; if Sunday, observed Monday after.
# ---------------------------------------------------------------------------

_NYSE_HOLIDAYS_RAW: list[tuple[int, int, int]] = [
    # --- 2024 ---
    (2024,  1,  1),   # New Year's Day
    (2024,  1, 15),   # Martin Luther King Jr. Day
    (2024,  2, 19),   # Presidents' Day
    (2024,  3, 29),   # Good Friday (Easter Apr 7)
    (2024,  5, 27),   # Memorial Day
    (2024,  6, 19),   # Juneteenth
    (2024,  7,  4),   # Independence Day
    (2024,  9,  2),   # Labor Day
    (2024, 11, 28),   # Thanksgiving
    (2024, 12, 25),   # Christmas
    # --- 2025 ---
    (2025,  1,  1),   # New Year's Day
    (2025,  1,  9),   # National Day of Mourning — President Carter
    (2025,  1, 20),   # Martin Luther King Jr. Day
    (2025,  2, 17),   # Presidents' Day
    (2025,  4, 18),   # Good Friday (Easter Apr 20)
    (2025,  5, 26),   # Memorial Day
    (2025,  6, 19),   # Juneteenth
    (2025,  7,  4),   # Independence Day
    (2025,  9,  1),   # Labor Day
    (2025, 11, 27),   # Thanksgiving
    (2025, 12, 25),   # Christmas
    # --- 2026 ---
    (2026,  1,  1),   # New Year's Day
    (2026,  1, 19),   # Martin Luther King Jr. Day
    (2026,  2, 16),   # Presidents' Day
    (2026,  4,  3),   # Good Friday (Easter Apr 5)
    (2026,  5, 25),   # Memorial Day
    (2026,  6, 19),   # Juneteenth
    (2026,  7,  3),   # Independence Day (observed; Jul 4 is Saturday)
    (2026,  9,  7),   # Labor Day
    (2026, 11, 26),   # Thanksgiving
    (2026, 12, 25),   # Christmas
]

NYSE_HOLIDAYS: frozenset[date] = frozenset(
    date(y, m, d) for y, m, d in _NYSE_HOLIDAYS_RAW
)


# ---------------------------------------------------------------------------
# Trading day predicates
# ---------------------------------------------------------------------------

def is_market_holiday(d: date | datetime) -> bool:
    """Return True if *d* is a NYSE market holiday.

    Accepts a ``date`` or a ``datetime``.  Datetimes are converted to their
    NYC calendar date first (important near midnight UTC).
    """
    return _resolve_nyc_date(d) in NYSE_HOLIDAYS


def is_trading_day(d: date | datetime) -> bool:
    """Return True if NYSE is open on *d* (weekday and not a holiday).

    Accepts a ``date`` or a ``datetime``.  Datetimes are converted to their
    NYC calendar date first.
    """
    nyc_date = _resolve_nyc_date(d)
    return nyc_date.weekday() < 5 and nyc_date not in NYSE_HOLIDAYS


def next_trading_day(d: date | datetime) -> date:
    """Return the next NYSE trading day strictly after *d*."""
    nyc_date = _resolve_nyc_date(d)
    candidate = nyc_date + timedelta(days=1)
    while not is_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def prev_trading_day(d: date | datetime) -> date:
    """Return the most recent NYSE trading day strictly before *d*."""
    nyc_date = _resolve_nyc_date(d)
    candidate = nyc_date - timedelta(days=1)
    while not is_trading_day(candidate):
        candidate -= timedelta(days=1)
    return candidate

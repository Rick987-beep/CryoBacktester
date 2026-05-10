#!/usr/bin/env python3
"""
expiry_utils.py — Shared expiry parsing and selection helpers for backtester strategies.

Previously each strategy file contained verbatim copies of these helpers. This module
is the single source of truth; all strategy files import from here.

Naming convention note:
    Functions are exported WITHOUT a leading underscore (public API).
    Strategy files that previously defined private versions (_parse_expiry_date etc.)
    should rename all call sites accordingly (parse_expiry_date, etc.).

Deribit expiry code format: '<day><MON><YY>'  e.g. '9MAR26', '21APR26', '28MAR26'.

Naming convention inconsistency note (data layer — not relevant here):
    Recorder-produced parquets: spot_track_YYYY-MM-DD.parquet
    Tardis-produced parquets:   spot_YYYY-MM-DD.parquet
    MarketReplay globs spot_*.parquet so both coexist in backtester/data/.
    This module has no dependency on parquet file names.
"""
import re
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, Optional

from backtester.pricing import EXPIRY_HOUR_UTC


# ---------------------------------------------------------------------------
# Month map and weekday maps
# ---------------------------------------------------------------------------

_MONTH_MAP: Dict[str, int] = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

_WEEKDAY_NAMES: Dict[str, int] = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}
_WEEKDAY_REVERSE: Dict[int, str] = {v: k for k, v in _WEEKDAY_NAMES.items()}


# ---------------------------------------------------------------------------
# Core expiry helpers
# ---------------------------------------------------------------------------

@lru_cache(maxsize=128)
def parse_expiry_date(expiry_code):
    # type: (str) -> Optional[datetime]
    """Parse Deribit expiry code like '9MAR26' or '21APR26' to a datetime.

    lru_cache: expiry codes are static strings. Without caching, this regex
    runs once per tick per open position — ~1.5M calls in a 560-combo grid
    run. With cache: at most ~30–50 unique codes ever seen per run.

    Note: lru_cache maxsize standardised to 128 across all callers
    (individual strategy files used inconsistent values of 64 or 128).
    """
    m = re.match(r"(\d{1,2})([A-Z]{3})(\d{2})", expiry_code)
    if not m:
        return None
    day = int(m.group(1))
    month = _MONTH_MAP.get(m.group(2))
    year = 2000 + int(m.group(3))
    if month is None:
        return None
    return datetime(year, month, day)


@lru_cache(maxsize=128)
def expiry_dt_utc(expiry_code, tzinfo):
    # type: (str, Any) -> Optional[datetime]
    """Return the UTC expiry deadline datetime for a Deribit expiry code.

    Deribit options expire at EXPIRY_HOUR_UTC (08:00) on the expiry date.
    lru_cache: called once per position open; also speeds up select_expiry
    callers that scan all available expiries each tick.
    """
    exp_date = parse_expiry_date(expiry_code)
    if exp_date is None:
        return None
    return exp_date.replace(hour=EXPIRY_HOUR_UTC, tzinfo=tzinfo)


def select_expiry(state, dte):
    # type: (Any, int) -> Optional[str]
    """Return the expiry code whose date is exactly `dte` calendar days from now.

    Returns None if no matching expiry exists in the snapshot (entry silently
    skipped by the calling strategy).
    """
    target_date = state.dt.date() + timedelta(days=dte)
    for exp in state.expiries():
        exp_date = parse_expiry_date(exp)
        if exp_date is not None and exp_date.date() == target_date:
            return exp
    return None


def select_expiry_for_week(state, target_weeks):
    # type: (Any, int) -> Optional[str]
    """Return the expiry whose DTE falls in [target_weeks*7, target_weeks*7+6].

    When multiple expiries qualify, picks the one with the lowest DTE
    (closest to the start of the bucket — most conservative choice).
    Returns None if no qualifying expiry exists in the data.

    Used by: short_strangle_weekly_tp, short_strangle_weekly_cap.
    """
    lo = target_weeks * 7
    hi = lo + 6
    today = state.dt.date()

    best_expiry = None
    best_dte = None
    for exp in state.expiries():
        exp_date = parse_expiry_date(exp)
        if exp_date is None:
            continue
        dte = (exp_date.date() - today).days
        if lo <= dte <= hi:
            if best_dte is None or dte < best_dte:
                best_expiry = exp
                best_dte = dte
    return best_expiry


def nearest_valid_expiry(state):
    # type: (Any) -> Optional[str]
    """Find the nearest expiry whose 08:00 UTC deadline hasn't passed yet.

    Before 08:00 UTC: today's expiry is used (0DTE).
    After  08:00 UTC: today's expiry is gone, so tomorrow's is used (~1DTE).

    Used by: straddle_strangle, deltaswipswap.
    """
    best = None
    best_dt = None
    for exp in state.expiries():
        exp_date = parse_expiry_date(exp)
        if exp_date is None:
            continue
        exp_dt = exp_date.replace(hour=EXPIRY_HOUR_UTC, tzinfo=state.dt.tzinfo)
        if exp_dt <= state.dt:
            continue  # already expired
        if best_dt is None or exp_dt < best_dt:
            best = exp
            best_dt = exp_dt
    return best


# ---------------------------------------------------------------------------
# Weekday helpers (used by short_strangle_weekend)
# ---------------------------------------------------------------------------

def parse_open_days(value):
    # type: (str) -> frozenset
    """Parse comma-separated weekday names into a frozenset of ints.

    Example: 'sunday,monday' -> frozenset({6, 0})
    Raises ValueError for unknown weekday names.
    """
    parts = [s.strip().lower() for s in value.split(",") if s.strip()]
    nums = []
    for p in parts:
        if p not in _WEEKDAY_NAMES:
            raise ValueError(f"Unknown weekday: {p!r}")
        nums.append(_WEEKDAY_NAMES[p])
    return frozenset(nums)


def open_days_label(days_set):
    # type: (frozenset) -> str
    """Convert frozenset of weekday ints back to sorted comma-separated string."""
    ordered = [0, 1, 2, 3, 4, 5, 6]
    return ",".join(_WEEKDAY_REVERSE[d] for d in ordered if d in days_set)

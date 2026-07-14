"""NYC session windows and slot-A entry day enumeration."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

from market_hours import NYC, is_trading_day, to_utc

UTC = timezone.utc

NYSE_MIDDAY = time(12, 0)


def nyc_dt(d: date, t: time) -> datetime:
    return datetime.combine(d, t, tzinfo=NYC)


def slot_a_entry_days(start: date, end: date) -> list[date]:
    """Mon–Thu NYSE open days in [start, end]."""
    days: list[date] = []
    d = start
    while d <= end:
        if d.weekday() in (0, 1, 2, 3) and is_trading_day(d):
            days.append(d)
        d += timedelta(days=1)
    return days


def predictor_bounds_utc(entry_day: date, variant: str) -> tuple[datetime, datetime]:
    """UTC bounds for predictor window on *entry_day* (half-open end)."""
    if variant == "us_morning":
        start = to_utc(nyc_dt(entry_day, time(9, 30)))
        end = to_utc(nyc_dt(entry_day, NYSE_MIDDAY)) + timedelta(seconds=1)
        return start, end
    if variant == "london_us":
        start = datetime.combine(entry_day, time(7, 0), tzinfo=UTC)
        end = to_utc(nyc_dt(entry_day, NYSE_MIDDAY)) + timedelta(seconds=1)
        return start, end
    raise ValueError(f"Unknown predictor variant: {variant!r}")


def decision_time_utc(entry_day: date, decision_time_nyc: str) -> datetime:
    """UTC instant for NYC decision clock on entry_day."""
    parts = decision_time_nyc.strip().split(":")
    hour = int(parts[0])
    minute = int(parts[1]) if len(parts) > 1 else 0
    return to_utc(nyc_dt(entry_day, time(hour, minute)))

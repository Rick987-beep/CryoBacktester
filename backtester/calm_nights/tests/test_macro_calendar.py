"""Unit tests for Starnberg local macro tier remapping helpers."""

from datetime import date, datetime, timezone
from unittest.mock import patch

import pandas as pd

from backtester.calm_nights.macro_calendar import (
    starnberg_local_tier,
    starnberg_macro_blocks_entry,
    starnberg_qualifying_events,
)


UTC = timezone.utc


class TestQualifyingEvents:
    def test_ignores_parquet_tier_column(self):
        d = date(2026, 1, 15)
        fake = pd.DataFrame([{
            "event_date": "2026-01-15",
            "event_type": "cpi",
            "tier": 99.0,
            "title": "Consumer Price Index",
            "available_utc": datetime(2026, 1, 15, 13, 30, tzinfo=UTC),
            "event_time_et": "08:30",
        }])
        with patch(
            "backtester.calm_nights.macro_calendar.events_on_date",
            return_value=fake,
        ):
            sub = starnberg_qualifying_events(d, max_tier=1)
        assert len(sub) == 1
        assert int(sub.iloc[0]["tier"]) == 1
        assert starnberg_local_tier("cpi") == 1

    def test_fallback_event_time_et(self):
        d = date(2026, 1, 15)
        fake = pd.DataFrame([{
            "event_date": "2026-01-15",
            "event_type": "fomc",
            "tier": 1.0,
            "title": "FOMC rate decision",
            "available_utc": None,
            "event_time_et": "14:00",
        }])
        with patch(
            "backtester.calm_nights.macro_calendar.events_on_date",
            return_value=fake,
        ):
            skip, cleared, events = starnberg_macro_blocks_entry(
                d,
                datetime(2026, 1, 15, 16, 30, tzinfo=UTC),
                delay_hours=2.0,
                max_tier=1,
            )
        # 14:00 ET in January = 19:00 UTC; +2h = 21:00 > 16:30 → skip
        assert skip is True
        assert events[0]["event_type"] == "fomc"
        assert cleared is not None

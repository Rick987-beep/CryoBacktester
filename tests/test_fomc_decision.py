"""FOMC policy-statement indicator: meetings only, embargo minute, entry gate."""
from datetime import date, datetime, timezone
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from backtester.indicators.fomc_decision import (
    KNOWN_NON_DECISION_DATES,
    TIME_SOURCE,
    all_events,
    announced_at_utc,
    event_on_nyc_date,
    events_from_frame,
    fomc_entry_gate,
)
from backtester.indicators.pipeline import IndicatorDep, build_indicators


UTC = timezone.utc
NYC = ZoneInfo("America/New_York")


def _utc(y, m, d, hh, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=UTC)


class TestMeetingLock:
    def test_no_minutes_or_framework(self):
        dates = {e.nyc_date.isoformat() for e in all_events()}
        for iso in KNOWN_NON_DECISION_DATES:
            assert iso not in dates

    def test_only_policy_statements(self):
        actions = {e.action for e in all_events()}
        assert actions <= {"hold", "cut", "hike", "pending"}
        cuts = [e for e in all_events() if e.action == "cut"]
        assert [e.nyc_date.isoformat() for e in cuts] == [
            "2025-09-17",
            "2025-10-29",
            "2025-12-10",
        ]

    def test_jan_and_mar_2025_are_holds(self):
        jan = event_on_nyc_date(date(2025, 1, 29))
        mar = event_on_nyc_date(date(2025, 3, 19))
        assert jan is not None and jan.action == "hold" and jan.delta_bp == 0
        assert mar is not None and mar.action == "hold" and mar.delta_bp == 0

    def test_embargo_is_1400_et_to_the_minute(self):
        winter = announced_at_utc(date(2025, 1, 29))
        summer = announced_at_utc(date(2025, 5, 7))
        assert winter == _utc(2025, 1, 29, 19, 0)
        assert summer == _utc(2025, 5, 7, 18, 0)
        for ev in all_events():
            assert ev.announced_at.second == 0
            assert ev.announced_at.microsecond == 0
            local = ev.announced_at.astimezone(NYC)
            assert (local.hour, local.minute) == (14, 0)
            assert ev.time_source == TIME_SOURCE

    def test_today_sep_2026_is_pending_meeting(self):
        ev = event_on_nyc_date(date(2026, 9, 16))
        assert ev is not None
        assert ev.action == "pending"
        assert ev.announced_at == _utc(2026, 9, 16, 18, 0)


class TestEntryGate:
    def test_off_never_blocks_on_meeting_day(self):
        now = _utc(2025, 9, 17, 17, 0)  # 13:00 ET, before print
        g = fomc_entry_gate(now, mode="off")
        assert g.blocked is False
        assert g.reason == "off"
        assert g.event is None

    def test_skip_blocks_whole_nyc_day(self):
        before = _utc(2025, 9, 17, 16, 0)   # 12:00 ET
        at_print = _utc(2025, 9, 17, 18, 0)  # 14:00 ET
        after = _utc(2025, 9, 17, 22, 0)     # 18:00 ET
        for now in (before, at_print, after):
            g = fomc_entry_gate(now, mode="skip")
            assert g.blocked is True
            assert g.reason == "skip_day"
            assert g.cleared_after is None
            assert g.event is not None
            assert g.event.action == "cut"

    def test_skip_does_not_block_next_day(self):
        g = fomc_entry_gate(_utc(2025, 9, 18, 18, 0), mode="skip")
        assert g.blocked is False
        assert g.reason == "no_event"

    def test_delay_30_blocks_until_1430_et(self):
        at_print = _utc(2025, 9, 17, 18, 0)
        plus_25 = _utc(2025, 9, 17, 18, 25)
        plus_30 = _utc(2025, 9, 17, 18, 30)
        morning = _utc(2025, 9, 17, 15, 0)
        for now in (morning, at_print, plus_25):
            g = fomc_entry_gate(now, mode="delay", delay_minutes=30)
            assert g.blocked is True, now
            assert g.reason == "before_clear"
            assert g.cleared_after == _utc(2025, 9, 17, 18, 30)
        g = fomc_entry_gate(plus_30, mode="delay", delay_minutes=30)
        assert g.blocked is False
        assert g.reason == "cleared"

    def test_delay_60_and_120(self):
        g60 = fomc_entry_gate(
            _utc(2025, 9, 17, 18, 59), mode="delay", delay_minutes=60,
        )
        assert g60.blocked is True
        g60c = fomc_entry_gate(
            _utc(2025, 9, 17, 19, 0), mode="delay", delay_minutes=60,
        )
        assert g60c.blocked is False
        g120 = fomc_entry_gate(
            _utc(2025, 9, 17, 19, 59), mode="delay", delay_minutes=120,
        )
        assert g120.blocked is True
        g120c = fomc_entry_gate(
            _utc(2025, 9, 17, 20, 0), mode="delay", delay_minutes=120,
        )
        assert g120c.blocked is False

    def test_delay_zero_allows_at_embargo_minute(self):
        g = fomc_entry_gate(
            _utc(2025, 9, 17, 18, 0), mode="delay", delay_minutes=0,
        )
        assert g.blocked is False
        assert g.reason == "cleared"

    def test_hold_meeting_still_gates(self):
        g = fomc_entry_gate(_utc(2025, 5, 7, 17, 0), mode="skip")
        assert g.blocked is True
        assert g.event is not None
        assert g.event.action == "hold"

    def test_unknown_mode_raises(self):
        with pytest.raises(ValueError, match="unknown fomc gate mode"):
            fomc_entry_gate(_utc(2025, 5, 7, 18, 0), mode="block")


class TestPipeline:
    def test_builder_skips_klines(self):
        with patch("backtester.indicators.pipeline.load_klines") as load:
            ind = build_indicators(
                deps=[IndicatorDep(
                    name="fomc_decision",
                    symbol="FOMC",
                    interval="event",
                    warmup_days=0,
                    needs_klines=False,
                )],
                start=_utc(2025, 4, 11, 0, 0),
                end=_utc(2026, 8, 31, 0, 0),
            )
        load.assert_not_called()
        df = ind["fomc_decision"]
        assert isinstance(df, pd.DataFrame)
        assert "action" in df.columns
        assert df.index.name == "announced_at"
        # Window starts after Jan 2025 meeting, before Sep 2026 pending.
        assert "2025-01-29" not in set(df["nyc_date"])
        assert "2025-09-17" in set(df["nyc_date"])
        rebuilt = events_from_frame(df)
        assert event_on_nyc_date(date(2025, 9, 17), rebuilt) is not None
        assert event_on_nyc_date(date(2025, 5, 28), rebuilt) is None

    def test_empty_frame_means_no_meetings(self):
        assert events_from_frame(pd.DataFrame()) == []
        assert events_from_frame(None)  # full lock, non-empty

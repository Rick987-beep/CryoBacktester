"""Indicator builder edge cases for calm-nights daily table."""

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from backtester.calm_nights.daily_features import build_daily_table
from backtester.calm_nights.sessions import slot_a_entry_days
from backtester.core.config import cfg
from backtester.core.market_hours import is_trading_day


def _options_dir() -> Path:
    return Path(cfg.data.options_parquet)


def _synthetic_bars(start: datetime, periods: int = 500, freq: str = "15min") -> pd.DataFrame:
    idx = pd.date_range(start, periods=periods, freq=freq, tz="UTC")
    base = 100_000.0
    return pd.DataFrame(
        {
            "open": base,
            "high": base * 1.002,
            "low": base * 0.998,
            "close": base * 1.001,
            "volume": 1.0,
        },
        index=idx,
    )


class TestSlotAEntryDays:
    def test_excludes_july4_2025(self):
        days = slot_a_entry_days(date(2025, 7, 1), date(2025, 7, 10))
        assert date(2025, 7, 4) not in days
        assert not is_trading_day(date(2025, 7, 4))

    def test_mon_thu_only(self):
        days = slot_a_entry_days(date(2025, 6, 2), date(2025, 6, 13))
        assert all(d.weekday() < 4 for d in days)
        assert date(2025, 6, 6) not in days  # Friday


class TestBuildDailyTable:
    def test_mci_after_warmup_with_klines(self):
        bars = _synthetic_bars(datetime(2025, 1, 1, tzinfo=timezone.utc), periods=30_000)
        daily = build_daily_table(
            bars,
            date(2025, 6, 1),
            date(2025, 9, 1),
            options_dir=_options_dir(),
        )
        assert not daily.empty
        warmed = daily[daily["morning_calm_index"].notna()]
        assert len(warmed) >= 20
        assert warmed["predictor_quiet"].notna().all()

    def test_iv_rank_nan_before_options_floor(self):
        bars = _synthetic_bars(datetime(2025, 1, 1, tzinfo=timezone.utc), periods=30_000)
        daily = build_daily_table(
            bars,
            date(2025, 4, 1),
            date(2025, 4, 15),
            options_dir=_options_dir(),
        )
        if daily.empty:
            pytest.skip("no slot-A days in April 2025 window")
        assert daily["iv_rank_60d"].isna().all()

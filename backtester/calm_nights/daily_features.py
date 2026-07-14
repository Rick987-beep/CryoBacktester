"""Build daily calm-nights feature table for slot-A days."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from backtester.calm_nights.calm_index import extrusion_calm_index
from backtester.calm_nights.extrusion import (
    drop_weekend_bars,
    normalize_extrusion,
    path_extrusion,
    quiet_tertile_flags,
    range_extrusion,
    slice_bars,
)
from backtester.calm_nights.iv_context import build_iv_series
from backtester.calm_nights.macro_calendar import tier_entry_day_series
from backtester.calm_nights.sessions import predictor_bounds_utc, slot_a_entry_days


def build_daily_table(
    bars: pd.DataFrame,
    start: date,
    end: date,
    *,
    options_dir: Path,
    predictor_variant: str = "london_us",
    decision_time_nyc: str = "12:00",
    rolling_days: int = 60,
    quiet_tertile: float = 0.33,
    exclude_weekends: bool = True,
    macro_tier_filter: int = 1,
    macro_calendar_dir: str | None = None,
) -> pd.DataFrame:
    """One row per slot-A day with MCI, quiet, IV rank, macro flags."""
    if bars.index.tz is None:
        bars = bars.copy()
        bars.index = bars.index.tz_localize("UTC")

    entry_days = slot_a_entry_days(start, end)
    rows: list[dict] = []
    for entry_day in entry_days:
        p0, p1 = predictor_bounds_utc(entry_day, predictor_variant)
        pb = slice_bars(bars, pd.Timestamp(p0), pd.Timestamp(p1))
        if exclude_weekends:
            pb = drop_weekend_bars(pb)
        rows.append(
            {
                "entry_date": entry_day.isoformat(),
                "predictor_range": range_extrusion(pb),
                "predictor_path": path_extrusion(pb),
                "has_klines": not pb.empty,
            }
        )

    daily = pd.DataFrame(rows)
    if daily.empty:
        return daily

    daily["predictor_range_norm"] = normalize_extrusion(
        daily["predictor_range"],
        rolling=rolling_days,
        exclude_weekends=exclude_weekends,
        entry_dates=daily["entry_date"],
    )
    daily["predictor_path_norm"] = normalize_extrusion(
        daily["predictor_path"],
        rolling=rolling_days,
        exclude_weekends=exclude_weekends,
        entry_dates=daily["entry_date"],
    )
    daily["predictor_quiet"] = quiet_tertile_flags(
        daily["predictor_range_norm"],
        q=quiet_tertile,
        rolling=rolling_days,
        exclude_weekends=exclude_weekends,
        entry_dates=daily["entry_date"],
    )
    daily["morning_calm_index"] = extrusion_calm_index(
        daily["predictor_range_norm"],
        daily["predictor_path_norm"],
        window=rolling_days,
        min_periods=20,
    )

    iv_df = build_iv_series(entry_days, options_dir, decision_time_nyc, rolling=rolling_days)
    if not iv_df.empty:
        daily = daily.merge(iv_df, on="entry_date", how="left")
    else:
        daily["atm_iv_dte1"] = float("nan")
        daily["iv_rank_60d"] = float("nan")
        daily["has_options"] = False

    tier_flags = tier_entry_day_series(
        entry_days, max_tier=macro_tier_filter, calendar_dir=macro_calendar_dir
    )
    daily["tier_entry_day"] = daily["entry_date"].map(tier_flags.to_dict())
    daily["has_macro"] = macro_tier_filter > 0

    daily["warmup_complete"] = (
        daily["morning_calm_index"].notna()
        & daily["predictor_quiet"].notna()
        & daily.index >= rolling_days // 4
    )

    daily = daily.set_index("entry_date")
    return daily

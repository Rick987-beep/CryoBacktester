"""Waterfall profile selection for calm-nights entry."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class Profile:
    tier: str
    entry_time: str
    delta: float
    min_otm_pct: float


SKIP = Profile(tier="skip", entry_time="", delta=0.0, min_otm_pct=0.0)


def _bool(val: Any, default: bool = False) -> bool:
    if val is None:
        return default
    try:
        if isinstance(val, float) and math.isnan(val):
            return default
    except TypeError:
        pass
    return bool(val)


def _float(val: Any, default: float = float("nan")) -> float:
    if val is None:
        return default
    try:
        f = float(val)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


def _late_base_profile(params: Mapping[str, Any]) -> Profile:
    return Profile(
        tier="late_base",
        entry_time=str(params.get("late_base_entry_time", "16:00")),
        delta=float(params.get("late_base_delta", 0.10)),
        min_otm_pct=float(params.get("late_base_min_otm_pct", 2.6)),
    )


def _middle_profile(params: Mapping[str, Any]) -> Profile:
    return Profile(
        tier="middle",
        entry_time=str(params.get("middle_entry_time", "12:00")),
        delta=float(params.get("middle_delta", 0.10)),
        min_otm_pct=float(params.get("middle_min_otm_pct", 1.0)),
    )


def _aggressive_profile(params: Mapping[str, Any]) -> Profile:
    return Profile(
        tier="aggressive",
        entry_time=str(params.get("aggressive_entry_time", "12:00")),
        delta=float(params.get("aggressive_delta", 0.20)),
        min_otm_pct=float(params.get("aggressive_min_otm_pct", 1.0)),
    )


def recommend_tier(
    row: Mapping[str, Any],
    params: Mapping[str, Any],
    *,
    tier_entry_day: bool = False,
) -> Profile:
    """Return profile for one slot-A day (fail-safe on NaN inputs)."""
    skip_tier1 = int(params.get("skip_tier1_entry_day", 1))
    tier1_action = str(params.get("tier1_entry_action", "skip"))
    non_quiet_action = str(params.get("non_quiet_action", "late_base"))

    if skip_tier1 and tier_entry_day:
        if tier1_action == "late_base":
            return _late_base_profile(params)
        return SKIP

    quiet = _bool(row.get("predictor_quiet"), default=False)
    iv_rank = _float(row.get("iv_rank_60d"))
    mci = _float(row.get("morning_calm_index"))

    if not quiet:
        if non_quiet_action == "skip":
            return SKIP
        return _late_base_profile(params)

    agg_iv_max = float(params.get("aggressive_iv_rank_max", 0.50))
    agg_mci_min = float(params.get("aggressive_mci_min", 0.55))
    if (
        quiet
        and not tier_entry_day
        and not math.isnan(iv_rank)
        and iv_rank <= agg_iv_max
        and not math.isnan(mci)
        and mci >= agg_mci_min
    ):
        return _aggressive_profile(params)

    mid_iv_max = float(params.get("middle_iv_rank_max", 0.67))
    mid_mci_min = float(params.get("middle_mci_min", 0.50))
    if (
        quiet
        and not tier_entry_day
        and not math.isnan(iv_rank)
        and iv_rank <= mid_iv_max
        and not math.isnan(mci)
        and mci >= mid_mci_min
    ):
        return _middle_profile(params)

    return _late_base_profile(params)

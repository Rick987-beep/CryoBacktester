"""Mode C locks + re-export of shared theta helpers.

Shared research DNA (entry clocks, Skew6, net-credit exits) lives in
``workspace.strategies.theta_shared``. This module keeps the frozen Mode C
book / TP / date range and re-exports helpers so existing

    from workspace.strategies.theta_engine._common import …

paths stay valid. Historical ``v1``–``v11`` keep local copies so old runs
stay bit-identical.

Canonical short-only strategy: ``base.py`` (catalog ``theta_engine_base``).
"""
from __future__ import annotations

from workspace.strategies.theta_shared import (
    BASELINE_DAILY15,
    BASELINE_DISPLAY,
    BASELINE_RICHFORCE16,
    ENTRY_DAYS_MON_FRI,
    ENTRY_POLICIES,
    ENTRY_SCHEDULES,
    FLOOR_BTC,
    FORCE_WEEKDAYS_MON_THU,
    MIN_QTY,
    SKEW_DTE,
    SKEW_MIN_RUNGS,
    SKEW_MODES,
    SKEW_RR_DELTA,
    EntryCondition,
    compute_skew6,
    entry_allowed_weekday,
    entry_at_or_after_utc,
    entry_max_concurrent,
    entry_once_per_calendar_day,
    entry_rich_or_forced,
    hold_until_planned_dt,
    listed_expiries_by_dte,
    net_credit_profit_target_pct,
    net_credit_stop_loss_pct,
    parse_entry_schedule,
    parse_entry_time,
    parse_skew_mode,
    resolve_entry_policy,
    rr_25d_on_expiry,
    skew6_neighbor_rungs,
    skew_zone,
)

# Frozen Mode C short-only book (no cover, no perps, no v9 trail/launch).
MODE_C_BOOK = {
    "delta": 0.25,
    "min_dte": 90,
    "hold_days": 0,
    "stop_loss_pct": 3.0,
    "max_concurrent": 20,
    "qty_per_1btc_equity": 0.2,
}
MODE_C_TP = {
    BASELINE_RICHFORCE16: 0.60,
    BASELINE_DAILY15: 0.50,
}
MODE_C_DATE_RANGE = ("2025-04-11", "2026-08-01")

__all__ = [
    "BASELINE_DAILY15",
    "BASELINE_DISPLAY",
    "BASELINE_RICHFORCE16",
    "ENTRY_DAYS_MON_FRI",
    "ENTRY_POLICIES",
    "ENTRY_SCHEDULES",
    "FLOOR_BTC",
    "FORCE_WEEKDAYS_MON_THU",
    "MIN_QTY",
    "MODE_C_BOOK",
    "MODE_C_DATE_RANGE",
    "MODE_C_TP",
    "SKEW_DTE",
    "SKEW_MIN_RUNGS",
    "SKEW_MODES",
    "SKEW_RR_DELTA",
    "EntryCondition",
    "compute_skew6",
    "entry_allowed_weekday",
    "entry_at_or_after_utc",
    "entry_max_concurrent",
    "entry_once_per_calendar_day",
    "entry_rich_or_forced",
    "hold_until_planned_dt",
    "listed_expiries_by_dte",
    "net_credit_profit_target_pct",
    "net_credit_stop_loss_pct",
    "parse_entry_schedule",
    "parse_entry_time",
    "parse_skew_mode",
    "resolve_entry_policy",
    "rr_25d_on_expiry",
    "skew6_neighbor_rungs",
    "skew_zone",
]

#!/usr/bin/env python3
"""
theta_engine/v14.py — Mode C DNA on exact 1–3 DTE singles.

Fork of ``base.py`` (not v9 / v12 / v13). Naked short call or put, Skew6 or
front 25Δ RR side pick, one live short (no 90 DTE warehouse).

Named entries (policy IDs are stable — never rename)::

    Daily16      daily_1600
        Mon–Fri clock 16:00 UTC, no VRP.

    RichDay16    rich_day_1600
        16:00 UTC iff VRP ≥ 4; else skip the day (no force).

    RichForce2   rich_force_2d_1600
        VRP ≥ 4 at 16:00, else force any weekday after 2 sessions without
        an open. Mode C used 5d / Mon–Thu because the book was working.

    FrontWait16  front_wait_1000_1600
        From 10:00 UTC enter when chosen-side 25Δ mark IV − DVOL ≥ 0;
        if never, force at/after 16:00. Signal updates every snapshot.

Daily VRP is a day gate, not an hour timer. FrontWait is the short-horizon
analog of rich-or-forced. Exact DTE via ``select_expiry``. Weekend cover
skips expiries that settle Sunday or Monday. SL/TP are net-credit capture
(mark SL, executable TP) like Mode C; 0 disables.

Catalog ID ``theta_engine_v14``.

**#1 book** (run 727 ``eff2523b17b8``): RichForce2 @ 15:00 / skew6 / VRP≥4 /
qty 2 / hold. Daily16, RichForce2-16-front, and RichDay15 are fan-out
variants for other logic — not co-equal defaults.
"""

import logging
import math
from datetime import date, datetime, timedelta
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

from backtester.core.config import cfg as _cfg
from backtester.core.expiry_utils import expiry_dt_utc, parse_expiry_date, select_expiry
from backtester.core.option_selection import select_by_delta
from backtester.core.pricing import deribit_fee_per_leg
from backtester.core.strategy_base import (
    OpenPosition,
    Trade,
    check_expiry,
    close_position,
    exit_expiry_window,
    strike_proximity_stop_pct,
)
from backtester.indicators import IndicatorDep
from backtester.indicators.vol_context import lookup_vol_context
from workspace.strategies.theta_engine._common import (
    ENTRY_DAYS_MON_FRI,
    EntryCondition,
    FLOOR_BTC,
    MIN_QTY,
    SKEW_DTE,
    compute_skew6,
    entry_allowed_weekday,
    entry_at_or_after_utc,
    entry_max_concurrent,
    entry_once_per_calendar_day,
    entry_rich_or_forced,
    hold_until_planned_dt,
    net_credit_profit_target_pct,
    net_credit_stop_loss_pct,
    parse_entry_time,
    parse_skew_mode,
    rr_25d_on_expiry,
    skew_zone,
)

logger = logging.getLogger(__name__)

# Stable policy IDs — never rename.
ENTRY_DAILY16 = "daily_1600"
ENTRY_RICH_DAY16 = "rich_day_1600"
ENTRY_RICH_FORCE2 = "rich_force_2d_1600"
ENTRY_FRONT_WAIT16 = "front_wait_1000_1600"

V14_DISPLAY = {
    ENTRY_DAILY16: "Daily16",
    ENTRY_RICH_DAY16: "RichDay16",
    ENTRY_RICH_FORCE2: "RichForce2",
    ENTRY_FRONT_WAIT16: "FrontWait16",
}

# Run 727. #1 is the book to extend; the rest are fan-out variants.
V14_FAVOURITE_HASH = "eff2523b17b8"
V14_FAVOURITE_BOOK = "rf2_1500"
V14_FAVOURITE_SKEW = "skew6"
V14_FANOUT_BOOKS: Tuple[str, ...] = (
    "daily16_1600",
    "rf2_1600",
    "rich_day_1500",
)

SKEW_SOURCES: Tuple[str, ...] = ("skew6", "front", "dte123", "agree")
# dte123: equal-weight 25Δ RR on exact 1/2/3 DTE (the trade tenors).
#   RR_d = IV^{25Δ}_call(d) − IV^{25Δ}_put(d)
#   S_123 = (1/n) Σ_{d ∈ {1,2,3}, RR_d defined} RR_d    (n ≥ 2)
# Run 730 (4 books × front/skew6/dte123): dte123 never beat front or skew6.
# Keep as a selector; do not put it back in PARAM_GRID by default.
DTE123_DTES: Tuple[int, ...] = (1, 2, 3)
DTE123_MIN_RUNGS = 2
WEEKEND_COVER_MODES: Tuple[str, ...] = ("skip",)
LATE_EXITS: Tuple[str, ...] = ("none", "sl_2", "prox_1pct")
LATE_EXIT_HOURS = 4.0
LATE_SL_PCT = 2.0
LATE_PROX_PCT = 1.0
_QUOTE_FREE_EXIT_REASONS = frozenset({"expiry", "strike_proximity_stop"})
# Sunday / Monday 08:00 UTC settlement covers the weekend session.
_WEEKEND_EXPIRY_WEEKDAYS: FrozenSet[int] = frozenset({0, 6})

V14_ENTRY_POLICIES: Dict[str, Dict[str, Any]] = {
    ENTRY_DAILY16: {
        "entry_mode": "daily_clock",
        "entry_time": "16:00",
        "look_start": "16:00",
        "vrp_min": 0.0,
        "force_after_days": 0,
        "force_weekdays": ENTRY_DAYS_MON_FRI,
        "entry_days": ENTRY_DAYS_MON_FRI,
        "entry_schedule": "mon_fri",
    },
    ENTRY_RICH_DAY16: {
        "entry_mode": "rich_day",
        "entry_time": "16:00",
        "look_start": "16:00",
        "vrp_min": 4.0,
        "force_after_days": 0,
        "force_weekdays": ENTRY_DAYS_MON_FRI,
        "entry_days": ENTRY_DAYS_MON_FRI,
        "entry_schedule": "mon_fri",
    },
    ENTRY_RICH_FORCE2: {
        "entry_mode": "rich_or_forced",
        "entry_time": "16:00",
        "look_start": "16:00",
        "vrp_min": 4.0,
        "force_after_days": 2,
        "force_weekdays": ENTRY_DAYS_MON_FRI,
        "entry_days": ENTRY_DAYS_MON_FRI,
        "entry_schedule": "mon_fri",
    },
    ENTRY_FRONT_WAIT16: {
        "entry_mode": "front_wait",
        "entry_time": "16:00",
        "look_start": "10:00",
        "vrp_min": 0.0,
        "force_after_days": 0,
        "force_weekdays": ENTRY_DAYS_MON_FRI,
        "entry_days": ENTRY_DAYS_MON_FRI,
        "entry_schedule": "mon_fri",
    },
}


def parse_skew_source(name: Any) -> str:
    key = str(name).strip().lower()
    if key not in SKEW_SOURCES:
        raise ValueError(
            "unknown skew_source %r; choose from %s"
            % (name, list(SKEW_SOURCES))
        )
    return key


def parse_late_exit(name: Any) -> str:
    key = str(name).strip().lower()
    if key not in LATE_EXITS:
        raise ValueError(
            "unknown late_exit %r; choose from %s" % (name, list(LATE_EXITS))
        )
    return key


def parse_weekend_cover(name: Any) -> str:
    key = str(name).strip().lower()
    if key not in WEEKEND_COVER_MODES:
        raise ValueError(
            "unknown weekend_cover %r; choose from %s"
            % (name, list(WEEKEND_COVER_MODES))
        )
    return key


# Locked (policy, clock) pairs — skew_source is selected separately.
V14_BOOKS: Dict[str, Dict[str, Any]] = {
    "daily16_1600": {
        "entry_policy": ENTRY_DAILY16,
        "entry_hour": 16,
    },
    "rf2_1500": {
        "entry_policy": ENTRY_RICH_FORCE2,
        "entry_hour": 15,
    },
    "rf2_1600": {
        "entry_policy": ENTRY_RICH_FORCE2,
        "entry_hour": 16,
    },
    "rich_day_1500": {
        "entry_policy": ENTRY_RICH_DAY16,
        "entry_hour": 15,
    },
}


def parse_book(name: Any) -> Dict[str, Any]:
    key = str(name).strip().lower()
    if key not in V14_BOOKS:
        raise ValueError(
            "unknown book %r; choose from %s" % (name, sorted(V14_BOOKS))
        )
    return dict(V14_BOOKS[key])


def resolve_v14_entry_policy(params: Dict[str, Any]) -> Dict[str, Any]:
    key = str(params.get("entry_policy", ENTRY_DAILY16))
    if key not in V14_ENTRY_POLICIES:
        raise ValueError(
            "unknown entry_policy %r; choose from %s"
            % (key, sorted(V14_ENTRY_POLICIES))
        )
    resolved = dict(V14_ENTRY_POLICIES[key])
    resolved["entry_policy"] = key
    return resolved


def compute_dte123(state: Any) -> Optional[Dict[str, Any]]:
    """Equal-weight 25Δ RR across exact 1/2/3 DTE.

    RR_d = IV_call_25Δ(d) − IV_put_25Δ(d) on ``select_expiry(d)``.
    S_123 = mean of defined RR_d.  Needs ≥ ``DTE123_MIN_RUNGS`` rungs.

    Closed on run 730: did not beat ``front`` or ``skew6``.
    """
    rr_by_dte: Dict[int, float] = {}
    for dte in DTE123_DTES:
        expiry = select_expiry(state, dte)
        if expiry is None:
            continue
        rr = rr_25d_on_expiry(state, expiry)
        if rr is not None:
            rr_by_dte[dte] = rr
    if len(rr_by_dte) < DTE123_MIN_RUNGS:
        return None
    vals = list(rr_by_dte.values())
    out: Dict[str, Any] = {
        "rr_composite": sum(vals) / float(len(vals)),
        "n_rungs": len(vals),
        "center_dte_actual": 2,
        "rr_dte1": rr_by_dte.get(1),
        "rr_dte2": rr_by_dte.get(2),
        "rr_dte3": rr_by_dte.get(3),
    }
    return out


def expiry_covers_weekend(exp_dt: Any) -> bool:
    """True when settlement is Sunday or Monday 08:00 UTC."""
    if exp_dt is None:
        return False
    wd = exp_dt.weekday() if hasattr(exp_dt, "weekday") else None
    return wd in _WEEKEND_EXPIRY_WEEKDAYS


def _clock_minutes(dt: datetime) -> int:
    return dt.hour * 60 + dt.minute


def entry_rich_day() -> EntryCondition:
    """Enter Mon–Fri when VRP ≥ vrp_min; otherwise skip (no force)."""
    def check(strategy, state):
        wd = state.dt.weekday()
        if wd not in ENTRY_DAYS_MON_FRI:
            return False
        ctx = strategy._vol_ctx_at(state)
        vrp = ctx.get("vrp", float("nan"))
        rich = (
            isinstance(vrp, (int, float))
            and not math.isnan(vrp)
            and vrp >= strategy._vrp_min
        )
        if not rich:
            return False
        strategy._pending_entry_reason = "rich"
        strategy._pending_vol_ctx = ctx
        return True
    return check


def entry_front_wait() -> EntryCondition:
    """Open the 10:00–force window. IV richness is decided in ``_do_open``."""
    def check(strategy, state):
        minutes = _clock_minutes(state.dt)
        start = strategy._look_start_hour * 60 + strategy._look_start_minute
        force = strategy._entry_hour * 60 + strategy._entry_minute
        if minutes < start:
            return False
        ctx = strategy._vol_ctx_at(state)
        if minutes >= force:
            strategy._pending_entry_reason = "forced"
        else:
            strategy._pending_entry_reason = "front_rich"
        strategy._pending_vol_ctx = ctx
        return True
    return check


class ThetaEngineV14:
    """Short-DTE Mode C: exact 1–3 DTE single, skew side, named entries."""

    name = "theta_engine_v14"
    DATE_RANGE = ("2025-08-17", "2026-08-18")
    DESCRIPTION = (
        "Short-DTE theta (v14): exact 1–3 DTE naked single, cheaper/richer "
        "Skew6 / front / dte123 / agree RR, Daily16 / RichDay / RichForce2 / FrontWait. "
        "entry_hour overrides the named-policy clock. One slot; weekend skip; "
        "net-credit SL/TP (0 = off)."
    )
    indicator_deps = [
        IndicatorDep(
            name="vol_context",
            symbol="BTCUSDT",
            interval="1d",
            warmup_days=60,
        ),
    ]

    # Working grid: #1 (rf2_1500 × skew6) plus fan-out occupancy/clock/skew.
    # dte123 closed (run 730).
    PARAM_GRID = {
        "dte": [1],
        "delta": [0.25],
        "skew_mode": ["cheaper"],
        "skew_source": ["front", "skew6"],
        "book": ["rf2_1500", "rf2_1600", "daily16_1600", "rich_day_1500"],
        "qty_per_1btc_equity": [2.0],
        "late_exit": ["none"],
        "take_profit_pct": [0],
        "stop_loss_pct": [0],
        "max_concurrent": [1],
        "hold_days": [0],
        "front_vrp_min": [0.0],
        "weekend_cover": ["skip"],
    }
    PARAM_HELP = {
        "dte": "Exact calendar DTE (select_expiry). Skip the bar if unlisted.",
        "delta": "Absolute target delta of the short (locked 0.25, Mode C).",
        "skew_mode": "cheaper = sell cheaper 25Δ side (Mode C); richer = invert.",
        "skew_source": (
            "Skew selector: front = 1 DTE 25Δ RR; skew6 = ~14d neighbour RR. "
            "dte123 (mean 1/2/3 DTE RR) lost to both on run 730 — kept in "
            "code, not in the default grid. agree = front and skew6 must match."
        ),
        "entry_policy": (
            "Named entry: daily_1600 / rich_day_1600 / "
            "rich_force_2d_1600 / front_wait_1000_1600. Clock from entry_hour."
        ),
        "book": (
            "#1 is rf2_1500 (RichForce2 15:00); rf2_1600 / daily16_1600 / "
            "rich_day_1500 are fan-out. Skew model is skew_source, not the book."
        ),
        "entry_hour": "UTC clock hour; overrides the named policy's 16:00.",
        "vrp_min": (
            "VRP skip threshold. 0 = ungated Daily; 2/4/6 = skip (Daily) "
            "or rich-or-force (RF2)."
        ),
        "late_exit": (
            "none = hold to expiry; sl_2 = net-credit 2× SL armed only in "
            "the last 4h; prox_1pct = strike proximity (1% of spot) last 4h."
        ),
        "take_profit_pct": "Net-credit capture on executable (0 = hold to expiry).",
        "stop_loss_pct": "Net-credit loss on mark (0 = off; 3.0 = Mode C).",
        "qty_per_1btc_equity": "Contracts per 1 BTC equity (Mode C slot size).",
        "max_concurrent": "Max live shorts (1 = no warehouse).",
        "hold_days": "0 = hold to expiry; else timed exit at the entry clock.",
        "front_vrp_min": "FrontWait: 25Δ mark IV − DVOL (vol points) to enter early.",
        "weekend_cover": "skip = do not sell expiries that settle Sun or Mon.",
    }

    def __init__(self):
        self._delta = 0.25
        self._dte = 1
        self._hold_days = 0
        self._sl_pct = 0.0
        self._tp_pct = 0.0
        self._late_exit = "none"
        self._qty_per_1btc_equity = 0.2
        self._max_concurrent = 1
        self._skew_mode = "cheaper"
        self._skew_source = "front"
        self._weekend_cover = "skip"
        self._front_vrp_min = 0.0
        self._book = None
        self._entry_policy = ENTRY_DAILY16
        self._entry_mode = "daily_clock"
        self._entry_time = "16:00"
        self._look_start = "16:00"
        self._entry_schedule = "mon_fri"
        self._entry_hour = 16
        self._entry_minute = 0
        self._look_start_hour = 16
        self._look_start_minute = 0
        self._entry_days = ENTRY_DAYS_MON_FRI
        self._vrp_min = 0.0
        self._force_after_days = 0
        self._force_weekdays = ENTRY_DAYS_MON_FRI
        self._positions: List[OpenPosition] = []
        self._last_trade_date: Optional[date] = None
        self._last_open_date: Optional[date] = None
        self._pos_counter = 0
        self._entry_conds: List[EntryCondition] = []
        self._exit_conds: List[Any] = []
        self._vol_context = None
        self._pending_entry_reason = "schedule"
        self._pending_vol_ctx: Dict[str, float] = {}

    def set_indicators(self, ind: Dict[str, Any]) -> None:
        self._vol_context = ind.get("vol_context")

    def configure(self, params: Dict[str, Any]) -> None:
        self._delta = float(params.get("delta", 0.25))
        self._dte = int(params.get("dte", 1))
        self._hold_days = int(params.get("hold_days", 0))
        self._sl_pct = float(params.get("stop_loss_pct", 0.0))
        self._tp_pct = float(params.get("take_profit_pct", 0.0))
        self._late_exit = parse_late_exit(params.get("late_exit", "none"))
        self._qty_per_1btc_equity = float(
            params.get("qty_per_1btc_equity", 0.2)
        )
        self._max_concurrent = int(params.get("max_concurrent", 1))
        self._skew_mode = parse_skew_mode(params.get("skew_mode", "cheaper"))
        self._skew_source = parse_skew_source(params.get("skew_source", "front"))
        self._weekend_cover = parse_weekend_cover(
            params.get("weekend_cover", "skip")
        )
        self._front_vrp_min = float(params.get("front_vrp_min", 0.0))

        if self._delta <= 0 or self._delta >= 1:
            raise ValueError("delta must be in (0, 1), got %r" % self._delta)
        if self._dte < 1:
            raise ValueError("dte must be >= 1, got %r" % self._dte)
        if self._hold_days < 0:
            raise ValueError("hold_days must be >= 0, got %r" % self._hold_days)
        if self._sl_pct < 0:
            raise ValueError("stop_loss_pct must be >= 0, got %r" % self._sl_pct)
        if self._tp_pct < 0:
            raise ValueError(
                "take_profit_pct must be >= 0, got %r" % self._tp_pct
            )
        if self._qty_per_1btc_equity <= 0:
            raise ValueError("qty_per_1btc_equity must be > 0")
        if self._max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")

        params = dict(params)
        self._book = None
        if params.get("book") is not None:
            spec = parse_book(params["book"])
            self._book = str(params["book"]).strip().lower()
            params["entry_policy"] = spec["entry_policy"]
            params["entry_hour"] = spec["entry_hour"]

        resolved = resolve_v14_entry_policy(params)
        self._entry_policy = str(resolved["entry_policy"])
        self._entry_mode = str(resolved["entry_mode"])
        self._entry_time = str(resolved["entry_time"])
        self._look_start = str(resolved.get("look_start") or self._entry_time)
        self._entry_schedule = str(resolved.get("entry_schedule") or "mon_fri")
        self._entry_hour, self._entry_minute = parse_entry_time(self._entry_time)
        self._look_start_hour, self._look_start_minute = parse_entry_time(
            self._look_start
        )
        self._entry_days = frozenset(resolved["entry_days"])
        self._vrp_min = float(resolved["vrp_min"])
        self._force_after_days = int(resolved["force_after_days"])
        self._force_weekdays = frozenset(resolved["force_weekdays"])

        if params.get("entry_hour") is not None:
            hour = int(params["entry_hour"])
            if hour < 0 or hour > 23:
                raise ValueError("entry_hour must be 0–23, got %r" % params["entry_hour"])
            clock = "%02d:00" % hour
            self._entry_time = clock
            if self._entry_mode != "front_wait":
                self._look_start = clock
            self._entry_hour, self._entry_minute = parse_entry_time(self._entry_time)
            self._look_start_hour, self._look_start_minute = parse_entry_time(
                self._look_start
            )

        if params.get("vrp_min") is not None:
            self._vrp_min = float(params["vrp_min"])
            if self._vrp_min < 0:
                raise ValueError("vrp_min must be >= 0, got %r" % params["vrp_min"])

        self.reset()

    def on_market_state(self, state: Any) -> List[Trade]:
        trades: List[Trade] = []

        # Exits first so a same-bar SL/TP does not also open a replacement.
        to_close: List[Tuple[OpenPosition, str]] = []
        for pos in list(self._positions):
            reason = self._check_exit(state, pos)
            if reason is not None:
                trades.append(self._do_close(state, pos, reason))
                to_close.append((pos, reason))
        for pos, _reason in to_close:
            self._positions.remove(pos)

        if self._should_enter(state):
            open_trade = self._do_open(state)
            if open_trade is not None:
                trades.append(open_trade)

        return trades

    def on_end(self, state: Any) -> List[Trade]:
        trades = [
            self._do_close(state, pos, "end_of_data")
            for pos in list(self._positions)
        ]
        self._positions.clear()
        return trades

    def reset(self) -> None:
        self._positions = []
        self._last_trade_date = None
        self._last_open_date = None
        self._pos_counter = 0
        self._pending_entry_reason = "schedule"
        self._pending_vol_ctx = {}
        self._entry_conds = self._build_entry_conds()
        self._exit_conds = self._build_exit_conds()

    def describe_params(self) -> Dict[str, Any]:
        return {
            "dte": self._dte,
            "delta": self._delta,
            "skew_mode": self._skew_mode,
            "skew_source": self._skew_source,
            "weekend_cover": self._weekend_cover,
            "front_vrp_min": self._front_vrp_min,
            "hold_days": self._hold_days,
            "book": self._book,
            "entry_policy": self._entry_policy,
            "entry_mode": self._entry_mode,
            "entry_time": self._entry_time,
            "entry_hour": self._entry_hour,
            "look_start": self._look_start,
            "entry_schedule": self._entry_schedule,
            "vrp_min": self._vrp_min,
            "force_after_days": self._force_after_days,
            "stop_loss_pct": self._sl_pct,
            "take_profit_pct": self._tp_pct,
            "late_exit": self._late_exit,
            "max_concurrent": self._max_concurrent,
            "qty_per_1btc_equity": self._qty_per_1btc_equity,
        }

    def n_short_positions(self) -> int:
        return len(self._positions)

    def _build_entry_conds(self) -> List[EntryCondition]:
        conds: List[EntryCondition] = [
            entry_once_per_calendar_day(),
            entry_max_concurrent(self._max_concurrent),
        ]
        if self._entry_mode == "front_wait":
            conds.append(entry_allowed_weekday())
            conds.append(entry_front_wait())
        elif self._entry_mode == "rich_or_forced":
            conds.append(
                entry_at_or_after_utc(self._entry_hour, self._entry_minute)
            )
            conds.append(entry_rich_or_forced())
        elif self._entry_mode == "rich_day":
            conds.append(
                entry_at_or_after_utc(self._entry_hour, self._entry_minute)
            )
            conds.append(entry_rich_day())
        else:
            def _schedule_reason(strategy, state):
                strategy._pending_entry_reason = "schedule"
                strategy._pending_vol_ctx = strategy._vol_ctx_at(state)
                return True
            conds.append(
                entry_at_or_after_utc(self._entry_hour, self._entry_minute)
            )
            if self._vrp_min > 0:
                conds.append(entry_rich_day())
            else:
                conds.append(entry_allowed_weekday())
                conds.append(_schedule_reason)
        return conds

    def _vol_ctx_at(self, state: Any) -> Dict[str, float]:
        return lookup_vol_context(self._vol_context, state.dt)

    def _build_exit_conds(self) -> List[Any]:
        conds: List[Any] = []
        if self._late_exit == "sl_2":
            conds.append(
                exit_expiry_window(
                    net_credit_stop_loss_pct(LATE_SL_PCT, price_mode="mark"),
                    only_final_hours=LATE_EXIT_HOURS,
                )
            )
        elif self._late_exit == "prox_1pct":
            conds.append(
                strike_proximity_stop_pct(LATE_EXIT_HOURS, LATE_PROX_PCT)
            )
        if self._sl_pct > 0:
            conds.append(
                net_credit_stop_loss_pct(self._sl_pct, price_mode="mark")
            )
        if self._tp_pct > 0:
            conds.append(
                net_credit_profit_target_pct(
                    self._tp_pct, price_mode="executable"
                )
            )
        if self._hold_days > 0:
            conds.append(hold_until_planned_dt())
        return conds

    def _tp_threshold_usd(self, net_credit_usd: float) -> Optional[float]:
        if self._tp_pct <= 0:
            return None
        return net_credit_usd * (1.0 - self._tp_pct)

    def _sl_threshold_usd(self, net_credit_usd: float) -> Optional[float]:
        if self._sl_pct <= 0:
            return None
        return net_credit_usd * (1.0 + self._sl_pct)

    def _equity_usd(self, state: Any) -> float:
        eq = getattr(state, "equity_usd", None)
        if eq is not None and eq > 0:
            return float(eq)
        return float(_cfg.simulation.account_size_usd)

    def _compute_quantity(self, state: Any) -> Optional[float]:
        if state.spot <= 0:
            return None
        equity_btc = self._equity_usd(state) / state.spot
        raw = equity_btc * self._qty_per_1btc_equity
        qty = max(round(raw, 1), MIN_QTY)
        if raw < MIN_QTY:
            return None
        return qty

    def _next_pos_id(self) -> int:
        self._pos_counter += 1
        return self._pos_counter

    def _planned_exit_dt(self, entry_date: date, tzinfo: Any) -> datetime:
        exit_day = entry_date + timedelta(days=self._hold_days)
        return datetime(
            exit_day.year, exit_day.month, exit_day.day,
            self._entry_hour, self._entry_minute,
            tzinfo=tzinfo,
        )

    def _should_enter(self, state: Any) -> bool:
        return all(cond(self, state) for cond in self._entry_conds)

    def _is_force_clock(self, state: Any) -> bool:
        minutes = _clock_minutes(state.dt)
        force = self._entry_hour * 60 + self._entry_minute
        return minutes >= force

    def _consume_entry_day(self, state: Any, reason: str, **extra: Any) -> None:
        self._last_trade_date = state.dt.date()
        logger.info(
            "[theta_engine_v14] ENTRY_SKIP dt=%s reason=%s %s",
            state.dt.isoformat(),
            reason,
            " ".join("%s=%s" % (k, v) for k, v in extra.items()),
        )

    def _retry_or_consume_skew(
        self, state: Any, reason: str, **extra: Any
    ) -> None:
        """FrontWait retries until the force clock; clock policies consume."""
        if self._entry_mode == "front_wait" and not self._is_force_clock(state):
            return
        self._consume_entry_day(state, reason, **extra)

    # ------------------------------------------------------------------
    # Open path
    # ------------------------------------------------------------------

    def _measure_front(self, state: Any, expiry: str) -> Optional[Dict[str, Any]]:
        rr = rr_25d_on_expiry(state, expiry)
        if rr is None:
            return None
        return {
            "rr_composite": rr,
            "n_rungs": 1,
            "center_dte_actual": self._dte,
        }

    def _measure_skew(self, state: Any, expiry: str) -> Optional[Dict[str, Any]]:
        if self._skew_source == "front":
            return self._measure_front(state, expiry)
        if self._skew_source == "dte123":
            return compute_dte123(state)
        return compute_skew6(state)

    def _do_open(self, state: Any) -> Optional[Trade]:
        quantity = self._compute_quantity(state)
        if quantity is None:
            return None
        expiry = select_expiry(state, self._dte)
        if expiry is None:
            return None
        chain = state.get_chain(expiry)
        if not chain:
            return None

        exp_dt = expiry_dt_utc(expiry, state.dt.tzinfo)
        if self._weekend_cover == "skip" and expiry_covers_weekend(exp_dt):
            self._consume_entry_day(
                state,
                "weekend_cover_skip",
                expiry=expiry,
                dte=self._dte,
            )
            return None

        calls = [q for q in chain if q.is_call]
        puts = [q for q in chain if not q.is_call]
        call_q = select_by_delta(calls, +self._delta)
        put_q = select_by_delta(puts, -self._delta)
        if call_q is None or put_q is None:
            return None
        if call_q.bid <= 0 or put_q.bid <= 0:
            return None

        if self._skew_source == "agree":
            front = self._measure_front(state, expiry)
            six = compute_skew6(state)
            if front is None or six is None:
                self._retry_or_consume_skew(
                    state,
                    "skew_unavailable",
                    skew_source=self._skew_source,
                )
                return None
            zone_front = skew_zone(front["rr_composite"], mode=self._skew_mode)
            zone_six = skew_zone(six["rr_composite"], mode=self._skew_mode)
            if (
                zone_front == "neutral"
                or zone_six == "neutral"
                or zone_front != zone_six
            ):
                self._retry_or_consume_skew(
                    state,
                    "skew_disagree_skip",
                    zone_front=zone_front,
                    zone_skew6=zone_six,
                    rr_front="%.4f" % front["rr_composite"],
                    rr_skew6="%.4f" % six["rr_composite"],
                )
                return None
            zone = zone_front
            skew = {
                "rr_composite": six["rr_composite"],
                "n_rungs": six["n_rungs"],
                "center_dte_actual": six.get("center_dte_actual"),
                "rr_front": front["rr_composite"],
                "rr_skew6": six["rr_composite"],
            }
        else:
            skew = self._measure_skew(state, expiry)
            if skew is None:
                self._retry_or_consume_skew(
                    state,
                    "skew_unavailable",
                    skew_source=self._skew_source,
                )
                return None
            zone = skew_zone(skew["rr_composite"], mode=self._skew_mode)
            if zone == "neutral":
                self._retry_or_consume_skew(
                    state,
                    "skew_neutral_skip",
                    rr_composite="%.4f" % skew["rr_composite"],
                )
                return None

        side_q = call_q if zone == "call" else put_q
        front_iv = float(getattr(side_q, "mark_iv", 0.0) or 0.0)
        ctx = getattr(self, "_pending_vol_ctx", None) or self._vol_ctx_at(state)
        dvol = ctx.get("dvol", float("nan"))
        front_vrp = (
            front_iv - float(dvol)
            if isinstance(dvol, (int, float)) and not math.isnan(dvol)
            else float("nan")
        )

        if self._entry_mode == "front_wait" and not self._is_force_clock(state):
            rich_front = (
                isinstance(front_vrp, (int, float))
                and not math.isnan(front_vrp)
                and front_vrp >= self._front_vrp_min
            )
            if not rich_front:
                return None
            self._pending_entry_reason = "front_rich"

        merged: Dict[str, Any] = {
            "skew_zone": zone,
            "rr_composite": skew["rr_composite"],
            "skew_n_rungs": skew["n_rungs"],
            "skew_dte": (
                self._dte if self._skew_source == "front"
                else 2 if self._skew_source == "dte123"
                else SKEW_DTE
            ),
            "rr_front": skew.get("rr_front"),
            "rr_skew6": skew.get("rr_skew6"),
            "rr_dte1": skew.get("rr_dte1"),
            "rr_dte2": skew.get("rr_dte2"),
            "rr_dte3": skew.get("rr_dte3"),
            "skew_center_dte_actual": skew.get("center_dte_actual"),
            "skew_mode": self._skew_mode,
            "skew_source": self._skew_source,
            "front_iv": front_iv,
            "front_vrp": front_vrp,
            "dte": self._dte,
        }

        return self._open_single(
            state, expiry, exp_dt, chain, zone == "call", quantity,
            call_bid=call_q.bid, put_bid=put_q.bid,
            meta_extra=merged,
        )

    def _open_single(
        self,
        state: Any,
        expiry: str,
        exp_dt: Any,
        chain: list,
        is_call: bool,
        quantity: float,
        call_bid: float = 0.0,
        put_bid: float = 0.0,
        meta_extra: Optional[Dict[str, Any]] = None,
    ) -> Optional[Trade]:
        quotes = [q for q in chain if q.is_call == is_call]
        target_delta = +self._delta if is_call else -self._delta
        short_q = select_by_delta(quotes, target_delta)
        if short_q is None or short_q.bid <= 0:
            return None

        short_btc = short_q.bid
        short_per_usd = short_btc * state.spot
        short_usd = short_per_usd * quantity
        if short_usd <= 0:
            return None

        fee_per = deribit_fee_per_leg(state.spot, short_per_usd)
        fees_open = fee_per * quantity
        legs = [
            self._leg_dict(
                short_q, expiry, short_btc, short_per_usd, state,
                fee_per, quantity, side="sell",
            )
        ]

        pos_id = self._next_pos_id()
        entry_date = state.dt.date()
        equity_usd = self._equity_usd(state)
        leg_type = "call" if is_call else "put"
        strike_key = "call_strike" if is_call else "put_strike"
        delta_key = "call_delta" if is_call else "put_delta"

        meta: Dict[str, Any] = {
            "direction": "sell",
            "leg_type": leg_type,
            "expiry": expiry,
            "expiry_dt": exp_dt,
            strike_key: short_q.strike,
            delta_key: short_q.delta,
            "call_bid": call_bid,
            "put_bid": put_bid,
            "qty": quantity,
            "net_credit_usd": short_usd,
            "pos_id": pos_id,
            "sl_threshold_usd": self._sl_threshold_usd(short_usd),
            "tp_threshold_usd": self._tp_threshold_usd(short_usd),
            "equity_usd_at_entry": equity_usd,
            "equity_btc_at_entry": equity_usd / state.spot if state.spot else 0.0,
            "qty_per_1btc_equity": self._qty_per_1btc_equity,
            "entry_policy": self._entry_policy,
            "entry_mode": self._entry_mode,
            "entry_reason": getattr(self, "_pending_entry_reason", "schedule"),
            "n_open_after": self.n_short_positions() + 1,
        }
        ctx = getattr(self, "_pending_vol_ctx", None) or {}
        if not ctx:
            ctx = self._vol_ctx_at(state)
        for k in ("dvol", "rv30", "vrp", "dvol_rank_60"):
            if k in ctx:
                meta[k] = ctx[k]
        if self._last_open_date is not None:
            meta["days_since_prior_entry"] = (
                state.dt.date() - self._last_open_date
            ).days
        else:
            meta["days_since_prior_entry"] = None
        if self._hold_days > 0:
            meta["planned_exit_dt"] = self._planned_exit_dt(
                entry_date, state.dt.tzinfo
            )
        if meta_extra:
            meta.update(meta_extra)

        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=legs,
            entry_price_usd=short_usd,
            fees_open=fees_open,
            metadata=meta,
        )
        self._positions.append(pos)
        self._last_trade_date = state.dt.date()
        self._last_open_date = state.dt.date()
        self._log_open(state, pos)
        return self._open_trade(state, pos, legs, pos_id, short_usd, fees_open)

    def _leg_dict(
        self,
        quote: Any,
        expiry: str,
        entry_btc: float,
        entry_usd_per: float,
        state: Any,
        fee_per: float,
        quantity: float,
        side: str = "sell",
    ) -> Dict[str, Any]:
        return {
            "strike": quote.strike,
            "is_call": quote.is_call,
            "expiry": expiry,
            "side": side,
            "qty": quantity,
            "price_btc": entry_btc,
            "entry_price": entry_btc,
            "entry_price_usd": entry_usd_per,
            "entry_spot": state.spot,
            "entry_bid": quote.bid,
            "entry_ask": quote.ask,
            "entry_mark": quote.mark,
            "entry_iv": quote.mark_iv,
            "entry_delta": quote.delta,
            "fee_usd_open": fee_per * quantity,
        }

    def _open_trade(
        self,
        state: Any,
        pos: OpenPosition,
        legs: list,
        pos_id: int,
        entry_usd: float,
        fees_open: float,
    ) -> Trade:
        return Trade(
            entry_time=state.dt,
            exit_time=state.dt,
            entry_spot=state.spot,
            exit_spot=state.spot,
            entry_price_usd=entry_usd,
            exit_price_usd=0.0,
            fees=fees_open,
            pnl=0.0,
            triggered=False,
            exit_reason="",
            exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={
                "direction": str(pos.metadata.get("direction", "sell")),
                "pos_id": pos_id,
                "leg_type": pos.metadata["leg_type"],
                "comment": pos.metadata.get("comment", ""),
                "legs": legs,
            },
        )

    # ------------------------------------------------------------------
    # Exit / close
    # ------------------------------------------------------------------

    def _check_exit(self, state: Any, pos: OpenPosition) -> Optional[str]:
        reason = check_expiry(state, pos)
        if reason is not None:
            return reason

        for cond in self._exit_conds:
            reason = cond(state, pos)
            if reason is not None:
                break

        if reason is not None and reason not in _QUOTE_FREE_EXIT_REASONS:
            if not self._quotes_available(state, pos):
                logger.debug(
                    "[%s] Exit '%s' suppressed for pos_id=%s — quote missing",
                    state.dt, reason, pos.metadata.get("pos_id"),
                )
                return None
        return reason

    def _quotes_available(self, state: Any, pos: OpenPosition) -> bool:
        expiry = pos.metadata["expiry"]
        is_call = pos.metadata["leg_type"] == "call"
        strike = (
            pos.metadata.get("call_strike") if is_call
            else pos.metadata.get("put_strike")
        )
        if strike is None and pos.legs:
            strike = pos.legs[0].get("strike")
        if strike is None:
            return False
        return state.get_option(expiry, strike, is_call) is not None

    def _do_close(self, state: Any, pos: OpenPosition, reason: str) -> Trade:
        trade = self._close_package(state, pos, reason)
        trade.metadata["skip_open_fill"] = True
        trade.metadata.update({
            "direction": "sell",
            "leg_type": pos.metadata["leg_type"],
            "comment": pos.metadata.get("comment", ""),
            "delta": self._delta,
            "dte": self._dte,
            "hold_days": self._hold_days,
            "stop_loss_pct": self._sl_pct,
            "take_profit_pct": self._tp_pct,
            "max_concurrent": self._max_concurrent,
            "qty": float(pos.metadata.get("qty", 1.0)),
            "net_credit_usd": float(pos.metadata.get("net_credit_usd", 0.0)),
            "qty_per_1btc_equity": self._qty_per_1btc_equity,
            "skew_mode": self._skew_mode,
            "skew_source": self._skew_source,
            "skew_dte": pos.metadata.get("skew_dte"),
            "rr_composite": pos.metadata.get("rr_composite"),
            "skew_zone": pos.metadata.get("skew_zone"),
            "entry_reason": pos.metadata.get("entry_reason"),
            "pos_id": pos.metadata.get("pos_id"),
        })
        self._log_close(state, pos, trade, reason)
        return trade

    def _close_package(
        self, state: Any, pos: OpenPosition, reason: str
    ) -> Trade:
        fees_close = 0.0
        exit_usd_total = 0.0
        for leg in pos.legs:
            strike = float(leg["strike"])
            is_call = bool(leg["is_call"])
            qty = float(leg.get("qty", 1.0))
            side = str(leg.get("side", "sell"))
            q = state.get_option(leg["expiry"], strike, is_call)

            if reason == "expiry":
                if is_call:
                    exit_usd = max(0.0, state.spot - strike)
                else:
                    exit_usd = max(0.0, strike - state.spot)
                exit_btc = (exit_usd / state.spot) if state.spot else 0.0
            else:
                if side == "buy":
                    exit_btc = q.bid if q and q.bid > 0 else FLOOR_BTC
                else:
                    exit_btc = q.ask if q and q.ask > 0 else FLOOR_BTC
                exit_usd = exit_btc * state.spot
                fees_close += deribit_fee_per_leg(state.spot, exit_usd) * qty

            leg["exit_price_btc"] = exit_btc
            leg["exit_price_usd"] = exit_usd
            exit_usd_total += exit_usd * qty

        return close_position(state, pos, reason, exit_usd_total, fees_close)

    def _log_open(self, state: Any, pos: OpenPosition) -> None:
        exp_date = parse_expiry_date(pos.metadata["expiry"])
        dte = (exp_date.date() - state.dt.date()).days if exp_date else -1
        logger.info(
            "[theta_engine_v14] OPEN pos_id=%s leg_type=%s skew_zone=%s "
            "skew_source=%s rr=%.4f reason=%s dt=%s expiry=%s dte=%s "
            "spot=%.2f qty=%.1f net_credit=%.2f",
            pos.metadata.get("pos_id"),
            pos.metadata.get("leg_type"),
            pos.metadata.get("skew_zone"),
            pos.metadata.get("skew_source"),
            float(pos.metadata.get("rr_composite") or 0.0),
            pos.metadata.get("entry_reason"),
            state.dt.isoformat(),
            pos.metadata.get("expiry"),
            dte,
            state.spot,
            float(pos.metadata.get("qty", 1.0)),
            float(pos.metadata.get("net_credit_usd", 0.0)),
        )

    def _log_close(
        self, state: Any, pos: OpenPosition, trade: Trade, reason: str
    ) -> None:
        held_h = int((state.dt - pos.entry_time).total_seconds() // 3600)
        logger.info(
            "[theta_engine_v14] CLOSE pos_id=%s reason=%s dt=%s held_h=%s "
            "qty=%.1f entry_usd=%.2f exit_usd=%.2f pnl=%.2f",
            pos.metadata.get("pos_id"),
            reason,
            state.dt.isoformat(),
            held_h,
            float(pos.metadata.get("qty", 1.0)),
            pos.entry_price_usd,
            trade.exit_price_usd,
            trade.pnl,
        )

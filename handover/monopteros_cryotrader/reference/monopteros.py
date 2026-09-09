#!/usr/bin/env python3
"""
TuDySho Monopteros — short-dated Bitcoin volatility selling (all weekday schedules).

Product / display name: **Monopteros**. Catalog ID: ``tudysho_monopteros``.

One strategy with three NYC entry schedules (Mon–Thu midday, Monday early,
Friday late morning) → one trade list → one equity curve. Forked from
``tudysho_eisbach`` (frozen live-parity artefact).

Schedule trade params are the Aug-2026 research locks (mon_thu /
mon_early / fri favourites). Watch windows follow CryoTrader slot-02.
Sizing is the product lock (0.5% NAV / max 6 contracts per 1 BTC equity).
Global equity drawdown stop is 5% (from the mon_thu lock).

Deliberate backtester choices (not live production):
    • Turbulence gate uses the current hour bucket (same as ``tudysho``).
    • Global ``nav_premium_pct`` / ``max_qty_per_1btc_equity`` — no absolute
      ``max_quantity`` cap; no per-schedule nav override (live has those).
    • Account is the simulation dollar book ($100k default).

Live behaviour mirrored here:
    • Schedule routing + watch windows (NYC entry times, DST-aware).
    • Max one concurrent open position.
    • Per-schedule daily cap on NYC calendar date (Monday can trade twice).
    • Per-schedule entry/exit params (delta, DTE, min_otm, turb, SL, proximity).
    • Strangle only; no take-profit.

See also: ``tudysho_eisbach`` — frozen live-parity twin; do not overwrite it
when refreshing Monopteros from a newer slot-02 TOML.
"""
from __future__ import annotations

import logging
import math
from copy import deepcopy
from datetime import datetime, time as dt_time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from backtester.core.option_selection import select_by_delta
from backtester.core.expiry_utils import expiry_dt_utc, select_expiry
from backtester.core.config import cfg as _cfg
from backtester.indicators import IndicatorDep
from backtester.core.pricing import deribit_fee_per_leg
from backtester.core.strategy_base import (
    OpenPosition,
    Trade,
    check_expiry,
    close_position,
    stop_loss_pct,
    strike_proximity_stop,
    position_quotes_available,
    equity_drawdown_stop,
    exit_expiry_window,
)
from backtester.core.market_hours import to_nyc, to_utc


logger = logging.getLogger(__name__)

__version__ = "1.0.0"
RELEASE_NAME = "Monopteros"

# Research lock source (Aug 2026 schedule picks). Watch windows kept from live slot-02.
_LIVE_SLOT_REF = (
    "Aug-2026 research lock: mon_thu=16dcb4f42c9a/768, "
    "mon_early=b012cb51c397/764, fri=fd39a836394d/766 "
    "(watch windows from CryoTrader slot-02.toml @ 2026-08-28)"
)

UTC = timezone.utc
_MIN_TICK_BTC = 0.0001
_QUOTE_FREE_EXIT_REASONS = frozenset({"strike_proximity_stop", "expiry"})

_SCHEDULE_IDS = ("mon_thu", "mon_early", "fri")

# Trade-param defaults — Aug 2026 Monopteros schedule locks (see _LIVE_SLOT_REF).
# mon_early.enabled=true in live; Monopteros always runs all three schedules.
# Per-schedule nav from slice discovery is NOT applied — product sizing is global
# PARAM_GRID (nav_premium_pct / max_qty_per_1btc_equity).
_SCHEDULE_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "mon_thu": {
        "label": "Mon–Thu afternoon (1-DTE)",
        "entry_time": "14:00",
        "dte": 1,
        "delta": 0.05,
        "min_otm_pct": 2.2,
        "turbulence_threshold": 70.0,
        "stop_loss_pct": 0.0,
        "proximity_stop_hours": 8.0,
        "proximity_buffer_usd": 500.0,
        "premium_sl_except_final_hours": 8.0,
        "watch_until_utc_midnight": True,
        "watch_until_utc_hour": 8,
    },
    "mon_early": {
        "label": "Monday early (0-DTE)",
        "entry_time": "00:05",
        "dte": 0,
        "delta": 0.05,
        "min_otm_pct": 1.0,
        "turbulence_threshold": 99.0,
        "stop_loss_pct": 6.0,
        "proximity_stop_hours": 4.0,
        "proximity_buffer_usd": 800.0,
        "premium_sl_except_final_hours": 4.0,
        "watch_until_utc_midnight": False,
        "watch_until_utc_hour": 7,  # live slot-02
    },
    "fri": {
        "label": "Friday late-morning (Sat expiry)",
        "entry_time": "12:30",
        "dte": 1,
        "delta": 0.06,
        "min_otm_pct": 2.4,
        "turbulence_threshold": 99.0,
        "stop_loss_pct": 3.5,
        "proximity_stop_hours": 8.0,
        "proximity_buffer_usd": 1000.0,
        "premium_sl_except_final_hours": 8.0,
        "watch_until_utc_midnight": True,
        "watch_until_utc_hour": 8,
    },
}


def _parse_entry_time(value: str) -> Tuple[int, int]:
    parts = value.strip().split(":")
    hour = int(parts[0])
    minute = int(parts[1]) if len(parts) > 1 else 0
    return hour, minute


def _at_or_after(t: dt_time, hhmm: str) -> bool:
    hour, minute = _parse_entry_time(hhmm)
    return t.hour > hour or (t.hour == hour and t.minute >= minute)


def resolve_schedule(
    nyc_weekday: int,
    nyc_time: dt_time,
    schedules: Dict[str, Dict[str, Any]],
) -> Optional[str]:
    """Return schedule id or None.

    Monday: ``mon_thu`` from its entry_time onward; ``mon_early`` from its
    entry_time until ``mon_thu`` entry_time.
    """
    if nyc_weekday == 0:
        if _at_or_after(nyc_time, schedules["mon_thu"]["entry_time"]):
            return "mon_thu"
        if _at_or_after(nyc_time, schedules["mon_early"]["entry_time"]):
            return "mon_early"
        return None
    if nyc_weekday in (1, 2, 3) and _at_or_after(
        nyc_time, schedules["mon_thu"]["entry_time"],
    ):
        return "mon_thu"
    if nyc_weekday == 4 and _at_or_after(nyc_time, schedules["fri"]["entry_time"]):
        return "fri"
    return None


def _entry_utc_today(schedule_params: Dict[str, Any], now_utc: datetime) -> datetime:
    hour, minute = _parse_entry_time(schedule_params["entry_time"])
    nyc_today = to_nyc(now_utc)
    nyc_entry = nyc_today.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return to_utc(nyc_entry)


def in_watch_window(schedule_params: Dict[str, Any], now_utc: datetime) -> bool:
    entry_utc = _entry_utc_today(schedule_params, now_utc)
    if now_utc < entry_utc:
        return False
    if schedule_params["watch_until_utc_midnight"]:
        watch_end = datetime.combine(
            entry_utc.date() + timedelta(days=1),
            dt_time(0, 0),
            tzinfo=UTC,
        )
        return now_utc < watch_end
    watch_hour = int(schedule_params["watch_until_utc_hour"])
    watch_end = datetime.combine(
        entry_utc.date(),
        dt_time(watch_hour, 0),
        tzinfo=UTC,
    )
    return now_utc < watch_end


def _apply_min_otm(chain, selected, spot, min_pct, is_call):
    # type: (list, Any, float, float, bool) -> Optional[Any]
    factor = min_pct / 100.0
    if is_call:
        floor = spot * (1.0 + factor)
        if selected.strike >= floor:
            return selected
        candidates = sorted(
            [q for q in chain if q.strike >= floor],
            key=lambda q: q.strike,
        )
    else:
        floor = spot * (1.0 - factor)
        if selected.strike <= floor:
            return selected
        candidates = sorted(
            [q for q in chain if q.strike <= floor],
            key=lambda q: q.strike,
            reverse=True,
        )
    return candidates[0] if candidates else None


def _sell_leg_dict(quote, expiry, qty, is_call, spot):
    # type: (Any, str, float, bool, float) -> Tuple[Dict[str, Any], float]
    fee_usd = deribit_fee_per_leg(spot, quote.bid_usd)
    leg = {
        "strike": quote.strike,
        "is_call": is_call,
        "expiry": expiry,
        "side": "sell",
        "qty": qty,
        "price_btc": quote.bid,
        "entry_price": quote.bid,
        "entry_price_usd": quote.bid_usd,
        "entry_spot": spot,
        "entry_bid": quote.bid,
        "entry_ask": quote.ask,
        "entry_mark": quote.mark,
        "entry_iv": quote.mark_iv,
        "entry_delta": quote.delta,
        "fee_usd_open": fee_usd,
    }
    return leg, fee_usd


def _build_exit_conds(schedule_params, equity_drawdown_stop_pct=0.0,
                      equity_sl_only_final_hours=0.0,
                      equity_sl_except_final_hours=0.0):
    # type: (Dict[str, Any], float, float, float) -> List[Any]
    """Wire proximity → optional equity → premium stops for one schedule."""
    conds = []
    prox_h = float(schedule_params.get("proximity_stop_hours", 0))
    if prox_h > 0:
        conds.append(
            strike_proximity_stop(
                prox_h,
                float(schedule_params.get("proximity_buffer_usd", 0)),
            )
        )

    if equity_drawdown_stop_pct > 0:
        equity_cond = equity_drawdown_stop(equity_drawdown_stop_pct, price_mode="mark")
        if equity_sl_only_final_hours > 0 or equity_sl_except_final_hours > 0:
            equity_cond = exit_expiry_window(
                equity_cond,
                only_final_hours=equity_sl_only_final_hours,
                except_final_hours=equity_sl_except_final_hours,
            )
        conds.append(equity_cond)

    sl_pct = float(schedule_params.get("stop_loss_pct", 0))
    if sl_pct > 0:
        premium_cond = stop_loss_pct(sl_pct, price_mode="mark")
        except_h = float(schedule_params.get("premium_sl_except_final_hours", 0))
        if except_h > 0:
            premium_cond = exit_expiry_window(
                premium_cond, except_final_hours=except_h,
            )
        conds.append(premium_cond)
    return conds


class TuDyShoMonopteros:
    """Monopteros: short-dated vol selling with three NYC schedules."""

    name = "tudysho_monopteros"
    # Full window through Aug-31 discovery locks.
    DATE_RANGE = ("2025-04-11", "2026-08-31")
    DESCRIPTION = (
        "TuDySho Monopteros (v1.0.0) — mon_thu / mon_early / fri from "
        f"{_LIVE_SLOT_REF}. Short-dated naked strangles; 0.5% NAV sizing "
        "(max 6 contracts per 1 BTC equity). Max 1 concurrent; per-schedule "
        "NYC daily cap; no take-profit. Global equity DD stop 5% (from mon_thu lock)."
    )

    indicator_deps = [
        IndicatorDep(name="turbulence", symbol="BTCUSDT", interval="15m"),
    ]

    # One-cell product lock — schedule knobs live in _SCHEDULE_DEFAULTS.
    PARAM_GRID = {
        "nav_premium_pct": [0.5],
        "max_qty_per_1btc_equity": [6],
        "leg_min_price": [0],
        "equity_drawdown_stop_pct": [5],
        "equity_sl_only_final_hours": [0],
        "equity_sl_except_final_hours": [0],
    }
    PARAM_HELP = {
        "nav_premium_pct": (
            "Target premium as % of USD NAV (0.5 = 0.5%). 0 = fixed 1 contract. "
            "Monopteros Aug-2026 product sizing."
        ),
        "max_qty_per_1btc_equity": (
            "Hard cap: contracts per 1 BTC of realized equity. 0 = uncapped. "
            "Product lock 6."
        ),
        "leg_min_price": (
            "Minimum bid in BTC per leg (0 = require bid > 0 only)."
        ),
        "equity_drawdown_stop_pct": (
            "Mark equity drawdown SL as fraction of equity_at_entry "
            "(5 = 5% — from mon_thu lock; applies to all schedules)."
        ),
        "equity_sl_only_final_hours": (
            "If > 0, equity SL only in final N hours before expiry."
        ),
        "equity_sl_except_final_hours": (
            "If > 0, suppress equity SL in final N hours before expiry."
        ),
    }

    def __init__(self):
        self._positions = []  # type: List[OpenPosition]
        self._schedules = deepcopy(_SCHEDULE_DEFAULTS)
        self._max_concurrent = 1
        self._nav_premium_pct = 0.5
        self._max_qty_per_1btc_equity = 6.0
        self._leg_min_price = 0.0
        self._equity_drawdown_stop_pct = 5.0
        self._equity_sl_only_final_hours = 0.0
        self._equity_sl_except_final_hours = 0.0
        self._last_trade_nyc_date = {sid: None for sid in _SCHEDULE_IDS}  # type: Dict[str, Optional[str]]
        self._pos_counter = 0
        self._turbulence = None  # type: Optional[Any]

    def set_indicators(self, ind):
        # type: (Dict[str, Any]) -> None
        self._turbulence = ind.get("turbulence")

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._nav_premium_pct = float(params.get("nav_premium_pct", 0.5))
        self._max_qty_per_1btc_equity = float(params.get("max_qty_per_1btc_equity", 6))
        self._leg_min_price = float(params.get("leg_min_price", 0))
        self._equity_drawdown_stop_pct = float(params.get("equity_drawdown_stop_pct", 5))
        self._equity_sl_only_final_hours = float(params.get("equity_sl_only_final_hours", 0))
        self._equity_sl_except_final_hours = float(
            params.get("equity_sl_except_final_hours", 0)
        )
        self._schedules = deepcopy(_SCHEDULE_DEFAULTS)
        for sid in _SCHEDULE_IDS:
            override = params.get(f"schedule_{sid}")
            if isinstance(override, dict):
                self._schedules[sid].update(override)
        self._max_concurrent = 1
        self._positions = []
        self._last_trade_nyc_date = {sid: None for sid in _SCHEDULE_IDS}
        self._pos_counter = 0

    def reset(self):
        # type: () -> None
        self._positions = []
        self._last_trade_nyc_date = {sid: None for sid in _SCHEDULE_IDS}
        self._pos_counter = 0

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "release_name": RELEASE_NAME,
            "strategy_version": __version__,
            "live_slot_ref": _LIVE_SLOT_REF,
            "nav_premium_pct": self._nav_premium_pct,
            "max_qty_per_1btc_equity": self._max_qty_per_1btc_equity,
            "leg_min_price": self._leg_min_price,
            "equity_drawdown_stop_pct": self._equity_drawdown_stop_pct,
            "equity_sl_only_final_hours": self._equity_sl_only_final_hours,
            "equity_sl_except_final_hours": self._equity_sl_except_final_hours,
            "max_concurrent": self._max_concurrent,
            "schedules": {
                sid: {
                    k: self._schedules[sid][k]
                    for k in (
                        "entry_time", "dte", "delta", "min_otm_pct",
                        "turbulence_threshold", "stop_loss_pct",
                        "proximity_stop_hours", "proximity_buffer_usd",
                        "premium_sl_except_final_hours",
                        "watch_until_utc_midnight", "watch_until_utc_hour",
                    )
                }
                for sid in _SCHEDULE_IDS
            },
        }

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        to_close = []
        for pos in list(self._positions):
            reason = self._check_exit(state, pos)
            if reason:
                trades.append(self._close(state, pos, reason))
                to_close.append(pos)
        for pos in to_close:
            self._positions.remove(pos)

        if len(self._positions) < self._max_concurrent:
            open_trade = self._maybe_open(state)
            if open_trade is not None:
                trades.append(open_trade)

        return trades

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        trades = []
        for pos in list(self._positions):
            trades.append(self._close(state, pos, "end_of_data"))
        self._positions.clear()
        return trades

    def _check_exit(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[str]
        reason = check_expiry(state, pos)
        if reason is None:
            sched = pos.metadata.get("schedule_params") or {}
            for cond in _build_exit_conds(
                sched,
                equity_drawdown_stop_pct=self._equity_drawdown_stop_pct,
                equity_sl_only_final_hours=self._equity_sl_only_final_hours,
                equity_sl_except_final_hours=self._equity_sl_except_final_hours,
            ):
                reason = cond(state, pos)
                if reason is not None:
                    break

        if reason is not None and reason not in _QUOTE_FREE_EXIT_REASONS:
            if not position_quotes_available(state, pos):
                logger.debug(
                    "[%s] Exit '%s' suppressed — option quote missing",
                    state.dt, reason,
                )
                return None
        return reason

    def _account_usd(self, state, attr):
        # type: (Any, str) -> float
        value = getattr(state, attr, None)
        if value is not None:
            return float(value)
        fallback = float(_cfg.simulation.account_size_usd)
        logger.debug(
            "[%s] state.%s missing; using account_size_usd=%.2f",
            getattr(state, "dt", None), attr, fallback,
        )
        return fallback

    def _nav_usd_at_entry(self, state):
        # type: (Any) -> float
        return self._account_usd(state, "nav_usd")

    def _equity_at_entry_usd(self, state):
        # type: (Any) -> float
        return self._account_usd(state, "equity_usd")

    def _bid_acceptable(self, bid_btc):
        # type: (float) -> bool
        if self._leg_min_price > 0:
            return bid_btc >= self._leg_min_price
        return bid_btc > 0

    def _compute_quantity(self, state, premium_usd_per_contract):
        # type: (Any, float) -> Optional[Tuple[float, Dict[str, Any]]]
        spot = float(getattr(state, "spot", 0.0) or 0.0)
        equity_usd = self._equity_at_entry_usd(state)

        if self._nav_premium_pct <= 0:
            equity_btc = (equity_usd / spot) if spot > 0 else 0.0
            return (1.0, {
                "nav_usd_at_entry": self._nav_usd_at_entry(state),
                "equity_usd_at_entry": equity_usd,
                "equity_btc_at_entry": equity_btc,
                "spot_at_entry": spot,
                "nav_premium_pct": 0.0,
                "target_premium_usd": 0.0,
                "premium_usd_per_contract": premium_usd_per_contract,
                "qty_from_premium": 1.0,
                "max_qty_per_1btc_equity": self._max_qty_per_1btc_equity,
                "max_contracts_applied": None,
                "premium_capped": False,
            })

        nav_usd = self._nav_usd_at_entry(state)
        if nav_usd <= 0 or premium_usd_per_contract <= 0 or spot <= 0:
            return None

        target_premium_usd = nav_usd * (self._nav_premium_pct / 100.0)
        qty_from_premium = target_premium_usd / premium_usd_per_contract
        equity_btc = equity_usd / spot

        if self._max_qty_per_1btc_equity > 0:
            max_contracts = equity_btc * self._max_qty_per_1btc_equity
            if max_contracts < 0.1:
                return None
            premium_capped = qty_from_premium > max_contracts
            quantity = max(round(min(qty_from_premium, max_contracts), 1), 0.1)
        else:
            max_contracts = None
            premium_capped = False
            quantity = max(round(qty_from_premium, 1), 0.1)

        sizing = {
            "nav_usd_at_entry": nav_usd,
            "equity_usd_at_entry": equity_usd,
            "equity_btc_at_entry": equity_btc,
            "spot_at_entry": spot,
            "nav_premium_pct": self._nav_premium_pct,
            "target_premium_usd": target_premium_usd,
            "premium_usd_per_contract": premium_usd_per_contract,
            "qty_from_premium": qty_from_premium,
            "max_qty_per_1btc_equity": self._max_qty_per_1btc_equity,
            "max_contracts_applied": max_contracts,
            "premium_capped": premium_capped,
        }
        return quantity, sizing

    def _turbulence_ok(self, dt, threshold):
        # type: (datetime, float) -> bool
        """Fail-open when indicator data is missing or NaN (backtester current-hour)."""
        if self._turbulence is None:
            return True

        hour_ts = dt.replace(minute=0, second=0, microsecond=0)
        try:
            composite = self._turbulence.loc[hour_ts]["composite"]
        except KeyError:
            return True

        try:
            if math.isnan(composite):
                return True
        except TypeError:
            return True

        return float(composite) < threshold

    def _maybe_open(self, state):
        # type: (Any) -> Optional[Trade]
        dt = state.dt
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        nyc = to_nyc(dt)
        schedule_id = resolve_schedule(nyc.weekday(), nyc.time(), self._schedules)
        if schedule_id is None:
            return None

        nyc_date = nyc.date().isoformat()
        if self._last_trade_nyc_date.get(schedule_id) == nyc_date:
            return None

        params = self._schedules[schedule_id]
        if not in_watch_window(params, dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)):
            return None

        if not self._turbulence_ok(dt, float(params["turbulence_threshold"])):
            return None

        return self._try_open(state, schedule_id, params)

    def _try_open(self, state, schedule_id, params):
        # type: (Any, str, Dict[str, Any]) -> Optional[Trade]
        dte = int(params["dte"])
        expiry = select_expiry(state, dte)
        if expiry is None:
            return None

        chain = state.get_chain(expiry)
        if not chain:
            return None

        calls = [q for q in chain if q.is_call]
        puts = [q for q in chain if not q.is_call]
        exp_dt = expiry_dt_utc(expiry, state.dt.tzinfo)
        return self._open_strangle(state, schedule_id, params, expiry, exp_dt, calls, puts)

    def _open_strangle(self, state, schedule_id, params, expiry, exp_dt, calls, puts):
        # type: (Any, str, Dict[str, Any], str, Any, list, list) -> Optional[Trade]
        delta = float(params["delta"])
        min_otm = float(params["min_otm_pct"])

        call = select_by_delta(calls, +delta)
        put = select_by_delta(puts, -delta)
        if call is None or put is None:
            return None

        if min_otm > 0:
            call = _apply_min_otm(calls, call, state.spot, min_otm, is_call=True)
            put = _apply_min_otm(puts, put, state.spot, min_otm, is_call=False)
            if call is None or put is None:
                return None

        if not self._bid_acceptable(call.bid) or not self._bid_acceptable(put.bid):
            return None

        entry_usd = call.bid_usd + put.bid_usd
        if entry_usd <= 0:
            return None

        sized = self._compute_quantity(state, entry_usd)
        if sized is None:
            return None
        quantity, sizing_meta = sized

        call_leg, fee_call = _sell_leg_dict(call, expiry, quantity, True, state.spot)
        put_leg, fee_put = _sell_leg_dict(put, expiry, quantity, False, state.spot)
        legs = [call_leg, put_leg]

        schedule_params = dict(params)
        schedule_params["schedule_id"] = schedule_id

        metadata = {
            "leg_type": "strangle",
            "target_delta": delta,
            "expiry": expiry,
            "expiry_dt": exp_dt,
            "direction": "sell",
            "call_strike": call.strike,
            "put_strike": put.strike,
            "call_delta": call.delta,
            "put_delta": put.delta,
            "quantity": quantity,
            "schedule_id": schedule_id,
            "schedule_params": schedule_params,
            "dte": int(params["dte"]),
            "stop_loss_pct": float(params["stop_loss_pct"]),
            "turbulence_threshold": float(params["turbulence_threshold"]),
            "min_otm_pct": min_otm,
            "proximity_stop_hours": float(params["proximity_stop_hours"]),
            "proximity_buffer_usd": float(params["proximity_buffer_usd"]),
            "premium_sl_except_final_hours": float(
                params["premium_sl_except_final_hours"]
            ),
        }
        return self._register_open(
            state,
            schedule_id=schedule_id,
            legs=legs,
            entry_usd=entry_usd * quantity,
            fees_open=(fee_call + fee_put) * quantity,
            metadata=metadata,
            sizing_meta=sizing_meta,
        )

    def _register_open(self, state, schedule_id, legs, entry_usd, fees_open,
                       metadata, sizing_meta):
        # type: (Any, str, List[Dict], float, float, Dict, Dict) -> Trade
        # Blueprint pattern: pos_id + Trade(side="open", metadata legs) → open fills.
        pos_id = self._next_pos_id()
        metadata = dict(metadata)
        metadata["pos_id"] = pos_id
        metadata["equity_at_entry_usd"] = self._equity_at_entry_usd(state)

        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=legs,
            entry_price_usd=entry_usd,
            fees_open=fees_open,
            metadata=metadata,
        )
        self._positions.append(pos)

        nyc_date = to_nyc(state.dt if state.dt.tzinfo else state.dt.replace(tzinfo=UTC))
        self._last_trade_nyc_date[schedule_id] = nyc_date.date().isoformat()

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
                "direction": "sell",
                "pos_id": pos_id,
                "schedule_id": schedule_id,
                "legs": legs,
                **sizing_meta,
            },
        )

    def _next_pos_id(self):
        # type: () -> int
        self._pos_counter += 1
        return self._pos_counter

    def _close(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        quantity = float(pos.metadata.get("quantity", 1.0))
        trade = self._close_strangle(state, pos, reason, quantity)
        # Open fills already emitted by the side="open" Trade.
        trade.metadata["skip_open_fill"] = True
        self._stamp_close_metadata(trade, pos, quantity)
        return trade

    def _close_strangle(self, state, pos, reason, quantity):
        # type: (Any, OpenPosition, str, float) -> Trade
        expiry = pos.metadata["expiry"]
        call_strike = pos.metadata["call_strike"]
        put_strike = pos.metadata["put_strike"]

        if reason == "expiry":
            call_exit_usd = max(0.0, state.spot - call_strike)
            put_exit_usd = max(0.0, put_strike - state.spot)
            call_exit_btc = (call_exit_usd / state.spot) if state.spot else 0.0
            put_exit_btc = (put_exit_usd / state.spot) if state.spot else 0.0
            fee_call = fee_put = 0.0
        else:
            min_tick_usd = _MIN_TICK_BTC * state.spot
            call_q = state.get_option(expiry, call_strike, True)
            put_q = state.get_option(expiry, put_strike, False)
            call_exit_usd = call_q.ask_usd if call_q and call_q.ask > 0 else min_tick_usd
            put_exit_usd = put_q.ask_usd if put_q and put_q.ask > 0 else min_tick_usd
            call_exit_btc = call_q.ask if call_q and call_q.ask > 0 else _MIN_TICK_BTC
            put_exit_btc = put_q.ask if put_q and put_q.ask > 0 else _MIN_TICK_BTC
            fee_call = deribit_fee_per_leg(state.spot, call_exit_usd)
            fee_put = deribit_fee_per_leg(state.spot, put_exit_usd)

        for leg in pos.legs:
            if leg["is_call"]:
                leg["exit_price_btc"] = call_exit_btc
                leg["exit_price_usd"] = call_exit_usd
                leg["fee_btc_close"] = (fee_call / state.spot) if state.spot else 0.0
            else:
                leg["exit_price_btc"] = put_exit_btc
                leg["exit_price_usd"] = put_exit_usd
                leg["fee_btc_close"] = (fee_put / state.spot) if state.spot else 0.0

        return close_position(
            state,
            pos,
            reason,
            (call_exit_usd + put_exit_usd) * quantity,
            (fee_call + fee_put) * quantity,
        )

    def _stamp_close_metadata(self, trade, pos, quantity):
        # type: (Trade, OpenPosition, float) -> None
        sched = pos.metadata.get("schedule_params") or {}
        trade.metadata.update({
            "leg_type": "strangle",
            "schedule_id": pos.metadata.get("schedule_id"),
            "dte": pos.metadata.get("dte", sched.get("dte")),
            "stop_loss_pct": pos.metadata.get("stop_loss_pct", sched.get("stop_loss_pct")),
            "turbulence_threshold": pos.metadata.get(
                "turbulence_threshold", sched.get("turbulence_threshold"),
            ),
            "quantity": quantity,
            "nav_premium_pct": self._nav_premium_pct,
            "max_qty_per_1btc_equity": self._max_qty_per_1btc_equity,
            "proximity_stop_hours": pos.metadata.get(
                "proximity_stop_hours", sched.get("proximity_stop_hours"),
            ),
            "proximity_buffer_usd": pos.metadata.get(
                "proximity_buffer_usd", sched.get("proximity_buffer_usd"),
            ),
            "premium_sl_except_final_hours": pos.metadata.get(
                "premium_sl_except_final_hours",
                sched.get("premium_sl_except_final_hours"),
            ),
            "equity_drawdown_stop_pct": self._equity_drawdown_stop_pct,
            "equity_sl_only_final_hours": self._equity_sl_only_final_hours,
            "equity_sl_except_final_hours": self._equity_sl_except_final_hours,
            "equity_at_entry_usd": pos.metadata.get("equity_at_entry_usd"),
            "release_name": RELEASE_NAME,
            "strategy_version": __version__,
        })

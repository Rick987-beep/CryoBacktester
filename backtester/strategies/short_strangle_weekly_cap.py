#!/usr/bin/env python3
"""
short_strangle_weekly_cap.py — Capacity-managed short strangle with weekly DTE targeting.

Extends short_strangle_weekly_tp with a capacity book:

    target_max_open  — maximum number of strangles allowed open simultaneously
    max_daily_new    — maximum new strangles to open on any single day

Each day, at most min(max_daily_new, target_max_open - current_open) new
strangles are opened.  Once any units are opened that day, no further
openings happen until the next calendar day.

This prevents lump-risk from opening many positions on one volatile day,
while letting the book refill gradually after TP/SL/expiry closures.

Exits per position (all evaluated independently each tick):
    take_profit  — combined ask <= entry_premium × (1 - take_profit_pct)
    stop_loss    — combined ask >= entry_premium × (1 + stop_loss_pct)
    max_hold     — position held >= max_hold_days calendar days (0 = disabled)
    expiry       — settlement at expiry hour (intrinsic value)
    end_of_data  — force-closed at market at last data tick

Counting:
    1 unit = 1 strangle = 1 contract per leg (call + put)
    current_open = len(self._positions)
"""
from typing import Any, Dict, List, Optional

from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import parse_expiry_date, expiry_dt_utc, select_expiry_for_week
from backtester.pricing import deribit_fee_per_leg, EXPIRY_HOUR_UTC
from backtester.strategy_base import (
    OpenPosition, Trade, close_trade,
    check_expiry,
    time_window, stop_loss_pct, max_hold_days,
)


# ------------------------------------------------------------------
# Strategy
# ------------------------------------------------------------------


class ShortStrangleWeeklyCap:
    """Capacity-managed daily short strangle with week-bucket DTE targeting.

    Capacity rules (evaluated once per day in entry window):
        slots_available = target_max_open - len(self._positions)
        n_to_open       = min(max_daily_new, slots_available)

    Once any units are opened on a given day, no further openings happen
    that day.  If the book is full, no openings happen (and the day is NOT
    marked as used — we try again tomorrow).
    """

    name = "short_strangle_weekly_cap"
    DATE_RANGE = ("2025-12-16", "2026-04-15")
    DESCRIPTION = (
        "Sells delta-selected OTM options targeting a week-bucket expiry. "
        "leg_mode controls whether a strangle (call+put), single put, or single call is sold. "
        "Capacity book: at most target_max_open positions open simultaneously; "
        "at most max_daily_new new positions per calendar day. "
        "Exits per-position on TP, SL, max-hold-days, expiry settlement, "
        "or end-of-data mark-to-market close."
    )

    PARAM_GRID = {
        "leg_mode":        ["put"],
        "target_weeks":    [1,2,3],
        "delta":           [0.05, 0.10, 0.15, 0.20],
        "entry_hour":      [9],
        "stop_loss_pct":   [3.0, 5.0, 7.0],
        "take_profit_pct": [0.33, 0.5, 0.75],
        "max_hold_days":   [0, 7, 14 ],
        "target_max_open": [10],
        "max_daily_new":   [1],
    }

    def __init__(self):
        self._positions = []          # type: List[OpenPosition]
        self._target_weeks = 2
        self._delta = 0.15
        self._sl_pct = 2.0
        self._tp_pct = 0.50
        self._entry_hour = 10
        self._max_hold_days = 0
        self._target_max_open = 5
        self._max_daily_new = 1
        self._leg_mode = "strangle"
        self._last_trade_date = None  # type: Optional[Any]
        self._entry_conditions = []
        self._exit_conditions = []

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._target_weeks = params.get("target_weeks", 2)
        self._delta = params["delta"]
        self._sl_pct = params["stop_loss_pct"]
        self._tp_pct = params["take_profit_pct"]
        self._entry_hour = params.get("entry_hour", 10)
        self._max_hold_days = params.get("max_hold_days", 0)
        self._target_max_open = params.get("target_max_open", 5)
        self._max_daily_new = params.get("max_daily_new", 1)
        self._leg_mode = params.get("leg_mode", "strangle")
        self._positions = []
        self._last_trade_date = None

        self._entry_conditions = [
            time_window(self._entry_hour, self._entry_hour + 1),
        ]
        self._exit_conditions = [
            stop_loss_pct(self._sl_pct),
        ]
        if self._max_hold_days > 0:
            self._exit_conditions.append(max_hold_days(self._max_hold_days))

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        # --- Exit pass ---
        to_close = []
        for pos in list(self._positions):
            reason = self._check_expiry(state, pos)
            if reason is None:
                reason = self._check_take_profit(state, pos)
            if reason is None:
                for exit_cond in self._exit_conditions:
                    reason = exit_cond(state, pos)
                    if reason:
                        break
            # Guard: skip tick if option data missing (data gap), except expiry settlement
            if reason and reason != "expiry":
                expiry = pos.metadata["expiry"]
                _lm = pos.metadata.get("leg_mode", "strangle")
                _call_ok = (_lm == "put") or (
                    state.get_option(expiry, pos.metadata["call_strike"], True) is not None)
                _put_ok  = (_lm == "call") or (
                    state.get_option(expiry, pos.metadata["put_strike"], False) is not None)
                if not (_call_ok and _put_ok):
                    reason = None
            if reason:
                trades.append(self._close(state, pos, reason))
                to_close.append(pos)
        for pos in to_close:
            self._positions.remove(pos)

        # --- Entry pass ---
        today = state.dt.date()
        if self._last_trade_date != today:
            if all(cond(state) for cond in self._entry_conditions):
                slots = self._target_max_open - len(self._positions)
                n_to_open = min(self._max_daily_new, slots)
                if n_to_open > 0:
                    for _ in range(n_to_open):
                        self._try_open(state)
                    # Mark today used only if we actually opened (try_open may skip
                    # if chain data unavailable, but we still mark to avoid retries
                    # within the same tick sequence)
                    self._last_trade_date = today

        return trades

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        """Force-close all remaining positions at market (last data tick)."""
        trades = []
        for pos in list(self._positions):
            trades.append(self._close(state, pos, "end_of_data"))
        self._positions.clear()
        return trades

    def reset(self):
        # type: () -> None
        self._positions = []
        self._last_trade_date = None

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "leg_mode":        self._leg_mode,
            "target_weeks":    self._target_weeks,
            "delta":           self._delta,
            "stop_loss_pct":   self._sl_pct,
            "take_profit_pct": self._tp_pct,
            "entry_hour":      self._entry_hour,
            "max_hold_days":   self._max_hold_days,
            "target_max_open": self._target_max_open,
            "max_daily_new":   self._max_daily_new,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _check_expiry(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[str]
        return check_expiry(state, pos)

    def _check_take_profit(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[str]
        """Close when ask drops to (1 - tp_pct) × entry premium.

        Single-leg mode prices only the relevant leg.
        Returns None if ask data is missing (skip tick).
        """
        if self._tp_pct <= 0:
            return None
        expiry   = pos.metadata["expiry"]
        leg_mode = pos.metadata.get("leg_mode", "strangle")
        if leg_mode == "strangle":
            call_q = state.get_option(expiry, pos.metadata["call_strike"], True)
            put_q  = state.get_option(expiry, pos.metadata["put_strike"], False)
            if call_q is None or put_q is None:
                return None
            if call_q.ask <= 0 or put_q.ask <= 0:
                return None
            current_usd = call_q.ask_usd + put_q.ask_usd
        elif leg_mode == "put":
            put_q = state.get_option(expiry, pos.metadata["put_strike"], False)
            if put_q is None or put_q.ask <= 0:
                return None
            current_usd = put_q.ask_usd
        else:  # call
            call_q = state.get_option(expiry, pos.metadata["call_strike"], True)
            if call_q is None or call_q.ask <= 0:
                return None
            current_usd = call_q.ask_usd
        profit_ratio = (pos.entry_price_usd - current_usd) / max(pos.entry_price_usd, 0.01)
        if profit_ratio >= self._tp_pct:
            return "take_profit"
        return None

    def _try_open(self, state):
        # type: (Any) -> None
        expiry = select_expiry_for_week(state, self._target_weeks)
        if expiry is None:
            return

        chain = state.get_chain(expiry)
        if not chain:
            return

        exp_dt   = expiry_dt_utc(expiry, state.dt.tzinfo)
        today    = state.dt.date()
        exp_date = parse_expiry_date(expiry)
        dte      = (exp_date.date() - today).days if exp_date else None

        if self._leg_mode == "strangle":
            calls = [q for q in chain if q.is_call]
            puts  = [q for q in chain if not q.is_call]
            call  = select_by_delta(calls, +self._delta)
            put   = select_by_delta(puts,  -self._delta)
            if call is None or put is None:
                return
            if call.bid <= 0 or put.bid <= 0:
                return
            call_entry_usd = call.bid_usd
            put_entry_usd  = put.bid_usd
            entry_usd = call_entry_usd + put_entry_usd
            if entry_usd <= 0:
                return
            fee  = deribit_fee_per_leg(state.spot, call_entry_usd) + deribit_fee_per_leg(state.spot, put_entry_usd)
            legs = [
                {"strike": call.strike, "is_call": True,  "expiry": expiry, "side": "sell",
                 "entry_price": call.bid, "entry_price_usd": call_entry_usd, "entry_delta": call.delta},
                {"strike": put.strike,  "is_call": False, "expiry": expiry, "side": "sell",
                 "entry_price": put.bid,  "entry_price_usd": put_entry_usd,  "entry_delta": put.delta},
            ]
            meta = {
                "target_delta": self._delta, "expiry": expiry, "expiry_dt": exp_dt,
                "direction": "sell", "leg_mode": "strangle",
                "call_strike": call.strike, "put_strike": put.strike,
                "call_delta": call.delta, "put_delta": put.delta, "dte_at_entry": dte,
            }
        elif self._leg_mode == "put":
            puts = [q for q in chain if not q.is_call]
            put  = select_by_delta(puts, -self._delta)
            if put is None or put.bid <= 0:
                return
            entry_usd = put.bid_usd
            if entry_usd <= 0:
                return
            fee  = deribit_fee_per_leg(state.spot, entry_usd)
            legs = [
                {"strike": put.strike, "is_call": False, "expiry": expiry, "side": "sell",
                 "entry_price": put.bid, "entry_price_usd": entry_usd, "entry_delta": put.delta},
            ]
            meta = {
                "target_delta": self._delta, "expiry": expiry, "expiry_dt": exp_dt,
                "direction": "sell", "leg_mode": "put",
                "put_strike": put.strike, "put_delta": put.delta, "dte_at_entry": dte,
            }
        else:  # call
            calls = [q for q in chain if q.is_call]
            call  = select_by_delta(calls, +self._delta)
            if call is None or call.bid <= 0:
                return
            entry_usd = call.bid_usd
            if entry_usd <= 0:
                return
            fee  = deribit_fee_per_leg(state.spot, entry_usd)
            legs = [
                {"strike": call.strike, "is_call": True, "expiry": expiry, "side": "sell",
                 "entry_price": call.bid, "entry_price_usd": entry_usd, "entry_delta": call.delta},
            ]
            meta = {
                "target_delta": self._delta, "expiry": expiry, "expiry_dt": exp_dt,
                "direction": "sell", "leg_mode": "call",
                "call_strike": call.strike, "call_delta": call.delta, "dte_at_entry": dte,
            }

        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=legs,
            entry_price_usd=entry_usd,
            fees_open=fee,
            metadata=meta,
        )
        self._positions.append(pos)

    def _close(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        expiry   = pos.metadata["expiry"]
        leg_mode = pos.metadata.get("leg_mode", "strangle")

        if leg_mode == "strangle":
            call_strike = pos.metadata["call_strike"]
            put_strike  = pos.metadata["put_strike"]
            if reason == "expiry":
                call_exit_usd = max(0.0, state.spot - call_strike)
                put_exit_usd  = max(0.0, put_strike - state.spot)
            else:
                call_q = state.get_option(expiry, call_strike, True)
                put_q  = state.get_option(expiry, put_strike, False)
                call_exit_usd = (call_q.ask_usd if call_q and call_q.ask > 0
                                 else pos.legs[0]["entry_price_usd"])
                put_exit_usd  = (put_q.ask_usd if put_q and put_q.ask > 0
                                 else pos.legs[1]["entry_price_usd"])
            exit_usd   = call_exit_usd + put_exit_usd
            fees_close = 0.0 if reason == "expiry" else (
                deribit_fee_per_leg(state.spot, call_exit_usd) +
                deribit_fee_per_leg(state.spot, put_exit_usd)
            )
        elif leg_mode == "put":
            put_strike = pos.metadata["put_strike"]
            if reason == "expiry":
                exit_usd = max(0.0, put_strike - state.spot)
            else:
                put_q    = state.get_option(expiry, put_strike, False)
                exit_usd = (put_q.ask_usd if put_q and put_q.ask > 0
                            else pos.legs[0]["entry_price_usd"])
            fees_close = 0.0 if reason == "expiry" else deribit_fee_per_leg(state.spot, exit_usd)
        else:  # call
            call_strike = pos.metadata["call_strike"]
            if reason == "expiry":
                exit_usd = max(0.0, state.spot - call_strike)
            else:
                call_q   = state.get_option(expiry, call_strike, True)
                exit_usd = (call_q.ask_usd if call_q and call_q.ask > 0
                            else pos.legs[0]["entry_price_usd"])
            fees_close = 0.0 if reason == "expiry" else deribit_fee_per_leg(state.spot, exit_usd)

        trade = close_trade(state, pos, reason, exit_usd, fees_close)
        trade.metadata["target_weeks"]    = self._target_weeks
        trade.metadata["stop_loss_pct"]   = self._sl_pct
        trade.metadata["take_profit_pct"] = self._tp_pct
        trade.metadata["max_hold_days"]   = self._max_hold_days
        trade.metadata["target_max_open"] = self._target_max_open
        trade.metadata["max_daily_new"]   = self._max_daily_new
        trade.metadata["dte_at_entry"]    = pos.metadata.get("dte_at_entry")
        return trade

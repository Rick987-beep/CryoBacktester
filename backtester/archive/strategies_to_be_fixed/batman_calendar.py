#!/usr/bin/env python3
"""
batman_calendar.py — Batman/mouse calendar spread: short inner strangle + long outer strangle.

Replaces traditional SL/TP with a wider, longer-dated long strangle that caps
downside risk structurally.

Structure (4 legs):
    Inner (short): N-DTE strangle, delta-selected (sell at bid)
    Outer (long):  (N + outer_dte_offset)-DTE strangle, strikes widened by
                   strike_offset USD from the inner legs (buy at ask)

    The result is a calendar spread that looks like a "batman" or "mouse" shape
    in the payoff diagram — short body with protective wings further OTM and
    further in time.

Entry:
    Time-window based (entry_hour), one entry per day, up to inner_dte+1
    concurrent positions.

Exit (all 4 legs close together):
    1. Inner expiry — inner legs settle at intrinsic (no close fee);
       outer legs sold at bid (close fee applies).
    2. Max hold hours — all legs closed at market before inner expiry.
    3. End of data — force-close.

PnL accounting:
    net_entry = inner_premium_received − outer_premium_paid − entry_fees
    net_exit  = inner_buyback_cost − outer_sellback_revenue + exit_fees
    pnl       = net_entry − net_exit

Grid parameters:
    inner_dte         — DTE for the short inner strangle (1, 2, 3)
    outer_dte_offset  — extra DTE for the long outer strangle (1, 2, 3, 4)
    delta             — target delta for inner leg selection
    strike_offset     — USD distance outer strikes are wider than inner (500, 1000, 2000)
    ratio             — outer contracts per inner contract (1, 2)
    entry_hour        — UTC hour for entry window
    max_hold_hours    — max hours to hold; 0 = hold to inner expiry

Pricing:
    Inner: sell at bid, buy back at ask (or intrinsic at expiry).
    Outer: buy at ask, sell back at bid.

Fees:
    Deribit model: MIN(0.03% × index, 12.5% × option_price) per leg,
    charged on both open and close (except inner legs at expiry).
"""
from typing import Any, Dict, List, Optional

from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import parse_expiry_date, expiry_dt_utc, select_expiry
from backtester.pricing import deribit_fee_per_leg, EXPIRY_HOUR_UTC
from backtester.strategy_base import (
    OpenPosition, Trade, close_trade,
    time_window, max_hold_hours,
)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _find_strike(chain, target_strike, is_call):
    # type: (list, float, bool) -> Optional[Any]
    """Find the option closest to target_strike on the given chain side."""
    side = [q for q in chain if q.is_call == is_call]
    if not side:
        return None
    return min(side, key=lambda q: abs(q.strike - target_strike))


# ------------------------------------------------------------------
# Strategy
# ------------------------------------------------------------------

class BatmanCalendar:
    """Short inner strangle + long outer strangle (batman/mouse calendar spread)."""

    name = "batman_calendar"
    DATE_RANGE = ("2026-02-01", "2026-04-15")
    DESCRIPTION = (
        "Sells a delta-selected inner strangle and buys a wider, longer-dated "
        "outer strangle to cap risk. All 4 legs close together at inner expiry "
        "or max_hold_hours."
    )

    PARAM_GRID = {
        "inner_dte":        [1, 2],
        "outer_dte_offset": [1, 2, 3, 4],
        "delta":            [0.10, 0.20, 0.30],
        "strike_offset":    [500, 1000],
        "ratio":            [1, 0.75, 0.5],
        "entry_hour":       [3,9,12],
        "max_hold_hours":   [0],
    }

    def __init__(self):
        self._positions = []          # type: List[OpenPosition]
        self._inner_dte = 1
        self._outer_dte_offset = 1
        self._max_concurrent = 2
        self._delta = 0.20
        self._strike_offset = 1000
        self._ratio = 1
        self._entry_hour = 18
        self._max_hold_hours = 0
        self._last_trade_date = None  # type: Optional[Any]
        self._entry_conditions = []
        self._exit_conditions = []

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._inner_dte = params.get("inner_dte", 1)
        self._outer_dte_offset = params.get("outer_dte_offset", 1)
        self._delta = params["delta"]
        self._strike_offset = params["strike_offset"]
        # Round to Deribit's 0.1 contract minimum
        self._ratio = round(params.get("ratio", 1) * 10) / 10
        self._entry_hour = params.get("entry_hour", 18)
        self._max_hold_hours = params.get("max_hold_hours", 0)
        self._max_concurrent = self._inner_dte + 1
        self._positions = []
        self._last_trade_date = None

        self._entry_conditions = [
            time_window(self._entry_hour, self._entry_hour + 1),
        ]
        self._exit_conditions = []
        if self._max_hold_hours > 0:
            self._exit_conditions.append(max_hold_hours(self._max_hold_hours))

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        # -- Check exits on all open positions --
        to_close = []
        for pos in list(self._positions):
            reason = self._check_inner_expiry(state, pos)
            if reason is None:
                for exit_cond in self._exit_conditions:
                    reason = exit_cond(state, pos)
                    if reason:
                        break
            # Verify data availability before closing (skip day-boundary gaps)
            if reason and reason != "expiry":
                if not self._data_available(state, pos):
                    reason = None
            if reason:
                trades.append(self._close(state, pos, reason))
                to_close.append(pos)
        for pos in to_close:
            self._positions.remove(pos)

        # -- Check entry --
        if len(self._positions) < self._max_concurrent:
            today = state.dt.date()
            if self._last_trade_date != today:
                if all(cond(state) for cond in self._entry_conditions):
                    self._try_open(state)

        return trades

    def on_end(self, state):
        # type: (Any) -> List[Trade]
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
            "inner_dte":        self._inner_dte,
            "outer_dte_offset": self._outer_dte_offset,
            "delta":            self._delta,
            "strike_offset":    self._strike_offset,
            "ratio":            self._ratio,
            "entry_hour":       self._entry_hour,
            "max_hold_hours":   self._max_hold_hours,
        }

    # ------------------------------------------------------------------
    # Internal — entry
    # ------------------------------------------------------------------

    def _try_open(self, state):
        # type: (Any) -> None
        inner_expiry = select_expiry(state, self._inner_dte)
        if inner_expiry is None:
            return
        # Outer expiry: walk forward from minimum DTE until we find one that
        # exists in the snapshot.  Deribit only has dailies for the nearest
        # few days, then weeklies/monthlies — so the exact offset may not
        # have an expiry.  Cap the search at 30 days to avoid infinite loops.
        min_outer_dte = self._inner_dte + self._outer_dte_offset
        outer_expiry = None
        for dte in range(min_outer_dte, min_outer_dte + 30):
            outer_expiry = select_expiry(state, dte)
            if outer_expiry is not None:
                break
        if outer_expiry is None:
            return

        # Select inner legs by delta
        inner_chain = state.get_chain(inner_expiry)
        if not inner_chain:
            return
        inner_calls = [q for q in inner_chain if q.is_call]
        inner_puts = [q for q in inner_chain if not q.is_call]

        inner_call = select_by_delta(inner_calls, +self._delta)
        inner_put = select_by_delta(inner_puts, -self._delta)
        if inner_call is None or inner_put is None:
            return
        if inner_call.bid <= 0 or inner_put.bid <= 0:
            return

        # Select outer legs: same side, wider by strike_offset
        outer_chain = state.get_chain(outer_expiry)
        if not outer_chain:
            return
        outer_call_target = inner_call.strike + self._strike_offset
        outer_put_target = inner_put.strike - self._strike_offset
        outer_call = _find_strike(outer_chain, outer_call_target, is_call=True)
        outer_put = _find_strike(outer_chain, outer_put_target, is_call=False)
        if outer_call is None or outer_put is None:
            return
        if outer_call.ask <= 0 or outer_put.ask <= 0:
            return

        # Compute entry prices
        inner_call_usd = inner_call.bid_usd
        inner_put_usd = inner_put.bid_usd
        inner_premium = inner_call_usd + inner_put_usd

        outer_call_usd = outer_call.ask_usd * self._ratio
        outer_put_usd = outer_put.ask_usd * self._ratio
        outer_cost = outer_call_usd + outer_put_usd

        net_entry_usd = inner_premium - outer_cost
        # Allow net-debit entries (outer costs more than inner collects)
        # — the structure still provides meaningful spread P&L.

        # Fees: inner (sell) + outer (buy)
        fee_inner = (deribit_fee_per_leg(state.spot, inner_call_usd)
                     + deribit_fee_per_leg(state.spot, inner_put_usd))
        fee_outer = (deribit_fee_per_leg(state.spot, outer_call.ask_usd)
                     + deribit_fee_per_leg(state.spot, outer_put.ask_usd)) * self._ratio

        inner_exp_dt = expiry_dt_utc(inner_expiry, state.dt.tzinfo)
        outer_exp_dt = expiry_dt_utc(outer_expiry, state.dt.tzinfo)

        legs = [
            {
                "strike": inner_call.strike, "is_call": True,
                "expiry": inner_expiry, "side": "sell",
                "entry_price": inner_call.bid, "entry_price_usd": inner_call_usd,
                "entry_delta": inner_call.delta, "layer": "inner",
            },
            {
                "strike": inner_put.strike, "is_call": False,
                "expiry": inner_expiry, "side": "sell",
                "entry_price": inner_put.bid, "entry_price_usd": inner_put_usd,
                "entry_delta": inner_put.delta, "layer": "inner",
            },
            {
                "strike": outer_call.strike, "is_call": True,
                "expiry": outer_expiry, "side": "buy",
                "entry_price": outer_call.ask, "entry_price_usd": outer_call.ask_usd,
                "entry_delta": outer_call.delta, "layer": "outer", "qty": self._ratio,
            },
            {
                "strike": outer_put.strike, "is_call": False,
                "expiry": outer_expiry, "side": "buy",
                "entry_price": outer_put.ask, "entry_price_usd": outer_put.ask_usd,
                "entry_delta": outer_put.delta, "layer": "outer", "qty": self._ratio,
            },
        ]

        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=legs,
            entry_price_usd=net_entry_usd,
            fees_open=fee_inner + fee_outer,
            metadata={
                "direction":        "sell",  # net short premium structure
                "inner_expiry":     inner_expiry,
                "outer_expiry":     outer_expiry,
                "inner_expiry_dt":  inner_exp_dt,
                "outer_expiry_dt":  outer_exp_dt,
                "inner_call_strike": inner_call.strike,
                "inner_put_strike":  inner_put.strike,
                "outer_call_strike": outer_call.strike,
                "outer_put_strike":  outer_put.strike,
                "inner_premium_usd": inner_premium,
                "outer_cost_usd":    outer_cost,
                "target_delta":      self._delta,
            },
        )
        self._positions.append(pos)
        self._last_trade_date = state.dt.date()

    # ------------------------------------------------------------------
    # Internal — exit
    # ------------------------------------------------------------------

    def _check_inner_expiry(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[str]
        exp_dt = pos.metadata.get("inner_expiry_dt")
        if exp_dt is None:
            return None
        if state.dt >= exp_dt:
            return "expiry"
        return None

    def _data_available(self, state, pos):
        # type: (Any, OpenPosition) -> bool
        """Check that market data exists for all 4 legs at this tick."""
        md = pos.metadata
        if (state.get_option(md["inner_expiry"], md["inner_call_strike"], True) is None
                or state.get_option(md["inner_expiry"], md["inner_put_strike"], False) is None):
            return False
        if (state.get_option(md["outer_expiry"], md["outer_call_strike"], True) is None
                or state.get_option(md["outer_expiry"], md["outer_put_strike"], False) is None):
            return False
        return True

    def _close(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        md = pos.metadata
        inner_expiry = md["inner_expiry"]
        outer_expiry = md["outer_expiry"]
        _min_tick_usd = 0.0001 * state.spot

        # -- Inner legs: short → must buy back --
        if reason == "expiry":
            # Inner settles at intrinsic
            inner_call_exit = max(0.0, state.spot - md["inner_call_strike"])
            inner_put_exit = max(0.0, md["inner_put_strike"] - state.spot)
        else:
            inner_call_q = state.get_option(inner_expiry, md["inner_call_strike"], True)
            inner_put_q = state.get_option(inner_expiry, md["inner_put_strike"], False)
            inner_call_exit = (inner_call_q.ask_usd if inner_call_q and inner_call_q.ask > 0
                               else _min_tick_usd)
            inner_put_exit = (inner_put_q.ask_usd if inner_put_q and inner_put_q.ask > 0
                              else _min_tick_usd)

        # -- Outer legs: long → sell to close at bid --
        outer_call_q = state.get_option(outer_expiry, md["outer_call_strike"], True)
        outer_put_q = state.get_option(outer_expiry, md["outer_put_strike"], False)

        if reason == "expiry" and (outer_call_q is None or outer_put_q is None):
            # Outer data may be missing at the inner expiry tick.
            # Fall back to intrinsic value of the outer legs (conservative).
            outer_call_exit = max(0.0, state.spot - md["outer_call_strike"])
            outer_put_exit = max(0.0, md["outer_put_strike"] - state.spot)
        else:
            outer_call_exit = (outer_call_q.bid_usd if outer_call_q and outer_call_q.bid > 0
                               else _min_tick_usd)
            outer_put_exit = (outer_put_q.bid_usd if outer_put_q and outer_put_q.bid > 0
                              else _min_tick_usd)

        outer_call_exit *= self._ratio
        outer_put_exit *= self._ratio

        # Net exit cost: what we pay to close inner − what we receive closing outer
        inner_buyback = inner_call_exit + inner_put_exit
        outer_sellback = outer_call_exit + outer_put_exit

        # For close_trade: exit_price_usd = net cost to unwind
        # For a sell-direction position: pnl = entry_price_usd - exit_price_usd - fees
        # So exit_price_usd should be the net cost of unwinding
        exit_usd = inner_buyback - outer_sellback

        # Fees: inner close + outer close
        if reason == "expiry":
            # No fees on inner (settled), fees on outer sell
            fees_inner_close = 0.0
        else:
            fees_inner_close = (
                deribit_fee_per_leg(state.spot, inner_call_exit)
                + deribit_fee_per_leg(state.spot, inner_put_exit)
            )
        # Outer close fees (per-leg, scaled by ratio)
        outer_call_per = outer_call_exit / self._ratio if self._ratio > 0 else 0
        outer_put_per = outer_put_exit / self._ratio if self._ratio > 0 else 0
        fees_outer_close = (
            deribit_fee_per_leg(state.spot, outer_call_per)
            + deribit_fee_per_leg(state.spot, outer_put_per)
        ) * self._ratio

        fees_close = fees_inner_close + fees_outer_close

        trade = close_trade(state, pos, reason, exit_usd, fees_close)
        trade.metadata["inner_dte"] = self._inner_dte
        trade.metadata["outer_dte_offset"] = self._outer_dte_offset
        trade.metadata["strike_offset"] = self._strike_offset
        trade.metadata["ratio"] = self._ratio
        trade.metadata["max_hold_hours"] = self._max_hold_hours
        trade.metadata["inner_buyback_usd"] = inner_buyback
        trade.metadata["outer_sellback_usd"] = outer_sellback
        return trade

#!/usr/bin/env python3
"""
cal_spread_weekend.py — Weekend calendar spread: short near-DTE, long far-DTE.

Structure
---------
A single calendar spread (two legs at the same strike, same option type):
    legs[0] — short (near-DTE, side="sell")
    legs[1] — long  (far-DTE,  side="buy")

Option type is determined by strike vs spot:
    nearest available strike < spot  → put calendar
    nearest available strike > spot  → call calendar

Up to MAX_POSITIONS spreads can be open concurrently.
Each position is tracked and exited independently.

Entry
-----
Fires on the configured entry_day (default: Saturday=5) at entry_hour:00 UTC.
One new position is opened per qualifying tick, capped at MAX_POSITIONS.

Exit — checked per position every tick
--------------------------------------
1. Short-leg expiry     → close both legs (short at 0, long at bid/mark)
2. Take profit          → net_pnl >= tp_pct × short_cost
3. Stop loss            → net_pnl <= -(sl_pct × short_cost)

net_pnl = (short_cost − short_buyback_usd) + (long_proceeds_usd − long_cost)

short_cost   = USD received for short leg at entry (bid × spot × qty)
long_cost    = USD paid for long leg at entry (ask × spot × qty)
short_buyback_usd = max(short_q.ask, short_q.mark) × spot × qty
long_proceeds_usd = (long_q.bid or long_q.mark fallback) × spot × qty

Fees
----
Deribit model applied at each open and close event.
"""
import logging
from typing import Any, Dict, List, Optional

from backtester.expiry_utils import (
    parse_expiry_date,
    expiry_dt_utc,
)
from backtester.pricing import deribit_fee_per_leg, EXPIRY_HOUR_UTC
from backtester.strategy_base import (
    OpenPosition,
    Trade,
    check_expiry,
    close_position,
)

log = logging.getLogger(__name__)

# Maximum concurrent open positions
MAX_POSITIONS = 4


# ---------------------------------------------------------------------------
# Expiry selection helper
# ---------------------------------------------------------------------------

def _nearest_expiry(state, target_dte):
    # type: (Any, int) -> Optional[str]
    """Return the expiry whose DTE is closest to target_dte.

    Only considers expiries with DTE > 0 (not already expired relative to
    state.dt.date()).  Returns None if no valid expiry exists.
    """
    today = state.dt.date()
    best_exp = None
    best_diff = None
    for exp in state.expiries():
        exp_date = parse_expiry_date(exp)
        if exp_date is None:
            continue
        dte = (exp_date.date() - today).days
        if dte <= 0:
            continue
        diff = abs(dte - target_dte)
        if best_diff is None or diff < best_diff:
            best_exp = exp
            best_diff = diff
    return best_exp


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

class CalSpreadWeekend:
    """Weekend calendar spread — short near DTE, long far DTE.

    Each position = one put or call calendar at the nearest ATM strike.
    Up to MAX_POSITIONS (4) positions can be open simultaneously.
    Each is managed independently for TP, SL, and expiry.
    """

    name = "cal_spread_weekend"
    DATE_RANGE = ("2026-03-15", "2026-05-15")
    DESCRIPTION = (
        "Opens a calendar spread on the configured entry_day at entry_hour UTC. "
        "Short near-DTE leg + long far-DTE leg at the nearest ATM strike. "
        "Strike < spot → put calendar; strike > spot → call calendar. "
        "Exits on TP, SL (both relative to short_cost), or short-leg expiry."
    )

    PARAM_GRID = {
        "near_dte":   [7],
        "far_dte":    [45],
        "entry_hour": [12],
        "entry_day":  [5],        # 5=Saturday, 6=Sunday
        "tp_pct":     [0.25, 0.50, 0.75],
        "sl_pct":     [0.75, 1.00],
        "qty":        [1.0],
    }

    def __init__(self):
        self._positions = []  # type: List[OpenPosition]
        self._near_dte = 14
        self._far_dte = 45
        self._entry_hour = 12
        self._entry_day = 5
        self._tp_pct = 0.50
        self._sl_pct = 1.00
        self._qty = 1.0
        self._pos_counter = 0

    # ------------------------------------------------------------------
    # Protocol
    # ------------------------------------------------------------------

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._near_dte = params["near_dte"]
        self._far_dte = params["far_dte"]
        self._entry_hour = params.get("entry_hour", 12)
        self._entry_day = params.get("entry_day", 5)
        self._tp_pct = params["tp_pct"]
        self._sl_pct = params["sl_pct"]
        self._qty = params.get("qty", 1.0)
        self._positions = []
        self._pos_counter = 0

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        # --- Check exits for each open position ---
        closed = []
        for pos in self._positions:
            reason = self._check_exit(state, pos)
            if reason:
                trade = self._close(state, pos, reason)
                trades.append(trade)
                closed.append(pos)

        for pos in closed:
            self._positions.remove(pos)

        # --- Entry ---
        if (
            state.dt.weekday() == self._entry_day
            and state.dt.hour == self._entry_hour
            and state.dt.minute == 0
            and len(self._positions) < MAX_POSITIONS
        ):
            open_trade = self._try_open(state)
            if open_trade is not None:
                trades.append(open_trade)

        return trades

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        trades = [self._close(state, pos, "end_of_data") for pos in self._positions]
        self._positions = []
        return trades

    def reset(self):
        # type: () -> None
        self._positions = []
        self._pos_counter = 0

    def _next_pos_id(self):
        # type: () -> int
        self._pos_counter += 1
        return self._pos_counter

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "near_dte":   self._near_dte,
            "far_dte":    self._far_dte,
            "entry_hour": self._entry_hour,
            "entry_day":  self._entry_day,
            "tp_pct":     self._tp_pct,
            "sl_pct":     self._sl_pct,
            "qty":        self._qty,
        }

    # ------------------------------------------------------------------
    # Entry
    # ------------------------------------------------------------------

    def _try_open(self, state):
        # type: (Any) -> Optional[Trade]
        """Attempt to open a new calendar spread. Returns open Trade or None."""
        near_exp = _nearest_expiry(state, self._near_dte)
        if near_exp is None:
            log.debug("%s: no near expiry found (target_dte=%d)", state.dt, self._near_dte)
            return None

        far_exp = _nearest_expiry(state, self._far_dte)
        if far_exp is None or far_exp == near_exp:
            log.debug("%s: no distinct far expiry found (target_dte=%d)", state.dt, self._far_dte)
            return None

        # Strike selection: nearest available strike to spot
        atm_strike = state.get_atm_strike(near_exp)
        if atm_strike is None:
            return None
        is_call = atm_strike > state.spot  # strike < spot → False (put); strike > spot → True (call)

        # Quotes
        short_q = state.get_option(near_exp, atm_strike, is_call)
        long_q  = state.get_option(far_exp,  atm_strike, is_call)

        if short_q is None or long_q is None:
            return None
        if short_q.bid <= 0 or long_q.ask <= 0:
            return None

        qty = self._qty

        # Fill prices: sell short at bid, buy long at ask
        short_price_btc = short_q.bid
        long_price_btc  = long_q.ask

        short_price_usd = short_price_btc * state.spot
        long_price_usd  = long_price_btc  * state.spot

        short_cost = short_price_usd * qty   # USD received for short
        long_cost  = long_price_usd  * qty   # USD paid for long

        # Fees
        fee_short_open = deribit_fee_per_leg(state.spot, short_price_usd)
        fee_long_open  = deribit_fee_per_leg(state.spot, long_price_usd)
        total_fees_open = (fee_short_open + fee_long_open) * qty

        # Expiry datetime of near (short) leg — used by check_expiry
        near_exp_dt = expiry_dt_utc(near_exp, state.dt.tzinfo)

        # Legs
        legs = [
            {
                "strike":          atm_strike,
                "is_call":         is_call,
                "expiry":          near_exp,
                "side":            "sell",
                "qty":             qty,
                "price_btc":       short_price_btc,
                "entry_price":     short_price_btc,
                "entry_price_usd": short_price_usd,
                "fee_usd_open":    fee_short_open * qty,
                "entry_delta":     short_q.delta,
                "entry_spot":      state.spot,
            },
            {
                "strike":          atm_strike,
                "is_call":         is_call,
                "expiry":          far_exp,
                "side":            "buy",
                "qty":             qty,
                "price_btc":       long_price_btc,
                "entry_price":     long_price_btc,
                "entry_price_usd": long_price_usd,
                "fee_usd_open":    fee_long_open * qty,
                "entry_delta":     long_q.delta,
                "entry_spot":      state.spot,
            },
        ]

        pos_id = self._next_pos_id()

        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=legs,
            entry_price_usd=long_cost - short_cost,   # net debit (+ = paid net)
            fees_open=total_fees_open,
            metadata={
                "pos_id":      pos_id,
                "near_expiry": near_exp,
                "far_expiry":  far_exp,
                "expiry_dt":   near_exp_dt,
                "strike":      atm_strike,
                "is_call":     is_call,
                "short_cost":  short_cost,
                "long_cost":   long_cost,
                "direction":   "spread",
            },
        )
        self._positions.append(pos)

        open_trade = Trade(
            entry_time=state.dt,
            exit_time=state.dt,
            entry_spot=state.spot,
            exit_spot=state.spot,
            entry_price_usd=pos.entry_price_usd,
            exit_price_usd=0.0,
            fees=total_fees_open,
            pnl=0.0,
            triggered=False,
            exit_reason="open",
            exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={
                "legs":   legs,
                "pos_id": pos_id,
                "near_expiry": near_exp,
                "far_expiry":  far_exp,
                "strike":      atm_strike,
                "is_call":     is_call,
                "near_dte":    self._near_dte,
                "far_dte":     self._far_dte,
            },
        )

        log.info(
            "%s: opened %s calendar at strike=%.0f near=%s far=%s "
            "short_cost=%.2f long_cost=%.2f",
            state.dt, "call" if is_call else "put",
            atm_strike, near_exp, far_exp, short_cost, long_cost,
        )
        return open_trade

    # ------------------------------------------------------------------
    # Exit check
    # ------------------------------------------------------------------

    def _check_exit(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[str]
        """Return exit reason string or None to hold."""
        # 1. Short-leg expiry
        reason = check_expiry(state, pos)
        if reason:
            return reason

        # 2. TP / SL — compute net P&L of the full 2-leg structure
        net_pnl = self._net_pnl(state, pos)
        if net_pnl is None:
            return None  # quotes unavailable — skip tick

        short_cost = pos.metadata["short_cost"]

        if net_pnl >= self._tp_pct * short_cost:
            return "take_profit"
        if net_pnl <= -(self._sl_pct * short_cost):
            return "stop_loss"

        return None

    def _net_pnl(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[float]
        """Compute net unrealized P&L for the 2-leg spread.

        net_pnl = (short_cost − short_buyback_usd) + (long_proceeds_usd − long_cost)

        short_buyback: max(ask, mark) × spot × qty   (floor at mark avoids phantom SL)
        long_proceeds: bid × spot × qty               (mark fallback if bid == 0)

        Returns None if quotes are unavailable (tick is skipped).
        """
        short_leg = pos.legs[0]  # side="sell"
        long_leg  = pos.legs[1]  # side="buy"
        qty = float(short_leg.get("qty", 1.0))

        short_q = state.get_option(short_leg["expiry"], short_leg["strike"], short_leg["is_call"])
        long_q  = state.get_option(long_leg["expiry"],  long_leg["strike"],  long_leg["is_call"])

        if short_q is None or long_q is None:
            return None

        # Short leg: cost to buy back — floor at mark to avoid wide-spread false SL
        if short_q.ask > 0:
            short_buyback_btc = max(short_q.ask, short_q.mark)
        elif short_q.mark > 0:
            short_buyback_btc = short_q.mark
        else:
            short_buyback_btc = 0.0
        short_buyback_usd = short_buyback_btc * state.spot * qty

        # Long leg: proceeds from selling — use bid; fall back to mark if zero
        if long_q.bid > 0:
            long_proceeds_btc = long_q.bid
        elif long_q.mark > 0:
            long_proceeds_btc = long_q.mark
        else:
            return None  # cannot value long leg — skip tick

        long_proceeds_usd = long_proceeds_btc * state.spot * qty

        return (
            (pos.metadata["short_cost"] - short_buyback_usd)
            + (long_proceeds_usd - pos.metadata["long_cost"])
        )

    # ------------------------------------------------------------------
    # Close
    # ------------------------------------------------------------------

    def _close(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        """Annotate legs with exit prices and call close_position."""
        short_leg = pos.legs[0]
        long_leg  = pos.legs[1]
        qty = float(short_leg.get("qty", 1.0))

        at_expiry = (reason == "expiry")

        # --- Short (near) leg ---
        if at_expiry:
            # Expired worthless
            short_exit_btc = 0.0
            short_exit_usd = 0.0
            fee_short_close = 0.0
        else:
            short_q = state.get_option(
                short_leg["expiry"], short_leg["strike"], short_leg["is_call"]
            )
            if short_q is not None and short_q.ask > 0:
                short_exit_btc = short_q.ask
            elif short_q is not None and short_q.mark > 0:
                short_exit_btc = short_q.mark
            else:
                short_exit_btc = 0.0
            short_exit_usd = short_exit_btc * state.spot
            fee_short_close = deribit_fee_per_leg(state.spot, short_exit_usd) * qty

        short_leg["exit_price_btc"] = short_exit_btc
        short_leg["exit_price_usd"] = short_exit_btc * state.spot
        short_leg["fee_btc_close"]  = (fee_short_close / state.spot) if state.spot else 0.0

        # --- Long (far) leg ---
        long_q = state.get_option(
            long_leg["expiry"], long_leg["strike"], long_leg["is_call"]
        )
        if long_q is not None and long_q.bid > 0:
            long_exit_btc = long_q.bid
        elif long_q is not None and long_q.mark > 0:
            long_exit_btc = long_q.mark
        else:
            long_exit_btc = 0.0
        long_exit_usd = long_exit_btc * state.spot
        fee_long_close = deribit_fee_per_leg(state.spot, long_exit_usd) * qty

        long_leg["exit_price_btc"] = long_exit_btc
        long_leg["exit_price_usd"] = long_exit_btc * state.spot
        long_leg["fee_btc_close"]  = (fee_long_close / state.spot) if state.spot else 0.0

        total_fees_close = fee_short_close + fee_long_close
        current_usd = (short_exit_btc + long_exit_btc) * state.spot * qty

        trade = close_position(state, pos, reason, current_usd=current_usd,
                               fees_close=total_fees_close)
        trade.side = "close"
        trade.metadata["skip_open_fill"] = True
        trade.metadata.update({
            "near_expiry": pos.metadata["near_expiry"],
            "far_expiry":  pos.metadata["far_expiry"],
            "strike":      pos.metadata["strike"],
            "is_call":     pos.metadata["is_call"],
            "short_cost":  pos.metadata["short_cost"],
            "long_cost":   pos.metadata["long_cost"],
            "near_dte":    self._near_dte,
            "far_dte":     self._far_dte,
            "tp_pct":      self._tp_pct,
            "sl_pct":      self._sl_pct,
        })

        log.info(
            "%s: closed %s calendar (pos_id=%s) reason=%s pnl=%.2f",
            state.dt, "call" if pos.metadata["is_call"] else "put",
            pos.metadata.get("pos_id"), reason, trade.pnl,
        )
        return trade

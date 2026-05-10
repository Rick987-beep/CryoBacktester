#!/usr/bin/env python3
"""
preopen_straddle.py — Long straddle/strangle entered during the NYSE pre-open
window (09:00–09:29 ET), exited on a spot excursion target or hard time cap.

Strategy logic
--------------
Entry:
  - NYSE trading day only (weekday + not a market holiday)
  - State timestamp falls in the 09:00–09:29 ET window
  - One trade per calendar day (no stacking)
  - ATM straddle (offset=0) or OTM strangle (offset>0) on the nearest
    unexpired Deribit daily expiry

Exit (evaluated each 5-min tick, first condition wins):
  1. Spot excursion: abs spot move from entry >= spot_move_pct (%)
       max(spot_high_since(entry), entry_spot) / entry_spot - 1
       or entry_spot / min(spot_low_since(entry), entry_spot) - 1
  2. Noon hard stop: NYC local time >= 12:00
  3. Max hold: elapsed minutes since entry >= max_hold_min
  4. Expiry guard: underlying option has expired

Execution model:
  - Open:  buy both legs at ask price
  - Close: sell both legs at bid price (0 if no quote — conservative)
  - Fees:  deribit_fee_per_leg on each leg each side

Naming note:
  spot_move_pct is the raw BTC spot excursion from entry — not a
  breakeven estimate. Real breakeven depends on the option premium paid.
"""
from datetime import timedelta
from typing import Any, Dict, List, Optional

from backtester.expiry_utils import parse_expiry_date, nearest_valid_expiry
from backtester.pricing import deribit_fee_per_leg, HOURS_PER_YEAR, EXPIRY_HOUR_UTC
from backtester.strategy_base import OpenPosition, Trade, close_trade
from market_hours import is_trading_day, to_nyc


class PreopenStraddle:
    """Long straddle/strangle entered in the 09:00–09:29 ET pre-open window.

    Exits when spot moves spot_move_pct% from entry, or at the noon ET
    hard stop, or after max_hold_min minutes — whichever comes first.
    """

    name = "preopen_straddle"
    DATE_RANGE = ("2025-10-01", "2026-04-23")
    DESCRIPTION = (
        "Buys an ATM straddle or OTM strangle during the NYSE pre-open window "
        "(09:00–09:29 ET) on NYSE trading days only. "
        "Exits when spot moves spot_move_pct% from entry, at noon ET, "
        "or after max_hold_min minutes."
    )

    PARAM_GRID = {
        "spot_move_pct": [1.00, 1.20, 1.40, 1.60, 1.80],
        "max_hold_min":  [60, 90, 120],
        "offset":        [1000],
        "min_dte":       [1, 7],
    }

    def __init__(self):
        self._position = None          # type: Optional[OpenPosition]
        self._spot_move_pct = 0.80
        self._max_hold_min = 60
        self._offset = 0
        self._min_dte = 1
        self._last_trade_date = None   # type: Optional[Any]

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._spot_move_pct = params["spot_move_pct"]
        self._max_hold_min = params["max_hold_min"]
        self._offset = params["offset"]
        self._min_dte = params["min_dte"]
        self._position = None
        self._last_trade_date = None

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        if self._position is not None:
            reason = self._check_expiry(state)
            if reason is None:
                reason = self._check_exits(state)
            if reason:
                trades.append(self._close(state, reason))

        if self._position is None:
            today = state.dt.date()
            if self._last_trade_date != today and self._is_valid_entry(state):
                self._try_open(state)

        return trades

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        if self._position is not None:
            return [self._close(state, "end_of_data")]
        return []

    def reset(self):
        # type: () -> None
        self._position = None
        self._last_trade_date = None

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "spot_move_pct": self._spot_move_pct,
            "max_hold_min":  self._max_hold_min,
            "offset":        self._offset,
            "min_dte":       self._min_dte,
        }

    # ------------------------------------------------------------------
    # Entry helpers
    # ------------------------------------------------------------------

    def _is_valid_entry(self, state):
        # type: (Any) -> bool
        """Return True if this tick qualifies as a valid entry."""
        if not is_trading_day(state.dt):
            return False
        nyc_dt = to_nyc(state.dt)
        # 09:00–09:29 ET (exclusive of 09:30 which is the NYSE open itself)
        return nyc_dt.hour == 9 and nyc_dt.minute < 30

    def _select_expiry(self, state):
        # type: (Any) -> Optional[str]
        """Return the nearest expiry with DTE >= min_dte that hasn't expired yet."""
        if self._min_dte <= 1:
            return nearest_valid_expiry(state)
        target_date = state.dt.date() + timedelta(days=self._min_dte)
        best = None
        best_dt = None
        for exp in state.expiries():
            exp_date = parse_expiry_date(exp)
            if exp_date is None:
                continue
            if exp_date.date() < target_date:
                continue
            exp_dt = exp_date.replace(hour=EXPIRY_HOUR_UTC, tzinfo=state.dt.tzinfo)
            if exp_dt <= state.dt:
                continue
            if best_dt is None or exp_dt < best_dt:
                best = exp
                best_dt = exp_dt
        return best

    def _try_open(self, state):
        # type: (Any) -> None
        expiry = self._select_expiry(state)
        if expiry is None:
            return

        if self._offset == 0:
            call, put = state.get_straddle(expiry)
        else:
            call, put = state.get_strangle(expiry, self._offset)

        if call is None or put is None:
            return
        if call.ask <= 0 or put.ask <= 0:
            return

        entry_usd = call.ask_usd + put.ask_usd
        if entry_usd <= 0 or entry_usd != entry_usd:
            return

        fee_call = deribit_fee_per_leg(state.spot, call.ask_usd)
        fee_put  = deribit_fee_per_leg(state.spot, put.ask_usd)

        self._position = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=[
                {"strike": call.strike, "is_call": True,
                 "expiry": expiry, "side": "buy",
                 "entry_price": call.ask, "entry_price_usd": call.ask_usd},
                {"strike": put.strike, "is_call": False,
                 "expiry": expiry, "side": "buy",
                 "entry_price": put.ask, "entry_price_usd": put.ask_usd},
            ],
            entry_price_usd=entry_usd,
            fees_open=fee_call + fee_put,
            metadata={
                "offset":       self._offset,
                "expiry":       expiry,
                "call_strike":  call.strike,
                "put_strike":   put.strike,
            },
        )

    # ------------------------------------------------------------------
    # Exit helpers
    # ------------------------------------------------------------------

    def _check_exits(self, state):
        # type: (Any) -> Optional[str]
        pos = self._position

        # 1. Spot excursion target
        threshold = self._spot_move_pct / 100.0
        high = state.spot_high_since(int(pos.entry_time.timestamp() * 1_000_000))
        low  = state.spot_low_since(int(pos.entry_time.timestamp() * 1_000_000))
        if (high - pos.entry_spot) / pos.entry_spot >= threshold:
            return "target_hit"
        if (pos.entry_spot - low) / pos.entry_spot >= threshold:
            return "target_hit"

        # 2. Noon ET hard stop
        if to_nyc(state.dt).hour >= 12:
            return "noon_exit"

        # 3. Max hold
        elapsed_min = (state.dt - pos.entry_time).total_seconds() / 60.0
        if elapsed_min >= self._max_hold_min:
            return "time_exit"

        return None

    def _check_expiry(self, state):
        # type: (Any) -> Optional[str]
        expiry_code = self._position.metadata.get("expiry")
        if expiry_code is None:
            return None
        exp_date = parse_expiry_date(expiry_code)
        if exp_date is None:
            return None
        exp_dt = exp_date.replace(hour=EXPIRY_HOUR_UTC, tzinfo=state.dt.tzinfo)
        if state.dt >= exp_dt:
            return "expiry"
        return None

    def _close(self, state, reason):
        # type: (Any, str) -> Trade
        pos = self._position
        expiry     = pos.metadata["expiry"]
        call_strike = pos.metadata["call_strike"]
        put_strike  = pos.metadata["put_strike"]

        if reason == "expiry":
            call_intrinsic = max(0.0, state.spot - call_strike)
            put_intrinsic  = max(0.0, put_strike  - state.spot)
            exit_usd   = call_intrinsic + put_intrinsic
            fees_close = 0.0
        else:
            call_q = state.get_option(expiry, call_strike, True)
            put_q  = state.get_option(expiry, put_strike,  False)
            call_bid_usd = (call_q.bid_usd if call_q else 0.0) or 0.0
            put_bid_usd  = (put_q.bid_usd  if put_q  else 0.0) or 0.0
            # NaN guard
            if call_bid_usd != call_bid_usd:
                call_bid_usd = 0.0
            if put_bid_usd != put_bid_usd:
                put_bid_usd = 0.0
            exit_usd   = call_bid_usd + put_bid_usd
            fees_close = (deribit_fee_per_leg(state.spot, call_bid_usd) +
                          deribit_fee_per_leg(state.spot, put_bid_usd))

        trade = close_trade(state, pos, reason, exit_usd, fees_close)
        trade.metadata.update({
            "spot_move_pct": self._spot_move_pct,
            "max_hold_min":  self._max_hold_min,
            "min_dte":       self._min_dte,
        })
        self._last_trade_date = pos.entry_time.date()
        self._position = None
        return trade

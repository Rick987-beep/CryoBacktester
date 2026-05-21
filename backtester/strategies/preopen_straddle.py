#!/usr/bin/env python3
"""
preopen_straddle.py — Long straddle/strangle entered during the NYSE pre-open
window (09:00–09:29 ET), exited on a spot excursion target or hard time cap.

Strategy logic
--------------
Entry:
  - NYSE trading day only (weekday + not a market holiday)
  - State timestamp falls in the entry_hour_et:entry_min_et ET window
    (30-minute window; DST-aware via market_hours.to_nyc)
  - One trade per calendar day (no stacking)
  - ATM straddle (offset=0) or OTM strangle (offset>0) on the nearest
    unexpired Deribit daily expiry

Exit — combined mode (individual_exit=0, evaluated each 5-min tick):
  1. Spot excursion: abs spot move from entry >= spot_move_pct (%)
       max(spot_high_since(entry), entry_spot) / entry_spot - 1
       or entry_spot / min(spot_low_since(entry), entry_spot) - 1
  2. Noon hard stop: NYC local time >= 12:00
  3. Max hold: elapsed minutes since entry >= max_hold_min
  4. Expiry guard: underlying option has expired

Exit — individual leg mode (individual_exit=1):
  - Call leg closes independently when spot moves UP >= spot_move_pct%
  - Put leg closes independently when spot moves DOWN >= spot_move_pct%
  - Each leg emits its own Trade record
  - Hard stops (noon, max hold, expiry) close any remaining open legs

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
from backtester.strategy_base import (
    OpenPosition, Trade, close_position, partial_close,
)
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
        "spot_move_pct":   [1.00, 1.20, 1.40, 1.60, 1.80],
        "max_hold_min":    [60, 90, 120, 180],
        "offset":          [500, 1000, 1500],
        "min_dte":         [1, 7],
        "entry_hour_et":   [6,7,8,9],
        "entry_min_et":    [0],
        "individual_exit": [0, 1],
    }

    def __init__(self):
        self._position = None          # type: Optional[OpenPosition]
        self._spot_move_pct = 0.80
        self._max_hold_min = 60
        self._offset = 0
        self._min_dte = 1
        self._entry_hour_et = 9
        self._entry_min_et = 0
        self._individual_exit = 0
        self._last_trade_date = None   # type: Optional[Any]
        self._pos_counter = 0          # monotonic pos_id source

    def _next_pos_id(self):
        # type: () -> int
        self._pos_counter += 1
        return self._pos_counter

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._spot_move_pct = params["spot_move_pct"]
        self._max_hold_min = params["max_hold_min"]
        self._offset = params["offset"]
        self._min_dte = params["min_dte"]
        self._entry_hour_et = params.get("entry_hour_et", 9)
        self._entry_min_et = params.get("entry_min_et", 0)
        self._individual_exit = params.get("individual_exit", 0)
        self._position = None
        self._last_trade_date = None
        self._pos_counter = 0

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        if self._position is not None:
            reason = self._check_expiry(state)
            if reason:
                if self._individual_exit:
                    trades.extend(self._close_all_remaining(state, reason))
                else:
                    trades.append(self._close(state, reason))
            elif self._individual_exit:
                trades.extend(self._check_individual_leg_exits(state))
                if self._position is not None:
                    hard_stop = self._check_hard_stops(state)
                    if hard_stop:
                        trades.extend(self._close_all_remaining(state, hard_stop))
            else:
                reason = self._check_exits(state)
                if reason:
                    trades.append(self._close(state, reason))

        if self._position is None:
            today = state.dt.date()
            if self._last_trade_date != today and self._is_valid_entry(state):
                open_trade = self._try_open(state)
                if open_trade is not None:
                    trades.append(open_trade)

        return trades

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        if self._position is not None:
            if self._individual_exit:
                return self._close_all_remaining(state, "end_of_data")
            return [self._close(state, "end_of_data")]
        return []

    def reset(self):
        # type: () -> None
        self._position = None
        self._last_trade_date = None
        self._pos_counter = 0

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "spot_move_pct":   self._spot_move_pct,
            "max_hold_min":    self._max_hold_min,
            "offset":          self._offset,
            "min_dte":         self._min_dte,
            "entry_hour_et":   self._entry_hour_et,
            "entry_min_et":    self._entry_min_et,
            "individual_exit": self._individual_exit,
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
        nyc_min = nyc_dt.hour * 60 + nyc_dt.minute
        start_min = self._entry_hour_et * 60 + self._entry_min_et
        return start_min <= nyc_min < start_min + 30

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
        # type: (Any) -> Optional[Trade]
        expiry = self._select_expiry(state)
        if expiry is None:
            return None

        if self._offset == 0:
            call, put = state.get_straddle(expiry)
        else:
            call, put = state.get_strangle(expiry, self._offset)

        if call is None or put is None:
            return None
        if call.ask <= 0 or put.ask <= 0:
            return None

        entry_usd = call.ask_usd + put.ask_usd
        if entry_usd <= 0 or entry_usd != entry_usd:
            return None

        fee_call = deribit_fee_per_leg(state.spot, call.ask_usd)
        fee_put  = deribit_fee_per_leg(state.spot, put.ask_usd)

        legs = [
            {"strike": call.strike, "is_call": True,
             "expiry": expiry, "side": "buy", "qty": 1.0,
             "price_btc": call.ask,
             "entry_price": call.ask, "entry_price_usd": call.ask_usd,
             "fee_usd_open": fee_call, "fees_open": fee_call},
            {"strike": put.strike,  "is_call": False,
             "expiry": expiry, "side": "buy", "qty": 1.0,
             "price_btc": put.ask,
             "entry_price": put.ask, "entry_price_usd": put.ask_usd,
             "fee_usd_open": fee_put, "fees_open": fee_put},
        ]

        pos_id = self._next_pos_id()
        self._position = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=legs,
            entry_price_usd=entry_usd,
            fees_open=fee_call + fee_put,
            metadata={
                "direction":    "buy",
                "pos_id":       pos_id,
                "offset":       self._offset,
                "expiry":       expiry,
                "call_strike":  call.strike,
                "put_strike":   put.strike,
            },
        )

        # Explicit open Trade — engine emits open fills, stamps leg['_open_idx'].
        return Trade(
            entry_time=state.dt,
            exit_time=state.dt,
            entry_spot=state.spot,
            exit_spot=state.spot,
            entry_price_usd=entry_usd,
            exit_price_usd=0.0,
            fees=fee_call + fee_put,
            pnl=0.0,
            triggered=False,
            exit_reason="",
            exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={
                "direction": "buy",
                "pos_id":    pos_id,
                "legs":      legs,
            },
        )

    # ------------------------------------------------------------------
    # Exit helpers
    # ------------------------------------------------------------------

    def _check_hard_stops(self, state):
        # type: (Any) -> Optional[str]
        pos = self._position
        if to_nyc(state.dt).hour >= 12:
            return "noon_exit"
        elapsed_min = (state.dt - pos.entry_time).total_seconds() / 60.0
        if elapsed_min >= self._max_hold_min:
            return "time_exit"
        return None

    def _check_exits(self, state):
        # type: (Any) -> Optional[str]
        pos = self._position
        threshold = self._spot_move_pct / 100.0
        high = state.spot_high_since(int(pos.entry_time.timestamp() * 1_000_000))
        low  = state.spot_low_since(int(pos.entry_time.timestamp() * 1_000_000))
        if (high - pos.entry_spot) / pos.entry_spot >= threshold:
            return "target_hit"
        if (pos.entry_spot - low) / pos.entry_spot >= threshold:
            return "target_hit"
        return self._check_hard_stops(state)

    def _check_individual_leg_exits(self, state):
        # type: (Any) -> List[Trade]
        pos = self._position
        trades = []
        threshold = self._spot_move_pct / 100.0
        high = state.spot_high_since(int(pos.entry_time.timestamp() * 1_000_000))
        low  = state.spot_low_since(int(pos.entry_time.timestamp() * 1_000_000))
        # Snapshot pos.legs — _close_leg mutates it (via partial_close).
        for leg in list(pos.legs):
            if leg["is_call"]:
                if (high - pos.entry_spot) / pos.entry_spot >= threshold:
                    trades.append(self._close_leg(state, leg, "target_hit"))
            else:
                if (pos.entry_spot - low) / pos.entry_spot >= threshold:
                    trades.append(self._close_leg(state, leg, "target_hit"))
            if self._position is None:
                break  # last leg closed — position torn down
        return trades

    def _close_leg(self, state, leg, reason):
        # type: (Any, Any, str) -> Trade
        pos = self._position
        expiry  = leg["expiry"]
        strike  = leg["strike"]
        is_call = leg["is_call"]

        if reason == "expiry":
            exit_usd_per = max(0.0, state.spot - strike) if is_call else max(0.0, strike - state.spot)
            exit_btc_per = (exit_usd_per / state.spot) if state.spot else 0.0
            fees_close = 0.0
        else:
            q = state.get_option(expiry, strike, is_call)
            bid_usd = (q.bid_usd if q else 0.0) or 0.0
            if bid_usd != bid_usd:  # NaN guard
                bid_usd = 0.0
            bid_btc = (q.bid if q else 0.0) or 0.0
            if bid_btc != bid_btc:
                bid_btc = 0.0
            exit_usd_per = bid_usd
            exit_btc_per = bid_btc
            fees_close = deribit_fee_per_leg(state.spot, bid_usd)

        leg["exit_price_btc"] = exit_btc_per
        leg["exit_price_usd"] = exit_usd_per
        if reason == "expiry":
            leg["fee_btc_close"] = 0.0

        if len(pos.legs) > 1:
            leg_idx = pos.legs.index(leg)
            trade = partial_close(state, pos, [leg_idx], reason, fees_close)
        else:
            trade = close_position(state, pos, reason, exit_usd_per, fees_close)
            trade.metadata["skip_open_fill"] = True
            self._last_trade_date = pos.entry_time.date()
            self._position = None

        trade.metadata.update({
            "leg":           "call" if is_call else "put",
            "strike":        strike,
            "spot_move_pct": self._spot_move_pct,
            "max_hold_min":  self._max_hold_min,
            "min_dte":       self._min_dte,
        })
        return trade

    def _close_all_remaining(self, state, reason):
        # type: (Any, str) -> List[Trade]
        trades = []
        # _close_leg removes the leg (via partial_close) or tears down the
        # position (on the final leg). Loop until empty.
        while self._position is not None and self._position.legs:
            leg = self._position.legs[0]
            trades.append(self._close_leg(state, leg, reason))
        return trades

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
        expiry      = pos.metadata["expiry"]
        call_strike = pos.metadata["call_strike"]
        put_strike  = pos.metadata["put_strike"]

        if reason == "expiry":
            call_exit_usd = max(0.0, state.spot - call_strike)
            put_exit_usd  = max(0.0, put_strike  - state.spot)
            call_exit_btc = (call_exit_usd / state.spot) if state.spot else 0.0
            put_exit_btc  = (put_exit_usd  / state.spot) if state.spot else 0.0
            fees_close = 0.0
        else:
            call_q = state.get_option(expiry, call_strike, True)
            put_q  = state.get_option(expiry, put_strike,  False)
            call_bid_usd = (call_q.bid_usd if call_q else 0.0) or 0.0
            put_bid_usd  = (put_q.bid_usd  if put_q  else 0.0) or 0.0
            call_bid_btc = (call_q.bid if call_q else 0.0) or 0.0
            put_bid_btc  = (put_q.bid  if put_q  else 0.0) or 0.0
            # NaN guards
            if call_bid_usd != call_bid_usd: call_bid_usd = 0.0
            if put_bid_usd  != put_bid_usd:  put_bid_usd  = 0.0
            if call_bid_btc != call_bid_btc: call_bid_btc = 0.0
            if put_bid_btc  != put_bid_btc:  put_bid_btc  = 0.0
            call_exit_usd = call_bid_usd
            put_exit_usd  = put_bid_usd
            call_exit_btc = call_bid_btc
            put_exit_btc  = put_bid_btc
            fees_close = (deribit_fee_per_leg(state.spot, call_bid_usd) +
                          deribit_fee_per_leg(state.spot, put_bid_usd))

        exit_usd = call_exit_usd + put_exit_usd

        # Annotate legs with BTC exit price (per contract) — engine uses for
        # close fills; close_position uses for leg-aware PnL.
        for leg in pos.legs:
            if leg["is_call"]:
                leg["exit_price_btc"] = call_exit_btc
                leg["exit_price_usd"] = call_exit_usd
            else:
                leg["exit_price_btc"] = put_exit_btc
                leg["exit_price_usd"] = put_exit_usd
            if reason == "expiry":
                leg["fee_btc_close"] = 0.0

        trade = close_position(state, pos, reason, exit_usd, fees_close)
        # Open fills were already emitted by the explicit side='open' Trade.
        trade.metadata["skip_open_fill"] = True
        trade.metadata.update({
            "spot_move_pct": self._spot_move_pct,
            "max_hold_min":  self._max_hold_min,
            "min_dte":       self._min_dte,
        })
        self._last_trade_date = pos.entry_time.date()
        self._position = None
        return trade

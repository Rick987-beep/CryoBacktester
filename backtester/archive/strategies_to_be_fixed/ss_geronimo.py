#!/usr/bin/env python3
"""
ss_geronimo.py — Sell a 1DTE straddle every evening, letting legs expire the
next morning, with per-leg dollar-distance stop losses.

Entry:
    - Time-based: entry_time (NYC wall-clock, default "18:00"), DST-aware.
    - Gated by the Turbulence indicator (same as short_str_turb_dyn).
    - NaN / missing turbulence → fail-open.
    - Skips weekends (skip_weekends) and NYSE holidays (skip_exchange_holidays).
    - One trade per calendar day.

Legs:
    - Always 1DTE, always a 2-leg strangle (call + put).
    - Delta-selected via select_by_delta; pushed further OTM by min_otm_pct.
    - Filtered by leg_min_price (minimum BTC bid; 0 = disabled).

Exit:
    - Per-leg dollar-distance stop loss, checked every tick:
        stop_price_put  = put_strike  - dollar_distance  → close put if spot < this
        stop_price_call = call_strike + dollar_distance  → close call if spot > this
      Whichever leg is stopped is closed alone via partial_close(); the other
      leg continues to its own stop or to expiry.  Both legs triggered on the
      same tick → full close_position().
    - Expiry settlement at Deribit 08:00 UTC (check_expiry).
    - No global % SL.  No TP.

Quantity sizing:
    dyn_target_premium / total_premium_usd → round to 0.1, capped at max_quantity.
    dyn_target_premium = 0 → fixed qty 1.
"""
import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import expiry_dt_utc, select_expiry
from backtester.indicators import IndicatorDep
from backtester.pricing import deribit_fee_per_leg
from backtester.strategy_base import (
    OpenPosition, Trade, close_position, partial_close,
    check_expiry,
)
from market_hours import is_market_holiday, to_nyc, to_utc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _apply_min_otm(chain, selected, spot, min_pct, is_call):
    # type: (list, Any, float, float, bool) -> Optional[Any]
    """Push selected leg further OTM if it is within min_pct% of spot."""
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
            key=lambda q: q.strike, reverse=True,
        )
    return candidates[0] if candidates else None


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

class SsGeronimo:
    """Sell a 1DTE strangle each evening, exit via per-leg dollar-distance SL
    or natural expiry.  Turbulence-gated entry.
    """

    name = "ss_geronimo"
    DATE_RANGE = ("2026-01-01", "2026-05-15")
    DESCRIPTION = (
        "1DTE strangle sold each evening (NYC entry_time), gated by Turbulence. "
        "Each leg has an independent dollar-distance stop loss: if spot crosses "
        "strike ± dollar_distance, that leg alone is bought back; the other leg "
        "continues to expiry.  No global % SL, no TP."
    )

    indicator_deps = [
        IndicatorDep(name="turbulence", symbol="BTCUSDT", interval="15m"),
    ]

    PARAM_GRID = {
        "delta":                    [0.05],
        "min_otm_pct":              [2.1],
        "entry_time":               ["18:00"],   # NYC (US Eastern), DST-aware
        "dollar_distance":          [200, 300, 500],
        "turbulence_threshold":     [60],
        "skip_weekends":            [1],
        "skip_exchange_holidays":   [1],
        "dyn_target_premium":       [800],
        "max_quantity":             [20],
        "leg_min_price":            [0],
    }

    def __init__(self):
        self._delta = 0.15
        self._min_otm_pct = 2.0
        self._entry_hour = 18
        self._entry_minute = 0
        self._dollar_distance = 500.0
        self._turbulence_threshold = 60
        self._skip_weekends = 1
        self._skip_exchange_holidays = 1
        self._dyn_target_premium = 800.0
        self._max_quantity = 25.0
        self._leg_min_price = 0.0

        self._positions = []          # type: List[OpenPosition]
        self._last_trade_date = None  # type: Optional[Any]
        self._watch_start = None      # type: Optional[datetime]
        self._pos_counter = 0

        self._turbulence = None       # type: Optional[Any]

    # ------------------------------------------------------------------
    # Indicator injection
    # ------------------------------------------------------------------

    def set_indicators(self, ind):
        # type: (Dict[str, Any]) -> None
        self._turbulence = ind.get("turbulence")

    # ------------------------------------------------------------------
    # Strategy protocol
    # ------------------------------------------------------------------

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._delta = params.get("delta", 0.15)
        self._min_otm_pct = params.get("min_otm_pct", 2.0)
        raw_time = params.get("entry_time", "18:00")
        h, m = (int(x) for x in raw_time.split(":"))
        self._entry_hour = h
        self._entry_minute = m
        self._dollar_distance = float(params.get("dollar_distance", 500))
        self._turbulence_threshold = params.get("turbulence_threshold", 60)
        self._skip_weekends = params.get("skip_weekends", 1)
        self._skip_exchange_holidays = params.get("skip_exchange_holidays", 1)
        self._dyn_target_premium = float(params.get("dyn_target_premium", 0))
        self._max_quantity = float(params.get("max_quantity", 25))
        self._leg_min_price = float(params.get("leg_min_price", 0))

        self._positions = []
        self._last_trade_date = None
        self._watch_start = None
        self._pos_counter = 0

    def reset(self):
        # type: () -> None
        self._positions = []
        self._last_trade_date = None
        self._watch_start = None
        self._pos_counter = 0

    def _next_pos_id(self):
        # type: () -> int
        self._pos_counter += 1
        return self._pos_counter

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "delta":                    self._delta,
            "min_otm_pct":              self._min_otm_pct,
            "entry_time":               f"{self._entry_hour:02d}:{self._entry_minute:02d}",
            "dollar_distance":          self._dollar_distance,
            "turbulence_threshold":     self._turbulence_threshold,
            "skip_weekends":            self._skip_weekends,
            "skip_exchange_holidays":   self._skip_exchange_holidays,
            "dyn_target_premium":       self._dyn_target_premium,
            "max_quantity":             self._max_quantity,
            "leg_min_price":            self._leg_min_price,
        }

    # ------------------------------------------------------------------
    # Main tick handler
    # ------------------------------------------------------------------

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        # ── Exits ──────────────────────────────────────────────────
        to_close = []
        for pos in list(self._positions):
            result = self._check_exits(state, pos)
            if result is not None:
                closed_trades, fully_closed = result
                trades.extend(closed_trades)
                if fully_closed:
                    to_close.append(pos)
        for pos in to_close:
            self._positions.remove(pos)

        # ── Entry ──────────────────────────────────────────────────
        if not self._positions:
            today = state.dt.date()
            if self._last_trade_date != today:
                if self._skip_weekends and state.dt.weekday() >= 5:
                    pass  # skip Saturday / Sunday
                elif self._skip_exchange_holidays and is_market_holiday(state.dt):
                    pass  # skip NYSE holidays
                else:
                    open_trade = self._maybe_open(state)
                    if open_trade is not None:
                        trades.append(open_trade)

        return trades

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        trades = []
        for pos in list(self._positions):
            trades.extend(self._force_close_all(state, pos))
        self._positions.clear()
        return trades

    # ------------------------------------------------------------------
    # Exit logic
    # ------------------------------------------------------------------

    def _check_exits(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[tuple]
        """Check all exit conditions for a position.

        Returns (list_of_trades, fully_closed) or None if nothing triggered.
        """
        # 1. Expiry
        reason = check_expiry(state, pos)
        if reason:
            return ([self._close_all(state, pos, reason)], True)

        # 2. Per-leg dollar-distance stop loss
        return self._check_leg_stops(state, pos)

    def _check_leg_stops(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[tuple]
        """Check per-leg dollar-distance stop losses.

        Returns (list_of_trades, fully_closed) or None.
        """
        spot = state.spot
        stop_call = pos.metadata.get("stop_price_call")
        stop_put  = pos.metadata.get("stop_price_put")

        # Determine which legs are still alive (leg identified by is_call)
        has_call = any(leg["is_call"] for leg in pos.legs)
        has_put  = any(not leg["is_call"] for leg in pos.legs)

        call_triggered = has_call and stop_call is not None and spot > stop_call
        put_triggered  = has_put  and stop_put  is not None and spot < stop_put

        if not call_triggered and not put_triggered:
            return None

        # Data gap guard: check quotes exist before closing
        expiry = pos.metadata["expiry"]
        if call_triggered:
            call_strike = pos.metadata["call_strike"]
            if state.get_option(expiry, call_strike, True) is None:
                call_triggered = False  # retry next tick
        if put_triggered:
            put_strike = pos.metadata["put_strike"]
            if state.get_option(expiry, put_strike, False) is None:
                put_triggered = False  # retry next tick

        if not call_triggered and not put_triggered:
            return None

        if call_triggered and put_triggered:
            # Both stopped on the same tick → full close
            return ([self._close_all(state, pos, "stop_loss")], True)

        if call_triggered:
            idx = next(i for i, leg in enumerate(pos.legs) if leg["is_call"])
            t = self._close_one_leg(state, pos, idx, "stop_loss_call")
            # pos is now a single-leg put position; stays alive
            return ([t], False)

        # put_triggered
        idx = next(i for i, leg in enumerate(pos.legs) if not leg["is_call"])
        t = self._close_one_leg(state, pos, idx, "stop_loss_put")
        return ([t], False)

    # ------------------------------------------------------------------
    # Close helpers
    # ------------------------------------------------------------------

    def _leg_exit_prices(self, state, leg):
        # type: (Any, dict) -> tuple
        """Return (exit_btc_per, exit_usd_per, fee_usd_per) for a leg at market ask."""
        _min_tick_btc = 0.0001
        _min_tick_usd = _min_tick_btc * state.spot
        expiry    = leg["expiry"]
        strike    = leg["strike"]
        is_call   = leg["is_call"]
        q = state.get_option(expiry, strike, is_call)
        exit_btc = q.ask     if q and q.ask > 0 else _min_tick_btc
        exit_usd = q.ask_usd if q and q.ask > 0 else _min_tick_usd
        fee_usd  = deribit_fee_per_leg(state.spot, exit_usd)
        return exit_btc, exit_usd, fee_usd

    def _leg_expiry_prices(self, state, leg):
        # type: (Any, dict) -> tuple
        """Return (exit_btc_per, exit_usd_per, fee_usd_per) for expiry settlement."""
        strike  = leg["strike"]
        is_call = leg["is_call"]
        spot    = state.spot
        if is_call:
            exit_usd = max(0.0, spot - strike)
        else:
            exit_usd = max(0.0, strike - spot)
        exit_btc = (exit_usd / spot) if spot else 0.0
        return exit_btc, exit_usd, 0.0  # no fee at expiry

    def _annotate_leg(self, state, leg, reason):
        # type: (Any, dict, str) -> tuple
        """Annotate a leg dict with exit prices; return (exit_btc, exit_usd, fee_usd)."""
        if reason == "expiry":
            exit_btc, exit_usd, fee_usd = self._leg_expiry_prices(state, leg)
        else:
            exit_btc, exit_usd, fee_usd = self._leg_exit_prices(state, leg)
        leg["exit_price_btc"] = exit_btc
        leg["exit_price_usd"] = exit_usd
        leg["fee_btc_close"]  = (fee_usd / state.spot) if state.spot else 0.0
        return exit_btc, exit_usd, fee_usd

    def _close_all(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        """Annotate all legs and return a full close Trade."""
        quantity    = float(pos.metadata.get("quantity", 1.0))
        total_exit  = 0.0
        total_fees  = 0.0
        for leg in pos.legs:
            _, exit_usd, fee_usd = self._annotate_leg(state, leg, reason)
            total_exit += exit_usd
            total_fees += fee_usd
        trade = close_position(
            state, pos, reason,
            total_exit * quantity,
            total_fees * quantity,
        )
        trade.metadata["dollar_distance"]      = self._dollar_distance
        trade.metadata["turbulence_threshold"] = self._turbulence_threshold
        trade.metadata["quantity"]             = quantity
        return trade

    def _close_one_leg(self, state, pos, leg_index, reason):
        # type: (Any, OpenPosition, int, str) -> Trade
        """Annotate a single leg and partial-close it."""
        leg = pos.legs[leg_index]
        qty = float(leg.get("qty", 1.0))
        _, exit_usd, fee_usd = self._annotate_leg(state, leg, reason)
        trade = partial_close(
            state, pos, [leg_index], reason,
            fees_close=fee_usd * qty,
        )
        trade.metadata["dollar_distance"]      = self._dollar_distance
        trade.metadata["turbulence_threshold"] = self._turbulence_threshold
        trade.metadata["quantity"]             = qty
        return trade

    def _force_close_all(self, state, pos):
        # type: (Any, OpenPosition) -> List[Trade]
        """Force-close everything at end of data."""
        return [self._close_all(state, pos, "end_of_data")]

    # ------------------------------------------------------------------
    # Quantity sizing
    # ------------------------------------------------------------------

    def _compute_quantity(self, premium_per_contract):
        # type: (float) -> float
        if self._dyn_target_premium <= 0:
            return 1.0
        if premium_per_contract <= 0:
            return 1.0
        raw_qty  = self._dyn_target_premium / premium_per_contract
        quantity = min(raw_qty, self._max_quantity)
        quantity = round(quantity, 1)
        quantity = max(quantity, 0.1)
        return quantity

    # ------------------------------------------------------------------
    # Entry logic
    # ------------------------------------------------------------------

    def _maybe_open(self, state):
        # type: (Any) -> Optional[Trade]
        """Return an open Trade if all entry conditions are met, else None."""
        dt    = state.dt
        today = dt.date()

        # Translate NYC entry_time to UTC (DST-aware)
        entry_utc = to_utc(
            to_nyc(dt).replace(
                hour=self._entry_hour, minute=self._entry_minute,
                second=0, microsecond=0,
            )
        )
        entry_utc_mins = entry_utc.hour * 60 + entry_utc.minute
        if dt.hour * 60 + dt.minute < entry_utc_mins:
            return None

        # Track watch start (informational)
        if self._watch_start is None or self._watch_start.date() != today:
            self._watch_start = dt.replace(minute=0, second=0, microsecond=0)

        # Turbulence gate
        if not self._turbulence_ok(dt):
            return None

        return self._try_open(state)

    def _turbulence_ok(self, dt):
        # type: (datetime) -> bool
        if self._turbulence is None:
            return True  # no data → fail-open
        hour_ts = dt.replace(minute=0, second=0, microsecond=0)
        try:
            row = self._turbulence.loc[hour_ts]
            composite = row["composite"]
        except KeyError:
            return True  # missing hour → fail-open
        try:
            if math.isnan(composite):
                return True
        except TypeError:
            return True
        return float(composite) < self._turbulence_threshold

    def _try_open(self, state):
        # type: (Any) -> Optional[Trade]
        expiry = select_expiry(state, dte=1)
        if expiry is None:
            return None

        chain = state.get_chain(expiry)
        if not chain:
            return None

        calls   = [q for q in chain if q.is_call]
        puts    = [q for q in chain if not q.is_call]
        exp_dt  = expiry_dt_utc(expiry, state.dt.tzinfo)

        call = select_by_delta(calls, +self._delta)
        put  = select_by_delta(puts,  -self._delta)
        if call is None or put is None:
            return None

        if self._min_otm_pct > 0:
            call = _apply_min_otm(calls, call, state.spot, self._min_otm_pct, is_call=True)
            put  = _apply_min_otm(puts,  put,  state.spot, self._min_otm_pct, is_call=False)
            if call is None or put is None:
                return None

        # Price floor check
        _min_p = self._leg_min_price
        if _min_p > 0:
            if call.bid < _min_p or put.bid < _min_p:
                return None
        elif call.bid <= 0 or put.bid <= 0:
            return None

        call_usd  = call.bid_usd
        put_usd   = put.bid_usd
        entry_usd = call_usd + put_usd
        if entry_usd <= 0:
            return None

        quantity  = self._compute_quantity(entry_usd)
        fee_call  = deribit_fee_per_leg(state.spot, call_usd)
        fee_put   = deribit_fee_per_leg(state.spot, put_usd)

        # Compute per-leg stop prices (dollar distance from strike)
        stop_price_call = call.strike + self._dollar_distance
        stop_price_put  = put.strike  - self._dollar_distance

        legs = [
            {
                "strike":          call.strike,
                "is_call":         True,
                "expiry":          expiry,
                "side":            "sell",
                "qty":             quantity,
                "price_btc":       call.bid,
                "entry_price":     call.bid,
                "entry_price_usd": call_usd,
                "entry_delta":     call.delta,
                "fee_usd_open":    fee_call,
            },
            {
                "strike":          put.strike,
                "is_call":         False,
                "expiry":          expiry,
                "side":            "sell",
                "qty":             quantity,
                "price_btc":       put.bid,
                "entry_price":     put.bid,
                "entry_price_usd": put_usd,
                "entry_delta":     put.delta,
                "fee_usd_open":    fee_put,
            },
        ]

        pos_id = self._next_pos_id()
        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=legs,
            entry_price_usd=entry_usd * quantity,
            fees_open=(fee_call + fee_put) * quantity,
            metadata={
                "direction":        "sell",
                "expiry":           expiry,
                "expiry_dt":        exp_dt,
                "call_strike":      call.strike,
                "put_strike":       put.strike,
                "call_delta":       call.delta,
                "put_delta":        put.delta,
                "stop_price_call":  stop_price_call,
                "stop_price_put":   stop_price_put,
                "dollar_distance":  self._dollar_distance,
                "quantity":         quantity,
                "pos_id":           pos_id,
            },
        )
        self._positions.append(pos)
        self._last_trade_date = state.dt.date()

        return Trade(
            entry_time=state.dt,
            exit_time=state.dt,
            entry_spot=state.spot,
            exit_spot=state.spot,
            entry_price_usd=entry_usd * quantity,
            exit_price_usd=0.0,
            fees=(fee_call + fee_put) * quantity,
            pnl=0.0,
            triggered=False,
            exit_reason="",
            exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={
                "direction": "sell",
                "pos_id":    pos_id,
                "legs":      legs,
            },
        )

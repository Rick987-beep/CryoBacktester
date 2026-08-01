#!/usr/bin/env python3
"""
ss_turb_dyn_mk2.py — Short N-DTE option (delta-selected) gated by the
Turbulence indicator, with SL + TP + time/expiry exit and dynamic
quantity sizing.

Extends short_str_turb_dyn with a **premium step-up** mechanism:

    min_desired_premium — if the initially selected leg's bid is below this
        threshold (BTC), the strategy walks inward (toward ATM) through the
        chain to find the first strike whose bid >= min_desired_premium.
        0 = disabled (original behaviour).

    step_up_max_delta — hard safety cap on the stepped-up option's absolute
        delta.  The walk stops as soon as abs(delta) would exceed this value.
        0 = no cap (walk as far as needed to find a premium-satisfying strike).

In plain English: if the market is quiet and the far-OTM option is nearly
worthless, try the next strike closer to ATM that has a more attractive
premium — but never exceed the delta cap.

leg_min_price remains as the hard absolute floor and is checked after any
step-up so we never trade an option below it regardless.

Everything else (turbulence gate, entry_time, min_otm_pct, TP/SL,
max_hold_hours, expiry settlement, dynamic quantity) is identical to
ShortStrTurbDyn.
"""
import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import parse_expiry_date, expiry_dt_utc, select_expiry
from backtester.indicators import IndicatorDep
from backtester.pricing import deribit_fee_per_leg
from backtester.strategy_base import (
    OpenPosition, Trade, close_position,
    check_expiry, check_take_profit_strangle,
    stop_loss_pct, max_hold_hours,
)
from market_hours import to_nyc, to_utc


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _apply_min_otm(chain, selected, spot, min_pct, is_call):
    # type: (list, Any, float, float, bool) -> Optional[Any]
    """Push `selected` further OTM if it is within min_pct% of spot.
    Returns None if no qualifying strike exists in the chain.

    Call leg: strike must be >= spot * (1 + min_pct/100)
    Put  leg: strike must be <= spot * (1 - min_pct/100)
    """
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


# ------------------------------------------------------------------
# Strategy
# ------------------------------------------------------------------


class SsTurbDynMk2:
    """Sell N-DTE OTM strangle (delta-selected), gated by Turbulence indicator,
    with a premium step-up mechanism.

    Entry: time-based window starting at entry_time (NYC wall-clock, e.g. "15:00").
    Opens as soon as the turbulence composite is below turbulence_threshold.

    Step-up: if the selected leg's bid < min_desired_premium, walk inward
    (toward ATM) until a strike with bid >= min_desired_premium is found,
    subject to the step_up_max_delta cap.

    Exit: take-profit, stop-loss, max hold duration, or expiry settlement.
    """

    name = "ss_turb_dyn_mk2"
    DATE_RANGE = ("2025-05-05", "2026-05-05")
    DESCRIPTION = (
        "Sells a strangle, naked call, or naked put on a Deribit expiry N calendar "
        "days ahead (dte=1/2/3), with legs chosen by target delta. "
        "leg_type controls the structure: 'strangle' = call+put, 'call' = call only, "
        "'put' = put only. "
        "Entry is gated by the Turbulence indicator: opens only when the composite "
        "score (0–100) is below turbulence_threshold. entry_time is a NYC wall-clock "
        "time (e.g. '15:00') translated to UTC at runtime (DST-aware). NaN score → open freely. "
        "min_otm_pct pushes delta-selected legs further OTM until they are at least that "
        "percentage from spot (0 = disabled). "
        "min_desired_premium: if a selected leg's bid is below this (BTC), step up toward ATM "
        "to find a better-priced option; step_up_max_delta caps how far inward we go. "
        "TP uses raw ask prices. SL and max-hold exits unchanged."
    )

    # Turbulence indicator — pre-computed once per grid run by the engine.
    indicator_deps = [
        IndicatorDep(name="turbulence", symbol="BTCUSDT", interval="15m"),
    ]

    PARAM_GRID = {
        # entry_time: NYC wall-clock (HH:MM); translated to UTC in _maybe_open (DST-aware)
        "leg_type":             ["strangle"],
        "dte":                  [1],
        "delta":                [0.15],
        "entry_time":           ["14:00", "14:30", "15:00", "15:30"],  # NYC (US Eastern)
        "stop_loss_pct":        [5.0, 5.75],
        "take_profit_pct":      [0],
        "max_hold_hours":       [0],
        "skip_weekends":        [1],
        "min_otm_pct":          [2.1, 2.3, 2.5],
        "turbulence_threshold": [50, 55, 60],
        "dyn_target_premium":   [600],
        "max_quantity":         [25],
        "leg_min_price":        [0],
        # Step-up mechanism
        "min_desired_premium":  [0, 0.0002, 0.0003],   # BTC; 0 = disabled
        "step_up_max_delta":    [0.2, 0.3],                # abs delta cap; 0 = no cap
    }

    def __init__(self):
        self._positions = []          # type: List[OpenPosition]
        self._leg_type = "strangle"
        self._dte = 1
        self._max_concurrent = 1
        self._delta = 0.15
        self._sl_pct = 4.0
        self._tp_pct = 0.75
        self._entry_hour = 15
        self._entry_minute = 0
        self._max_hold_hours = 0
        self._skip_weekends = 0
        self._turbulence_threshold = 50
        self._min_otm_pct = 0
        self._dyn_target_premium = 0.0
        self._max_quantity = 10.0
        self._leg_min_price = 0.0002
        self._min_desired_premium = 0.0
        self._step_up_max_delta = 0.0
        self._last_trade_date = None  # type: Optional[Any]
        self._watch_start = None      # type: Optional[datetime]
        self._exit_conditions = []

        # Turbulence DataFrame (hourly index, "composite" column); None until injected.
        self._turbulence = None       # type: Optional[Any]
        self._pos_counter = 0         # monotonic pos_id source

    def _next_pos_id(self):
        # type: () -> int
        self._pos_counter += 1
        return self._pos_counter

    # ------------------------------------------------------------------
    # Indicator injection (called once by engine before tick loop)
    # ------------------------------------------------------------------

    def set_indicators(self, ind):
        # type: (Dict[str, Any]) -> None
        self._turbulence = ind.get("turbulence")

    # ------------------------------------------------------------------
    # Strategy protocol
    # ------------------------------------------------------------------

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._leg_type = params.get("leg_type", "strangle")
        self._dte = params.get("dte", 1)
        self._delta = params["delta"]
        self._sl_pct = params["stop_loss_pct"]
        self._tp_pct = params["take_profit_pct"]
        raw_time = params.get("entry_time", "15:00")
        h, m = (int(x) for x in raw_time.split(":"))
        self._entry_hour = h
        self._entry_minute = m
        self._max_hold_hours = params.get("max_hold_hours", 0)
        self._skip_weekends = params.get("skip_weekends", 0)
        self._turbulence_threshold = params.get("turbulence_threshold", 50)
        self._min_otm_pct = params.get("min_otm_pct", 0)
        self._dyn_target_premium = params.get("dyn_target_premium", 0.0)
        self._max_quantity = params.get("max_quantity", 10.0)
        self._leg_min_price = params.get("leg_min_price", 0.0002)
        self._min_desired_premium = params.get("min_desired_premium", 0.0)
        self._step_up_max_delta = params.get("step_up_max_delta", 0.0)
        self._max_concurrent = self._dte + 1
        self._positions = []
        self._last_trade_date = None
        self._watch_start = None
        self._pos_counter = 0

        self._exit_conditions = [
            stop_loss_pct(self._sl_pct),
        ]
        if self._max_hold_hours > 0:
            self._exit_conditions.append(max_hold_hours(self._max_hold_hours))

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        # ── Exits ──────────────────────────────────────────────────
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
            if reason and reason != "expiry":
                expiry = pos.metadata["expiry"]
                leg_type = pos.metadata["leg_type"]
                if leg_type == "strangle":
                    if (state.get_option(expiry, pos.metadata["call_strike"], True) is None
                            or state.get_option(expiry, pos.metadata["put_strike"], False) is None):
                        reason = None  # data gap — retry next tick
                else:
                    is_call = (leg_type == "call")
                    strike = pos.metadata["call_strike"] if is_call else pos.metadata["put_strike"]
                    if state.get_option(expiry, strike, is_call) is None:
                        reason = None  # data gap — retry next tick
            if reason:
                trades.append(self._close(state, pos, reason))
                to_close.append(pos)
        for pos in to_close:
            self._positions.remove(pos)

        # ── Entry ──────────────────────────────────────────────────
        if len(self._positions) < self._max_concurrent:
            today = state.dt.date()
            if self._last_trade_date != today:

                if self._skip_weekends and state.dt.weekday() >= 5:
                    pass  # skip Saturday/Sunday

                else:
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

    def reset(self):
        # type: () -> None
        self._positions = []
        self._last_trade_date = None
        self._watch_start = None
        self._pos_counter = 0

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "leg_type":              self._leg_type,
            "dte":                   self._dte,
            "delta":                 self._delta,
            "stop_loss_pct":         self._sl_pct,
            "take_profit_pct":       self._tp_pct,
            "entry_time":            f"{self._entry_hour:02d}:{self._entry_minute:02d}",
            "max_hold_hours":        self._max_hold_hours,
            "skip_weekends":         self._skip_weekends,
            "min_otm_pct":           self._min_otm_pct,
            "turbulence_threshold":  self._turbulence_threshold,
            "dyn_target_premium":    self._dyn_target_premium,
            "max_quantity":          self._max_quantity,
            "leg_min_price":         self._leg_min_price,
            "min_desired_premium":   self._min_desired_premium,
            "step_up_max_delta":     self._step_up_max_delta,
        }

    # ------------------------------------------------------------------
    # Dynamic quantity sizing
    # ------------------------------------------------------------------

    def _compute_quantity(self, premium_per_contract):
        # type: (float) -> float
        """Return the number of contracts to sell for this trade.

        When dyn_target_premium == 0 the strategy behaves like the original
        (fixed quantity of 1).  Otherwise:
            raw_qty  = dyn_target_premium / premium_per_contract
            quantity = min(raw_qty, max_quantity)
            quantity = round to nearest 0.1 (Deribit tick)
            quantity = max(quantity, 0.1)       (Deribit minimum)
        """
        if self._dyn_target_premium <= 0:
            return 1.0
        if premium_per_contract <= 0:
            return 1.0
        raw_qty = self._dyn_target_premium / premium_per_contract
        quantity = min(raw_qty, self._max_quantity)
        quantity = round(quantity, 1)
        quantity = max(quantity, 0.1)
        return quantity

    # ------------------------------------------------------------------
    # Step-up helper
    # ------------------------------------------------------------------

    def _step_up(self, chain, current_option, is_call):
        # type: (list, Any, bool) -> Optional[Any]
        """Walk from current_option toward ATM to find the first strike
        where bid >= min_desired_premium, subject to the step_up_max_delta cap.

        For calls: ATM is at a lower strike → walk descending strikes.
        For puts:  ATM is at a higher strike → walk ascending strikes.

        Returns the current option unchanged if min_desired_premium is 0 (disabled),
        or if the current option already meets the threshold.
        Returns None if no satisfying strike is found within the delta cap.
        """
        # Feature disabled or current option already good enough.
        if self._min_desired_premium <= 0:
            return current_option
        if current_option.bid >= self._min_desired_premium:
            return current_option

        # Build walk direction: nearest candidates first.
        if is_call:
            candidates = sorted(
                [q for q in chain if q.strike < current_option.strike],
                key=lambda q: q.strike, reverse=True,  # highest strike first = nearest
            )
        else:
            candidates = sorted(
                [q for q in chain if q.strike > current_option.strike],
                key=lambda q: q.strike,  # lowest strike first = nearest
            )

        cap = self._step_up_max_delta if self._step_up_max_delta > 0 else 1.0

        for cand in candidates:
            if abs(cand.delta) > cap:
                # Monotonically riskier as we walk in — stop.
                break
            if cand.bid >= self._min_desired_premium:
                return cand

        return None  # nothing found within the delta cap

    # ------------------------------------------------------------------
    # Entry logic
    # ------------------------------------------------------------------

    def _maybe_open(self, state):
        # type: (Any) -> Optional[Trade]
        """Check turbulence gate and open if conditions are met.

        Returns the open Trade (side='open') for the engine to emit open
        fills, or None if no entry occurred this tick.
        """
        dt = state.dt
        today = dt.date()

        # Translate the NYC entry_time (HH:MM) to its UTC equivalent for this date
        # (DST-aware), then apply all logic in UTC.
        entry_utc = to_utc(
            to_nyc(dt).replace(hour=self._entry_hour, minute=self._entry_minute, second=0, microsecond=0)
        )
        entry_utc_mins = entry_utc.hour * 60 + entry_utc.minute
        if dt.hour * 60 + dt.minute < entry_utc_mins:
            return None

        # Record watch start (for reference / metadata) but don't expire it.
        if self._watch_start is None or self._watch_start.date() != today:
            self._watch_start = dt.replace(minute=0, second=0, microsecond=0)

        # Turbulence gate
        if not self._turbulence_ok(dt):
            return None  # wait for next tick

        # All gates passed — try to open
        return self._try_open(state)

    def _turbulence_ok(self, dt):
        # type: (datetime) -> bool
        """Return True if turbulence is below threshold (or data is unavailable)."""
        if self._turbulence is None:
            return True  # no data → fail-open

        hour_ts = dt.replace(minute=0, second=0, microsecond=0)
        try:
            row = self._turbulence.loc[hour_ts]
            composite = row["composite"]
        except KeyError:
            return True  # missing hour → fail-open

        # NaN (weekend / warmup) → fail-open
        try:
            if math.isnan(composite):
                return True
        except TypeError:
            return True

        return float(composite) < self._turbulence_threshold

    # ------------------------------------------------------------------
    # Open / close helpers
    # ------------------------------------------------------------------

    def _try_open(self, state):
        # type: (Any) -> Optional[Trade]
        expiry = select_expiry(state, self._dte)
        if expiry is None:
            return None

        chain = state.get_chain(expiry)
        if not chain:
            return None

        calls = [q for q in chain if q.is_call]
        puts  = [q for q in chain if not q.is_call]
        exp_dt = expiry_dt_utc(expiry, state.dt.tzinfo)

        if self._leg_type == "strangle":
            return self._open_strangle(state, expiry, exp_dt, calls, puts)
        elif self._leg_type == "call":
            return self._open_single(state, expiry, exp_dt, calls, is_call=True)
        else:  # put
            return self._open_single(state, expiry, exp_dt, puts, is_call=False)

    def _open_strangle(self, state, expiry, exp_dt, calls, puts):
        # type: (Any, str, Any, list, list) -> Optional[Trade]
        call = select_by_delta(calls, +self._delta)
        put  = select_by_delta(puts,  -self._delta)
        if call is None or put is None:
            return None

        # Apply min_otm_pct filter (push further OTM if needed).
        if self._min_otm_pct > 0:
            call = _apply_min_otm(calls, call, state.spot, self._min_otm_pct, is_call=True)
            put  = _apply_min_otm(puts,  put,  state.spot, self._min_otm_pct, is_call=False)
            if call is None or put is None:
                return None  # no qualifying strike this tick — skip entry

        # Step-up: if either leg's premium is below min_desired_premium, walk
        # inward toward ATM to find a better-priced strike within the delta cap.
        if self._min_desired_premium > 0:
            call = self._step_up(calls, call, is_call=True)
            put  = self._step_up(puts,  put,  is_call=False)
            if call is None or put is None:
                return None  # no satisfying strike within cap — skip entry

        # Hard price floor: both legs must meet leg_min_price (BTC).
        # When leg_min_price == 0 the check is disabled; fall back to the
        # zero-bid guard so we never trade on stale/bad quotes.
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
        quantity = self._compute_quantity(entry_usd)

        # Per-contract fees (USD).
        fee_call = deribit_fee_per_leg(state.spot, call_usd)
        fee_put  = deribit_fee_per_leg(state.spot, put_usd)

        legs = [
            {"strike": call.strike, "is_call": True,  "expiry": expiry, "side": "sell",
             "qty":   quantity,
             "price_btc":       call.bid,
             "entry_price":     call.bid,
             "entry_price_usd": call_usd,
             "entry_delta":     call.delta,
             "fee_usd_open":    fee_call},
            {"strike": put.strike,  "is_call": False, "expiry": expiry, "side": "sell",
             "qty":   quantity,
             "price_btc":       put.bid,
             "entry_price":     put.bid,
             "entry_price_usd": put_usd,
             "entry_delta":     put.delta,
             "fee_usd_open":    fee_put},
        ]
        pos_id = self._next_pos_id()
        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=legs,
            # Quantity-scaled totals so stop_loss_pct / take_profit ratios stay
            # consistent with _reprice_legs (which scales by leg.qty).
            entry_price_usd=entry_usd * quantity,
            fees_open=(fee_call + fee_put) * quantity,
            metadata={
                "leg_type":      "strangle",
                "target_delta":  self._delta,
                "expiry":        expiry,
                "expiry_dt":     exp_dt,
                "direction":     "sell",
                "call_strike":   call.strike,
                "put_strike":    put.strike,
                "call_delta":    call.delta,
                "put_delta":     put.delta,
                "quantity":      quantity,
                "pos_id":        pos_id,
            },
        )
        self._positions.append(pos)
        self._last_trade_date = state.dt.date()

        # Explicit open Trade — engine emits open fills only (pnl=0).
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

    def _open_single(self, state, expiry, exp_dt, quotes, is_call):
        # type: (Any, str, Any, list, bool) -> Optional[Trade]
        target_delta = +self._delta if is_call else -self._delta
        leg = select_by_delta(quotes, target_delta)
        if leg is None:
            return None

        # Apply min_otm_pct filter.
        if self._min_otm_pct > 0:
            leg = _apply_min_otm(quotes, leg, state.spot, self._min_otm_pct, is_call=is_call)
            if leg is None:
                return None  # no qualifying strike this tick — skip entry

        # Step-up: if premium is below min_desired_premium, walk toward ATM.
        if self._min_desired_premium > 0:
            leg = self._step_up(quotes, leg, is_call=is_call)
            if leg is None:
                return None  # no satisfying strike within cap — skip entry

        # Hard price floor.
        _min_p = self._leg_min_price
        if _min_p > 0:
            if leg.bid < _min_p:
                return None
        elif leg.bid <= 0:
            return None

        entry_usd = leg.bid_usd
        if entry_usd <= 0:
            return None
        quantity   = self._compute_quantity(entry_usd)
        leg_type   = "call" if is_call else "put"
        strike_key = "call_strike" if is_call else "put_strike"
        delta_key  = "call_delta"  if is_call else "put_delta"
        fee_leg    = deribit_fee_per_leg(state.spot, entry_usd)
        legs = [
            {"strike": leg.strike, "is_call": is_call, "expiry": expiry, "side": "sell",
             "qty":   quantity,
             "price_btc":       leg.bid,
             "entry_price":     leg.bid,
             "entry_price_usd": entry_usd,
             "entry_delta":     leg.delta,
             "fee_usd_open":    fee_leg},
        ]
        pos_id = self._next_pos_id()
        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=legs,
            entry_price_usd=entry_usd * quantity,
            fees_open=fee_leg * quantity,
            metadata={
                "leg_type":     leg_type,
                "target_delta": self._delta,
                "expiry":       expiry,
                "expiry_dt":    exp_dt,
                "direction":    "sell",
                strike_key:     leg.strike,
                delta_key:      leg.delta,
                "quantity":     quantity,
                "pos_id":       pos_id,
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
            fees=fee_leg * quantity,
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

    def _check_expiry(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[str]
        return check_expiry(state, pos)

    def _check_take_profit(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[str]
        if self._tp_pct <= 0:
            return None
        leg_type = pos.metadata["leg_type"]
        if leg_type == "strangle":
            return check_take_profit_strangle(state, pos, self._tp_pct)
        # Single leg
        expiry  = pos.metadata["expiry"]
        is_call = (leg_type == "call")
        strike  = pos.metadata["call_strike"] if is_call else pos.metadata["put_strike"]
        q = state.get_option(expiry, strike, is_call)
        if q is None or q.ask <= 0:
            return None
        profit_ratio = (pos.entry_price_usd - q.ask_usd) / max(pos.entry_price_usd, 0.01)
        return "take_profit" if profit_ratio >= self._tp_pct else None

    def _close(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        leg_type = pos.metadata["leg_type"]
        quantity = float(pos.metadata.get("quantity", 1.0))

        if leg_type == "strangle":
            expiry      = pos.metadata["expiry"]
            call_strike = pos.metadata["call_strike"]
            put_strike  = pos.metadata["put_strike"]

            if reason == "expiry":
                call_exit_usd_per = max(0.0, state.spot - call_strike)
                put_exit_usd_per  = max(0.0, put_strike  - state.spot)
                call_exit_btc_per = (call_exit_usd_per / state.spot) if state.spot else 0.0
                put_exit_btc_per  = (put_exit_usd_per  / state.spot) if state.spot else 0.0
                fees_close_per = 0.0
                fee_call_per   = 0.0
                fee_put_per    = 0.0
            else:
                _min_tick_btc = 0.0001
                _min_tick_usd = _min_tick_btc * state.spot
                call_q = state.get_option(expiry, call_strike, True)
                put_q  = state.get_option(expiry, put_strike,  False)
                call_exit_usd_per = (call_q.ask_usd if call_q and call_q.ask > 0 else _min_tick_usd)
                put_exit_usd_per  = (put_q.ask_usd  if put_q  and put_q.ask  > 0 else _min_tick_usd)
                call_exit_btc_per = call_q.ask if call_q and call_q.ask > 0 else _min_tick_btc
                put_exit_btc_per  = put_q.ask  if put_q  and put_q.ask  > 0 else _min_tick_btc
                # Per-contract fees, then scale by quantity afterwards.
                fee_call_per   = deribit_fee_per_leg(state.spot, call_exit_usd_per)
                fee_put_per    = deribit_fee_per_leg(state.spot, put_exit_usd_per)
                fees_close_per = fee_call_per + fee_put_per

            # Annotate legs with BTC exit price and explicit close-fee override.
            for leg in pos.legs:
                if leg["is_call"]:
                    leg["exit_price_btc"] = call_exit_btc_per
                    leg["exit_price_usd"] = call_exit_usd_per
                    leg["fee_btc_close"]  = (fee_call_per / state.spot) if state.spot else 0.0
                else:
                    leg["exit_price_btc"] = put_exit_btc_per
                    leg["exit_price_usd"] = put_exit_usd_per
                    leg["fee_btc_close"]  = (fee_put_per / state.spot) if state.spot else 0.0

            exit_usd_scaled   = (call_exit_usd_per + put_exit_usd_per) * quantity
            fees_close_scaled = fees_close_per * quantity

            trade = close_position(state, pos, reason, exit_usd_scaled, fees_close_scaled)
        else:
            trade = self._close_single_leg(state, pos, reason)

        trade.metadata["leg_type"]             = leg_type
        trade.metadata["dte"]                  = self._dte
        trade.metadata["stop_loss_pct"]        = self._sl_pct
        trade.metadata["take_profit_pct"]      = self._tp_pct
        trade.metadata["max_hold_hours"]       = self._max_hold_hours
        trade.metadata["turbulence_threshold"] = self._turbulence_threshold
        trade.metadata["quantity"]             = quantity
        return trade

    def _close_single_leg(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        leg_type = pos.metadata["leg_type"]
        is_call  = (leg_type == "call")
        expiry   = pos.metadata["expiry"]
        strike   = pos.metadata["call_strike"] if is_call else pos.metadata["put_strike"]
        quantity = float(pos.metadata.get("quantity", 1.0))
        leg      = pos.legs[0]

        if reason == "expiry":
            exit_usd_per = max(0.0, state.spot - strike) if is_call else max(0.0, strike - state.spot)
            exit_btc_per = (exit_usd_per / state.spot) if state.spot else 0.0
            fee_per      = 0.0
        else:
            _min_tick_btc = 0.0001
            _min_tick_usd = _min_tick_btc * state.spot
            q = state.get_option(expiry, strike, is_call)
            exit_usd_per = q.ask_usd if q and q.ask > 0 else _min_tick_usd
            exit_btc_per = q.ask     if q and q.ask > 0 else _min_tick_btc
            fee_per      = deribit_fee_per_leg(state.spot, exit_usd_per)

        leg["exit_price_btc"] = exit_btc_per
        leg["exit_price_usd"] = exit_usd_per
        leg["fee_btc_close"]  = (fee_per / state.spot) if state.spot else 0.0

        return close_position(
            state, pos, reason,
            exit_usd_per * quantity,
            fee_per * quantity,
        )

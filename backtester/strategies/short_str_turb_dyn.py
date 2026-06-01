#!/usr/bin/env python3
"""
short_str_turb_dyn.py — Short N-DTE option (delta-selected) gated by
the Turbulence indicator, with SL + TP + time/expiry exit. Extends
short_strangle_turbulence_tp with dynamic quantity sizing.

Supports three leg configurations via the ``leg_type`` parameter:

    "strangle" — sell one call + one put (default)
    "call"     — sell only the call leg
    "put"      — sell only the put leg

Based on short_generic.py but combines the min_otm_pct filter with a
turbulence gate:

    min_otm_pct — after delta selection, push legs further OTM until they are
                  at least min_otm_pct% away from spot.  0 disables the filter.

    turbulence_threshold — open only when the composite turbulence score (0–100) is
                           BELOW this value.  Default: 50.

Entry logic:
    1. At entry_time (NYC wall-clock, e.g. "15:00"), the strategy starts watching the turbulence indicator.
    2. The NYC time is translated to UTC once; all internal logic runs in UTC.
    3. Each 5-min tick it checks: composite < turbulence_threshold → open immediately.
    4. Watching continues until a trade opens or end of day — no hard cut-off.
    5. If the turbulence score is NaN (weekend, missing data) it is treated as calm
       — entry is allowed immediately.

Everything else (delta-selected legs, TP, SL, max_hold_hours, expiry settlement)
is identical to ShortStrangleDeltaTp.

Indicator dependency:
    turbulence(BTCUSDT 15m) → hourly composite score.
    Pre-computed once before the grid loop by backtester/engine.py via
    the indicator_deps / set_indicators protocol.
"""
import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import parse_expiry_date, expiry_dt_utc, select_expiry
from backtester.indicators import IndicatorDep
from backtester.pricing import deribit_fee_per_leg, EXPIRY_HOUR_UTC
from backtester.strategy_base import (
    OpenPosition, Trade, close_position,
    check_expiry,
    stop_loss_pct, profit_target_pct, max_hold_hours,
)
from market_hours import to_nyc, to_utc


# ------------------------------------------------------------------
# Strategy
# ------------------------------------------------------------------


def _apply_min_otm(chain, selected, spot, min_pct, is_call):
    # type: (list, Any, float, float, bool) -> Optional[Any]
    """If `selected` is within min_pct% of spot, push to the nearest strike
    that satisfies the minimum OTM distance.  Returns None if none exists.

    Call leg: strike must be >= spot * (1 + min_pct/100)
    Put  leg: strike must be <= spot * (1 - min_pct/100)
    """
    factor = min_pct / 100.0
    if is_call:
        floor = spot * (1.0 + factor)
        if selected.strike >= floor:
            return selected  # already far enough out
        candidates = sorted(
            [q for q in chain if q.strike >= floor],
            key=lambda q: q.strike,
        )
    else:
        floor = spot * (1.0 - factor)
        if selected.strike <= floor:
            return selected  # already far enough out
        candidates = sorted(
            [q for q in chain if q.strike <= floor],
            key=lambda q: q.strike, reverse=True,
        )
    return candidates[0] if candidates else None


class ShortStrTurbDyn:
    """Sell N-DTE OTM strangle (delta-selected), gated by Turbulence indicator.

    Entry: time-based window starting at entry_time (NYC wall-clock, e.g. "15:00").
    Opens as soon as the turbulence composite is below turbulence_threshold.

    Exit: take-profit, stop-loss, max hold duration, or expiry settlement.
    """

    name = "short_str_turb_dyn"
    DATE_RANGE = ("2026-01-01", "2026-05-29")
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
        "TP uses raw ask prices. SL and max-hold exits unchanged."
    )

    # Turbulence indicator — pre-computed once per grid run by the engine.
    indicator_deps = [
        IndicatorDep(name="turbulence", symbol="BTCUSDT", interval="15m"),
    ]

    PARAM_GRID = {
        "leg_type":             ["strangle"],
        "dte":                  [1],
        "delta":                [0.05],
        "entry_time":           ["14:00"],   # NYC (US Eastern, EDT) — 18:00 UTC
        "stop_loss_pct":        [4.5],
        "take_profit_pct":      [0.0],
        "max_hold_hours":       [0],
        "skip_weekends":        [1],
        "min_otm_pct":          [2.2],
        "turbulence_threshold": [60],
        "dyn_target_premium":   [800],
        "max_quantity":         [25],
        "leg_min_price":        [0],
    }

    def __init__(self):
        self._positions = []          # type: List[OpenPosition]
        self._leg_type = "strangle"
        self._dte = 1
        self._max_concurrent = 1
        self._delta = 0.15
        self._sl_pct = 4.0
        self._tp_pct = 0.75
        self._entry_hour = 15   # NYC hour (default)
        self._entry_minute = 0   # NYC minute (default)
        self._max_hold_hours = 0
        self._skip_weekends = 0
        self._turbulence_threshold = 50
        self._min_otm_pct = 0
        self._dyn_target_premium = 0.0
        self._max_quantity = 10.0
        self._leg_min_price = 0.0002   # BTC; 0 = disabled
        self._last_trade_date = None  # type: Optional[Any]
        self._watch_start = None      # type: Optional[datetime]  # when watching began
        self._exit_conditions = []
        self._pos_counter = 0         # monotonic pos_id source

        # Turbulence DataFrame (hourly index, "composite" column); None until injected.
        self._turbulence = None       # type: Optional[Any]

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
        self._leg_min_price = params.get("leg_min_price", 0.0002)  # BTC; 0 = disabled
        self._max_concurrent = self._dte + 1
        self._positions = []
        self._last_trade_date = None
        self._watch_start = None
        self._pos_counter = 0

        self._exit_conditions = [
            stop_loss_pct(self._sl_pct, price_mode="mark"),
        ]
        if self._tp_pct > 0:
            self._exit_conditions.append(profit_target_pct(self._tp_pct, price_mode="executable"))
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

    def _next_pos_id(self):
        # type: () -> int
        self._pos_counter += 1
        return self._pos_counter

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "leg_type":           self._leg_type,
            "dte":                self._dte,
            "delta":              self._delta,
            "stop_loss_pct":      self._sl_pct,
            "take_profit_pct":    self._tp_pct,
            "entry_time":         f"{self._entry_hour:02d}:{self._entry_minute:02d}",
            "max_hold_hours":     self._max_hold_hours,
            "skip_weekends":        self._skip_weekends,
            "min_otm_pct":          self._min_otm_pct,
            "turbulence_threshold":  self._turbulence_threshold,
            "dyn_target_premium":    self._dyn_target_premium,
            "max_quantity":          self._max_quantity,
            "leg_min_price":         self._leg_min_price,
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
    # Entry logic
    # ------------------------------------------------------------------

    def _maybe_open(self, state):
        # type: (Any) -> None
        """Check turbulence gate and open if conditions are met."""
        dt = state.dt
        today = dt.date()

        # Translate the NYC entry_time (HH:MM) to its UTC equivalent for this date (DST-aware),
        # then apply all logic in UTC — as the rest of the strategy does.
        entry_utc = to_utc(
            to_nyc(dt).replace(hour=self._entry_hour, minute=self._entry_minute, second=0, microsecond=0)
        )
        entry_utc_mins = entry_utc.hour * 60 + entry_utc.minute
        if dt.hour * 60 + dt.minute < entry_utc_mins:
            return

        # Record watch start (for reference / metadata) but don't expire it
        if self._watch_start is None or self._watch_start.date() != today:
            self._watch_start = dt.replace(minute=0, second=0, microsecond=0)

        # Turbulence gate
        if not self._turbulence_ok(dt):
            return  # wait for next tick

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
        if self._min_otm_pct > 0:
            call = _apply_min_otm(calls, call, state.spot, self._min_otm_pct, is_call=True)
            put  = _apply_min_otm(puts,  put,  state.spot, self._min_otm_pct, is_call=False)
            if call is None or put is None:
                return None  # no qualifying strike this tick — skip entry
        # Price floor: both legs must meet leg_min_price (BTC) before we open.
        # When leg_min_price == 0 the check is disabled; fall back to the
        # standard zero-bid guard so we never trade on stale/bad quotes.
        _min_p = self._leg_min_price
        if _min_p > 0:
            if call.bid < _min_p or put.bid < _min_p:
                return None  # retry next 5-min tick
        elif call.bid <= 0 or put.bid <= 0:
            return None
        call_usd  = call.bid_usd
        put_usd   = put.bid_usd
        entry_usd = call_usd + put_usd
        if entry_usd <= 0:
            return None
        quantity = self._compute_quantity(entry_usd)
        fee_call = deribit_fee_per_leg(state.spot, call_usd)
        fee_put  = deribit_fee_per_leg(state.spot, put_usd)
        legs = [
            {"strike": call.strike, "is_call": True,  "expiry": expiry, "side": "sell",
             "qty": quantity,
             "price_btc":       call.bid,
             "entry_price":     call.bid,
             "entry_price_usd": call_usd,
             "entry_spot":      state.spot,
             "entry_bid":       call.bid,
             "entry_ask":       call.ask,
             "entry_mark":      call.mark,
             "entry_iv":        call.mark_iv,
             "entry_delta":     call.delta,
             "fee_usd_open":    fee_call},
            {"strike": put.strike,  "is_call": False, "expiry": expiry, "side": "sell",
             "qty": quantity,
             "price_btc":       put.bid,
             "entry_price":     put.bid,
             "entry_price_usd": put_usd,
             "entry_spot":      state.spot,
             "entry_bid":       put.bid,
             "entry_ask":       put.ask,
             "entry_mark":      put.mark,
             "entry_iv":        put.mark_iv,
             "entry_delta":     put.delta,
             "fee_usd_open":    fee_put},
        ]
        pos_id = self._next_pos_id()
        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=legs,
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
        if self._min_otm_pct > 0:
            leg = _apply_min_otm(quotes, leg, state.spot, self._min_otm_pct, is_call=is_call)
            if leg is None:
                return None  # no qualifying strike this tick — skip entry
        # Price floor: the leg must meet leg_min_price (BTC) before we open.
        # When leg_min_price == 0 the check is disabled; fall back to zero-bid guard.
        _min_p = self._leg_min_price
        if _min_p > 0:
            if leg.bid < _min_p:
                return None  # retry next 5-min tick
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
             "qty": quantity,
             "price_btc":       leg.bid,
             "entry_price":     leg.bid,
             "entry_price_usd": entry_usd,
             "entry_spot":      state.spot,
             "entry_bid":       leg.bid,
             "entry_ask":       leg.ask,
             "entry_mark":      leg.mark,
             "entry_iv":        leg.mark_iv,
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
                fee_call_per   = 0.0
                fee_put_per    = 0.0
                fees_close_per = 0.0
            else:
                _min_tick_btc = 0.0001
                _min_tick_usd = _min_tick_btc * state.spot
                call_q = state.get_option(expiry, call_strike, True)
                put_q  = state.get_option(expiry, put_strike,  False)
                call_exit_usd_per = (call_q.ask_usd if call_q and call_q.ask > 0 else _min_tick_usd)
                put_exit_usd_per  = (put_q.ask_usd  if put_q  and put_q.ask  > 0 else _min_tick_usd)
                call_exit_btc_per = call_q.ask if call_q and call_q.ask > 0 else _min_tick_btc
                put_exit_btc_per  = put_q.ask  if put_q  and put_q.ask  > 0 else _min_tick_btc
                fee_call_per   = deribit_fee_per_leg(state.spot, call_exit_usd_per)
                fee_put_per    = deribit_fee_per_leg(state.spot, put_exit_usd_per)
                fees_close_per = fee_call_per + fee_put_per

            for leg in pos.legs:
                if leg["is_call"]:
                    leg["exit_price_btc"] = call_exit_btc_per
                    leg["exit_price_usd"] = call_exit_usd_per
                    leg["fee_btc_close"]  = (fee_call_per / state.spot) if state.spot else 0.0
                else:
                    leg["exit_price_btc"] = put_exit_btc_per
                    leg["exit_price_usd"] = put_exit_usd_per
                    leg["fee_btc_close"]  = (fee_put_per / state.spot) if state.spot else 0.0

            trade = close_position(
                state, pos, reason,
                (call_exit_usd_per + put_exit_usd_per) * quantity,
                fees_close_per * quantity,
            )
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

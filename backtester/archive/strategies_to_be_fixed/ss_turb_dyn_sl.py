#!/usr/bin/env python3
"""
ss_turb_dyn_sl.py — Short N-DTE option (delta-selected) gated by the
Turbulence indicator, with USD-denominated SL + TP + time/expiry exit.

Like short_str_turb_dyn but stop-loss and take-profit are expressed as
**dollar amounts on the whole position** (both legs × quantity) rather
than as percentages of the per-contract entry price.

Examples
--------
Position opened: call leg bid 0.0006 BTC, put leg bid 0.0005 BTC,
quantity = 10, spot = 95 000 USD.
  entry_price_usd (per contract) = (0.0006 + 0.0005) × 95 000 = 104.50 USD
  total premium collected        = 104.50 × 10 = 1 045 USD

  stop_loss_usd  = 600  → fires when position-level unrealised loss ≥ $600
  take_profit_usd = 400 → fires when position-level unrealised profit ≥ $400

Setting either to 0 disables that exit.

Everything else — turbulence gate, dynamic quantity sizing, min_otm_pct,
leg configurations (strangle / call / put), skip_weekends, max_hold_hours
— is identical to ShortStrTurbDyn.
"""
import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import expiry_dt_utc, select_expiry
from backtester.indicators import IndicatorDep
from backtester.pricing import deribit_fee_per_leg, EXPIRY_HOUR_UTC
from backtester.strategy_base import (
    OpenPosition, Trade, close_trade,
    check_expiry, close_short_strangle,
    max_hold_hours,
    _reprice_legs,
)


# ------------------------------------------------------------------
# Strategy
# ------------------------------------------------------------------


def _apply_min_otm(chain, selected, spot, min_pct, is_call):
    # type: (list, Any, float, float, bool) -> Optional[Any]
    """Push `selected` further OTM until it is at least min_pct% from spot.
    Returns None if no qualifying strike exists.
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


class SsTurbDynSl:
    """Sell N-DTE OTM strangle (delta-selected), gated by Turbulence indicator.

    SL and TP are dollar amounts on the **whole position** (per-contract
    P&L × quantity).  Set to 0 to disable.

    Entry: time-based window starting at entry_hour UTC; opens as soon as
    the turbulence composite is below turbulence_threshold.

    Exit: stop-loss USD, take-profit USD, max hold duration, or expiry.
    """

    name = "ss_turb_dyn_sl"
    DATE_RANGE = ("2026-01-05", "2026-05-05")
    DESCRIPTION = (
        "Sells a strangle, naked call, or naked put on a Deribit expiry N calendar "
        "days ahead (dte=1/2/3), with legs chosen by target delta. "
        "leg_type controls the structure: 'strangle' = call+put, 'call' = call only, "
        "'put' = put only. "
        "Entry is gated by the Turbulence indicator: opens only when the composite "
        "score (0–100) is below turbulence_threshold. NaN score → open freely. "
        "min_otm_pct pushes delta-selected legs further OTM until they are at least "
        "that percentage from spot (0 = disabled). "
        "stop_loss_usd and take_profit_usd are position-level dollar thresholds "
        "(per-contract P&L × quantity).  0 = disabled."
    )

    # Turbulence indicator — pre-computed once per grid run by the engine.
    indicator_deps = [
        IndicatorDep(name="turbulence", symbol="BTCUSDT", interval="15m"),
    ]

    PARAM_GRID = {
        "leg_type":             ["strangle"],
        "dte":                  [1],
        "delta":                [0.15],
        "entry_hour":           [19],
        "stop_loss_usd":        [500,1000,1500,2000,2500,3000, 3500, 4000, 4500, 5000],
        "take_profit_usd":      [0],
        "max_hold_hours":       [0],
        "skip_weekends":        [1],
        "min_otm_pct":          [2.5],
        "turbulence_threshold": [50, 60],
        "dyn_target_premium":   [600],  # USD; 0 = disabled (qty=1)
        "max_quantity":         [25],
        "leg_min_price":        [0.0001],
    }

    def __init__(self):
        self._positions = []          # type: List[OpenPosition]
        self._leg_type = "strangle"
        self._dte = 1
        self._max_concurrent = 1
        self._delta = 0.15
        self._sl_usd = 0.0
        self._tp_usd = 0.0
        self._entry_hour = 9
        self._max_hold_hours = 0
        self._skip_weekends = 0
        self._turbulence_threshold = 50
        self._min_otm_pct = 0
        self._dyn_target_premium = 0.0
        self._max_quantity = 10.0
        self._leg_min_price = 0.0002
        self._last_trade_date = None  # type: Optional[Any]
        self._watch_start = None      # type: Optional[datetime]
        self._exit_conditions = []

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
        self._sl_usd = params.get("stop_loss_usd", 0.0)
        self._tp_usd = params.get("take_profit_usd", 0.0)
        self._entry_hour = params.get("entry_hour", 9)
        self._max_hold_hours = params.get("max_hold_hours", 0)
        self._skip_weekends = params.get("skip_weekends", 0)
        self._turbulence_threshold = params.get("turbulence_threshold", 50)
        self._min_otm_pct = params.get("min_otm_pct", 0)
        self._dyn_target_premium = params.get("dyn_target_premium", 0.0)
        self._max_quantity = params.get("max_quantity", 10.0)
        self._leg_min_price = params.get("leg_min_price", 0.0002)
        self._max_concurrent = self._dte + 1
        self._positions = []
        self._last_trade_date = None
        self._watch_start = None

        self._exit_conditions = []
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
                reason = self._check_sl_tp(state, pos)
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
                    self._maybe_open(state)

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

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "leg_type":              self._leg_type,
            "dte":                   self._dte,
            "delta":                 self._delta,
            "stop_loss_usd":         self._sl_usd,
            "take_profit_usd":       self._tp_usd,
            "entry_hour":            self._entry_hour,
            "max_hold_hours":        self._max_hold_hours,
            "skip_weekends":         self._skip_weekends,
            "min_otm_pct":           self._min_otm_pct,
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
    # SL / TP — position-level dollar checks
    # ------------------------------------------------------------------

    def _check_sl_tp(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[str]
        """Return 'stop_loss' or 'take_profit' based on position-level P&L in USD.

        current_usd is the per-contract cost to buy back (ask side).
        Multiply by quantity to get the full position value.

        Profit (short premium): entry_price_usd > current_usd
        Loss   (short premium): current_usd > entry_price_usd
        """
        if self._sl_usd <= 0 and self._tp_usd <= 0:
            return None
        current_usd = _reprice_legs(state, pos)
        if current_usd is None:
            return None
        qty = pos.metadata.get("quantity", 1.0)
        # position-level P&L: positive = profit, negative = loss
        pnl_usd = (pos.entry_price_usd - current_usd) * qty
        if self._sl_usd > 0 and -pnl_usd >= self._sl_usd:
            return "stop_loss"
        if self._tp_usd > 0 and pnl_usd >= self._tp_usd:
            return "take_profit"
        return None

    # ------------------------------------------------------------------
    # Entry logic
    # ------------------------------------------------------------------

    def _maybe_open(self, state):
        # type: (Any) -> None
        """Check turbulence gate and open if conditions are met."""
        dt = state.dt
        today = dt.date()

        # Not yet in the watch window
        if dt.hour < self._entry_hour:
            return

        # Record watch start date for reference
        if self._watch_start is None or self._watch_start.date() != today:
            self._watch_start = dt.replace(minute=0, second=0, microsecond=0)

        # Turbulence gate
        if not self._turbulence_ok(dt):
            return  # wait for next tick

        # All gates passed — try to open
        self._try_open(state)

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
        # type: (Any) -> None
        expiry = select_expiry(state, self._dte)
        if expiry is None:
            return

        chain = state.get_chain(expiry)
        if not chain:
            return

        calls = [q for q in chain if q.is_call]
        puts  = [q for q in chain if not q.is_call]
        exp_dt = expiry_dt_utc(expiry, state.dt.tzinfo)

        if self._leg_type == "strangle":
            self._open_strangle(state, expiry, exp_dt, calls, puts)
        elif self._leg_type == "call":
            self._open_single(state, expiry, exp_dt, calls, is_call=True)
        else:  # put
            self._open_single(state, expiry, exp_dt, puts, is_call=False)

    def _open_strangle(self, state, expiry, exp_dt, calls, puts):
        # type: (Any, str, Any, list, list) -> None
        call = select_by_delta(calls, +self._delta)
        put  = select_by_delta(puts,  -self._delta)
        if call is None or put is None:
            return
        if self._min_otm_pct > 0:
            call = _apply_min_otm(calls, call, state.spot, self._min_otm_pct, is_call=True)
            put  = _apply_min_otm(puts,  put,  state.spot, self._min_otm_pct, is_call=False)
            if call is None or put is None:
                return
        _min_p = self._leg_min_price
        if _min_p > 0:
            if call.bid < _min_p or put.bid < _min_p:
                return
        elif call.bid <= 0 or put.bid <= 0:
            return
        call_usd  = call.bid_usd
        put_usd   = put.bid_usd
        entry_usd = call_usd + put_usd
        if entry_usd <= 0:
            return
        quantity = self._compute_quantity(entry_usd)
        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=[
                {"strike": call.strike, "is_call": True,  "expiry": expiry, "side": "sell",
                 "entry_price": call.bid, "entry_price_usd": call_usd, "entry_delta": call.delta},
                {"strike": put.strike,  "is_call": False, "expiry": expiry, "side": "sell",
                 "entry_price": put.bid, "entry_price_usd": put_usd,  "entry_delta": put.delta},
            ],
            entry_price_usd=entry_usd,
            fees_open=deribit_fee_per_leg(state.spot, call_usd) + deribit_fee_per_leg(state.spot, put_usd),
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
            },
        )
        self._positions.append(pos)
        self._last_trade_date = state.dt.date()

    def _open_single(self, state, expiry, exp_dt, quotes, is_call):
        # type: (Any, str, Any, list, bool) -> None
        target_delta = +self._delta if is_call else -self._delta
        leg = select_by_delta(quotes, target_delta)
        if leg is None:
            return
        if self._min_otm_pct > 0:
            leg = _apply_min_otm(quotes, leg, state.spot, self._min_otm_pct, is_call=is_call)
            if leg is None:
                return
        _min_p = self._leg_min_price
        if _min_p > 0:
            if leg.bid < _min_p:
                return
        elif leg.bid <= 0:
            return
        entry_usd = leg.bid_usd
        if entry_usd <= 0:
            return
        quantity   = self._compute_quantity(entry_usd)
        leg_type   = "call" if is_call else "put"
        strike_key = "call_strike" if is_call else "put_strike"
        delta_key  = "call_delta"  if is_call else "put_delta"
        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=[
                {"strike": leg.strike, "is_call": is_call, "expiry": expiry, "side": "sell",
                 "entry_price": leg.bid, "entry_price_usd": entry_usd, "entry_delta": leg.delta},
            ],
            entry_price_usd=entry_usd,
            fees_open=deribit_fee_per_leg(state.spot, entry_usd),
            metadata={
                "leg_type":     leg_type,
                "target_delta": self._delta,
                "expiry":       expiry,
                "expiry_dt":    exp_dt,
                "direction":    "sell",
                strike_key:     leg.strike,
                delta_key:      leg.delta,
                "quantity":     quantity,
            },
        )
        self._positions.append(pos)
        self._last_trade_date = state.dt.date()

    def _check_expiry(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[str]
        return check_expiry(state, pos)

    def _close(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        leg_type = pos.metadata["leg_type"]
        if leg_type == "strangle":
            trade = close_short_strangle(state, pos, reason)
        else:
            trade = self._close_single_leg(state, pos, reason)
        trade.metadata["leg_type"]             = leg_type
        trade.metadata["dte"]                  = self._dte
        trade.metadata["stop_loss_usd"]        = self._sl_usd
        trade.metadata["take_profit_usd"]      = self._tp_usd
        trade.metadata["max_hold_hours"]       = self._max_hold_hours
        trade.metadata["turbulence_threshold"] = self._turbulence_threshold
        # Scale all dollar amounts by quantity (trade helpers return per-contract values)
        qty = pos.metadata.get("quantity", 1.0)
        trade.metadata["quantity"]    = qty
        trade.entry_price_usd        *= qty
        trade.exit_price_usd         *= qty
        trade.fees                   *= qty
        trade.pnl                    *= qty
        return trade

    def _close_single_leg(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        leg_type = pos.metadata["leg_type"]
        is_call  = (leg_type == "call")
        expiry   = pos.metadata["expiry"]
        strike   = pos.metadata["call_strike"] if is_call else pos.metadata["put_strike"]

        if reason == "expiry":
            exit_usd   = max(0.0, state.spot - strike) if is_call else max(0.0, strike - state.spot)
            fees_close = 0.0
        else:
            _min_tick_usd = 0.0001 * state.spot
            q = state.get_option(expiry, strike, is_call)
            exit_usd   = q.ask_usd if q and q.ask > 0 else _min_tick_usd
            fees_close = deribit_fee_per_leg(state.spot, exit_usd)

        return close_trade(state, pos, reason, exit_usd, fees_close)

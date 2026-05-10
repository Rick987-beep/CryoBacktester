#!/usr/bin/env python3
"""
deltaswipswap.py — Long straddle/strangle + dynamic delta hedging via BTC-PERPETUAL.

Opens a long ATM straddle (offset=0) or OTM strangle (offset>0) on the nearest
unexpired Deribit expiry. At entry the combined options delta is offset by a
BTC-PERPETUAL position so the whole structure starts delta-neutral.

As BTC spot moves, the net portfolio delta drifts away from zero:
    spot ↑  →  call_delta ↑, put_delta ↑ (less -ve)  →  net option δ +ve
               →  sell perp to re-neutralise (sell high)
    spot ↓  →  net option δ -ve  →  buy perp (buy low)

Whenever |net_portfolio_delta| >= rehedge_delta, we trade the perp back to zero.
This is classic gamma scalping: the rehedging banks realised gamma P&L, offset
by theta decay on the options and taker fees on each perp trade.

The whole structure (options + perp) is closed at close_hour UTC (same day as
entry). max_hold is a safety-net fallback if the data ends earlier.

Delta source:
    Snapshot quote.delta is used when available (non-zero, non-NaN).
    Falls back to Black-Scholes delta using the most-recently-seen mark IV.

Perp simplifications for backtesting:
    - Perp price == spot (negligible basis on sub-12h horizons)
    - No funding rate (~0.01%/8h — minor for intraday; easy to add later)
    - Taker fee = 0.05% per trade (config.toml fees.perp_taker_rate)

P&L accounting (gross cash-flow model):
    Each perp trade records: cash_flows -= delta_qty * price   (buy = outflow)
    Final unwind records:    cash_flows -= (-perp_qty) * close_spot
    perp_pnl = cash_flows - total_perp_fees
    option_pnl = exit_usd - entry_usd - (option_fees_open + option_fees_close)
    total_pnl = option_pnl + perp_pnl

Trade metadata breakdown (for analysis):
    option_pnl, perp_pnl, perp_trades, perp_fees, perp_qty_pre_close
"""
from typing import Any, Dict, List, Optional, Tuple

from backtester.expiry_utils import parse_expiry_date, nearest_valid_expiry
from backtester.pricing import (
    deribit_fee_per_leg, deribit_perp_fee,
    bs_call_delta, bs_put_delta,
    HOURS_PER_YEAR, EXPIRY_HOUR_UTC,
)
from backtester.strategy_base import (
    OpenPosition, Trade,
    time_window, weekday_only, time_exit, max_hold_hours,
)


# ------------------------------------------------------------------
# Expiry helpers (shared pattern with straddle_strangle)
# ------------------------------------------------------------------


def _hours_to_expiry(current_dt, expiry_code):
    # type: (Any, str) -> float
    """Hours remaining until expiry (08:00 UTC on expiry date)."""
    exp_date = parse_expiry_date(expiry_code)
    if exp_date is None:
        return 0.0
    exp_dt = exp_date.replace(hour=EXPIRY_HOUR_UTC)
    current_naive = current_dt.replace(tzinfo=None)
    return max((exp_dt - current_naive).total_seconds() / 3600.0, 0.0)


# ------------------------------------------------------------------
# Strategy
# ------------------------------------------------------------------

class DeltaSwipSwap:
    """Long straddle/strangle + dynamic delta-hedging via BTC-PERPETUAL.

    Entered daily at entry_hour UTC, closed at close_hour UTC (same day).
    Perp is rebalanced whenever net portfolio delta exceeds rehedge_delta.
    """

    name = "deltaswipswap"
    DATE_RANGE = ("2026-03-09", "2026-03-23")
    DESCRIPTION = (
        "Long ATM straddle or OTM strangle, delta-neutralised at entry via BTC-PERPETUAL. "
        "Perp rebalanced when |net_delta| >= rehedge_delta. "
        "Captures gamma P&L less theta + perp fees. Closed at close_hour UTC."
    )

    PARAM_GRID = {
        "offset":        [1500,2000,2500 ],
        "entry_hour":    [9,10,12, 13, 14,16],
        "close_hour":    [17, 18, 19,20,21,22],
        "rehedge_delta": [0.05, 0.10, 0.20, 0.30],
        "max_hold":      [4, 6, 8,10]
    }

    def __init__(self):
        self._position = None         # type: Optional[OpenPosition]
        self._offset = 0
        self._entry_hour = 9
        self._close_hour = 15
        self._rehedge_delta = 0.10
        self._max_hold = 8
        self._last_trade_date = None  # type: Optional[Any]

        # Perp state — reset for each new trade
        self._perp_qty = 0.0          # net BTC (+ = long, - = short)
        self._perp_cash_flows = 0.0   # gross USD cash in/out across all perp trades
        self._perp_fees_total = 0.0   # accumulated taker fees (USD)
        self._perp_trades = 0         # rehedge count (including entry and close)

        # Cached mark IVs for BS delta fallback (updated each tick from snapshots)
        self._last_iv_call = 0.5      # type: float
        self._last_iv_put = 0.5       # type: float

        self._entry_conditions = []
        self._exit_conditions = []

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._offset = params["offset"]
        self._entry_hour = params["entry_hour"]
        self._close_hour = params["close_hour"]
        self._rehedge_delta = params["rehedge_delta"]
        self._max_hold = params.get("max_hold", 8)
        self._position = None
        self._last_trade_date = None
        self._reset_perp()

        self._entry_conditions = [
            weekday_only(),
            time_window(self._entry_hour, self._entry_hour + 1),
        ]
        self._exit_conditions = [
            time_exit(self._close_hour),
            max_hold_hours(self._max_hold),
        ]

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        if self._position is not None:
            # Check exits first
            reason = self._check_expiry(state)
            if reason is None:
                for cond in self._exit_conditions:
                    reason = cond(state, self._position)
                    if reason:
                        break
            if reason:
                trades.append(self._close(state, reason))
            else:
                # Still open: rehedge if delta has drifted beyond threshold
                self._maybe_rehedge(state)

        if self._position is None:
            today = state.dt.date()
            if self._last_trade_date != today:
                if all(cond(state) for cond in self._entry_conditions):
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
        self._reset_perp()

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "offset":        self._offset,
            "entry_hour":    self._entry_hour,
            "close_hour":    self._close_hour,
            "rehedge_delta": self._rehedge_delta,
            "max_hold":      self._max_hold,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _reset_perp(self):
        # type: () -> None
        self._perp_qty = 0.0
        self._perp_cash_flows = 0.0
        self._perp_fees_total = 0.0
        self._perp_trades = 0
        self._last_iv_call = 0.5
        self._last_iv_put = 0.5

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

    def _get_option_deltas(self, state):
        # type: (Any) -> Tuple[float, float]
        """Return (delta_call, delta_put) from snapshot or BS fallback.

        Snapshot delta is used when non-zero and non-NaN (Deribit convention:
        call delta in (0, 1], put delta in [-1, 0)).
        Falls back to BS delta using the most-recently-cached mark IV.
        """
        pos = self._position
        expiry = pos.metadata["expiry"]
        call_strike = pos.metadata["call_strike"]
        put_strike = pos.metadata["put_strike"]
        T = max(_hours_to_expiry(state.dt, expiry), 0.001) / HOURS_PER_YEAR

        call_q = state.get_option(expiry, call_strike, True)
        if call_q is not None and call_q.delta == call_q.delta and call_q.delta != 0.0:
            delta_call = call_q.delta
            if call_q.mark_iv > 0:
                self._last_iv_call = call_q.mark_iv / 100.0
        else:
            delta_call = bs_call_delta(state.spot, call_strike, T, self._last_iv_call)

        put_q = state.get_option(expiry, put_strike, False)
        if put_q is not None and put_q.delta == put_q.delta and put_q.delta != 0.0:
            delta_put = put_q.delta
            if put_q.mark_iv > 0:
                self._last_iv_put = put_q.mark_iv / 100.0
        else:
            delta_put = bs_put_delta(state.spot, put_strike, T, self._last_iv_put)

        return delta_call, delta_put

    def _trade_perp(self, delta_qty, spot):
        # type: (float, float) -> None
        """Record a perp trade of delta_qty BTC at spot price.

        Cash-flow model: buying costs cash (negative), selling receives cash
        (positive). cash_flows -= delta_qty * price works for both signs.
        Fee is always positive (taker fee on |notional|).
        """
        notional = abs(delta_qty) * spot
        fee = deribit_perp_fee(notional)
        self._perp_cash_flows -= delta_qty * spot
        self._perp_fees_total += fee
        self._perp_qty += delta_qty
        self._perp_trades += 1

    def _maybe_rehedge(self, state):
        # type: (Any) -> None
        """Compute net portfolio delta; rehedge to zero if threshold is breached."""
        delta_call, delta_put = self._get_option_deltas(state)
        net_delta = delta_call + delta_put + self._perp_qty
        if abs(net_delta) >= self._rehedge_delta:
            self._trade_perp(-net_delta, state.spot)

    def _try_open(self, state):
        # type: (Any) -> None
        """Try to open a straddle/strangle and delta-neutralise with a perp."""
        expiry = nearest_valid_expiry(state)
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
        fee_put = deribit_fee_per_leg(state.spot, put.ask_usd)

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
                "offset": self._offset,
                "expiry": expiry,
                "direction": "buy",
                "call_strike": call.strike,
                "put_strike": put.strike,
            },
        )

        # Cache IVs for BS delta fallback
        if call.mark_iv > 0:
            self._last_iv_call = call.mark_iv / 100.0
        if put.mark_iv > 0:
            self._last_iv_put = put.mark_iv / 100.0

        # Delta-neutralise at entry
        T_entry = max(_hours_to_expiry(state.dt, expiry), 0.001) / HOURS_PER_YEAR
        if call.delta == call.delta and call.delta != 0.0:
            delta_call_entry = call.delta
        else:
            delta_call_entry = bs_call_delta(state.spot, call.strike, T_entry, self._last_iv_call)
        if put.delta == put.delta and put.delta != 0.0:
            delta_put_entry = put.delta
        else:
            delta_put_entry = bs_put_delta(state.spot, put.strike, T_entry, self._last_iv_put)

        net_option_delta = delta_call_entry + delta_put_entry
        if abs(net_option_delta) > 1e-6:
            self._trade_perp(-net_option_delta, state.spot)

    def _close(self, state, reason):
        # type: (Any, str) -> Trade
        """Close all options and the perp, compute full P&L."""
        pos = self._position
        expiry = pos.metadata["expiry"]
        call_strike = pos.metadata["call_strike"]
        put_strike = pos.metadata["put_strike"]

        # --- Close options ---
        if reason == "expiry":
            call_intrinsic = max(0.0, state.spot - call_strike)
            put_intrinsic = max(0.0, put_strike - state.spot)
            exit_usd = call_intrinsic + put_intrinsic
            fees_close_options = 0.0
        else:
            call_q = state.get_option(expiry, call_strike, True)
            put_q = state.get_option(expiry, put_strike, False)
            call_bid_usd = call_q.bid_usd if call_q else 0.0
            put_bid_usd = put_q.bid_usd if put_q else 0.0
            if call_bid_usd != call_bid_usd:
                call_bid_usd = 0.0
            if put_bid_usd != put_bid_usd:
                put_bid_usd = 0.0
            exit_usd = call_bid_usd + put_bid_usd
            fees_close_options = (
                deribit_fee_per_leg(state.spot, call_bid_usd)
                + deribit_fee_per_leg(state.spot, put_bid_usd)
            )

        # --- Close perp: unwind entire position at spot ---
        perp_qty_pre_close = self._perp_qty
        if abs(self._perp_qty) > 1e-9:
            close_delta = -self._perp_qty   # reverse: short → buy, long → sell
            self._perp_cash_flows -= close_delta * state.spot
            self._perp_fees_total += deribit_perp_fee(abs(close_delta) * state.spot)
            self._perp_trades += 1

        # --- P&L ---
        option_pnl = exit_usd - pos.entry_price_usd - (pos.fees_open + fees_close_options)
        perp_pnl = self._perp_cash_flows - self._perp_fees_total
        total_pnl = option_pnl + perp_pnl
        total_fees = pos.fees_open + fees_close_options + self._perp_fees_total

        held_s = (state.dt - pos.entry_time).total_seconds()
        trade = Trade(
            entry_time=pos.entry_time,
            exit_time=state.dt,
            entry_spot=pos.entry_spot,
            exit_spot=state.spot,
            entry_price_usd=pos.entry_price_usd,
            exit_price_usd=exit_usd,
            fees=total_fees,
            pnl=total_pnl,
            triggered=False,
            exit_reason=reason,
            exit_hour=int(held_s / 3600),
            entry_date=pos.entry_time.strftime("%Y-%m-%d"),
            metadata={
                **pos.metadata,
                "option_pnl":       round(option_pnl, 4),
                "perp_pnl":         round(perp_pnl, 4),
                "perp_trades":      self._perp_trades,
                "perp_fees":        round(self._perp_fees_total, 4),
                "perp_qty_pre_close": round(perp_qty_pre_close, 6),
                "rehedge_delta":    self._rehedge_delta,
                "close_hour":       self._close_hour,
            },
        )

        self._last_trade_date = pos.entry_time.date()
        self._position = None
        self._reset_perp()
        return trade

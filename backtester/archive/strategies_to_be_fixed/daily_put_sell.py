#!/usr/bin/env python3
"""
daily_put_sell.py — Short OTM put, exit on stop-loss or expiry.

Maps to production's daily_put_sell strategy. Sells a 1DTE OTM put at a
target delta, collects premium, and exits either when the stop-loss
triggers or the option expires.

Grid parameters:
    target_delta  [-0.05, -0.10, -0.15, -0.20, -0.25]  — OTM put delta to target
    stop_loss_pct [0.5, 0.7, 1.0, 1.5, 2.0, 2.5]       — SL as fraction of premium

Pricing modes:
    "real"  — sell at bid, buy back at ask (conservative, default)
    "bs"    — Black-Scholes with snapshot IV
"""
from typing import Any, Dict, List, Optional

from backtester.expiry_utils import parse_expiry_date, expiry_dt_utc
from backtester.pricing import deribit_fee_per_leg, bs_put, HOURS_PER_YEAR, EXPIRY_HOUR_UTC
from backtester.strategy_base import (
    OpenPosition, Trade, close_trade,
    time_window, weekday_only, stop_loss_pct,
)


def _nearest_1dte_expiry(state):
    # type: (Any) -> Optional[str]
    """Find the 1DTE expiry (expires tomorrow at 08:00 UTC).

    Returns the expiry with diff == 1. If tomorrow's expiry is absent from
    the snapshot, logs a warning and returns None (no trade that tick).
    """
    import warnings
    today = state.dt.date()

    best = None
    best_diff = None
    for exp in state.expiries():
        exp_date = parse_expiry_date(exp)
        if exp_date is None:
            continue
        diff = (exp_date.date() - today).days
        if diff >= 1:
            if best_diff is None or diff < best_diff:
                best = exp
                best_diff = diff

    if best is not None and best_diff != 1:
        # Suppress warning at exactly 00:00 UTC: this is the known Tardis daily-file
        # seam where the snapshot carries only 1–4 sparse expiries (same midnight
        # data-gap artifact seen in the option snapshot builder). Entries are
        # correctly skipped; no user action needed.
        if not (state.dt.hour == 0 and state.dt.minute == 0):
            warnings.warn(
                f"No 1DTE expiry at {state.dt.date()}, nearest is {best_diff}DTE ({best}). Skipping entry.",
                stacklevel=2,
            )
        return None
    return best


class DailyPutSell:
    """Sell 1DTE OTM put daily, exit on stop-loss or expiry."""

    name = "daily_put_sell"
    DATE_RANGE = ("2026-03-09", "2026-03-23")
    contracts = 1  # contracts per trade
    DESCRIPTION = (
        "Sells a 1DTE OTM put daily at a target delta. "
        "Exits on stop-loss (fraction of premium received) or at expiry (08:00 UTC). "
        "One trade per day at a configurable UTC entry hour."
    )

    PARAM_GRID = {
        "target_delta": [-0.10, -0.15, -0.20, -0.25, -0.30],
        "stop_loss_pct": [1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5],
        "entry_hour": [2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23], 
    }

    def __init__(self):
        self._positions = []  # type: List[OpenPosition]
        self._target_delta = -0.10
        self._sl_pct = 1.0
        self._entry_hour = 3
        self._pricing_mode = "real"
        self._max_concurrent = 2
        self._entry_conditions = []
        self._exit_conditions = []
        self._trades_today = 0
        self._last_date = None

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._target_delta = params["target_delta"]
        self._sl_pct = params["stop_loss_pct"]
        self._entry_hour = params.get("entry_hour", 3)
        self._pricing_mode = params.get("pricing_mode", "real")
        self._max_concurrent = params.get("max_concurrent", 2)
        self._positions = []
        self._trades_today = 0
        self._last_date = None

        self._entry_conditions = [
            time_window(self._entry_hour, self._entry_hour + 1),
        ]
        self._exit_conditions = [
            stop_loss_pct(self._sl_pct),
        ]

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        # Reset daily counter
        today = state.dt.date()
        if today != self._last_date:
            self._trades_today = 0
            self._last_date = today

        # Check each open position for expiry or stop-loss
        still_open = []
        for pos in self._positions:
            reason = self._check_expiry(state, pos)
            if reason is None:
                for exit_cond in self._exit_conditions:
                    reason = exit_cond(state, pos)
                    if reason:
                        break
            if reason:
                trades.append(self._close(state, pos, reason))
            else:
                still_open.append(pos)
        self._positions = still_open

        # Check entry: max 1 new trade per day, max_concurrent open at once
        if self._trades_today < 1 and len(self._positions) < self._max_concurrent:
            if all(cond(state) for cond in self._entry_conditions):
                self._try_open(state)

        return trades

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        trades = [self._close(state, pos, "end_of_data") for pos in self._positions]
        self._positions = []
        return trades

    def reset(self):
        # type: () -> None
        self._positions = []
        self._trades_today = 0
        self._last_date = None

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "target_delta": self._target_delta,
            "stop_loss_pct": self._sl_pct,
            "entry_hour": self._entry_hour,
        }

    def _check_expiry(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[str]
        """Check if a position's expiry has passed."""
        exp_dt = pos.metadata.get("expiry_dt")
        if exp_dt is None:
            return None
        if state.dt >= exp_dt:
            return "expiry"
        return None

    def _try_open(self, state):
        # type: (Any) -> None
        """Find and sell the OTM put nearest to target delta."""
        expiry = _nearest_1dte_expiry(state)
        if expiry is None:
            return

        chain = state.get_chain(expiry)
        if not chain:
            return

        # Filter to puts with valid delta
        puts = [q for q in chain if not q.is_call and q.delta is not None
                and q.delta < 0]
        if not puts:
            return

        # Find put nearest to target delta
        best = min(puts, key=lambda q: abs(q.delta - self._target_delta))

        if self._pricing_mode == "real":
            # Sell at bid (worst fill for seller)
            entry_usd = best.bid_usd
        else:
            # BS mode
            exp_date = parse_expiry_date(expiry)
            dte_h = (exp_date.replace(hour=EXPIRY_HOUR_UTC) -
                     state.dt.replace(tzinfo=None)).total_seconds() / 3600
            if dte_h <= 0:
                return
            T = dte_h / HOURS_PER_YEAR
            put_iv = best.mark_iv / 100.0
            entry_usd = bs_put(state.spot, best.strike, T, put_iv)

        # Skip if premium too low
        if entry_usd < 1.0:
            return

        fees = deribit_fee_per_leg(state.spot, entry_usd)

        exp_dt = expiry_dt_utc(expiry, state.dt.tzinfo)

        self._positions.append(OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=[{
                "strike": best.strike,
                "is_call": False,
                "expiry": expiry,
                "side": "sell",
                "entry_price": best.bid,
                "entry_price_usd": entry_usd,
            }],
            entry_price_usd=entry_usd,
            fees_open=fees,
            metadata={
                "target_delta": self._target_delta,
                "actual_delta": best.delta,
                "expiry": expiry,
                "expiry_dt": exp_dt,
                "direction": "sell",
                "strike": best.strike,
                "pricing_mode": self._pricing_mode,
            },
        ))
        self._trades_today += 1

    def _close(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        """Close a short put position."""
        leg = pos.legs[0]
        expiry = pos.metadata["expiry"]
        strike = pos.metadata["strike"]

        if reason == "expiry":
            # At expiry: put intrinsic value (what we owe if ITM)
            exit_usd = max(0.0, strike - state.spot)
        elif self._pricing_mode == "real":
            # Buy back at ask (worst fill for buyer).
            # If ask is missing (=0), fall back to mark — never to 0,
            # which would falsely record a windfall profit.
            quote = state.get_option(expiry, strike, is_call=False)
            if quote is None:
                exit_usd = pos.entry_price_usd  # no data: assume flat (no gain/no loss)
            elif quote.ask > 0:
                exit_usd = quote.ask_usd
            else:
                exit_usd = quote.mark_usd
        else:
            # BS mode
            exp_date = parse_expiry_date(expiry)
            dte_h = (exp_date.replace(hour=EXPIRY_HOUR_UTC) -
                     state.dt.replace(tzinfo=None)).total_seconds() / 3600
            dte_h = max(dte_h, 0.001)
            T = dte_h / HOURS_PER_YEAR
            quote = state.get_option(expiry, strike, is_call=False)
            put_iv = (quote.mark_iv / 100.0) if quote else 0.5
            exit_usd = bs_put(state.spot, strike, T, put_iv)

        fees_close = 0.0 if reason == "expiry" else deribit_fee_per_leg(state.spot, exit_usd)

        trade = close_trade(state, pos, reason, exit_usd, fees_close)
        trade.metadata["stop_loss_pct"] = self._sl_pct
        return trade

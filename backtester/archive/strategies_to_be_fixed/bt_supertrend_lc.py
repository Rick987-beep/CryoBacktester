#!/usr/bin/env python3
"""
bt_supertrend_lc.py — Long call gated by SuperTrend(period, multiplier)
on 1h BTCUSDT klines.

Named with the ``bt_`` prefix to disambiguate from the live strategy at
``strategies/supertrend_long_call.py``. Mirrors the live strategy's logic:

  Entry: SuperTrend flips from -1 to +1 on the latest fully-closed 1h bar.
  Exit:  SuperTrend trend == -1 on the latest fully-closed 1h bar.

Option selection:
  - Single leg, BUY CALL.
  - Expiry: nearest available DTE to ``target_dte`` within
            [dte_min, dte_max].
  - Strike: closest delta to ``target_delta`` (~ATM by default).
  - Quantity: 1 contract per trade (engine simulates 1-contract notional).

Execution model (worst-case, no execution-pipeline emulation):
  - Open: BUY at ASK.
  - Close: SELL at BID. Falls back to mark if bid is absent (never zero,
           which would falsify a windfall profit).
  - Fees: deribit_fee_per_leg on each side.

Indicator dependency:
  ``supertrend`` (BTCUSDT 1h) — pre-computed once by backtester/engine.py
  via the indicator_deps / set_indicators protocol. Returns the full
  DataFrame produced by ``indicators.supertrend.supertrend(...)``.
"""
from typing import Any, Dict, List, Optional

import pandas as pd

from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import parse_expiry_date, expiry_dt_utc
from backtester.indicators import IndicatorDep
from backtester.pricing import deribit_fee_per_leg
from backtester.strategy_base import OpenPosition, Trade, close_trade


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _select_expiry_window(state, target_dte, dte_min, dte_max):
    # type: (Any, int, int, int) -> Optional[str]
    """Return the expiry whose DTE is closest to ``target_dte`` within
    [dte_min, dte_max]. None if no qualifying expiry exists.
    """
    today = state.dt.date()
    best = None
    best_diff = None
    for exp in state.expiries():
        exp_date = parse_expiry_date(exp)
        if exp_date is None:
            continue
        dte = (exp_date.date() - today).days
        if dte < dte_min or dte > dte_max:
            continue
        diff = abs(dte - target_dte)
        if best_diff is None or diff < best_diff:
            best = exp
            best_diff = diff
    return best


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

class BtSupertrendLc:
    """Buy a ~target_dte / target_delta call on SuperTrend flip-up;
    close on SuperTrend turning down.
    """

    name = "bt_supertrend_lc"
    DATE_RANGE = ("2025-09-21", "2026-04-21")
    DESCRIPTION = (
        "Long call buyer driven by SuperTrend(period, multiplier) on 1h "
        "BTCUSDT klines. Enters on flip from -1 → +1; exits when trend == -1. "
        "Buys at ASK, sells at BID (worst-case fills). One open position max."
    )

    indicator_deps = [
        IndicatorDep(
            name="supertrend",
            symbol="BTCUSDT",
            interval="1h",
            params={"period": 7, "multiplier": 3.0, "strict_first_cycle": True},
            warmup_days=10,
        ),
    ]

    PARAM_GRID = {
        "target_delta": [0.2, 0.4, 0.50],
        "target_dte":   [7,14,21,30],
        "dte_min":      [7],
        "dte_max":      [60],
    }

    def __init__(self):
        self._positions = []          # type: List[OpenPosition]
        self._target_delta = 0.50
        self._target_dte = 30
        self._dte_min = 7
        self._dte_max = 60
        self._max_concurrent = 1
        self._supertrend = None       # type: Optional[pd.DataFrame]
        self._last_entry_bar = None   # type: Optional[pd.Timestamp]

    # ------------------------------------------------------------------
    # Indicator injection
    # ------------------------------------------------------------------

    def set_indicators(self, ind):
        # type: (Dict[str, Any]) -> None
        self._supertrend = ind.get("supertrend")

    # ------------------------------------------------------------------
    # Strategy protocol
    # ------------------------------------------------------------------

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._target_delta = params.get("target_delta", 0.50)
        self._target_dte = params.get("target_dte", 30)
        self._dte_min = params.get("dte_min", 7)
        self._dte_max = params.get("dte_max", 60)
        self._positions = []
        self._last_entry_bar = None

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        # Evaluate the SuperTrend signal once per closed 1h bar — only at the
        # top of the hour. This matches the live strategy's "latest fully
        # closed bar" semantics and avoids redundant lookups within an hour.
        if state.dt.minute != 0:
            return trades

        sig = self._latest_supertrend_signal(state.dt)

        # ── Exits: trend == -1 on latest closed bar ────────────────
        if sig is not None and sig["trend"] == -1 and self._positions:
            for pos in self._positions:
                trades.append(self._close(state, pos, "trigger"))
            self._positions = []
        # else: hold

        # ── Entry: flip_up on latest closed bar ────────────────────
        if (
            sig is not None
            and sig["flip_up"]
            and len(self._positions) < self._max_concurrent
            and self._last_entry_bar != sig["bar_ts"]
        ):
            self._last_entry_bar = sig["bar_ts"]
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
        self._last_entry_bar = None

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "target_delta": self._target_delta,
            "target_dte":   self._target_dte,
            "dte_min":      self._dte_min,
            "dte_max":      self._dte_max,
        }

    # ------------------------------------------------------------------
    # Indicator lookup
    # ------------------------------------------------------------------

    def _latest_supertrend_signal(self, dt):
        # type: (Any) -> Optional[Dict[str, Any]]
        """Return the indicator row for the most recently closed 1h bar.

        At wall-clock 14:00 UTC, the bar that just closed has open-time 13:00.
        The pre-computed DataFrame is indexed by bar open-time.
        """
        if self._supertrend is None or self._supertrend.empty:
            return None

        # Build a tz-naive Timestamp matching the indicator df's index format.
        # Indicator builder uses load_klines which returns tz-aware UTC index;
        # match that.
        bar_ts = pd.Timestamp(dt).floor("1h") - pd.Timedelta("1h")
        # Ensure same tz handling as the indicator index.
        idx = self._supertrend.index
        if idx.tz is not None and bar_ts.tz is None:
            bar_ts = bar_ts.tz_localize("UTC")
        elif idx.tz is None and bar_ts.tz is not None:
            bar_ts = bar_ts.tz_convert("UTC").tz_localize(None)

        try:
            row = self._supertrend.loc[bar_ts]
        except KeyError:
            return None

        return {
            "bar_ts":    bar_ts,
            "trend":     int(row["trend"]),
            "flip_up":   bool(row["flip_up"]),
            "flip_down": bool(row["flip_down"]),
        }

    # ------------------------------------------------------------------
    # Open / close
    # ------------------------------------------------------------------

    def _try_open(self, state):
        # type: (Any) -> None
        expiry = _select_expiry_window(
            state, self._target_dte, self._dte_min, self._dte_max,
        )
        if expiry is None:
            return

        chain = state.get_chain(expiry)
        if not chain:
            return

        calls = [q for q in chain if q.is_call and q.delta is not None and q.delta > 0]
        if not calls:
            return

        best = select_by_delta(calls, self._target_delta)
        if best is None:
            return

        # Buy at ASK (worst-case long fill). Skip tick if ask absent.
        if best.ask <= 0:
            return
        entry_usd = best.ask_usd
        if entry_usd <= 0:
            return

        fees = deribit_fee_per_leg(state.spot, entry_usd)
        exp_dt = expiry_dt_utc(expiry, state.dt.tzinfo)

        self._positions.append(OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=[{
                "strike": best.strike,
                "is_call": True,
                "expiry": expiry,
                "side": "buy",
                "entry_price": best.ask,
                "entry_price_usd": entry_usd,
                "entry_delta": best.delta,
            }],
            entry_price_usd=entry_usd,
            fees_open=fees,
            metadata={
                "target_delta": self._target_delta,
                "actual_delta": best.delta,
                "target_dte":   self._target_dte,
                "expiry":       expiry,
                "expiry_dt":    exp_dt,
                "direction":    "buy",
                "strike":       best.strike,
            },
        ))

    def _close(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        """Close a long call: sell at BID. Fall back to mark if bid==0.

        On 'end_of_data' / 'expiry' settlements: use call intrinsic value.
        """
        expiry = pos.metadata["expiry"]
        strike = pos.metadata["strike"]

        if reason == "expiry":
            exit_usd = max(0.0, state.spot - strike)
        else:
            quote = state.get_option(expiry, strike, is_call=True)
            if quote is None:
                # No data: assume flat (no gain/no loss).
                exit_usd = pos.entry_price_usd
            elif quote.bid > 0:
                exit_usd = quote.bid_usd
            else:
                # Bid missing — fall back to mark, never zero.
                exit_usd = quote.mark_usd

        fees_close = 0.0 if reason == "expiry" else deribit_fee_per_leg(state.spot, exit_usd)

        trade = close_trade(state, pos, reason, exit_usd, fees_close)
        return trade

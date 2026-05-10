#!/usr/bin/env python3
"""
long_gamma_whitelist.py — Long call (BULL sleeve) or long put (BEAR sleeve)
gated by the Long-Gamma Regime indicator and human-curated calendar whitelists.

Spec: backtester/newstrategy/coincall_signal_schedule_bull/STRATEGY_SPEC.md

Indicator dependency:
    ``long_gamma_regime`` (BTCUSDT 4h) — pre-computed once by the engine via
    the indicator_deps / set_indicators protocol.  Columns consumed:
      bull_armed:      EMA8 > EMA21 regime AND bar-close date in bull whitelist
      bear_armed:      SMA20 < SMA50 regime AND bar-close date in bear whitelist
      ema_cross_up:    entry trigger, BULL sleeve
      ema_cross_down:  exit trigger, BULL sleeve
      rsi_cross_up55:  entry trigger, BEAR sleeve
      rsi_cross_dn45:  exit trigger, BEAR sleeve

Signal pairing (fan-out):
    Pairs are pre-computed in configure() via pair_signals(regime_df, mode).
    Each entry bar independently finds its next exit bar.  The exit is NOT
    consumed — multiple entries before the same exit all receive it (fan-out).
    Entries with no subsequent exit are discarded.

Bar timing:
    The regime DataFrame is indexed by 4h bar-OPEN timestamps (Binance
    convention).  A signal on bar T is confirmed at bar close T+4h.
    on_market_state() therefore acts on the bar that just closed:
      prev_bar_ts = state.dt − 4h
    and checks whether prev_bar_ts is a signal entry or exit bar.

Execution:
    Entry: BUY at ASK (long option — worst-case fill).  Skip if ask == 0.
    Exit:  SELL at BID.  Fall back to mark if bid == 0.
    Close at: min(signal_exit_bar_close, option_expiry_dt).
"""
from typing import Any, Dict, List, Optional

import pandas as pd

from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import parse_expiry_date, expiry_dt_utc
from backtester.indicators import IndicatorDep, pair_signals
from backtester.pricing import deribit_fee_per_leg
from backtester.strategy_base import OpenPosition, Trade, close_trade


class LongGammaWhitelist:
    """Buy a call (BULL sleeve), put (BEAR sleeve), or both simultaneously (BOTH),
    gated by the Long-Gamma Regime indicator and human-curated calendar whitelists.

    mode="BULL"  — only the call sleeve runs
    mode="BEAR"  — only the put sleeve runs
    mode="BOTH"  — both sleeves run concurrently, each with its own entry/exit map
                   and its own per-sleeve delta/DTE target

    Fan-out: no concurrency cap — one new position per entry bar per sleeve.
    """

    name = "long_gamma_whitelist"
    DATE_RANGE = ("2024-10-24", "2026-04-24")
    DESCRIPTION = (
        "Long call (BULL) or long put (BEAR) gated by 4h EMA/RSI regime signals "
        "and human-curated calendar whitelists.  Fan-out: one position per entry bar. "
        "Exits on regime reversal signal or option expiry.  mode=BOTH runs both "
        "sleeves simultaneously, each with independent signals."
    )

    indicator_deps = [
        IndicatorDep(
            name="long_gamma_regime",
            symbol="BTCUSDT",
            interval="4h",
            warmup_days=60,
        ),
    ]

    PARAM_GRID = {
        # mode=BOTH: run both sleeves simultaneously.
        # Grid sweeps delta (0.5–0.8) and DTE (7–15 for bull, 14–27 for bear)
        # to confirm that signal quality dominates over instrument selection.
        # Quant reference params: bull_delta=0.70, bull_dte=11, bear_delta=0.60,
        # bear_dte=21, dte_min=2, dte_max=60.
        "mode":              ["BOTH"],
        "bull_target_delta": [0.5, 0.6, 0.70, 0.8],
        "bull_target_dte":   [7, 11, 15],
        "bear_target_delta": [0.5, 0.60, 0.7, 0.8],
        "bear_target_dte":   [14, 21, 27],
        "dte_min":           [2],
        "dte_max":           [40, 60, 90],
    }

    # Quant's exact combined params — use as a named preset, not a grid sweep.
    QUANT_PARAMS = {
        "mode":             "BOTH",
        "bull_target_delta": 0.70,
        "bull_target_dte":   11,
        "bear_target_delta": 0.60,
        "bear_target_dte":   21,
        "dte_min":           2,
        "dte_max":           60,
    }

    def __init__(self):
        self._positions = []       # type: List[OpenPosition]
        self._mode = "BULL"
        # Single-sleeve params (used when mode != BOTH)
        self._target_delta = 0.70
        self._target_dte = 11
        # Per-sleeve params (used when mode == BOTH; fall back to above for single modes)
        self._bull_target_delta = 0.70
        self._bull_target_dte = 11
        self._bear_target_delta = 0.60
        self._bear_target_dte = 21
        self._dte_min = 2
        self._dte_max = 60
        self._regime_df = None     # type: Optional[pd.DataFrame]
        # For BULL/BEAR single modes: one map.  For BOTH: _bull_map and _bear_map.
        self._entry_exit_map = {}  # type: Dict[pd.Timestamp, pd.Timestamp]
        self._bull_map = {}        # type: Dict[pd.Timestamp, pd.Timestamp]
        self._bear_map = {}        # type: Dict[pd.Timestamp, pd.Timestamp]

    # ------------------------------------------------------------------
    # Indicator injection  (called once before the grid loop)
    # ------------------------------------------------------------------

    def _rebuild_pairs(self):
        # type: () -> None
        """(Re)build signal maps from current _regime_df and _mode.

        - mode BULL/BEAR: populates _entry_exit_map only.
        - mode BOTH:      populates _bull_map and _bear_map; _entry_exit_map is
                          left empty (on_market_state checks the sleeve maps directly).

        Called from both set_indicators() and configure().  Safe when _regime_df
        is None — produces empty maps.
        """
        self._entry_exit_map = {}
        self._bull_map = {}
        self._bear_map = {}

        if self._regime_df is None or self._regime_df.empty:
            return

        if self._mode == "BOTH":
            for entry_ts, exit_ts in pair_signals(self._regime_df, "BULL"):
                self._bull_map[entry_ts] = exit_ts
            for entry_ts, exit_ts in pair_signals(self._regime_df, "BEAR"):
                self._bear_map[entry_ts] = exit_ts
        else:
            for entry_ts, exit_ts in pair_signals(self._regime_df, self._mode):
                self._entry_exit_map[entry_ts] = exit_ts

    def set_indicators(self, ind):
        # type: (Dict[str, Any]) -> None
        self._regime_df = ind.get("long_gamma_regime")
        self._rebuild_pairs()  # indicators are now available; build the map

    # ------------------------------------------------------------------
    # Strategy protocol
    # ------------------------------------------------------------------

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._mode = params.get("mode", "BULL").upper()
        # Single-sleeve params
        self._target_delta = params.get("target_delta", 0.70)
        self._target_dte = params.get("target_dte", 11)
        # Per-sleeve params for BOTH mode; fall back to single-sleeve values
        self._bull_target_delta = params.get("bull_target_delta", self._target_delta)
        self._bull_target_dte   = params.get("bull_target_dte",   self._target_dte)
        self._bear_target_delta = params.get("bear_target_delta", params.get("target_delta", 0.60))
        self._bear_target_dte   = params.get("bear_target_dte",   params.get("target_dte",   21))
        self._dte_min = params.get("dte_min", 2)
        self._dte_max = params.get("dte_max", 60)
        self._positions = []
        # Rebuild pairs: if set_indicators was already called (e.g. in tests), this
        # uses the live _regime_df.  In the engine, _regime_df is still None here
        # (configure is called before set_indicators), so the maps start empty and
        # get filled when set_indicators fires _rebuild_pairs().
        self._rebuild_pairs()

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        # Only evaluate at 4h bar boundaries.
        if state.dt.minute != 0 or state.dt.hour % 4 != 0:
            return trades

        active_map   = self._entry_exit_map if self._mode != "BOTH" else {}
        has_anything = (
            self._positions
            or active_map
            or self._bull_map
            or self._bear_map
        )
        if not has_anything:
            return trades

        # The 4h bar that just CLOSED has bar_open = state.dt − 4h.
        prev_bar_ts = self._prev_bar_ts(state.dt)

        # ── Exits ─────────────────────────────────────────────────────────
        still_open = []
        for pos in self._positions:
            is_expired = state.dt >= pos.metadata["expiry_dt"]
            is_signal_exit = prev_bar_ts >= pos.metadata["exit_ts"]
            if is_expired or is_signal_exit:
                reason = "expiry" if is_expired else "trigger"
                trades.append(self._close(state, pos, reason))
            else:
                still_open.append(pos)
        self._positions = still_open

        # ── Entries ───────────────────────────────────────────────────────
        if self._mode == "BOTH":
            if prev_bar_ts in self._bull_map:
                self._try_open(state, self._bull_map[prev_bar_ts], sleeve="BULL")
            if prev_bar_ts in self._bear_map:
                self._try_open(state, self._bear_map[prev_bar_ts], sleeve="BEAR")
        else:
            if prev_bar_ts in self._entry_exit_map:
                self._try_open(state, self._entry_exit_map[prev_bar_ts],
                               sleeve=self._mode)

        return trades

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        trades = [self._close(state, pos, "end_of_data") for pos in self._positions]
        self._positions = []
        return trades

    def reset(self):
        # type: () -> None
        self._positions = []

    def describe_params(self):
        # type: () -> Dict[str, Any]
        if self._mode == "BOTH":
            return {
                "mode":             "BOTH",
                "bull_target_delta": self._bull_target_delta,
                "bull_target_dte":   self._bull_target_dte,
                "bear_target_delta": self._bear_target_delta,
                "bear_target_dte":   self._bear_target_dte,
                "dte_min":           self._dte_min,
            }
        return {
            "mode":         self._mode,
            "target_delta": self._target_delta,
            "target_dte":   self._target_dte,
            "dte_min":      self._dte_min,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _prev_bar_ts(self, dt):
        # type: (Any) -> pd.Timestamp
        """Return the bar-open timestamp of the 4h bar that just closed.

        Normalises timezone to match the indicator DataFrame index so that
        dict membership tests work correctly regardless of whether state.dt
        is tz-aware or tz-naive.
        """
        ts = pd.Timestamp(dt) - pd.Timedelta(hours=4)
        if self._regime_df is None or self._regime_df.empty:
            return ts
        idx_tz = self._regime_df.index.tz
        if idx_tz is not None and ts.tz is None:
            ts = ts.tz_localize("UTC")
        elif idx_tz is None and ts.tz is not None:
            ts = ts.tz_convert("UTC").tz_localize(None)
        return ts

    def _select_expiry(self, state, target_dte):
        # type: (Any, int) -> Optional[str]
        """Return the expiry code closest to target_dte within [dte_min, dte_max]."""
        today = state.dt.date()
        best = None
        best_diff = None
        for exp in state.expiries():
            exp_date = parse_expiry_date(exp)
            if exp_date is None:
                continue
            dte = (exp_date.date() - today).days
            if dte < self._dte_min or dte > self._dte_max:
                continue
            diff = abs(dte - target_dte)
            if best_diff is None or diff < best_diff:
                best = exp
                best_diff = diff
        return best

    def _try_open(self, state, exit_ts, sleeve):
        # type: (Any, pd.Timestamp, str) -> None
        """Open a long call (sleeve=BULL) or long put (sleeve=BEAR).

        sleeve is always explicit — either the strategy's own mode (for single-sleeve
        runs) or the specific sleeve that fired (for mode=BOTH).
        """
        # Resolve delta/DTE for this sleeve
        if sleeve == "BULL":
            target_delta = self._bull_target_delta
            target_dte   = self._bull_target_dte
        else:
            target_delta = self._bear_target_delta
            target_dte   = self._bear_target_dte

        expiry = self._select_expiry(state, target_dte)
        if expiry is None:
            return

        chain = state.get_chain(expiry)
        if not chain:
            return

        is_call = (sleeve == "BULL")
        if is_call:
            candidates = [
                q for q in chain
                if q.is_call and q.delta is not None and q.delta > 0
            ]
            selection_delta = +abs(target_delta)
        else:
            candidates = [
                q for q in chain
                if not q.is_call and q.delta is not None and q.delta < 0
            ]
            selection_delta = -abs(target_delta)

        if not candidates:
            return

        best = select_by_delta(candidates, selection_delta)
        if best is None or best.ask <= 0:
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
                "strike":          best.strike,
                "is_call":         is_call,
                "expiry":          expiry,
                "side":            "buy",
                "entry_price":     best.ask,
                "entry_price_usd": entry_usd,
                "entry_delta":     best.delta,
            }],
            entry_price_usd=entry_usd,
            fees_open=fees,
            metadata={
                "mode":         self._mode,
                "sleeve":       sleeve,
                "target_delta": target_delta,
                "actual_delta": best.delta,
                "target_dte":   target_dte,
                "expiry":       expiry,
                "expiry_dt":    exp_dt,
                "direction":    "buy",
                "strike":       best.strike,
                "exit_ts":      exit_ts,
            },
        ))

    def _close(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        """Close the long option: SELL at BID; fall back to mark if bid == 0.
        At expiry: settle at intrinsic value; no close fee charged.
        """
        expiry  = pos.metadata["expiry"]
        strike  = pos.metadata["strike"]
        is_call = (pos.metadata["sleeve"] == "BULL")

        if reason == "expiry":
            if is_call:
                exit_usd = max(0.0, state.spot - strike)
            else:
                exit_usd = max(0.0, strike - state.spot)
            fees_close = 0.0
        else:
            quote = state.get_option(expiry, strike, is_call=is_call)
            if quote is None:
                exit_usd = pos.entry_price_usd     # no market data → flat
            elif quote.bid > 0:
                exit_usd = quote.bid_usd
            else:
                exit_usd = quote.mark_usd          # bid absent → fallback to mark
            fees_close = deribit_fee_per_leg(state.spot, exit_usd)

        return close_trade(state, pos, reason, exit_usd, fees_close)

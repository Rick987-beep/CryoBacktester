#!/usr/bin/env python3
"""
long_gamma_MOVE.py — Two independent long-gamma legs (call + put) with
                     spot-move take-profit and time-based exit.

Strategy overview
-----------------
Manages two independent option slots — one call, one put — each operated
as a standalone long-option trade within the same strategy instance.

Slot eligibility:
  ``direction = "call"``  → only the call slot runs.
  ``direction = "put"``   → only the put slot runs.
  ``direction = "both"``  → both slots run independently; each enters and
                            exits on its own conditions with no coupling.

Entry (per slot, weekdays only):
  1. Entry time falls within [entry_hour_start, entry_hour_end) UTC.
  2. A valid expiry exists for the target DTE (exact date match).
  3. The selected option's mark_iv ≤ iv_max_entry (avoid overpriced sessions).
  4. No open position in this slot today.
  Option is chosen by ``delta_target`` from the expiry's chain.  Buy at ask.

Exit (per slot, fully independent):
  - ``trigger``:   BTC spot (or any 1-min bar excursion) moves ≥ trigger_pct%
                   in the profitable direction (up for calls, down for puts).
  - ``max_hold``:  position has been open for ``max_hold_hours`` hours.
  - ``expiry``:    position is past the expiry deadline → settled at intrinsic.
  - ``end_of_data``: backtester end — force-close at current bid.

No stop loss: max loss per slot = premium paid (full theta decay if no move).

Grid parameters
---------------
direction          "call" | "put" | "both"
dte                1 / 2 / 3  — exact calendar days to expiry at entry
delta_target       target |delta| for option selection (e.g. 0.30 = 30-delta)
entry_hour_start   first UTC hour eligible for entry (inclusive)
entry_hour_end     last  UTC hour eligible for entry (exclusive);
                   special value 0 = exact-hour mode: one attempt at
                   entry_hour_start, skip the entire day if IV gate fails
iv_max_entry       reject entry if selected option's mark_iv exceeds this (%)
trigger_pct        % BTC move required to take profit (e.g. 2.0 = 2 %)
max_hold_hours     close position after N hours regardless of P&L

Research basis: IndicatorBench/research/intraday_options/
  Sweet spot: 09:00–13:00 UTC entry, 1-DTE calls, trigger ~2%, score 1.45–1.77.
"""
import logging
from typing import Any, Dict, List, Optional

from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import parse_expiry_date, expiry_dt_utc, select_expiry
from backtester.pricing import deribit_fee_per_leg
from backtester.strategy_base import (
    OpenPosition, Trade, close_trade,
    weekday_only,
)

log = logging.getLogger(__name__)

# Weekday gate — stateless, built once at import time
_WEEKDAY_GATE = weekday_only()


class LongGammaMove:
    """Two independent long-gamma slots (call / put / both) with move-based TP.

    See module docstring for full description.
    """

    name = "long_gamma_MOVE"
    DATE_RANGE = ("2026-01-02", "2026-05-12")
    DESCRIPTION = (
        "Opens independent call and/or put legs during the London–NY overlap "
        "(configurable UTC window). Each leg is closed when BTC spot moves "
        "trigger_pct% in its profitable direction, or when time_stop_h_before "
        "hours remain before expiry. No stop loss — max loss is the premium paid."
    )

    PARAM_GRID = {
        "direction":           ["call"],
        "dte":                 [1],
        "delta_target":        [0.2, 0.30, 0.4],
        "entry_hour_start":    [4],
        "entry_hour_end":      [0],
        "iv_max_entry":        [90],
        "trigger_pct":         [1.0, 1.2, 1.4, 1.6],
        "max_hold_hours":      [13],
    }

    def __init__(self):
        # type: () -> None
        self._call_pos = None   # type: Optional[OpenPosition]
        self._put_pos = None    # type: Optional[OpenPosition]
        # Track last-entry date per slot so each slot enters at most once/day
        self._call_date = None       # type: Optional[Any]
        self._put_date = None        # type: Optional[Any]
        # For exact-hour mode (entry_hour_end==0): mark day as attempted so we
        # don't retry later in the day even if the IV gate rejected the entry.
        self._call_skip_date = None  # type: Optional[Any]
        self._put_skip_date = None   # type: Optional[Any]

        # Parameters (set by configure)
        self._direction = "both"
        self._dte = 1
        self._delta_target = 0.30
        self._entry_hour_start = 9
        self._entry_hour_end = 13
        self._iv_max_entry = 50.0
        self._trigger_pct = 2.0
        self._max_hold_hours = 6

    # ------------------------------------------------------------------
    # Strategy protocol
    # ------------------------------------------------------------------

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._direction = params.get("direction", "both")
        self._dte = int(params["dte"])
        self._delta_target = float(params["delta_target"])
        self._entry_hour_start = int(params["entry_hour_start"])
        self._entry_hour_end = int(params["entry_hour_end"])
        self._iv_max_entry = float(params["iv_max_entry"])
        self._trigger_pct = float(params["trigger_pct"])
        self._max_hold_hours = float(params["max_hold_hours"])
        self.reset()

    def reset(self):
        # type: () -> None
        self._call_pos = None
        self._put_pos = None
        self._call_date = None
        self._put_date = None
        self._call_skip_date = None
        self._put_skip_date = None

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []
        today = state.dt.date()

        # ── Exit checks — run every tick, regardless of entry window ──────────

        if self._call_pos is not None:
            reason = self._check_exit(state, self._call_pos, is_call=True)
            if reason:
                trades.append(
                    self._close_leg(state, self._call_pos, reason, is_call=True)
                )
                self._call_date = self._call_pos.entry_time.date()
                self._call_pos = None

        if self._put_pos is not None:
            reason = self._check_exit(state, self._put_pos, is_call=False)
            if reason:
                trades.append(
                    self._close_leg(state, self._put_pos, reason, is_call=False)
                )
                self._put_date = self._put_pos.entry_time.date()
                self._put_pos = None

        # ── Entry checks — weekday + time window gate ─────────────────────────

        if not _WEEKDAY_GATE(state):
            return trades

        h = state.dt.hour
        call_enabled = self._direction in ("call", "both")
        put_enabled = self._direction in ("put", "both")

        if self._entry_hour_end == 0:
            # Exact-hour mode: one attempt per day at entry_hour_start.
            # Skip date is set before _try_open so even a failed IV gate
            # blocks any further attempt for the rest of the day.
            if h == self._entry_hour_start:
                if call_enabled and self._call_pos is None \
                        and self._call_date != today \
                        and self._call_skip_date != today:
                    self._call_skip_date = today
                    self._try_open(state, is_call=True)
                if put_enabled and self._put_pos is None \
                        and self._put_date != today \
                        and self._put_skip_date != today:
                    self._put_skip_date = today
                    self._try_open(state, is_call=False)
        else:
            # Window mode: enter on first eligible tick within [start, end).
            if not (self._entry_hour_start <= h < self._entry_hour_end):
                return trades
            if call_enabled and self._call_pos is None and self._call_date != today:
                self._try_open(state, is_call=True)
            if put_enabled and self._put_pos is None and self._put_date != today:
                self._try_open(state, is_call=False)

        return trades

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        trades = []
        if self._call_pos is not None:
            trades.append(
                self._close_leg(state, self._call_pos, "end_of_data", is_call=True)
            )
            self._call_pos = None
        if self._put_pos is not None:
            trades.append(
                self._close_leg(state, self._put_pos, "end_of_data", is_call=False)
            )
            self._put_pos = None
        return trades

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "direction": self._direction,
            "dte": self._dte,
            "delta_target": self._delta_target,
            "entry_hour_start": self._entry_hour_start,
            "entry_hour_end": self._entry_hour_end,
            "iv_max_entry": self._iv_max_entry,
            "trigger_pct": self._trigger_pct,
            "max_hold_hours": self._max_hold_hours,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _try_open(self, state, is_call):
        # type: (Any, bool) -> None
        """Select and open a single-leg long option. No-op if no valid quote."""
        expiry = select_expiry(state, self._dte)
        if expiry is None:
            return

        chain = state.get_chain(expiry)
        legs = [q for q in chain if q.is_call is is_call]
        if not legs:
            return

        opt = select_by_delta(legs, self._delta_target)
        if opt is None:
            return

        # IV gate: reject sessions where IV is elevated
        if opt.mark_iv > self._iv_max_entry:
            return

        # Skip if no executable ask
        if opt.ask <= 0:
            return

        entry_usd = opt.ask_usd
        if entry_usd <= 0 or entry_usd != entry_usd:  # 0 or NaN
            return

        fee = deribit_fee_per_leg(state.spot, entry_usd)
        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=[{
                "strike": opt.strike,
                "is_call": is_call,
                "expiry": expiry,
                "side": "buy",
                "entry_price": opt.ask,
                "entry_price_usd": entry_usd,
            }],
            entry_price_usd=entry_usd,
            fees_open=fee,
            metadata={
                "direction": "buy",
                "expiry": expiry,
                "strike": opt.strike,
                "is_call": is_call,
                "delta_at_entry": opt.delta,
                "mark_iv_entry": opt.mark_iv,
                "trigger_pct": self._trigger_pct,
                "max_hold_hours": self._max_hold_hours,
            },
        )

        if is_call:
            self._call_pos = pos
            log.debug(
                "CALL opened  %s  strike=%.0f  ask_usd=%.1f  iv=%.1f%%",
                state.dt, opt.strike, entry_usd, opt.mark_iv,
            )
        else:
            self._put_pos = pos
            log.debug(
                "PUT  opened  %s  strike=%.0f  ask_usd=%.1f  iv=%.1f%%",
                state.dt, opt.strike, entry_usd, opt.mark_iv,
            )

    def _check_exit(self, state, pos, is_call):
        # type: (Any, OpenPosition, bool) -> Optional[str]
        """Return an exit reason string, or None to keep holding."""
        expiry = pos.metadata["expiry"]

        # 1. Expiry: past the Deribit 08:00 UTC deadline
        exp_dt = expiry_dt_utc(expiry, state.dt.tzinfo)
        if exp_dt is not None and state.dt >= exp_dt:
            return "expiry"

        # 2. Max hold: position has been open long enough
        held_hours = (state.dt - pos.entry_time).total_seconds() / 3600.0
        if held_hours >= self._max_hold_hours:
            return "max_hold"

        # 3. Move trigger: BTC excursion in the profitable direction
        trigger_dist = pos.entry_spot * self._trigger_pct / 100.0
        if is_call:
            up_target = pos.entry_spot + trigger_dist
            # Check 5-min close first (fast path)
            if state.spot >= up_target:
                return "trigger"
            # Check intra-bar excursion via 1-min bars
            for bar in state.spot_bars:
                if bar.high >= up_target:
                    return "trigger"
        else:
            dn_target = pos.entry_spot - trigger_dist
            if state.spot <= dn_target:
                return "trigger"
            for bar in state.spot_bars:
                if bar.low <= dn_target:
                    return "trigger"

        return None

    def _close_leg(self, state, pos, reason, is_call):
        # type: (Any, OpenPosition, str, bool) -> Trade
        """Build a Trade record for closing a single leg."""
        expiry = pos.metadata["expiry"]
        strike = pos.metadata["strike"]

        if reason == "expiry":
            # Settled at intrinsic value; no closing fee
            if is_call:
                exit_usd = max(0.0, state.spot - strike)
            else:
                exit_usd = max(0.0, strike - state.spot)
            fees_close = 0.0
        else:
            # Close at current bid (conservative fill)
            q = state.get_option(expiry, strike, is_call)
            if q is not None and q.bid > 0:
                exit_usd = q.bid_usd
            else:
                exit_usd = 0.0
            fees_close = deribit_fee_per_leg(state.spot, exit_usd) if exit_usd > 0 else 0.0

        trade = close_trade(state, pos, reason, exit_usd, fees_close)
        trade.metadata["slot"] = "call" if is_call else "put"
        return trade

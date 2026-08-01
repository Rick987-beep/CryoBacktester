#!/usr/bin/env python3
"""
str_volburst_pullback.py — Strangle on volatility-regime signals.

Strategy overview
-----------------
Buys a short-dated OTM strangle (independent call + put legs) when one of
two regime signals fires on the 1h BTCUSDT chart:

  ``pullback``   A 4h trend reversal pullback into the direction (bull
                 or bear) detected via ret_4h / ret_1h divergence, combined
                 with adequate realised-volatility rank (rv_rank ≥ 0.60).
                 Uses DTE=2 / δ=0.30 legs for a two-day holding window.

  ``vol_burst``  A short-term volume spike (vol_z ≥ 1.5) signalling
                 abnormal market participation, also gated on rv_rank ≥ 0.60.
                 Uses DTE=1 / δ=0.35 legs for a one-day scalp window.

Both tiers are suppressed when rv_rank has stayed below 0.35 for ≥ 12
consecutive hours (stand_aside), and a 4-hour cooldown prevents re-entry
immediately after a trade closes.

Each strategy instance handles exactly one tier (controlled by the ``tier``
grid parameter).  Two independent option legs (call + put at different OTM
strikes) are opened simultaneously as a strangle.

Exit priority (checked every bar):
  1. expiry_stop  — ≤ 1h before option expiry (Deribit 08:00 UTC deadline)
  2. take_profit  — current strangle value ≥ entry_cost × tp_x (if set)
  3. stop_loss    — current strangle value ≤ entry_cost × sl_x (if set)
  4. time_stop    — position held ≥ ts_h hours

Grid parameters (150 combos = 2 × 5 × 5 × 3)
----------------------------------------------
tier     "pullback" | "vol_burst"
tp_x     None | 1.3 | 1.5 | 2.0 | 2.5   (take-profit multiplier on entry cost)
ts_h     4 | 8 | 12 | 16 | 20            (time-stop in hours)
sl_x     None | 0.3 | 0.5               (stop-loss floor as fraction of entry cost)

Availability filter (entry guard)
----------------------------------
  ask_usd ≥ $75 per leg
  (ask − bid) / ask ≤ 30% per leg
  hours_to_expiry ≥ 4h at entry

Research basis: backtester/planning/STRATEGY_SPEC.md
"""
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import expiry_dt_utc, select_expiry
from backtester.pricing import deribit_fee_per_leg
from backtester.strategy_base import OpenPosition, Trade, _reprice_legs, close_trade
from backtester.indicators import IndicatorDep

log = logging.getLogger(__name__)


class StrVolBurstPullback:
    """OTM strangle on pullback / vol-burst regime signals.

    See module docstring for full description.
    """

    name = "Str_VolBurst_Pullback"
    DATE_RANGE = ("2026-01-02", "2026-05-12")
    DESCRIPTION = (
        "Buys an OTM strangle (call + put at independent strikes) when a "
        "pullback or vol-burst regime signal fires on the 1h BTCUSDT chart, "
        "gated on rv_rank ≥ 0.60 and a 4-hour cooldown between entries."
    )

    PARAM_GRID = {
        "tier": ["pullback", "vol_burst"],
        "tp_x": [0, 1.3, 1.5, 2.0, 2.5],
        "ts_h": [4, 8, 12, 16, 20],
        "sl_x": [0, 0.3, 0.5],
    }

    indicator_deps = [
        IndicatorDep(name="vol_burst_pullback", symbol="BTCUSDT", interval="1h", warmup_days=35),
    ]

    # Per-tier option selection parameters
    _TIER_DTE: Dict[str, int] = {"pullback": 2, "vol_burst": 1}
    _TIER_DELTA: Dict[str, float] = {"pullback": 0.30, "vol_burst": 0.35}
    _SIGNAL_COL: Dict[str, str] = {
        "pullback": "pullback_signal",
        "vol_burst": "vol_burst_signal",
    }

    _COOLDOWN_H: float = 4.0
    _EXPIRY_STOP_H: float = 1.0          # close when ≤ this many hours to expiry
    _MIN_HOURS_TO_EXPIRY: float = 4.0    # skip expiry if < this at entry
    _MIN_ASK_USD: float = 75.0           # minimum option ask in USD
    _MAX_SPREAD_PCT: float = 0.30        # maximum (ask-bid)/ask spread

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def __init__(self) -> None:
        self._position: Optional[OpenPosition] = None
        self._last_fire_ts: Optional[datetime] = None
        self._signals: Optional[pd.DataFrame] = None

        # Parameters (set by configure)
        self._tier: str = "pullback"
        self._dte: int = 2
        self._delta: float = 0.30
        self._tp_x: Optional[float] = None
        self._ts_h: float = 20.0
        self._sl_x: Optional[float] = None

    def configure(self, params: Dict[str, Any]) -> None:
        self._tier = params["tier"]
        self._dte = self._TIER_DTE[self._tier]
        self._delta = self._TIER_DELTA[self._tier]
        tp_x = params.get("tp_x", 0)
        self._tp_x = float(tp_x) if tp_x else None
        self._ts_h = float(params["ts_h"])
        sl_x = params.get("sl_x", 0)
        self._sl_x = float(sl_x) if sl_x else None
        self.reset()

    def reset(self) -> None:
        self._position = None
        self._last_fire_ts = None

    def set_indicators(self, ind: Dict[str, Any]) -> None:
        self._signals = ind.get("vol_burst_pullback")

    def describe_params(self) -> Dict[str, Any]:
        return {
            "tier": self._tier,
            "dte": self._dte,
            "delta": self._delta,
            "tp_x": self._tp_x,
            "ts_h": self._ts_h,
            "sl_x": self._sl_x,
        }

    # ------------------------------------------------------------------
    # Strategy protocol
    # ------------------------------------------------------------------

    def on_market_state(self, state: Any) -> List[Trade]:
        trades: List[Trade] = []

        # ── Exit check — every bar ───────────────────────────────────────────
        if self._position is not None:
            reason = self._check_exit(state, self._position)
            if reason:
                trades.append(self._close_strangle(state, self._position, reason))
                self._position = None

        # ── Entry check — only at top of hour ────────────────────────────────
        if state.dt.minute != 0:
            return trades

        # Already holding — no pyramiding
        if self._position is not None:
            return trades

        if self._signals is None:
            return trades

        # Look up the most recently closed 1h bar (one bar behind current time)
        bar_ts = state.dt - timedelta(hours=1)
        try:
            sig = self._signals.loc[bar_ts]
        except KeyError:
            return trades

        # Stand-aside: rv_rank has been depressed for ≥ 12 consecutive bars
        if sig["stand_aside"]:
            return trades

        # Cooldown: must wait ≥ 4h since last entry
        if self._last_fire_ts is not None:
            elapsed_h = (state.dt - self._last_fire_ts).total_seconds() / 3600.0
            if elapsed_h < self._COOLDOWN_H:
                return trades

        # Signal gate
        if not sig[self._SIGNAL_COL[self._tier]]:
            return trades

        # Try to open a strangle; sets self._position on success
        self._try_open(state)

        return trades

    def on_end(self, state: Any) -> List[Trade]:
        trades: List[Trade] = []
        if self._position is not None:
            trades.append(self._close_strangle(state, self._position, "end_of_data"))
            self._position = None
        return trades

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_expiry(self, state: Any):
        """Return (expiry_code, expiry_dt) with ≥ 4h to expiry.

        Tries self._dte first; if not found or too close, tries self._dte + 1.
        Returns (None, None) if no suitable expiry exists.
        Maximum DTE tried is 3 (never dte ≥ 4 per spec).
        """
        for offset in (0, 1):
            dte = self._dte + offset
            if dte >= 4:
                break
            expiry = select_expiry(state, dte)
            if expiry is None:
                continue
            exp_dt = expiry_dt_utc(expiry, state.dt.tzinfo)
            if exp_dt is None:
                continue
            hours_to_expiry = (exp_dt - state.dt).total_seconds() / 3600.0
            if hours_to_expiry >= self._MIN_HOURS_TO_EXPIRY:
                return expiry, exp_dt
        return None, None

    def _try_open(self, state: Any) -> None:
        """Select two OTM legs and open a strangle. No-op if any check fails."""
        expiry, exp_dt = self._find_expiry(state)
        if expiry is None:
            return

        chain = state.get_chain(expiry)
        call_legs = [q for q in chain if q.is_call]
        put_legs = [q for q in chain if not q.is_call]

        call_opt = select_by_delta(call_legs, self._delta)
        put_opt = select_by_delta(put_legs, -self._delta)

        if call_opt is None or put_opt is None:
            return

        # Availability filter: both legs must have executable quotes
        for opt in (call_opt, put_opt):
            if opt.ask <= 0:
                return
            if opt.ask_usd < self._MIN_ASK_USD:
                return
            if (opt.ask - opt.bid) / opt.ask > self._MAX_SPREAD_PCT:
                return

        # Compute entry cost and fees
        call_ask_usd = call_opt.ask_usd
        put_ask_usd = put_opt.ask_usd
        entry_cost_usd = call_ask_usd + put_ask_usd
        fees_open = (
            deribit_fee_per_leg(state.spot, call_ask_usd)
            + deribit_fee_per_leg(state.spot, put_ask_usd)
        )

        # Compute exit targets
        tp_target_usd = entry_cost_usd * self._tp_x if self._tp_x is not None else None
        sl_floor_usd = entry_cost_usd * self._sl_x if self._sl_x is not None else None

        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=[
                {
                    "strike": call_opt.strike,
                    "is_call": True,
                    "expiry": expiry,
                    "side": "buy",
                    "entry_price": call_opt.ask,
                    "entry_price_usd": call_ask_usd,
                },
                {
                    "strike": put_opt.strike,
                    "is_call": False,
                    "expiry": expiry,
                    "side": "buy",
                    "entry_price": put_opt.ask,
                    "entry_price_usd": put_ask_usd,
                },
            ],
            entry_price_usd=entry_cost_usd,
            fees_open=fees_open,
            metadata={
                "direction": "buy",
                "tier": self._tier,
                "expiry": expiry,
                "expiry_dt": exp_dt,
                "ts_h": self._ts_h,
                "tp_target_usd": tp_target_usd,
                "sl_floor_usd": sl_floor_usd,
                "call_strike": call_opt.strike,
                "put_strike": put_opt.strike,
                "delta_target": self._delta,
                "call_delta_entry": getattr(call_opt, "delta", None),
                "put_delta_entry": getattr(put_opt, "delta", None),
                "call_iv_entry": getattr(call_opt, "mark_iv", None),
                "put_iv_entry": getattr(put_opt, "mark_iv", None),
            },
        )

        self._position = pos
        self._last_fire_ts = state.dt
        log.debug(
            "%s OPEN  %s  expiry=%s  call=%.0f  put=%.0f  cost_usd=%.1f",
            self._tier, state.dt, expiry,
            call_opt.strike, put_opt.strike, entry_cost_usd,
        )

    def _check_exit(self, state: Any, pos: OpenPosition) -> Optional[str]:
        """Return exit reason string or None to keep holding.

        Priority order: expiry_stop → take_profit → stop_loss → time_stop.
        """
        # 1. Expiry-stop: close before option settles to avoid overnight risk
        exp_dt = pos.metadata["expiry_dt"]
        expiry_stop_dt = exp_dt - timedelta(hours=self._EXPIRY_STOP_H)
        if state.dt >= expiry_stop_dt:
            return "expiry_stop"

        # 2. Take-profit and stop-loss (require current market price)
        tp_target = pos.metadata.get("tp_target_usd")
        sl_floor = pos.metadata.get("sl_floor_usd")
        if tp_target is not None or sl_floor is not None:
            current_usd = _reprice_legs(state, pos)
            if current_usd is not None:
                if tp_target is not None and current_usd >= tp_target:
                    return "take_profit"
                if sl_floor is not None and current_usd <= sl_floor:
                    return "stop_loss"

        # 3. Time-stop
        held_h = (state.dt - pos.entry_time).total_seconds() / 3600.0
        if held_h >= pos.metadata["ts_h"]:
            return "time_stop"

        return None

    def _close_strangle(self, state: Any, pos: OpenPosition, reason: str) -> Trade:
        """Build a Trade record for a closing event."""
        current_usd = _reprice_legs(state, pos)
        if current_usd is None:
            current_usd = 0.0

        fees_close = 0.0
        if current_usd > 0:
            for leg in pos.legs:
                q = state.get_option(leg["expiry"], leg["strike"], leg["is_call"])
                if q is not None and q.bid > 0:
                    fees_close += deribit_fee_per_leg(state.spot, q.bid_usd)

        log.debug(
            "%s CLOSE %s  reason=%s  exit_usd=%.1f  entry_usd=%.1f",
            pos.metadata["tier"], state.dt, reason,
            current_usd, pos.entry_price_usd,
        )
        return close_trade(state, pos, reason, current_usd, fees_close)

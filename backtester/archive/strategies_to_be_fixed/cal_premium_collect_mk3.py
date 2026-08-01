#!/usr/bin/env python3
"""
cal_premium_collect_mk3.py — Calendar spread premium collection (mk3 rewrite).

Two fully independent positions: one short leg and one long leg.
Each reopens immediately on the next 5-minute tick after being closed for any
reason (expiry, strike-cross, take-profit, or end-of-data).

Exit triggers
-------------
expiry       — leg settles at intrinsic (zero fee); reopen queued for next tick.
strike_cross — spot crosses the short leg's strike; close BOTH legs, reopen both.
take_profit  — combined (short + long) unrealised P&L >= tp_pct * most-recent
               long entry cost; close BOTH legs, reopen both.

Parameters
----------
sides               "puts" | "calls" | "both"
long_target_delta   target absolute delta for the long leg
short_target_delta  target absolute delta for the short leg
delta_drift_threshold  (unused; kept for grid compat)
long_dte            target DTE for the long leg (~30 days)
short_dte           target DTE for the short leg (~7 days)
tp_pct              TP fraction of long entry cost (0.0 = disabled)
strike_cross        True | False -- enable spot-crosses-strike exit

on_market_state flow (per is_call type)
---------------------------------------
1. Open any queued legs (first tick init + post-close reopens)
2. Handle expiry (each leg checked independently)
3. Check combined triggers (strike-cross + TP)

Exit-reason strings
-------------------
    "take_profit"   -- TP trigger fired
    "strike_cross"  -- spot crossed the short strike
    "expiry"        -- leg settled at intrinsic at expiry
    "end_of_data"   -- forced close at replay end
"""
import logging
from typing import Any, Dict, List, Optional, Set

from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import (
    parse_expiry_date, expiry_dt_utc, select_expiry_for_week,
)
from backtester.pricing import deribit_fee_per_leg
from backtester.strategy_base import (
    OpenPosition,
    Trade,
    close_position,
    _reprice_legs,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Expiry selection helpers
# ---------------------------------------------------------------------------

def _nearest_short_expiry(
    state: Any, target_dte: int, exclude: Optional[str] = None
) -> Optional[str]:
    """Return the available future expiry closest to target_dte DTE.

    DTE >= 1 required.  Optionally exclude one expiry code (e.g. the long
    leg's expiry).  Always returns *some* expiry as long as any future
    expiry exists -- fixes the mid-week gap that caused the flood of
    "no short expiry" warnings when DTE probe was exact-match only.
    """
    best_exp: Optional[str] = None
    best_diff = float("inf")
    today = state.dt.date()
    for exp in state.expiries():
        if exclude is not None and exp == exclude:
            continue
        d = parse_expiry_date(exp)
        if d is None:
            continue
        dte = (d.date() - today).days
        if dte < 1:
            continue
        diff = abs(dte - target_dte)
        if diff < best_diff:
            best_diff = diff
            best_exp = exp
    return best_exp


def _select_long_expiry(
    state: Any, target_dte: int, min_dte: int
) -> Optional[str]:
    """Return the expiry closest to target_dte weeks out with DTE >= min_dte."""
    today = state.dt.date()
    target_week = target_dte // 7
    week_order = [
        target_week,
        target_week + 1,
        target_week - 1,
        target_week + 2,
        target_week - 2,
    ]
    for week in week_order:
        if week < 1:
            continue
        exp = select_expiry_for_week(state, week)
        if exp is None:
            continue
        d = parse_expiry_date(exp)
        if d is None:
            continue
        if (d.date() - today).days >= min_dte:
            return exp
    return None


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

class CalPremiumCollectMk3:
    """Calendar spread: independent short and long legs.

    Short leg: ~short_dte DTE, sell to open, reopen next tick after any close.
    Long  leg: ~long_dte  DTE, buy  to open, reopen next tick after any close.

    Two independent OpenPosition objects (one leg each).  Combined strike-cross
    and TP triggers close both simultaneously.  Expiry is handled per-leg.
    """

    name = "cal_premium_collect_mk3"
    DATE_RANGE = ("2026-01-02", "2026-05-25")
    DESCRIPTION = (
        "Calendar spread: independent short and long legs. "
        "Short reopens next tick after any close. "
        "Combined strike-cross and TP triggers close both legs simultaneously."
    )

    PARAM_GRID = {
        "sides":                 ["puts"],
        "long_target_delta":     [0.20],
        "short_target_delta":    [0.10],
        "delta_drift_threshold": [0.0],
        "long_dte":              [30],
        "short_dte":             [7],
        "tp_pct":                [0.0],
        "strike_cross":          [True],
    }

    def __init__(self) -> None:
        # Per-type position state (keyed by is_call: False=put, True=call)
        self._short: Dict[bool, Optional[OpenPosition]] = {}
        self._long: Dict[bool, Optional[OpenPosition]] = {}
        self._queue_short: Set[bool] = set()   # is_call values queued to open
        self._queue_long: Set[bool] = set()
        self._long_entry_cost: Dict[bool, float] = {}  # latest long open premium (USD)
        # Parameters
        self._sides_list: List[bool] = [False]
        self._long_target_delta: float = 0.20
        self._short_target_delta: float = 0.10
        self._long_dte: int = 30
        self._short_dte: int = 7
        self._tp_pct: float = 0.0
        self._strike_cross: bool = False
        self._id_counter: int = 0

    # ------------------------------------------------------------------
    # Protocol
    # ------------------------------------------------------------------

    def configure(self, params: Dict[str, Any]) -> None:
        sides = params.get("sides", "puts")
        if sides == "calls":
            self._sides_list = [True]
        elif sides == "both":
            self._sides_list = [False, True]
        else:
            self._sides_list = [False]
        self._long_target_delta = float(params["long_target_delta"])
        self._short_target_delta = float(params.get("short_target_delta", 0.10))
        self._long_dte = int(params["long_dte"])
        self._short_dte = int(params["short_dte"])
        self._tp_pct = float(params.get("tp_pct", 0.0))
        self._strike_cross = bool(params.get("strike_cross", False))
        self._short = {ic: None for ic in self._sides_list}
        self._long = {ic: None for ic in self._sides_list}
        self._queue_short = set(self._sides_list)
        self._queue_long = set(self._sides_list)
        self._long_entry_cost = {ic: 0.0 for ic in self._sides_list}
        self._id_counter = 0

    def reset(self) -> None:
        for ic in self._sides_list:
            self._short[ic] = None
            self._long[ic] = None
        self._queue_short = set(self._sides_list)
        self._queue_long = set(self._sides_list)
        self._long_entry_cost = {ic: 0.0 for ic in self._sides_list}
        self._id_counter = 0

    def on_market_state(self, state: Any) -> List[Trade]:
        trades: List[Trade] = []
        for is_call in self._sides_list:
            trades.extend(self._process_side(state, is_call))
        return trades

    def on_end(self, state: Any) -> List[Trade]:
        trades: List[Trade] = []
        for is_call in self._sides_list:
            for pos in (self._short[is_call], self._long[is_call]):
                if pos is not None:
                    trades.append(self._close_at_market(state, pos, "end_of_data"))
            self._short[is_call] = None
            self._long[is_call] = None
        return trades

    def describe_params(self) -> Dict[str, Any]:
        if self._sides_list == [True]:
            sides_str = "calls"
        elif len(self._sides_list) == 2:
            sides_str = "both"
        else:
            sides_str = "puts"
        return {
            "sides":                 sides_str,
            "long_target_delta":     self._long_target_delta,
            "short_target_delta":    self._short_target_delta,
            "delta_drift_threshold": 0.0,
            "long_dte":              self._long_dte,
            "short_dte":             self._short_dte,
            "tp_pct":                self._tp_pct,
            "strike_cross":          self._strike_cross,
        }

    # ------------------------------------------------------------------
    # Per-side processing
    # ------------------------------------------------------------------

    def _process_side(self, state: Any, is_call: bool) -> List[Trade]:
        trades: List[Trade] = []

        # -- Step 1: Open queued legs (first-tick init + post-close reopens) --
        if is_call in self._queue_short:
            t = self._open_short(state, is_call)
            if t is not None:
                trades.append(t)
                self._queue_short.discard(is_call)

        if is_call in self._queue_long:
            t = self._open_long(state, is_call)
            if t is not None:
                trades.append(t)
                self._queue_long.discard(is_call)

        # -- Step 2: Handle expiry (each leg independently) --
        short_pos = self._short[is_call]
        if short_pos is not None:
            exp_dt = short_pos.metadata.get("expiry_dt")
            if exp_dt is not None and state.dt >= exp_dt:
                trades.append(self._settle_at_intrinsic(state, short_pos))
                self._short[is_call] = None
                self._queue_short.add(is_call)

        long_pos = self._long[is_call]
        if long_pos is not None:
            exp_dt = long_pos.metadata.get("expiry_dt")
            if exp_dt is not None and state.dt >= exp_dt:
                trades.append(self._settle_at_intrinsic(state, long_pos))
                self._long[is_call] = None
                self._queue_long.add(is_call)

        # -- Step 3: Combined triggers (re-read after expiry mutations) --
        short_pos = self._short[is_call]
        long_pos = self._long[is_call]
        if short_pos is not None or long_pos is not None:
            reason = self._check_triggers(state, is_call, short_pos, long_pos)
            if reason is not None:
                if short_pos is not None:
                    trades.append(self._close_at_market(state, short_pos, reason))
                    self._short[is_call] = None
                if long_pos is not None:
                    trades.append(self._close_at_market(state, long_pos, reason))
                    self._long[is_call] = None
                self._queue_short.add(is_call)
                self._queue_long.add(is_call)

        return trades

    # ------------------------------------------------------------------
    # Trigger checks
    # ------------------------------------------------------------------

    def _check_triggers(
        self,
        state: Any,
        is_call: bool,
        short_pos: Optional[OpenPosition],
        long_pos: Optional[OpenPosition],
    ) -> Optional[str]:
        # Strike-cross: short strike only; short must be open.
        if self._strike_cross and short_pos is not None:
            short_strike = short_pos.metadata.get("strike")
            if short_strike is not None:
                if is_call and state.spot >= float(short_strike):
                    return "strike_cross"
                if not is_call and state.spot <= float(short_strike):
                    return "strike_cross"

        # Take-profit: both legs must be open.
        if self._tp_pct > 0.0 and short_pos is not None and long_pos is not None:
            long_cost = self._long_entry_cost.get(is_call, 0.0)
            if long_cost > 0.0:
                combined = self._combined_unrealised_pnl(state, short_pos, long_pos)
                if combined is not None and combined >= self._tp_pct * long_cost:
                    return "take_profit"

        return None

    def _combined_unrealised_pnl(
        self,
        state: Any,
        short_pos: OpenPosition,
        long_pos: OpenPosition,
    ) -> Optional[float]:
        """Unrealised P&L (USD): (short received - short reprice) + (long reprice - long paid)."""
        short_cur = _reprice_legs(state, short_pos)
        if short_cur is None:
            return None
        long_cur = _reprice_legs(state, long_pos)
        if long_cur is None:
            return None
        return (short_pos.entry_price_usd - short_cur) + (long_cur - long_pos.entry_price_usd)

    # ------------------------------------------------------------------
    # Open helpers
    # ------------------------------------------------------------------

    def _next_id(self) -> int:
        self._id_counter += 1
        return self._id_counter

    def _open_short(self, state: Any, is_call: bool) -> Optional[Trade]:
        """Sell a new short option. Returns open Trade on success, None on failure."""
        long_pos = self._long.get(is_call)
        long_expiry = long_pos.metadata.get("expiry") if long_pos is not None else None
        short_expiry = _nearest_short_expiry(state, self._short_dte, exclude=long_expiry)
        if short_expiry is None:
            log.debug("cal_premium_collect_mk3: no short expiry at %s", state.dt)
            return None

        chain = state.get_chain(short_expiry)
        if not chain:
            return None

        candidates = [q for q in chain if q.is_call == is_call]
        target_d = self._short_target_delta if is_call else -self._short_target_delta
        opt = select_by_delta(candidates, target_d)
        if opt is None or opt.bid <= 0:
            return None

        entry_usd = opt.bid_usd
        fee = deribit_fee_per_leg(state.spot, entry_usd)
        exp_dt = expiry_dt_utc(short_expiry, state.dt.tzinfo)
        pos_id = self._next_id()

        leg: Dict[str, Any] = {
            "strike":          opt.strike,
            "is_call":         is_call,
            "expiry":          short_expiry,
            "side":            "sell",
            "price_btc":       opt.bid,
            "entry_price":     opt.bid,
            "entry_price_usd": entry_usd,
            "qty":             1.0,
            "entry_delta":     opt.delta,
            "layer":           "short",
            "fee_usd_open":    fee,
        }
        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=[leg],
            entry_price_usd=entry_usd,
            fees_open=fee,
            metadata={
                "pos_id":    pos_id,
                "pos_type":  "short",
                "is_call":   is_call,
                "expiry":    short_expiry,
                "expiry_dt": exp_dt,
                "strike":    opt.strike,
            },
        )
        self._short[is_call] = pos
        return Trade(
            entry_time=state.dt,
            exit_time=state.dt,
            entry_spot=state.spot,
            exit_spot=state.spot,
            entry_price_usd=entry_usd,
            exit_price_usd=0.0,
            fees=fee,
            pnl=0.0,
            triggered=False,
            exit_reason="",
            exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={
                "direction": "sell",
                "pos_id":    pos_id,
                "pos_type":  "short",
                "legs":      [leg],
            },
        )

    def _open_long(self, state: Any, is_call: bool) -> Optional[Trade]:
        """Buy a new long option. Returns open Trade on success, None on failure."""
        long_min_dte = max(self._long_dte - 16, 7)
        long_expiry = _select_long_expiry(state, self._long_dte, long_min_dte)
        if long_expiry is None:
            log.debug("cal_premium_collect_mk3: no long expiry at %s", state.dt)
            return None

        chain = state.get_chain(long_expiry)
        if not chain:
            return None

        candidates = [q for q in chain if q.is_call == is_call]
        target_d = self._long_target_delta if is_call else -self._long_target_delta
        opt = select_by_delta(candidates, target_d)
        if opt is None or opt.ask <= 0:
            return None

        entry_usd = opt.ask_usd
        fee = deribit_fee_per_leg(state.spot, entry_usd)
        exp_dt = expiry_dt_utc(long_expiry, state.dt.tzinfo)
        pos_id = self._next_id()

        self._long_entry_cost[is_call] = entry_usd  # track for TP base

        leg: Dict[str, Any] = {
            "strike":          opt.strike,
            "is_call":         is_call,
            "expiry":          long_expiry,
            "side":            "buy",
            "price_btc":       opt.ask,
            "entry_price":     opt.ask,
            "entry_price_usd": entry_usd,
            "qty":             1.0,
            "entry_delta":     opt.delta,
            "layer":           "long",
            "fee_usd_open":    fee,
        }
        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=[leg],
            entry_price_usd=entry_usd,
            fees_open=fee,
            metadata={
                "pos_id":    pos_id,
                "pos_type":  "long",
                "is_call":   is_call,
                "expiry":    long_expiry,
                "expiry_dt": exp_dt,
                "strike":    opt.strike,
            },
        )
        self._long[is_call] = pos
        return Trade(
            entry_time=state.dt,
            exit_time=state.dt,
            entry_spot=state.spot,
            exit_spot=state.spot,
            entry_price_usd=entry_usd,
            exit_price_usd=0.0,
            fees=fee,
            pnl=0.0,
            triggered=False,
            exit_reason="",
            exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={
                "direction": "buy",
                "pos_id":    pos_id,
                "pos_type":  "long",
                "legs":      [leg],
            },
        )

    # ------------------------------------------------------------------
    # Close helpers
    # ------------------------------------------------------------------

    def _settle_at_intrinsic(self, state: Any, pos: OpenPosition) -> Trade:
        """Settle an expired leg at intrinsic value with zero closing fee."""
        leg = pos.legs[0]
        strike = float(leg["strike"])
        is_call = bool(leg["is_call"])
        intrinsic_usd = (
            max(0.0, state.spot - strike) if is_call
            else max(0.0, strike - state.spot)
        )
        leg["exit_price_btc"] = intrinsic_usd / state.spot if state.spot else 0.0
        leg["fee_btc_close"] = 0.0
        return close_position(state, pos, "expiry", fees_close=0.0)

    def _close_at_market(self, state: Any, pos: OpenPosition, reason: str) -> Trade:
        """Close a leg at current market price (ask for shorts, bid for longs)."""
        leg = pos.legs[0]
        quote = state.get_option(leg["expiry"], leg["strike"], leg["is_call"])
        if quote is None:
            leg["exit_price_btc"] = 0.0
            leg["fee_btc_close"] = 0.0
            return close_position(state, pos, reason, fees_close=0.0)

        if leg["side"] == "sell":
            price_btc = float(quote.ask) if quote.ask > 0 else float(quote.mark)
        else:
            price_btc = float(quote.bid) if quote.bid > 0 else float(quote.mark)

        leg["exit_price_btc"] = price_btc
        fee_usd = deribit_fee_per_leg(state.spot, price_btc * state.spot)
        leg["fee_btc_close"] = fee_usd / state.spot if state.spot else 0.0
        return close_position(state, pos, reason, fees_close=fee_usd)

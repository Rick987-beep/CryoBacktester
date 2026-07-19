#!/usr/bin/env python3
"""
strategy_base.py — Strategy protocol, trade dataclasses, and composable
condition helpers.

Defines the contract between strategies and the backtest engine:
  • Trade / OpenPosition dataclasses — carried through the engine and
    consumed by results.py.
  • Strategy protocol — structural typing (configure → on_market_state →
    on_end → reset). Strategies do not need to inherit from anything.
  • Entry condition factories — composable callables (MarketState) → bool:
      time_window, weekday_only, at_interval
  • Exit condition factories — composable callables (MarketState, OpenPosition)
    → Optional[str]:
      stop_loss_pct, profit_target_pct, max_hold_hours, max_hold_days,
      time_exit, index_move_trigger, strike_proximity_stop,
      short_premium_stop_near_expiry, position_quotes_available,
      position_unrealized_pnl, equity_drawdown_stop, exit_expiry_window
  • price_legs(state, pos, mode) — prices all legs of an open position.
    mode controls which price to use:
      "mark"        — exchange model price (stable; use for SL checks)
      "executable"  — bid for buy legs, ask for sell legs (use for TP checks)
      "bid"         — always bid regardless of leg side
      "ask"         — always ask regardless of leg side
    Writes the result to pos._last_reprice_usd so the engine's NAV tracker
    can reuse it without a second call (avoids double reprice per tick).
  • _reprice_legs() — backward-compat alias for price_legs(mode="executable").
  • close_trade() — builds a Trade from an OpenPosition being closed.

Security note: no user-supplied strings are evaluated; all strategy
parameters are plain Python scalars validated by the strategy’s configure().
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from backtester.core.config import cfg as _cfg


# ------------------------------------------------------------------
# Data types
# ------------------------------------------------------------------

@dataclass
class Trade:
    """A trade event (open or close) with P&L accounting.

    For open events: pnl=0.0, exit_time=entry_time, exit_price_usd=0.0,
    fees=opening fees only.  The engine excludes open events from P&L metrics.
    """
    entry_time: datetime
    exit_time: datetime
    entry_spot: float           # BTC spot at entry
    exit_spot: float            # BTC spot at exit
    entry_price_usd: float      # Total premium paid/received (all legs, USD)
    exit_price_usd: float       # Total premium at close (all legs, USD)
    fees: float                 # Deribit fees for this event (USD)
    pnl: float                  # Net P&L after fees (USD); 0.0 for open events
    triggered: bool             # Whether primary exit trigger fired
    exit_reason: str            # "trigger", "time_exit", "max_hold", "expiry", etc.
    exit_hour: int              # Hours held (int, for V1 metrics compat)
    entry_date: str             # "YYYY-MM-DD"
    status: int = 0             # reason code; strategy defines meaning via TRADE_STATUS
    side: str = "close"         # "open" or "close"; engine skips PnL for "open"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OpenPosition:
    """Internal state held by a strategy while a trade is open."""
    entry_time: datetime
    entry_spot: float
    legs: List[Dict[str, Any]]  # [{strike, is_call, expiry, side, qty, entry_price}]
    entry_price_usd: float      # Total premium paid/received (sum of legs)
    fees_open: float            # Entry fees (USD)
    metadata: Dict[str, Any] = field(default_factory=dict)
    # Reprice cache: set by _reprice_legs on each call; read by the engine's
    # _open_unrealized_pnl to avoid a second reprice of the same tick.
    _last_reprice_usd: Optional[float] = field(default=None, repr=False)
    # Per-leg reprice cache (parallel to self.legs list): USD value per leg.
    # Set alongside _last_reprice_usd; used by engine for leg-aware PnL.
    _last_reprice_legs: Optional[List[float]] = field(default=None, repr=False)


# ------------------------------------------------------------------
# Strategy protocol (structural typing — no base class needed)
# ------------------------------------------------------------------
# Any class with these attributes/methods satisfies the protocol.
# We use a runtime-checkable Protocol for type checking, but
# strategies don't need to inherit from anything.

try:
    from typing import Protocol, runtime_checkable
except ImportError:
    # Python 3.7 fallback
    from typing_extensions import Protocol, runtime_checkable


@runtime_checkable
class Strategy(Protocol):
    """Protocol for backtest strategies.

    Lifecycle:
        1. configure(params)         — set parameters for this run
        2. on_market_state(state)    — called each 5-min tick
        3. on_end(state)             — force-close at end of data
        4. reset()                   — clear state for next run
    """

    name: str  # type: str

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        """Set parameters for this backtest run."""
        ...

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        """Process one time step. Return completed trades (if any)."""
        ...

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        """Force-close any open positions at end of data."""
        ...

    def reset(self):
        # type: () -> None
        """Clear internal state between grid runs."""
        ...

    def describe_params(self):
        # type: () -> Dict[str, Any]
        """Return current parameters for result labeling."""
        ...


# ------------------------------------------------------------------
# Type aliases for condition callables
# ------------------------------------------------------------------

# Entry condition: (MarketState) → bool
EntryCondition = Callable  # Callable[[MarketState], bool]

# Exit condition: (MarketState, OpenPosition) → Optional[str]
# Returns None to hold, or a reason string to exit.
ExitCondition = Callable  # Callable[[MarketState, OpenPosition], Optional[str]]


# ------------------------------------------------------------------
# Entry conditions (composable)
# ------------------------------------------------------------------

def time_window(start_hour, end_hour):
    # type: (int, int) -> EntryCondition
    """Allow entry only during UTC hour range [start_hour, end_hour).

    Handles wrap-around (e.g. start=22, end=4 → 22:00–03:59).
    """
    def check(state):
        h = state.dt.hour
        if start_hour <= end_hour:
            return start_hour <= h < end_hour
        else:
            # Wrap-around: e.g. 22–04 means 22,23,0,1,2,3
            return h >= start_hour or h < end_hour
    return check


def weekday_only():
    # type: () -> EntryCondition
    """Block entries on Saturday (5) and Sunday (6)."""
    def check(state):
        return state.dt.weekday() < 5
    return check


def at_interval(minute_offset=0):
    # type: (int) -> EntryCondition
    """Only allow entry at specific minute-of-hour (default: top of hour).

    Use minute_offset=0 for hourly entries, 30 for half-hour, etc.
    """
    def check(state):
        return state.dt.minute == minute_offset
    return check


# ------------------------------------------------------------------
# Exit conditions (composable)
# ------------------------------------------------------------------

def index_move_trigger(distance_usd):
    # type: (float) -> ExitCondition
    """Exit when BTC spot moves >= distance_usd from entry spot.

    Uses 1-min spot bars for intra-bar excursion detection, so a
    spike within a 5-min interval isn't missed.
    """
    def check(state, pos):
        # Check current spot
        excursion = abs(state.spot - pos.entry_spot)
        if excursion >= distance_usd:
            return "trigger"
        # Check 1-min bars for intra-bar spikes
        for bar in state.spot_bars:
            up = abs(bar.high - pos.entry_spot)
            down = abs(pos.entry_spot - bar.low)
            if up >= distance_usd or down >= distance_usd:
                return "trigger"
        return None
    return check


def max_hold_hours(hours):
    # type: (int) -> ExitCondition
    """Force-close after N hours held."""
    def check(state, pos):
        held_s = (state.dt - pos.entry_time).total_seconds()
        if held_s >= hours * 3600:
            return "max_hold"
        return None
    return check


def max_hold_days(days):
    # type: (int) -> ExitCondition
    """Force-close after N calendar days held (day-boundary check at midnight UTC)."""
    def check(state, pos):
        held_days = (state.dt.date() - pos.entry_time.date()).days
        if held_days >= days:
            return "max_hold"
        return None
    return check


def time_exit(hour, minute=0):
    # type: (int, int) -> ExitCondition
    """Hard close at specific UTC wall-clock time (same day as entry)."""
    def check(state, pos):
        if pos.entry_time.date() != state.dt.date():
            return None  # Only fires on entry day
        target_mins = hour * 60 + minute
        current_mins = state.dt.hour * 60 + state.dt.minute
        if current_mins >= target_mins:
            return "time_exit"
        return None
    return check


def _in_proximity_window(state, pos, hours_before_expiry):
    # type: (Any, OpenPosition, float) -> bool
    """True when state.dt is in [expiry_dt - hours, expiry_dt)."""
    if hours_before_expiry <= 0:
        return False
    expiry_dt = pos.metadata.get("expiry_dt")
    if expiry_dt is None:
        return False
    window_start = expiry_dt - timedelta(hours=hours_before_expiry)
    return window_start <= state.dt < expiry_dt


def _strike_breach(spot, pos, buffer_usd):
    # type: (float, OpenPosition, float) -> bool
    """True if spot has crossed a short leg's strike ± buffer_usd."""
    leg_type = pos.metadata.get("leg_type", "strangle")
    if leg_type in ("strangle", "call"):
        call_strike = pos.metadata.get("call_strike")
        if call_strike is not None and spot > float(call_strike) + buffer_usd:
            return True
    if leg_type in ("strangle", "put"):
        put_strike = pos.metadata.get("put_strike")
        if put_strike is not None and spot < float(put_strike) - buffer_usd:
            return True
    return False


def exit_expiry_window(condition, only_final_hours=0.0, except_final_hours=0.0):
    # type: (ExitCondition, float, float) -> ExitCondition
    """Wrap an exit condition with optional expiry-relative time gates.

    Gates are measured against ``pos.metadata['expiry_dt']`` (Deribit 08:00 UTC).

    only_final_hours > 0:
        Inner condition runs ONLY inside [expiry_dt − N hours, expiry_dt).

    except_final_hours > 0:
        Inner condition is SKIPPED inside that same interval (useful to turn
        premium SL off near expiry while a separate proximity rule is active).

    Both 0:
        No gating — inner condition always evaluated.

    If both only_final_hours and except_final_hours are > 0, only_final_hours
    takes precedence (except_final_hours is ignored).
    """
    def check(state, pos):
        if only_final_hours > 0:
            if not _in_proximity_window(state, pos, only_final_hours):
                return None
        elif except_final_hours > 0:
            if _in_proximity_window(state, pos, except_final_hours):
                return None
        return condition(state, pos)
    return check


def stop_loss_pct(pct, price_mode="mark", suppress_hours_before_expiry=0.0):
    # type: (float, str, float) -> ExitCondition
    """Close when unrealized loss exceeds pct (as fraction, e.g. 1.5 = 150% of premium).

    price_mode controls which price is used to evaluate the loss:
      "mark"        — exchange model price (default; stable, not manipulable by wide spreads)
      "executable"  — bid for buy legs, ask for sell legs
      "bid" / "ask" — always that side regardless of leg direction

    suppress_hours_before_expiry — when > 0, premium SL is skipped during the
    final N hours before pos.metadata['expiry_dt'] (for expiry-day proximity SL).

    The default is "mark" because SL decisions should be based on the exchange's
    fair-value estimate, not on bid/ask which can be wide in thin early-morning books.

    Handles both long and short premium via leg 'side' field (per-leg) or
    pos.metadata['direction'] (position-level fallback).
    """
    def check(state, pos):
        if suppress_hours_before_expiry > 0 and _in_proximity_window(
            state, pos, suppress_hours_before_expiry
        ):
            return None
        current_usd = price_legs(state, pos, mode=price_mode)
        if current_usd is None:
            return None
        _ep = pos.entry_price_usd
        _denom = _ep if _ep > 0.01 else 0.01
        if pos.metadata.get("direction") == "sell":
            # Short premium: loss = current cost to buy back exceeds received
            if (current_usd - _ep) / _denom >= pct:
                return "stop_loss"
        else:
            # Long premium: loss = value dropped below entry cost
            if (_ep - current_usd) / _denom >= pct:
                return "stop_loss"
        return None
    return check


def strike_proximity_stop(hours_before_expiry, buffer_usd=0.0):
    # type: (float, float) -> ExitCondition
    """Close short premium when spot breaches strike ± buffer_usd near expiry.

    Active only in the final ``hours_before_expiry`` before pos.metadata['expiry_dt'].
    Set hours_before_expiry=0 to disable.

    Strangle: fires if either call or put strike is breached.
    Single call/put: only the open leg's strike is checked.
    """
    def check(state, pos):
        if hours_before_expiry <= 0:
            return None
        if pos.metadata.get("direction", "sell") != "sell":
            return None
        if not _in_proximity_window(state, pos, hours_before_expiry):
            return None
        if _strike_breach(state.spot, pos, buffer_usd):
            return "strike_proximity_stop"
        return None
    return check


def position_quotes_available(state, pos):
    # type: (Any, OpenPosition) -> bool
    """Return True when all open legs have option quote rows in this snapshot."""
    expiry = pos.metadata.get("expiry")
    if expiry is None:
        return False
    leg_type = pos.metadata.get("leg_type", "strangle")
    if leg_type == "strangle":
        call_strike = pos.metadata.get("call_strike")
        put_strike = pos.metadata.get("put_strike")
        if call_strike is None or put_strike is None:
            return False
        if state.get_option(expiry, call_strike, True) is None:
            return False
        if state.get_option(expiry, put_strike, False) is None:
            return False
        return True
    is_call = leg_type == "call"
    strike = pos.metadata.get("call_strike") if is_call else pos.metadata.get("put_strike")
    if strike is None:
        return False
    return state.get_option(expiry, strike, is_call) is not None


def short_premium_stop_near_expiry(
    sl_pct,
    proximity_hours=0.0,
    proximity_buffer_usd=0.0,
    sl_price_mode="mark",
):
    # type: (float, float, float, str) -> ExitCondition
    """Premium stop-loss with optional expiry-day strike-proximity handoff.

    When ``proximity_hours`` > 0 and the tick is inside the final N hours before
    ``expiry_dt``, premium SL is off and a strike-proximity stop is active instead
    (returns ``strike_proximity_stop``).  Outside that window, behaves like
    ``stop_loss_pct(sl_pct, price_mode=sl_price_mode)``.

    Set ``proximity_hours=0`` for premium SL only (no proximity mode).
    """
    premium_sl = stop_loss_pct(sl_pct, price_mode=sl_price_mode)

    def check(state, pos):
        if proximity_hours > 0 and _in_proximity_window(state, pos, proximity_hours):
            if pos.metadata.get("direction", "sell") != "sell":
                return None
            if _strike_breach(state.spot, pos, proximity_buffer_usd):
                return "strike_proximity_stop"
            return None
        return premium_sl(state, pos)

    return check


def position_unrealized_pnl(state, pos, price_mode="mark"):
    # type: (Any, OpenPosition, str) -> Optional[float]
    """Unrealized PnL (USD) for one open position, net of open fees.

    Uses leg-aware math when legs carry ``side`` and entry prices (same rules as
    the engine's ``_open_unrealized_pnl``).  Returns ``None`` on quote data gap.
    """
    current_usd = price_legs(state, pos, mode=price_mode)
    if current_usd is None:
        return None
    per_leg_vals = pos._last_reprice_legs
    direction = pos.metadata.get("direction", "buy")
    can_leg_aware = (
        per_leg_vals is not None
        and len(per_leg_vals) == len(pos.legs)
        and bool(pos.legs)
        and all(
            leg.get("side") in ("buy", "sell")
            and ("price_btc" in leg or "entry_price" in leg)
            for leg in pos.legs
        )
    )
    if can_leg_aware:
        pnl = 0.0
        for leg, cur_val in zip(pos.legs, per_leg_vals):
            qty = float(leg.get("qty", 1.0))
            entry_btc = float(leg.get("price_btc", leg.get("entry_price", 0.0)))
            entry_spot_leg = float(leg.get("entry_spot", pos.entry_spot))
            entry_usd = entry_btc * entry_spot_leg * qty
            if leg["side"] == "sell":
                pnl += entry_usd - cur_val
            else:
                pnl += cur_val - entry_usd
        return pnl - float(pos.fees_open)
    if direction == "sell":
        return pos.entry_price_usd - current_usd - float(pos.fees_open)
    return current_usd - pos.entry_price_usd - float(pos.fees_open)


def equity_drawdown_stop(pct, price_mode="mark"):
    # type: (float, str) -> ExitCondition
    """Close when position unrealized loss exceeds pct of equity at entry.

    ``pct`` is a fraction of ``pos.metadata['equity_at_entry_usd']`` (set by the
    strategy at open), e.g. ``0.05`` = 5%.  ``pct <= 0`` disables.

    Uses mark pricing by default (stable SL semantics).  Requires option quotes.
    """
    def check(state, pos):
        if pct <= 0:
            return None
        ref = pos.metadata.get("equity_at_entry_usd")
        if ref is None or float(ref) <= 0:
            return None
        pnl = position_unrealized_pnl(state, pos, price_mode=price_mode)
        if pnl is None or pnl >= 0:
            return None
        if (-pnl) / float(ref) >= pct:
            return "equity_drawdown_stop"
        return None
    return check


def profit_target_pct(pct, price_mode="executable"):
    # type: (float, str) -> ExitCondition
    """Close when unrealized profit reaches pct (as fraction, e.g. 0.30 = 30% of premium).

    price_mode controls which price is used to evaluate the profit:
      "executable"  — bid for buy legs, ask for sell legs (default; use executable prices
                      so TP only fires when you can actually get that price)
      "mark"        — exchange model price
      "bid" / "ask" — always that side regardless of leg direction

    The default is "executable" because TP should fire only when there is a real
    market price at which you can exit profitably, not just a model estimate.
    """
    def check(state, pos):
        current_usd = price_legs(state, pos, mode=price_mode)
        if current_usd is None:
            return None
        if pos.metadata.get("direction") == "sell":
            # Short premium: profit = premium received > current cost to buy back
            profit_ratio = (pos.entry_price_usd - current_usd) / max(pos.entry_price_usd, 0.01)
        else:
            # Long premium: profit = current value > entry cost
            profit_ratio = (current_usd - pos.entry_price_usd) / max(pos.entry_price_usd, 0.01)
        if profit_ratio >= pct:
            return "profit_target"
        return None
    return check


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def price_legs(state, pos, mode="executable"):
    # type: (Any, OpenPosition, str) -> Optional[float]
    """Price all legs of pos at current market. Returns total USD value.

    mode — which price to use per leg:
      "mark"        — quote.mark_usd (exchange model; stable)
      "executable"  — ask for sell legs, bid for buy legs (what you'd pay/receive)
      "bid"         — always bid regardless of leg side
      "ask"         — always ask regardless of leg side

    Returns None only when a quote row is entirely missing from the snapshot
    (genuine data gap — row absent from parquet). Never returns None for
    zero-mark or zero-bid/ask options: those are priced at $0 (worthless).

    Zero-price handling:
      "mark":       mark == 0.0  → $0 (exchange says worthless)
      "executable"/"ask" (sell): mark == 0  → $0; ask == 0 but mark > 0 → use mark
                                             (or mark × (1+slip) if large enough)
      "executable"/"bid" (buy):  mark == 0  → $0; bid == 0 but mark > 0 → use mark
                                             (or mark × (1-slip) if large enough)

    Writes total and per-leg values to pos._last_reprice_usd / pos._last_reprice_legs
    so the engine's NAV tracker can reuse them without a second call.
    """
    total = 0.0
    direction = pos.metadata.get("direction", "buy")
    _min_mark_usd = _cfg.repricing.min_mark_usd
    _slip = _cfg.repricing.slip_pct_zero_price
    per_leg = []

    for leg in pos.legs:
        quote = state.get_option(leg["expiry"], leg["strike"], leg["is_call"])
        if quote is None:
            return None  # Row missing entirely — genuine data gap, skip tick

        _leg_qty = float(leg.get("qty", 1.0))
        _leg_side = leg.get("side")
        # Per-leg side takes priority; fall back to position-level direction.
        _is_sell = (_leg_side == "sell") if _leg_side is not None else (direction == "sell")

        if mode == "mark":
            # ── Mark mode: always use the exchange model price ────────────
            if quote.mark == 0.0:
                leg_val = 0.0
            else:
                leg_val = quote.mark * quote.spot * _leg_qty

        elif mode == "bid":
            # ── Bid mode: always use bid regardless of side ───────────────
            if quote.mark == 0.0:
                leg_val = 0.0
            elif quote.bid == 0.0:
                leg_val = (quote.mark * (1.0 - _slip) if quote.mark_usd > _min_mark_usd
                           else quote.mark) * quote.spot * _leg_qty
            else:
                leg_val = quote.bid * quote.spot * _leg_qty

        elif mode == "ask":
            # ── Ask mode: always use ask regardless of side ───────────────
            if quote.mark == 0.0:
                leg_val = 0.0
            elif quote.ask == 0.0:
                leg_val = (quote.mark * (1.0 + _slip) if quote.mark_usd > _min_mark_usd
                           else quote.mark) * quote.spot * _leg_qty
            else:
                leg_val = quote.ask * quote.spot * _leg_qty

        else:
            # ── Executable mode: ask for sell legs, bid for buy legs ──────
            if _is_sell:
                if quote.mark == 0.0:
                    # Exchange says worthless — $0 to close a short leg
                    effective_price = 0.0
                elif quote.ask == 0.0:
                    # Ask missing but mark is real: estimate close cost
                    effective_price = (quote.mark * (1.0 + _slip) if quote.mark_usd > _min_mark_usd
                                       else quote.mark)
                else:
                    # Floor at mark: prevents wide-spread ask from understating cost
                    effective_price = quote.ask if quote.ask > quote.mark else quote.mark
                leg_val = effective_price * quote.spot * _leg_qty
            else:
                if quote.mark == 0.0:
                    # Exchange says worthless — $0 proceeds from selling a long leg
                    leg_val = 0.0
                elif quote.bid == 0.0:
                    # Bid missing but mark is real: estimate proceeds
                    effective_price = (quote.mark * (1.0 - _slip) if quote.mark_usd > _min_mark_usd
                                       else quote.mark)
                    leg_val = effective_price * quote.spot * _leg_qty
                else:
                    leg_val = quote.bid_usd * _leg_qty

        total += leg_val
        per_leg.append(leg_val)

    pos._last_reprice_usd = total
    pos._last_reprice_legs = per_leg
    return total


def _reprice_legs(state, pos):
    # type: (Any, OpenPosition) -> Optional[float]
    """Backward-compat alias for price_legs(mode="executable").

    The engine imports and calls this function directly for NAV tracking.
    New code should call price_legs() with an explicit mode.
    """
    return price_legs(state, pos, mode="executable")


def close_trade(state, pos, reason, current_usd=None, fees_close=0.0):
    # type: (Any, OpenPosition, str, Optional[float], float) -> Trade
    """Helper to build a Trade from an OpenPosition being closed.

    PnL is computed leg-by-leg using each leg's ``side`` and explicit
    ``price_btc`` / ``exit_price_btc``. All legs must carry these fields;
    a missing annotation raises ValueError immediately so the bug is
    surfaced during development rather than silently producing wrong PnL.
    """
    if current_usd is None:
        current_usd = _reprice_legs(state, pos) or 0.0

    total_fees = pos.fees_open + fees_close

    _can_leg_aware = bool(pos.legs) and all(
        leg.get("side") in ("buy", "sell")
        and ("price_btc" in leg or "entry_price" in leg)
        and "exit_price_btc" in leg
        for leg in pos.legs
    )
    if _can_leg_aware:
        pnl = 0.0
        for leg in pos.legs:
            _qty = float(leg.get("qty", 1.0))
            _entry_btc = float(leg.get("price_btc", leg.get("entry_price", 0.0)))
            _exit_btc  = float(leg["exit_price_btc"])
            _leg_entry_spot = float(leg.get("entry_spot", pos.entry_spot))
            _entry_usd = _entry_btc * _leg_entry_spot * _qty
            _exit_usd  = _exit_btc  * state.spot      * _qty
            if leg["side"] == "sell":
                pnl += _entry_usd - _exit_usd
            else:
                pnl += _exit_usd - _entry_usd
        pnl -= total_fees
    else:
        raise ValueError(
            "Leg-aware PnL requires all legs to carry 'side', "
            "'price_btc'/'entry_price', and 'exit_price_btc'. "
            "Missing annotations in legs: {}".format(pos.legs)
        )

    held_s = (state.dt - pos.entry_time).total_seconds()
    return Trade(
        entry_time=pos.entry_time,
        exit_time=state.dt,
        entry_spot=pos.entry_spot,
        exit_spot=state.spot,
        entry_price_usd=pos.entry_price_usd,
        exit_price_usd=current_usd,
        fees=total_fees,
        pnl=pnl,
        triggered=(reason == "trigger"),
        exit_reason=reason,
        exit_hour=int(held_s / 3600),
        entry_date=pos.entry_time.strftime("%Y-%m-%d"),
        metadata={
            **pos.metadata,
            "legs": pos.legs,
            "fees_open": pos.fees_open,
        },
    )


# ------------------------------------------------------------------
# Engine-owned position lifecycle API (Phase A of fills refactor)
# ------------------------------------------------------------------
# The functions below are the canonical way for strategies to close or
# partially close a position. They derive Trades that the engine then
# expands into per-leg fills. Strategies should NOT author exit prices
# on legs that are not actually being transacted.
#
# close_position()  — full close, drop-in replacement for close_trade().
# partial_close()   — close a subset of legs; mutate pos to retain the rest.
# add_legs()        — extend an open position with additional legs.
#
# All three preserve the strategy's existing pos_id linkage so fills
# emit with correct open_idx → close_idx references.


def close_position(state, pos, reason, current_usd=None, fees_close=0.0):
    # type: (Any, OpenPosition, str, Optional[float], float) -> Trade
    """Close a full position.

    Drop-in replacement for close_trade(). The returned Trade should be
    yielded by the strategy with side='close'; the engine derives close
    fills from trade.metadata['legs'] and links them to the original open
    fills via pos.metadata['pos_id'] (if present).

    Strategies must set leg['exit_price_btc'] on each leg BEFORE calling
    (and optionally leg['fee_btc_close'] for fee overrides).
    """
    return close_trade(state, pos, reason, current_usd=current_usd, fees_close=fees_close)


def partial_close(state, pos, leg_indices, reason, fees_close=0.0):
    # type: (Any, OpenPosition, List[int], str, float) -> Trade
    """Close a subset of legs from a multi-leg position.

    Mutates ``pos`` in place to drop the closed legs and reduce
    ``entry_price_usd`` and ``fees_open`` by the closed legs' contribution.
    The surviving legs remain in ``pos`` and continue to be marked-to-market
    under the original open trade (same pos_id, same entry_time).

    Returns a Trade representing the closed legs only. The Trade has
    ``side='close'`` and ``metadata['partial_close']=True`` so the engine
    emits close fills for the closed legs while RETAINING the pos_id →
    open_trade_idx mapping for the surviving legs' eventual close.

    PnL is computed per-leg by side:
      sell leg: leg_entry_usd - leg_exit_usd
      buy  leg: leg_exit_usd  - leg_entry_usd
    minus the partial fees (closed legs' fees_open + fees_close).

    Caller responsibilities BEFORE calling:
      • Set leg['exit_price_btc'] on each closed leg.
      • Optionally set leg['fee_btc_close'] for fee overrides.
      • Each closed leg MUST have 'price_btc' (per-contract BTC at open),
        'side' ('buy'|'sell'), and 'qty' for entry-value reconstruction.
      • Each closed leg MUST have 'fee_usd_open' (per-leg USD fee at open)
        OR the position's fees_open will be allocated by entry-value.
    """
    n = len(pos.legs)
    leg_indices = sorted(set(int(i) for i in leg_indices))
    if not leg_indices:
        raise ValueError("partial_close: leg_indices must be non-empty")
    if leg_indices[0] < 0 or leg_indices[-1] >= n:
        raise ValueError("partial_close: leg_indices out of range (n={})".format(n))
    if len(leg_indices) == n:
        raise ValueError(
            "partial_close: all legs requested — use close_position() instead"
        )

    closed_legs = [pos.legs[i] for i in leg_indices]
    survivors = [leg for i, leg in enumerate(pos.legs) if i not in set(leg_indices)]

    # Per-leg entry USD: price_btc * leg.entry_spot (or pos.entry_spot fallback) * qty.
    # Legs added later via add_legs() may carry their own entry_spot.
    closed_entry_usd_total = 0.0
    closed_leg_entries = []  # parallel list: per-closed-leg entry_usd
    for leg in closed_legs:
        _qty = float(leg.get("qty", 1.0))
        _price_btc = float(leg.get("price_btc", leg.get("entry_price", 0.0)))
        _leg_entry_spot = float(leg.get("entry_spot", pos.entry_spot))
        _e = _price_btc * _leg_entry_spot * _qty
        closed_leg_entries.append(_e)
        closed_entry_usd_total += _e

    # Per-leg exit USD: exit_price_btc * exit_spot * qty.
    _exit_spot = state.spot
    closed_exit_usd_total = 0.0
    closed_leg_exits = []
    for leg in closed_legs:
        _qty = float(leg.get("qty", 1.0))
        _ex_btc = float(leg.get("exit_price_btc", 0.0))
        _x = _ex_btc * _exit_spot * _qty
        closed_leg_exits.append(_x)
        closed_exit_usd_total += _x

    # Fees: prefer per-leg ``fee_usd_open`` when every closed leg carries it
    # (most accurate); else allocate ``pos.fees_open`` across all legs
    # proportional to entry value; else equal-split as a last resort.
    _all_have_fee_usd_open = all("fee_usd_open" in leg for leg in closed_legs)
    if _all_have_fee_usd_open:
        closed_fees_open_alloc = sum(
            float(leg["fee_usd_open"]) for leg in closed_legs
        )
    else:
        _all_entries_total = 0.0
        for leg in pos.legs:
            _q = float(leg.get("qty", 1.0))
            _p = float(leg.get("price_btc", leg.get("entry_price", 0.0)))
            _ls = float(leg.get("entry_spot", pos.entry_spot))
            _all_entries_total += _p * _ls * _q
        if _all_entries_total > 0.0:
            closed_fees_open_alloc = (
                closed_entry_usd_total / _all_entries_total
            ) * pos.fees_open
        else:
            closed_fees_open_alloc = pos.fees_open * (len(closed_legs) / float(n))

    closed_fees_total = closed_fees_open_alloc + float(fees_close)

    # Per-leg PnL (side-aware), then sum and subtract fees.
    pnl = 0.0
    for leg, _e, _x in zip(closed_legs, closed_leg_entries, closed_leg_exits):
        _side = leg.get("side", "buy")
        if _side == "sell":
            pnl += _e - _x
        else:
            pnl += _x - _e
    pnl -= closed_fees_total

    held_s = (state.dt - pos.entry_time).total_seconds()

    # Mutate pos: drop closed legs, reduce entry_price_usd, fees_open.
    # entry_price_usd convention in existing strategies: sum of positive
    # leg-entry USD values (premium received for shorts, premium paid for
    # longs — both stored as positive). We subtract the closed legs'
    # positive contribution to preserve that convention for survivors.
    pos.legs = survivors
    pos.entry_price_usd = max(0.0, pos.entry_price_usd - closed_entry_usd_total)
    pos.fees_open = max(0.0, pos.fees_open - closed_fees_open_alloc)
    pos._last_reprice_usd = None  # force re-mark on next tick

    return Trade(
        entry_time=pos.entry_time,
        exit_time=state.dt,
        entry_spot=pos.entry_spot,
        exit_spot=state.spot,
        entry_price_usd=closed_entry_usd_total,
        exit_price_usd=closed_exit_usd_total,
        fees=closed_fees_total,
        pnl=pnl,
        triggered=(reason == "trigger"),
        exit_reason=reason,
        exit_hour=int(held_s / 3600),
        entry_date=pos.entry_time.strftime("%Y-%m-%d"),
        metadata={
            **pos.metadata,
            "legs": closed_legs,
            "fees_open": closed_fees_open_alloc,
            "partial_close": True,
        },
    )


def add_legs(pos, new_legs, new_entry_price_usd, new_fees_open):
    # type: (OpenPosition, List[Dict[str, Any]], float, float) -> None
    """Extend an open position with additional legs.

    Appends ``new_legs`` to ``pos.legs`` and adds the corresponding
    ``new_entry_price_usd`` and ``new_fees_open`` contributions to the
    position aggregates. Does NOT create a Trade — the caller is
    responsible for yielding a separate side='open' Trade (with its own
    pos_id or sharing the existing pos.metadata['pos_id']) so the engine
    emits open fills for the new legs.

    Invalidates the reprice cache so the next mark-to-market call sees
    the extended leg set.
    """
    if not new_legs:
        return
    pos.legs = list(pos.legs) + list(new_legs)
    pos.entry_price_usd = float(pos.entry_price_usd) + float(new_entry_price_usd)
    pos.fees_open = float(pos.fees_open) + float(new_fees_open)
    pos._last_reprice_usd = None


# ------------------------------------------------------------------
# Exit condition helpers
# ------------------------------------------------------------------

def check_expiry(state, pos):
    # type: (Any, OpenPosition) -> Optional[str]
    """Return 'expiry' if pos.metadata['expiry_dt'] has been reached, else None."""
    exp_dt = pos.metadata.get("expiry_dt")
    if exp_dt is None:
        return None
    return "expiry" if state.dt >= exp_dt else None


def check_take_profit_strangle(state, pos, tp_pct):
    # type: (Any, OpenPosition, float) -> Optional[str]
    """Return 'take_profit' when combined buy-back cost (ask) drops to (1-tp_pct) × entry.

    Reads call_strike / put_strike from pos.metadata.
    Returns None if tp_pct <= 0, quotes are absent, or ask == 0 (no executable
    price — skip tick rather than firing on phantom liquidity).
    """
    if tp_pct <= 0:
        return None
    expiry = pos.metadata["expiry"]
    call_q = state.get_option(expiry, pos.metadata["call_strike"], True)
    put_q  = state.get_option(expiry, pos.metadata["put_strike"], False)
    if call_q is None or put_q is None:
        return None
    if call_q.ask <= 0 or put_q.ask <= 0:
        return None
    current_usd = call_q.ask_usd + put_q.ask_usd
    profit_ratio = (pos.entry_price_usd - current_usd) / max(pos.entry_price_usd, 0.01)
    if profit_ratio >= tp_pct:
        return "take_profit"
    return None


#!/usr/bin/env python3
"""
cal_premium_collect.py — Calendar spread premium collection with structural protection.

Structure:
    Two independent calendar pairs (put pair below ATM, call pair above ATM).
    Each pair = long option (~45 DTE, min 29 DTE) + short option (7 DTE) at the
    same strike.  The long leg provides downside/upside protection while the short
    leg collects faster theta decay.

Entry:
    Every Friday at 09:00 UTC.  Both pairs opened on first entry Friday.
    Which pairs are active is controlled by the `sides` parameter.

Weekly roll (every subsequent Friday at 09:00 UTC):
    Short leg: always close (buy back at ask; 0 cost if already expired) and
               reopen a fresh 7 DTE short at the current long's strike.
    Long leg:  keep open if |current_delta − target_delta| ≤ delta_drift_threshold.
               Otherwise close (sell at bid) and reopen at target_delta / target DTE.
               When a fresh long is selected the new short also moves to that strike.

Sides:
    "puts"  — put calendar pair only
    "calls" — call calendar pair only
    "both"  — both pairs (default)

Grid parameters:
    sides                 ["puts", "calls", "both"]
    target_delta          [0.15, 0.20, 0.25]   — absolute delta for long strike selection
    delta_drift_threshold [0.08, 0.10, 0.12]   — max allowed delta drift before rolling long

Fees:
    Deribit model applied at each open and close event.
    Long leg carried across rolls is NOT re-charged opening fees unless it is
    actually re-entered (delta drifted).  The roll close trade records only the
    fees actually transacted during that roll.

PnL accounting (per pair, per roll cycle):
    net_entry_usd  = short_premium_received − long_premium_paid
    net_exit_usd   = short_buyback_cost − long_sell_proceeds
    pnl            = net_entry_usd − net_exit_usd − total_fees

    For roll cycles where the long is NOT re-entered, the long leg is marked to
    current ask for the new position's entry price (no fee charged), so the roll
    close trade captures exactly one cycle of combined spread P&L.
"""
import logging
from typing import Any, Dict, List, Optional, Tuple

from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import (
    parse_expiry_date, expiry_dt_utc, select_expiry, select_expiry_for_week,
)
from backtester.pricing import deribit_fee_per_leg
from backtester.strategy_base import (
    OpenPosition,
    Trade,
    add_legs,
    close_position,
    partial_close,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Expiry selection helpers
# ---------------------------------------------------------------------------

def _select_long_expiry(state, min_dte=29):
    # type: (Any, int) -> Optional[str]
    """Return the expiry closest to 45 DTE, searching from week 6 outward.

    Uses select_expiry_for_week from expiry_utils.  Each call returns the
    lowest DTE in the bucket [weeks*7, weeks*7+6].  Priority order tries
    the 42-48 DTE bucket first (closest to target 45), then expands.
    Enforces min_dte to avoid selecting an expiry too close to expiry.
    """
    today = state.dt.date()
    # Priority: week 6 (42-48 DTE), 7 (49-55), 5 (35-41), 8, 4 (28-34)
    for week in [6, 7, 5, 8, 4]:
        exp = select_expiry_for_week(state, week)
        if exp is None:
            continue
        exp_date = parse_expiry_date(exp)
        if exp_date is None:
            continue
        if (exp_date.date() - today).days >= min_dte:
            return exp
    return None


def _select_short_expiry(state):
    # type: (Any,) -> Optional[str]
    """Return the expiry closest to 7 DTE.

    Uses select_expiry from expiry_utils (exact DTE match), probing from
    the target outward: 7, 6, 8, 5, 9.  This mirrors the batman_calendar
    approach of walking DTEs until a match is found.
    """
    for dte in [7, 6, 8, 5, 9]:
        exp = select_expiry(state, dte)
        if exp is not None:
            return exp
    return None


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

class CalPremiumCollect:
    """Calendar spread premium collection with long-leg protection.

    Each calendar pair is modelled as an OpenPosition with two legs:
        legs[0]  — long  (buy, protective, ~45 DTE)
        legs[1]  — short (sell, theta collector, 7 DTE)
    """

    name = "cal_premium_collect"
    DATE_RANGE = ("2026-03-20", "2026-04-04")
    DESCRIPTION = (
        "Sells a 7 DTE option and buys a ~45 DTE option at the same strike "
        "(one put pair below ATM, one call pair above ATM). "
        "Rolls short every Friday; rolls long only when delta drifts."
    )

    PARAM_GRID = {
        "sides":                 ["puts"],
        "target_delta":          [0.25],
        "delta_drift_threshold": [0.10],
    }

    # Long DTE targets (fixed — not in grid to keep combo count manageable)
    LONG_MIN_DTE = 29

    def __init__(self):
        self._positions = []  # List[OpenPosition] — one per active pair
        self._sides = "both"
        self._target_delta = 0.20
        self._drift_threshold = 0.10
        self._last_roll_isoweek = None  # Optional[Tuple[int, int]]
        self._pos_counter = 0  # int — monotonically increasing pos_id

    # ------------------------------------------------------------------
    # Protocol
    # ------------------------------------------------------------------

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._sides = params.get("sides", "both")
        self._target_delta = params["target_delta"]
        self._drift_threshold = params["delta_drift_threshold"]
        self._positions = []
        self._last_roll_isoweek = None
        self._pos_counter = 0

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        # --- Weekly roll / first entry: Friday at 09:00 UTC ---
        if state.dt.weekday() == 4 and state.dt.hour == 9 and state.dt.minute == 0:
            iso_week = (state.dt.isocalendar()[0], state.dt.isocalendar()[1])
            if iso_week != self._last_roll_isoweek:
                self._last_roll_isoweek = iso_week
                roll_trades = self._do_weekly_roll(state)
                trades.extend(roll_trades)

        return trades

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        trades = [
            self._close_pair(state, pos, "end_of_data", fees_long=True)
            for pos in self._positions
        ]
        self._positions = []
        return trades

    def reset(self):
        # type: () -> None
        self._positions = []
        self._last_roll_isoweek = None
        self._pos_counter = 0

    def _next_pos_id(self):
        # type: () -> int
        self._pos_counter += 1
        return self._pos_counter

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "sides":                 self._sides,
            "target_delta":          self._target_delta,
            "delta_drift_threshold": self._drift_threshold,
        }

    # ------------------------------------------------------------------
    # Weekly roll logic
    # ------------------------------------------------------------------

    def _do_weekly_roll(self, state):
        # type: (Any) -> List[Trade]
        """Roll existing pairs (yielding close + open Trades per leg lifecycle),
        then open any missing pair types (yielding an open Trade each).
        """
        trades = []

        # Determine which pair types should be active
        active_types = self._active_pair_types()

        # Roll all existing open pairs
        still_open = []
        for pos in self._positions:
            roll_trades, kept_pos = self._roll_pair(state, pos)
            trades.extend(roll_trades)
            if kept_pos is not None:
                still_open.append(kept_pos)
        self._positions = still_open

        # Open any pair type that is not currently open (first entry or after SL closed it)
        open_types = {pos.metadata["pair_type"] for pos in self._positions}
        for pair_type in active_types:
            if pair_type not in open_types:
                result = self._open_pair(state, pair_type)
                if result is not None:
                    open_trade, new_pos = result
                    trades.append(open_trade)
                    self._positions.append(new_pos)

        return trades

    def _active_pair_types(self):
        # type: () -> List[str]
        if self._sides == "puts":
            return ["put"]
        if self._sides == "calls":
            return ["call"]
        return ["put", "call"]

    # ------------------------------------------------------------------
    # Open a fresh calendar pair
    # ------------------------------------------------------------------

    def _open_pair(self, state, pair_type):
        # type: (Any, str) -> Optional[Tuple[Trade, OpenPosition]]
        """Open a brand-new calendar pair for the given pair_type ("put"/"call").

        Returns (open_Trade, OpenPosition) or None on failure.  The open Trade
        has side='open' and carries both legs in metadata['legs'] so the engine
        emits open fills with linkage back to future close fills.
        """
        is_call = (pair_type == "call")

        long_expiry = _select_long_expiry(state, self.LONG_MIN_DTE)
        if long_expiry is None:
            log.warning("cal_premium_collect: no long expiry available for %s pair", pair_type)
            return None

        short_expiry = _select_short_expiry(state)
        if short_expiry is None:
            log.warning("cal_premium_collect: no short expiry available for %s pair", pair_type)
            return None

        # Must not select the same expiry for both legs
        if long_expiry == short_expiry:
            log.warning(
                "cal_premium_collect: long and short expiry identical (%s) for %s pair",
                long_expiry, pair_type,
            )
            return None

        long_chain = state.get_chain(long_expiry)
        if not long_chain:
            return None

        candidates = [q for q in long_chain if q.is_call == is_call]
        # For calls: target positive delta; for puts: negative delta
        target_d = self._target_delta if is_call else -self._target_delta
        long_opt = select_by_delta(candidates, target_d)
        if long_opt is None or long_opt.ask <= 0:
            return None

        long_usd = long_opt.ask_usd
        long_strike = long_opt.strike

        return self._build_pair_position(
            state=state,
            pair_type=pair_type,
            is_call=is_call,
            long_expiry=long_expiry,
            long_strike=long_strike,
            long_entry_usd=long_usd,
            long_fee=deribit_fee_per_leg(state.spot, long_usd),
            long_delta=long_opt.delta,
            short_expiry=short_expiry,
        )

    # ------------------------------------------------------------------
    # Roll an existing pair
    # ------------------------------------------------------------------

    def _roll_pair(self, state, pos):
        # type: (Any, OpenPosition) -> Tuple[Optional[Trade], Optional[OpenPosition]]
        """
        Close the current pair position and reopen it.

        Returns (list_of_trades, kept_or_new_position).

        Three outcomes:
          • Data gap: returns ([], pos) — carry the position forward unchanged.
          • Long drifted: full close of the pair + open of a fresh pair.
              Returns ([close_trade, open_trade], new_pos).
          • Long kept: partial close of the short leg only + add a fresh short.
              Returns ([partial_close_trade, open_trade_for_new_short], pos)
              where pos has been mutated to drop the old short and add the new one.
        """
        md = pos.metadata
        is_call = md["is_call"]
        pair_type = md["pair_type"]
        long_expiry = md["long_expiry"]
        long_strike = md["long_strike"]
        short_expiry = md["short_expiry"]
        short_strike = md["short_strike"]  # always == long_strike

        # --- Price the short leg exit ---
        short_exp_dt = md.get("short_expiry_dt")
        short_already_expired = (short_exp_dt is not None and state.dt >= short_exp_dt)

        if short_already_expired:
            # Short expired this week: intrinsic settlement (cost to us)
            if is_call:
                short_exit_usd = max(0.0, state.spot - short_strike)
            else:
                short_exit_usd = max(0.0, short_strike - state.spot)
            short_close_fee = 0.0
        else:
            short_q = state.get_option(short_expiry, short_strike, is_call)
            if short_q is None:
                # Data gap — skip roll, carry position forward
                return [], pos
            # Buy back at ask (or mark if ask==0)
            short_exit_usd = short_q.ask_usd if short_q.ask > 0 else short_q.mark_usd
            short_close_fee = deribit_fee_per_leg(state.spot, short_exit_usd)

        # --- Price the long leg (needed for drift detection) ---
        long_q = state.get_option(long_expiry, long_strike, is_call)
        if long_q is None:
            # Data gap — skip roll
            return [], pos

        long_exit_bid = long_q.bid_usd if long_q.bid > 0 else long_q.mark_usd

        # --- Evaluate long delta drift ---
        current_delta = long_q.delta  # signed
        abs_delta = abs(current_delta) if current_delta is not None else 0.0
        long_drifted = abs(abs_delta - self._target_delta) > self._drift_threshold

        # --- Locate leg indices within pos.legs ---
        long_idx = next(i for i, leg in enumerate(pos.legs) if leg.get("layer") == "long")
        short_idx = next(i for i, leg in enumerate(pos.legs) if leg.get("layer") == "short")

        # --- Common: annotate short leg exit price (always closes on roll) ---
        _short_exit_btc = short_exit_usd / state.spot if state.spot else 0.0
        pos.legs[short_idx]["exit_price_btc"] = _short_exit_btc
        if short_already_expired:
            pos.legs[short_idx]["fee_btc_close"] = 0.0

        # =================================================================
        # Path A — Long drifted: full close + full reopen.
        # =================================================================
        if long_drifted:
            _long_exit_btc = long_exit_bid / state.spot if state.spot else 0.0
            pos.legs[long_idx]["exit_price_btc"] = _long_exit_btc

            net_exit_usd = short_exit_usd - long_exit_bid
            long_close_fee = deribit_fee_per_leg(state.spot, long_exit_bid)
            fees_close = short_close_fee + long_close_fee

            roll_close = close_position(state, pos, "roll", net_exit_usd, fees_close)
            roll_close.metadata["skip_open_fill"] = True
            roll_close.metadata["pair_type"] = pair_type

            # Open a fresh pair (new long + new short) at target params.
            result = self._open_pair(state, pair_type)
            if result is None:
                # Reopen failed — close emitted, but no new position.
                return [roll_close], None
            open_trade, new_pos = result
            return [roll_close, open_trade], new_pos

        # =================================================================
        # Path B — Long kept: partial close of short + add fresh short.
        # =================================================================
        partial_reason = "expiry" if short_already_expired else "roll"
        partial = partial_close(
            state, pos, [short_idx], partial_reason, fees_close=short_close_fee,
        )
        partial.metadata["pair_type"] = pair_type

        # pos.legs is now [long_leg]; pos.metadata still carries the old
        # short_strike/expiry — we will overwrite them when the new short attaches.

        # Select new short expiry
        new_short_expiry = _select_short_expiry(state)
        if new_short_expiry is None:
            log.warning(
                "cal_premium_collect: no short expiry for roll of %s pair", pair_type
            )
            return [partial], pos

        if new_short_expiry == long_expiry:
            log.warning(
                "cal_premium_collect: short and long expiry identical after roll (%s), %s pair",
                new_short_expiry, pair_type,
            )
            return [partial], pos

        new_short_chain = state.get_chain(new_short_expiry)
        if not new_short_chain:
            return [partial], pos

        new_short_candidates = [q for q in new_short_chain if q.is_call == is_call]
        new_short_opt = min(
            (q for q in new_short_candidates if q.strike is not None),
            key=lambda q: abs(q.strike - long_strike),
            default=None,
        )
        if new_short_opt is None or new_short_opt.bid <= 0:
            return [partial], pos

        new_short_entry_usd = new_short_opt.bid_usd
        new_short_fee = deribit_fee_per_leg(state.spot, new_short_entry_usd)
        new_short_exp_dt = expiry_dt_utc(new_short_expiry, state.dt.tzinfo)

        new_short_leg = {
            "strike":      new_short_opt.strike,
            "is_call":     is_call,
            "expiry":      new_short_expiry,
            "side":        "sell",
            "price_btc":   new_short_opt.bid,
            "qty":         1.0,
            "entry_delta": new_short_opt.delta,
            "layer":       "short",
            "fee_usd_open": new_short_fee,
        }
        add_legs(pos, [new_short_leg], new_short_entry_usd, new_short_fee)

        # Refresh metadata fields that describe the *current* short.
        pos.metadata["short_expiry"]            = new_short_expiry
        pos.metadata["short_strike"]            = new_short_opt.strike
        pos.metadata["short_expiry_dt"]         = new_short_exp_dt
        pos.metadata["short_entry_premium_usd"] = new_short_entry_usd

        # Emit a side='open' Trade for the new short leg only. Give it a
        # fresh pos_id so the engine open_idx map records this open
        # event independently of the original pair-level pos_id.
        new_pos_id = self._next_pos_id()
        open_trade_new_short = Trade(
            entry_time=state.dt,
            exit_time=state.dt,
            entry_spot=state.spot,
            exit_spot=state.spot,
            entry_price_usd=new_short_entry_usd,
            exit_price_usd=0.0,
            fees=new_short_fee,
            pnl=0.0,
            triggered=False,
            exit_reason="",
            exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={
                "direction": "sell",
                "pos_id":    new_pos_id,
                "pair_type": pair_type,
                "legs":      [new_short_leg],
            },
        )
        return [partial, open_trade_new_short], pos

    # ------------------------------------------------------------------
    # Build a pair OpenPosition (used by both first-entry and roll)
    # ------------------------------------------------------------------

    def _build_pair_position(
        self, state, pair_type, is_call,
        long_expiry, long_strike, long_entry_usd, long_fee, long_delta,
        short_expiry,
    ):
        # type: (Any, str, bool, str, float, float, float, Any, str) -> Optional[Tuple[Trade, OpenPosition]]
        """Build a new OpenPosition for one calendar pair AND emit its open Trade.

        Returns (open_Trade, OpenPosition) or None on failure.
        """
        short_chain = state.get_chain(short_expiry)
        if not short_chain:
            return None

        # Short leg: same strike as long, same side (call/put)
        short_candidates = [q for q in short_chain if q.is_call == is_call]
        short_opt = min(
            (q for q in short_candidates if q.strike is not None),
            key=lambda q: abs(q.strike - long_strike),
            default=None,
        )
        if short_opt is None or short_opt.bid <= 0:
            return None

        short_entry_usd = short_opt.bid_usd
        short_fee = deribit_fee_per_leg(state.spot, short_entry_usd)

        # net credit: short received − long paid (positive = net credit, negative = net debit)
        net_entry_usd = short_entry_usd - long_entry_usd

        long_exp_dt = expiry_dt_utc(long_expiry, state.dt.tzinfo)
        short_exp_dt = expiry_dt_utc(short_expiry, state.dt.tzinfo)

        legs = [
            {
                "strike":          long_strike,
                "is_call":         is_call,
                "expiry":          long_expiry,
                "side":            "buy",
                "price_btc":       long_entry_usd / state.spot if state.spot else 0.0,
                "qty":             1.0,
                "entry_delta":     long_delta,
                "layer":           "long",
                "fee_usd_open":    long_fee,
                **({"fee_btc_open": 0.0} if long_fee == 0.0 else {}),
            },
            {
                "strike":          short_opt.strike,
                "is_call":         is_call,
                "expiry":          short_expiry,
                "side":            "sell",
                "price_btc":       short_opt.bid,
                "qty":             1.0,
                "entry_delta":     short_opt.delta,
                "layer":           "short",
                "fee_usd_open":    short_fee,
            },
        ]

        pos_id = self._next_pos_id()
        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=legs,
            entry_price_usd=net_entry_usd,
            fees_open=long_fee + short_fee,
            metadata={
                "direction":               "sell",   # net-short spread
                "pos_id":                  pos_id,
                "pair_type":               pair_type,
                "is_call":                 is_call,
                "long_expiry":             long_expiry,
                "long_strike":             long_strike,
                "long_expiry_dt":          long_exp_dt,
                "long_entry_premium_usd":  long_entry_usd,
                "short_expiry":            short_expiry,
                "short_strike":            short_opt.strike,
                "short_expiry_dt":         short_exp_dt,
                "short_entry_premium_usd": short_entry_usd,
                "target_delta":            self._target_delta,
            },
        )

        # Emit the open Trade for this pair. Both legs carried in metadata['legs']
        # so the engine emits two open fills and stores leg['_open_idx'] on each.
        open_trade = Trade(
            entry_time=state.dt,
            exit_time=state.dt,
            entry_spot=state.spot,
            exit_spot=state.spot,
            entry_price_usd=net_entry_usd,
            exit_price_usd=0.0,
            fees=long_fee + short_fee,
            pnl=0.0,
            triggered=False,
            exit_reason="",
            exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={
                "direction": "sell",
                "pos_id":    pos_id,
                "pair_type": pair_type,
                "legs":      legs,
            },
        )
        return open_trade, pos

    # ------------------------------------------------------------------
    # Close a pair position
    # ------------------------------------------------------------------

    def _close_pair(self, state, pos, reason, fees_long=True):
        # type: (Any, OpenPosition, str, bool) -> Trade
        """Close all currently-open legs of a calendar pair and return a Trade.

        Works regardless of how many legs the pair currently has (the long is
        always present; the short may have been added via a partial-close roll).
        Uses the engine-owned `close_position` API with `skip_open_fill=True`
        so the engine emits close fills only — the legs' open fills were
        emitted earlier when each leg was first added to the position.
        """
        md = pos.metadata
        is_call = md["is_call"]

        net_exit_usd = 0.0
        fees_close = 0.0

        # Per-leg: compute exit price (intrinsic if expired, bid/ask from book
        # otherwise) and annotate exit_price_btc + fee_btc_close on the leg.
        for leg in pos.legs:
            layer = leg.get("layer")
            leg_expiry = leg["expiry"]
            leg_strike = leg["strike"]
            leg_exp_dt = (
                md.get("long_expiry_dt") if layer == "long"
                else md.get("short_expiry_dt")
            )
            already_expired = (leg_exp_dt is not None and state.dt >= leg_exp_dt)

            if layer == "long":
                # Long: sell at bid (proceeds).  Intrinsic if expired.
                if already_expired:
                    if is_call:
                        leg_exit_usd = max(0.0, state.spot - leg_strike)
                    else:
                        leg_exit_usd = max(0.0, leg_strike - state.spot)
                    leg_close_fee = 0.0
                else:
                    long_q = state.get_option(leg_expiry, leg_strike, is_call)
                    if long_q is not None:
                        leg_exit_usd = long_q.bid_usd if long_q.bid > 0 else long_q.mark_usd
                    else:
                        leg_exit_usd = md["long_entry_premium_usd"]  # flat fallback
                    leg_close_fee = (
                        deribit_fee_per_leg(state.spot, leg_exit_usd) if fees_long else 0.0
                    )
                # net_exit = short_buyback − long_sell_proceeds; long is a credit.
                net_exit_usd -= leg_exit_usd
            else:
                # Short: buy back at ask (cost).  Intrinsic if expired.
                if already_expired:
                    if is_call:
                        leg_exit_usd = max(0.0, state.spot - leg_strike)
                    else:
                        leg_exit_usd = max(0.0, leg_strike - state.spot)
                    leg_close_fee = 0.0
                else:
                    short_q = state.get_option(leg_expiry, leg_strike, is_call)
                    if short_q is not None:
                        leg_exit_usd = short_q.ask_usd if short_q.ask > 0 else short_q.mark_usd
                    else:
                        leg_exit_usd = md["short_entry_premium_usd"]  # flat fallback
                    leg_close_fee = deribit_fee_per_leg(state.spot, leg_exit_usd)
                net_exit_usd += leg_exit_usd

            fees_close += leg_close_fee

            leg["exit_price_btc"] = leg_exit_usd / state.spot if state.spot else 0.0
            if leg_close_fee == 0.0:
                leg["fee_btc_close"] = 0.0

        trade = close_position(state, pos, reason, net_exit_usd, fees_close)
        trade.metadata["skip_open_fill"] = True
        trade.metadata["pair_type"] = md["pair_type"]
        return trade

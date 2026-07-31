#!/usr/bin/env python3
"""
blueprint_howto.py — Reference implementation and annotated how-to guide.

╔══════════════════════════════════════════════════════════════════════════════╗
║  PURPOSE                                                                    ║
║                                                                              ║
║  This file is the canonical blueprint for writing backtester strategies.    ║
║  Location: workspace/strategies/other/blueprint_howto.py                     ║
║  Register strategies in workspace/catalog.py (stable IDs; never rename).     ║
║  backtester/strategies/ holds compatibility shims only.                      ║
║                                                                              ║
║  It demonstrates every pattern you need:                                     ║
║    • configure() / on_market_state() / on_end() / reset() lifecycle         ║
║    • Opening a long OR short strangle with full leg annotations              ║
║    • Using stop_loss_pct() and profit_target_pct() correctly                ║
║    • The meaning of "direction" metadata and how _reprice_legs uses it       ║
║    • How close_position() handles PnL via leg-aware accounting               ║
║    • Expiry settlement (OTM → $0, ITM → intrinsic value)                    ║
║    • Detailed human- and machine-readable logging at open and close          ║
║    • Correct leg dict fields required by close_trade() / the fills engine   ║
║                                                                              ║
║  RUNNING IT                                                                  ║
║  This strategy is self-contained and needs no indicators.                    ║
║  Set DATE_RANGE and entry_date to a day with 1-DTE options in your data.    ║
║  It will open exactly one trade and exit via SL, TP, or expiry.             ║
║                                                                              ║
║  READING THE LOG                                                             ║
║  Every OPEN / CLOSE event prints a table with:                               ║
║    bid / ask / mark prices in BTC, strike, delta, IV, spot at that tick     ║
║  A person with access to TradingView (BTC spot) and the option parquet files ║
║  can independently verify every number.                                      ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

# ── SECTION 1 — IMPORTS ──────────────────────────────────────────────────────
# Use the exact same import paths as other strategies.

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from backtester.core.option_selection import select_by_delta
from backtester.core.expiry_utils import expiry_dt_utc, select_expiry
from backtester.core.pricing import deribit_fee_per_leg, EXPIRY_HOUR_UTC
from backtester.core.strategy_base import (
    OpenPosition,
    Trade,
    check_expiry,
    close_position,
    price_legs,
    profit_target_pct,
    stop_loss_pct,
)

logger = logging.getLogger(__name__)


# ── SECTION 2 — PARAMETER GRID ───────────────────────────────────────────────
#
# PARAM_GRID is the wide, unbiased discovery grid.  The engine expands it into
# every combination and runs each combo over DATE_RANGE in a single data pass.
#
# For this blueprint the grid has one combo per direction so you can see both
# a long and a short trade execute on the same underlying day.
#
# Parameter reference:
#   entry_date      — "YYYY-MM-DD".  Strategy only opens on this exact UTC date.
#   entry_hour_utc  — UTC hour (0–23).  First tick at or after this hour triggers entry.
#   direction       — "long"  → BUY call + BUY put  (debit strangle)
#                     "short" → SELL call + SELL put (credit strangle)
#   delta           — Target |delta| per leg.  0.10 ≈ 10-delta (OTM).
#   dte             — Calendar days to target expiry.  1 = next-day 08:00 UTC expiry.
#   stop_loss_pct   — Fraction of entry premium allowed as max loss.
#                     0.50 → fire when loss equals 50% of premium paid/received.
#                     See SL/TP section below for the exact formula.
#   take_profit_pct — Fraction of entry premium as target profit.
#                     0.30 → fire when profit equals 30% of premium paid/received.
#                     Set to 0.0 to disable.
#   qty             — Contracts per leg.  Always 1.0 in this blueprint.
#
# These thresholds are intentionally set tight (SL=150%, TP=30%) so that at
# least one triggers on most real market days and you can observe the log.

# ── SECTION 3 — STRATEGY CLASS ───────────────────────────────────────────────

class BlueprintHowto:
    """
    Single-trade strangle demonstrating all open/close/SL/TP patterns.

    Opens exactly one strangle on entry_date at entry_hour_utc (UTC).
    Closes via take-profit, stop-loss, expiry settlement, or end-of-data.

    This class intentionally has no indicator dependencies, no dynamic sizing,
    and no weekday filtering.  Those patterns are shown in short_str_turb_dyn.py.
    """

    name = "blueprint_howto"
    DATE_RANGE = ("2026-05-26", "2026-05-29")
    DESCRIPTION = (
        "Blueprint/how-to strategy.  Opens a fixed-date OTM strangle "
        "(long or short) with configurable SL/TP.  Intended as a teaching "
        "reference, not a discovery strategy."
    )

    # ── SECTION 2 — PARAMETER GRID ───────────────────────────────────────────
    #
    # PARAM_GRID is the wide, unbiased discovery grid.  The engine expands it
    # into every combination and runs each combo over DATE_RANGE in a single
    # data pass.
    #
    # For this blueprint the grid has one combo per direction so you can see
    # both a long and a short trade execute on the same underlying day.
    #
    # Parameter reference:
    #   entry_date      — "YYYY-MM-DD".  Opens only on this exact UTC date.
    #   entry_hour_utc  — UTC hour (0–23).  First tick at or after this hour.
    #   direction       — "long"  → BUY call + BUY put  (debit strangle)
    #                     "short" → SELL call + SELL put (credit strangle)
    #   delta           — Target |delta| per leg.  0.10 ≈ 10-delta (OTM).
    #   dte             — Calendar days to target expiry.  1 = next-day expiry.
    #   stop_loss_pct   — Fraction of entry premium allowed as max loss.
    #                     1.5 → fire when loss equals 150% of premium paid/received.
    #                     See SL/TP section in configure() for the exact formula.
    #   take_profit_pct — Fraction of entry premium as target profit.
    #                     0.30 → fire when profit equals 30% of premium paid/received.
    #                     Set to 0.0 to disable.
    #   qty             — Contracts per leg.  Always 1.0 in this blueprint.
    #
    # SL=150% and TP=30% are intentionally set so that at least one triggers
    # on most real market days and you can observe the log output.
    PARAM_GRID = {
        "entry_date":      ["2026-05-27"],
        "entry_hour_utc":  [15],
        "direction":       ["short", "long"],
        "delta":           [0.20],
        "dte":             [1],
        "stop_loss_pct":   [1.5],
        "take_profit_pct": [0.30],
        "qty":             [1.0],
    }
    # Optional per-param help shown in the Research UI New Run table.
    # Missing keys (or omitting PARAM_HELP entirely) show "—" in the help column.
    PARAM_HELP = {
        "entry_date":      "UTC date (YYYY-MM-DD) on which the trade may open.",
        "entry_hour_utc":  "UTC hour (0–23); first tick at or after this hour.",
        "direction":       '"short" = sell strangle; "long" = buy strangle.',
        "delta":           "Target |delta| per leg (0.20 ≈ 20-delta OTM).",
        "dte":             "Calendar days to target expiry (1 = next-day).",
        "stop_loss_pct":   "Max loss as fraction of entry premium (1.5 = 150%).",
        "take_profit_pct": "Target profit as fraction of premium (0 = disabled).",
        "qty":             "Contracts per leg.",
    }
    # To add an indicator: uncomment and follow the pattern in short_str_turb_dyn.py.
    # indicator_deps = [
    #     IndicatorDep(name="turbulence", symbol="BTCUSDT", interval="15m"),
    # ]

    def __init__(self):
        # Internal state — always reset in configure() too (grid re-runs the
        # same instance, so __init__ state leaking across combos is a common bug).
        self._positions  = []             # type: List[OpenPosition]  # canonical; read by engine NAV tracker
        self._opened     = False          # True once we've opened this combo's trade
        self._direction  = "short"
        self._delta      = 0.10
        self._dte        = 1
        self._sl_pct     = 1.5
        self._tp_pct     = 0.30
        self._qty        = 1.0
        self._entry_date = None           # datetime.date
        self._entry_hour = 18
        self._exit_conds = []             # type: List[Any]

    # ── LIFECYCLE: configure ─────────────────────────────────────────────────

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        """
        Called once per parameter combo before the tick loop.

        Always reset ALL mutable state here, not just in __init__.
        The engine reuses the same instance across grid combos.
        """
        self._direction  = params["direction"]          # "long" or "short"
        self._delta      = float(params["delta"])
        self._dte        = int(params["dte"])
        self._sl_pct     = float(params["stop_loss_pct"])
        self._tp_pct     = float(params.get("take_profit_pct", 0.0))
        self._qty        = float(params.get("qty", 1.0))
        self._entry_hour = int(params.get("entry_hour_utc", 18))
        self._entry_date = datetime.strptime(
            params["entry_date"], "%Y-%m-%d"
        ).date()

        # Reset position state for this combo run.
        self._positions = []
        self._opened    = False

        # ── Exit condition factories ─────────────────────────────────────────
        #
        # stop_loss_pct(p) and profit_target_pct(p) are closures that capture p
        # and return a callable (state, pos) → Optional[str].
        #
        # They read pos.metadata["direction"] ("buy" / "sell") to know which
        # way the loss/profit math runs:
        #
        #   SHORT ("sell"):
        #     SL:  fires when (current_buyback_cost - entry_premium) / entry_premium >= p
        #          current_buyback_cost is priced at mark (price_mode="mark").
        #          With p=1.5: fires when mark cost to close = 2.5× collected premium.
        #
        #     TP:  fires when (entry_premium - current_buyback_cost) / entry_premium >= p
        #          current_buyback_cost is priced at ask (price_mode="executable").
        #          With p=0.30: fires when 30% of premium has been retained.
        #
        #   LONG ("buy"):
        #     SL:  fires when (entry_premium - current_mark_value) / entry_premium >= p
        #          current_mark_value is priced at mark (price_mode="mark").
        #          With p=0.50: fires when mark value has dropped 50% below cost.
        #
        #     TP:  fires when (current_bid_value - entry_premium) / entry_premium >= p
        #          current_bid_value is priced at bid (price_mode="executable").
        #          With p=0.30: fires when bid value is 1.30× entry cost.
        #
        # price_modes:
        #   "mark"        — exchange model price; stable, not manipulable by wide spreads.
        #                   Use for SL so thin-book ask spikes don't trigger early exit.
        #   "executable"  — ask for sell legs, bid for buy legs.
        #                   Use for TP so it only fires when you can actually get that price.
        #   "bid" / "ask" — always that side regardless of leg direction.
        #
        # price_legs() returns None only if a quote row is missing from parquet
        # (genuine data gap → skip tick).  Zero-mark options are priced at $0.

        # SL uses mark prices (stable); TP uses executable prices (bid/ask).
        self._exit_conds = [stop_loss_pct(self._sl_pct, price_mode="mark")]
        if self._tp_pct > 0:
            self._exit_conds.append(profit_target_pct(self._tp_pct, price_mode="executable"))

    # ── LIFECYCLE: on_market_state ───────────────────────────────────────────

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        """
        Called on every 5-minute snapshot.  Returns a list of Trade objects:
          - An "open" Trade (side="open") when a position is entered.
          - A "close" Trade (side="close", pnl≠0) when a position exits.
          - An empty list when nothing happens this tick.

        The engine appends all returned trades to the result set for this combo.
        """
        trades = []

        # ── CHECK EXITS (always before entry) ───────────────────────────────
        #
        # Process exits first so that a position that triggers SL/TP on the
        # same tick it could theoretically re-enter doesn't do so.

        if self._positions:
            pos = self._positions[0]
            reason = self._check_exit(state, pos)
            if reason is not None:
                trades.append(self._do_close(state, pos, reason))
                self._positions = []

        # ── CHECK ENTRY ─────────────────────────────────────────────────────
        #
        # Only open once per combo (self._opened guards against re-entry after
        # a close).  Only on the configured date and hour.

        if not self._positions and not self._opened:
            if (state.dt.date() == self._entry_date
                    and state.dt.hour >= self._entry_hour):
                open_trade = self._do_open(state)
                if open_trade is not None:
                    trades.append(open_trade)

        return trades

    # ── LIFECYCLE: on_end ────────────────────────────────────────────────────

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        """
        Called once at the end of the replay (no more ticks).
        Force-close any still-open position so it appears in results.
        """
        if self._positions:
            trade = self._do_close(state, self._positions[0], "end_of_data")
            self._positions = []
            return [trade]
        return []

    # ── LIFECYCLE: reset ─────────────────────────────────────────────────────

    def reset(self):
        # type: () -> None
        """Reset state between grid combos.  Called by the engine automatically."""
        self._positions = []
        self._opened    = False

    def describe_params(self):
        # type: () -> Dict[str, Any]
        """Return current parameters for labelling results and reports."""
        return {
            "entry_date":      str(self._entry_date),
            "entry_hour_utc":  self._entry_hour,
            "direction":       self._direction,
            "delta":           self._delta,
            "dte":             self._dte,
            "stop_loss_pct":   self._sl_pct,
            "take_profit_pct": self._tp_pct,
            "qty":             self._qty,
        }

    # ── SECTION 4 — ENTRY: opening the position ──────────────────────────────

    def _do_open(self, state):
        # type: (Any) -> Optional[Trade]
        """
        Select options, build leg dicts, create OpenPosition, log everything.

        Returns an "open" Trade (side="open", pnl=0) that the engine turns
        into fill records.  Returns None if no tradable quotes exist this tick
        (engine will retry on the next 5-min tick automatically because
        self._opened remains False).
        """
        # ── Select expiry ─────────────────────────────────────────────────
        # select_expiry() returns just the expiry code string (e.g. "28MAY26"),
        # or None if no matching expiry exists in the chain.
        # expiry_dt_utc() converts the code to a UTC-aware datetime (08:00 UTC).
        expiry = select_expiry(state, self._dte)
        if expiry is None:
            return None  # no expiry available in the chain today
        exp_dt = expiry_dt_utc(expiry, state.dt.tzinfo)

        # ── Select legs by delta ──────────────────────────────────────────
        # get_chain() returns all options for this expiry as OptionQuote objects.
        # select_by_delta() picks the option whose |delta| is closest to target.
        # For a call: target_delta = +delta  (positive, e.g. +0.10)
        # For a put:  target_delta = -delta  (negative, e.g. -0.10)
        chain = state.get_chain(expiry)
        if not chain:
            return None

        call_q = select_by_delta(chain, +self._delta)
        put_q  = select_by_delta(chain, -self._delta)
        if call_q is None or put_q is None:
            return None

        # ── Price check ───────────────────────────────────────────────────
        # For SHORT: we sell at the bid.  If bid == 0 there is no buyer and we
        #            cannot open — skip this tick.
        # For LONG:  we buy at the ask.  If ask == 0 there is no seller —
        #            skip this tick.
        # Never open a position on a zero bid/ask — it means no real market.
        if self._direction == "short":
            if call_q.bid <= 0 or put_q.bid <= 0:
                return None
            call_entry_btc = call_q.bid
            put_entry_btc  = put_q.bid
        else:  # long
            if call_q.ask <= 0 or put_q.ask <= 0:
                return None
            call_entry_btc = call_q.ask
            put_entry_btc  = put_q.ask

        # ── Compute USD values ────────────────────────────────────────────
        # All internal accounting is in USD.  spot × btc_price = USD value.
        call_entry_usd = call_entry_btc * state.spot * self._qty
        put_entry_usd  = put_entry_btc  * state.spot * self._qty
        total_entry_usd = call_entry_usd + put_entry_usd

        # ── Fees ──────────────────────────────────────────────────────────
        # Deribit: MIN(0.03% × index, 12.5% × option_price) per leg per trade.
        fee_call = deribit_fee_per_leg(state.spot, call_entry_usd)
        fee_put  = deribit_fee_per_leg(state.spot, put_entry_usd)
        fees_open = (fee_call + fee_put) * self._qty

        # ── Leg dicts ─────────────────────────────────────────────────────
        # These fields are REQUIRED by close_trade() for leg-aware PnL.
        # Do NOT omit any of them.  Missing fields raise ValueError at close time.
        #
        #   strike        — float strike price in USD
        #   is_call       — True for call, False for put
        #   expiry        — expiry code string (e.g. "28MAY26")
        #   side          — "sell" for short legs, "buy" for long legs.
        #                   This is what price_legs() reads per-leg to decide
        #                   whether to use ask (sell) or bid (buy) in "executable" mode.
        #   qty           — number of contracts for this leg
        #   price_btc     — entry fill price in BTC (bid for short, ask for long)
        #   entry_price   — same as price_btc (legacy alias, keep both)
        #   entry_price_usd — entry value in USD (price_btc × spot × qty)
        #   entry_spot    — spot price at entry (used for USD accounting at close)
        #   entry_bid     — bid at entry (for the log — not used in PnL math)
        #   entry_ask     — ask at entry (for the log)
        #   entry_mark    — mark at entry (for the log)
        #   entry_iv      — implied vol at entry (for the log)
        #   entry_delta   — delta at entry (for the log)
        #   fee_usd_open  — opening fee for this leg (for the log)
        #   exit_price_btc — set at close time by _do_close()

        leg_side = "sell" if self._direction == "short" else "buy"

        legs = [
            {
                "strike":          call_q.strike,
                "is_call":         True,
                "expiry":          expiry,
                "side":            leg_side,
                "qty":             self._qty,
                "price_btc":       call_entry_btc,
                "entry_price":     call_entry_btc,
                "entry_price_usd": call_entry_usd,
                "entry_spot":      state.spot,
                "entry_bid":       call_q.bid,
                "entry_ask":       call_q.ask,
                "entry_mark":      call_q.mark,
                "entry_iv":        call_q.mark_iv,
                "entry_delta":     call_q.delta,
                "fee_usd_open":    fee_call * self._qty,
            },
            {
                "strike":          put_q.strike,
                "is_call":         False,
                "expiry":          expiry,
                "side":            leg_side,
                "qty":             self._qty,
                "price_btc":       put_entry_btc,
                "entry_price":     put_entry_btc,
                "entry_price_usd": put_entry_usd,
                "entry_spot":      state.spot,
                "entry_bid":       put_q.bid,
                "entry_ask":       put_q.ask,
                "entry_mark":      put_q.mark,
                "entry_iv":        put_q.mark_iv,
                "entry_delta":     put_q.delta,
                "fee_usd_open":    fee_put * self._qty,
            },
        ]

        # ── Build OpenPosition ────────────────────────────────────────────
        # metadata["direction"] MUST be "buy" or "sell" — this is how
        # stop_loss_pct() and profit_target_pct() know which way PnL flows.
        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=legs,
            entry_price_usd=total_entry_usd,
            fees_open=fees_open,
            metadata={
                "direction":   "sell" if self._direction == "short" else "buy",
                "expiry":      expiry,
                "expiry_dt":   exp_dt,
                "call_strike": call_q.strike,
                "put_strike":  put_q.strike,
                "call_delta":  call_q.delta,
                "put_delta":   put_q.delta,
                "qty":         self._qty,
                # SL/TP thresholds stored in metadata so the close log can
                # show them without recomputing.
                "sl_threshold_usd": total_entry_usd * (1.0 + self._sl_pct),
                "tp_threshold_usd": total_entry_usd * (1.0 - self._tp_pct),
            },
        )

        self._positions = [pos]
        self._opened    = True

        # ── Human-readable open log ───────────────────────────────────────
        self._log_open(state, pos, call_q, put_q)

        # ── Return "open" Trade ───────────────────────────────────────────
        # side="open" tells the engine this is an entry event (pnl excluded
        # from summary metrics).  All fields are still required.
        return Trade(
            entry_time=state.dt,
            exit_time=state.dt,
            entry_spot=state.spot,
            exit_spot=state.spot,
            entry_price_usd=total_entry_usd,
            exit_price_usd=0.0,
            fees=fees_open,
            pnl=0.0,
            triggered=False,
            exit_reason="",
            exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={
                "direction": "sell" if self._direction == "short" else "buy",
                "legs":      legs,
            },
        )

    # ── SECTION 5 — EXIT: checking and executing closes ──────────────────────

    def _check_exit(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[str]
        """
        Run all exit checks in priority order.  Returns the exit reason string
        or None to hold.

        Priority:
          1. Expiry settlement — always check first.  If the expiry datetime has
             passed, settle immediately regardless of SL/TP (the option has ceased
             to exist as a tradable instrument).
          2. SL / TP / other conditions — run in configured order.

        Data gap guard:
          If a non-expiry exit fires but one of the option quotes is missing from
          the snapshot (state.get_option returns None), we skip the tick rather
          than closing at a potentially garbage price.  This prevents phantom closes
          on ticks where the data vendor didn't capture a quote.
        """
        # 1. Expiry settlement
        reason = check_expiry(state, pos)
        if reason is not None:
            return reason

        # 2. Composable exit conditions
        for cond in self._exit_conds:
            reason = cond(state, pos)
            if reason is not None:
                break

        # 3. Data gap guard (skip non-expiry closes if quotes are missing)
        if reason is not None:
            expiry = pos.metadata["expiry"]
            call_missing = state.get_option(expiry, pos.metadata["call_strike"], True) is None
            put_missing  = state.get_option(expiry, pos.metadata["put_strike"],  False) is None
            if call_missing or put_missing:
                logger.debug(
                    "[%s] Exit reason '%s' suppressed — quote missing (call=%s put=%s)",
                    state.dt, reason, call_missing, put_missing,
                )
                return None  # retry next tick

        return reason

    def _do_close(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        """
        Set exit_price_btc on each leg, call close_position(), log the result.

        close_position() (from strategy_base) calls close_trade() internally,
        which computes leg-aware PnL using the leg dicts we built at entry:

          For a SHORT ("sell") leg:
            leg_pnl = (entry_price_btc × entry_spot − exit_price_btc × exit_spot) × qty

          For a LONG ("buy") leg:
            leg_pnl = (exit_price_btc × exit_spot − entry_price_btc × entry_spot) × qty

          Total trade PnL = sum(leg_pnl) − total_fees

        This is why all the leg fields set in _do_open() are required: the
        close function references them by key.
        """
        expiry      = pos.metadata["expiry"]
        call_strike = pos.metadata["call_strike"]
        put_strike  = pos.metadata["put_strike"]
        qty         = float(pos.metadata["qty"])

        call_q = state.get_option(expiry, call_strike, True)
        put_q  = state.get_option(expiry, put_strike,  False)

        if reason == "expiry":
            # ── Expiry settlement ────────────────────────────────────────
            # OTM options expire worthless (intrinsic = 0).
            # ITM options settle at intrinsic value: spot − strike (call),
            #                                        strike − spot (put).
            # Deribit uses the index price at 08:00 UTC as the settlement spot.
            # We use state.spot as the best available approximation.
            call_exit_btc = max(0.0, state.spot - call_strike) / state.spot if state.spot else 0.0
            put_exit_btc  = max(0.0, put_strike  - state.spot) / state.spot if state.spot else 0.0
            call_exit_usd = call_exit_btc * state.spot
            put_exit_usd  = put_exit_btc  * state.spot
            fees_close    = 0.0  # Deribit does not charge fees on expiry settlement
        else:
            # ── SL / TP / end-of-data close ──────────────────────────────
            # Close at the current ask (for short legs: cost to buy back)
            #                  or bid (for long legs:  proceeds from selling).
            # Use a $0.0001 BTC floor so the trade closes even if the
            # market has no visible ask (the option is deep OTM / worthless).
            _floor_btc = 0.0001
            _floor_usd = _floor_btc * state.spot

            if self._direction == "short":
                # Short legs: we buy them back → use ask price
                call_exit_btc = (call_q.ask if call_q and call_q.ask > 0 else _floor_btc)
                put_exit_btc  = (put_q.ask  if put_q  and put_q.ask  > 0 else _floor_btc)
            else:
                # Long legs: we sell them → use bid price
                call_exit_btc = (call_q.bid if call_q and call_q.bid > 0 else _floor_btc)
                put_exit_btc  = (put_q.bid  if put_q  and put_q.bid  > 0 else _floor_btc)

            call_exit_usd = call_exit_btc * state.spot
            put_exit_usd  = put_exit_btc  * state.spot
            fee_call  = deribit_fee_per_leg(state.spot, call_exit_usd)
            fee_put   = deribit_fee_per_leg(state.spot, put_exit_usd)
            fees_close = (fee_call + fee_put) * qty

        # ── Stamp exit prices onto the leg dicts ──────────────────────────
        # close_trade() reads leg["exit_price_btc"] from here.
        # Do NOT set these before this method — it would corrupt in-flight PnL.
        for leg in pos.legs:
            if leg["is_call"]:
                leg["exit_price_btc"] = call_exit_btc
                leg["exit_price_usd"] = call_exit_usd
            else:
                leg["exit_price_btc"] = put_exit_btc
                leg["exit_price_usd"] = put_exit_usd

        total_exit_usd = (call_exit_usd + put_exit_usd) * qty

        # ── Call close_position() ─────────────────────────────────────────
        # close_position() is a thin wrapper around close_trade() that also
        # handles the fill linkage (open_idx → close_idx for the fills report).
        # Always prefer close_position() over close_trade() directly.
        trade = close_position(
            state, pos, reason,
            current_usd=total_exit_usd,
            fees_close=fees_close,
        )

        # Attach useful metadata for the HTML report and fills parquet.
        trade.metadata.update({
            "direction":       self._direction,
            "delta":           self._delta,
            "dte":             self._dte,
            "stop_loss_pct":   self._sl_pct,
            "take_profit_pct": self._tp_pct,
            "qty":             qty,
        })

        self._log_close(state, pos, trade, call_q, put_q,
                        call_exit_btc, put_exit_btc, reason)
        return trade

    # ── SECTION 6 — LOGGING ──────────────────────────────────────────────────
    #
    # Both _log_open and _log_close emit structured tables that a human can
    # follow with access to TradingView (BTC spot) and the option parquet files.
    #
    # Machine-readable: all values are in logger.info structured lines tagged
    # with [blueprint_howto] for easy grepping.  Also written to trade metadata.
    #
    # Human-readable: printed table with bid / ask / mark / IV / delta / spot.

    def _log_open(self, state, pos, call_q, put_q):
        # type: (Any, OpenPosition, Any, Any) -> None
        direction_label = "SHORT (sell)" if self._direction == "short" else "LONG (buy)"
        entry_price     = call_q.bid if self._direction == "short" else call_q.ask
        entry_price_put = put_q.bid  if self._direction == "short" else put_q.ask
        sl_usd = pos.metadata["sl_threshold_usd"]
        tp_usd = pos.metadata["tp_threshold_usd"]

        lines = [
            f"\n{'─'*72}",
            f"  OPEN  [{direction_label}]  {state.dt.strftime('%Y-%m-%d %H:%M UTC')}",
            f"  Expiry: {pos.metadata['expiry']}  |  Spot: ${state.spot:,.2f}",
            f"  {'Leg':<22} {'Strike':>8}  {'Side':>5}  {'Bid':>8}  {'Ask':>8}  {'Mark':>8}  {'IV%':>6}  {'Delta':>7}  {'Fill BTC':>10}  {'Fill USD':>10}",
            f"  {'-'*118}",
            f"  {'CALL ' + str(int(call_q.strike)) + '-C':<22} {call_q.strike:>8.0f}  {('sell' if self._direction == 'short' else 'buy'):>5}  {call_q.bid:>8.4f}  {call_q.ask:>8.4f}  {call_q.mark:>8.4f}  {call_q.mark_iv:>6.1f}  {call_q.delta:>7.4f}  {entry_price:>10.4f}  ${entry_price * state.spot * self._qty:>9.2f}",
            f"  {'PUT  ' + str(int(put_q.strike))  + '-P':<22} {put_q.strike:>8.0f}  {('sell' if self._direction == 'short' else 'buy'):>5}  {put_q.bid:>8.4f}  {put_q.ask:>8.4f}  {put_q.mark:>8.4f}  {put_q.mark_iv:>6.1f}  {put_q.delta:>7.4f}  {entry_price_put:>10.4f}  ${entry_price_put * state.spot * self._qty:>9.2f}",
            f"  {'-'*118}",
            f"  Entry premium (total):  ${pos.entry_price_usd:>10.2f}  ({pos.entry_price_usd / state.spot:.6f} BTC)",
            f"  SL threshold:           ${sl_usd:>10.2f}  (entry × {1 + self._sl_pct:.2f} = loss of {self._sl_pct:.0%} of premium)",
        ]
        if self._tp_pct > 0:
            lines.append(
                f"  TP threshold:           ${tp_usd:>10.2f}  (entry × {1 - self._tp_pct:.2f} = profit of {self._tp_pct:.0%} of premium)"
            )
        lines.append(f"{'─'*72}")

        print("\n".join(lines))

        logger.info(
            "[blueprint_howto] OPEN direction=%s dt=%s expiry=%s "
            "call_strike=%.0f put_strike=%.0f spot=%.2f "
            "call_fill_btc=%.4f put_fill_btc=%.4f entry_usd=%.2f "
            "sl_usd=%.2f tp_usd=%.2f fees_open=%.2f",
            self._direction, state.dt.isoformat(), pos.metadata["expiry"],
            pos.metadata["call_strike"], pos.metadata["put_strike"], state.spot,
            entry_price, entry_price_put, pos.entry_price_usd,
            sl_usd, tp_usd if self._tp_pct > 0 else 0.0, pos.fees_open,
        )

    def _log_close(self, state, pos, trade, call_q, put_q,
                   call_exit_btc, put_exit_btc, reason):
        # type: (Any, OpenPosition, Trade, Any, Any, float, float, str) -> None
        held_h  = trade.exit_hour
        held_m  = int((state.dt - pos.entry_time).total_seconds() % 3600 / 60)
        sl_ratio = (
            (trade.exit_price_usd - pos.entry_price_usd) / max(pos.entry_price_usd, 0.01)
            if self._direction == "short"
            else (pos.entry_price_usd - trade.exit_price_usd) / max(pos.entry_price_usd, 0.01)
        )

        # Current quote values at close (may be None for expiry)
        def _fmt_quote(q):
            # type: (Any) -> str
            if q is None:
                return "bid=----  ask=----  mark=----  IV=----  delta=----"
            return (f"bid={q.bid:.4f}  ask={q.ask:.4f}  mark={q.mark:.4f}"
                    f"  IV={q.mark_iv:.1f}%  delta={q.delta:.4f}")

        lines = [
            f"\n{'─'*72}",
            f"  CLOSE [{reason.upper()}]  {state.dt.strftime('%Y-%m-%d %H:%M UTC')}  (held {held_h}h {held_m}m)",
            f"  Expiry: {pos.metadata['expiry']}  |  Spot at close: ${state.spot:,.2f}",
            f"  {'Leg':<22} {'Strike':>8}  {'Side':>5}  {'Exit BTC':>10}  {'Exit USD':>10}  Current market",
            f"  {'-'*100}",
            f"  {'CALL ' + str(int(pos.metadata['call_strike'])) + '-C':<22} {pos.metadata['call_strike']:>8.0f}  {('buy' if self._direction == 'short' else 'sell'):>5}  {call_exit_btc:>10.4f}  ${call_exit_btc * state.spot * pos.metadata['qty']:>9.2f}  {_fmt_quote(call_q)}",
            f"  {'PUT  ' + str(int(pos.metadata['put_strike']))  + '-P':<22} {pos.metadata['put_strike']:>8.0f}  {('buy' if self._direction == 'short' else 'sell'):>5}  {put_exit_btc:>10.4f}  ${put_exit_btc * state.spot * pos.metadata['qty']:>9.2f}  {_fmt_quote(put_q)}",
            f"  {'-'*100}",
            f"  Exit cost / proceeds:   ${trade.exit_price_usd:>10.2f}",
            f"  Entry premium:          ${pos.entry_price_usd:>10.2f}",
            f"  Gross PnL:              ${trade.pnl + trade.fees:>10.2f}",
            f"  Fees (total):           ${trade.fees:>10.2f}",
            f"  Net PnL:                ${trade.pnl:>10.2f}",
            f"  Loss/profit ratio:      {sl_ratio:+.2f}×  (SL threshold was {self._sl_pct:.2f}×)",
            f"{'─'*72}",
        ]

        print("\n".join(lines))

        logger.info(
            "[blueprint_howto] CLOSE direction=%s reason=%s dt=%s "
            "spot=%.2f call_exit_btc=%.4f put_exit_btc=%.4f "
            "exit_usd=%.2f entry_usd=%.2f pnl=%.2f fees=%.2f ratio=%.4f",
            self._direction, reason, state.dt.isoformat(),
            state.spot, call_exit_btc, put_exit_btc,
            trade.exit_price_usd, pos.entry_price_usd,
            trade.pnl, trade.fees, sl_ratio,
        )

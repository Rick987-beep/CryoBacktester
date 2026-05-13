#!/usr/bin/env python3
"""
hedged_put_sell.py — Sell puts on a schedule; hedge with a long put when delta
spikes; close hedge when delta recovers. Exit on take profit or expiry.

Entry:  sell OTM put at target_delta with target DTE (closest available) on a
        specific weekday + UTC hour.  Two independent entry gates must both pass:
          Gate A — weekday + hour match.
          Gate B — cooldown_days calendar days have elapsed since the last entry.
        Up to max_concurrent positions may be open simultaneously; each is
        treated as a fully independent position with its own hedge state.
Hedge:  open long put (same expiry, selected by hedge_delta) when the short put's
        abs(delta) crosses hedge_trigger_delta.  Qty = round(main_delta / hedge_delta)
        to the nearest 0.1 Deribit contract (≥ 0.1), making the position delta-neutral.
        Multiple hedge cycles per position are allowed.
Exit:   close main put when take_profit_pct of the entry credit has been captured,
        or at expiry (intrinsic-value settlement).  Any open hedge is closed first.

Accounting:
  - pos.entry_price_usd  = initial short-put bid (USD)
  - pos.fees_open        = entry fees only (not hedge fees)
  - Hedge PnL and fees live entirely in the hedge's own Trade records (status 6/7/8);
    they are NOT accumulated onto the parent put position.
  - Trade.pnl (main put) = (entry_credit − buyback_cost) − (entry_fees + close_fees)
"""
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import parse_expiry_date, expiry_dt_utc
from backtester.pricing import deribit_fee_per_leg
from backtester.strategy_base import OpenPosition, Trade


_CONTRACT_STEP = 0.1   # Deribit minimum contract size (BTC options)


def _round_contracts(qty):
    # type: (float) -> float
    """Round qty to the nearest 0.1 Deribit contract, minimum 0.1."""
    n = round(qty / _CONTRACT_STEP)
    return max(1, n) * _CONTRACT_STEP


def _select_closest_dte_expiry(state, target_dte):
    # type: (Any, int) -> Optional[str]
    """Return the available expiry whose DTE is closest to target_dte (DTE > 0 only)."""
    today = state.dt.date()
    best_exp, best_diff = None, None
    for exp in state.expiries():
        d = parse_expiry_date(exp)
        if d is None:
            continue
        dte = (d.date() - today).days
        if dte <= 0:
            continue  # already expired or expiring today
        diff = abs(dte - target_dte)
        if best_diff is None or diff < best_diff:
            best_exp, best_diff = exp, diff
    return best_exp


class HedgedPutSell:
    """Sell a put on a schedule; hedge dynamically on delta spike; TP or expiry exit."""

    name = "hedged_put_sell"
    DESCRIPTION = (
        "Sells an OTM put at a target delta on a weekday/hour schedule. "
        "Dynamically hedges with a long put (same expiry, near ATM) when the "
        "short put delta spikes past a threshold. Closes the hedge when delta "
        "recovers. Multiple hedge cycles per position are supported. "
        "Exits on take profit (% of initial credit) or at expiry."
    )

    DATE_RANGE = ("2025-12-09", "2026-05-09")

    # Named weekday schedules — edit here to add/remove options.
    # Each value is a tuple of allowed ISO weekday integers (0=Mon … 4=Fri).
    ENTRY_SCHEDULES = {
        "fri":          (4,),
        "mon":          (0,),
        "mon_fri":      (0, 4),
        "mon_wed_fri":  (0, 2, 4),
        "any_weekday":  (0, 1, 2, 3, 4),
    }

    # Reason codes for Trade.status — one per event, picked up by the report.
    # 1–5: main put events; 6–8: hedge events.
    TRADE_STATUS = {
        1: "put-open",
        2: "put-tp",
        3: "put-sl",
        4: "put-expiry",
        5: "put-end-data",
        6: "hedge-open",
        7: "hedge-close",
        8: "hedge-forced",
    }

    PARAM_GRID = {
        "entry_delta":          [0.15, 0.20],  # abs(put delta) at entry
        "dte":                  [7],
        "entry_day":            ["mon_wed_fri"],
        "entry_hour":           [12],      # UTC
        "cooldown_days":        [1],       # min calendar days between entries
        "max_concurrent":       [3],       # max open positions at once
        "hedge_trigger_delta":  [0.48],    # 0 = no hedging; else open hedge when abs(parent put delta) >= this
        "hedge_delta":          [0.30],    # abs(parent put delta) for hedge leg
        "hedge_close_delta":    [0.30],       # close hedge when abs(parent put delta) recovers <= this
        "take_profit_pct":      [0, 0.7], # take profit
    }

    def __init__(self):
        self._positions = []            # type: List[OpenPosition]
        self._entry_delta = 0.15
        self._dte = 7
        self._entry_day = "fri"
        self._entry_hour = 14
        self._cooldown_days = 7
        self._max_concurrent = 1
        self._hedge_trigger = 0.45
        self._hedge_delta = 0.50
        self._hedge_close_delta = 0.30
        self._take_profit_pct = 0.50
        self._last_entry_date = None    # date of the most recent entry
        self._pos_counter = 0           # monotonic position ID for open/close linkage

    # ------------------------------------------------------------------
    # Protocol
    # ------------------------------------------------------------------

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._entry_delta = params["entry_delta"]
        self._dte = params["dte"]
        self._entry_day = params["entry_day"]
        self._entry_hour = params["entry_hour"]
        self._cooldown_days = params["cooldown_days"]
        self._max_concurrent = params["max_concurrent"]
        self._hedge_trigger = params["hedge_trigger_delta"]
        self._hedge_delta = params["hedge_delta"]
        self._hedge_close_delta = params["hedge_close_delta"]
        self._take_profit_pct = params["take_profit_pct"]
        self._positions = []
        self._last_entry_date = None
        self._pos_counter = 0

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []
        still_open = []
        for pos in self._positions:
            main_trade, side_trades = self._manage(state, pos)
            trades.extend(side_trades)
            if main_trade is not None:
                trades.append(main_trade)
            else:
                still_open.append(pos)
        self._positions = still_open
        if self._should_enter(state):
            open_trade = self._open(state)
            if open_trade is not None:
                trades.append(open_trade)
        return trades

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        trades = []
        for pos in self._positions:
            if pos.metadata.get("hedge_pos") is not None:
                trades.append(self._close_hedge(state, pos, clean=False))
            trades.append(self._close(state, "end_of_data", pos))
        self._positions = []
        return trades

    def reset(self):
        # type: () -> None
        self._positions = []
        self._last_entry_date = None
        self._pos_counter = 0

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "entry_delta":          self._entry_delta,
            "dte":                  self._dte,
            "entry_day":            self._entry_day,
            "entry_hour":           self._entry_hour,
            "cooldown_days":        self._cooldown_days,
            "max_concurrent":       self._max_concurrent,
            "hedge_trigger_delta":  self._hedge_trigger,
            "hedge_delta":          self._hedge_delta,
            "hedge_close_delta":    self._hedge_close_delta,
            "take_profit_pct":      self._take_profit_pct,
        }

    # ------------------------------------------------------------------
    # Entry
    # ------------------------------------------------------------------

    def _should_enter(self, state):
        # type: (Any) -> bool
        """True when weekday/hour gate and cooldown gate both pass and capacity allows."""
        allowed = self.ENTRY_SCHEDULES[self._entry_day]
        if state.dt.weekday() not in allowed:
            return False
        if state.dt.hour != self._entry_hour:
            return False
        if len(self._positions) >= self._max_concurrent:
            return False
        if self._last_entry_date is not None:
            if (state.dt.date() - self._last_entry_date) < timedelta(days=self._cooldown_days):
                return False
        return True

    def _open(self, state):
        # type: (Any) -> Optional[Trade]
        expiry = _select_closest_dte_expiry(state, self._dte)
        if expiry is None:
            return

        chain = state.get_chain(expiry)
        if not chain:
            return

        puts = [q for q in chain
                if not q.is_call and q.delta is not None and q.delta < 0]
        if not puts:
            return

        best = select_by_delta(puts, -self._entry_delta)  # raw put delta is negative
        if best is None or best.bid <= 0:
            return

        entry_usd = best.bid_usd
        if entry_usd < 1.0:
            return

        fees = deribit_fee_per_leg(state.spot, entry_usd)
        exp_dt = expiry_dt_utc(expiry, state.dt.tzinfo)
        self._last_entry_date = state.dt.date()
        self._pos_counter += 1
        _pos_id = self._pos_counter

        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=[{
                "strike": best.strike,
                "is_call": False,
                "expiry": expiry,
                "side": "sell",
                "qty": 1.0,
                "entry_price": best.bid,
                "entry_price_usd": entry_usd,
            }],
            entry_price_usd=entry_usd,
            fees_open=fees,
            metadata={
                "direction": "sell",
                "expiry": expiry,
                "expiry_dt": exp_dt,
                "strike": best.strike,
                "entry_delta": best.delta,
                "hedge_pos": None,
                "hedge_cycle_count": 0,
                "pos_id": _pos_id,
            },
        )
        self._positions.append(pos)
        return Trade(
            entry_time=state.dt,
            exit_time=state.dt,
            entry_spot=state.spot,
            exit_spot=state.spot,
            entry_price_usd=entry_usd,
            exit_price_usd=0.0,
            fees=fees,
            pnl=0.0,
            triggered=False,
            exit_reason="",
            exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            status=1,   # put-open
            side="open",
            metadata={"legs": pos.legs, "pos_id": _pos_id},
        )

    # ------------------------------------------------------------------
    # Per-tick management
    # ------------------------------------------------------------------

    def _manage(self, state, pos):
        # type: (Any, OpenPosition) -> Tuple[Optional[Trade], List[Trade]]
        """Tick-level management.  Returns (main_trade_or_None, side_trades).

        side_trades may contain hedge open and close Trades even when the
        main position is still open.  Callers must emit side_trades regardless."""
        side_trades = []  # type: List[Trade]
        expiry = pos.metadata["expiry"]
        strike = pos.metadata["strike"]

        # Expiry
        exp_dt = pos.metadata.get("expiry_dt")
        if exp_dt is not None and state.dt >= exp_dt:
            if pos.metadata["hedge_pos"] is not None:
                side_trades.append(self._close_hedge(state, pos, clean=False))
            return self._close(state, "expiry", pos), side_trades

        # Short put quote
        quote = state.get_option(expiry, strike, is_call=False)
        if quote is None:
            return None, side_trades   # no data this tick — hold

        ask_usd = quote.ask_usd if quote.ask > 0 else quote.mark_usd

        # Take profit: % of entry credit captured (0 = disabled)
        if self._take_profit_pct > 0:
            gain_pct = (pos.entry_price_usd - ask_usd) / max(pos.entry_price_usd, 0.01)
            if gain_pct >= self._take_profit_pct:
                if pos.metadata["hedge_pos"] is not None:
                    side_trades.append(self._close_hedge(state, pos, clean=False))
                return self._close(state, "take_profit", pos), side_trades

        # Hedge management — multiple cycles allowed.
        # Skip entirely when delta is unavailable: None means no data, not "recovered".
        # Also suppress delta-recovery close within 30 min of expiry — greeks become
        # unreliable near expiry and the position will be settled cleanly at the expiry bar.
        exp_dt = pos.metadata.get("expiry_dt")
        near_expiry = exp_dt is not None and (exp_dt - state.dt).total_seconds() <= 1800
        if quote.delta is not None:
            current_abs_delta = abs(quote.delta)
            hedge_pos = pos.metadata["hedge_pos"]
            if hedge_pos is not None:
                if not near_expiry and current_abs_delta <= self._hedge_close_delta:   # delta recovered toward OTM
                    side_trades.append(self._close_hedge(state, pos, clean=True))
            else:
                if self._hedge_trigger != 0 and current_abs_delta >= self._hedge_trigger:  # delta spiked toward ITM
                    ht = self._open_hedge(state, pos)
                    if ht is not None:
                        side_trades.append(ht)

        # NAV reprice: net cost to flatten = buy back short put − sell hedge
        hedge_bid_total = 0.0
        hedge_pos = pos.metadata["hedge_pos"]   # re-read: may have just changed
        if hedge_pos is not None:
            hq = state.get_option(expiry, hedge_pos.metadata["strike"], is_call=False)
            if hq is not None and hq.bid > 0:
                hedge_bid_total = hq.bid_usd * hedge_pos.metadata["qty"]
        pos._last_reprice_usd = max(0.0, ask_usd - hedge_bid_total)

        return None, side_trades

    def _open_hedge(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[Trade]
        """Buy a long put to delta-hedge the short put.

        Creates a new OpenPosition stored in pos.metadata["hedge_pos"] and
        returns an open Trade for the fills log."""
        expiry = pos.metadata["expiry"]

        mq = state.get_option(expiry, pos.metadata["strike"], is_call=False)
        if mq is None or mq.delta is None:
            return
        short_delta_abs = abs(mq.delta)  # magnitude for qty calculation

        chain = state.get_chain(expiry)
        if not chain:
            return

        candidates = [q for q in chain
                      if not q.is_call and q.delta is not None and q.delta < 0]
        if not candidates:
            return

        hq = select_by_delta(candidates, -self._hedge_delta)
        if hq.ask <= 0:
            return

        hedge_qty = _round_contracts(short_delta_abs / abs(hq.delta))
        fees = deribit_fee_per_leg(state.spot, hq.ask_usd) * hedge_qty

        hedge_pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=[{
                "strike": hq.strike,
                "is_call": False,
                "expiry": expiry,
                "side": "buy",
                "qty": hedge_qty,
                "entry_price": hq.ask,
                "entry_price_usd": hq.ask_usd,
            }],
            entry_price_usd=hq.ask_usd * hedge_qty,
            fees_open=fees,
            metadata={
                "is_hedge": True,
                "direction": "buy",
                "expiry": expiry,
                "strike": hq.strike,
                "qty": hedge_qty,
            },
        )
        pos.metadata["hedge_pos"] = hedge_pos
        pos.metadata["hedge_cycle_count"] += 1
        # Hedge gets its own pos_id: parent_pos_id * 1000 + cycle_count (unique, traceable)
        _hedge_pos_id = pos.metadata["pos_id"] * 1000 + pos.metadata["hedge_cycle_count"]
        hedge_pos.metadata["pos_id"] = _hedge_pos_id
        return Trade(
            entry_time=state.dt,
            exit_time=state.dt,
            entry_spot=state.spot,
            exit_spot=state.spot,
            entry_price_usd=hq.ask_usd * hedge_qty,
            exit_price_usd=0.0,
            fees=fees,
            pnl=0.0,
            triggered=False,
            exit_reason="",
            exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            status=6,   # hedge-open
            side="open",
            metadata={"legs": hedge_pos.legs, "pos_id": _hedge_pos_id},
        )

    def _close_hedge(self, state, pos, clean=True):
        # type: (Any, OpenPosition, bool) -> Trade
        """Close the open hedge position and return a Trade for the fills log.

        clean=True: delta recovered (normal cycle end).
        clean=False: forced close (main position is also closing, or expiry)."""
        hedge_pos = pos.metadata["hedge_pos"]
        expiry = hedge_pos.metadata["expiry"]
        h_strike = hedge_pos.metadata["strike"]
        h_qty = hedge_pos.metadata["qty"]

        # At expiry settle at intrinsic; otherwise use market bid/mark
        exp_dt = pos.metadata.get("expiry_dt")
        at_expiry = exp_dt is not None and state.dt >= exp_dt
        if at_expiry:
            close_usd_per = max(0.0, h_strike - state.spot)
            fees_close = 0.0
        else:
            hq = state.get_option(expiry, h_strike, is_call=False)
            if hq is None:
                close_usd_per = 0.0
            elif hq.bid > 0:
                close_usd_per = hq.bid_usd
            else:
                close_usd_per = hq.mark_usd
            fees_close = deribit_fee_per_leg(state.spot, close_usd_per) * h_qty if close_usd_per > 0 else 0.0

        close_usd = close_usd_per * h_qty
        total_fees = hedge_pos.fees_open + fees_close
        net_pnl = (close_usd - hedge_pos.entry_price_usd) - total_fees

        held_s = (state.dt - hedge_pos.entry_time).total_seconds()
        hedge_trade = Trade(
            entry_time=hedge_pos.entry_time,
            exit_time=state.dt,
            entry_spot=hedge_pos.entry_spot,
            exit_spot=state.spot,
            entry_price_usd=hedge_pos.entry_price_usd,
            exit_price_usd=close_usd,
            fees=total_fees,
            pnl=net_pnl,
            triggered=False,
            exit_reason="hedge_clean" if clean else "hedge_forced",
            exit_hour=int(held_s / 3600),
            entry_date=hedge_pos.entry_time.strftime("%Y-%m-%d"),
            status=7 if clean else 8,   # hedge-close / hedge-forced
            side="close",
            metadata={
                "is_hedge": True,
                "skip_open_fill": True,
                "legs": hedge_pos.legs,
                "pos_id": hedge_pos.metadata.get("pos_id"),
            },
        )
        pos.metadata["hedge_pos"] = None
        return hedge_trade

    # ------------------------------------------------------------------
    # Position close
    # ------------------------------------------------------------------

    def _close(self, state, reason, pos):
        # type: (Any, str, OpenPosition) -> Trade
        """Close the short put and return a Trade.

        Any open hedge must already be closed and its Trade emitted
        before calling this method."""
        expiry = pos.metadata["expiry"]
        strike = pos.metadata["strike"]

        if reason == "expiry":
            short_put_close_usd = max(0.0, strike - state.spot)
            fees_close = 0.0
        else:
            sq = state.get_option(expiry, strike, is_call=False)
            if sq is not None and sq.ask > 0:
                short_put_close_usd = sq.ask_usd
            elif sq is not None:
                short_put_close_usd = sq.mark_usd
            else:
                short_put_close_usd = pos.entry_price_usd  # no quote: assume flat
            fees_close = deribit_fee_per_leg(state.spot, short_put_close_usd)

        total_fees = pos.fees_open + fees_close
        net_pnl = (pos.entry_price_usd - short_put_close_usd) - total_fees

        held_s = (state.dt - pos.entry_time).total_seconds()

        _exit_status = {
            "take_profit": 2,
            "stop_loss":   3,
            "expiry":      4,
            "end_of_data": 5,
        }.get(reason, 0)

        meta = {k: v for k, v in pos.metadata.items() if k != "hedge_pos"}
        meta.update({
            "legs": pos.legs,
            "fees_open": pos.fees_open,
            "skip_open_fill": True,
            "take_profit_pct": self._take_profit_pct,
            "hedge_trigger_delta": self._hedge_trigger,
            "hedge_delta": self._hedge_delta,
            "hedge_close_delta": self._hedge_close_delta,
            # pos_id already in meta via pos.metadata
        })
        return Trade(
            entry_time=pos.entry_time,
            exit_time=state.dt,
            entry_spot=pos.entry_spot,
            exit_spot=state.spot,
            entry_price_usd=pos.entry_price_usd,
            exit_price_usd=short_put_close_usd,
            fees=total_fees,
            pnl=net_pnl,
            triggered=(reason == "take_profit"),
            exit_reason=reason,
            exit_hour=int(held_s / 3600),
            entry_date=pos.entry_time.strftime("%Y-%m-%d"),
            status=_exit_status,
            side="close",
            metadata=meta,
        )

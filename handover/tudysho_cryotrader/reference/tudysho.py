#!/usr/bin/env python3
"""
TuDySho — turbulence-gated short premium on Deribit BTC options.

Opens delta-selected OTM structures (strangle, naked call, or naked put) on an
N-DTE expiry.  Premium target scales with USD NAV; contract cap scales with BTC
equity.  Positions exit at expiry or via an independent three-rule stop-loss layer.

Leg types (``leg_type``):
    strangle — sell OTM call + put (default)
    call     — sell OTM call only
    put      — sell OTM put only

Entry (at most one new position per calendar day):
    • ``entry_time`` — NYC wall-clock, converted to UTC each tick (DST-aware).
    • After entry_time, open when turbulence composite < ``turbulence_threshold``.
    • Missing / NaN turbulence → fail-open (treat as calm).
    • ``trade_monday`` … ``trade_sunday`` (0/1) gate entry on each weekday
      (Mon=0 … Sun=6).  Defaults: Mon–Fri on, Sat–Sun off.
    • ``min_otm_pct`` pushes delta-selected strikes further OTM (0 = off).
    • ``leg_min_price`` — minimum bid in BTC per leg (0 = positive-bid guard only).

Sizing:
    • ``nav_premium_pct`` — target premium as % of USD NAV (``state.nav_usd``; 0 = fixed
      1 contract).  NAV includes open PnL so the income target tracks current book value.
    • ``max_qty_per_1btc_equity`` — hard cap on BTC contracts per 1 BTC of realized
      equity (``state.equity_usd / state.spot``).  0 = no cap (premium target only).
      Uses realized equity (not NAV) — same basis as equity drawdown SL.

Exits (first match in ``_check_exit``):

    1. Expiry — settlement at 08:00 UTC on ``expiry_dt``.

    2. Stop-loss layer — each rule omitted when its threshold or hours param is 0:

       a. Strike proximity — final ``proximity_stop_hours`` before expiry;
          spot vs strike ± ``proximity_buffer_usd`` (spot-only, no quotes needed).

       b. Equity drawdown — mark loss vs ``equity_at_entry_usd`` at
          ``equity_drawdown_stop_pct``; optional windows via
          ``equity_sl_only_final_hours`` / ``equity_sl_except_final_hours``.

       c. Premium multiplier — ``stop_loss_pct`` on mark; optional suppress in
          final ``premium_sl_except_final_hours`` before expiry.

    Quote guard: premium and equity stops need option quotes; proximity needs spot.

Indicator: turbulence(BTCUSDT 15m) hourly composite via ``set_indicators``.
"""
import logging
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from backtester.core.option_selection import select_by_delta
from backtester.core.expiry_utils import expiry_dt_utc, select_expiry
from backtester.core.config import cfg as _cfg
from backtester.indicators import IndicatorDep
from backtester.core.pricing import deribit_fee_per_leg
from backtester.core.strategy_base import (
    OpenPosition,
    Trade,
    check_expiry,
    close_position,
    stop_loss_pct,
    strike_proximity_stop,
    position_quotes_available,
    equity_drawdown_stop,
    exit_expiry_window,
)
from backtester.core.market_hours import to_nyc, to_utc


logger = logging.getLogger(__name__)

# Minimum BTC tick used when ask is missing on early close.
_MIN_TICK_BTC = 0.0001

# Exit reasons that do not require option quote rows (see _check_exit).
_QUOTE_FREE_EXIT_REASONS = frozenset({"strike_proximity_stop"})

# (param name, default) for each weekday Mon=0 … Sun=6.
_TRADE_WEEKDAY_DEFAULTS = (
    ("trade_monday", 1),
    ("trade_tuesday", 1),
    ("trade_wednesday", 1),
    ("trade_thursday", 1),
    ("trade_friday", 1),
    ("trade_saturday", 0),
    ("trade_sunday", 0),
)


def _entry_days_from_params(params):
    # type: (Dict[str, Any]) -> frozenset
    """Build allowed weekday ints from ``trade_*`` toggles."""
    return frozenset(
        wd
        for wd, (key, default) in enumerate(_TRADE_WEEKDAY_DEFAULTS)
        if int(params.get(key, default))
    )


def _apply_min_otm(chain, selected, spot, min_pct, is_call):
    # type: (list, Any, float, float, bool) -> Optional[Any]
    """Push a delta-selected quote further OTM if inside ``min_pct`` of spot."""
    factor = min_pct / 100.0
    if is_call:
        floor = spot * (1.0 + factor)
        if selected.strike >= floor:
            return selected
        candidates = sorted(
            [q for q in chain if q.strike >= floor],
            key=lambda q: q.strike,
        )
    else:
        floor = spot * (1.0 - factor)
        if selected.strike <= floor:
            return selected
        candidates = sorted(
            [q for q in chain if q.strike <= floor],
            key=lambda q: q.strike,
            reverse=True,
        )
    return candidates[0] if candidates else None


def _sell_leg_dict(quote, expiry, qty, is_call, spot):
    # type: (Any, str, float, bool, float) -> Tuple[Dict[str, Any], float]
    """Build a short-leg dict and its open fee (USD per contract)."""
    fee_usd = deribit_fee_per_leg(spot, quote.bid_usd)
    leg = {
        "strike": quote.strike,
        "is_call": is_call,
        "expiry": expiry,
        "side": "sell",
        "qty": qty,
        "price_btc": quote.bid,
        "entry_price": quote.bid,
        "entry_price_usd": quote.bid_usd,
        "entry_spot": spot,
        "entry_bid": quote.bid,
        "entry_ask": quote.ask,
        "entry_mark": quote.mark,
        "entry_iv": quote.mark_iv,
        "entry_delta": quote.delta,
        "fee_usd_open": fee_usd,
    }
    return leg, fee_usd


class TuDySho:
    """Short N-DTE OTM options, turbulence-gated, NAV target / BTC-equity cap."""

    name = "tudysho"
    DATE_RANGE = ("2026-01-01", "2026-05-29")
    DESCRIPTION = (
        "Sells a delta-selected strangle, naked call, or naked put on a Deribit "
        "expiry N calendar days ahead. Entry requires turbulence composite below "
        "turbulence_threshold after entry_time (NYC, DST-aware). Entry weekdays "
        "are toggled via trade_monday … trade_sunday. Size targets "
        "nav_premium_pct of USD NAV, capped by max_qty_per_1btc_equity (contracts per "
        "1 BTC realized equity). Exits: expiry settlement or independent proximity / "
        "expiry settlement or independent proximity / equity / premium stops."
    )

    indicator_deps = [
        IndicatorDep(name="turbulence", symbol="BTCUSDT", interval="15m"),
    ]

    PARAM_GRID = {
        "leg_type": ["strangle"],
        "dte": [1],
        "delta": [0.10],
        "entry_time": ["15:00"],
        "stop_loss_pct": [7.5],
        "trade_monday": [1],
        "trade_tuesday": [1],
        "trade_wednesday": [1],
        "trade_thursday": [1],
        "trade_friday": [1],
        "trade_saturday": [0],
        "trade_sunday": [0],
        "min_otm_pct": [2.8],
        "turbulence_threshold": [60],
        "nav_premium_pct": [0.8],
        "max_qty_per_1btc_equity": [12],
        "leg_min_price": [0],
        "proximity_stop_hours": [4],
        "proximity_buffer_usd": [0],
        "premium_sl_except_final_hours": [4],
        "equity_drawdown_stop_pct": [0],
        "equity_sl_only_final_hours": [0],
        "equity_sl_except_final_hours": [0],
    }

    def __init__(self):
        self._positions = []          # type: List[OpenPosition]
        self._leg_type = "strangle"
        self._dte = 1
        self._max_concurrent = 1
        self._delta = 0.05
        self._sl_pct = 4.5
        self._entry_hour = 14
        self._entry_minute = 0
        self._entry_days = _entry_days_from_params({})
        self._turbulence_threshold = 60
        self._min_otm_pct = 0.0
        self._nav_premium_pct = 0.8
        self._max_qty_per_1btc_equity = 20.0
        self._leg_min_price = 0.0
        self._proximity_stop_hours = 4.0
        self._proximity_buffer_usd = 0.0
        self._premium_sl_except_final_hours = 4.0
        self._equity_drawdown_stop_pct = 0.0
        self._equity_sl_only_final_hours = 0.0
        self._equity_sl_except_final_hours = 0.0
        self._last_trade_date = None  # type: Optional[Any]
        self._exit_conds = []         # type: List[Any]
        self._pos_counter = 0
        self._turbulence = None       # type: Optional[Any]

    def set_indicators(self, ind):
        # type: (Dict[str, Any]) -> None
        self._turbulence = ind.get("turbulence")

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        """Load one grid combo and reset per-run state."""
        self._leg_type = params.get("leg_type", "strangle")
        self._dte = int(params.get("dte", 1))
        self._delta = float(params["delta"])
        self._sl_pct = float(params["stop_loss_pct"])
        raw_time = params.get("entry_time", "15:00")
        h, m = (int(x) for x in raw_time.split(":"))
        self._entry_hour = h
        self._entry_minute = m
        self._entry_days = _entry_days_from_params(params)
        self._turbulence_threshold = float(params.get("turbulence_threshold", 50))
        self._min_otm_pct = float(params.get("min_otm_pct", 0))
        self._nav_premium_pct = float(params.get("nav_premium_pct", 0.8))
        self._max_qty_per_1btc_equity = float(params.get("max_qty_per_1btc_equity", 20))
        self._leg_min_price = float(params.get("leg_min_price", 0))
        self._proximity_stop_hours = float(params.get("proximity_stop_hours", 4))
        self._proximity_buffer_usd = float(params.get("proximity_buffer_usd", 0))
        self._premium_sl_except_final_hours = float(
            params.get("premium_sl_except_final_hours", 4)
        )
        self._equity_drawdown_stop_pct = float(params.get("equity_drawdown_stop_pct", 0))
        self._equity_sl_only_final_hours = float(params.get("equity_sl_only_final_hours", 0))
        self._equity_sl_except_final_hours = float(
            params.get("equity_sl_except_final_hours", 0)
        )
        self._max_concurrent = self._dte + 1
        self._positions = []
        self._last_trade_date = None
        self._pos_counter = 0
        self._build_exit_conds()

    def reset(self):
        # type: () -> None
        self._positions = []
        self._last_trade_date = None
        self._pos_counter = 0
        self._build_exit_conds()

    def describe_params(self):
        # type: () -> Dict[str, Any]
        return {
            "leg_type": self._leg_type,
            "dte": self._dte,
            "delta": self._delta,
            "stop_loss_pct": self._sl_pct,
            "entry_time": f"{self._entry_hour:02d}:{self._entry_minute:02d}",
            **{
                key: int(wd in self._entry_days)
                for wd, (key, _) in enumerate(_TRADE_WEEKDAY_DEFAULTS)
            },
            "min_otm_pct": self._min_otm_pct,
            "turbulence_threshold": self._turbulence_threshold,
            "nav_premium_pct": self._nav_premium_pct,
            "max_qty_per_1btc_equity": self._max_qty_per_1btc_equity,
            "leg_min_price": self._leg_min_price,
            "proximity_stop_hours": self._proximity_stop_hours,
            "proximity_buffer_usd": self._proximity_buffer_usd,
            "premium_sl_except_final_hours": self._premium_sl_except_final_hours,
            "equity_drawdown_stop_pct": self._equity_drawdown_stop_pct,
            "equity_sl_only_final_hours": self._equity_sl_only_final_hours,
            "equity_sl_except_final_hours": self._equity_sl_except_final_hours,
        }

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        to_close = []
        for pos in list(self._positions):
            reason = self._check_exit(state, pos)
            if reason:
                trades.append(self._close(state, pos, reason))
                to_close.append(pos)
        for pos in to_close:
            self._positions.remove(pos)

        if len(self._positions) < self._max_concurrent:
            if self._last_trade_date != state.dt.date():
                if self._entry_day_allowed(state.dt.weekday()):
                    open_trade = self._maybe_open(state)
                    if open_trade is not None:
                        trades.append(open_trade)

        return trades

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        trades = []
        for pos in list(self._positions):
            trades.append(self._close(state, pos, "end_of_data"))
        self._positions.clear()
        return trades

    def _build_exit_conds(self):
        # type: () -> None
        """Wire composable SL factories into ``_exit_conds`` (proximity → equity → premium)."""
        self._exit_conds = []

        if self._proximity_stop_hours > 0:
            self._exit_conds.append(
                strike_proximity_stop(
                    self._proximity_stop_hours,
                    self._proximity_buffer_usd,
                )
            )

        if self._equity_drawdown_stop_pct > 0:
            equity_cond = equity_drawdown_stop(
                self._equity_drawdown_stop_pct,
                price_mode="mark",
            )
            if (
                self._equity_sl_only_final_hours > 0
                or self._equity_sl_except_final_hours > 0
            ):
                equity_cond = exit_expiry_window(
                    equity_cond,
                    only_final_hours=self._equity_sl_only_final_hours,
                    except_final_hours=self._equity_sl_except_final_hours,
                )
            self._exit_conds.append(equity_cond)

        if self._sl_pct > 0:
            premium_cond = stop_loss_pct(self._sl_pct, price_mode="mark")
            if self._premium_sl_except_final_hours > 0:
                premium_cond = exit_expiry_window(
                    premium_cond,
                    except_final_hours=self._premium_sl_except_final_hours,
                )
            self._exit_conds.append(premium_cond)

    def _check_exit(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[str]
        """Expiry first, then SL layer. Suppress quote-based exits on data gaps."""
        reason = check_expiry(state, pos)
        if reason is None:
            for cond in self._exit_conds:
                reason = cond(state, pos)
                if reason is not None:
                    break

        if reason is not None and reason not in _QUOTE_FREE_EXIT_REASONS:
            if not position_quotes_available(state, pos):
                logger.debug(
                    "[%s] Exit '%s' suppressed — option quote missing",
                    state.dt, reason,
                )
                return None

        return reason

    def _entry_day_allowed(self, weekday):
        # type: (int) -> bool
        """Return True if new entries are allowed on this weekday (Mon=0 … Sun=6)."""
        return weekday in self._entry_days

    def _account_usd(self, state, attr):
        # type: (Any, str) -> float
        """Read engine-injected USD field from state, else simulation account size."""
        value = getattr(state, attr, None)
        if value is not None:
            return float(value)
        fallback = float(_cfg.simulation.account_size_usd)
        logger.debug(
            "[%s] state.%s missing; using account_size_usd=%.2f",
            getattr(state, "dt", None), attr, fallback,
        )
        return fallback

    def _nav_usd_at_entry(self, state):
        # type: (Any) -> float
        return self._account_usd(state, "nav_usd")

    def _equity_at_entry_usd(self, state):
        # type: (Any) -> float
        return self._account_usd(state, "equity_usd")

    def _bid_acceptable(self, bid_btc):
        # type: (float) -> bool
        if self._leg_min_price > 0:
            return bid_btc >= self._leg_min_price
        return bid_btc > 0

    def _compute_quantity(self, state, premium_usd_per_contract):
        # type: (Any, float) -> Optional[Tuple[float, Dict[str, Any]]]
        """Return (qty, sizing_metadata) or None when entry should be skipped.

        Target: ``nav_premium_pct`` % of USD NAV divided by per-contract premium.
        Cap (when ``max_qty_per_1btc_equity`` > 0):
            max_contracts = (equity_usd / spot) × max_qty_per_1btc_equity
        where equity_usd is realized equity (``state.equity_usd``), not NAV.
        """
        spot = float(getattr(state, "spot", 0.0) or 0.0)
        equity_usd = self._equity_at_entry_usd(state)

        if self._nav_premium_pct <= 0:
            equity_btc = (equity_usd / spot) if spot > 0 else 0.0
            return (1.0, {
                "nav_usd_at_entry": self._nav_usd_at_entry(state),
                "equity_usd_at_entry": equity_usd,
                "equity_btc_at_entry": equity_btc,
                "spot_at_entry": spot,
                "nav_premium_pct": 0.0,
                "target_premium_usd": 0.0,
                "premium_usd_per_contract": premium_usd_per_contract,
                "qty_from_premium": 1.0,
                "max_qty_per_1btc_equity": self._max_qty_per_1btc_equity,
                "max_contracts_applied": None,
                "premium_capped": False,
            })

        nav_usd = self._nav_usd_at_entry(state)
        if nav_usd <= 0 or premium_usd_per_contract <= 0 or spot <= 0:
            return None

        target_premium_usd = nav_usd * (self._nav_premium_pct / 100.0)
        qty_from_premium = target_premium_usd / premium_usd_per_contract
        equity_btc = equity_usd / spot

        if self._max_qty_per_1btc_equity > 0:
            max_contracts = equity_btc * self._max_qty_per_1btc_equity
            if max_contracts < 0.1:
                return None
            premium_capped = qty_from_premium > max_contracts
            quantity = max(round(min(qty_from_premium, max_contracts), 1), 0.1)
        else:
            max_contracts = None
            premium_capped = False
            quantity = max(round(qty_from_premium, 1), 0.1)

        sizing = {
            "nav_usd_at_entry": nav_usd,
            "equity_usd_at_entry": equity_usd,
            "equity_btc_at_entry": equity_btc,
            "spot_at_entry": spot,
            "nav_premium_pct": self._nav_premium_pct,
            "target_premium_usd": target_premium_usd,
            "premium_usd_per_contract": premium_usd_per_contract,
            "qty_from_premium": qty_from_premium,
            "max_qty_per_1btc_equity": self._max_qty_per_1btc_equity,
            "max_contracts_applied": max_contracts,
            "premium_capped": premium_capped,
        }
        return quantity, sizing

    def _maybe_open(self, state):
        # type: (Any) -> Optional[Trade]
        dt = state.dt
        entry_utc = to_utc(
            to_nyc(dt).replace(
                hour=self._entry_hour,
                minute=self._entry_minute,
                second=0,
                microsecond=0,
            )
        )
        if dt.hour * 60 + dt.minute < entry_utc.hour * 60 + entry_utc.minute:
            return None
        if not self._turbulence_ok(dt):
            return None
        return self._try_open(state)

    def _turbulence_ok(self, dt):
        # type: (datetime) -> bool
        """Fail-open when indicator data is missing or NaN."""
        if self._turbulence is None:
            return True

        hour_ts = dt.replace(minute=0, second=0, microsecond=0)
        try:
            composite = self._turbulence.loc[hour_ts]["composite"]
        except KeyError:
            return True

        try:
            if math.isnan(composite):
                return True
        except TypeError:
            return True

        return float(composite) < self._turbulence_threshold

    def _try_open(self, state):
        # type: (Any) -> Optional[Trade]
        expiry = select_expiry(state, self._dte)
        if expiry is None:
            return None

        chain = state.get_chain(expiry)
        if not chain:
            return None

        calls = [q for q in chain if q.is_call]
        puts = [q for q in chain if not q.is_call]
        exp_dt = expiry_dt_utc(expiry, state.dt.tzinfo)

        if self._leg_type == "strangle":
            return self._open_strangle(state, expiry, exp_dt, calls, puts)
        if self._leg_type == "call":
            return self._open_single(state, expiry, exp_dt, calls, is_call=True)
        return self._open_single(state, expiry, exp_dt, puts, is_call=False)

    def _open_strangle(self, state, expiry, exp_dt, calls, puts):
        # type: (Any, str, Any, list, list) -> Optional[Trade]
        call = select_by_delta(calls, +self._delta)
        put = select_by_delta(puts, -self._delta)
        if call is None or put is None:
            return None

        if self._min_otm_pct > 0:
            call = _apply_min_otm(calls, call, state.spot, self._min_otm_pct, is_call=True)
            put = _apply_min_otm(puts, put, state.spot, self._min_otm_pct, is_call=False)
            if call is None or put is None:
                return None

        if not self._bid_acceptable(call.bid) or not self._bid_acceptable(put.bid):
            return None

        entry_usd = call.bid_usd + put.bid_usd
        if entry_usd <= 0:
            return None

        sized = self._compute_quantity(state, entry_usd)
        if sized is None:
            return None
        quantity, sizing_meta = sized

        call_leg, fee_call = _sell_leg_dict(call, expiry, quantity, True, state.spot)
        put_leg, fee_put = _sell_leg_dict(put, expiry, quantity, False, state.spot)
        legs = [call_leg, put_leg]

        metadata = {
            "leg_type": "strangle",
            "target_delta": self._delta,
            "expiry": expiry,
            "expiry_dt": exp_dt,
            "direction": "sell",
            "call_strike": call.strike,
            "put_strike": put.strike,
            "call_delta": call.delta,
            "put_delta": put.delta,
            "quantity": quantity,
        }
        return self._register_open(
            state,
            legs=legs,
            entry_usd=entry_usd * quantity,
            fees_open=(fee_call + fee_put) * quantity,
            metadata=metadata,
            sizing_meta=sizing_meta,
        )

    def _open_single(self, state, expiry, exp_dt, quotes, is_call):
        # type: (Any, str, Any, list, bool) -> Optional[Trade]
        target_delta = self._delta if is_call else -self._delta
        quote = select_by_delta(quotes, target_delta)
        if quote is None:
            return None

        if self._min_otm_pct > 0:
            quote = _apply_min_otm(quotes, quote, state.spot, self._min_otm_pct, is_call=is_call)
            if quote is None:
                return None

        if not self._bid_acceptable(quote.bid):
            return None

        entry_usd = quote.bid_usd
        if entry_usd <= 0:
            return None

        sized = self._compute_quantity(state, entry_usd)
        if sized is None:
            return None
        quantity, sizing_meta = sized

        leg_type = "call" if is_call else "put"
        strike_key = "call_strike" if is_call else "put_strike"
        delta_key = "call_delta" if is_call else "put_delta"
        legs = [_sell_leg_dict(quote, expiry, quantity, is_call, state.spot)[0]]
        fee_open = legs[0]["fee_usd_open"]

        metadata = {
            "leg_type": leg_type,
            "target_delta": self._delta,
            "expiry": expiry,
            "expiry_dt": exp_dt,
            "direction": "sell",
            strike_key: quote.strike,
            delta_key: quote.delta,
            "quantity": quantity,
        }
        return self._register_open(
            state,
            legs=legs,
            entry_usd=entry_usd * quantity,
            fees_open=fee_open * quantity,
            metadata=metadata,
            sizing_meta=sizing_meta,
        )

    def _register_open(self, state, legs, entry_usd, fees_open, metadata, sizing_meta):
        # type: (Any, List[Dict], float, float, Dict, Dict) -> Trade
        pos_id = self._next_pos_id()
        metadata = dict(metadata)
        metadata["pos_id"] = pos_id
        metadata["equity_at_entry_usd"] = self._equity_at_entry_usd(state)

        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=legs,
            entry_price_usd=entry_usd,
            fees_open=fees_open,
            metadata=metadata,
        )
        self._positions.append(pos)
        self._last_trade_date = state.dt.date()

        return Trade(
            entry_time=state.dt,
            exit_time=state.dt,
            entry_spot=state.spot,
            exit_spot=state.spot,
            entry_price_usd=entry_usd,
            exit_price_usd=0.0,
            fees=fees_open,
            pnl=0.0,
            triggered=False,
            exit_reason="",
            exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={
                "direction": "sell",
                "pos_id": pos_id,
                "legs": legs,
                **sizing_meta,
            },
        )

    def _next_pos_id(self):
        # type: () -> int
        self._pos_counter += 1
        return self._pos_counter

    def _close(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        leg_type = pos.metadata["leg_type"]
        quantity = float(pos.metadata.get("quantity", 1.0))

        if leg_type == "strangle":
            trade = self._close_strangle(state, pos, reason, quantity)
        else:
            trade = self._close_single_leg(state, pos, reason, quantity)

        self._stamp_close_metadata(trade, pos, quantity)
        return trade

    def _close_strangle(self, state, pos, reason, quantity):
        # type: (Any, OpenPosition, str, float) -> Trade
        expiry = pos.metadata["expiry"]
        call_strike = pos.metadata["call_strike"]
        put_strike = pos.metadata["put_strike"]

        if reason == "expiry":
            call_exit_usd = max(0.0, state.spot - call_strike)
            put_exit_usd = max(0.0, put_strike - state.spot)
            call_exit_btc = (call_exit_usd / state.spot) if state.spot else 0.0
            put_exit_btc = (put_exit_usd / state.spot) if state.spot else 0.0
            fee_call = fee_put = 0.0
        else:
            min_tick_usd = _MIN_TICK_BTC * state.spot
            call_q = state.get_option(expiry, call_strike, True)
            put_q = state.get_option(expiry, put_strike, False)
            call_exit_usd = call_q.ask_usd if call_q and call_q.ask > 0 else min_tick_usd
            put_exit_usd = put_q.ask_usd if put_q and put_q.ask > 0 else min_tick_usd
            call_exit_btc = call_q.ask if call_q and call_q.ask > 0 else _MIN_TICK_BTC
            put_exit_btc = put_q.ask if put_q and put_q.ask > 0 else _MIN_TICK_BTC
            fee_call = deribit_fee_per_leg(state.spot, call_exit_usd)
            fee_put = deribit_fee_per_leg(state.spot, put_exit_usd)

        for leg in pos.legs:
            if leg["is_call"]:
                leg["exit_price_btc"] = call_exit_btc
                leg["exit_price_usd"] = call_exit_usd
                leg["fee_btc_close"] = (fee_call / state.spot) if state.spot else 0.0
            else:
                leg["exit_price_btc"] = put_exit_btc
                leg["exit_price_usd"] = put_exit_usd
                leg["fee_btc_close"] = (fee_put / state.spot) if state.spot else 0.0

        return close_position(
            state,
            pos,
            reason,
            (call_exit_usd + put_exit_usd) * quantity,
            (fee_call + fee_put) * quantity,
        )

    def _close_single_leg(self, state, pos, reason, quantity):
        # type: (Any, OpenPosition, str, float) -> Trade
        leg_type = pos.metadata["leg_type"]
        is_call = leg_type == "call"
        expiry = pos.metadata["expiry"]
        strike = pos.metadata["call_strike"] if is_call else pos.metadata["put_strike"]
        leg = pos.legs[0]

        if reason == "expiry":
            exit_usd = max(0.0, state.spot - strike) if is_call else max(0.0, strike - state.spot)
            exit_btc = (exit_usd / state.spot) if state.spot else 0.0
            fee = 0.0
        else:
            min_tick_usd = _MIN_TICK_BTC * state.spot
            q = state.get_option(expiry, strike, is_call)
            exit_usd = q.ask_usd if q and q.ask > 0 else min_tick_usd
            exit_btc = q.ask if q and q.ask > 0 else _MIN_TICK_BTC
            fee = deribit_fee_per_leg(state.spot, exit_usd)

        leg["exit_price_btc"] = exit_btc
        leg["exit_price_usd"] = exit_usd
        leg["fee_btc_close"] = (fee / state.spot) if state.spot else 0.0

        return close_position(state, pos, reason, exit_usd * quantity, fee * quantity)

    def _stamp_close_metadata(self, trade, pos, quantity):
        # type: (Trade, OpenPosition, float) -> None
        trade.metadata.update({
            "leg_type": pos.metadata["leg_type"],
            "dte": self._dte,
            "stop_loss_pct": self._sl_pct,
            "turbulence_threshold": self._turbulence_threshold,
            "quantity": quantity,
            "nav_premium_pct": self._nav_premium_pct,
            "max_qty_per_1btc_equity": self._max_qty_per_1btc_equity,
            "proximity_stop_hours": self._proximity_stop_hours,
            "proximity_buffer_usd": self._proximity_buffer_usd,
            "premium_sl_except_final_hours": self._premium_sl_except_final_hours,
            "equity_drawdown_stop_pct": self._equity_drawdown_stop_pct,
            "equity_sl_only_final_hours": self._equity_sl_only_final_hours,
            "equity_sl_except_final_hours": self._equity_sl_except_final_hours,
            "equity_at_entry_usd": pos.metadata.get("equity_at_entry_usd"),
        })

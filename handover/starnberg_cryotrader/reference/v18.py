#!/usr/bin/env python3
"""
theta_engine/v18.py — RichForce2 16 front + stop exits + optional 1:1 wing.

Fork of ``theta_engine_v17`` (v14 short-DTE + investor D/G sidecar). Book
locked to run-727 **RichForce2 16 front** (``c8573c839903``).

Stop baselines from **run 742** (packed ``stop_book``):

| Display | stop_book | Run 742 hash | prox | equity% |
|---------|-----------|--------------|------|---------|
| **Late1Eq5** | ``late1_eq5`` | ``d550e3296f17`` | 1@4 | 5 |
| **Full2Eq8** | ``full2_eq8`` | ``731b1d03b15d`` | 2@16 | 8 |

Optional long wing (theta_spreads-style): ``wing_pct`` is the target width
as a fraction of the short strike (further OTM, same expiry, **equal qty**).
``wing_pct=0`` = naked (no long). ``min_width_usd`` floors listed outer
width (0 = no floor); there is no ``max_width_usd``. If ``wing_pct>0`` and
no outer is available, the short is rolled back (no naked fill).

Design-A 252 stop grid kept as ``V18_STOP_DISCOVERY_GRID`` (artefact only).
Rich qty sizing stays closed (``rich_mode=none``).

Catalog ID ``theta_engine_v18``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backtester.core.pricing import deribit_fee_per_leg
from backtester.core.strategy_base import (
    OpenPosition,
    Trade,
    position_unrealized_pnl,
    strike_proximity_stop_pct,
)
from workspace.strategies.theta_engine._common import (
    hold_until_planned_dt,
    net_credit_profit_target_pct,
    net_credit_stop_loss_pct,
)
from workspace.strategies.theta_engine.v17 import (
    V17_BASE_BOOK,
    V17_BASE_SKEW,
    V17_BASE_V14_HASH,
    V17_RICH_CLOSED_DEFAULTS,
    ThetaEngineV17,
)

logger = logging.getLogger(__name__)

V18_BASE_V14_HASH = V17_BASE_V14_HASH
V18_BASE_BOOK = V17_BASE_BOOK
V18_BASE_SKEW = V17_BASE_SKEW

# Run 742 favourites — working stop baselines.
V18_BASELINE_LATE1_EQ5 = "late1_eq5"
V18_BASELINE_FULL2_EQ8 = "full2_eq8"
V18_BASELINE_LATE1_EQ5_HASH = "d550e3296f17"
V18_BASELINE_FULL2_EQ8_HASH = "731b1d03b15d"

V18_STOP_BOOKS: Dict[str, Dict[str, Any]] = {
    V18_BASELINE_LATE1_EQ5: {
        "display": "Late1Eq5",
        "hash": V18_BASELINE_LATE1_EQ5_HASH,
        "prox_stop": "1@4",
        "open_pnl_equity_stop_pct": 5.0,
        "stop_loss_pct": 0.0,
    },
    V18_BASELINE_FULL2_EQ8: {
        "display": "Full2Eq8",
        "hash": V18_BASELINE_FULL2_EQ8_HASH,
        "prox_stop": "2@16",
        "open_pnl_equity_stop_pct": 8.0,
        "stop_loss_pct": 0.0,
    },
}
V18_BASELINE_DISPLAY: Dict[str, str] = {
    k: str(v["display"]) for k, v in V18_STOP_BOOKS.items()
}

# Full overnight window ≈ 16:00 → 08:00 expiry (16h); 4h = run-729 late window.
V18_PROX_FULL_HOURS = 16.0

# CLOSED (run 742): design-A discovery artefact — do not put back in PARAM_GRID
# as a performance search. Credit SL lost; tight prox/equity hurt.
V18_PROX_STOP_GRID: Tuple[str, ...] = (
    "none",
    "0.5@4",
    "0.5@16",
    "1.0@4",
    "1.0@16",
    "2.0@4",
    "2.0@16",
)
V18_STOP_LOSS_GRID: Tuple[float, ...] = (0.0, 0.5, 1.0, 1.5, 2.0, 3.0)
V18_EQUITY_STOP_GRID: Tuple[float, ...] = (0.0, 1.0, 2.0, 3.0, 5.0, 8.0)
V18_STOP_DISCOVERY_GRID = {
    "stop_loss_pct": list(V18_STOP_LOSS_GRID),
    "prox_stop": list(V18_PROX_STOP_GRID),
    "open_pnl_equity_stop_pct": list(V18_EQUITY_STOP_GRID),
}

# 0 = naked; else target outer width = wing_pct × short strike.
V18_WING_PCT_GRID: Tuple[float, ...] = (0.0, 0.03, 0.05, 0.07, 0.10, 0.15)
_WING_PCT_MAX = 0.50
# Floor on listed outer width (USD). 0 = no floor. No max_width_usd.
# ~$1k matches theta_spreads default; 2.5k / 5k probe wider defined-risk.
V18_MIN_WIDTH_USD_GRID: Tuple[float, ...] = (0.0, 1000.0, 2500.0, 5000.0)


def parse_prox_stop(spec: Any) -> Tuple[float, float]:
    """Return ``(buffer_pct, hours_before_expiry)``; ``(0, 0)`` means off."""
    raw = str(spec).strip().lower()
    if raw in ("", "none", "off", "0", "0.0"):
        return 0.0, 0.0
    if "@" not in raw:
        raise ValueError(
            "prox_stop must be 'none' or '<pct>@<hours>' (e.g. '1.0@16'), got %r"
            % spec
        )
    pct_s, hours_s = raw.split("@", 1)
    pct = float(pct_s)
    hours = float(hours_s)
    if pct <= 0 or hours <= 0:
        raise ValueError("prox_stop pct and hours must be > 0, got %r" % spec)
    return pct, hours


def parse_stop_book(name: Any) -> Dict[str, Any]:
    key = str(name).strip().lower()
    if key not in V18_STOP_BOOKS:
        raise ValueError(
            "unknown stop_book %r; choose from %s"
            % (name, sorted(V18_STOP_BOOKS))
        )
    return dict(V18_STOP_BOOKS[key])


def wing_target_width_usd(short_strike: float, wing_pct: float) -> float:
    """Target dollar width: ``wing_pct × short_strike`` (before min floor)."""
    return abs(float(wing_pct) * float(short_strike))


def pick_wing_quote_by_pct(
    quotes: Sequence[Any],
    is_call: bool,
    short_strike: float,
    wing_pct: float,
    min_width_usd: float = 0.0,
) -> Optional[Any]:
    """Nearest further-OTM ask>0 quote to target; optional ``min_width_usd`` floor."""
    inner = float(short_strike)
    width = wing_target_width_usd(inner, wing_pct)
    if width <= 0:
        return None
    target_k = inner + width if is_call else inner - width
    floor = max(0.0, float(min_width_usd))
    candidates: List[Any] = []
    for q in quotes:
        k = float(q.strike)
        if is_call:
            if k <= inner:
                continue
            listed_width = k - inner
        else:
            if k >= inner:
                continue
            listed_width = inner - k
        if listed_width < floor:
            continue
        ask = float(getattr(q, "ask", 0.0) or 0.0)
        if ask <= 0:
            continue
        candidates.append(q)
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda q: (abs(float(q.strike) - target_k), abs(float(q.strike) - inner)),
    )


class ThetaEngineV18(ThetaEngineV17):
    """v17 book + Late1Eq5/Full2Eq8 stops + optional 1:1 wing_pct hedge."""

    name = "theta_engine_v18"
    DATE_RANGE = ThetaEngineV17.DATE_RANGE
    DESCRIPTION = (
        "Short-DTE theta (v18): RichForce2 16 front with Late1Eq5 / Full2Eq8 "
        "stops (run 742) and optional 1:1 further-OTM wing (wing_pct; 0=naked; "
        "min_width_usd floor). Investor D/G sidecar retained."
    )

    PARAM_GRID = {
        "dte": [1],
        "delta": [0.25],
        "skew_mode": ["cheaper"],
        "skew_source": [V18_BASE_SKEW],
        "book": [V18_BASE_BOOK],
        "qty_per_1btc_equity": [2.0],
        "late_exit": ["none"],
        "take_profit_pct": [0],
        "max_concurrent": [1],
        "hold_days": [0],
        "front_vrp_min": [0.0],
        "weekend_cover": ["skip"],
        **V17_RICH_CLOSED_DEFAULTS,
        "stop_book": [V18_BASELINE_LATE1_EQ5, V18_BASELINE_FULL2_EQ8],
        "stop_loss_pct": [0],
        "wing_pct": list(V18_WING_PCT_GRID),
        "min_width_usd": list(V18_MIN_WIDTH_USD_GRID),
    }
    PARAM_HELP = {
        **ThetaEngineV17.PARAM_HELP,
        "stop_book": (
            "Working baselines from run 742: late1_eq5 (d550e3296f17 — "
            "prox 1@4 + equity 5%) and full2_eq8 (731b1d03b15d — "
            "prox 2@16 + equity 8%). Expands to prox_stop / equity% / SL=0."
        ),
        "stop_loss_pct": (
            "Full-session net-credit mark SL (fraction of credit). "
            "0 = off. CLOSED for performance on run 742 — locked to 0 on "
            "working baselines. See V18_STOP_DISCOVERY_GRID."
        ),
        "prox_stop": (
            "Strike proximity stop: 'none' or '<buffer_pct>@<hours>' "
            "(buffer is %% of spot; hours = window before expiry). "
            "Working books: 1@4 (late) / 2@16 (full overnight)."
        ),
        "open_pnl_equity_stop_pct": (
            "Close all when aggregate open mark loss ≥ this %% of realized "
            "equity (percent points; 5 = 5%). Working books: 5 / 8."
        ),
        "wing_pct": (
            "1:1 further-OTM long wing: target width = wing_pct × short "
            "strike, same expiry, equal qty. 0 = naked (no long). If >0 "
            "and no outer, skip the trade."
        ),
        "min_width_usd": (
            "Minimum listed outer width in USD (strike distance). "
            "0 = no floor. No max_width_usd. Ignored when wing_pct=0."
        ),
        "late_exit": (
            "Locked to none on v18 — use stop_book / prox_stop instead "
            "of the v14 late_exit enum."
        ),
    }

    def __init__(self):
        super().__init__()
        self._stop_book: Optional[str] = None
        self._prox_stop_pct = 0.0
        self._prox_hours = 0.0
        self._open_pnl_equity_stop_pct = 0.0
        self._wing_pct = 0.0
        self._min_width_usd = 0.0

    def configure(self, params: Dict[str, Any]) -> None:
        params = dict(params)
        stop_book = params.get("stop_book")
        if stop_book is not None and str(stop_book).strip() != "":
            spec = parse_stop_book(stop_book)
            self._stop_book = str(stop_book).strip().lower()
            params["prox_stop"] = spec["prox_stop"]
            params["open_pnl_equity_stop_pct"] = spec[
                "open_pnl_equity_stop_pct"
            ]
            params["stop_loss_pct"] = spec["stop_loss_pct"]
        else:
            self._stop_book = None

        # Parent builds exit conds via reset(); rebuild after our knobs are set.
        super().configure(params)
        self._prox_stop_pct, self._prox_hours = parse_prox_stop(
            params.get("prox_stop", "none")
        )
        self._open_pnl_equity_stop_pct = float(
            params.get("open_pnl_equity_stop_pct", 0.0)
        )
        if self._open_pnl_equity_stop_pct < 0:
            raise ValueError(
                "open_pnl_equity_stop_pct must be >= 0, got %r"
                % self._open_pnl_equity_stop_pct
            )
        wp = float(params.get("wing_pct", 0.0))
        if wp < 0 or wp > _WING_PCT_MAX:
            raise ValueError(
                "wing_pct must be in [0, %.2f], got %r" % (_WING_PCT_MAX, wp)
            )
        self._wing_pct = wp
        mw = float(params.get("min_width_usd", 0.0))
        if mw < 0:
            raise ValueError("min_width_usd must be >= 0, got %r" % mw)
        self._min_width_usd = mw
        # Force late_exit unused; stops come from the three axes.
        self._late_exit = "none"
        self._exit_conds = self._build_exit_conds()

    def describe_params(self) -> Dict[str, Any]:
        d = super().describe_params()
        d["stop_book"] = self._stop_book
        d["stop_display"] = (
            V18_BASELINE_DISPLAY.get(self._stop_book, self._stop_book)
            if self._stop_book
            else None
        )
        d["prox_stop"] = (
            "none"
            if self._prox_stop_pct <= 0
            else "%.4g@%.4g" % (self._prox_stop_pct, self._prox_hours)
        )
        d["prox_stop_pct"] = self._prox_stop_pct
        d["prox_hours"] = self._prox_hours
        d["open_pnl_equity_stop_pct"] = self._open_pnl_equity_stop_pct
        d["wing_pct"] = self._wing_pct
        d["min_width_usd"] = self._min_width_usd
        return d

    def _build_exit_conds(self) -> List[Any]:
        conds: List[Any] = []
        if self._sl_pct > 0:
            conds.append(
                net_credit_stop_loss_pct(self._sl_pct, price_mode="mark")
            )
        if self._prox_stop_pct > 0 and self._prox_hours > 0:
            conds.append(
                strike_proximity_stop_pct(
                    self._prox_hours, self._prox_stop_pct
                )
            )
        if self._tp_pct > 0:
            conds.append(
                net_credit_profit_target_pct(
                    self._tp_pct, price_mode="executable"
                )
            )
        if self._hold_days > 0:
            conds.append(hold_until_planned_dt())
        return conds

    def on_market_state(self, state: Any) -> List[Any]:
        trades: List[Any] = []

        if self._open_pnl_equity_stop_hit(state):
            if self._all_open_quotes_available(state):
                self._log_portfolio_stop(state)
                for pos in list(self._positions):
                    trades.append(
                        self._do_close(state, pos, "open_pnl_equity_stop")
                    )
                self._positions.clear()
            else:
                logger.debug(
                    "[%s] open_pnl_equity_stop suppressed — quote missing "
                    "on one or more legs",
                    state.dt,
                )
        else:
            to_close: List[OpenPosition] = []
            for pos in list(self._positions):
                reason = self._check_exit(state, pos)
                if reason is not None:
                    trades.append(self._do_close(state, pos, reason))
                    to_close.append(pos)
            for pos in to_close:
                self._positions.remove(pos)

        if self._should_enter(state):
            open_trade = self._do_open(state)
            if open_trade is not None:
                trades.append(open_trade)

        self._meter_investor_greeks(state)
        return trades

    def _quotes_available(self, state: Any, pos: OpenPosition) -> bool:
        """True when every open leg (short + wing) has a snapshot quote row."""
        if not pos.legs:
            return False
        for leg in pos.legs:
            expiry = leg.get("expiry") or pos.metadata.get("expiry")
            strike = leg.get("strike")
            if expiry is None or strike is None:
                return False
            is_call = bool(leg.get("is_call"))
            if state.get_option(expiry, float(strike), is_call) is None:
                return False
        return True

    def _all_open_quotes_available(self, state: Any) -> bool:
        if not self._positions:
            return False
        return all(self._quotes_available(state, pos) for pos in self._positions)

    def _open_single(
        self,
        state: Any,
        expiry: str,
        exp_dt: Any,
        chain: list,
        is_call: bool,
        quantity: float,
        call_bid: float = 0.0,
        put_bid: float = 0.0,
        meta_extra: Optional[Dict[str, Any]] = None,
    ) -> Optional[Trade]:
        prev_trade_date = self._last_trade_date
        prev_open_date = self._last_open_date
        prev_counter = self._pos_counter

        trade = super()._open_single(
            state,
            expiry,
            exp_dt,
            chain,
            is_call,
            quantity,
            call_bid=call_bid,
            put_bid=put_bid,
            meta_extra=meta_extra,
        )
        if trade is None:
            return None

        pos = self._positions[-1]
        pos.metadata["wing_pct"] = self._wing_pct
        pos.metadata.setdefault("wing_attached", False)

        if self._wing_pct <= 0:
            return trade

        if not self._attach_wing(state, pos):
            self._positions.pop()
            self._last_trade_date = prev_trade_date
            self._last_open_date = prev_open_date
            self._pos_counter = prev_counter
            logger.info(
                "[theta_engine_v18] WING_SKIP pos_id=%s reason=no_outer "
                "wing_pct=%.4g short_expiry=%s — trade rolled back",
                pos.metadata.get("pos_id"),
                self._wing_pct,
                pos.metadata.get("expiry"),
            )
            return None

        trade.metadata["legs"] = pos.legs
        trade.fees = pos.fees_open
        trade.entry_price_usd = pos.entry_price_usd
        return trade

    def _attach_wing(self, state: Any, pos: OpenPosition) -> bool:
        """Append equal-qty further-OTM long on same expiry. False = abort."""
        if not pos.legs:
            return False
        short = pos.legs[0]
        short_expiry = str(short.get("expiry") or pos.metadata.get("expiry") or "")
        short_strike = float(short["strike"])
        short_qty = float(short.get("qty") or pos.metadata.get("qty") or 0.0)
        is_call = bool(short["is_call"])
        short_credit = float(pos.metadata.get("net_credit_usd") or 0.0)
        if short_qty <= 0 or short_credit <= 0 or not short_expiry:
            return False

        quote = pick_wing_quote_by_pct(
            self._ask_quotes(state, short_expiry, is_call),
            is_call,
            short_strike,
            self._wing_pct,
            min_width_usd=self._min_width_usd,
        )
        if quote is None:
            return False

        ask_btc = float(quote.ask or 0.0)
        ask_usd_per = ask_btc * float(state.spot)
        if ask_usd_per <= 0:
            return False

        debit = ask_usd_per * short_qty
        net_credit = short_credit - debit
        if net_credit <= 0:
            logger.info(
                "[theta_engine_v18] WING_SKIP pos_id=%s reason=non_positive_credit "
                "short=%.2f debit=%.2f",
                pos.metadata.get("pos_id"),
                short_credit,
                debit,
            )
            return False

        fee_per = deribit_fee_per_leg(state.spot, ask_usd_per)
        fees = fee_per * short_qty
        wing_leg = self._leg_dict(
            quote,
            short_expiry,
            ask_btc,
            ask_usd_per,
            state,
            fee_per,
            short_qty,
            side="buy",
        )
        pos.legs.append(wing_leg)
        pos.fees_open = float(pos.fees_open) + fees
        pos.entry_price_usd = net_credit
        pos.metadata["short_credit_usd"] = short_credit
        pos.metadata["net_credit_usd"] = net_credit
        pos.metadata["sl_threshold_usd"] = self._sl_threshold_usd(net_credit)
        pos.metadata["tp_threshold_usd"] = self._tp_threshold_usd(net_credit)
        pos.metadata["wing_attached"] = True
        pos.metadata["wing_qty"] = short_qty
        pos.metadata["wing_expiry"] = short_expiry
        pos.metadata["wing_strike"] = float(quote.strike)
        pos.metadata["wing_target_width_usd"] = wing_target_width_usd(
            short_strike, self._wing_pct
        )
        pos.metadata["min_width_usd"] = self._min_width_usd
        pos.metadata["wing_delta"] = float(getattr(quote, "delta", 0.0) or 0.0)
        pos.metadata["wing_debit_usd"] = debit
        pos.metadata["wing_pct"] = self._wing_pct
        logger.info(
            "[theta_engine_v18] WING_OPEN pos_id=%s expiry=%s short_k=%.0f "
            "wing_k=%.0f qty=%.1f debit=%.2f net_credit=%.2f wing_pct=%.4g",
            pos.metadata.get("pos_id"),
            short_expiry,
            short_strike,
            float(quote.strike),
            short_qty,
            debit,
            net_credit,
            self._wing_pct,
        )
        return True

    def _ask_quotes(self, state: Any, expiry: str, is_call: bool) -> List[Any]:
        chain = state.get_chain(expiry)
        if not chain:
            return []
        return [
            q
            for q in chain
            if bool(q.is_call) == is_call and float(getattr(q, "ask", 0) or 0) > 0
        ]

    def _total_open_pnl_usd(self, state: Any) -> Optional[float]:
        total = 0.0
        for pos in self._positions:
            pnl = position_unrealized_pnl(state, pos, price_mode="mark")
            if pnl is None:
                return None
            total += pnl
        return total

    def _open_pnl_equity_stop_hit(self, state: Any) -> bool:
        """True when open mark loss ≥ open_pnl_equity_stop_pct of equity."""
        if self._open_pnl_equity_stop_pct <= 0:
            return False
        if not self._positions:
            return False
        equity = self._equity_usd(state)
        if equity <= 0:
            return False
        total_pnl = self._total_open_pnl_usd(state)
        if total_pnl is None or total_pnl >= 0:
            return False
        return (-total_pnl) / equity >= (self._open_pnl_equity_stop_pct / 100.0)

    def _log_portfolio_stop(self, state: Any) -> None:
        equity = self._equity_usd(state)
        open_pnl = self._total_open_pnl_usd(state)
        n_pos = len(self._positions)
        loss = (-open_pnl) if open_pnl is not None else float("nan")
        loss_pct = (
            100.0 * loss / equity
            if equity > 0 and open_pnl is not None
            else float("nan")
        )
        logger.info(
            "[theta_engine_v18] PORTFOLIO_STOP dt=%s n_pos=%s equity_usd=%.2f "
            "open_pnl_usd=%.2f loss_pct=%.2f threshold_pct=%.1f",
            state.dt.isoformat(),
            n_pos,
            equity,
            open_pnl if open_pnl is not None else float("nan"),
            loss_pct,
            self._open_pnl_equity_stop_pct,
        )

    def _do_close(self, state: Any, pos: OpenPosition, reason: str) -> Trade:
        trade = super()._do_close(state, pos, reason)
        trade.metadata["stop_book"] = self._stop_book
        trade.metadata["prox_stop_pct"] = self._prox_stop_pct
        trade.metadata["prox_hours"] = self._prox_hours
        trade.metadata["open_pnl_equity_stop_pct"] = self._open_pnl_equity_stop_pct
        trade.metadata["wing_pct"] = self._wing_pct
        trade.metadata["min_width_usd"] = self._min_width_usd
        trade.metadata["wing_attached"] = bool(pos.metadata.get("wing_attached"))
        return trade

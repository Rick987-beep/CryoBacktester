#!/usr/bin/env python3
"""
theta_engine/v17.py — v14 short-DTE shorts + investor D/G sidecar.

Fork of ``theta_engine_v14`` (not v9 / v12 / v13). Default book is run-727
**RichForce2 16 front** (``c8573c839903``): VRP≥4 else force after 2d @ 16:00
UTC, front 25Δ RR, qty 2, hold, no SL/TP.

Investor D/G sidecar: ``investor_greeks.parquet`` when tracking is on.

Rich qty sizing (``rich_mode=on``, front_vrp × iv_rank_60 → multiplicative
factor on base qty) was discovery-tested on **run 741**
(``theta_engine_v17_20260822_090110``, 216 combos) and **closed**: every
``on`` cell lost PnL / Sharpe / Calmar to naked ``none``; cuts hit winners
harder than losers. Logic stays in code; default grid is ``rich_mode=none``.
See ``V17_RICH_DISCOVERY_GRID`` (artefact only — not the working PARAM_GRID).

Catalog ID ``theta_engine_v17``.
"""

from __future__ import annotations

import logging
import math
from array import array
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

from backtester.core.portfolio_risk import dg_limits_ok, portfolio_cash_greeks
from backtester.core.strategy_base import Trade
from backtester.indicators import IndicatorDep
from backtester.indicators.front_25d_iv_rank import lookup_front_25d_iv_rank
from workspace.strategies.theta_engine._common import MIN_QTY
from workspace.strategies.theta_engine.v14 import (
    ENTRY_RICH_FORCE2,
    ThetaEngineV14,
)

logger = logging.getLogger(__name__)

# Run 727 fan-out cell this version is locked to.
V17_BASE_V14_HASH = "c8573c839903"
V17_BASE_BOOK = "rf2_1600"
V17_BASE_SKEW = "front"
V17_BASE_POLICY = ENTRY_RICH_FORCE2
V17_BASE_HOUR = 16

# CLOSED (run 741): rich qty discovery artefact — do not put back in PARAM_GRID
# as a performance search. Kept for reproduction / A-B only.
# 216 = 2 × 3 × 3 × 2 × 3 × 2 on rf2_1600 × front; all ``on`` cells lost to naked.
V17_RICH_DISCOVERY_GRID = {
    "rich_mode": ["none", "on"],
    "vrp_lo": [0.0],
    "vrp_hi": [5.0, 7.0, 9.0],
    "alpha_vrp": [0.4, 0.6, 0.8],
    "rank_lo": [0.5],
    "rank_hi": [0.85, 1.0],
    "alpha_rank": [0.4, 0.6, 0.8],
    "f_min": [0.2, 0.35],
}

# Working defaults after run 741 close-out (naked base qty).
V17_RICH_CLOSED_DEFAULTS = {
    "rich_mode": ["none"],
    "vrp_lo": [0.0],
    "vrp_hi": [8.0],
    "alpha_vrp": [0.5],
    "rank_lo": [0.5],
    "rank_hi": [1.0],
    "alpha_rank": [0.5],
    "f_min": [0.2],
}


def _rich_clip(x: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0 if x <= lo else 1.0
    return max(0.0, min(1.0, (x - lo) / (hi - lo)))


def _is_finite(x: Any) -> bool:
    return isinstance(x, (int, float)) and not math.isnan(float(x))


def compute_rich_qty_factor(
    *,
    front_vrp: float,
    iv_rank_60: float,
    vrp_lo: float,
    vrp_hi: float,
    alpha_vrp: float,
    rank_lo: float,
    rank_hi: float,
    alpha_rank: float,
    f_min: float,
) -> Tuple[float, Dict[str, float]]:
    """Multiplicative rich factor and diagnostic fields for trade metadata."""
    r_vrp = _rich_clip(float(front_vrp), vrp_lo, vrp_hi) if _is_finite(front_vrp) else 0.0
    r_rank = _rich_clip(float(iv_rank_60), rank_lo, rank_hi) if _is_finite(iv_rank_60) else 0.0
    f_vrp = max(f_min, min(1.0, 1.0 - alpha_vrp * r_vrp))
    f_rank = max(f_min, min(1.0, 1.0 - alpha_rank * r_rank))
    factor = max(f_min, min(1.0, f_vrp * f_rank))
    return factor, {
        "rich_R_vrp": r_vrp,
        "rich_R_rank": r_rank,
        "rich_f_vrp": f_vrp,
        "rich_f_rank": f_rank,
        "rich_factor": factor,
    }


class ThetaEngineV17(ThetaEngineV14):
    """v14 short-DTE book plus investor D/G metering (rich qty closed run 741)."""

    name = "theta_engine_v17"
    DATE_RANGE = ThetaEngineV14.DATE_RANGE
    DESCRIPTION = (
        "Short-DTE theta (v17): v14 naked 1 DTE 25Δ plus investor D/G "
        "sidecar. Default book is RichForce2 16 front (run 727 "
        "c8573c839903). Rich qty sizing closed on run 741 (did not beat naked)."
    )

    indicator_deps = [
        IndicatorDep(
            name="vol_context",
            symbol="BTCUSDT",
            interval="1d",
            warmup_days=60,
        ),
        IndicatorDep(
            name="front_25d_iv_rank",
            symbol="BTCUSDT",
            interval="1d",
            warmup_days=60,
            params={
                "entry_hour": V17_BASE_HOUR,
                "entry_minute": 0,
                "dte": 1,
                "delta": 0.25,
                "rank_lookback": 60,
            },
        ),
    ]

    PARAM_GRID = {
        "dte": [1],
        "delta": [0.25],
        "skew_mode": ["cheaper"],
        "skew_source": [V17_BASE_SKEW],
        "book": [V17_BASE_BOOK],
        "qty_per_1btc_equity": [2.0],
        "late_exit": ["none"],
        "take_profit_pct": [0],
        "stop_loss_pct": [0],
        "max_concurrent": [1],
        "hold_days": [0],
        "front_vrp_min": [0.0],
        "weekend_cover": ["skip"],
        **V17_RICH_CLOSED_DEFAULTS,
    }
    PARAM_HELP = {
        **ThetaEngineV14.PARAM_HELP,
        "book": (
            "Locked to rf2_1600 (RichForce2 @ 16:00 UTC). Run 727 hash "
            "c8573c839903. Overlays fork this cell, not the v14 #1 book."
        ),
        "skew_source": (
            "Locked to front (1 DTE 25Δ RR) — the run-727 favourite's "
            "side pick, not skew6."
        ),
        "track_investor_greeks": (
            "1 = meter shorts-only vs full-book D/G each live bar "
            "(sidecar parquet; vega is a meter, not a gate). 0 = off."
        ),
        "rich_mode": (
            "CLOSED for performance (run 741): none = naked base qty "
            "(default). on = front_vrp × iv_rank_60 size-down — kept in "
            "code, not a working search. See V17_RICH_DISCOVERY_GRID."
        ),
        "vrp_lo": "front_vrp (sold IV − DVOL) at/below → R_vrp = 0.",
        "vrp_hi": "front_vrp at/above → R_vrp = 1.",
        "alpha_vrp": "Max fractional cut from leg-vs-DVOL input at R_vrp = 1.",
        "rank_lo": "iv_rank_60 at/below → R_rank = 0.",
        "rank_hi": "iv_rank_60 at/above → R_rank = 1.",
        "alpha_rank": "Max fractional cut from IV-rank input at R_rank = 1.",
        "f_min": "Floor on per-input and combined rich_factor.",
    }

    def __init__(self):
        super().__init__()
        self._track_investor_greeks = True
        self._ig_short_view = SimpleNamespace(legs=[])
        self._rich_mode = "none"
        self._vrp_lo = 0.0
        self._vrp_hi = 8.0
        self._alpha_vrp = 0.5
        self._rank_lo = 0.5
        self._rank_hi = 1.0
        self._alpha_rank = 0.5
        self._f_min = 0.2
        self._front_25d_iv_rank = None
        self._pending_rich_meta: Dict[str, float] = {}
        self._ig_reset()

    def set_indicators(self, ind: Dict[str, Any]) -> None:
        super().set_indicators(ind)
        self._front_25d_iv_rank = ind.get("front_25d_iv_rank")

    def configure(self, params: Dict[str, Any]) -> None:
        super().configure(params)
        self._track_investor_greeks = int(params.get("track_investor_greeks", 1)) != 0
        self._rich_mode = str(params.get("rich_mode", "none")).strip().lower()
        if self._rich_mode not in ("none", "on"):
            raise ValueError("rich_mode must be 'none' or 'on', got %r" % self._rich_mode)
        self._vrp_lo = float(params.get("vrp_lo", 0.0))
        self._vrp_hi = float(params.get("vrp_hi", 8.0))
        self._alpha_vrp = float(params.get("alpha_vrp", 0.5))
        self._rank_lo = float(params.get("rank_lo", 0.5))
        self._rank_hi = float(params.get("rank_hi", 1.0))
        self._alpha_rank = float(params.get("alpha_rank", 0.5))
        self._f_min = float(params.get("f_min", 0.2))
        if self._vrp_hi <= self._vrp_lo:
            raise ValueError("vrp_hi must be > vrp_lo")
        if self._rank_hi <= self._rank_lo:
            raise ValueError("rank_hi must be > rank_lo")
        if not (0.0 < self._f_min <= 1.0):
            raise ValueError("f_min must be in (0, 1]")
        for name, val in (
            ("alpha_vrp", self._alpha_vrp),
            ("alpha_rank", self._alpha_rank),
        ):
            if not (0.0 <= val <= 1.0):
                raise ValueError("%s must be in [0, 1]" % name)

    def describe_params(self) -> Dict[str, Any]:
        d = super().describe_params()
        d["track_investor_greeks"] = int(self._track_investor_greeks)
        d["rich_mode"] = self._rich_mode
        d["vrp_lo"] = self._vrp_lo
        d["vrp_hi"] = self._vrp_hi
        d["alpha_vrp"] = self._alpha_vrp
        d["rank_lo"] = self._rank_lo
        d["rank_hi"] = self._rank_hi
        d["alpha_rank"] = self._alpha_rank
        d["f_min"] = self._f_min
        return d

    def reset(self) -> None:
        super().reset()
        self._pending_rich_meta = {}
        self._ig_reset()

    def on_market_state(self, state: Any) -> List[Any]:
        trades = super().on_market_state(state)
        self._meter_investor_greeks(state)
        return trades

    def _base_qty_raw(self, state: Any) -> Optional[float]:
        if state.spot <= 0:
            return None
        return self._equity_usd(state) / state.spot * self._qty_per_1btc_equity

    def _scaled_quantity(
        self,
        state: Any,
        is_call: bool,
        meta_extra: Optional[Dict[str, Any]],
    ) -> Optional[float]:
        q0 = self._base_qty_raw(state)
        if q0 is None or q0 < MIN_QTY:
            return None
        if self._rich_mode != "on":
            self._pending_rich_meta = {}
            return max(round(q0, 1), MIN_QTY)

        ctx = meta_extra or {}
        vol_ctx = getattr(self, "_pending_vol_ctx", None) or self._vol_ctx_at(state)
        front_vrp = ctx.get("front_vrp")
        if not _is_finite(front_vrp):
            dvol = vol_ctx.get("dvol", float("nan"))
            front_iv = ctx.get("front_iv", float("nan"))
            if _is_finite(front_iv) and _is_finite(dvol):
                front_vrp = float(front_iv) - float(dvol)
            else:
                front_vrp = float("nan")

        iv_ctx = lookup_front_25d_iv_rank(
            self._front_25d_iv_rank, state.dt, is_call,
        )
        iv_rank_60 = iv_ctx.get("iv_rank_60", float("nan"))

        factor, rich_meta = compute_rich_qty_factor(
            front_vrp=float(front_vrp) if _is_finite(front_vrp) else float("nan"),
            iv_rank_60=float(iv_rank_60) if _is_finite(iv_rank_60) else float("nan"),
            vrp_lo=self._vrp_lo,
            vrp_hi=self._vrp_hi,
            alpha_vrp=self._alpha_vrp,
            rank_lo=self._rank_lo,
            rank_hi=self._rank_hi,
            alpha_rank=self._alpha_rank,
            f_min=self._f_min,
        )
        scaled = q0 * factor
        rich_meta = dict(rich_meta)
        rich_meta["qty_base"] = q0
        rich_meta["iv_rank_60"] = iv_rank_60
        if _is_finite(front_vrp):
            rich_meta["front_vrp_used"] = float(front_vrp)
        self._pending_rich_meta = rich_meta

        if scaled < MIN_QTY:
            self._consume_entry_day(
                state,
                "rich_qty_skip",
                rich_factor=factor,
                qty_base=q0,
            )
            return None
        return round(max(scaled, MIN_QTY), 1)

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
        scaled_qty = self._scaled_quantity(state, is_call, meta_extra)
        if scaled_qty is None:
            return None
        trade = super()._open_single(
            state,
            expiry,
            exp_dt,
            chain,
            is_call,
            scaled_qty,
            call_bid=call_bid,
            put_bid=put_bid,
            meta_extra=meta_extra,
        )
        if trade is not None and self._pending_rich_meta and self._positions:
            self._positions[-1].metadata.update(self._pending_rich_meta)
        return trade

    def _ig_reset(self) -> None:
        self._ig_live = 0
        self._ig_wing_on = 0
        self._ig_short_d = 0
        self._ig_short_g = 0
        self._ig_full_d = 0
        self._ig_full_g = 0
        self._ig_recovered = 0
        self._ig_still_breach = 0
        self._ig_induced = 0
        self._ig_short_abs_d = array("f")
        self._ig_full_abs_d = array("f")
        self._ig_short_g_pct = array("f")
        self._ig_full_g_pct = array("f")
        self._ig_full_abs_v = array("f")

    def _meter_investor_greeks(self, state: Any) -> None:
        if not self._track_investor_greeks:
            return
        positions = self._positions
        if not positions:
            return
        sell_legs: List[Any] = []
        buy_on = False
        for pos in positions:
            for leg in pos.legs:
                if str(leg.get("side")) == "sell":
                    sell_legs.append(leg)
                elif str(leg.get("side")) == "buy":
                    buy_on = True
        if not sell_legs:
            return
        self._ig_short_view.legs = sell_legs
        g_short = portfolio_cash_greeks((self._ig_short_view,), state)
        g_full = portfolio_cash_greeks(positions, state)
        c_short = dg_limits_ok(g_short)
        c_full = dg_limits_ok(g_full)
        self._ig_live += 1
        if buy_on:
            self._ig_wing_on += 1
        if not c_short.delta_ok:
            self._ig_short_d += 1
        if not c_short.gamma_ok:
            self._ig_short_g += 1
        if not c_full.delta_ok:
            self._ig_full_d += 1
        if not c_full.gamma_ok:
            self._ig_full_g += 1
        if (not c_short.ok) and c_full.ok:
            self._ig_recovered += 1
        elif (not c_short.ok) and (not c_full.ok):
            self._ig_still_breach += 1
        elif c_short.ok and (not c_full.ok):
            self._ig_induced += 1
        self._ig_short_abs_d.append(abs(g_short.delta_pct))
        self._ig_full_abs_d.append(abs(g_full.delta_pct))
        self._ig_short_g_pct.append(g_short.gamma_pct)
        self._ig_full_g_pct.append(g_full.gamma_pct)
        self._ig_full_abs_v.append(abs(g_full.vega_pct))

    def investor_greeks_sidecar(self) -> Optional[Dict[str, Any]]:
        """One-row dump for the run bundle (None when tracking is off)."""
        if not self._track_investor_greeks:
            return None
        n = self._ig_live

        def _pct(count: int) -> float:
            if n <= 0:
                return float("nan")
            return 100.0 * float(count) / float(n)

        def _p95(xs: array) -> float:
            if not xs:
                return float("nan")
            ordered = sorted(float(x) for x in xs)
            if len(ordered) == 1:
                return ordered[0]
            # Linear interpolation (same default as numpy.percentile).
            k = (len(ordered) - 1) * 0.95
            f = math.floor(k)
            c = math.ceil(k)
            if f == c:
                return ordered[int(k)]
            return ordered[f] * (c - k) + ordered[c] * (k - f)

        def _max(xs: array) -> float:
            if not xs:
                return float("nan")
            return float(max(xs))

        def _min(xs: array) -> float:
            if not xs:
                return float("nan")
            return float(min(xs))

        def _mean(xs: array) -> float:
            if not xs:
                return float("nan")
            return float(sum(xs) / len(xs))

        return {
            "live_bars": n,
            "wing_on_pct": _pct(self._ig_wing_on),
            "short_breach_d_pct": _pct(self._ig_short_d),
            "short_breach_g_pct": _pct(self._ig_short_g),
            "full_breach_d_pct": _pct(self._ig_full_d),
            "full_breach_g_pct": _pct(self._ig_full_g),
            "recovered_pct": _pct(self._ig_recovered),
            "still_breach_pct": _pct(self._ig_still_breach),
            "induced_pct": _pct(self._ig_induced),
            "short_p95_abs_d_pct": _p95(self._ig_short_abs_d),
            "short_max_abs_d_pct": _max(self._ig_short_abs_d),
            "full_p95_abs_d_pct": _p95(self._ig_full_abs_d),
            "full_max_abs_d_pct": _max(self._ig_full_abs_d),
            "short_min_g_pct": _min(self._ig_short_g_pct),
            "full_min_g_pct": _min(self._ig_full_g_pct),
            "mean_abs_v_pct": _mean(self._ig_full_abs_v),
            "track_investor_greeks": 1,
        }

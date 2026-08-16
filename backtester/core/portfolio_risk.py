"""
Portfolio cash-Greek risk as % of AUM.

Research conventions (desk-common defaults until an investor confirms “(1%)”):

    D$  = Σ sign · qty · δ · S          cash delta (USD notional)
    G$  = Σ sign · qty · Γ · S · (0.01·S)
          = change in cash delta for a +1% spot move
    V$  = Σ sign · qty · ν_1pt          USD P&L for +1.00 vol point
    T$  = Σ sign · qty · θ_day          USD P&L per calendar day
    %   = 100 · $ / AUM                 AUM defaults to state.nav_usd

sign = +1 buy / −1 sell.

Delta prefers live exchange ``quote.delta``; gamma / vega / theta are always
Black-Scholes (r=0) from mark_iv (percent → /100). Missing quotes fall back to
leg ``entry_iv`` so risk is never silently dropped to zero.

Default investor bands (%% of AUM)::

    |D%| < 30 when G$ > 0, else |D%| < 10
    G% > −10
    |V%| < 0.2
    T% > −1

Strategy-agnostic: any iterable of OpenPosition with standard leg dicts
(``side``, ``qty``, ``strike``, ``is_call``, ``expiry``, optional ``entry_iv``).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import timezone
from typing import Any, Iterable, List, Optional, Tuple

from backtester.core.expiry_utils import expiry_dt_utc
from backtester.core.pricing import (
    HOURS_PER_YEAR,
    bs_call_delta,
    bs_gamma,
    bs_put_delta,
    bs_theta_per_day,
    bs_vega_1pt,
)

_MIN_AUM = 1.0
_QTY_STEP = 0.1


@dataclass(frozen=True)
class CashGreeks:
    """Dollar cash Greeks for a unit size (typically qty=1) or a portfolio sum."""

    delta_cash: float = 0.0
    gamma_cash_1pct: float = 0.0
    vega_cash_1pt: float = 0.0
    theta_cash_day: float = 0.0

    def scaled(self, qty: float) -> "CashGreeks":
        return CashGreeks(
            delta_cash=self.delta_cash * qty,
            gamma_cash_1pct=self.gamma_cash_1pct * qty,
            vega_cash_1pt=self.vega_cash_1pt * qty,
            theta_cash_day=self.theta_cash_day * qty,
        )

    def plus(self, other: "CashGreeks") -> "CashGreeks":
        return CashGreeks(
            delta_cash=self.delta_cash + other.delta_cash,
            gamma_cash_1pct=self.gamma_cash_1pct + other.gamma_cash_1pct,
            vega_cash_1pt=self.vega_cash_1pt + other.vega_cash_1pt,
            theta_cash_day=self.theta_cash_day + other.theta_cash_day,
        )


@dataclass(frozen=True)
class PortfolioGreeks:
    """Portfolio cash Greeks in USD and as % of AUM."""

    delta_cash: float
    gamma_cash_1pct: float
    vega_cash_1pt: float
    theta_cash_day: float
    aum: float
    delta_pct: float
    gamma_pct: float
    vega_pct: float
    theta_pct: float
    n_legs: int = 0
    n_degraded: int = 0  # legs priced from entry_iv fallback

    @staticmethod
    def from_cash(cash: CashGreeks, aum: float, n_legs: int = 0, n_degraded: int = 0) -> "PortfolioGreeks":
        a = max(float(aum), _MIN_AUM)
        return PortfolioGreeks(
            delta_cash=cash.delta_cash,
            gamma_cash_1pct=cash.gamma_cash_1pct,
            vega_cash_1pt=cash.vega_cash_1pt,
            theta_cash_day=cash.theta_cash_day,
            aum=a,
            delta_pct=100.0 * cash.delta_cash / a,
            gamma_pct=100.0 * cash.gamma_cash_1pct / a,
            vega_pct=100.0 * cash.vega_cash_1pt / a,
            theta_pct=100.0 * cash.theta_cash_day / a,
            n_legs=n_legs,
            n_degraded=n_degraded,
        )

    def as_cash(self) -> CashGreeks:
        return CashGreeks(
            delta_cash=self.delta_cash,
            gamma_cash_1pct=self.gamma_cash_1pct,
            vega_cash_1pt=self.vega_cash_1pt,
            theta_cash_day=self.theta_cash_day,
        )


@dataclass(frozen=True)
class InvestorGreekLimits:
    """Cash-Greek bands as % of AUM (investor email defaults)."""

    delta_pct_when_gamma_pos: float = 30.0
    delta_pct_when_gamma_neg: float = 10.0
    gamma_pct_floor: float = -10.0
    vega_pct_abs: float = 0.2
    theta_pct_floor: float = -1.0


DEFAULT_INVESTOR_LIMITS = InvestorGreekLimits()


@dataclass(frozen=True)
class LimitsCheck:
    ok: bool
    delta_ok: bool
    gamma_ok: bool
    vega_ok: bool
    theta_ok: bool
    delta_band: float
    binding: Tuple[str, ...] = field(default_factory=tuple)


def _leg_sign(side: str) -> float:
    return -1.0 if str(side).lower() == "sell" else 1.0


def _resolve_aum(state: Any, aum: Optional[float]) -> float:
    if aum is not None and aum > 0:
        return float(aum)
    nav = getattr(state, "nav_usd", None)
    if nav is not None and float(nav) > 0:
        return float(nav)
    eq = getattr(state, "equity_usd", None)
    if eq is not None and float(eq) > 0:
        return float(eq)
    return _MIN_AUM


def _t_years(state: Any, expiry: str) -> float:
    tz = getattr(getattr(state, "dt", None), "tzinfo", None) or timezone.utc
    exp_dt = expiry_dt_utc(str(expiry), tz)
    if exp_dt is None:
        return 0.0
    now = getattr(state, "dt", None)
    if now is None:
        return 0.0
    seconds = (exp_dt - now).total_seconds()
    if seconds <= 0:
        return 0.0
    return seconds / (HOURS_PER_YEAR * 3600.0)


def _sigma_from_iv_pct(mark_iv: float) -> float:
    """Parquet / quote mark_iv is a percent (34.4 = 34.4%)."""
    iv = float(mark_iv)
    if iv <= 0:
        return 0.0
    return iv / 100.0


def unit_cash_greeks(
    state: Any,
    *,
    strike: float,
    is_call: bool,
    expiry: str,
    side: str = "sell",
    mark_iv: Optional[float] = None,
    delta: Optional[float] = None,
    qty: float = 1.0,
) -> CashGreeks:
    """Cash Greeks for ``qty`` contracts of one leg (signed by ``side``)."""
    spot = float(getattr(state, "spot", 0.0) or 0.0)
    if spot <= 0 or qty == 0:
        return CashGreeks()

    T = _t_years(state, expiry)
    sigma = _sigma_from_iv_pct(mark_iv if mark_iv is not None else 0.0)

    if delta is not None and math.isfinite(float(delta)):
        d = float(delta)
    elif is_call:
        d = bs_call_delta(spot, float(strike), T, sigma)
    else:
        d = bs_put_delta(spot, float(strike), T, sigma)

    gamma = bs_gamma(spot, float(strike), T, sigma)
    vega_1pt = bs_vega_1pt(spot, float(strike), T, sigma)
    theta_day = bs_theta_per_day(spot, float(strike), T, sigma, is_call=is_call)

    sign = _leg_sign(side)
    q = sign * float(qty)
    # D$ = sign·qty·δ·S; G$ = sign·qty·Γ·S·(0.01·S)
    return CashGreeks(
        delta_cash=q * d * spot,
        gamma_cash_1pct=q * gamma * spot * (0.01 * spot),
        vega_cash_1pt=q * vega_1pt,
        theta_cash_day=q * theta_day,
    )


def _leg_unit_from_position_leg(state: Any, leg: dict) -> Tuple[CashGreeks, bool]:
    """Return (cash greeks for full leg qty, degraded_flag)."""
    expiry = str(leg.get("expiry", ""))
    strike = float(leg.get("strike", 0.0))
    is_call = bool(leg.get("is_call"))
    side = str(leg.get("side", "sell"))
    qty = float(leg.get("qty", 1.0))

    quote = None
    get_option = getattr(state, "get_option", None)
    if callable(get_option) and expiry:
        quote = get_option(expiry, strike, is_call)

    degraded = False
    if quote is not None:
        mark_iv = float(getattr(quote, "mark_iv", 0.0) or 0.0)
        delta = getattr(quote, "delta", None)
        if delta is not None:
            delta = float(delta)
    else:
        degraded = True
        mark_iv = float(leg.get("entry_iv", 0.0) or 0.0)
        delta = None  # force BS from entry_iv

    cash = unit_cash_greeks(
        state,
        strike=strike,
        is_call=is_call,
        expiry=expiry,
        side=side,
        mark_iv=mark_iv,
        delta=delta,
        qty=qty,
    )
    return cash, degraded


def portfolio_cash_greeks(
    positions: Iterable[Any],
    state: Any,
    aum: Optional[float] = None,
) -> PortfolioGreeks:
    """Sum cash Greeks over all legs of ``positions``; % of AUM (nav_usd)."""
    total = CashGreeks()
    n_legs = 0
    n_degraded = 0
    for pos in positions:
        legs = getattr(pos, "legs", None) or []
        for leg in legs:
            cash, degraded = _leg_unit_from_position_leg(state, leg)
            total = total.plus(cash)
            n_legs += 1
            if degraded:
                n_degraded += 1
    return PortfolioGreeks.from_cash(
        total,
        _resolve_aum(state, aum),
        n_legs=n_legs,
        n_degraded=n_degraded,
    )


def limits_ok(
    greeks: PortfolioGreeks,
    limits: InvestorGreekLimits = DEFAULT_INVESTOR_LIMITS,
) -> LimitsCheck:
    """Check portfolio cash-Greek % against investor bands."""
    if greeks.gamma_cash_1pct > 0:
        delta_band = float(limits.delta_pct_when_gamma_pos)
    else:
        delta_band = float(limits.delta_pct_when_gamma_neg)

    delta_ok = abs(greeks.delta_pct) < delta_band
    gamma_ok = greeks.gamma_pct > float(limits.gamma_pct_floor)
    vega_ok = abs(greeks.vega_pct) < float(limits.vega_pct_abs)
    theta_ok = greeks.theta_pct > float(limits.theta_pct_floor)

    binding: List[str] = []
    if not delta_ok:
        binding.append("delta")
    if not gamma_ok:
        binding.append("gamma")
    if not vega_ok:
        binding.append("vega")
    if not theta_ok:
        binding.append("theta")

    return LimitsCheck(
        ok=not binding,
        delta_ok=delta_ok,
        gamma_ok=gamma_ok,
        vega_ok=vega_ok,
        theta_ok=theta_ok,
        delta_band=delta_band,
        binding=tuple(binding),
    )


def dg_limits_ok(
    greeks: PortfolioGreeks,
    limits: InvestorGreekLimits = DEFAULT_INVESTOR_LIMITS,
) -> LimitsCheck:
    """Like ``limits_ok`` but vega and theta are ignored (always treated as ok)."""
    check = limits_ok(greeks, limits)
    binding = tuple(b for b in check.binding if b in ("delta", "gamma"))
    return LimitsCheck(
        ok=not binding,
        delta_ok=check.delta_ok,
        gamma_ok=check.gamma_ok,
        vega_ok=True,
        theta_ok=True,
        delta_band=check.delta_band,
        binding=binding,
    )


def scaled_investor_limits(
    fraction: float,
    limits: InvestorGreekLimits = DEFAULT_INVESTOR_LIMITS,
) -> InvestorGreekLimits:
    """Tighten every band to ``fraction`` of itself (inner hysteresis).

    ``fraction`` must be in (0, 1].  Negative floors (gamma, theta) move
    toward zero — e.g. −10 with 0.7 → −7, a stricter “deeply inside” test.
    """
    f = float(fraction)
    if not (0.0 < f <= 1.0):
        raise ValueError("limits fraction must be in (0, 1], got %r" % fraction)
    return InvestorGreekLimits(
        delta_pct_when_gamma_pos=float(limits.delta_pct_when_gamma_pos) * f,
        delta_pct_when_gamma_neg=float(limits.delta_pct_when_gamma_neg) * f,
        gamma_pct_floor=float(limits.gamma_pct_floor) * f,
        vega_pct_abs=float(limits.vega_pct_abs) * f,
        theta_pct_floor=float(limits.theta_pct_floor) * f,
    )


def _portfolio_from_sum(cash: CashGreeks, aum: float) -> PortfolioGreeks:
    return PortfolioGreeks.from_cash(cash, aum)


def max_qty_within_limits(
    current: CashGreeks | PortfolioGreeks,
    unit_candidate: CashGreeks,
    base_qty: float,
    limits: InvestorGreekLimits = DEFAULT_INVESTOR_LIMITS,
    aum: Optional[float] = None,
    min_qty: float = _QTY_STEP,
    qty_step: float = _QTY_STEP,
) -> Optional[float]:
    """Largest qty ≤ base_qty (stepped) such that current + qty·unit is inside limits.

    ``unit_candidate`` must already include side sign for qty=1.
    Returns None if even ``min_qty`` would breach.
    """
    if isinstance(current, PortfolioGreeks):
        aum_val = float(aum) if aum is not None else current.aum
        cur = current.as_cash()
    else:
        aum_val = float(aum) if aum is not None else _MIN_AUM
        cur = current

    if base_qty < min_qty - 1e-12:
        return None

    # Round base down to step
    n_max = int(math.floor(base_qty / qty_step + 1e-9))
    if n_max < 1:
        return None

    best: Optional[float] = None
    for n in range(1, n_max + 1):
        qty = round(n * qty_step, 10)
        post = _portfolio_from_sum(cur.plus(unit_candidate.scaled(qty)), aum_val)
        if limits_ok(post, limits).ok:
            best = qty
        # Keep searching upward — bands are not always monotone in qty for
        # mixed books, but for a single short add they usually tighten.
        # Still evaluate all steps so we return the max feasible.
    return best


def min_qty_for_dg_limits(
    current: CashGreeks | PortfolioGreeks,
    unit_candidate: CashGreeks,
    limits: InvestorGreekLimits = DEFAULT_INVESTOR_LIMITS,
    aum: Optional[float] = None,
    min_qty: float = _QTY_STEP,
    qty_step: float = _QTY_STEP,
    max_qty: float = 50.0,
) -> Optional[float]:
    """Smallest qty in ``[min_qty, max_qty]`` such that current + qty·unit is inside D/G bands.

    Vega and theta are ignored.  ``unit_candidate`` is the cash Greeks of
    qty=1 (side sign already applied).  Returns None if no size in the
    search window works (empty window or search ceiling hit still in breach).
    ``max_qty`` is a computational ceiling, not a strategy inventory cap.
    """
    if isinstance(current, PortfolioGreeks):
        aum_val = float(aum) if aum is not None else current.aum
        cur = current.as_cash()
    else:
        aum_val = float(aum) if aum is not None else _MIN_AUM
        cur = current

    if max_qty + 1e-12 < min_qty:
        return None

    qty = float(min_qty)
    while qty <= max_qty + 1e-12:
        post = _portfolio_from_sum(cur.plus(unit_candidate.scaled(qty)), aum_val)
        if dg_limits_ok(post, limits).ok:
            return round(qty, 10)
        nxt = round(qty + qty_step, 10)
        if nxt <= qty:
            break
        qty = nxt
    return None

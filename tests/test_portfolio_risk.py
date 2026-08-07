"""Unit tests for BS greeks in pricing.py and portfolio_risk.py."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from backtester.core.pricing import (
    bs_call_delta,
    bs_gamma,
    bs_put_delta,
    bs_theta_per_day,
    bs_vega,
    bs_vega_1pt,
    norm_pdf,
)
from backtester.core.portfolio_risk import (
    CashGreeks,
    limits_ok,
    max_qty_within_limits,
    portfolio_cash_greeks,
    unit_cash_greeks,
)
from backtester.core.strategy_base import OpenPosition

UTC = timezone.utc
SPOT = 100_000.0


def test_norm_pdf_at_zero():
    assert abs(norm_pdf(0.0) - 0.3989422804) < 1e-8


def test_bs_gamma_vega_theta_atm_positive_finite():
    S, K, T, sig = SPOT, SPOT, 90 / 365.0, 0.50
    g = bs_gamma(S, K, T, sig)
    v = bs_vega(S, K, T, sig)
    v1 = bs_vega_1pt(S, K, T, sig)
    th = bs_theta_per_day(S, K, T, sig, is_call=True)
    assert g > 0
    assert v > 0
    assert abs(v1 - v * 0.01) < 1e-9
    assert th < 0  # long option decays


def test_bs_put_call_delta_parity():
    S, K, T, sig = SPOT, 95_000.0, 0.25, 0.4
    assert abs(bs_call_delta(S, K, T, sig) - bs_put_delta(S, K, T, sig) - 1.0) < 1e-12


def test_bs_gamma_zero_at_expiry():
    assert bs_gamma(SPOT, SPOT, 0.0, 0.5) == 0.0


def _state(dt, spot=SPOT, nav_usd=100_000.0, quotes=None):
    quotes = quotes or {}

    def get_option(expiry, strike, is_call):
        return quotes.get((expiry, float(strike), bool(is_call)))

    return SimpleNamespace(
        dt=dt,
        spot=spot,
        nav_usd=nav_usd,
        equity_usd=nav_usd,
        get_option=get_option,
    )


def _quote(strike, is_call, delta, mark_iv=50.0, expiry="28MAY26"):
    return SimpleNamespace(
        strike=strike,
        is_call=is_call,
        expiry=expiry,
        delta=delta,
        mark_iv=mark_iv,
        bid=0.01,
        ask=0.011,
        mark=0.01,
        spot=SPOT,
    )


def test_unit_short_has_negative_vega_and_gamma():
    dt = datetime(2026, 2, 1, 15, 0, tzinfo=UTC)
    state = _state(dt)
    u = unit_cash_greeks(
        state,
        strike=105_000.0,
        is_call=True,
        expiry="28MAY26",
        side="sell",
        mark_iv=50.0,
        delta=0.25,
        qty=1.0,
    )
    assert u.vega_cash_1pt < 0
    assert u.gamma_cash_1pct < 0
    assert u.delta_cash < 0  # short call
    assert u.theta_cash_day > 0  # short earns theta


def test_portfolio_pct_scales_with_aum():
    dt = datetime(2026, 2, 1, 15, 0, tzinfo=UTC)
    exp = "28MAY26"
    q = _quote(105_000.0, True, 0.25, mark_iv=50.0, expiry=exp)
    state = _state(dt, nav_usd=100_000.0, quotes={(exp, 105_000.0, True): q})
    pos = OpenPosition(
        legs=[{
            "strike": 105_000.0,
            "is_call": True,
            "expiry": exp,
            "side": "sell",
            "qty": 1.0,
            "entry_iv": 50.0,
        }],
        entry_time=dt,
        entry_spot=SPOT,
        entry_price_usd=100.0,
        fees_open=0.0,
        metadata={"direction": "sell"},
    )
    g1 = portfolio_cash_greeks([pos], state, aum=100_000.0)
    g2 = portfolio_cash_greeks([pos], state, aum=200_000.0)
    assert abs(g1.vega_pct - 2.0 * g2.vega_pct) < 1e-9
    assert abs(g1.vega_cash_1pt - g2.vega_cash_1pt) < 1e-9


def test_limits_ok_short_gamma_uses_tight_delta_band():
    g = CashGreeks(delta_cash=-15_000, gamma_cash_1pct=-100, vega_cash_1pt=-50, theta_cash_day=10)
    from backtester.core.portfolio_risk import PortfolioGreeks
    pg = PortfolioGreeks.from_cash(g, aum=100_000.0)
    # |D%|=15 > 10 short-gamma band
    check = limits_ok(pg)
    assert check.delta_band == 10.0
    assert not check.delta_ok
    assert "delta" in check.binding


def test_max_qty_respects_vega_band():
    aum = 100_000.0
    # unit short: V$ = -500 → V% = -0.5% per contract; band ±0.2%
    unit = CashGreeks(
        delta_cash=-100.0,
        gamma_cash_1pct=-50.0,
        vega_cash_1pt=-500.0,
        theta_cash_day=10.0,
    )
    current = CashGreeks()
    # 0.1 * 500 = 50 → 0.05% OK; 0.2 * 500 = 100 → 0.1% OK; 0.4*500=200 → 0.2% NOT < 0.2
    qty = max_qty_within_limits(current, unit, base_qty=1.0, aum=aum, min_qty=0.1)
    assert qty is not None
    assert qty <= 0.3 + 1e-9  # |V%| must stay < 0.2
    post = current.plus(unit.scaled(qty))
    from backtester.core.portfolio_risk import PortfolioGreeks
    assert limits_ok(PortfolioGreeks.from_cash(post, aum)).ok


def test_max_qty_none_when_min_breaches():
    aum = 100_000.0
    unit = CashGreeks(
        delta_cash=0.0,
        gamma_cash_1pct=0.0,
        vega_cash_1pt=-5_000.0,  # 0.1 → V% = -0.5 already over
        theta_cash_day=0.0,
    )
    assert max_qty_within_limits(CashGreeks(), unit, base_qty=1.0, aum=aum) is None


def test_degraded_fallback_uses_entry_iv():
    dt = datetime(2026, 2, 1, 15, 0, tzinfo=UTC)
    state = _state(dt, quotes={})  # no live quotes
    pos = OpenPosition(
        legs=[{
            "strike": 100_000.0,
            "is_call": True,
            "expiry": "28MAY26",
            "side": "sell",
            "qty": 0.5,
            "entry_iv": 40.0,
        }],
        entry_time=dt,
        entry_spot=SPOT,
        entry_price_usd=100.0,
        fees_open=0.0,
        metadata={"direction": "sell"},
    )
    g = portfolio_cash_greeks([pos], state)
    assert g.n_legs == 1
    assert g.n_degraded == 1
    assert g.vega_cash_1pt != 0.0

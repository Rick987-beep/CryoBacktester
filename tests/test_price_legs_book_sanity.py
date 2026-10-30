"""Absurd / illiquid book quotes in price_legs (NAV executable path).

Regression for the Aug 2026 winged vertical case: wing bid=0.01 vs mark=0.125
must not be trusted while the short uses mark×(1+slip). See
docs/repricing_illiquid_book.md.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from backtester.core.config import cfg
from backtester.core.market_replay import OptionQuote
from backtester.core.strategy_base import OpenPosition, price_legs


def _quote(**kwargs) -> OptionQuote:
    base = dict(
        strike=70000.0,
        is_call=True,
        expiry="4SEP26",
        bid=0.0,
        ask=0.0,
        mark=0.1,
        mark_iv=50.0,
        delta=0.5,
        spot=80000.0,
    )
    base.update(kwargs)
    return OptionQuote(**base)


def _state(quotes: dict[tuple, OptionQuote]):
    def get_option(expiry, strike, is_call):
        return quotes.get((str(expiry), float(strike), bool(is_call)))

    return SimpleNamespace(
        dt=datetime(2026, 8, 25, 9, 15, tzinfo=timezone.utc),
        spot=79822.0625,
        get_option=get_option,
    )


def _vertical_pos(qty: float = 1.6) -> OpenPosition:
    spot = 62851.5
    short = {
        "expiry": "4SEP26",
        "strike": 66000.0,
        "is_call": True,
        "side": "sell",
        "qty": qty,
        "price_btc": 0.0085,
        "entry_price": 0.0085,
        "entry_spot": spot,
    }
    wing = {
        "expiry": "4SEP26",
        "strike": 70000.0,
        "is_call": True,
        "side": "buy",
        "qty": qty,
        "price_btc": 0.0016,
        "entry_price": 0.0016,
        "entry_spot": spot,
    }
    return OpenPosition(
        legs=[short, wing],
        entry_time=datetime(2026, 8, 17, tzinfo=timezone.utc),
        entry_spot=spot,
        entry_price_usd=0.0,
        fees_open=41.91,
        metadata={"direction": "sell", "expiry": "4SEP26"},
    )


@pytest.fixture
def book_frac_default(monkeypatch):
    """Ensure default frac without mutating other tests' process forever."""
    old = cfg.repricing
    monkeypatch.setattr(
        cfg,
        "repricing",
        replace(old, book_vs_mark_min_frac=0.25, slip_pct_zero_price=0.25, min_mark_usd=15.0),
        raising=False,
    )
    # strategy_base imports cfg as _cfg — same object
    yield
    monkeypatch.setattr(cfg, "repricing", old, raising=False)


def test_garbage_wing_bid_uses_mark_fallback(book_frac_default):
    spot = 79822.0625
    short_q = _quote(
        strike=66000.0, bid=0.17, ask=0.0, mark=0.1746, spot=spot, delta=-0.9,
    )
    wing_q = _quote(
        strike=70000.0, bid=0.01, ask=0.13, mark=0.1253, spot=spot, delta=0.8,
    )
    state = _state({
        ("4SEP26", 66000.0, True): short_q,
        ("4SEP26", 70000.0, True): wing_q,
    })
    pos = _vertical_pos()
    total = price_legs(state, pos, mode="executable")
    assert total is not None
    per = pos._last_reprice_legs
    assert per is not None and len(per) == 2
    short_usd, wing_usd = per

    # Short: ask=0 → mark × 1.25
    assert short_usd == pytest.approx(0.1746 * 1.25 * spot * 1.6, rel=1e-6)
    # Wing: bid/mark ≈ 0.08 < 0.25 → mark × 0.75 (not 0.01)
    assert wing_usd == pytest.approx(0.1253 * 0.75 * spot * 1.6, rel=1e-6)
    assert wing_usd > 0.01 * spot * 1.6 * 5  # far above junk bid


def test_legacy_frac_zero_trusts_micro_bid(monkeypatch):
    old = cfg.repricing
    monkeypatch.setattr(
        cfg,
        "repricing",
        replace(old, book_vs_mark_min_frac=0.0, slip_pct_zero_price=0.25, min_mark_usd=15.0),
        raising=False,
    )
    try:
        spot = 79822.0625
        state = _state({
            ("4SEP26", 66000.0, True): _quote(
                strike=66000.0, ask=0.0, mark=0.1746, spot=spot,
            ),
            ("4SEP26", 70000.0, True): _quote(
                strike=70000.0, bid=0.01, mark=0.1253, spot=spot,
            ),
        })
        pos = _vertical_pos()
        price_legs(state, pos, mode="executable")
        wing_usd = pos._last_reprice_legs[1]
        assert wing_usd == pytest.approx(0.01 * spot * 1.6, rel=1e-6)
    finally:
        monkeypatch.setattr(cfg, "repricing", old, raising=False)


def test_mark_mode_ignores_book_sanity(book_frac_default):
    spot = 79822.0625
    state = _state({
        ("4SEP26", 66000.0, True): _quote(
            strike=66000.0, ask=0.0, mark=0.1746, spot=spot,
        ),
        ("4SEP26", 70000.0, True): _quote(
            strike=70000.0, bid=0.01, mark=0.1253, spot=spot,
        ),
    })
    pos = _vertical_pos()
    price_legs(state, pos, mode="mark")
    short_usd, wing_usd = pos._last_reprice_legs
    assert short_usd == pytest.approx(0.1746 * spot * 1.6, rel=1e-6)
    assert wing_usd == pytest.approx(0.1253 * spot * 1.6, rel=1e-6)


def test_healthy_bid_still_used(book_frac_default):
    spot = 80000.0
    state = _state({
        ("4SEP26", 70000.0, True): _quote(
            strike=70000.0, bid=0.10, ask=0.12, mark=0.11, spot=spot,
        ),
    })
    pos = OpenPosition(
        legs=[{
            "expiry": "4SEP26",
            "strike": 70000.0,
            "is_call": True,
            "side": "buy",
            "qty": 1.0,
            "price_btc": 0.05,
            "entry_price": 0.05,
            "entry_spot": spot,
        }],
        entry_time=datetime(2026, 8, 1, tzinfo=timezone.utc),
        entry_spot=spot,
        entry_price_usd=0.0,
        fees_open=0.0,
        metadata={"direction": "buy"},
    )
    price_legs(state, pos, mode="executable")
    assert pos._last_reprice_legs[0] == pytest.approx(0.10 * spot, rel=1e-9)

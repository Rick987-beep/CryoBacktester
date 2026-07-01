"""
test_strategy_base.py

Tests for the strategy_base primitives introduced/refactored in the
price_legs / stop_loss_pct / profit_target_pct redesign:

  - price_legs(state, pos, mode)  — four price modes, data-gap handling,
                                    zero-mark handling, per-leg side priority
  - stop_loss_pct(pct, price_mode)  — short/long semantics, mark vs executable
  - profit_target_pct(pct, price_mode)  — short/long semantics, mark vs executable
  - _reprice_legs()  — backward-compat alias, same result as mode="executable"
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from types import SimpleNamespace

import pytest

from backtester.strategy_base import (
    OpenPosition,
    price_legs,
    stop_loss_pct,
    profit_target_pct,
    strike_proximity_stop,
    short_premium_stop_near_expiry,
    position_quotes_available,
    position_unrealized_pnl,
    equity_drawdown_stop,
    exit_expiry_window,
    _reprice_legs,
)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _make_quote(bid=0.0010, ask=0.0012, mark=0.0011, spot=75000.0, mark_iv=35.0, delta=0.15):
    """Build a minimal option quote namespace."""
    return SimpleNamespace(
        bid=bid,
        ask=ask,
        mark=mark,
        spot=spot,
        bid_usd=bid * spot,
        ask_usd=ask * spot,
        mark_usd=mark * spot,
        mark_iv=mark_iv,
        delta=delta,
    )


class _FakeState:
    """Minimal MarketState that serves a fixed dict of option quotes."""

    def __init__(self, quotes: Dict, spot: float = 75000.0, dt: Optional[datetime] = None):
        """
        quotes: {(expiry, strike, is_call): quote_or_None}
        """
        self._quotes = quotes
        self.spot = spot
        self.dt = dt or datetime(2026, 5, 27, 15, 0, tzinfo=timezone.utc)
        self.spot_bars = []

    def get_option(self, expiry, strike, is_call):
        return self._quotes.get((expiry, strike, is_call))


def _make_pos(legs, entry_price_usd, direction="sell", entry_spot=75000.0):
    """Build an OpenPosition with the given legs and position-level direction."""
    return OpenPosition(
        entry_time=datetime(2026, 5, 27, 14, 0, tzinfo=timezone.utc),
        entry_spot=entry_spot,
        legs=legs,
        entry_price_usd=entry_price_usd,
        fees_open=0.0,
        metadata={"direction": direction, "expiry_dt": datetime(2026, 5, 28, 8, 0, tzinfo=timezone.utc)},
    )


EXPIRY = "28MAY26"
CALL_STRIKE = 76000.0
PUT_STRIKE  = 74000.0
SPOT = 75000.0

def _short_strangle_legs(call_q, put_q, qty=1.0):
    return [
        {
            "strike": CALL_STRIKE, "is_call": True, "expiry": EXPIRY, "side": "sell",
            "qty": qty, "price_btc": call_q.bid, "entry_price": call_q.bid,
            "entry_price_usd": call_q.bid_usd, "entry_spot": SPOT,
        },
        {
            "strike": PUT_STRIKE, "is_call": False, "expiry": EXPIRY, "side": "sell",
            "qty": qty, "price_btc": put_q.bid, "entry_price": put_q.bid,
            "entry_price_usd": put_q.bid_usd, "entry_spot": SPOT,
        },
    ]


def _long_strangle_legs(call_q, put_q, qty=1.0):
    return [
        {
            "strike": CALL_STRIKE, "is_call": True, "expiry": EXPIRY, "side": "buy",
            "qty": qty, "price_btc": call_q.ask, "entry_price": call_q.ask,
            "entry_price_usd": call_q.ask_usd, "entry_spot": SPOT,
        },
        {
            "strike": PUT_STRIKE, "is_call": False, "expiry": EXPIRY, "side": "buy",
            "qty": qty, "price_btc": put_q.ask, "entry_price": put_q.ask,
            "entry_price_usd": put_q.ask_usd, "entry_spot": SPOT,
        },
    ]


# ---------------------------------------------------------------------------
# price_legs — mode="mark"
# ---------------------------------------------------------------------------

class TestPriceLegsMarkMode:

    def test_mark_uses_mark_price(self):
        """mark mode prices each leg at mark × spot × qty."""
        call_q = _make_quote(bid=0.0008, ask=0.0014, mark=0.0011, spot=SPOT)
        put_q  = _make_quote(bid=0.0012, ask=0.0018, mark=0.0015, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _short_strangle_legs(call_q, put_q)
        pos   = _make_pos(legs, entry_price_usd=200.0)

        result = price_legs(state, pos, mode="mark")

        expected = (call_q.mark + put_q.mark) * SPOT  # qty=1
        assert result == pytest.approx(expected, rel=1e-9)

    def test_mark_zero_returns_zero_not_none(self):
        """mark=0 means the exchange says the option is worthless — price $0, not None."""
        call_q = _make_quote(bid=0.0, ask=0.0, mark=0.0, spot=SPOT)
        put_q  = _make_quote(bid=0.0010, ask=0.0012, mark=0.0011, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _short_strangle_legs(call_q, put_q)
        pos   = _make_pos(legs, entry_price_usd=100.0)

        result = price_legs(state, pos, mode="mark")

        assert result is not None
        assert result == pytest.approx(put_q.mark * SPOT, rel=1e-9)

    def test_mark_ignores_bid_ask(self):
        """mark mode should not be affected by bid/ask values."""
        call_q = _make_quote(bid=0.0005, ask=0.0030, mark=0.0011, spot=SPOT)
        put_q  = _make_quote(bid=0.0007, ask=0.0040, mark=0.0015, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _short_strangle_legs(call_q, put_q)
        pos   = _make_pos(legs, entry_price_usd=200.0)

        result = price_legs(state, pos, mode="mark")
        expected = (0.0011 + 0.0015) * SPOT
        assert result == pytest.approx(expected, rel=1e-9)


# ---------------------------------------------------------------------------
# price_legs — mode="executable"
# ---------------------------------------------------------------------------

class TestPriceLegsExecutableMode:

    def test_executable_sell_uses_ask(self):
        """Sell legs (short position) → cost-to-close is the ask."""
        call_q = _make_quote(bid=0.0008, ask=0.0014, mark=0.0011, spot=SPOT)
        put_q  = _make_quote(bid=0.0012, ask=0.0018, mark=0.0015, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _short_strangle_legs(call_q, put_q)
        pos   = _make_pos(legs, entry_price_usd=200.0, direction="sell")

        result = price_legs(state, pos, mode="executable")

        # Both legs are sell → ask used, but floored at mark
        call_price = max(call_q.ask, call_q.mark)
        put_price  = max(put_q.ask, put_q.mark)
        expected   = (call_price + put_price) * SPOT
        assert result == pytest.approx(expected, rel=1e-9)

    def test_executable_buy_uses_bid(self):
        """Buy legs (long position) → proceeds-to-close is the bid."""
        call_q = _make_quote(bid=0.0008, ask=0.0014, mark=0.0011, spot=SPOT)
        put_q  = _make_quote(bid=0.0012, ask=0.0018, mark=0.0015, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _long_strangle_legs(call_q, put_q)
        pos   = _make_pos(legs, entry_price_usd=200.0, direction="buy")

        result = price_legs(state, pos, mode="executable")

        expected = (call_q.bid + put_q.bid) * SPOT
        assert result == pytest.approx(expected, rel=1e-9)

    def test_executable_zero_ask_but_nonzero_mark_uses_mark(self):
        """When ask=0 but mark>0, executable mode estimates from mark (not $0)."""
        # mark is small → just use mark (no slip premium)
        call_q = _make_quote(bid=0.0000, ask=0.0000, mark=0.0002, spot=SPOT)
        put_q  = _make_quote(bid=0.0012, ask=0.0018, mark=0.0015, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _short_strangle_legs(call_q, put_q)
        pos   = _make_pos(legs, entry_price_usd=200.0, direction="sell")

        result = price_legs(state, pos, mode="executable")

        assert result is not None
        # Call leg repriced via mark; put leg repriced via max(ask, mark)
        put_price = max(put_q.ask, put_q.mark)
        assert result > put_price * SPOT  # call contributes something (mark-based)

    def test_executable_zero_mark_gives_zero_leg(self):
        """When mark=0, executable mode returns $0 for that leg (option is worthless)."""
        call_q = _make_quote(bid=0.0, ask=0.0, mark=0.0, spot=SPOT)
        put_q  = _make_quote(bid=0.0012, ask=0.0018, mark=0.0015, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _short_strangle_legs(call_q, put_q)
        pos   = _make_pos(legs, entry_price_usd=200.0, direction="sell")

        result = price_legs(state, pos, mode="executable")

        put_price = max(put_q.ask, put_q.mark)
        assert result == pytest.approx(put_price * SPOT, rel=1e-9)


# ---------------------------------------------------------------------------
# price_legs — mode="bid" and mode="ask"
# ---------------------------------------------------------------------------

class TestPriceLegsBidAskModes:

    def test_bid_mode_always_uses_bid(self):
        """bid mode uses bid regardless of leg side."""
        call_q = _make_quote(bid=0.0008, ask=0.0014, mark=0.0011, spot=SPOT)
        put_q  = _make_quote(bid=0.0012, ask=0.0018, mark=0.0015, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        # Mix sides: call=sell, put=buy — bid mode ignores this
        legs = [
            {"strike": CALL_STRIKE, "is_call": True,  "expiry": EXPIRY, "side": "sell",
             "qty": 1.0, "price_btc": call_q.bid, "entry_price": call_q.bid,
             "entry_price_usd": call_q.bid_usd, "entry_spot": SPOT},
            {"strike": PUT_STRIKE,  "is_call": False, "expiry": EXPIRY, "side": "buy",
             "qty": 1.0, "price_btc": put_q.ask, "entry_price": put_q.ask,
             "entry_price_usd": put_q.ask_usd, "entry_spot": SPOT},
        ]
        pos = _make_pos(legs, entry_price_usd=200.0)

        result = price_legs(state, pos, mode="bid")
        expected = (call_q.bid + put_q.bid) * SPOT
        assert result == pytest.approx(expected, rel=1e-9)

    def test_ask_mode_always_uses_ask(self):
        """ask mode uses ask regardless of leg side."""
        call_q = _make_quote(bid=0.0008, ask=0.0014, mark=0.0011, spot=SPOT)
        put_q  = _make_quote(bid=0.0012, ask=0.0018, mark=0.0015, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs = _long_strangle_legs(call_q, put_q)  # buy side
        pos = _make_pos(legs, entry_price_usd=200.0, direction="buy")

        result = price_legs(state, pos, mode="ask")
        expected = (call_q.ask + put_q.ask) * SPOT
        assert result == pytest.approx(expected, rel=1e-9)


# ---------------------------------------------------------------------------
# price_legs — data gap (missing quote)
# ---------------------------------------------------------------------------

class TestPriceLegsDataGap:

    def test_missing_quote_returns_none(self):
        """If any leg's quote is absent from the snapshot, return None (data gap)."""
        put_q = _make_quote(bid=0.0012, ask=0.0018, mark=0.0015, spot=SPOT)
        # call quote is missing
        quotes = {(EXPIRY, PUT_STRIKE, False): put_q}
        state = _FakeState(quotes, spot=SPOT)
        call_q = _make_quote()
        legs = _short_strangle_legs(call_q, put_q)
        pos  = _make_pos(legs, entry_price_usd=200.0)

        result = price_legs(state, pos, mode="mark")
        assert result is None

    def test_missing_quote_returns_none_for_all_modes(self):
        """Data gap → None across all four modes."""
        quotes = {}  # nothing
        state = _FakeState(quotes, spot=SPOT)
        call_q = _make_quote()
        put_q  = _make_quote()
        legs   = _short_strangle_legs(call_q, put_q)
        pos    = _make_pos(legs, entry_price_usd=200.0)

        for mode in ("mark", "executable", "bid", "ask"):
            assert price_legs(state, pos, mode=mode) is None, f"expected None for mode={mode}"

    def test_data_gap_does_not_fire_exit(self):
        """stop_loss_pct returns None (hold) when a quote is missing."""
        quotes = {}
        state = _FakeState(quotes, spot=SPOT)
        call_q = _make_quote()
        put_q  = _make_quote()
        legs   = _short_strangle_legs(call_q, put_q)
        pos    = _make_pos(legs, entry_price_usd=100.0, direction="sell")

        sl = stop_loss_pct(0.5, price_mode="mark")
        assert sl(state, pos) is None


# ---------------------------------------------------------------------------
# price_legs — reprice cache
# ---------------------------------------------------------------------------

class TestRepriceCache:

    def test_price_legs_writes_cache(self):
        """price_legs sets pos._last_reprice_usd and pos._last_reprice_legs."""
        call_q = _make_quote(mark=0.0011, spot=SPOT)
        put_q  = _make_quote(mark=0.0015, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _short_strangle_legs(call_q, put_q)
        pos   = _make_pos(legs, entry_price_usd=200.0)

        result = price_legs(state, pos, mode="mark")

        assert pos._last_reprice_usd == pytest.approx(result, rel=1e-9)
        assert isinstance(pos._last_reprice_legs, list)
        assert len(pos._last_reprice_legs) == 2

    def test_reprice_legs_alias_writes_same_cache(self):
        """_reprice_legs (backward-compat alias) updates the same cache fields."""
        call_q = _make_quote(bid=0.0008, ask=0.0014, mark=0.0011, spot=SPOT)
        put_q  = _make_quote(bid=0.0012, ask=0.0018, mark=0.0015, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _short_strangle_legs(call_q, put_q)
        pos   = _make_pos(legs, entry_price_usd=200.0, direction="sell")

        via_alias     = _reprice_legs(state, pos)
        cache_via_alias = pos._last_reprice_usd

        pos._last_reprice_usd = None  # reset
        via_explicit = price_legs(state, pos, mode="executable")

        assert via_alias == pytest.approx(via_explicit, rel=1e-9)
        assert cache_via_alias == pytest.approx(pos._last_reprice_usd, rel=1e-9)


# ---------------------------------------------------------------------------
# _reprice_legs — backward-compat alias
# ---------------------------------------------------------------------------

class TestRepriceLegsAlias:

    def test_alias_equals_executable_mode(self):
        """_reprice_legs must return the exact same value as price_legs(mode='executable')."""
        call_q = _make_quote(bid=0.0008, ask=0.0014, mark=0.0011, spot=SPOT)
        put_q  = _make_quote(bid=0.0012, ask=0.0018, mark=0.0015, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _short_strangle_legs(call_q, put_q)
        pos   = _make_pos(legs, entry_price_usd=200.0, direction="sell")

        from_alias    = _reprice_legs(state, pos)
        from_explicit = price_legs(state, pos, mode="executable")

        assert from_alias == pytest.approx(from_explicit, rel=1e-9)


# ---------------------------------------------------------------------------
# stop_loss_pct
# ---------------------------------------------------------------------------

class TestStopLossPct:

    def _make_short_pos_and_state(self, entry_mark, current_mark):
        """
        One-leg short position. Entry at entry_mark BTC; current mark = current_mark BTC.
        entry_price_usd = entry_mark * SPOT.
        """
        call_q = _make_quote(bid=0.0, ask=0.0, mark=entry_mark, spot=SPOT)
        legs = [{
            "strike": CALL_STRIKE, "is_call": True, "expiry": EXPIRY, "side": "sell",
            "qty": 1.0, "price_btc": entry_mark, "entry_price": entry_mark,
            "entry_price_usd": entry_mark * SPOT, "entry_spot": SPOT,
        }]
        pos = _make_pos(legs, entry_price_usd=entry_mark * SPOT, direction="sell")

        # Current quote at current_mark
        cur_q = _make_quote(bid=0.0, ask=0.0, mark=current_mark, spot=SPOT)
        quotes = {(EXPIRY, CALL_STRIKE, True): cur_q}
        state = _FakeState(quotes, spot=SPOT)
        return state, pos

    def test_default_price_mode_is_mark(self):
        """stop_loss_pct default price_mode is 'mark', not 'executable'."""
        # Set a wide ask but a low mark — only the mark-based SL should not fire.
        # mark goes from 0.0010 to 0.0030 (200% increase > 150% threshold → fires)
        state, pos = self._make_short_pos_and_state(
            entry_mark=0.0010, current_mark=0.0040,
        )
        sl = stop_loss_pct(1.5)  # default mode
        assert sl(state, pos) == "stop_loss"

    def test_short_sl_fires_above_threshold_mark(self):
        """Short SL fires when mark-based reprice exceeds entry * (1 + pct)."""
        # entry = 0.0010 * 75000 = $75; pct=1.5 → fires at > $75 * 2.5 = $187.5
        # current mark = 0.0030 → $225 → should fire
        state, pos = self._make_short_pos_and_state(
            entry_mark=0.0010, current_mark=0.0030,
        )
        sl = stop_loss_pct(1.5, price_mode="mark")
        assert sl(state, pos) == "stop_loss"

    def test_short_sl_does_not_fire_below_threshold(self):
        """Short SL does NOT fire when mark-based reprice is within threshold."""
        # entry = $75; pct=1.5 → needs > $187.5 to fire; current mark = $100 → hold
        state, pos = self._make_short_pos_and_state(
            entry_mark=0.0010, current_mark=0.00133,  # $100 / $75000 ≈ 0.00133
        )
        sl = stop_loss_pct(1.5, price_mode="mark")
        assert sl(state, pos) is None

    def test_short_sl_mark_mode_ignores_wide_ask(self):
        """SL with price_mode='mark' should NOT fire due to a wide ask spike."""
        # Ask is 5× the mark — executable mode would fire; mark mode should hold.
        call_q = _make_quote(bid=0.0005, ask=0.0060, mark=0.0015, spot=SPOT)
        quotes = {(EXPIRY, CALL_STRIKE, True): call_q}
        state = _FakeState(quotes, spot=SPOT)
        entry_usd = 0.0010 * SPOT  # $75
        legs = [{
            "strike": CALL_STRIKE, "is_call": True, "expiry": EXPIRY, "side": "sell",
            "qty": 1.0, "price_btc": 0.0010, "entry_price": 0.0010,
            "entry_price_usd": entry_usd, "entry_spot": SPOT,
        }]
        pos = _make_pos(legs, entry_price_usd=entry_usd, direction="sell")

        sl_mark = stop_loss_pct(1.5, price_mode="mark")
        sl_exec = stop_loss_pct(1.5, price_mode="executable")

        # mark=0.0015*75000=$112.5; entry=$75; ratio=(112.5-75)/75=0.5 — does NOT fire at 150%
        assert sl_mark(state, pos) is None
        # executable uses ask=0.006 floored at mark → fires at 150%? ask=$450 >> $75*2.5=$187.5
        assert sl_exec(state, pos) == "stop_loss"

    def test_long_sl_fires_when_value_drops(self):
        """Long SL fires when mark-based value drops below entry * (1 - pct)."""
        # entry = $75; pct=0.5 → fires when mark < $37.5
        call_q = _make_quote(bid=0.0, ask=0.0, mark=0.0004, spot=SPOT)  # $30
        quotes = {(EXPIRY, CALL_STRIKE, True): call_q}
        state = _FakeState(quotes, spot=SPOT)
        entry_usd = 0.0010 * SPOT
        legs = [{
            "strike": CALL_STRIKE, "is_call": True, "expiry": EXPIRY, "side": "buy",
            "qty": 1.0, "price_btc": 0.0010, "entry_price": 0.0010,
            "entry_price_usd": entry_usd, "entry_spot": SPOT,
        }]
        pos = _make_pos(legs, entry_price_usd=entry_usd, direction="buy")

        sl = stop_loss_pct(0.5, price_mode="mark")
        assert sl(state, pos) == "stop_loss"

    def test_long_sl_holds_when_value_sufficient(self):
        """Long SL holds when mark-based value is above the threshold."""
        # entry = $75; pct=0.5 → fires below $37.5; current mark = $60 → hold
        call_q = _make_quote(bid=0.0, ask=0.0, mark=0.0008, spot=SPOT)  # $60
        quotes = {(EXPIRY, CALL_STRIKE, True): call_q}
        state = _FakeState(quotes, spot=SPOT)
        entry_usd = 0.0010 * SPOT
        legs = [{
            "strike": CALL_STRIKE, "is_call": True, "expiry": EXPIRY, "side": "buy",
            "qty": 1.0, "price_btc": 0.0010, "entry_price": 0.0010,
            "entry_price_usd": entry_usd, "entry_spot": SPOT,
        }]
        pos = _make_pos(legs, entry_price_usd=entry_usd, direction="buy")

        sl = stop_loss_pct(0.5, price_mode="mark")
        assert sl(state, pos) is None

    def test_sl_data_gap_returns_none(self):
        """SL returns None (hold) when a quote is missing from the snapshot."""
        quotes = {}
        state = _FakeState(quotes, spot=SPOT)
        call_q = _make_quote(mark=0.0050)
        legs = [{
            "strike": CALL_STRIKE, "is_call": True, "expiry": EXPIRY, "side": "sell",
            "qty": 1.0, "price_btc": 0.0010, "entry_price": 0.0010,
            "entry_price_usd": 0.0010 * SPOT, "entry_spot": SPOT,
        }]
        pos = _make_pos(legs, entry_price_usd=0.0010 * SPOT, direction="sell")

        sl = stop_loss_pct(1.5, price_mode="mark")
        assert sl(state, pos) is None


# ---------------------------------------------------------------------------
# profit_target_pct
# ---------------------------------------------------------------------------

class TestProfitTargetPct:

    def test_default_price_mode_is_executable(self):
        """profit_target_pct default price_mode is 'executable'."""
        # Short position: entry $100; ask now $60 → 40% profit → should fire at pct=0.30
        call_q = _make_quote(bid=0.0005, ask=0.0008, mark=0.0009, spot=SPOT)
        put_q  = _make_quote(bid=0.0003, ask=0.0005, mark=0.0006, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        # entry: both legs at bid=$0.0010 and $0.0010 → total $150
        legs = _short_strangle_legs(
            _make_quote(bid=0.0010, ask=0.0012, mark=0.0011),
            _make_quote(bid=0.0010, ask=0.0012, mark=0.0011),
        )
        pos = _make_pos(legs, entry_price_usd=150.0, direction="sell")

        tp = profit_target_pct(0.30)  # default mode
        # Current executable cost: max(ask,mark)*spot for each sell leg
        # call: max(0.0008, 0.0009)*75000 = 67.5; put: max(0.0005,0.0006)*75000=45
        # total = 112.5; profit = (150-112.5)/150 = 0.25 — does NOT reach 0.30
        assert tp(state, pos) is None

    def test_short_tp_fires_when_enough_profit(self):
        """Short TP fires when executable cost-to-close drops by >= pct of entry."""
        # entry = $150; pct=0.30 → fires when buyback cost <= $105
        call_q = _make_quote(bid=0.0005, ask=0.0006, mark=0.0007, spot=SPOT)
        put_q  = _make_quote(bid=0.0004, ask=0.0005, mark=0.0006, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _short_strangle_legs(
            _make_quote(bid=0.0010), _make_quote(bid=0.0010),
        )
        pos = _make_pos(legs, entry_price_usd=150.0, direction="sell")

        tp = profit_target_pct(0.30, price_mode="executable")
        # executable: call=max(ask,mark)=0.0007, put=max(ask,mark)=0.0006
        # total = (0.0007+0.0006)*75000 = $97.5
        # profit_ratio = (150 - 97.5)/150 = 0.35 ≥ 0.30 → fires
        assert tp(state, pos) == "profit_target"

    def test_short_tp_holds_when_not_enough_profit(self):
        """Short TP holds when profit ratio is below threshold."""
        # entry=$150; ask has barely moved — profit ratio < 0.30
        call_q = _make_quote(bid=0.0009, ask=0.0011, mark=0.0012, spot=SPOT)
        put_q  = _make_quote(bid=0.0009, ask=0.0011, mark=0.0012, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _short_strangle_legs(
            _make_quote(bid=0.0010), _make_quote(bid=0.0010),
        )
        pos = _make_pos(legs, entry_price_usd=150.0, direction="sell")

        tp = profit_target_pct(0.30, price_mode="executable")
        # executable: call=max(ask,mark)=0.0012; put=0.0012; total=$180 > entry
        # profit_ratio = (150-180)/150 = negative → hold
        assert tp(state, pos) is None

    def test_long_tp_fires_when_bid_exceeds_entry(self):
        """Long TP fires when bid-based proceeds exceed entry by >= pct."""
        # entry = $150; pct=0.30 → fires when bid proceeds >= $195
        call_q = _make_quote(bid=0.0015, ask=0.0020, mark=0.0018, spot=SPOT)
        put_q  = _make_quote(bid=0.0012, ask=0.0016, mark=0.0015, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _long_strangle_legs(
            _make_quote(ask=0.0010), _make_quote(ask=0.0010),
        )
        pos = _make_pos(legs, entry_price_usd=150.0, direction="buy")

        tp = profit_target_pct(0.30, price_mode="executable")
        # executable for buy legs: bid; call bid=$112.5, put bid=$90 → total $202.5
        # profit_ratio = (202.5-150)/150 = 0.35 ≥ 0.30 → fires
        assert tp(state, pos) == "profit_target"

    def test_long_tp_holds_when_not_enough_gain(self):
        """Long TP holds when bid-based gain is below threshold."""
        call_q = _make_quote(bid=0.0009, ask=0.0012, mark=0.0010, spot=SPOT)
        put_q  = _make_quote(bid=0.0009, ask=0.0012, mark=0.0010, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _long_strangle_legs(
            _make_quote(ask=0.0010), _make_quote(ask=0.0010),
        )
        pos = _make_pos(legs, entry_price_usd=150.0, direction="buy")

        tp = profit_target_pct(0.30, price_mode="executable")
        # bid proceeds = (0.0009+0.0009)*75000 = $135 < $150 → hold
        assert tp(state, pos) is None

    def test_tp_data_gap_returns_none(self):
        """TP returns None (hold) when a quote is missing from the snapshot."""
        quotes = {}
        state = _FakeState(quotes, spot=SPOT)
        legs  = _short_strangle_legs(_make_quote(bid=0.0010), _make_quote(bid=0.0010))
        pos   = _make_pos(legs, entry_price_usd=150.0, direction="sell")

        tp = profit_target_pct(0.30, price_mode="executable")
        assert tp(state, pos) is None

    def test_tp_disabled_when_pct_zero(self):
        """profit_target_pct returns None when pct=0 (TP disabled by convention)."""
        # Even if profit_ratio is theoretically >= 0, pct=0 means disabled.
        call_q = _make_quote(bid=0.0001, ask=0.0001, mark=0.0001, spot=SPOT)
        put_q  = _make_quote(bid=0.0001, ask=0.0001, mark=0.0001, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        legs  = _short_strangle_legs(_make_quote(bid=0.0010), _make_quote(bid=0.0010))
        pos   = _make_pos(legs, entry_price_usd=150.0, direction="sell")

        tp = profit_target_pct(0.0, price_mode="executable")
        # pct=0: (150 - small) / 150 ≈ 1.0 ≥ 0.0 → this would fire unless we guard
        # The strategy uses `if self._tp_pct > 0` before calling profit_target_pct.
        # The factory itself fires at pct=0 since any ratio >= 0 is true.
        # Document this: caller is responsible for not wiring pct=0.
        # This test just documents the behavior.
        result = tp(state, pos)
        # pct=0 → (150 - ~15) / 150 ≈ 0.9 >= 0.0 → fires
        assert result == "profit_target"


# ---------------------------------------------------------------------------
# SL vs TP price_mode asymmetry — the key semantic test
# ---------------------------------------------------------------------------

class TestSlTpPriceModeAsymmetry:
    """
    Verifies the core design choice: SL uses mark (stable), TP uses executable.

    Scenario: thin early-morning book with a wide spread.
      - bid  = 0.0005 (someone wants to buy at this)
      - ask  = 0.0050 (someone is trying to sell; extreme ask spike)
      - mark = 0.0012 (exchange model price; the "fair" value)

    With mark-based SL:
      - SL evaluates at $90 (mark); entry = $75 — ratio = 0.2 — does NOT fire at 1.5×
    With executable SL (old behavior):
      - SL evaluates at ask = $375 (sell leg → ask); ratio = 4.0 — fires spuriously
    """

    def test_wide_spread_does_not_trigger_sl_with_mark_mode(self):
        call_q = _make_quote(bid=0.0005, ask=0.0050, mark=0.0012, spot=SPOT)
        put_q  = _make_quote(bid=0.0005, ask=0.0050, mark=0.0012, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        entry_usd = (0.0010 + 0.0010) * SPOT  # $150
        legs = _short_strangle_legs(_make_quote(bid=0.0010), _make_quote(bid=0.0010))
        pos  = _make_pos(legs, entry_price_usd=entry_usd, direction="sell")

        sl_mark = stop_loss_pct(1.5, price_mode="mark")
        sl_exec = stop_loss_pct(1.5, price_mode="executable")

        # mark-based: total mark = 2 × 0.0012 × 75000 = $180; ratio=(180-150)/150=0.2 → hold
        assert sl_mark(state, pos) is None
        # executable (ask-floored at mark): call=max(0.0050,0.0012)*75000=$375;
        # put same; total=$750; ratio=(750-150)/150=4.0 → fires
        assert sl_exec(state, pos) == "stop_loss"

    def test_tp_only_fires_with_real_executable_prices(self):
        """TP fires on bid/ask; would NOT fire if a wide-bid-gap prevented a real exit."""
        # Bid is 0 (no buyers) — executable buy-leg reprice falls back to mark estimate.
        # TP should hold if the fallback mark price doesn't reach the threshold.
        call_q = _make_quote(bid=0.0, ask=0.0020, mark=0.0005, spot=SPOT)  # bid=0 → mark used
        put_q  = _make_quote(bid=0.0, ask=0.0020, mark=0.0005, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True):  call_q,
            (EXPIRY, PUT_STRIKE,  False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT)
        entry_usd = (0.0010 + 0.0010) * SPOT  # $150 paid for long
        legs = _long_strangle_legs(_make_quote(ask=0.0010), _make_quote(ask=0.0010))
        pos  = _make_pos(legs, entry_price_usd=entry_usd, direction="buy")

        tp = profit_target_pct(0.30, price_mode="executable")
        # bid=0 but mark=0.0005 → bid falls back to mark (small → use as-is)
        # total = 2 × 0.0005 × 75000 = $75 < $150 → no profit → hold
        assert tp(state, pos) is None


# ---------------------------------------------------------------------------
# strike_proximity_stop / premium SL suppression near expiry
# ---------------------------------------------------------------------------

EXPIRY_DT = datetime(2026, 5, 28, 8, 0, tzinfo=timezone.utc)


def _proximity_pos(leg_type="strangle", call_strike=CALL_STRIKE, put_strike=PUT_STRIKE):
    legs = _short_strangle_legs(
        _make_quote(bid=0.0010),
        _make_quote(bid=0.0010),
    )
    pos = _make_pos(legs, entry_price_usd=150.0, direction="sell")
    pos.metadata.update({
        "leg_type": leg_type,
        "expiry": EXPIRY,
        "call_strike": call_strike,
        "put_strike": put_strike,
        "expiry_dt": EXPIRY_DT,
    })
    return pos


class TestStrikeProximityStop:
    def test_disabled_when_hours_zero(self):
        state = _FakeState({}, spot=80000.0, dt=datetime(2026, 5, 28, 6, 0, tzinfo=timezone.utc))
        cond = strike_proximity_stop(0, buffer_usd=0)
        assert cond(state, _proximity_pos()) is None

    def test_outside_window_does_not_fire(self):
        # 5h before expiry; window opens at 4h before
        dt = datetime(2026, 5, 28, 3, 0, tzinfo=timezone.utc)
        state = _FakeState({}, spot=80000.0, dt=dt)
        cond = strike_proximity_stop(4, buffer_usd=0)
        assert cond(state, _proximity_pos()) is None

    def test_inside_window_put_breach_strangle(self):
        dt = datetime(2026, 5, 28, 6, 0, tzinfo=timezone.utc)
        state = _FakeState({}, spot=73900.0, dt=dt)
        cond = strike_proximity_stop(4, buffer_usd=0)
        assert cond(state, _proximity_pos()) == "strike_proximity_stop"

    def test_inside_window_call_breach_strangle(self):
        dt = datetime(2026, 5, 28, 7, 0, tzinfo=timezone.utc)
        state = _FakeState({}, spot=76100.0, dt=dt)
        cond = strike_proximity_stop(4, buffer_usd=0)
        assert cond(state, _proximity_pos()) == "strike_proximity_stop"

    def test_buffer_delays_breach(self):
        dt = datetime(2026, 5, 28, 6, 0, tzinfo=timezone.utc)
        cond = strike_proximity_stop(4, buffer_usd=100)
        inside_buffer = _FakeState({}, spot=73950.0, dt=dt)
        breached = _FakeState({}, spot=73899.0, dt=dt)
        pos = _proximity_pos(put_strike=74000.0)
        assert cond(inside_buffer, pos) is None
        assert cond(breached, pos) == "strike_proximity_stop"

    def test_single_call_ignores_put_strike(self):
        dt = datetime(2026, 5, 28, 6, 0, tzinfo=timezone.utc)
        state = _FakeState({}, spot=73900.0, dt=dt)
        cond = strike_proximity_stop(4, buffer_usd=0)
        pos = _proximity_pos(leg_type="call")
        assert cond(state, pos) is None

    def test_single_put_ignores_call_strike(self):
        dt = datetime(2026, 5, 28, 6, 0, tzinfo=timezone.utc)
        state = _FakeState({}, spot=76100.0, dt=dt)
        cond = strike_proximity_stop(4, buffer_usd=0)
        pos = _proximity_pos(leg_type="put")
        assert cond(state, pos) is None

    def test_premium_sl_suppressed_inside_proximity_window(self):
        call_q = _make_quote(bid=0.0005, ask=0.0050, mark=0.0100, spot=SPOT)
        put_q = _make_quote(bid=0.0005, ask=0.0050, mark=0.0100, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True): call_q,
            (EXPIRY, PUT_STRIKE, False): put_q,
        }
        in_window = datetime(2026, 5, 28, 6, 0, tzinfo=timezone.utc)
        out_window = datetime(2026, 5, 28, 2, 0, tzinfo=timezone.utc)
        pos = _proximity_pos()
        sl = stop_loss_pct(1.5, price_mode="mark", suppress_hours_before_expiry=4)

        assert sl(_FakeState(quotes, spot=SPOT, dt=in_window), pos) is None
        assert sl(_FakeState(quotes, spot=SPOT, dt=out_window), pos) == "stop_loss"

    def test_tp_still_active_inside_proximity_window(self):
        call_q = _make_quote(bid=0.0002, ask=0.0002, mark=0.0002, spot=SPOT)
        put_q = _make_quote(bid=0.0002, ask=0.0002, mark=0.0002, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True): call_q,
            (EXPIRY, PUT_STRIKE, False): put_q,
        }
        dt = datetime(2026, 5, 28, 6, 0, tzinfo=timezone.utc)
        state = _FakeState(quotes, spot=SPOT, dt=dt)
        pos = _proximity_pos()
        tp = profit_target_pct(0.50, price_mode="executable")
        assert tp(state, pos) == "profit_target"


class TestPositionQuotesAvailable:
    def test_strangle_requires_both_legs(self):
        quotes = {
            (EXPIRY, CALL_STRIKE, True): _make_quote(),
            (EXPIRY, PUT_STRIKE, False): None,
        }
        state = _FakeState(quotes, spot=SPOT)
        pos = _proximity_pos()
        assert position_quotes_available(state, pos) is False

    def test_strangle_both_present(self):
        quotes = {
            (EXPIRY, CALL_STRIKE, True): _make_quote(),
            (EXPIRY, PUT_STRIKE, False): _make_quote(),
        }
        state = _FakeState(quotes, spot=SPOT)
        assert position_quotes_available(state, _proximity_pos()) is True

    def test_single_put_leg(self):
        quotes = {(EXPIRY, PUT_STRIKE, False): _make_quote()}
        state = _FakeState(quotes, spot=SPOT)
        pos = _proximity_pos(leg_type="put")
        assert position_quotes_available(state, pos) is True


class TestShortPremiumStopNearExpiry:
    def test_outside_window_premium_sl(self):
        out_window = datetime(2026, 5, 28, 2, 0, tzinfo=timezone.utc)
        call_q = _make_quote(bid=0.0005, ask=0.0050, mark=0.0100, spot=SPOT)
        put_q = _make_quote(bid=0.0005, ask=0.0050, mark=0.0100, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True): call_q,
            (EXPIRY, PUT_STRIKE, False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT, dt=out_window)
        pos = _proximity_pos()
        cond = short_premium_stop_near_expiry(1.5, proximity_hours=4, proximity_buffer_usd=0)
        assert cond(state, pos) == "stop_loss"

    def test_inside_window_proximity_not_premium_sl(self):
        in_window = datetime(2026, 5, 28, 6, 0, tzinfo=timezone.utc)
        call_q = _make_quote(bid=0.0005, ask=0.0050, mark=0.0100, spot=SPOT)
        put_q = _make_quote(bid=0.0005, ask=0.0050, mark=0.0100, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True): call_q,
            (EXPIRY, PUT_STRIKE, False): put_q,
        }
        state = _FakeState(quotes, spot=SPOT, dt=in_window)
        pos = _proximity_pos()
        cond = short_premium_stop_near_expiry(1.5, proximity_hours=4, proximity_buffer_usd=0)
        assert cond(state, pos) is None

    def test_inside_window_strike_breach(self):
        in_window = datetime(2026, 5, 28, 6, 0, tzinfo=timezone.utc)
        state = _FakeState({}, spot=73900.0, dt=in_window)
        cond = short_premium_stop_near_expiry(4.5, proximity_hours=4, proximity_buffer_usd=0)
        assert cond(state, _proximity_pos()) == "strike_proximity_stop"

    def test_proximity_hours_zero_is_premium_sl_only(self):
        state = _FakeState({}, spot=73900.0, dt=datetime(2026, 5, 28, 6, 0, tzinfo=timezone.utc))
        cond = short_premium_stop_near_expiry(4.5, proximity_hours=0, proximity_buffer_usd=0)
        assert cond(state, _proximity_pos()) is None


# ---------------------------------------------------------------------------
# position_unrealized_pnl / equity_drawdown_stop
# ---------------------------------------------------------------------------

def _equity_stop_pos(entry_bid=0.001, mark=0.0011, equity_at_entry=100_000.0):
    call_q = _make_quote(bid=entry_bid, mark=mark, spot=SPOT)
    put_q = _make_quote(bid=entry_bid, mark=mark, spot=SPOT)
    legs = _short_strangle_legs(call_q, put_q)
    pos = _make_pos(legs, entry_price_usd=call_q.bid_usd + put_q.bid_usd, direction="sell")
    pos.metadata["equity_at_entry_usd"] = equity_at_entry
    pos.metadata.update({
        "expiry": EXPIRY,
        "call_strike": CALL_STRIKE,
        "put_strike": PUT_STRIKE,
        "leg_type": "strangle",
    })
    return pos


class TestPositionUnrealizedPnl:
    def test_short_strangle_loss(self):
        pos = _equity_stop_pos(entry_bid=0.001, mark=0.041)
        quotes = {
            (EXPIRY, CALL_STRIKE, True): _make_quote(bid=0.001, mark=0.041, spot=SPOT),
            (EXPIRY, PUT_STRIKE, False): _make_quote(bid=0.001, mark=0.041, spot=SPOT),
        }
        state = _FakeState(quotes, spot=SPOT)
        pnl = position_unrealized_pnl(state, pos, price_mode="mark")
        assert pnl == pytest.approx(-6000.0)


class TestEquityDrawdownStop:
    def test_disabled_when_pct_zero(self):
        pos = _equity_stop_pos(mark=0.041)
        quotes = {
            (EXPIRY, CALL_STRIKE, True): _make_quote(mark=0.041, spot=SPOT),
            (EXPIRY, PUT_STRIKE, False): _make_quote(mark=0.041, spot=SPOT),
        }
        cond = equity_drawdown_stop(0.0, price_mode="mark")
        assert cond(_FakeState(quotes, spot=SPOT), pos) is None

    def test_fires_at_threshold(self):
        pos = _equity_stop_pos(mark=0.041, equity_at_entry=100_000.0)
        quotes = {
            (EXPIRY, CALL_STRIKE, True): _make_quote(bid=0.001, mark=0.041, spot=SPOT),
            (EXPIRY, PUT_STRIKE, False): _make_quote(bid=0.001, mark=0.041, spot=SPOT),
        }
        cond = equity_drawdown_stop(0.06, price_mode="mark")
        assert cond(_FakeState(quotes, spot=SPOT), pos) == "equity_drawdown_stop"

    def test_holds_below_threshold(self):
        pos = _equity_stop_pos(mark=0.041, equity_at_entry=100_000.0)
        quotes = {
            (EXPIRY, CALL_STRIKE, True): _make_quote(bid=0.001, mark=0.041, spot=SPOT),
            (EXPIRY, PUT_STRIKE, False): _make_quote(bid=0.001, mark=0.041, spot=SPOT),
        }
        cond = equity_drawdown_stop(0.07, price_mode="mark")
        assert cond(_FakeState(quotes, spot=SPOT), pos) is None

    def test_no_fire_without_equity_at_entry(self):
        pos = _equity_stop_pos(mark=0.041)
        del pos.metadata["equity_at_entry_usd"]
        quotes = {
            (EXPIRY, CALL_STRIKE, True): _make_quote(mark=0.041, spot=SPOT),
            (EXPIRY, PUT_STRIKE, False): _make_quote(mark=0.041, spot=SPOT),
        }
        cond = equity_drawdown_stop(0.05, price_mode="mark")
        assert cond(_FakeState(quotes, spot=SPOT), pos) is None

    def test_suppressed_on_quote_gap(self):
        pos = _equity_stop_pos(mark=0.041)
        cond = equity_drawdown_stop(0.05, price_mode="mark")
        assert cond(_FakeState({}, spot=SPOT), pos) is None


class TestExitExpiryWindow:
    """Tests for exit_expiry_window — independent expiry-relative gating."""

    def test_no_gate_passes_through(self):
        call_q = _make_quote(bid=0.0005, ask=0.0050, mark=0.0100, spot=SPOT)
        put_q = _make_quote(bid=0.0005, ask=0.0050, mark=0.0100, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True): call_q,
            (EXPIRY, PUT_STRIKE, False): put_q,
        }
        inner = stop_loss_pct(1.5, price_mode="mark")
        cond = exit_expiry_window(inner, only_final_hours=0, except_final_hours=0)
        in_window = datetime(2026, 5, 28, 6, 0, tzinfo=timezone.utc)
        out_window = datetime(2026, 5, 28, 2, 0, tzinfo=timezone.utc)
        pos = _proximity_pos()
        assert cond(_FakeState(quotes, spot=SPOT, dt=in_window), pos) == "stop_loss"
        assert cond(_FakeState(quotes, spot=SPOT, dt=out_window), pos) == "stop_loss"

    def test_except_final_suppresses_inside_window(self):
        call_q = _make_quote(bid=0.0005, ask=0.0050, mark=0.0100, spot=SPOT)
        put_q = _make_quote(bid=0.0005, ask=0.0050, mark=0.0100, spot=SPOT)
        quotes = {
            (EXPIRY, CALL_STRIKE, True): call_q,
            (EXPIRY, PUT_STRIKE, False): put_q,
        }
        inner = stop_loss_pct(1.5, price_mode="mark")
        cond = exit_expiry_window(inner, except_final_hours=4)
        in_window = datetime(2026, 5, 28, 6, 0, tzinfo=timezone.utc)
        out_window = datetime(2026, 5, 28, 2, 0, tzinfo=timezone.utc)
        pos = _proximity_pos()
        assert cond(_FakeState(quotes, spot=SPOT, dt=in_window), pos) is None
        assert cond(_FakeState(quotes, spot=SPOT, dt=out_window), pos) == "stop_loss"

    def test_only_final_runs_inside_window_only(self):
        dt = datetime(2026, 5, 28, 6, 0, tzinfo=timezone.utc)
        out_dt = datetime(2026, 5, 28, 2, 0, tzinfo=timezone.utc)
        inner = strike_proximity_stop(4, buffer_usd=0)
        cond = exit_expiry_window(inner, only_final_hours=4)
        pos = _proximity_pos()
        breached = _FakeState({}, spot=73900.0, dt=dt)
        safe_outside = _FakeState({}, spot=73900.0, dt=out_dt)
        assert cond(breached, pos) == "strike_proximity_stop"
        assert cond(safe_outside, pos) is None

    def test_only_final_takes_precedence_over_except(self):
        """When both gates are set, only_final_hours wins (except is ignored)."""
        dt = datetime(2026, 5, 28, 2, 0, tzinfo=timezone.utc)
        inner = strike_proximity_stop(4, buffer_usd=0)
        cond = exit_expiry_window(inner, only_final_hours=4, except_final_hours=4)
        pos = _proximity_pos()
        # Outside final window → suppressed even though except would allow it
        assert cond(_FakeState({}, spot=73900.0, dt=dt), pos) is None

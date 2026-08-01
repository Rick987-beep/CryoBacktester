"""
Unit tests for backtester/strategies/short_generic.py

Tests cover:
    - TP fires when combined ask drops enough
    - TP does NOT fire when ask is still too high
    - SL still fires independently
    - Expiry settlement
    - No duplicate entry on same calendar day
    - Entry blocked outside time window
    - Missing ask skips TP tick (no phantom trade)
    - take_profit metadata recorded on trade

Run:
    .venv/bin/python -m pytest backtester/strategies/tests/test_short_generic.py -v
"""
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from backtester.strategies.short_generic import ShortGeneric

# ── Helpers ──────────────────────────────────────────────────────────────────

SPOT = 85_000.0
EXPIRY = "2APR26"           # one day ahead of entry date 1-APR-26
CALL_STRIKE = 88_000.0
PUT_STRIKE  = 82_000.0

# Token entry premium: 400 USD total (200 call + 200 put)
# At spot=85000, 200 USD = 200/85000 BTC ~= 0.002353 BTC
ENTRY_BID_BTC = 200.0 / SPOT   # per leg, in BTC


def _make_quote(strike, is_call, bid, ask, delta):
    """Build a mock OptionQuote-like SimpleNamespace."""
    obj = SimpleNamespace(
        strike=strike,
        is_call=is_call,
        expiry=EXPIRY,
        bid=bid,
        ask=ask,
        mark=bid,           # not used in TP path
        delta=delta,
        spot=SPOT,
    )
    obj.bid_usd = bid * SPOT
    obj.ask_usd = ask * SPOT
    obj.mark_usd = bid * SPOT
    return obj


def _make_state(hour, minute=0, spot=SPOT, call_ask=None, put_ask=None,
                call_bid=None, put_bid=None, dt_date=None, has_chain=True):
    """Build a minimal mock MarketState."""
    if dt_date is None:
        dt_date = datetime(2026, 4, 1, tzinfo=timezone.utc)
    dt = dt_date.replace(hour=hour, minute=minute)

    if call_bid is None:
        call_bid = ENTRY_BID_BTC
    if put_bid is None:
        put_bid = ENTRY_BID_BTC
    if call_ask is None:
        call_ask = ENTRY_BID_BTC
    if put_ask is None:
        put_ask = ENTRY_BID_BTC

    call_q = _make_quote(CALL_STRIKE, True, call_bid, call_ask, delta=0.25)
    put_q  = _make_quote(PUT_STRIKE, False, put_bid, put_ask, delta=-0.25)

    state = MagicMock()
    state.dt = dt
    state.spot = spot
    state.spot_bars = []

    def get_option(expiry, strike, is_call):
        if not has_chain:
            return None
        if strike == CALL_STRIKE and is_call:
            return call_q
        if strike == PUT_STRIKE and not is_call:
            return put_q
        return None

    state.get_option.side_effect = get_option

    # get_chain returns both call and put for selection
    call_chain = _make_quote(CALL_STRIKE, True, call_bid, call_ask, delta=0.25)
    put_chain  = _make_quote(PUT_STRIKE, False, put_bid, put_ask, delta=-0.25)

    def get_chain(expiry):
        if not has_chain:
            return []
        return [call_chain, put_chain]

    state.get_chain.side_effect = get_chain

    def expiries():
        # Return EXPIRY when 1 DTE ahead of dt.date()
        return [EXPIRY]

    state.expiries.side_effect = expiries

    return state


def _entry_state(**kwargs):
    """State at the entry hour (12 UTC = within entry window 12–13)."""
    return _make_state(hour=12, **kwargs)


def _make_strategy(sl_pct=2.0, tp_pct=0.60):
    s = ShortGeneric()
    s.configure({
        "dte":             1,
        "delta":           0.25,
        "stop_loss_pct":   sl_pct,
        "take_profit_pct": tp_pct,
        "entry_hour":      12,
        "max_hold_hours":  0,
    })
    return s


# ── Tests: entry ─────────────────────────────────────────────────────────────

class TestEntry:
    def test_entry_opens_position(self):
        s = _make_strategy()
        state = _entry_state()
        trades = s.on_market_state(state)
        assert trades == []
        assert len(s._positions) == 1

    def test_no_duplicate_entry_same_day(self):
        s = _make_strategy()
        state1 = _entry_state()
        s.on_market_state(state1)
        state2 = _make_state(hour=13)  # same day, different hour
        s.on_market_state(state2)
        assert len(s._positions) == 1

    def test_entry_blocked_outside_window(self):
        s = _make_strategy()
        state = _make_state(hour=11)   # window is 12–13
        trades = s.on_market_state(state)
        assert len(s._positions) == 0
        assert trades == []

    def test_entry_premium_recorded(self):
        s = _make_strategy()
        s.on_market_state(_entry_state())
        pos = s._positions[0]
        expected = (ENTRY_BID_BTC + ENTRY_BID_BTC) * SPOT
        assert abs(pos.entry_price_usd - expected) < 0.01


# ── Tests: take-profit ────────────────────────────────────────────────────────

class TestTakeProfit:
    def _open_then_reprice(self, tp_pct, ask_fraction):
        """Open at entry, then present asks at ask_fraction × entry bid."""
        s = _make_strategy(tp_pct=tp_pct)
        s.on_market_state(_entry_state())
        # Next tick: ask has dropped to ask_fraction of entry bid
        new_ask = ENTRY_BID_BTC * ask_fraction
        next_state = _make_state(hour=14, call_ask=new_ask, put_ask=new_ask)
        trades = s.on_market_state(next_state)
        return trades, s

    def test_tp_fires_when_ask_drops_enough(self):
        # TP=60%: entry=400 USD, trigger when ask_total ≤ 400*(1-0.60)=160 USD
        # ask_fraction=0.39 → ask_total = 0.39*400 = 156 USD  → should fire
        trades, s = self._open_then_reprice(tp_pct=0.60, ask_fraction=0.39)
        assert len(trades) == 1
        assert trades[0].exit_reason == "take_profit"
        assert len(s._positions) == 0

    def test_tp_does_not_fire_when_ask_still_high(self):
        # ask_fraction=0.50 → 50% profit, but tp_pct=0.60 → no fire yet
        trades, s = self._open_then_reprice(tp_pct=0.60, ask_fraction=0.50)
        assert trades == []
        assert len(s._positions) == 1

    def test_tp_fires_exactly_at_threshold(self):
        # ask_fraction=0.40 → profit_ratio=0.60 exactly → fires
        trades, s = self._open_then_reprice(tp_pct=0.60, ask_fraction=0.40)
        assert len(trades) == 1
        assert trades[0].exit_reason == "take_profit"

    def test_tp_metadata_recorded(self):
        trades, _ = self._open_then_reprice(tp_pct=0.60, ask_fraction=0.39)
        assert trades[0].metadata["take_profit_pct"] == 0.60

    def test_tp_skips_tick_if_ask_missing(self):
        """If ask=0 on either leg, no TP fires (skip that tick)."""
        s = _make_strategy(tp_pct=0.60)
        s.on_market_state(_entry_state())
        # ask=0 → missing data
        next_state = _make_state(hour=14, call_ask=0.0, put_ask=0.0)
        trades = s.on_market_state(next_state)
        assert trades == []
        assert len(s._positions) == 1


# ── Tests: stop-loss ─────────────────────────────────────────────────────────

class TestStopLoss:
    def test_sl_fires_when_ask_spikes(self):
        # SL=1.0 (100%): fires when ask_total > 2× entry, i.e. loss > 100%
        # ask_fraction=2.1 → ask_total=2.1× entry → loss_ratio=1.1 > 1.0
        s = _make_strategy(sl_pct=1.0, tp_pct=0.99)  # TP won't trigger
        s.on_market_state(_entry_state())
        new_ask = ENTRY_BID_BTC * 2.1
        next_state = _make_state(hour=14, call_ask=new_ask, put_ask=new_ask)
        trades = s.on_market_state(next_state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "stop_loss"

    def test_sl_does_not_fire_below_threshold(self):
        s = _make_strategy(sl_pct=1.0, tp_pct=0.99)
        s.on_market_state(_entry_state())
        new_ask = ENTRY_BID_BTC * 1.5   # loss=50%, sl=100% → no fire
        next_state = _make_state(hour=14, call_ask=new_ask, put_ask=new_ask)
        trades = s.on_market_state(next_state)
        assert trades == []


# ── Tests: expiry settlement ─────────────────────────────────────────────────

class TestExpiry:
    def test_expiry_closes_at_expiry_dt(self):
        s = _make_strategy()
        s.on_market_state(_entry_state())
        pos = s._positions[0]

        # Expiry datetime stored in metadata
        exp_dt = pos.metadata["expiry_dt"]
        assert exp_dt is not None

        # Present a state at the expiry time; spot between wings → full profit
        exp_state = MagicMock()
        exp_state.dt = exp_dt
        exp_state.spot = SPOT  # between strikes → both legs expire worthless
        exp_state.spot_bars = []
        exp_state.get_option.return_value = None  # expiry path bypasses get_option

        trades = s.on_market_state(exp_state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "expiry"

    def test_expiry_no_close_fees(self):
        s = _make_strategy()
        s.on_market_state(_entry_state())
        pos = s._positions[0]
        exp_dt = pos.metadata["expiry_dt"]

        exp_state = MagicMock()
        exp_state.dt = exp_dt
        exp_state.spot = SPOT
        exp_state.spot_bars = []
        exp_state.get_option.return_value = None

        trades = s.on_market_state(exp_state)
        # Fees at close should be zero for expiry path
        entry_fees = pos.fees_open
        assert trades[0].fees == entry_fees  # only entry fees


# ── Tests: on_end ────────────────────────────────────────────────────────────

class TestOnEnd:
    def test_on_end_closes_open_position(self):
        s = _make_strategy()
        s.on_market_state(_entry_state())
        assert len(s._positions) == 1
        end_state = _make_state(hour=22)
        trades = s.on_end(end_state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "end_of_data"
        assert len(s._positions) == 0


# ── Tests: reset ─────────────────────────────────────────────────────────────

class TestReset:
    def test_reset_clears_state(self):
        s = _make_strategy()
        s.on_market_state(_entry_state())
        s.reset()
        assert s._positions == []
        assert s._last_trade_date is None


# ── Tests: describe_params ───────────────────────────────────────────────────

class TestDescribeParams:
    def test_includes_tp_key(self):
        s = _make_strategy(tp_pct=0.50)
        p = s.describe_params()
        assert "take_profit_pct" in p
        assert p["take_profit_pct"] == 0.50
        assert "stop_loss_pct" in p

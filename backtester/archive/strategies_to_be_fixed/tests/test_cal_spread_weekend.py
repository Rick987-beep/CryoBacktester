"""
Unit tests for backtester/strategies/cal_spread_weekend.py

Tests cover:
    - Entry fires on configured entry_day + entry_hour
    - No entry on wrong weekday or wrong hour
    - No entry when MAX_POSITIONS already open
    - Position has correct leg structure (short near, long far)
    - Option type: put when strike < spot, call when strike > spot
    - Take profit fires when net_pnl >= tp_pct × short_cost
    - Stop loss fires when net_pnl <= -(sl_pct × short_cost)
    - Expiry of short leg closes both legs (short at 0, long at bid)
    - on_end() closes all open positions
    - Multiple concurrent positions managed independently

Run:
    python -m pytest backtester/strategies/tests/test_cal_spread_weekend.py -v
"""
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from backtester.pricing import deribit_fee_per_leg
from backtester.strategies.cal_spread_weekend import CalSpreadWeekend, MAX_POSITIONS

# ── Constants ─────────────────────────────────────────────────────────────────

SPOT = 90_000.0

NEAR_EXPIRY = "29MAY26"   # ~8 DTE from entry 2026-05-21 (Thursday — adjust per test)
FAR_EXPIRY  = "25JUN26"   # ~35 DTE

# Strike just below spot → puts
PUT_STRIKE  = 89_000.0
# Strike just above spot → calls
CALL_STRIKE = 91_000.0

# BTC prices
SHORT_BID_BTC = 0.0020   # received for near leg
SHORT_ASK_BTC = 0.0025
SHORT_MARK_BTC = 0.0022
LONG_ASK_BTC  = 0.0050   # paid for far leg
LONG_BID_BTC  = 0.0045
LONG_MARK_BTC = 0.0047


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_quote(strike, is_call, bid, ask, mark, delta, expiry, spot=SPOT):
    obj = SimpleNamespace(
        strike=strike,
        is_call=is_call,
        expiry=expiry,
        bid=bid,
        ask=ask,
        mark=mark,
        delta=delta,
        spot=spot,
    )
    obj.bid_usd  = bid  * spot
    obj.ask_usd  = ask  * spot
    obj.mark_usd = mark * spot
    return obj


def _make_state(
    dt,
    spot=SPOT,
    atm_strike=PUT_STRIKE,   # nearest strike returned by get_atm_strike
    near_expiry=NEAR_EXPIRY,
    far_expiry=FAR_EXPIRY,
    near_dte=8,
    far_dte=35,
    short_bid=SHORT_BID_BTC,
    short_ask=SHORT_ASK_BTC,
    short_mark=SHORT_MARK_BTC,
    long_ask=LONG_ASK_BTC,
    long_bid=LONG_BID_BTC,
    long_mark=LONG_MARK_BTC,
):
    """Build a minimal mock MarketState for cal_spread_weekend tests."""
    state = MagicMock()
    state.dt   = dt
    state.spot = spot
    state.spot_bars = []

    is_call = atm_strike > spot  # mirrors strategy logic

    near_q = _make_quote(atm_strike, is_call, short_bid, short_ask, short_mark, 0.20, near_expiry, spot)
    far_q  = _make_quote(atm_strike, is_call, long_bid,  long_ask,  long_mark,  0.30, far_expiry,  spot)

    def expiries():
        from backtester.expiry_utils import parse_expiry_date
        today = dt.date()
        result = []
        for exp, target in [(near_expiry, near_dte), (far_expiry, far_dte)]:
            exp_date = parse_expiry_date(exp)
            if exp_date:
                dte = (exp_date.date() - today).days
                if dte > 0:
                    result.append(exp)
        return result

    def get_atm_strike(expiry):
        if expiry in (near_expiry, far_expiry):
            return atm_strike
        return None

    def get_option(expiry, strike, _is_call):
        if strike != atm_strike or _is_call != is_call:
            return None
        if expiry == near_expiry:
            return near_q
        if expiry == far_expiry:
            return far_q
        return None

    state.expiries.side_effect       = expiries
    state.get_atm_strike.side_effect = get_atm_strike
    state.get_option.side_effect     = get_option

    return state


def _saturday(hour=12, minute=0):
    """Return a Saturday datetime at the given UTC hour. 2026-05-23 is a Saturday."""
    return datetime(2026, 5, 23, hour, minute, 0, tzinfo=timezone.utc)


def _sunday(hour=12, minute=0):
    """Return a Sunday datetime. 2026-05-24 is a Sunday."""
    return datetime(2026, 5, 24, hour, minute, 0, tzinfo=timezone.utc)


def _make_strategy(**overrides):
    s = CalSpreadWeekend()
    params = {
        "near_dte":   8,
        "far_dte":    35,
        "entry_hour": 12,
        "entry_day":  5,   # Saturday
        "tp_pct":     0.50,
        "sl_pct":     1.00,
        "qty":        1.0,
    }
    params.update(overrides)
    s.configure(params)
    return s


# ── Tests: entry ──────────────────────────────────────────────────────────────

class TestEntry:

    def test_entry_on_saturday_at_entry_hour(self):
        s = _make_strategy()
        state = _make_state(_saturday())
        trades = s.on_market_state(state)
        assert len(s._positions) == 1
        assert len(trades) == 1
        assert trades[0].side == "open"
        assert trades[0].pnl == 0.0

    def test_no_entry_on_friday(self):
        s = _make_strategy()
        # 2026-05-22 is a Friday
        dt = datetime(2026, 5, 22, 12, 0, tzinfo=timezone.utc)
        state = _make_state(dt)
        s.on_market_state(state)
        assert len(s._positions) == 0

    def test_no_entry_wrong_hour(self):
        s = _make_strategy()
        state = _make_state(_saturday(hour=10))
        s.on_market_state(state)
        assert len(s._positions) == 0

    def test_no_entry_wrong_minute(self):
        s = _make_strategy()
        state = _make_state(_saturday(minute=5))
        s.on_market_state(state)
        assert len(s._positions) == 0

    def test_entry_on_sunday_when_configured(self):
        s = _make_strategy(entry_day=6)
        state = _make_state(_sunday())
        s.on_market_state(state)
        assert len(s._positions) == 1

    def test_max_positions_not_exceeded(self):
        s = _make_strategy()
        # Fill to MAX_POSITIONS
        for _ in range(MAX_POSITIONS):
            state = _make_state(_saturday())
            s.on_market_state(state)
        assert len(s._positions) == MAX_POSITIONS
        # One more tick — should NOT open a 5th position
        state = _make_state(_saturday())
        trades = s.on_market_state(state)
        open_trades = [t for t in trades if t.side == "open"]
        assert len(s._positions) == MAX_POSITIONS
        assert len(open_trades) == 0

    def test_no_entry_when_near_bid_zero(self):
        s = _make_strategy()
        state = _make_state(_saturday(), short_bid=0.0)
        s.on_market_state(state)
        assert len(s._positions) == 0

    def test_no_entry_when_far_ask_zero(self):
        s = _make_strategy()
        state = _make_state(_saturday(), long_ask=0.0)
        s.on_market_state(state)
        assert len(s._positions) == 0


# ── Tests: leg structure ──────────────────────────────────────────────────────

class TestLegStructure:

    def test_put_calendar_when_strike_below_spot(self):
        s = _make_strategy()
        # atm_strike=89000 < spot=90000 → puts
        state = _make_state(_saturday(), atm_strike=89_000.0, spot=90_000.0)
        s.on_market_state(state)
        pos = s._positions[0]
        assert pos.legs[0]["is_call"] is False   # short leg
        assert pos.legs[1]["is_call"] is False   # long leg

    def test_call_calendar_when_strike_above_spot(self):
        s = _make_strategy()
        # atm_strike=91000 > spot=90000 → calls
        state = _make_state(_saturday(), atm_strike=91_000.0, spot=90_000.0)
        s.on_market_state(state)
        pos = s._positions[0]
        assert pos.legs[0]["is_call"] is True
        assert pos.legs[1]["is_call"] is True

    def test_short_leg_is_near_expiry(self):
        s = _make_strategy()
        state = _make_state(_saturday())
        s.on_market_state(state)
        pos = s._positions[0]
        assert pos.legs[0]["side"] == "sell"
        assert pos.legs[0]["expiry"] == NEAR_EXPIRY

    def test_long_leg_is_far_expiry(self):
        s = _make_strategy()
        state = _make_state(_saturday())
        s.on_market_state(state)
        pos = s._positions[0]
        assert pos.legs[1]["side"] == "buy"
        assert pos.legs[1]["expiry"] == FAR_EXPIRY

    def test_short_cost_stored_in_metadata(self):
        s = _make_strategy()
        state = _make_state(_saturday())
        s.on_market_state(state)
        pos = s._positions[0]
        expected = SHORT_BID_BTC * SPOT * 1.0
        assert abs(pos.metadata["short_cost"] - expected) < 0.01

    def test_long_cost_stored_in_metadata(self):
        s = _make_strategy()
        state = _make_state(_saturday())
        s.on_market_state(state)
        pos = s._positions[0]
        expected = LONG_ASK_BTC * SPOT * 1.0
        assert abs(pos.metadata["long_cost"] - expected) < 0.01

    def test_expiry_dt_is_near_leg_expiry(self):
        s = _make_strategy()
        state = _make_state(_saturday())
        s.on_market_state(state)
        pos = s._positions[0]
        assert pos.metadata["expiry_dt"] is not None
        from backtester.expiry_utils import parse_expiry_date
        from backtester.pricing import EXPIRY_HOUR_UTC
        near_date = parse_expiry_date(NEAR_EXPIRY)
        assert pos.metadata["expiry_dt"].date() == near_date.date()
        assert pos.metadata["expiry_dt"].hour == EXPIRY_HOUR_UTC

    def test_pos_id_increments(self):
        s = _make_strategy()
        state1 = _make_state(_saturday())
        s.on_market_state(state1)
        assert s._positions[0].metadata["pos_id"] == 1
        # Second entry (different time doesn't matter — just need capacity)
        state2 = _make_state(_saturday())
        s.on_market_state(state2)
        assert s._positions[1].metadata["pos_id"] == 2


# ── Tests: take profit ────────────────────────────────────────────────────────

class TestTakeProfit:

    def _open_position(self, s):
        state = _make_state(_saturday())
        s.on_market_state(state)
        return s._positions[0]

    def test_tp_fires_when_net_pnl_exceeds_threshold(self):
        s = _make_strategy(tp_pct=0.50)
        state_open = _make_state(_saturday())
        s.on_market_state(state_open)
        pos = s._positions[0]
        short_cost = pos.metadata["short_cost"]

        # Engineer quotes so net_pnl = short_cost (100% of short_cost > 50%)
        # net_pnl = (short_cost - short_buyback) + (long_proceeds - long_cost)
        # Set short_buyback = 0, long_proceeds = long_cost → net_pnl = short_cost
        state_exit = _make_state(
            _saturday(hour=14),
            short_ask=0.0,
            short_mark=0.0,    # short worthless
            long_bid=LONG_ASK_BTC,  # long recovered full entry cost
        )
        trades = s.on_market_state(state_exit)
        close_trades = [t for t in trades if t.side == "close"]
        assert len(close_trades) == 1
        assert close_trades[0].exit_reason == "take_profit"
        assert len(s._positions) == 0

    def test_tp_does_not_fire_below_threshold(self):
        s = _make_strategy(tp_pct=0.50)
        state_open = _make_state(_saturday())
        s.on_market_state(state_open)
        pos = s._positions[0]
        short_cost = pos.metadata["short_cost"]

        # net_pnl ≈ 0.1 × short_cost (below 50% threshold)
        # short_buyback = short_cost * 0.9 → saved 10%
        tiny_short_ask = SHORT_BID_BTC * 0.9  # ≈ 90% of what we received
        state_hold = _make_state(
            _saturday(hour=14),
            short_ask=tiny_short_ask,
            short_mark=tiny_short_ask,
            long_bid=LONG_BID_BTC,
        )
        trades = s.on_market_state(state_hold)
        close_trades = [t for t in trades if t.side == "close"]
        assert len(close_trades) == 0
        assert len(s._positions) == 1


# ── Tests: stop loss ──────────────────────────────────────────────────────────

class TestStopLoss:

    def test_sl_fires_when_net_loss_exceeds_threshold(self):
        s = _make_strategy(sl_pct=1.00)
        state_open = _make_state(_saturday())
        s.on_market_state(state_open)
        pos = s._positions[0]
        short_cost = pos.metadata["short_cost"]

        # net_pnl = -1.5 × short_cost (below -100% threshold)
        # Make short expensive to buy back and long worth nothing
        # short_buyback = short_cost * 2.5  → short_pnl = -1.5 × short_cost
        # long_proceeds = long_cost         → long_pnl = 0
        inflated_short_ask = SHORT_BID_BTC * 2.5
        state_exit = _make_state(
            _saturday(hour=14),
            short_ask=inflated_short_ask,
            short_mark=inflated_short_ask,
            long_bid=LONG_ASK_BTC,
        )
        trades = s.on_market_state(state_exit)
        close_trades = [t for t in trades if t.side == "close"]
        assert len(close_trades) == 1
        assert close_trades[0].exit_reason == "stop_loss"
        assert len(s._positions) == 0

    def test_sl_does_not_fire_below_threshold(self):
        s = _make_strategy(sl_pct=1.00)
        state_open = _make_state(_saturday())
        s.on_market_state(state_open)

        # net_pnl = -0.2 × short_cost (above -100% threshold — should not fire)
        slightly_up_ask = SHORT_BID_BTC * 1.2
        state_hold = _make_state(
            _saturday(hour=14),
            short_ask=slightly_up_ask,
            short_mark=slightly_up_ask,
            long_bid=LONG_BID_BTC,
        )
        trades = s.on_market_state(state_hold)
        close_trades = [t for t in trades if t.side == "close"]
        assert len(close_trades) == 0


# ── Tests: expiry ─────────────────────────────────────────────────────────────

class TestExpiry:

    def test_short_leg_expiry_closes_both_legs(self):
        s = _make_strategy()
        state_open = _make_state(_saturday())
        s.on_market_state(state_open)
        assert len(s._positions) == 1

        # Tick at or after EXPIRY_HOUR_UTC on near expiry date
        from backtester.expiry_utils import parse_expiry_date
        from backtester.pricing import EXPIRY_HOUR_UTC
        near_date = parse_expiry_date(NEAR_EXPIRY)
        expiry_dt = datetime(
            near_date.year, near_date.month, near_date.day,
            EXPIRY_HOUR_UTC, 0, tzinfo=timezone.utc,
        )
        state_expiry = _make_state(expiry_dt, near_dte=0)
        trades = s.on_market_state(state_expiry)

        close_trades = [t for t in trades if t.side == "close"]
        assert len(close_trades) == 1
        assert close_trades[0].exit_reason == "expiry"
        assert len(s._positions) == 0

    def test_short_leg_exit_price_zero_at_expiry(self):
        s = _make_strategy()
        state_open = _make_state(_saturday())
        s.on_market_state(state_open)

        from backtester.expiry_utils import parse_expiry_date
        from backtester.pricing import EXPIRY_HOUR_UTC
        near_date = parse_expiry_date(NEAR_EXPIRY)
        expiry_dt = datetime(
            near_date.year, near_date.month, near_date.day,
            EXPIRY_HOUR_UTC, 0, tzinfo=timezone.utc,
        )
        state_expiry = _make_state(expiry_dt, near_dte=0)
        pos = s._positions[0]
        s.on_market_state(state_expiry)

        short_leg = pos.legs[0]
        assert short_leg["exit_price_btc"] == 0.0


# ── Tests: on_end ─────────────────────────────────────────────────────────────

class TestOnEnd:

    def test_on_end_closes_all_positions(self):
        s = _make_strategy()
        # Open 2 positions
        for _ in range(2):
            state = _make_state(_saturday())
            s.on_market_state(state)
        assert len(s._positions) == 2

        state_end = _make_state(datetime(2026, 6, 1, 8, 0, tzinfo=timezone.utc))
        trades = s.on_end(state_end)
        assert len(trades) == 2
        assert all(t.exit_reason == "end_of_data" for t in trades)
        assert len(s._positions) == 0

    def test_on_end_empty_when_no_positions(self):
        s = _make_strategy()
        state = _make_state(datetime(2026, 6, 1, 8, 0, tzinfo=timezone.utc))
        trades = s.on_end(state)
        assert trades == []


# ── Tests: concurrent positions managed independently ─────────────────────────

class TestConcurrentPositions:

    def test_two_positions_exit_independently(self):
        s = _make_strategy(tp_pct=0.50, sl_pct=1.00)

        # Open position 1 on first tick
        state_open = _make_state(_saturday())
        s.on_market_state(state_open)
        assert len(s._positions) == 1
        pos1 = s._positions[0]

        # Open position 2 on same tick (both at same market conditions)
        s.on_market_state(state_open)
        assert len(s._positions) == 2

        # Trigger TP on the next tick for both — expect two close trades
        state_tp = _make_state(
            _saturday(hour=14),
            short_ask=0.0,
            short_mark=0.0,
            long_bid=LONG_ASK_BTC,
        )
        trades = s.on_market_state(state_tp)
        close_trades = [t for t in trades if t.side == "close"]
        assert len(close_trades) == 2
        assert all(t.exit_reason == "take_profit" for t in close_trades)
        assert len(s._positions) == 0

    def test_reset_clears_positions_and_counter(self):
        s = _make_strategy()
        state = _make_state(_saturday())
        s.on_market_state(state)
        assert len(s._positions) == 1
        assert s._pos_counter == 1

        s.reset()
        assert len(s._positions) == 0
        assert s._pos_counter == 0

    def test_configure_resets_state(self):
        s = _make_strategy()
        state = _make_state(_saturday())
        s.on_market_state(state)
        assert len(s._positions) == 1

        # Re-configure for a new combo — must wipe positions
        s.configure({
            "near_dte": 14, "far_dte": 45, "entry_hour": 12,
            "entry_day": 5, "tp_pct": 0.25, "sl_pct": 0.50, "qty": 1.0,
        })
        assert len(s._positions) == 0
        assert s._pos_counter == 0

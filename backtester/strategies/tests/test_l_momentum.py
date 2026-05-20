"""
Unit tests for backtester/strategies/long_kernel.py

Tests cover:
    - Call entry fires at 4h boundary with strong up momentum
    - Put entry fires at 4h boundary with strong down momentum
    - No entry when momentum below threshold
    - No entry outside 4h boundary hours
    - No entry when spread too wide
    - No entry when delta out of range
    - TP fires when ask crosses tp_mult × entry_ask
    - TP does NOT fire when ask is below threshold
    - Spot stop fires for call when spot drops
    - Spot stop fires for put when spot rises
    - Spot stop disabled when spot_stop_pct=0
    - Time gate fires when held >= gate hours and below gain threshold
    - Time gate does NOT fire when above gain threshold
    - Max concurrent positions respected
    - on_end closes all open positions

Run:
    python -m pytest backtester/strategies/tests/test_long_kernel.py -v
"""
import math
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from backtester.strategies.l_momentum import LMomentum, _lookup_mom

# ── Constants ──────────────────────────────────────────────────────────────

SPOT = 95_000.0
EXPIRY_5DTE = "20MAY26"   # 2026-05-20: 5 DTE from 2026-05-15
EXPIRY_4DTE = "19MAY26"   # 2026-05-19: 4 DTE from 2026-05-15
STRIKE_CALL = 98_000.0
STRIKE_PUT  = 92_000.0
DELTA_CALL  =  0.35
DELTA_PUT   = -0.35

# Entry ask in BTC — $500 USD at spot 95k
ENTRY_ASK_BTC = 500.0 / SPOT
ENTRY_MARK_BTC = ENTRY_ASK_BTC * 0.98  # mark slightly below ask
ENTRY_BID_BTC = ENTRY_ASK_BTC * 0.92   # bid below ask

ENTRY_DT = datetime(2026, 5, 15, 16, 0, tzinfo=timezone.utc)  # 4h boundary


# ── Quote / state factories ────────────────────────────────────────────────

def _make_quote(strike, is_call, bid_btc, ask_btc, mark_btc, delta, spot=SPOT):
    q = SimpleNamespace(
        strike=strike,
        is_call=is_call,
        expiry=EXPIRY_5DTE,
        bid=bid_btc,
        ask=ask_btc,
        mark=mark_btc,
        delta=delta,
        spot=spot,
    )
    q.bid_usd  = bid_btc  * spot
    q.ask_usd  = ask_btc  * spot
    q.mark_usd = mark_btc * spot
    return q


def _call_quote(bid_btc=ENTRY_BID_BTC, ask_btc=ENTRY_ASK_BTC,
                mark_btc=ENTRY_MARK_BTC, delta=DELTA_CALL, spot=SPOT):
    return _make_quote(STRIKE_CALL, True,  bid_btc, ask_btc, mark_btc, delta, spot)


def _put_quote(bid_btc=ENTRY_BID_BTC, ask_btc=ENTRY_ASK_BTC,
               mark_btc=ENTRY_MARK_BTC, delta=DELTA_PUT, spot=SPOT):
    return _make_quote(STRIKE_PUT,  False, bid_btc, ask_btc, mark_btc, delta, spot)


def _make_state(dt=ENTRY_DT, spot=SPOT, call=None, put=None,
                expiries=None, spot_bars=None):
    """Build a minimal mock MarketState for one tick."""
    if call is None:
        call = _call_quote(spot=spot)
    if put is None:
        put = _put_quote(spot=spot)
    if expiries is None:
        expiries = [EXPIRY_5DTE]

    state = MagicMock()
    state.dt = dt
    state.spot = spot
    state.spot_bars = spot_bars or []

    def get_option(expiry, strike, is_call):
        if strike == STRIKE_CALL and is_call:
            return call
        if strike == STRIKE_PUT and not is_call:
            return put
        return None

    state.get_option.side_effect = get_option

    def get_chain(expiry):
        if expiry not in expiries:
            return []
        return [call, put]

    state.get_chain.side_effect = get_chain
    state.expiries.return_value = expiries
    return state


def _make_strategy(mom_4h_val=2.0, mom_1h_val=1.0, **params):
    """Construct a configured LMomentum with mock momentum indicators."""
    strat = LMomentum()
    cfg = {
        "mom_4h_thr": 1.5,
        "mom_1h_thr": 0.5,
        "tp_mult": 2.0,
        "spot_stop_pct": 2.0,
        "time_gate_h": 36,
        **params,
    }
    strat.configure(cfg)

    # Build a mock pd.Series where the 4h-lookup key is ENTRY_DT − 4h
    # and the 1h-lookup key is ENTRY_DT − 1h.
    # Must be tz-aware UTC to match hist_data.py output and _lookup_mom logic.
    ts_4h = ENTRY_DT - timedelta(hours=4)
    ts_1h = ENTRY_DT - timedelta(hours=1)
    mom4h_series = pd.Series([mom_4h_val], index=pd.DatetimeIndex([ts_4h]))
    mom1h_series = pd.Series([mom_1h_val], index=pd.DatetimeIndex([ts_1h]))

    strat.set_indicators({"spot_mom_4h": mom4h_series, "spot_mom_1h": mom1h_series})
    return strat


# ── Entry tests ────────────────────────────────────────────────────────────

class TestEntry:
    def test_call_entry_on_up_momentum(self):
        strat = _make_strategy(mom_4h_val=2.0, mom_1h_val=1.0)
        state = _make_state()
        trades = strat.on_market_state(state)
        assert len(strat._positions) == 1
        pos = strat._positions[0]
        assert pos.metadata["is_call"] is True
        assert pos.metadata["expiry"] == EXPIRY_5DTE

    def test_put_entry_on_down_momentum(self):
        strat = _make_strategy(mom_4h_val=-2.0, mom_1h_val=-1.0)
        state = _make_state()
        trades = strat.on_market_state(state)
        assert len(strat._positions) == 1
        pos = strat._positions[0]
        assert pos.metadata["is_call"] is False

    def test_no_entry_weak_4h_momentum(self):
        strat = _make_strategy(mom_4h_val=1.0, mom_1h_val=1.0)  # 4h_thr=1.5
        state = _make_state()
        strat.on_market_state(state)
        assert len(strat._positions) == 0

    def test_no_entry_weak_1h_momentum(self):
        strat = _make_strategy(mom_4h_val=2.0, mom_1h_val=0.3)  # 1h_thr=0.5
        state = _make_state()
        strat.on_market_state(state)
        assert len(strat._positions) == 0

    def test_no_entry_mixed_signal(self):
        # 4h up but 1h down → no call, no put
        strat = _make_strategy(mom_4h_val=2.0, mom_1h_val=-1.0)
        state = _make_state()
        strat.on_market_state(state)
        assert len(strat._positions) == 0

    def test_no_entry_outside_4h_boundary(self):
        # 17:05 is not a 4h boundary minute==0 with hour in {0,4,8,12,16,20}
        strat = _make_strategy()
        dt_off = ENTRY_DT.replace(hour=17, minute=5)
        state = _make_state(dt=dt_off)
        strat.on_market_state(state)
        assert len(strat._positions) == 0

    def test_no_entry_non_boundary_hour(self):
        # 06:00 is not a 4h boundary hour
        strat = _make_strategy()
        dt_off = ENTRY_DT.replace(hour=6, minute=0)
        state = _make_state(dt=dt_off)
        strat.on_market_state(state)
        assert len(strat._positions) == 0

    def test_no_entry_spread_too_wide(self):
        # ask=mark*1.2 → spread/mark = 120% — way over 10%
        strat = _make_strategy()
        wide_call = _call_quote(
            bid_btc=0.0,
            ask_btc=ENTRY_ASK_BTC * 1.2,
            mark_btc=ENTRY_MARK_BTC,
        )
        wide_put = _put_quote(
            bid_btc=0.0,
            ask_btc=ENTRY_ASK_BTC * 1.2,
            mark_btc=ENTRY_MARK_BTC,
        )
        state = _make_state(call=wide_call, put=wide_put)
        strat.on_market_state(state)
        assert len(strat._positions) == 0

    def test_no_entry_delta_out_of_range(self):
        strat = _make_strategy()
        # delta=0.25 is below 0.30 lower bound
        ood_call = _call_quote(delta=0.25)
        ood_put  = _put_quote(delta=-0.25)
        state = _make_state(call=ood_call, put=ood_put)
        strat.on_market_state(state)
        assert len(strat._positions) == 0

    def test_prefers_5dte_over_4dte(self):
        # Both expiries eligible; 5DTE should be chosen
        strat = _make_strategy()
        state = _make_state(expiries=[EXPIRY_4DTE, EXPIRY_5DTE])

        # Override get_chain to return quotes for both expiries
        call5 = _call_quote()
        put5  = _put_quote()
        call4 = _call_quote()
        put4  = _put_quote()

        def get_chain(expiry):
            if expiry == EXPIRY_5DTE:
                return [call5, put5]
            if expiry == EXPIRY_4DTE:
                return [call4, put4]
            return []

        state.get_chain.side_effect = get_chain
        strat.on_market_state(state)
        assert len(strat._positions) == 1
        assert strat._positions[0].metadata["expiry"] == EXPIRY_5DTE

    def test_max_concurrent_respected(self):
        strat = _make_strategy(**{"max_concurrent": 1})
        state = _make_state()
        # First window: opens one position
        strat.on_market_state(state)
        assert len(strat._positions) == 1
        # Second window at next 4h boundary: blocked by max_concurrent=1
        dt2 = ENTRY_DT + timedelta(hours=4)

        ts_4h2 = dt2 - timedelta(hours=4)  # tz-aware UTC
        ts_1h2 = dt2 - timedelta(hours=1)  # tz-aware UTC
        mom4h2 = pd.Series([2.0], index=pd.DatetimeIndex([ts_4h2]))
        mom1h2 = pd.Series([1.0], index=pd.DatetimeIndex([ts_1h2]))
        strat._mom_4h = pd.concat([strat._mom_4h, mom4h2])
        strat._mom_1h = pd.concat([strat._mom_1h, mom1h2])

        state2 = _make_state(dt=dt2)
        # patch get_option so existing position doesn't trigger TP/stop
        existing = strat._positions[0]
        state2.get_option.side_effect = lambda e, s, c: _call_quote()
        strat.on_market_state(state2)
        assert len(strat._positions) == 1  # still only 1

    def test_one_trade_per_window(self):
        # Only one trade per 4h window even if multiple expiries match
        strat = _make_strategy()
        state = _make_state(expiries=[EXPIRY_4DTE, EXPIRY_5DTE])

        def get_chain(expiry):
            return [_call_quote(), _put_quote()]

        state.get_chain.side_effect = get_chain
        strat.on_market_state(state)
        assert len(strat._positions) == 1


# ── Exit tests ─────────────────────────────────────────────────────────────

class TestTakeProfit:
    def test_tp_fires_on_ask_threshold(self):
        strat = _make_strategy()
        entry_state = _make_state()
        strat.on_market_state(entry_state)
        assert len(strat._positions) == 1

        tp_ask_btc = ENTRY_ASK_BTC * strat._tp_mult + 0.00001
        tp_call = _call_quote(ask_btc=tp_ask_btc)
        exit_state = _make_state(
            dt=ENTRY_DT + timedelta(hours=6),
            call=tp_call,
        )
        trades = strat.on_market_state(exit_state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "take_profit"
        assert len(strat._positions) == 0

    def test_tp_does_not_fire_below_threshold(self):
        strat = _make_strategy()
        entry_state = _make_state()
        strat.on_market_state(entry_state)

        # ask = 1.5× entry_ask → below tp_mult=2.0
        below_tp = _call_quote(ask_btc=ENTRY_ASK_BTC * 1.5)
        exit_state = _make_state(dt=ENTRY_DT + timedelta(hours=6), call=below_tp)
        trades = strat.on_market_state(exit_state)
        assert len(trades) == 0
        assert len(strat._positions) == 1

    def test_tp_pnl_is_positive(self):
        strat = _make_strategy()
        strat.on_market_state(_make_state())

        tp_ask_btc = ENTRY_ASK_BTC * (strat._tp_mult + 0.01)
        tp_bid_btc = tp_ask_btc * 0.95
        tp_call = _call_quote(ask_btc=tp_ask_btc, bid_btc=tp_bid_btc,
                              mark_btc=tp_ask_btc * 0.97)
        exit_state = _make_state(dt=ENTRY_DT + timedelta(hours=8), call=tp_call)
        trades = strat.on_market_state(exit_state)
        assert trades[0].pnl > 0


class TestSpotStop:
    def test_call_stopped_on_spot_drop(self):
        strat = _make_strategy()
        strat.on_market_state(_make_state())
        assert strat._positions[0].metadata["is_call"] is True

        # Spot drops 2.1% below entry
        drop_spot = SPOT * (1 - 0.021)
        exit_state = _make_state(dt=ENTRY_DT + timedelta(hours=3), spot=drop_spot)
        trades = strat.on_market_state(exit_state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "spot_stop"

    def test_call_not_stopped_on_small_drop(self):
        strat = _make_strategy()
        strat.on_market_state(_make_state())

        # Spot drops 1.5% — below the 2.0% threshold
        small_drop = SPOT * (1 - 0.015)
        exit_state = _make_state(dt=ENTRY_DT + timedelta(hours=3), spot=small_drop)
        trades = strat.on_market_state(exit_state)
        assert len(trades) == 0

    def test_put_stopped_on_spot_rise(self):
        strat = _make_strategy(mom_4h_val=-2.0, mom_1h_val=-1.0)
        strat.on_market_state(_make_state())
        assert strat._positions[0].metadata["is_call"] is False

        # Spot rises 2.1% → adverse for put
        rise_spot = SPOT * (1 + 0.021)
        exit_state = _make_state(dt=ENTRY_DT + timedelta(hours=3), spot=rise_spot)
        trades = strat.on_market_state(exit_state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "spot_stop"

    def test_call_not_stopped_on_spot_rise(self):
        # Spot RISES for a call — favourable, no stop
        strat = _make_strategy()
        strat.on_market_state(_make_state())

        rise_spot = SPOT * 1.05
        exit_state = _make_state(dt=ENTRY_DT + timedelta(hours=2), spot=rise_spot)
        trades = strat.on_market_state(exit_state)
        assert len(trades) == 0

    def test_spot_stop_disabled_when_zero(self):
        strat = _make_strategy(spot_stop_pct=0.0)
        strat.on_market_state(_make_state())

        # Massive adverse move — stop disabled
        crash_spot = SPOT * 0.80
        exit_state = _make_state(dt=ENTRY_DT + timedelta(hours=3), spot=crash_spot)
        trades = strat.on_market_state(exit_state)
        assert len(trades) == 0


class TestTimeGate:
    def test_gate_fires_when_stalled(self):
        strat = _make_strategy(time_gate_h=36)
        strat.on_market_state(_make_state())

        entry_ask_usd = strat._positions[0].metadata["entry_ask_usd"]
        # bid below time_gate_min_gain threshold (1.30×) at gate time
        stalled_bid_btc = (entry_ask_usd * 1.10) / SPOT
        stalled_call = _call_quote(bid_btc=stalled_bid_btc)
        gate_dt = ENTRY_DT + timedelta(hours=36, minutes=5)
        exit_state = _make_state(dt=gate_dt, call=stalled_call)
        trades = strat.on_market_state(exit_state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "time_gate"

    def test_gate_does_not_fire_before_threshold_time(self):
        strat = _make_strategy(time_gate_h=36)
        strat.on_market_state(_make_state())

        # Only 12 hours held — gate hasn't opened yet
        entry_ask_usd = strat._positions[0].metadata["entry_ask_usd"]
        stalled_bid_btc = (entry_ask_usd * 1.05) / SPOT
        stalled_call = _call_quote(bid_btc=stalled_bid_btc)
        exit_state = _make_state(dt=ENTRY_DT + timedelta(hours=12), call=stalled_call)
        trades = strat.on_market_state(exit_state)
        assert len(trades) == 0

    def test_gate_does_not_fire_when_above_gain(self):
        strat = _make_strategy(time_gate_h=36)
        strat.on_market_state(_make_state())

        # bid is 1.5× entry_ask — above the 1.30 threshold → hold
        entry_ask_usd = strat._positions[0].metadata["entry_ask_usd"]
        good_bid_btc = (entry_ask_usd * 1.5) / SPOT
        good_call = _call_quote(bid_btc=good_bid_btc)
        gate_dt = ENTRY_DT + timedelta(hours=37)
        exit_state = _make_state(dt=gate_dt, call=good_call)
        trades = strat.on_market_state(exit_state)
        assert len(trades) == 0

    def test_gate_disabled_when_zero(self):
        strat = _make_strategy(time_gate_h=0)
        strat.on_market_state(_make_state())

        # 100 hours held with zero bid — gate disabled → stays open
        entry_ask_usd = strat._positions[0].metadata["entry_ask_usd"]
        stalled_call = _call_quote(bid_btc=(entry_ask_usd * 0.0) / SPOT)
        exit_state = _make_state(dt=ENTRY_DT + timedelta(hours=100), call=stalled_call)
        trades = strat.on_market_state(exit_state)
        assert len(trades) == 0


class TestLifecycle:
    def test_on_end_closes_all_positions(self):
        strat = _make_strategy()
        strat.on_market_state(_make_state())
        assert len(strat._positions) == 1

        end_state = _make_state(dt=ENTRY_DT + timedelta(hours=120))
        trades = strat.on_end(end_state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "end_of_data"
        assert len(strat._positions) == 0

    def test_reset_clears_positions(self):
        strat = _make_strategy()
        strat.on_market_state(_make_state())
        strat.reset()
        assert len(strat._positions) == 0


# ── _lookup_mom unit tests ─────────────────────────────────────────────────

class TestLookupMom:
    def test_returns_correct_value(self):
        ts = datetime(2026, 5, 15, 12, 0)  # bar open at 12:00
        series = pd.Series([1.5], index=pd.DatetimeIndex([ts]))
        # At 16:00, looking back 4h → key = 12:00
        result = _lookup_mom(series, datetime(2026, 5, 15, 16, 0), interval_h=4)
        assert result == pytest.approx(1.5)

    def test_returns_none_for_missing_key(self):
        series = pd.Series([], index=pd.DatetimeIndex([]), dtype=float)
        result = _lookup_mom(series, datetime(2026, 5, 15, 16, 0), interval_h=4)
        assert result is None

    def test_returns_none_for_nan(self):
        ts = datetime(2026, 5, 15, 12, 0)
        series = pd.Series([float("nan")], index=pd.DatetimeIndex([ts]))
        result = _lookup_mom(series, datetime(2026, 5, 15, 16, 0), interval_h=4)
        assert result is None

    def test_returns_none_when_series_is_none(self):
        assert _lookup_mom(None, datetime(2026, 5, 15, 16, 0), interval_h=4) is None

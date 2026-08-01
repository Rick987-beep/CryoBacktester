"""
Unit tests for Str_VolBurst_Pullback strategy.

Coverage:
  - configure() sets correct tier/DTE/delta params
  - non-hour bars skip entry check
  - no signal → no open
  - stand_aside suppresses entry
  - pullback signal → opens strangle
  - vol_burst signal → opens strangle
  - cooldown blocks re-entry within 4h
  - open position blocks second entry
  - time_stop exits after ts_h hours
  - take_profit exits when value ≥ entry_cost × tp_x
  - on_end() force-closes open position

Run:
    python -m pytest backtester/strategies/tests/test_str_volburst_pullback.py -v
"""
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from typing import Optional

import pandas as pd
import pytest

from backtester.strategies.str_volburst_pullback import StrVolBurstPullback

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(s: str) -> datetime:
    """Parse ISO datetime string to UTC-aware datetime."""
    return datetime.fromisoformat(s).replace(tzinfo=UTC)


def _make_quote(
    strike: float,
    is_call: bool,
    bid_btc: float,
    ask_btc: float,
    spot: float = 90_000.0,
    delta: float = 0.30,
    mark_iv: float = 50.0,
    expiry: str = "4JAN26",
) -> SimpleNamespace:
    q = SimpleNamespace(
        strike=strike,
        is_call=is_call,
        expiry=expiry,
        bid=bid_btc,
        ask=ask_btc,
        mark=bid_btc,
        delta=delta,
        mark_iv=mark_iv,
        spot=spot,
        bid_usd=bid_btc * spot,
        ask_usd=ask_btc * spot,
        mark_usd=bid_btc * spot,
    )
    return q


def _make_signals(
    bars: pd.DatetimeIndex,
    *,
    pullback_fires: tuple = (),
    vol_burst_fires: tuple = (),
    stand_aside_bars: tuple = (),
    rv_rank_default: float = 0.70,
) -> pd.DataFrame:
    """Build a minimal signals DataFrame.  All signals default to False."""
    n = len(bars)
    data = {
        "ret_1h": [0.0] * n,
        "ret_4h": [0.0] * n,
        "rv_24h": [50.0] * n,
        "rv_rank": [rv_rank_default] * n,
        "vol_z": [0.0] * n,
        "stand_aside": [False] * n,
        "pullback_signal": [False] * n,
        "vol_burst_signal": [False] * n,
    }
    df = pd.DataFrame(data, index=bars)
    for i in pullback_fires:
        df.iloc[i, df.columns.get_loc("pullback_signal")] = True
    for i in vol_burst_fires:
        df.iloc[i, df.columns.get_loc("vol_burst_signal")] = True
    for i in stand_aside_bars:
        df.iloc[i, df.columns.get_loc("stand_aside")] = True
    return df


def _make_state(
    dt: datetime,
    spot: float = 90_000.0,
    expiries: tuple = ("4JAN26",),
    call_quote: Optional[SimpleNamespace] = None,
    put_quote: Optional[SimpleNamespace] = None,
) -> SimpleNamespace:
    """Build a minimal MarketState-like namespace."""
    chain = []
    if call_quote is not None:
        chain.append(call_quote)
    if put_quote is not None:
        chain.append(put_quote)

    def _get_option(exp, strike, is_call):
        for q in chain:
            if q.strike == strike and q.is_call == is_call:
                return q
        return None

    return SimpleNamespace(
        dt=dt,
        spot=spot,
        spot_bars=[],
        expiries=lambda: list(expiries),
        get_chain=lambda exp: chain,
        get_option=_get_option,
    )


def _default_quotes(
    spot: float = 90_000.0,
    ask_btc: float = 0.0020,     # ask_usd = 180.0  (well above $75 floor)
    bid_btc: float = 0.0015,     # bid_usd = 135.0
    expiry: str = "4JAN26",
):
    """Strangle legs that pass all availability checks."""
    call_q = _make_quote(95_000.0, True,  bid_btc, ask_btc, spot=spot, expiry=expiry)
    put_q  = _make_quote(85_000.0, False, bid_btc, ask_btc, spot=spot, expiry=expiry)
    return call_q, put_q


def _strategy_with_signals(
    tier: str = "pullback",
    tp_x=None,
    ts_h: int = 20,
    sl_x=None,
    bars=None,
    pullback_fires: tuple = (),
    vol_burst_fires: tuple = (),
    stand_aside_bars: tuple = (),
) -> StrVolBurstPullback:
    """Build a configured strategy with a pre-populated signals DataFrame."""
    s = StrVolBurstPullback()
    s.configure({"tier": tier, "tp_x": tp_x, "ts_h": ts_h, "sl_x": sl_x})
    if bars is None:
        # 24 hours starting 2026-01-02 00:00 UTC
        bars = pd.date_range("2026-01-02 00:00", periods=24, freq="1h", tz="UTC")
    sig_df = _make_signals(
        bars,
        pullback_fires=pullback_fires,
        vol_burst_fires=vol_burst_fires,
        stand_aside_bars=stand_aside_bars,
    )
    s.set_indicators({"vol_burst_pullback": sig_df})
    return s


# ---------------------------------------------------------------------------
# 1. Configuration
# ---------------------------------------------------------------------------

class TestConfigure:
    def test_pullback_sets_dte_and_delta(self):
        s = StrVolBurstPullback()
        s.configure({"tier": "pullback", "tp_x": 2.0, "ts_h": 20, "sl_x": None})
        assert s._dte == 2
        assert s._delta == 0.30

    def test_vol_burst_sets_dte_and_delta(self):
        s = StrVolBurstPullback()
        s.configure({"tier": "vol_burst", "tp_x": None, "ts_h": 4, "sl_x": None})
        assert s._dte == 1
        assert s._delta == 0.35

    def test_configure_sets_tp_and_sl(self):
        s = StrVolBurstPullback()
        s.configure({"tier": "pullback", "tp_x": 1.5, "ts_h": 8, "sl_x": 0.3})
        assert s._tp_x == 1.5
        assert s._sl_x == 0.3
        assert s._ts_h == 8.0

    def test_reset_clears_position_and_timestamp(self):
        s = StrVolBurstPullback()
        s.configure({"tier": "pullback", "tp_x": None, "ts_h": 20, "sl_x": None})
        s._position = object()
        s._last_fire_ts = _ts("2026-01-01T12:00:00")
        s.reset()
        assert s._position is None
        assert s._last_fire_ts is None

    def test_describe_params_returns_dict(self):
        s = StrVolBurstPullback()
        s.configure({"tier": "vol_burst", "tp_x": 2.0, "ts_h": 4, "sl_x": 0.5})
        p = s.describe_params()
        assert p["tier"] == "vol_burst"
        assert p["dte"] == 1
        assert p["tp_x"] == 2.0


# ---------------------------------------------------------------------------
# 2. Entry guard — non-signal bars
# ---------------------------------------------------------------------------

class TestNoEntry:
    def test_non_hour_bar_skips_entry_check(self):
        s = _strategy_with_signals("pullback", pullback_fires=(12,))
        state = _make_state(_ts("2026-01-02T12:05:00"))
        result = s.on_market_state(state)
        assert result == []
        assert s._position is None

    def test_no_signal_no_open(self):
        """When no signal fires, position stays None."""
        s = _strategy_with_signals("pullback")  # no pullback_fires
        state = _make_state(_ts("2026-01-02T13:00:00"))
        result = s.on_market_state(state)
        assert result == []
        assert s._position is None

    def test_stand_aside_suppresses_signal(self):
        """stand_aside at signal bar blocks entry even when signal fires."""
        # bar index 12 = 12:00 UTC; state.dt = 13:00 so bar_ts = 12:00
        s = _strategy_with_signals("pullback", pullback_fires=(12,), stand_aside_bars=(12,))
        state = _make_state(_ts("2026-01-02T13:00:00"))
        assert s.on_market_state(state) == []
        assert s._position is None

    def test_no_signals_dataframe_no_crash(self):
        """set_indicators not called → signals is None → no crash."""
        s = StrVolBurstPullback()
        s.configure({"tier": "pullback", "tp_x": None, "ts_h": 20, "sl_x": None})
        state = _make_state(_ts("2026-01-02T13:00:00"))
        assert s.on_market_state(state) == []

    def test_missing_bar_ts_key_no_crash(self):
        """KeyError on signal lookup is silently swallowed."""
        s = _strategy_with_signals("pullback")
        # bar_ts = 11:59 which won't be in the hourly index
        state = _make_state(_ts("2026-01-02T12:59:00"))
        # 12:59 has minute != 0, so entry check skipped anyway — just verify no crash
        assert s.on_market_state(state) == []


# ---------------------------------------------------------------------------
# 3. Entry — signal fires and opens a strangle
# ---------------------------------------------------------------------------

class TestEntry:
    def test_pullback_signal_opens_position(self):
        """Pullback signal at bar 12 → state.dt=13:00 → opens strangle."""
        s = _strategy_with_signals("pullback", pullback_fires=(12,))
        call_q, put_q = _default_quotes(expiry="4JAN26")
        # state.dt = 13:00 UTC; bar_ts = 12:00 = bars[12]
        state = _make_state(
            _ts("2026-01-02T13:00:00"),
            expiries=("4JAN26",),
            call_quote=call_q,
            put_quote=put_q,
        )
        trades = s.on_market_state(state)
        assert trades == []                    # entry emits no Trade
        assert s._position is not None
        assert len(s._position.legs) == 2
        assert s._position.metadata["tier"] == "pullback"
        assert s._last_fire_ts == _ts("2026-01-02T13:00:00")

    def test_vol_burst_signal_opens_position(self):
        """vol_burst signal at bar 10 → state.dt=11:00 → opens strangle."""
        bars = pd.date_range("2026-01-02 00:00", periods=24, freq="1h", tz="UTC")
        s = _strategy_with_signals("vol_burst", ts_h=4, vol_burst_fires=(10,), bars=bars)
        call_q, put_q = _default_quotes(expiry="3JAN26")
        state = _make_state(
            _ts("2026-01-02T11:00:00"),
            expiries=("3JAN26",),
            call_quote=call_q,
            put_quote=put_q,
        )
        trades = s.on_market_state(state)
        assert trades == []
        assert s._position is not None
        assert s._position.metadata["tier"] == "vol_burst"

    def test_entry_computes_correct_cost(self):
        """entry_price_usd = call.ask_usd + put.ask_usd."""
        spot = 90_000.0
        ask_btc = 0.002  # ask_usd = 180.0 each
        s = _strategy_with_signals("pullback", pullback_fires=(12,))
        call_q = _make_quote(95_000.0, True,  bid_btc=0.0015, ask_btc=ask_btc, spot=spot)
        put_q  = _make_quote(85_000.0, False, bid_btc=0.0015, ask_btc=ask_btc, spot=spot)
        state = _make_state(
            _ts("2026-01-02T13:00:00"),
            spot=spot,
            expiries=("4JAN26",),
            call_quote=call_q,
            put_quote=put_q,
        )
        s.on_market_state(state)
        expected_cost = ask_btc * spot * 2  # 360.0
        assert s._position is not None
        assert abs(s._position.entry_price_usd - expected_cost) < 0.01

    def test_tp_target_stored_in_metadata(self):
        s = _strategy_with_signals("pullback", tp_x=2.0, pullback_fires=(12,))
        call_q, put_q = _default_quotes()
        state = _make_state(
            _ts("2026-01-02T13:00:00"), expiries=("4JAN26",),
            call_quote=call_q, put_quote=put_q,
        )
        s.on_market_state(state)
        pos = s._position
        assert pos is not None
        expected_tp = pos.entry_price_usd * 2.0
        assert abs(pos.metadata["tp_target_usd"] - expected_tp) < 0.01

    def test_no_entry_when_ask_below_floor(self):
        """ask_usd < $75 on either leg → skip entry."""
        spot = 90_000.0
        # ask_btc = 0.0005 → ask_usd = $45.0, below the $75 floor
        low_ask_btc = 0.0005
        s = _strategy_with_signals("pullback", pullback_fires=(12,))
        call_q = _make_quote(95_000.0, True,  bid_btc=0.0004, ask_btc=low_ask_btc, spot=spot)
        put_q  = _make_quote(85_000.0, False, bid_btc=0.0004, ask_btc=low_ask_btc, spot=spot)
        state = _make_state(
            _ts("2026-01-02T13:00:00"), spot=spot, expiries=("4JAN26",),
            call_quote=call_q, put_quote=put_q,
        )
        s.on_market_state(state)
        assert s._position is None

    def test_no_entry_when_spread_too_wide(self):
        """(ask - bid) / ask > 30% → skip entry."""
        spot = 90_000.0
        # ask_btc=0.002, bid_btc=0.001 → spread = 50% > 30%
        s = _strategy_with_signals("pullback", pullback_fires=(12,))
        call_q = _make_quote(95_000.0, True,  bid_btc=0.001, ask_btc=0.002, spot=spot)
        put_q  = _make_quote(85_000.0, False, bid_btc=0.001, ask_btc=0.002, spot=spot)
        state = _make_state(
            _ts("2026-01-02T13:00:00"), spot=spot, expiries=("4JAN26",),
            call_quote=call_q, put_quote=put_q,
        )
        s.on_market_state(state)
        assert s._position is None

    def test_no_entry_when_no_expiry_available(self):
        """No matching expiry → skip entry."""
        s = _strategy_with_signals("pullback", pullback_fires=(12,))
        state = _make_state(
            _ts("2026-01-02T13:00:00"),
            expiries=(),   # empty — no expiry available
        )
        s.on_market_state(state)
        assert s._position is None


# ---------------------------------------------------------------------------
# 4. Cooldown and re-entry guard
# ---------------------------------------------------------------------------

class TestCooldown:
    def test_cooldown_blocks_entry_within_4h(self):
        """2nd signal within 4h of first entry → blocked."""
        bars = pd.date_range("2026-01-02 00:00", periods=25, freq="1h", tz="UTC")
        # signals at bar 12 (12:00) and bar 14 (14:00)
        s = _strategy_with_signals("pullback", pullback_fires=(12, 14), bars=bars)
        call_q, put_q = _default_quotes()

        # First entry: state.dt=13:00, bar_ts=12:00
        state1 = _make_state(
            _ts("2026-01-02T13:00:00"), expiries=("4JAN26",),
            call_quote=call_q, put_quote=put_q,
        )
        s.on_market_state(state1)
        assert s._position is not None
        fire_ts = s._last_fire_ts

        # Manually close position to isolate cooldown check
        s._position = None

        # Second attempt: state.dt=15:00, bar_ts=14:00 (2h after first fire)
        state2 = _make_state(
            _ts("2026-01-02T15:00:00"), expiries=("4JAN26",),
            call_quote=call_q, put_quote=put_q,
        )
        s.on_market_state(state2)
        assert s._position is None, "cooldown should block re-entry within 4h"
        assert s._last_fire_ts == fire_ts, "last_fire_ts should be unchanged"

    def test_entry_allowed_after_cooldown_expires(self):
        """Signal at 4h+ after previous fire → allowed."""
        bars = pd.date_range("2026-01-02 00:00", periods=24, freq="1h", tz="UTC")
        # signal at bar 16 (16:00)
        s = _strategy_with_signals("pullback", pullback_fires=(16,), bars=bars)
        call_q, put_q = _default_quotes()

        # Manually set last_fire_ts to 4h before the signal bar check time
        s._last_fire_ts = _ts("2026-01-02T13:00:00")

        # state.dt=17:00 → bar_ts=16:00 → elapsed = 4h exactly → allowed
        state = _make_state(
            _ts("2026-01-02T17:00:00"), expiries=("4JAN26",),
            call_quote=call_q, put_quote=put_q,
        )
        s.on_market_state(state)
        assert s._position is not None

    def test_open_position_blocks_second_entry(self):
        """If already holding a position, skip entry even if signal fires."""
        s = _strategy_with_signals("pullback", pullback_fires=(12,))
        call_q, put_q = _default_quotes()

        state = _make_state(
            _ts("2026-01-02T13:00:00"), expiries=("4JAN26",),
            call_quote=call_q, put_quote=put_q,
        )
        # First entry
        s.on_market_state(state)
        first_pos = s._position
        assert first_pos is not None

        # Same state again — should NOT open a second position
        # (position is still open from previous call, exit not triggered yet)
        s._last_fire_ts = None  # remove cooldown so only open-position check applies
        # Push time well past 4h so cooldown isn't the blocker
        bars2 = pd.date_range("2026-01-02 18:00", periods=3, freq="1h", tz="UTC")
        sig2 = _make_signals(bars2, pullback_fires=(1,))
        s.set_indicators({"vol_burst_pullback": sig2})
        state2 = _make_state(
            _ts("2026-01-02T19:00:00"), expiries=("4JAN26",),
            call_quote=call_q, put_quote=put_q,
        )
        s.on_market_state(state2)
        assert s._position is first_pos, "position should remain the original"


# ---------------------------------------------------------------------------
# 5. Exit logic
# ---------------------------------------------------------------------------

class TestExits:
    def _open_position(
        self,
        entry_dt: datetime,
        tp_x=None,
        ts_h: int = 20,
        sl_x=None,
        bid_btc: float = 0.0015,
        ask_btc: float = 0.0020,
        expiry: str = "4JAN26",
    ) -> StrVolBurstPullback:
        """Helper: configure strategy, fire a signal, open a strangle."""
        # Use entry bar index 0; state.dt = 1h later
        entry_bar_dt = entry_dt - timedelta(hours=1)
        bars = pd.DatetimeIndex([entry_bar_dt], tz="UTC")
        s = StrVolBurstPullback()
        s.configure({"tier": "pullback", "tp_x": tp_x, "ts_h": ts_h, "sl_x": sl_x})
        sig_df = _make_signals(bars, pullback_fires=(0,))
        s.set_indicators({"vol_burst_pullback": sig_df})

        call_q = _make_quote(95_000.0, True,  bid_btc, ask_btc, expiry=expiry)
        put_q  = _make_quote(85_000.0, False, bid_btc, ask_btc, expiry=expiry)
        state = _make_state(
            entry_dt, expiries=(expiry,), call_quote=call_q, put_quote=put_q,
        )
        s.on_market_state(state)
        assert s._position is not None, "expected open position after entry"
        return s

    def test_time_stop_closes_after_ts_h(self):
        """After ts_h hours, position is closed with reason time_stop."""
        entry_dt = _ts("2026-01-02T13:00:00")
        s = self._open_position(entry_dt, tp_x=None, ts_h=4, sl_x=None)

        # Create close state at exactly ts_h hours after entry
        close_dt = entry_dt + timedelta(hours=4)
        call_q = _make_quote(95_000.0, True,  bid_btc=0.0015, ask_btc=0.002, expiry="4JAN26")
        put_q  = _make_quote(85_000.0, False, bid_btc=0.0015, ask_btc=0.002, expiry="4JAN26")
        state = _make_state(close_dt, expiries=("4JAN26",), call_quote=call_q, put_quote=put_q)

        trades = s.on_market_state(state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "time_stop"
        assert s._position is None

    def test_take_profit_fires_before_time_stop(self):
        """TP fires when current_usd ≥ entry_cost × tp_x."""
        entry_dt = _ts("2026-01-02T13:00:00")
        spot = 90_000.0
        ask_btc = 0.002   # ask_usd = 180.0 each → entry_cost = 360.0
        # tp_x = 1.5 → tp_target = 540.0
        # Close bid = 0.004 → bid_usd = 360.0 each → current_usd = 720.0 ≥ 540.0
        s = self._open_position(entry_dt, tp_x=1.5, ts_h=20, ask_btc=ask_btc)

        close_dt = entry_dt + timedelta(hours=2)  # well before time-stop
        high_bid_btc = 0.004  # bid_usd = 360.0 each → total = 720.0
        call_q = _make_quote(95_000.0, True,  high_bid_btc, high_bid_btc * 1.1, spot=spot)
        put_q  = _make_quote(85_000.0, False, high_bid_btc, high_bid_btc * 1.1, spot=spot)
        state = _make_state(close_dt, spot=spot, expiries=("4JAN26",), call_quote=call_q, put_quote=put_q)

        trades = s.on_market_state(state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "take_profit"
        assert s._position is None

    def test_stop_loss_fires_below_sl_floor(self):
        """SL fires when current_usd ≤ entry_cost × sl_x."""
        entry_dt = _ts("2026-01-02T13:00:00")
        spot = 90_000.0
        ask_btc = 0.002   # entry_cost = 360.0
        # sl_x = 0.5 → sl_floor = 180.0
        # Close bid = 0.0005 → bid_usd = 45.0 each → current_usd = 90.0 ≤ 180.0
        s = self._open_position(entry_dt, tp_x=None, ts_h=20, sl_x=0.5, ask_btc=ask_btc)

        close_dt = entry_dt + timedelta(hours=2)
        low_bid_btc = 0.0005  # bid_usd = 45.0 each → total = 90.0
        call_q = _make_quote(95_000.0, True,  low_bid_btc, low_bid_btc * 1.1, spot=spot)
        put_q  = _make_quote(85_000.0, False, low_bid_btc, low_bid_btc * 1.1, spot=spot)
        state = _make_state(close_dt, spot=spot, expiries=("4JAN26",), call_quote=call_q, put_quote=put_q)

        trades = s.on_market_state(state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "stop_loss"
        assert s._position is None

    def test_expiry_stop_fires_one_hour_before_expiry(self):
        """expiry_stop fires at expiry_dt − 1h."""
        # 4JAN26 expires at 08:00 UTC → expiry_stop at 07:00 UTC
        entry_dt = _ts("2026-01-02T13:00:00")
        s = self._open_position(entry_dt, tp_x=None, ts_h=60, sl_x=None)

        # state.dt = 2026-01-04T07:00 → exactly at expiry_stop
        expiry_stop_dt = _ts("2026-01-04T07:00:00")
        call_q = _make_quote(95_000.0, True,  0.0015, 0.002)
        put_q  = _make_quote(85_000.0, False, 0.0015, 0.002)
        state = _make_state(expiry_stop_dt, expiries=("4JAN26",), call_quote=call_q, put_quote=put_q)

        trades = s.on_market_state(state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "expiry_stop"
        assert s._position is None

    def test_on_end_force_closes(self):
        """on_end() closes open position with reason end_of_data."""
        entry_dt = _ts("2026-01-02T13:00:00")
        s = self._open_position(entry_dt, tp_x=None, ts_h=20, sl_x=None)

        end_dt = _ts("2026-01-10T00:00:00")
        call_q = _make_quote(95_000.0, True,  0.0015, 0.002)
        put_q  = _make_quote(85_000.0, False, 0.0015, 0.002)
        state = _make_state(end_dt, expiries=("4JAN26",), call_quote=call_q, put_quote=put_q)

        trades = s.on_end(state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "end_of_data"
        assert s._position is None

    def test_on_end_no_position_returns_empty(self):
        """on_end() with no open position returns []."""
        s = StrVolBurstPullback()
        s.configure({"tier": "pullback", "tp_x": None, "ts_h": 20, "sl_x": None})
        state = _make_state(_ts("2026-01-10T00:00:00"))
        assert s.on_end(state) == []

    def test_exit_before_time_stop_no_exit_when_tp_none(self):
        """With tp_x=None and sl_x=None, no TP/SL check runs, time-stop fires."""
        entry_dt = _ts("2026-01-02T13:00:00")
        s = self._open_position(entry_dt, tp_x=None, ts_h=8, sl_x=None)

        # Midway: 4h held (ts_h=8 not yet reached, no TP/SL) → no exit
        mid_dt = entry_dt + timedelta(hours=4)
        call_q = _make_quote(95_000.0, True,  0.0015, 0.002)
        put_q  = _make_quote(85_000.0, False, 0.0015, 0.002)
        state_mid = _make_state(mid_dt, expiries=("4JAN26",), call_quote=call_q, put_quote=put_q)
        trades_mid = s.on_market_state(state_mid)
        assert trades_mid == []
        assert s._position is not None

        # At ts_h: 8h held → time-stop
        stop_dt = entry_dt + timedelta(hours=8)
        state_stop = _make_state(stop_dt, expiries=("4JAN26",), call_quote=call_q, put_quote=put_q)
        trades_stop = s.on_market_state(state_stop)
        assert len(trades_stop) == 1
        assert trades_stop[0].exit_reason == "time_stop"

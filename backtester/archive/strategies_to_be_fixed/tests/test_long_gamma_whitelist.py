"""
Unit + integration tests for:
    backtester/indicators.py         — long_gamma_regime(), pair_signals(),
                                       _pair_first_entry_then_next_exit()
    backtester/strategies/long_gamma_whitelist.py  — strategy open/close protocol

Test groups:
    1. Pairing algorithm  — fan-out, no subsequent exit, empty inputs
    2. Whitelist gate     — bull_armed requires date in BULL_WHITELIST_DATES
    3. Regime gate        — bull_armed requires bull_regime (SMA8 > SMA21)
    4. SIDEWAYS / invalid mode
    5. Strategy: open, exit, expiry, no-chain guard
    6. Reference replay (live, requires network) — compare against the 7 bull /
       39 bear pairs in the reference CSVs delivered by the strategy author.

Run all (except live):
    .venv/bin/python -m pytest backtester/strategies/tests/test_long_gamma_whitelist.py -v

Run live tests too:
    .venv/bin/python -m pytest backtester/strategies/tests/test_long_gamma_whitelist.py -v -m live
"""
import os
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from backtester.indicators import (
    BULL_WHITELIST_DATES,
    BEAR_WHITELIST_DATES,
    long_gamma_regime,
    pair_signals,
    _pair_first_entry_then_next_exit,
)
from backtester.strategies.long_gamma_whitelist import LongGammaWhitelist

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_SPOT = 85_000.0
_EXPIRY = "11MAY26"   # ~11 DTE from a test base date of 2026-04-30
_STRIKE_CALL = 90_000.0
_STRIKE_PUT  = 80_000.0

_REF_DIR = Path(__file__).parents[2] / "newstrategy" / "coincall_signal_schedule_bull"
_REF_BULL = _REF_DIR / "coincall_signal_schedule_bull.csv"
_REF_BEAR = _REF_DIR / "coincall_signal_schedule_bear.csv"


def _make_4h_df(closes, start_ts):
    """Minimal 4h OHLCV DataFrame with given close prices."""
    idx = pd.date_range(start=start_ts, periods=len(closes), freq="4h", tz="UTC")
    return pd.DataFrame({"close": closes}, index=idx)


def _make_regime_df(n, entry_bull_idxs=(), exit_bull_idxs=(),
                    entry_bear_idxs=(), exit_bear_idxs=(),
                    start_ts="2026-04-20 00:00"):
    """
    Build a minimal regime DataFrame with explicit signal bars.

    All boolean columns default to False; the listed index positions are
    flipped to True.  bull_regime / bear_regime are set to True throughout
    so that bull_armed / bear_armed are driven by the whitelist gate only
    when tested separately — but these columns are not used by pair_signals().
    """
    idx = pd.date_range(start=start_ts, periods=n, freq="4h", tz="UTC")
    data = {
        "sma8": [50000.0] * n, "sma21": [49000.0] * n,
        "sma20": [49000.0] * n, "sma50": [50000.0] * n,
        "ema8": [50000.0] * n, "ema21": [49000.0] * n,
        "rsi14": [55.0] * n,
        "bull_regime": [True] * n,
        "bear_regime": [True] * n,
        "ema_cross_up":    [False] * n,
        "ema_cross_down":  [False] * n,
        "rsi_cross_up55":  [False] * n,
        "rsi_cross_dn45":  [False] * n,
        "bull_armed":  [False] * n,
        "bear_armed":  [False] * n,
    }
    df = pd.DataFrame(data, index=idx)
    for i in entry_bull_idxs:
        df.iloc[i, df.columns.get_loc("ema_cross_up")]  = True
        df.iloc[i, df.columns.get_loc("bull_armed")]    = True
    for i in exit_bull_idxs:
        df.iloc[i, df.columns.get_loc("ema_cross_down")] = True
    for i in entry_bear_idxs:
        df.iloc[i, df.columns.get_loc("rsi_cross_up55")] = True
        df.iloc[i, df.columns.get_loc("bear_armed")]     = True
    for i in exit_bear_idxs:
        df.iloc[i, df.columns.get_loc("rsi_cross_dn45")] = True
    return df


def _make_quote(strike, is_call, bid, ask, delta):
    q = SimpleNamespace(
        strike=strike, is_call=is_call, expiry=_EXPIRY,
        bid=bid, ask=ask, mark=bid, delta=delta, spot=_SPOT,
    )
    q.bid_usd  = bid  * _SPOT
    q.ask_usd  = ask  * _SPOT
    q.mark_usd = bid  * _SPOT
    return q


def _make_state(dt, spot=_SPOT, has_chain=True, has_option=True):
    call_q = _make_quote(_STRIKE_CALL, True,  bid=0.002, ask=0.0025, delta=+0.70)
    put_q  = _make_quote(_STRIKE_PUT,  False, bid=0.002, ask=0.0025, delta=-0.60)

    state = MagicMock()
    state.dt   = dt
    state.spot = spot
    state.spot_bars = []

    def _get_option(expiry, strike, is_call):
        if not has_option:
            return None
        if is_call:
            return call_q
        return put_q

    def _get_chain(expiry):
        if not has_chain:
            return []
        return [call_q, put_q]

    def _expiries():
        return [_EXPIRY]

    state.get_option.side_effect  = _get_option
    state.get_chain.side_effect   = _get_chain
    state.expiries.side_effect    = _expiries
    return state


def _make_strategy(mode="BULL", target_delta=0.70, target_dte=11, regime_df=None):
    s = LongGammaWhitelist()
    if regime_df is None:
        regime_df = _make_regime_df(20, entry_bull_idxs=[3], exit_bull_idxs=[8])
    s.set_indicators({"long_gamma_regime": regime_df})
    s.configure({"mode": mode, "target_delta": target_delta, "target_dte": target_dte,
                 "dte_min": 2, "dte_max": 60})
    return s


# ===========================================================================
# 1. Pairing algorithm
# ===========================================================================

class TestPairingAlgorithm:
    def test_fan_out_two_entries_same_exit(self):
        """Two entries before the same exit → both receive that exit."""
        base = pd.Timestamp("2025-03-01 00:00", tz="UTC")
        T = [base + pd.Timedelta(hours=4 * i) for i in range(10)]
        pairs = _pair_first_entry_then_next_exit(
            entries=[T[1], T[2], T[6]],
            exits=[T[4], T[8]],
        )
        assert pairs == [(T[1], T[4]), (T[2], T[4]), (T[6], T[8])]

    def test_exit_not_consumed_by_first_entry(self):
        """The same exit timestamp is re-used by subsequent entries (fan-out)."""
        base = pd.Timestamp("2025-03-01 00:00", tz="UTC")
        T = [base + pd.Timedelta(hours=4 * i) for i in range(6)]
        pairs = _pair_first_entry_then_next_exit(
            entries=[T[0], T[1]],
            exits=[T[3]],
        )
        # Both entries should receive the same exit
        assert len(pairs) == 2
        assert pairs[0] == (T[0], T[3])
        assert pairs[1] == (T[1], T[3])

    def test_entry_with_no_subsequent_exit_dropped(self):
        """Entry that has no exit after it is silently dropped."""
        base = pd.Timestamp("2025-03-01 00:00", tz="UTC")
        T = [base + pd.Timedelta(hours=4 * i) for i in range(5)]
        pairs = _pair_first_entry_then_next_exit(
            entries=[T[3]],
            exits=[T[1]],   # exit is BEFORE entry
        )
        assert pairs == []

    def test_empty_entries(self):
        base = pd.Timestamp("2025-03-01 00:00", tz="UTC")
        T = [base + pd.Timedelta(hours=4 * i) for i in range(3)]
        assert _pair_first_entry_then_next_exit([], T) == []

    def test_empty_exits(self):
        base = pd.Timestamp("2025-03-01 00:00", tz="UTC")
        T = [base + pd.Timedelta(hours=4 * i) for i in range(3)]
        assert _pair_first_entry_then_next_exit(T, []) == []

    def test_pair_signals_bull_smoke(self):
        """pair_signals BULL returns correct pairs from a minimal regime_df."""
        df = _make_regime_df(20, entry_bull_idxs=[2, 4], exit_bull_idxs=[7])
        pairs = pair_signals(df, "BULL")
        assert len(pairs) == 2
        idx = df.index
        assert pairs[0] == (idx[2], idx[7])
        assert pairs[1] == (idx[4], idx[7])

    def test_pair_signals_bear_smoke(self):
        """pair_signals BEAR returns correct pairs from a minimal regime_df."""
        df = _make_regime_df(20, entry_bull_idxs=[], exit_bull_idxs=[],
                             entry_bear_idxs=[3, 5], exit_bear_idxs=[9])
        pairs = pair_signals(df, "BEAR")
        assert len(pairs) == 2
        idx = df.index
        assert pairs[0] == (idx[3], idx[9])
        assert pairs[1] == (idx[5], idx[9])


# ===========================================================================
# 2. Whitelist gate
# ===========================================================================

class TestWhitelistGate:
    def test_bull_armed_never_true_outside_whitelist(self):
        """Wherever bull_armed is True, the bar-close date must be in BULL_WHITELIST_DATES."""
        # Continuous uptrend starting in June 2025 — mostly NOT in bull whitelist.
        start = pd.Timestamp("2025-06-01 00:00", tz="UTC")
        closes = [50_000.0 + i * 5 for i in range(300)]
        df = _make_4h_df(closes, start)
        result = long_gamma_regime(df)
        armed = result[result["bull_armed"]]
        if len(armed) > 0:
            close_dates = (armed.index + pd.Timedelta(hours=4)).normalize().date
            for d in close_dates:
                assert d in BULL_WHITELIST_DATES, (
                    f"bull_armed=True on {d} but that date is not in BULL_WHITELIST_DATES"
                )

    def test_bear_armed_never_true_outside_whitelist(self):
        """Wherever bear_armed is True, the bar-close date must be in BEAR_WHITELIST_DATES."""
        # Continuous downtrend starting in June 2025 — not in bear whitelist.
        start = pd.Timestamp("2025-06-30 00:00", tz="UTC")
        closes = [60_000.0 - i * 5 for i in range(300)]
        df = _make_4h_df(closes, start)
        result = long_gamma_regime(df)
        armed = result[result["bear_armed"]]
        if len(armed) > 0:
            close_dates = (armed.index + pd.Timedelta(hours=4)).normalize().date
            for d in close_dates:
                assert d in BEAR_WHITELIST_DATES, (
                    f"bear_armed=True on {d} but that date is not in BEAR_WHITELIST_DATES"
                )


# ===========================================================================
# 3. Regime gate
# ===========================================================================

class TestRegimeGate:
    def test_bull_armed_false_in_downtrend(self):
        """Downtrend → SMA8 < SMA21 → bull_regime=False → bull_armed always False."""
        # Oct 2024 is in BULL_WHITELIST_DATES; downtrend ensures bull_regime=False.
        start = pd.Timestamp("2024-10-24 00:00", tz="UTC")
        closes = [60_000.0 - i * 10 for i in range(200)]
        df = _make_4h_df(closes, start)
        result = long_gamma_regime(df)
        # In a monotone downtrend SMA8 is always below SMA21
        assert not result["bull_armed"].any(), (
            "Downtrend should produce no bull_armed bars"
        )

    def test_bear_armed_false_in_uptrend(self):
        """Uptrend → SMA20 > SMA50 → bear_regime=False → bear_armed always False."""
        # Dec 2024 is in BEAR_WHITELIST_DATES; uptrend ensures bear_regime=False.
        start = pd.Timestamp("2024-12-17 00:00", tz="UTC")
        closes = [40_000.0 + i * 10 for i in range(200)]
        df = _make_4h_df(closes, start)
        result = long_gamma_regime(df)
        # In a monotone uptrend SMA20 is always above SMA50
        assert not result["bear_armed"].any(), (
            "Uptrend should produce no bear_armed bars"
        )


# ===========================================================================
# 4. SIDEWAYS / invalid mode
# ===========================================================================

class TestModes:
    def test_sideways_returns_empty(self):
        df = _make_regime_df(20, entry_bull_idxs=[2], exit_bull_idxs=[8])
        assert pair_signals(df, "SIDEWAYS") == []

    def test_invalid_mode_raises(self):
        df = _make_regime_df(10, [], [])
        with pytest.raises(ValueError, match="INVALID"):
            pair_signals(df, "INVALID")

    def test_mode_case_insensitive(self):
        df = _make_regime_df(20, entry_bull_idxs=[2], exit_bull_idxs=[8])
        assert pair_signals(df, "bull") == pair_signals(df, "BULL")


# ===========================================================================
# 5. Strategy: open / close protocol
# ===========================================================================

class TestStrategyProtocol:
    # ── Helpers ────────────────────────────────────────────────────────────

    # (no shared helpers needed — use regime_df.index for all timestamps)

    # ── Open ───────────────────────────────────────────────────────────────

    def test_opens_position_on_entry_bar(self):
        """Strategy opens a call position when an entry signal bar closes."""
        # entry at bar index 3 (bar_open = base+4*3h), exit at bar index 8
        regime_df = _make_regime_df(20, entry_bull_idxs=[3], exit_bull_idxs=[8])
        s = _make_strategy(mode="BULL", regime_df=regime_df)

        # At state.dt = idx[4] the bar that just closed is idx[3] (entry bar)
        dt_open = regime_df.index[4].to_pydatetime()
        state = _make_state(dt_open)
        trades = s.on_market_state(state)

        assert trades == [], "No close yet — just opened"
        assert len(s._positions) == 1
        pos = s._positions[0]
        assert pos.metadata["mode"] == "BULL"
        assert pos.legs[0]["is_call"] is True

    def test_opens_put_in_bear_mode(self):
        """Strategy opens a put position when in BEAR mode."""
        regime_df = _make_regime_df(20, entry_bear_idxs=[3], exit_bear_idxs=[8])
        s = _make_strategy(mode="BEAR", target_delta=0.60, regime_df=regime_df)

        dt_open = regime_df.index[4].to_pydatetime()
        state = _make_state(dt_open)
        s.on_market_state(state)

        assert len(s._positions) == 1
        pos = s._positions[0]
        assert pos.metadata["mode"] == "BEAR"
        assert pos.legs[0]["is_call"] is False

    def test_no_position_at_non_4h_tick(self):
        """on_market_state ignores ticks that are not 4h boundaries."""
        regime_df = _make_regime_df(20, entry_bull_idxs=[3], exit_bull_idxs=[8])
        s = _make_strategy(mode="BULL", regime_df=regime_df)

        base_dt = regime_df.index[4].to_pydatetime()
        for off in [1, 30, 59]:
            state = _make_state(base_dt.replace(minute=off))
            trades = s.on_market_state(state)
            assert trades == []
            assert len(s._positions) == 0

    def test_no_chain_skips_entry(self):
        """No qualifying chain → no position opened."""
        regime_df = _make_regime_df(20, entry_bull_idxs=[3], exit_bull_idxs=[8])
        s = _make_strategy(mode="BULL", regime_df=regime_df)

        dt_open = regime_df.index[4].to_pydatetime()
        state = _make_state(dt_open, has_chain=False)
        s.on_market_state(state)

        assert len(s._positions) == 0

    def test_zero_ask_skips_entry(self):
        """ask == 0 on best option → skip silently."""
        regime_df = _make_regime_df(20, entry_bull_idxs=[3], exit_bull_idxs=[8])
        s = _make_strategy(mode="BULL", regime_df=regime_df)

        call_q = _make_quote(_STRIKE_CALL, True, bid=0.0, ask=0.0, delta=0.70)
        put_q  = _make_quote(_STRIKE_PUT,  False, bid=0.0, ask=0.0, delta=-0.60)
        state = _make_state(regime_df.index[4].to_pydatetime())
        state.get_chain.side_effect = lambda _: [call_q, put_q]
        s.on_market_state(state)

        assert len(s._positions) == 0

    # ── Fan-out ────────────────────────────────────────────────────────────

    def test_fan_out_multiple_positions(self):
        """Two entry bars both before the same exit → two simultaneous positions."""
        regime_df = _make_regime_df(20, entry_bull_idxs=[2, 4], exit_bull_idxs=[9])
        s = _make_strategy(mode="BULL", regime_df=regime_df)

        # Process bar 3 (closes bar 2 — first entry)
        s.on_market_state(_make_state(regime_df.index[3].to_pydatetime()))
        assert len(s._positions) == 1

        # Process bar 5 (closes bar 4 — second entry)
        s.on_market_state(_make_state(regime_df.index[5].to_pydatetime()))
        assert len(s._positions) == 2

    # ── Exit ───────────────────────────────────────────────────────────────

    def test_exits_on_trigger(self):
        """Position is closed with reason 'trigger' when exit signal bar closes."""
        regime_df = _make_regime_df(20, entry_bull_idxs=[2], exit_bull_idxs=[7])
        s = _make_strategy(mode="BULL", regime_df=regime_df)

        # Open
        s.on_market_state(_make_state(regime_df.index[3].to_pydatetime()))
        assert len(s._positions) == 1

        # Intermediate ticks — should not close
        for i in range(4, 8):
            trades = s.on_market_state(_make_state(regime_df.index[i].to_pydatetime()))
            assert trades == []

        # Trigger: state.dt = idx[8] (bar 7 just closed → exit signal)
        trades = s.on_market_state(_make_state(regime_df.index[8].to_pydatetime()))
        assert len(trades) == 1
        assert trades[0].exit_reason == "trigger"
        assert len(s._positions) == 0

    def test_exits_at_expiry(self):
        """Position closes with reason 'expiry' when option expiry_dt is reached."""
        regime_df = _make_regime_df(30, entry_bull_idxs=[2], exit_bull_idxs=[25])
        s = _make_strategy(mode="BULL", regime_df=regime_df)

        # Inject a very near-term expiry so it fires before the signal exit
        # Open position
        dt_open = regime_df.index[3].to_pydatetime()
        state_open = _make_state(dt_open)
        s.on_market_state(state_open)
        assert len(s._positions) == 1

        # Manually set expiry_dt to 4h from now (will fire at next 4h tick)
        s._positions[0].metadata["expiry_dt"] = dt_open + timedelta(hours=4)

        # Process next 4h tick — should expire
        dt_expire = regime_df.index[4].to_pydatetime()
        trades = s.on_market_state(_make_state(dt_expire))
        assert len(trades) == 1
        assert trades[0].exit_reason == "expiry"

    def test_on_end_closes_all_positions(self):
        """on_end() closes every open position with 'end_of_data'."""
        regime_df = _make_regime_df(20, entry_bull_idxs=[2, 4], exit_bull_idxs=[18])
        s = _make_strategy(mode="BULL", regime_df=regime_df)

        s.on_market_state(_make_state(regime_df.index[3].to_pydatetime()))
        s.on_market_state(_make_state(regime_df.index[5].to_pydatetime()))
        assert len(s._positions) == 2

        final_state = _make_state(regime_df.index[15].to_pydatetime())
        trades = s.on_end(final_state)
        assert len(trades) == 2
        assert all(t.exit_reason == "end_of_data" for t in trades)
        assert len(s._positions) == 0

    # ── Reset ──────────────────────────────────────────────────────────────

    def test_reset_clears_positions(self):
        regime_df = _make_regime_df(20, entry_bull_idxs=[2], exit_bull_idxs=[8])
        s = _make_strategy(mode="BULL", regime_df=regime_df)
        s.on_market_state(_make_state(regime_df.index[3].to_pydatetime()))
        assert len(s._positions) == 1
        s.reset()
        assert len(s._positions) == 0

    def test_configure_rebuilds_pairs_on_mode_change(self):
        """configure() with a different mode rebuilds the entry_exit_map correctly."""
        regime_df = _make_regime_df(
            20,
            entry_bull_idxs=[2], exit_bull_idxs=[8],
            entry_bear_idxs=[5], exit_bear_idxs=[12],
        )
        s = LongGammaWhitelist()
        s.set_indicators({"long_gamma_regime": regime_df})

        s.configure({"mode": "BULL", "target_delta": 0.70, "target_dte": 11,
                     "dte_min": 2, "dte_max": 60})
        bull_keys = set(s._entry_exit_map.keys())

        s.configure({"mode": "BEAR", "target_delta": 0.60, "target_dte": 21,
                     "dte_min": 2, "dte_max": 60})
        bear_keys = set(s._entry_exit_map.keys())

        assert bull_keys != bear_keys, "BULL and BEAR entry maps should differ"
        assert regime_df.index[2] in bull_keys
        assert regime_df.index[5] in bear_keys

    # ── Fallback close: no market data ────────────────────────────────────

    def test_close_fallback_when_no_option_data(self):
        """When get_option returns None, close at entry_price_usd (flat P&L)."""
        regime_df = _make_regime_df(20, entry_bull_idxs=[2], exit_bull_idxs=[7])
        s = _make_strategy(mode="BULL", regime_df=regime_df)

        s.on_market_state(_make_state(regime_df.index[3].to_pydatetime()))
        assert len(s._positions) == 1
        entry_usd = s._positions[0].entry_price_usd

        exit_state = _make_state(regime_df.index[8].to_pydatetime(), has_option=False)
        trades = s.on_market_state(exit_state)
        assert len(trades) == 1
        # P&L should be ≈ 0 minus fees (flat fallback)
        assert trades[0].exit_price_usd == pytest.approx(entry_usd)


# ===========================================================================
# 6. mode=BOTH — dual-sleeve concurrent operation
# ===========================================================================

class TestBothMode:
    """mode=BOTH runs the BULL and BEAR sleeves simultaneously."""

    def _make_both_strategy(self, regime_df):
        s = LongGammaWhitelist()
        s.set_indicators({"long_gamma_regime": regime_df})
        s.configure({
            "mode": "BOTH",
            "bull_target_delta": 0.70, "bull_target_dte": 11,
            "bear_target_delta": 0.60, "bear_target_dte": 21,
            "dte_min": 2, "dte_max": 60,
        })
        return s

    def test_both_mode_builds_two_maps(self):
        """mode=BOTH populates _bull_map and _bear_map; _entry_exit_map stays empty."""
        regime_df = _make_regime_df(
            20,
            entry_bull_idxs=[2], exit_bull_idxs=[8],
            entry_bear_idxs=[5], exit_bear_idxs=[12],
        )
        s = self._make_both_strategy(regime_df)
        assert len(s._bull_map) == 1
        assert len(s._bear_map) == 1
        assert len(s._entry_exit_map) == 0

    def test_both_opens_call_on_bull_signal(self):
        """mode=BOTH opens a call when the BULL entry bar closes."""
        regime_df = _make_regime_df(
            20,
            entry_bull_idxs=[3], exit_bull_idxs=[10],
            entry_bear_idxs=[6], exit_bear_idxs=[14],
        )
        s = self._make_both_strategy(regime_df)

        # Bar 3 closes at idx[4]
        s.on_market_state(_make_state(regime_df.index[4].to_pydatetime()))

        assert len(s._positions) == 1
        pos = s._positions[0]
        assert pos.metadata["sleeve"] == "BULL"
        assert pos.legs[0]["is_call"] is True

    def test_both_opens_put_on_bear_signal(self):
        """mode=BOTH opens a put when the BEAR entry bar closes."""
        regime_df = _make_regime_df(
            20,
            entry_bull_idxs=[3], exit_bull_idxs=[10],
            entry_bear_idxs=[6], exit_bear_idxs=[14],
        )
        s = self._make_both_strategy(regime_df)

        # Bar 6 closes at idx[7]
        s.on_market_state(_make_state(regime_df.index[7].to_pydatetime()))

        assert len(s._positions) == 1
        pos = s._positions[0]
        assert pos.metadata["sleeve"] == "BEAR"
        assert pos.legs[0]["is_call"] is False

    def test_both_holds_call_and_put_simultaneously(self):
        """mode=BOTH can hold a call and a put at the same time."""
        regime_df = _make_regime_df(
            20,
            entry_bull_idxs=[2], exit_bull_idxs=[12],
            entry_bear_idxs=[4], exit_bear_idxs=[14],
        )
        s = self._make_both_strategy(regime_df)

        # Open BULL at idx[3]
        s.on_market_state(_make_state(regime_df.index[3].to_pydatetime()))
        assert len(s._positions) == 1

        # Open BEAR at idx[5]
        s.on_market_state(_make_state(regime_df.index[5].to_pydatetime()))
        assert len(s._positions) == 2

        sleeves = {p.metadata["sleeve"] for p in s._positions}
        assert sleeves == {"BULL", "BEAR"}

    def test_both_exits_sleeves_independently(self):
        """BULL exit signal closes only the call; BEAR position stays open."""
        regime_df = _make_regime_df(
            25,
            entry_bull_idxs=[2], exit_bull_idxs=[8],
            entry_bear_idxs=[3], exit_bear_idxs=[15],
        )
        s = self._make_both_strategy(regime_df)

        s.on_market_state(_make_state(regime_df.index[3].to_pydatetime()))  # open BULL
        s.on_market_state(_make_state(regime_df.index[4].to_pydatetime()))  # open BEAR
        assert len(s._positions) == 2

        # BULL exit: bar 8 closes at idx[9]
        trades = s.on_market_state(_make_state(regime_df.index[9].to_pydatetime()))
        assert len(trades) == 1
        assert trades[0].exit_reason == "trigger"
        # One position (BEAR) still open
        assert len(s._positions) == 1
        assert s._positions[0].metadata["sleeve"] == "BEAR"

    def test_quant_params_preset(self):
        """QUANT_PARAMS preset has mode=BOTH with correct per-sleeve values."""
        qp = LongGammaWhitelist.QUANT_PARAMS
        assert qp["mode"] == "BOTH"
        assert qp["bull_target_delta"] == 0.70
        assert qp["bull_target_dte"] == 11
        assert qp["bear_target_delta"] == 0.60
        assert qp["bear_target_dte"] == 21


# ===========================================================================
# 7. Reference replay  (requires Binance kline fetch — mark as live)
# ===========================================================================

def _load_reference_csv(path):
    """Load a reference CSV; return list of (entry_ts, exit_ts) as UTC Timestamps."""
    df = pd.read_csv(path)
    pairs = []
    for _, row in df.iterrows():
        entry_ts = pd.Timestamp(row["schedule_entry_bar_utc"]).tz_convert("UTC")
        exit_ts  = pd.Timestamp(row["schedule_exit_bar_utc"]).tz_convert("UTC")
        pairs.append((entry_ts, exit_ts))
    return pairs


@pytest.mark.live
def test_bull_reference_replay():
    """
    Load real 4h BTCUSDT klines, run long_gamma_regime + pair_signals(BULL),
    and verify that at least 5 of the 7 reference bull entries are reproduced.

    Up to 2 mismatches are accepted — these correspond to marginal crossings
    at tight price levels where the indicator result depends on the data feed
    (Binance BTCUSDT vs the original author's source).
    """
    from indicators.hist_data import load_klines

    ref_pairs = _load_reference_csv(_REF_BULL)
    ref_entries = {e for e, _ in ref_pairs}

    start = datetime(2024, 9, 1, tzinfo=timezone.utc)
    end   = datetime(2026, 5, 1, tzinfo=timezone.utc)
    df_4h = load_klines("BTCUSDT", "4h", start, end, warmup_days=60)
    regime = long_gamma_regime(df_4h)
    gen_pairs  = pair_signals(regime, "BULL")
    gen_entries = {e for e, _ in gen_pairs}

    matched  = ref_entries & gen_entries
    n_ref    = len(ref_entries)
    n_missed = n_ref - len(matched)

    assert n_missed <= 2, (
        f"Bull replay: {n_missed}/{n_ref} reference entries not reproduced.\n"
        f"Missing:  {sorted(ref_entries - gen_entries)}\n"
        f"Extra:    {sorted(gen_entries - ref_entries)}"
    )


@pytest.mark.live
def test_bear_reference_replay():
    """
    Load real 4h BTCUSDT klines, run long_gamma_regime + pair_signals(BEAR),
    and verify that at least 36 of the 39 reference bear entries are reproduced.

    Up to 3 mismatches are accepted for the same data-source reasons.
    """
    from indicators.hist_data import load_klines

    ref_pairs = _load_reference_csv(_REF_BEAR)
    ref_entries = {e for e, _ in ref_pairs}

    start = datetime(2024, 10, 1, tzinfo=timezone.utc)
    end   = datetime(2026, 5, 1, tzinfo=timezone.utc)
    df_4h = load_klines("BTCUSDT", "4h", start, end, warmup_days=60)
    regime = long_gamma_regime(df_4h)
    gen_pairs   = pair_signals(regime, "BEAR")
    gen_entries = {e for e, _ in gen_pairs}

    matched  = ref_entries & gen_entries
    n_ref    = len(ref_entries)
    n_missed = n_ref - len(matched)

    assert n_missed <= 3, (
        f"Bear replay: {n_missed}/{n_ref} reference entries not reproduced.\n"
        f"Missing:  {sorted(ref_entries - gen_entries)}\n"
        f"Extra:    {sorted(gen_entries - ref_entries)}"
    )

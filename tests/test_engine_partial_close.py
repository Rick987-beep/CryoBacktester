"""
tests/test_engine_partial_close.py — Phase A of the fills refactor.

Tests the new engine-owned lifecycle API in strategy_base:
  • close_position()  — full close (drop-in for close_trade)
  • partial_close()   — close a subset of legs, keep the rest open
  • add_legs()        — extend an open position with additional legs

Reconciliation invariants:
  • Per-trade: sum(amount_usd) − sum(fee_usd) ≈ trade.pnl
  • Cumulative: across all trades for a position, fills PnL ≈ sum(trade.pnl)
  • Open fills are NOT duplicated for partial closes.
  • Survivor legs' eventual close still links to the original open via open_idx.

Run:
    python -m pytest tests/test_engine_partial_close.py -v
"""
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest


# ---------------------------------------------------------------------------
# Fake replay infrastructure
# ---------------------------------------------------------------------------

def _make_state(dt, spot):
    s = SimpleNamespace(dt=dt, spot=spot)
    s.get_option = lambda expiry, strike, is_call: None
    return s


class _FakeReplay:
    def __init__(self, states):
        self._states = states

    def __len__(self):
        return len(self._states)

    def __iter__(self):
        return iter(self._states)


# ---------------------------------------------------------------------------
# Test 1: close_position is a drop-in for close_trade (no behavior change)
# ---------------------------------------------------------------------------

_SPOT_OPEN  = 70_000.0
_SPOT_CLOSE = 71_000.0
_SHORT_OPEN_BTC = 0.005
_SHORT_CLOSE_BTC = 0.002


class _CloseFullStrategy:
    """One short leg, opens t=1, fully closes via close_position() at t=2."""
    name = "close_full_test"
    PARAM_GRID = {"dummy": [0]}

    def configure(self, params):
        self._position = None
        self._tick = 0

    def reset(self):
        self._position = None
        self._tick = 0

    def on_end(self, state):
        return []

    def on_market_state(self, state):
        self._tick += 1
        if self._tick == 1:
            return self._open(state)
        if self._tick == 2:
            return self._close(state)
        return []

    def _open(self, state):
        from backtester.strategy_base import OpenPosition, Trade
        from backtester.pricing import fee_btc_per_contract

        leg = {
            "strike": 80_000.0, "is_call": True, "expiry": "30JUN26",
            "side": "sell", "price_btc": _SHORT_OPEN_BTC, "qty": 1.0,
        }
        fee_open_usd = fee_btc_per_contract(_SHORT_OPEN_BTC) * state.spot
        entry_usd = _SHORT_OPEN_BTC * state.spot

        pos = OpenPosition(
            entry_time=state.dt, entry_spot=state.spot, legs=[leg],
            entry_price_usd=entry_usd, fees_open=fee_open_usd,
            metadata={"direction": "sell", "pos_id": 1},
        )
        self._position = pos
        return [Trade(
            entry_time=state.dt, exit_time=state.dt,
            entry_spot=state.spot, exit_spot=state.spot,
            entry_price_usd=entry_usd, exit_price_usd=0.0,
            fees=fee_open_usd, pnl=0.0, triggered=False,
            exit_reason="", exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={"direction": "sell", "pos_id": 1, "legs": [leg]},
        )]

    def _close(self, state):
        from backtester.strategy_base import close_position
        from backtester.pricing import fee_btc_per_contract

        pos = self._position
        leg = pos.legs[0]
        leg["exit_price_btc"] = _SHORT_CLOSE_BTC

        exit_usd = _SHORT_CLOSE_BTC * state.spot
        fee_close_usd = fee_btc_per_contract(_SHORT_CLOSE_BTC) * state.spot

        trade = close_position(state, pos, "take_profit",
                               current_usd=exit_usd, fees_close=fee_close_usd)
        self._position = None
        return [trade]


def _run(strat_cls):
    from backtester.engine import run_grid_full
    T = [datetime(2026, 4, 1, 12 + i, 0, tzinfo=timezone.utc)
         for i in range(6)]
    spots = [_SPOT_OPEN, _SPOT_OPEN, _SPOT_CLOSE, _SPOT_CLOSE,
             _SPOT_CLOSE, _SPOT_CLOSE]
    replay = _FakeReplay([_make_state(t, s) for t, s in zip(T, spots)])
    return run_grid_full(strat_cls, {"dummy": [0]}, replay, progress=False)


class TestClosePosition:
    def test_one_close_trade(self):
        df, _, _, _, df_fills = _run(_CloseFullStrategy)
        assert len(df) == 1

    def test_two_fills(self):
        _, _, _, _, df_fills = _run(_CloseFullStrategy)
        assert len(df_fills) == 2
        assert list(df_fills["event"].astype(str)) == ["open", "close"]

    def test_recon(self):
        df, _, _, _, df_fills = _run(_CloseFullStrategy)
        trade_pnl = float(df.iloc[0]["pnl"])
        fills_pnl = float(
            df_fills["amount_usd"].astype("float64").sum()
            - df_fills["fee_usd"].astype("float64").sum()
        )
        assert abs(fills_pnl - trade_pnl) < 0.5

    def test_open_idx_links(self):
        _, _, _, _, df_fills = _run(_CloseFullStrategy)
        open_row  = df_fills[df_fills["event"] == "open"].iloc[0]
        close_row = df_fills[df_fills["event"] == "close"].iloc[0]
        assert int(close_row["open_idx"]) == int(open_row["trade_idx"])


# ---------------------------------------------------------------------------
# Test 2: partial_close — calendar-style two-leg position
# Open long+short at t=1. At t=3 partial_close the short only (long kept).
# At t=5 close_position the survivor (long).
# Expect: 4 fills total, 2 trades, cumulative recon holds.
# ---------------------------------------------------------------------------

_LONG_OPEN_BTC   = 0.020   # long put
_SHORT_OPEN_BTC2 = 0.004   # short put
_SHORT_EXIT_BTC  = 0.001   # short put cheaper at close
_LONG_EXIT_BTC   = 0.022   # long put richer at close


class _CalendarStrategy:
    """Two-leg: long + short. Partial-close short at t=3, close long at t=5."""
    name = "calendar_partial_test"
    PARAM_GRID = {"dummy": [0]}

    def configure(self, params):
        self._position = None
        self._tick = 0

    def reset(self):
        self._position = None
        self._tick = 0

    def on_end(self, state):
        return []

    def on_market_state(self, state):
        self._tick += 1
        if self._tick == 1:
            return self._open(state)
        if self._tick == 3:
            return self._partial_close_short(state)
        if self._tick == 5:
            return self._close_long(state)
        return []

    def _open(self, state):
        from backtester.strategy_base import OpenPosition, Trade
        from backtester.pricing import fee_btc_per_contract

        long_leg = {
            "strike": 60_000.0, "is_call": False, "expiry": "27APR26",
            "side": "buy",  "price_btc": _LONG_OPEN_BTC,   "qty": 1.0,
        }
        short_leg = {
            "strike": 60_000.0, "is_call": False, "expiry": "06APR26",
            "side": "sell", "price_btc": _SHORT_OPEN_BTC2, "qty": 1.0,
        }
        entry_usd = (_LONG_OPEN_BTC + _SHORT_OPEN_BTC2) * state.spot
        fee_open_usd = (
            fee_btc_per_contract(_LONG_OPEN_BTC)
            + fee_btc_per_contract(_SHORT_OPEN_BTC2)
        ) * state.spot

        pos = OpenPosition(
            entry_time=state.dt, entry_spot=state.spot,
            legs=[long_leg, short_leg],
            entry_price_usd=entry_usd, fees_open=fee_open_usd,
            metadata={"direction": "sell", "pos_id": 7},
        )
        self._position = pos
        return [Trade(
            entry_time=state.dt, exit_time=state.dt,
            entry_spot=state.spot, exit_spot=state.spot,
            entry_price_usd=entry_usd, exit_price_usd=0.0,
            fees=fee_open_usd, pnl=0.0, triggered=False,
            exit_reason="", exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={"direction": "sell", "pos_id": 7,
                      "legs": [long_leg, short_leg]},
        )]

    def _partial_close_short(self, state):
        from backtester.strategy_base import partial_close
        from backtester.pricing import fee_btc_per_contract

        pos = self._position
        short_idx = 1
        pos.legs[short_idx]["exit_price_btc"] = _SHORT_EXIT_BTC
        fee_close_usd = fee_btc_per_contract(_SHORT_EXIT_BTC) * state.spot

        trade = partial_close(state, pos, [short_idx], "roll",
                              fees_close=fee_close_usd)
        return [trade]

    def _close_long(self, state):
        from backtester.strategy_base import close_position
        from backtester.pricing import fee_btc_per_contract

        pos = self._position
        leg = pos.legs[0]
        leg["exit_price_btc"] = _LONG_EXIT_BTC

        exit_usd = _LONG_EXIT_BTC * state.spot
        fee_close_usd = fee_btc_per_contract(_LONG_EXIT_BTC) * state.spot

        trade = close_position(state, pos, "end",
                               current_usd=exit_usd, fees_close=fee_close_usd)
        self._position = None
        return [trade]


class TestPartialClose:
    def test_two_closed_trades(self):
        df, _, _, _, _ = _run(_CalendarStrategy)
        assert len(df) == 2

    def test_four_fills_total(self):
        """2 open (at t=1) + 1 close (short at t=3) + 1 close (long at t=5)."""
        _, _, _, _, df_fills = _run(_CalendarStrategy)
        assert len(df_fills) == 4
        events = list(df_fills["event"].astype(str))
        # Order: 2 opens, then close (partial), then close (long)
        assert events.count("open") == 2
        assert events.count("close") == 2

    def test_no_duplicate_opens(self):
        """Partial close must NOT emit phantom open fills for the short leg.

        The short leg's open fill was already emitted at t=1. The partial
        close at t=3 should add a close fill only.
        """
        _, _, _, _, df_fills = _run(_CalendarStrategy)
        # Two distinct contracts, each opened once.
        opens = df_fills[df_fills["event"] == "open"]
        assert len(opens) == 2
        contracts = set(opens["contract"].astype(str))
        assert len(contracts) == 2

    def test_partial_close_links_to_original_open(self):
        """Short-leg close fill (t=3) must point back to t=1's open of the short."""
        _, _, _, _, df_fills = _run(_CalendarStrategy)
        # Find the short-leg open fill and the short-leg close fill.
        # Short is is_call=False, expiry 06APR26 — contract endswith "-P".
        short_open = df_fills[(df_fills["event"] == "open")
                              & (df_fills["contract"].str.contains("06APR26"))].iloc[0]
        short_close = df_fills[(df_fills["event"] == "close")
                               & (df_fills["contract"].str.contains("06APR26"))].iloc[0]
        assert int(short_close["open_idx"]) == int(short_open["trade_idx"])

    def test_survivor_close_links_to_original_open(self):
        """Long-leg close fill (t=5) must point back to t=1's open of the long."""
        _, _, _, _, df_fills = _run(_CalendarStrategy)
        long_open = df_fills[(df_fills["event"] == "open")
                             & (df_fills["contract"].str.contains("27APR26"))].iloc[0]
        long_close = df_fills[(df_fills["event"] == "close")
                              & (df_fills["contract"].str.contains("27APR26"))].iloc[0]
        assert int(long_close["open_idx"]) == int(long_open["trade_idx"])

    def test_cumulative_recon(self):
        """Sum of fills PnL must equal sum of trade PnL across both closes."""
        df, _, _, _, df_fills = _run(_CalendarStrategy)
        trades_pnl = float(df["pnl"].astype("float64").sum())
        fills_pnl = float(
            df_fills["amount_usd"].astype("float64").sum()
            - df_fills["fee_usd"].astype("float64").sum()
        )
        assert abs(fills_pnl - trades_pnl) < 0.5, (
            f"Cumulative fills PnL {fills_pnl:.4f} ≠ trades PnL {trades_pnl:.4f}"
        )

    def test_partial_close_pnl_correct(self):
        """First trade is the partial close of the short leg only.

        Short PnL = entry_usd - exit_usd - (fee_open_alloc + fee_close)
                  = (open_btc - exit_btc) * spot
                    - fee_open_alloc - fee_close
        """
        from backtester.pricing import fee_btc_per_contract

        # Per-leg open fees at the open spot.
        fee_open_long_usd  = fee_btc_per_contract(_LONG_OPEN_BTC)   * _SPOT_OPEN
        fee_open_short_usd = fee_btc_per_contract(_SHORT_OPEN_BTC2) * _SPOT_OPEN
        total_open_fee = fee_open_long_usd + fee_open_short_usd
        # Fees allocated to short by entry-value ratio:
        entry_long_usd  = _LONG_OPEN_BTC   * _SPOT_OPEN
        entry_short_usd = _SHORT_OPEN_BTC2 * _SPOT_OPEN
        short_share = entry_short_usd / (entry_long_usd + entry_short_usd)
        short_fee_open_alloc = total_open_fee * short_share

        fee_close_usd = fee_btc_per_contract(_SHORT_EXIT_BTC) * _SPOT_CLOSE

        expected_short_pnl = (
            _SHORT_OPEN_BTC2 * _SPOT_OPEN
            - _SHORT_EXIT_BTC * _SPOT_CLOSE
            - short_fee_open_alloc
            - fee_close_usd
        )

        df, _, _, _, _ = _run(_CalendarStrategy)
        # First closed trade is the partial close (short).
        actual = float(df.iloc[0]["pnl"])
        assert abs(actual - expected_short_pnl) < 0.5, (
            f"Partial short PnL {actual:.4f} ≠ expected {expected_short_pnl:.4f}"
        )


# ---------------------------------------------------------------------------
# Test 3: add_legs — extend an open position, then close everything.
# Open one short at t=1. At t=2 add a second short via add_legs (+ yield
# side='open' Trade for new leg). At t=4 close_position the whole thing.
# Expect: 3 fills (2 open + 1 combined close... no, full close emits 2 close).
# Actually: 2 open fills (t=1 for first leg, t=2 for added leg) + 2 close
# fills (both legs at t=4) = 4 fills total.
# ---------------------------------------------------------------------------

class _AddLegsStrategy:
    name = "add_legs_test"
    PARAM_GRID = {"dummy": [0]}

    def configure(self, params):
        self._position = None
        self._tick = 0

    def reset(self):
        self._position = None
        self._tick = 0

    def on_end(self, state):
        return []

    def on_market_state(self, state):
        self._tick += 1
        if self._tick == 1:
            return self._open_first(state)
        if self._tick == 2:
            return self._add_second(state)
        if self._tick == 4:
            return self._close_all(state)
        return []

    def _open_first(self, state):
        from backtester.strategy_base import OpenPosition, Trade
        from backtester.pricing import fee_btc_per_contract

        leg = {
            "strike": 80_000.0, "is_call": True, "expiry": "30JUN26",
            "side": "sell", "price_btc": 0.003, "qty": 1.0,
        }
        entry_usd = 0.003 * state.spot
        fee_usd = fee_btc_per_contract(0.003) * state.spot
        pos = OpenPosition(
            entry_time=state.dt, entry_spot=state.spot, legs=[leg],
            entry_price_usd=entry_usd, fees_open=fee_usd,
            metadata={"direction": "sell", "pos_id": 100},
        )
        self._position = pos
        return [Trade(
            entry_time=state.dt, exit_time=state.dt,
            entry_spot=state.spot, exit_spot=state.spot,
            entry_price_usd=entry_usd, exit_price_usd=0.0,
            fees=fee_usd, pnl=0.0, triggered=False,
            exit_reason="", exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={"direction": "sell", "pos_id": 100, "legs": [leg]},
        )]

    def _add_second(self, state):
        from backtester.strategy_base import add_legs, Trade
        from backtester.pricing import fee_btc_per_contract

        pos = self._position
        new_leg = {
            "strike": 90_000.0, "is_call": True, "expiry": "30JUN26",
            "side": "sell", "price_btc": 0.002, "qty": 1.0,
        }
        new_entry_usd = 0.002 * state.spot
        new_fee_usd = fee_btc_per_contract(0.002) * state.spot
        add_legs(pos, [new_leg], new_entry_usd, new_fee_usd)

        # Yield a side='open' Trade for the new leg only, sharing the pos_id.
        return [Trade(
            entry_time=state.dt, exit_time=state.dt,
            entry_spot=state.spot, exit_spot=state.spot,
            entry_price_usd=new_entry_usd, exit_price_usd=0.0,
            fees=new_fee_usd, pnl=0.0, triggered=False,
            exit_reason="", exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={"direction": "sell", "pos_id": 100, "legs": [new_leg]},
        )]

    def _close_all(self, state):
        from backtester.strategy_base import close_position
        from backtester.pricing import fee_btc_per_contract

        pos = self._position
        # Mark both legs' exit_price_btc.
        pos.legs[0]["exit_price_btc"] = 0.001
        pos.legs[1]["exit_price_btc"] = 0.0005

        exit_usd = (0.001 + 0.0005) * state.spot
        fee_close_usd = (
            fee_btc_per_contract(0.001) + fee_btc_per_contract(0.0005)
        ) * state.spot

        trade = close_position(state, pos, "end",
                               current_usd=exit_usd, fees_close=fee_close_usd)
        self._position = None
        return [trade]


class TestAddLegs:
    def test_pos_extended(self):
        """Sanity: after add_legs, pos.legs has two entries with combined aggregates."""
        # Direct unit test, no engine.
        from backtester.strategy_base import OpenPosition, add_legs
        leg1 = {"strike": 80_000.0, "is_call": True, "expiry": "30JUN26",
                "side": "sell", "price_btc": 0.003, "qty": 1.0}
        pos = OpenPosition(
            entry_time=datetime(2026, 4, 1, tzinfo=timezone.utc),
            entry_spot=70_000.0, legs=[leg1],
            entry_price_usd=210.0, fees_open=15.0,
            metadata={"direction": "sell"},
        )
        leg2 = {"strike": 90_000.0, "is_call": True, "expiry": "30JUN26",
                "side": "sell", "price_btc": 0.002, "qty": 1.0}
        add_legs(pos, [leg2], 140.0, 10.0)
        assert len(pos.legs) == 2
        assert pos.entry_price_usd == 350.0
        assert pos.fees_open == 25.0
        assert pos._last_reprice_usd is None

    def test_engine_fills_count(self):
        """2 opens + 2 closes (combined full close)."""
        _, _, _, _, df_fills = _run(_AddLegsStrategy)
        assert len(df_fills) == 4
        assert (df_fills["event"].astype(str) == "open").sum() == 2
        assert (df_fills["event"].astype(str) == "close").sum() == 2

    def test_engine_recon(self):
        df, _, _, _, df_fills = _run(_AddLegsStrategy)
        trades_pnl = float(df["pnl"].astype("float64").sum())
        fills_pnl = float(
            df_fills["amount_usd"].astype("float64").sum()
            - df_fills["fee_usd"].astype("float64").sum()
        )
        assert abs(fills_pnl - trades_pnl) < 0.5


# ---------------------------------------------------------------------------
# Test 4: partial_close API validation
# ---------------------------------------------------------------------------

class TestPartialCloseValidation:
    def _make_two_leg_pos(self):
        from backtester.strategy_base import OpenPosition
        leg1 = {"strike": 80_000.0, "is_call": True, "expiry": "30JUN26",
                "side": "sell", "price_btc": 0.003, "qty": 1.0,
                "exit_price_btc": 0.001}
        leg2 = {"strike": 90_000.0, "is_call": True, "expiry": "30JUN26",
                "side": "sell", "price_btc": 0.002, "qty": 1.0,
                "exit_price_btc": 0.0005}
        pos = OpenPosition(
            entry_time=datetime(2026, 4, 1, tzinfo=timezone.utc),
            entry_spot=70_000.0, legs=[leg1, leg2],
            entry_price_usd=350.0, fees_open=25.0,
            metadata={"direction": "sell", "pos_id": 1},
        )
        return pos

    def _state(self):
        return _make_state(
            datetime(2026, 4, 1, 13, tzinfo=timezone.utc), 71_000.0
        )

    def test_rejects_empty_indices(self):
        from backtester.strategy_base import partial_close
        with pytest.raises(ValueError, match="non-empty"):
            partial_close(self._state(), self._make_two_leg_pos(), [], "test")

    def test_rejects_out_of_range(self):
        from backtester.strategy_base import partial_close
        with pytest.raises(ValueError, match="out of range"):
            partial_close(self._state(), self._make_two_leg_pos(), [2], "test")

    def test_rejects_all_legs(self):
        from backtester.strategy_base import partial_close
        with pytest.raises(ValueError, match="all legs"):
            partial_close(self._state(), self._make_two_leg_pos(), [0, 1], "test")

    def test_mutates_pos_correctly(self):
        from backtester.strategy_base import partial_close
        pos = self._make_two_leg_pos()
        _ = partial_close(self._state(), pos, [1], "test")
        assert len(pos.legs) == 1
        assert pos.legs[0]["strike"] == 80_000.0   # surviving leg is leg1
        # entry_price_usd reduced by closed leg's open value: 0.002 * 70_000 = 140
        assert abs(pos.entry_price_usd - 210.0) < 0.01
        # fees_open reduced proportionally (140 / 350 = 0.4)
        assert abs(pos.fees_open - 25.0 * 0.6) < 0.01
        assert pos._last_reprice_usd is None

    def test_returned_trade_has_partial_close_flag(self):
        from backtester.strategy_base import partial_close
        pos = self._make_two_leg_pos()
        trade = partial_close(self._state(), pos, [1], "test")
        assert trade.metadata.get("partial_close") is True
        assert len(trade.metadata["legs"]) == 1
        assert trade.metadata["legs"][0]["strike"] == 90_000.0

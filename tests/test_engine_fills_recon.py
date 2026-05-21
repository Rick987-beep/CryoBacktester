"""
tests/test_engine_fills_recon.py — Fills ↔ trade PnL reconciliation.

For each closed trade the engine produces, the sum of per-fill USD cash flows
minus per-fill USD fees must equal the trade-level PnL computed by close_position().

Both paths are independent:
  • trade.pnl   — computed by close_position() inside the strategy
  • fills PnL   — computed by _append_fills() inside run_grid_full from
                  leg["price_btc"] / leg["exit_price_btc"] × spot × qty

A mismatch means the two accounting paths have diverged.

This test uses a fully synthetic strategy and fake replay — no parquet data needed.

Run:
    python -m pytest tests/test_engine_fills_recon.py -v
"""
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

# ---------------------------------------------------------------------------
# Known prices for the synthetic trade
# ---------------------------------------------------------------------------

_SPOT_OPEN   = 85_000.0
_SPOT_CLOSE  = 84_000.0
_PRICE_BTC   = 0.002      # option price at entry, BTC per contract
_EXIT_BTC    = 0.001      # option price at close, BTC per contract
_QTY         = 1.0

# ---------------------------------------------------------------------------
# Minimal fake replay
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
# Synthetic single-leg sell strategy
# Opens on tick 0, closes on tick 1 with explicit price_btc annotations.
# ---------------------------------------------------------------------------

class _SingleLegSellStrategy:
    name = "synthetic_recon"
    PARAM_GRID = {"dummy": [0]}

    def configure(self, params):
        self._position = None
        self._tick = 0

    def on_market_state(self, state):
        self._tick += 1
        if self._tick == 1:
            return self._open(state)
        if self._tick == 2:
            return self._close(state)
        return []

    def on_end(self, state):
        return []

    def reset(self):
        self._position = None
        self._tick = 0

    def _open(self, state):
        from backtester.strategy_base import OpenPosition, Trade
        from backtester.pricing import fee_btc_per_contract

        leg = {
            "strike":    90_000.0,
            "is_call":   True,
            "expiry":    "30JUN26",
            "side":      "sell",
            "price_btc": _PRICE_BTC,
            "qty":       _QTY,
        }
        fee_open_btc = fee_btc_per_contract(_PRICE_BTC) * _QTY
        fee_open_usd = fee_open_btc * state.spot
        entry_price_usd = _PRICE_BTC * state.spot * _QTY

        pos = OpenPosition(
            entry_time=state.dt,
            entry_spot=state.spot,
            legs=[leg],
            entry_price_usd=entry_price_usd,
            fees_open=fee_open_usd,
            metadata={"direction": "sell", "pos_id": 42},
        )
        self._position = pos

        open_trade = Trade(
            entry_time=state.dt,
            exit_time=state.dt,
            entry_spot=state.spot,
            exit_spot=state.spot,
            entry_price_usd=entry_price_usd,
            exit_price_usd=0.0,
            fees=fee_open_usd,
            pnl=0.0,
            triggered=False,
            exit_reason="",
            exit_hour=0,
            entry_date=state.dt.strftime("%Y-%m-%d"),
            side="open",
            metadata={"direction": "sell", "pos_id": 42, "legs": [leg]},
        )
        return [open_trade]

    def _close(self, state):
        from backtester.strategy_base import close_position
        from backtester.pricing import fee_btc_per_contract

        pos = self._position
        leg = pos.legs[0]
        leg["exit_price_btc"] = _EXIT_BTC

        fee_close_btc = fee_btc_per_contract(_EXIT_BTC) * _QTY
        fee_close_usd = fee_close_btc * state.spot
        net_exit_usd  = _EXIT_BTC * state.spot * _QTY

        trade = close_position(state, pos, "take_profit", net_exit_usd, fee_close_usd)
        self._position = None
        return [trade]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run():
    """Execute the synthetic strategy through run_grid_full; return (df, df_fills)."""
    from backtester.engine import run_grid_full

    T0 = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    T1 = datetime(2026, 4, 2, 12, 0, tzinfo=timezone.utc)
    replay = _FakeReplay([_make_state(T0, _SPOT_OPEN), _make_state(T1, _SPOT_CLOSE)])

    df, _keys, _nav, _final, df_fills = run_grid_full(
        _SingleLegSellStrategy,
        {"dummy": [0]},
        replay,
        progress=False,
    )
    return df, df_fills


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFillsReconciliation:

    def test_one_closed_trade_produced(self):
        df, _ = _run()
        assert len(df) == 1

    def test_two_fills_produced(self):
        """One open fill (from open Trade) + one close fill (from close Trade)."""
        _, df_fills = _run()
        assert len(df_fills) == 2
        events = list(df_fills["event"].astype(str))
        assert events == ["open", "close"]

    def test_fills_pnl_matches_trade_pnl(self):
        """Core reconciliation: sum(amount_usd) − sum(fee_usd) ≈ trade.pnl.

        Both values are float32 in the DataFrames, so we cast to float64 before
        summing to avoid accumulation errors. Tolerance is 1 cent — Phase A–C
        of the engine-owned fills refactor brought reconciliation to sub-cent
        for both synthetic and real-data backtests.
        """
        df, df_fills = _run()
        trade_pnl = float(df.iloc[0]["pnl"])
        fills_pnl = float(
            df_fills["amount_usd"].astype("float64").sum()
            - df_fills["fee_usd"].astype("float64").sum()
        )
        assert abs(fills_pnl - trade_pnl) < 0.01, (
            f"Fills PnL {fills_pnl:.4f} ≠ trade PnL {trade_pnl:.4f} "
            f"(diff = {fills_pnl - trade_pnl:.6f})"
        )

    def test_fills_pnl_correct_absolute_value(self):
        """Verify fills produce the expected PnL from first principles.

        Expected PnL = premium_received − buyback_cost − fees
                     = (price_btc × spot_open − exit_btc × spot_close) × qty
                       − fee_open_btc × spot_open × qty
                       − fee_close_btc × spot_close × qty
        """
        from backtester.pricing import fee_btc_per_contract

        fee_open_btc  = fee_btc_per_contract(_PRICE_BTC) * _QTY
        fee_close_btc = fee_btc_per_contract(_EXIT_BTC)  * _QTY

        expected_pnl = (
            _PRICE_BTC * _SPOT_OPEN  * _QTY   # premium received
            - _EXIT_BTC * _SPOT_CLOSE * _QTY   # cost to buy back
            - fee_open_btc  * _SPOT_OPEN        # open fee
            - fee_close_btc * _SPOT_CLOSE       # close fee
        )

        _, df_fills = _run()
        fills_pnl = float(
            df_fills["amount_usd"].astype("float64").sum()
            - df_fills["fee_usd"].astype("float64").sum()
        )
        assert abs(fills_pnl - expected_pnl) < 0.01, (
            f"Fills PnL {fills_pnl:.4f} ≠ expected {expected_pnl:.4f}"
        )

    def test_open_fill_side_is_sell(self):
        _, df_fills = _run()
        open_fill = df_fills[df_fills["event"] == "open"].iloc[0]
        assert str(open_fill["side"]) == "sell"

    def test_close_fill_side_is_buy(self):
        _, df_fills = _run()
        close_fill = df_fills[df_fills["event"] == "close"].iloc[0]
        assert str(close_fill["side"]) == "buy"

    def test_fee_usd_positive_for_all_fills(self):
        _, df_fills = _run()
        assert (df_fills["fee_usd"].astype("float64") >= 0).all()

    def test_open_idx_links_close_to_open(self):
        """close_fill.open_idx should equal open_fill.trade_idx."""
        _, df_fills = _run()
        open_fill  = df_fills[df_fills["event"] == "open"].iloc[0]
        close_fill = df_fills[df_fills["event"] == "close"].iloc[0]
        assert int(close_fill["open_idx"]) == int(open_fill["trade_idx"])

    def test_close_fill_open_idx_differs_from_close_trade_idx(self):
        """The close fill must point back to the open trade, not itself."""
        _, df_fills = _run()
        close_fill = df_fills[df_fills["event"] == "close"].iloc[0]
        assert int(close_fill["open_idx"]) != int(close_fill["trade_idx"])

    def test_amount_usd_sign_convention(self):
        """Sell fill → positive amount_usd; buy fill → negative amount_usd."""
        _, df_fills = _run()
        open_fill  = df_fills[df_fills["event"] == "open"].iloc[0]
        close_fill = df_fills[df_fills["event"] == "close"].iloc[0]
        assert float(open_fill["amount_usd"])  > 0   # sell → cash in
        assert float(close_fill["amount_usd"]) < 0   # buy  → cash out

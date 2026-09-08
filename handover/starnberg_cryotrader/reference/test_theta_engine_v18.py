"""Unit tests for ThetaEngineV18 — stops + optional 1:1 wing_pct."""

from datetime import datetime, timezone
from itertools import product
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from backtester.core.strategy_base import OpenPosition
from workspace.strategies.theta_engine.v14 import ENTRY_RICH_FORCE2
from workspace.strategies.theta_engine.v18 import (
    ThetaEngineV18,
    V18_BASE_BOOK,
    V18_BASE_SKEW,
    V18_BASE_V14_HASH,
    V18_BASELINE_FULL2_EQ8,
    V18_BASELINE_FULL2_EQ8_HASH,
    V18_BASELINE_LATE1_EQ5,
    V18_BASELINE_LATE1_EQ5_HASH,
    V18_EQUITY_STOP_GRID,
    V18_MIN_WIDTH_USD_GRID,
    V18_PROX_STOP_GRID,
    V18_STOP_BOOKS,
    V18_STOP_DISCOVERY_GRID,
    V18_STOP_LOSS_GRID,
    V18_WING_PCT_GRID,
    parse_prox_stop,
    parse_stop_book,
    pick_wing_quote_by_pct,
    wing_target_width_usd,
)

UTC = timezone.utc
SPOT = 100_000.0
CALL_STRIKE = 105_000.0


def _params(**overrides):
    base = {
        "book": V18_BASE_BOOK,
        "skew_source": V18_BASE_SKEW,
        "dte": 1,
        "delta": 0.25,
        "skew_mode": "cheaper",
        "qty_per_1btc_equity": 2.0,
        "late_exit": "none",
        "take_profit_pct": 0,
        "stop_loss_pct": 0,
        "max_concurrent": 1,
        "hold_days": 0,
        "front_vrp_min": 0.0,
        "weekend_cover": "skip",
        "rich_mode": "none",
        "prox_stop": "none",
        "open_pnl_equity_stop_pct": 0,
        "wing_pct": 0,
        "min_width_usd": 0,
    }
    base.update(overrides)
    return base


def _strategy(**params):
    s = ThetaEngineV18()
    s.configure(_params(**params))
    return s


class _FakeState:
    def __init__(
        self, dt, equity_usd=100_000.0, spot=SPOT, chain=None, options=None,
    ):
        self.dt = dt
        self.spot = spot
        self.equity_usd = equity_usd
        self.nav_usd = equity_usd
        self._chain = chain or []
        # (expiry, strike, is_call) -> quote; default None = missing row
        self._options = options or {}

    def get_option(self, expiry, strike, is_call):
        return self._options.get((expiry, float(strike), bool(is_call)))

    def get_chain(self, expiry):
        return list(self._chain)


def _quote(strike, is_call=True, ask=0.002, bid=0.001, delta=0.15):
    return SimpleNamespace(
        strike=strike,
        is_call=is_call,
        ask=ask,
        bid=bid,
        mark=ask,
        mark_iv=40.0,
        delta=delta,
    )


def _short_pos(t0, qty=2.0, strike=CALL_STRIKE, credit_usd=400.0):
    mark = credit_usd / (SPOT * qty)
    return OpenPosition(
        entry_time=t0,
        entry_spot=SPOT,
        legs=[
            {
                "strike": strike, "is_call": True, "expiry": "28MAY26",
                "side": "sell", "qty": qty,
                "price_btc": mark, "entry_price": mark,
                "entry_price_usd": credit_usd / qty, "entry_spot": SPOT,
                "entry_iv": 40.0, "entry_delta": 0.25,
                "fee_usd_open": 0.0,
            },
        ],
        entry_price_usd=credit_usd,
        fees_open=0.0,
        metadata={
            "direction": "sell",
            "leg_type": "call",
            "expiry": "28MAY26",
            "expiry_dt": datetime(2026, 5, 28, 8, 0, tzinfo=UTC),
            "call_strike": strike,
            "qty": qty,
            "net_credit_usd": credit_usd,
            "pos_id": 1,
        },
    )


class TestParamGridAndCatalog:
    def test_param_grid_is_baselines_times_wing(self):
        g = ThetaEngineV18.PARAM_GRID
        assert g["book"] == [V18_BASE_BOOK]
        assert g["stop_book"] == [
            V18_BASELINE_LATE1_EQ5,
            V18_BASELINE_FULL2_EQ8,
        ]
        assert g["wing_pct"] == list(V18_WING_PCT_GRID)
        assert g["min_width_usd"] == list(V18_MIN_WIDTH_USD_GRID)
        assert 0.0 in g["wing_pct"]
        assert 0.0 in g["min_width_usd"]
        n = 1
        for v in g.values():
            n *= len(v)
        assert n == 2 * len(V18_WING_PCT_GRID) * len(V18_MIN_WIDTH_USD_GRID)
        assert len(list(product(*g.values()))) == n

    def test_discovery_artefact_is_design_a_252(self):
        g = V18_STOP_DISCOVERY_GRID
        assert g["stop_loss_pct"] == list(V18_STOP_LOSS_GRID)
        assert g["prox_stop"] == list(V18_PROX_STOP_GRID)
        assert g["open_pnl_equity_stop_pct"] == list(V18_EQUITY_STOP_GRID)
        n = 1
        for v in g.values():
            n *= len(v)
        assert n == 252

    def test_baselines_match_run_742_favourites(self):
        assert V18_BASE_V14_HASH == "c8573c839903"
        assert V18_BASELINE_LATE1_EQ5_HASH == "d550e3296f17"
        assert V18_BASELINE_FULL2_EQ8_HASH == "731b1d03b15d"

        late = _strategy(stop_book=V18_BASELINE_LATE1_EQ5)
        assert late._entry_policy == ENTRY_RICH_FORCE2
        assert late._prox_stop_pct == 1.0
        assert late._prox_hours == 4.0
        assert late._open_pnl_equity_stop_pct == 5.0
        assert late._wing_pct == 0.0

        full = _strategy(stop_book=V18_BASELINE_FULL2_EQ8, wing_pct=0.10)
        assert full._prox_stop_pct == 2.0
        assert full._prox_hours == 16.0
        assert full._open_pnl_equity_stop_pct == 8.0
        assert full._wing_pct == 0.10

    def test_catalog_id(self):
        from workspace.catalog import SPECS
        assert "theta_engine_v18" in SPECS
        assert SPECS["theta_engine_v18"].cls is ThetaEngineV18
        assert ThetaEngineV18().name == "theta_engine_v18"


class TestProxParse:
    def test_none(self):
        assert parse_prox_stop("none") == (0.0, 0.0)

    def test_packed(self):
        assert parse_prox_stop("1.0@16") == (1.0, 16.0)
        assert parse_prox_stop("1@4") == (1.0, 4.0)

    def test_bad(self):
        with pytest.raises(ValueError):
            parse_prox_stop("1.0")
        with pytest.raises(ValueError):
            parse_prox_stop("0@16")

    def test_stop_book_parse(self):
        spec = parse_stop_book(V18_BASELINE_LATE1_EQ5)
        assert spec["hash"] == V18_BASELINE_LATE1_EQ5_HASH
        assert set(V18_STOP_BOOKS) == {
            V18_BASELINE_LATE1_EQ5,
            V18_BASELINE_FULL2_EQ8,
        }


class TestWingPick:
    def test_target_width(self):
        assert wing_target_width_usd(100_000.0, 0.05) == 5_000.0

    def test_picks_nearest_further_otm(self):
        quotes = [
            _quote(106_000, ask=0.003),
            _quote(110_000, ask=0.0015),  # target at 105k * 0.05 = 5.25k → 110.25k
            _quote(112_000, ask=0.001),
            _quote(100_000, ask=0.01),  # not further OTM
        ]
        picked = pick_wing_quote_by_pct(quotes, True, CALL_STRIKE, 0.05)
        assert picked is not None
        assert picked.strike == 110_000

    def test_min_width_filters_narrow_outers(self):
        quotes = [
            _quote(106_000, ask=0.003),  # width 1k — below 2500 floor
            _quote(110_000, ask=0.0015),  # width 5k
        ]
        assert pick_wing_quote_by_pct(
            quotes, True, CALL_STRIKE, 0.05, min_width_usd=2500.0,
        ).strike == 110_000
        assert pick_wing_quote_by_pct(
            quotes, True, CALL_STRIKE, 0.01, min_width_usd=2500.0,
        ).strike == 110_000  # target near 106k but floor skips it

    def test_no_candidates(self):
        quotes = [_quote(100_000, ask=0.01)]
        assert pick_wing_quote_by_pct(quotes, True, CALL_STRIKE, 0.05) is None

    def test_min_width_rejects_all(self):
        quotes = [_quote(106_000, ask=0.003)]  # width 1k
        assert pick_wing_quote_by_pct(
            quotes, True, CALL_STRIKE, 0.05, min_width_usd=5000.0,
        ) is None



class TestExitWiring:
    def test_credit_sl_arms_exit_cond(self):
        s = _strategy(stop_loss_pct=1.5)
        assert s._sl_pct == 1.5
        assert len(s._exit_conds) == 1

    def test_prox_arms_exit_cond(self):
        s = _strategy(prox_stop="1.0@16")
        assert len(s._exit_conds) == 1

    def test_naked_has_no_exit_conds(self):
        s = _strategy()
        assert s._exit_conds == []

    def test_baseline_arms_prox_only(self):
        s = _strategy(stop_book=V18_BASELINE_LATE1_EQ5)
        assert len(s._exit_conds) == 1


class TestWingAttach:
    def test_wing_pct_zero_default(self):
        assert _strategy(wing_pct=0)._wing_pct == 0.0

    def test_attach_equal_qty_same_expiry(self):
        s = _strategy(wing_pct=0.05)
        t0 = datetime(2026, 2, 2, 16, 0, tzinfo=UTC)
        pos = _short_pos(t0, qty=2.0, credit_usd=800.0)
        # Wing ask cheap enough that net credit stays positive.
        wing = _quote(110_000, ask=0.001)  # $100/contract → $200 debit
        state = _FakeState(t0, chain=[wing])
        assert s._attach_wing(state, pos) is True
        assert pos.metadata["wing_attached"] is True
        assert len(pos.legs) == 2
        assert pos.legs[1]["side"] == "buy"
        assert pos.legs[1]["qty"] == 2.0
        assert pos.legs[1]["expiry"] == "28MAY26"
        assert pos.metadata["wing_strike"] == 110_000
        assert pos.metadata["net_credit_usd"] == pytest.approx(600.0)

    def test_attach_fails_without_outer(self):
        s = _strategy(wing_pct=0.05)
        t0 = datetime(2026, 2, 2, 16, 0, tzinfo=UTC)
        pos = _short_pos(t0)
        state = _FakeState(t0, chain=[])
        assert s._attach_wing(state, pos) is False
        assert len(pos.legs) == 1

    def test_open_single_rolls_back_when_no_wing(self):
        s = _strategy(wing_pct=0.05)
        t0 = datetime(2026, 2, 2, 16, 0, tzinfo=UTC)
        state = _FakeState(t0, chain=[])
        s._last_trade_date = None
        s._last_open_date = None
        s._pos_counter = 0

        def _parent_open(
            self, state, expiry, exp_dt, chain, is_call, quantity,
            call_bid=0.0, put_bid=0.0, meta_extra=None,
        ):
            p = _short_pos(state.dt)
            self._positions.append(p)
            self._last_trade_date = state.dt.date()
            self._last_open_date = state.dt.date()
            self._pos_counter = 1
            return SimpleNamespace(
                metadata={"legs": p.legs}, fees=0.0, entry_price_usd=400.0,
            )

        with patch(
            "workspace.strategies.theta_engine.v17.ThetaEngineV17._open_single",
            _parent_open,
        ):
            out = s._open_single(state, "28MAY26", t0, [], True, 2.0)
        assert out is None
        assert s._positions == []
        assert s._last_trade_date is None
        assert s._pos_counter == 0


class TestQuotesAvailable:
    def test_naked_requires_short_quote(self):
        s = _strategy()
        t0 = datetime(2026, 2, 2, 16, 0, tzinfo=UTC)
        pos = _short_pos(t0)
        assert s._quotes_available(_FakeState(t0), pos) is False
        q = _quote(CALL_STRIKE)
        state = _FakeState(
            t0, options={("28MAY26", float(CALL_STRIKE), True): q},
        )
        assert s._quotes_available(state, pos) is True

    def test_winged_requires_both_legs(self):
        s = _strategy(wing_pct=0.05)
        t0 = datetime(2026, 2, 2, 16, 0, tzinfo=UTC)
        pos = _short_pos(t0, qty=2.0, credit_usd=800.0)
        wing = _quote(110_000, ask=0.001)
        assert s._attach_wing(_FakeState(t0, chain=[wing]), pos) is True
        short_q = _quote(CALL_STRIKE)
        # Short only — wing missing
        state_short = _FakeState(
            t0, options={("28MAY26", float(CALL_STRIKE), True): short_q},
        )
        assert s._quotes_available(state_short, pos) is False
        # Both legs present
        state_both = _FakeState(
            t0,
            options={
                ("28MAY26", float(CALL_STRIKE), True): short_q,
                ("28MAY26", 110_000.0, True): wing,
            },
        )
        assert s._quotes_available(state_both, pos) is True


class TestEquityStop:
    def test_off_never_hits(self):
        s = _strategy(open_pnl_equity_stop_pct=0)
        t0 = datetime(2026, 2, 2, 16, 0, tzinfo=UTC)
        s._positions = [_short_pos(t0)]
        with patch(
            "workspace.strategies.theta_engine.v18.position_unrealized_pnl",
            return_value=-50_000.0,
        ):
            assert s._open_pnl_equity_stop_hit(_FakeState(t0)) is False

    def test_hits_when_loss_exceeds_threshold(self):
        s = _strategy(open_pnl_equity_stop_pct=5)
        t0 = datetime(2026, 2, 2, 16, 0, tzinfo=UTC)
        s._positions = [_short_pos(t0)]
        state = _FakeState(t0, equity_usd=100_000.0)
        with patch(
            "workspace.strategies.theta_engine.v18.position_unrealized_pnl",
            return_value=-6_000.0,
        ):
            assert s._open_pnl_equity_stop_hit(state) is True

    def test_on_market_state_closes_on_equity_stop(self):
        s = _strategy(open_pnl_equity_stop_pct=5)
        t0 = datetime(2026, 2, 2, 16, 0, tzinfo=UTC)
        s._positions = [_short_pos(t0)]
        state = _FakeState(t0)

        def _close(state, pos, reason):
            return SimpleNamespace(exit_reason=reason, metadata={})

        with patch.object(s, "_open_pnl_equity_stop_hit", return_value=True):
            with patch.object(s, "_all_open_quotes_available", return_value=True):
                with patch.object(s, "_do_close", side_effect=_close) as mocked:
                    with patch.object(s, "_should_enter", return_value=False):
                        with patch.object(s, "_meter_investor_greeks"):
                            trades = s.on_market_state(state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "open_pnl_equity_stop"
        assert s._positions == []
        mocked.assert_called_once()

    def test_equity_stop_suppressed_when_quotes_missing(self):
        s = _strategy(open_pnl_equity_stop_pct=5)
        t0 = datetime(2026, 2, 2, 16, 0, tzinfo=UTC)
        pos = _short_pos(t0)
        s._positions = [pos]
        state = _FakeState(t0)  # get_option → None

        with patch.object(s, "_open_pnl_equity_stop_hit", return_value=True):
            with patch.object(s, "_do_close") as mocked:
                with patch.object(s, "_should_enter", return_value=False):
                    with patch.object(s, "_meter_investor_greeks"):
                        trades = s.on_market_state(state)
        assert trades == []
        assert s._positions == [pos]
        mocked.assert_not_called()

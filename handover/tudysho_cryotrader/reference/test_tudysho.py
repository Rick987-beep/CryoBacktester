"""Unit tests for TuDySho day-of-week entry gating and exit polish."""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from backtester.strategy_base import OpenPosition
from backtester.strategies.tudysho import TuDySho


UTC = timezone.utc


def _strategy(**overrides):
    base = {
        "leg_type": "strangle",
        "dte": 1,
        "delta": 0.05,
        "entry_time": "14:00",
        "stop_loss_pct": 4.5,
        "trade_monday": 1,
        "trade_tuesday": 1,
        "trade_wednesday": 1,
        "trade_thursday": 1,
        "trade_friday": 1,
        "trade_saturday": 0,
        "trade_sunday": 0,
        "min_otm_pct": 0,
        "turbulence_threshold": 60,
        "nav_premium_pct": 0.8,
        "max_qty_per_1btc_equity": 20,
        "leg_min_price": 0,
        "proximity_stop_hours": 4,
        "proximity_buffer_usd": 0,
        "premium_sl_except_final_hours": 4,
        "equity_drawdown_stop_pct": 0,
        "equity_sl_only_final_hours": 0,
        "equity_sl_except_final_hours": 0,
    }
    base.update(overrides)
    s = TuDySho()
    s.configure(base)
    return s


class TestEntryDayAllowed:
    @pytest.mark.parametrize(
        "weekday,key",
        [
            (0, "trade_monday"),
            (1, "trade_tuesday"),
            (2, "trade_wednesday"),
            (3, "trade_thursday"),
            (4, "trade_friday"),
            (5, "trade_saturday"),
            (6, "trade_sunday"),
        ],
    )
    @pytest.mark.parametrize("flag,expected", [(1, True), (0, False)])
    def test_weekday_toggle(self, weekday, key, flag, expected):
        s = _strategy(**{key: flag})
        assert s._entry_day_allowed(weekday) is expected

    def test_defaults_match_legacy_skip_weekends(self):
        """Default grid blocks Sat/Sun but allows Mon–Fri."""
        s = _strategy()
        for wd in range(5):
            assert s._entry_day_allowed(wd) is True
        assert s._entry_day_allowed(5) is False  # Saturday
        assert s._entry_day_allowed(6) is False  # Sunday

    def test_all_weekend_days_enabled(self):
        s = _strategy(trade_friday=1, trade_saturday=1, trade_sunday=1)
        for wd in (4, 5, 6):
            assert s._entry_day_allowed(wd) is True

    def test_tue_thu_only(self):
        s = _strategy(
            trade_monday=0,
            trade_tuesday=1,
            trade_wednesday=0,
            trade_thursday=1,
            trade_friday=0,
            trade_saturday=0,
            trade_sunday=0,
        )
        assert s._entry_day_allowed(0) is False
        assert s._entry_day_allowed(1) is True
        assert s._entry_day_allowed(2) is False
        assert s._entry_day_allowed(3) is True
        assert s._entry_day_allowed(4) is False


class TestEntryDayDates:
    """Sanity-check weekday ints against real calendar dates."""

    def test_saturday_blocked_by_default(self):
        s = _strategy()
        # 2024-10-26 is a Saturday
        assert datetime(2024, 10, 26, 12, 0, tzinfo=UTC).weekday() == 5
        assert s._entry_day_allowed(5) is False

    def test_saturday_allowed_when_enabled(self):
        s = _strategy(trade_saturday=1)
        assert s._entry_day_allowed(5) is True


class TestExitCondWiring:
    def test_default_has_proximity_and_premium_stops(self):
        s = _strategy()
        assert s._proximity_stop_hours == 4.0
        assert s._premium_sl_except_final_hours == 4.0
        assert len(s._exit_conds) == 2  # proximity + premium

    def test_proximity_disabled_leaves_premium_only(self):
        s = _strategy(proximity_stop_hours=0)
        assert len(s._exit_conds) == 1

    def test_premium_disabled_leaves_proximity_only(self):
        s = _strategy(stop_loss_pct=0)
        assert len(s._exit_conds) == 1

    def test_all_stops_disabled(self):
        s = _strategy(stop_loss_pct=0, proximity_stop_hours=0, equity_drawdown_stop_pct=0)
        assert len(s._exit_conds) == 0

    def test_reset_rebuilds_exit_conds(self):
        s = _strategy()
        assert len(s._exit_conds) == 2
        s._exit_conds = []
        s.reset()
        assert len(s._exit_conds) == 2

    def test_equity_drawdown_adds_third_stop_cond(self):
        s = _strategy(equity_drawdown_stop_pct=0.05)
        assert len(s._exit_conds) == 3  # proximity + equity + premium


def _strangle_pos(call_strike=76000.0, put_strike=74000.0, quantity=1.0):
    expiry = "28MAY26"
    legs = [
        {"strike": call_strike, "is_call": True, "expiry": expiry, "side": "sell",
         "qty": quantity, "price_btc": 0.001, "entry_price": 0.001,
         "entry_price_usd": 75.0, "entry_spot": 75000.0,
         "entry_bid": 0.001, "entry_ask": 0.0012, "entry_mark": 0.0011,
         "entry_iv": 35.0, "entry_delta": 0.05, "fee_usd_open": 1.0},
        {"strike": put_strike, "is_call": False, "expiry": expiry, "side": "sell",
         "qty": quantity, "price_btc": 0.001, "entry_price": 0.001,
         "entry_price_usd": 75.0, "entry_spot": 75000.0,
         "entry_bid": 0.001, "entry_ask": 0.0012, "entry_mark": 0.0011,
         "entry_iv": 35.0, "entry_delta": -0.05, "fee_usd_open": 1.0},
    ]
    return OpenPosition(
        entry_time=datetime(2026, 5, 27, 14, 0, tzinfo=UTC),
        entry_spot=75000.0,
        legs=legs,
        entry_price_usd=150.0 * quantity,
        fees_open=2.0 * quantity,
        metadata={
            "leg_type": "strangle",
            "direction": "sell",
            "expiry": expiry,
            "expiry_dt": datetime(2026, 5, 28, 8, 0, tzinfo=UTC),
            "call_strike": call_strike,
            "put_strike": put_strike,
            "quantity": quantity,
            "pos_id": 1,
        },
    )


class TestEquityDrawdownExit:
    def test_fires_via_check_exit(self):
        s = _strategy(
            equity_drawdown_stop_pct=0.06,
            stop_loss_pct=0,
            proximity_stop_hours=0,
        )
        pos = _strangle_pos()
        pos.metadata["equity_at_entry_usd"] = 100_000.0
        state = SimpleNamespace(
            dt=datetime(2026, 5, 27, 20, 0, tzinfo=UTC),
            spot=75_000.0,
            equity_usd=100_000.0,
            nav_usd=94_000.0,
            get_option=lambda *args: SimpleNamespace(
                bid=0.001, ask=0.0012, mark=0.041, spot=75_000.0,
                bid_usd=75.0, ask_usd=90.0, mark_usd=3075.0,
            ),
        )
        assert s._check_exit(state, pos) == "equity_drawdown_stop"


class TestExitNearExpiry:
    def test_premium_suppressed_inside_except_final_window(self):
        """premium_sl_except_final_hours turns premium SL off near expiry."""
        s = _strategy(stop_loss_pct=4.5, premium_sl_except_final_hours=4)
        pos = _strangle_pos()
        # Inside 4h window; marks would trigger premium SL if not suppressed
        state = SimpleNamespace(
            dt=datetime(2026, 5, 28, 6, 0, tzinfo=UTC),
            spot=75_000.0,
            get_option=lambda *args: SimpleNamespace(
                bid=0.001, ask=0.0012, mark=0.041, spot=75_000.0,
                bid_usd=75.0, ask_usd=90.0, mark_usd=3075.0,
            ),
        )
        assert s._check_exit(state, pos) is None

    def test_premium_can_fire_when_except_window_zero(self):
        s = _strategy(
            stop_loss_pct=4.5,
            proximity_stop_hours=0,
            premium_sl_except_final_hours=0,
        )
        pos = _strangle_pos()
        state = SimpleNamespace(
            dt=datetime(2026, 5, 28, 6, 0, tzinfo=UTC),
            spot=75_000.0,
            get_option=lambda *args: SimpleNamespace(
                bid=0.001, ask=0.0012, mark=0.041, spot=75_000.0,
                bid_usd=75.0, ask_usd=90.0, mark_usd=3075.0,
            ),
        )
        assert s._check_exit(state, pos) == "stop_loss"


class TestProximityDataGap:
    def test_proximity_close_without_option_quotes(self):
        s = _strategy()
        s._positions = [_strangle_pos()]
        state = SimpleNamespace(
            dt=datetime(2026, 5, 28, 6, 0, tzinfo=UTC),
            spot=73900.0,
            get_option=lambda *args: None,
        )
        trades = s.on_market_state(state)
        assert len(trades) == 1
        assert trades[0].side == "close"
        assert trades[0].exit_reason == "strike_proximity_stop"
        assert trades[0].metadata["proximity_stop_hours"] == 4.0
        assert trades[0].metadata["proximity_buffer_usd"] == 0.0

    def test_stop_loss_suppressed_when_quotes_missing(self):
        s = _strategy(proximity_stop_hours=0)
        s._positions = [_strangle_pos()]
        s._exit_conds = [lambda state, pos: "stop_loss"]
        state = SimpleNamespace(
            dt=datetime(2026, 5, 27, 20, 0, tzinfo=UTC),
            spot=75000.0,
            get_option=lambda *args: None,
        )
        s._last_trade_date = state.dt.date()
        assert s.on_market_state(state) == []


class TestNavPremiumSizing:
    def _state(self, nav_usd, equity_usd, spot):
        return SimpleNamespace(nav_usd=nav_usd, equity_usd=equity_usd, spot=spot)

    def test_target_premium_from_nav(self):
        s = _strategy(nav_premium_pct=0.8, max_qty_per_1btc_equity=20)
        state = self._state(100_000.0, 100_000.0, 100_000.0)
        qty, meta = s._compute_quantity(state, premium_usd_per_contract=100.0)
        assert qty == 8.0
        assert meta["target_premium_usd"] == 800.0
        assert meta["qty_from_premium"] == 8.0
        assert meta["equity_btc_at_entry"] == 1.0
        assert meta["max_contracts_applied"] == 20.0
        assert meta["premium_capped"] is False

    def test_contract_cap_on_cheap_premium(self):
        s = _strategy(nav_premium_pct=0.8, max_qty_per_1btc_equity=20)
        state = self._state(100_000.0, 100_000.0, 100_000.0)
        # target $800 / $7.50 ≈ 106.7 → capped at 1.0 BTC equity × 20
        qty, meta = s._compute_quantity(state, premium_usd_per_contract=7.5)
        assert qty == 20.0
        assert meta["premium_capped"] is True
        assert meta["max_contracts_applied"] == 20.0

    def test_max_contracts_scales_with_equity_btc(self):
        s = _strategy(nav_premium_pct=0.8, max_qty_per_1btc_equity=20)
        # Half the BTC equity → half the contract cap
        state = self._state(50_000.0, 50_000.0, 100_000.0)
        _, meta = s._compute_quantity(state, premium_usd_per_contract=7.5)
        assert meta["equity_btc_at_entry"] == 0.5
        assert meta["max_contracts_applied"] == 10.0

    def test_max_contracts_scales_with_spot(self):
        s = _strategy(nav_premium_pct=0.8, max_qty_per_1btc_equity=20)
        # Same USD equity, lower spot → more BTC equity → higher cap
        state = self._state(100_000.0, 100_000.0, 50_000.0)
        _, meta = s._compute_quantity(state, premium_usd_per_contract=7.5)
        assert meta["equity_btc_at_entry"] == 2.0
        assert meta["max_contracts_applied"] == 40.0

    def test_zero_cap_param_means_uncapped(self):
        s = _strategy(nav_premium_pct=0.8, max_qty_per_1btc_equity=0)
        state = self._state(100_000.0, 100_000.0, 100_000.0)
        qty, meta = s._compute_quantity(state, premium_usd_per_contract=7.5)
        assert qty == 106.7
        assert meta["max_contracts_applied"] is None
        assert meta["premium_capped"] is False

    def test_nav_premium_pct_zero_fixed_qty(self):
        s = _strategy(nav_premium_pct=0)
        state = self._state(100_000.0, 100_000.0, 100_000.0)
        qty, _ = s._compute_quantity(state, premium_usd_per_contract=50.0)
        assert qty == 1.0

    def test_skips_when_nav_non_positive(self):
        s = _strategy()
        state = self._state(0.0, 100_000.0, 100_000.0)
        assert s._compute_quantity(state, premium_usd_per_contract=50.0) is None

    def test_skips_when_spot_non_positive(self):
        s = _strategy()
        state = self._state(100_000.0, 100_000.0, 0.0)
        assert s._compute_quantity(state, premium_usd_per_contract=50.0) is None

    def test_skips_when_cap_would_be_below_min_qty(self):
        s = _strategy(max_qty_per_1btc_equity=20)
        # equity_btc=0.004 → max_contracts=0.08 < 0.1 floor
        state = self._state(100_000.0, 400.0, 100_000.0)
        assert s._compute_quantity(state, premium_usd_per_contract=50.0) is None

    def test_falls_back_to_account_size_without_state_fields(self):
        s = _strategy(nav_premium_pct=0.8, max_qty_per_1btc_equity=20)
        from backtester.config import cfg
        state = SimpleNamespace(spot=100_000.0)
        qty, meta = s._compute_quantity(state, premium_usd_per_contract=100.0)
        acct = cfg.simulation.account_size_usd
        expected_target = acct * 0.008
        assert meta["target_premium_usd"] == expected_target
        assert meta["equity_usd_at_entry"] == acct
        assert meta["max_contracts_applied"] == (acct / 100_000.0) * 20.0
        assert qty == round(min(expected_target / 100.0, meta["max_contracts_applied"]), 1)

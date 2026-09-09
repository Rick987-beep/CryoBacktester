"""Unit tests for TuDySho Monopteros (master three-schedule combo)."""

from datetime import datetime, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from backtester.core.strategy_base import OpenPosition
from workspace.strategies.tudysho.monopteros import (
    RELEASE_NAME,
    TuDyShoMonopteros,
    in_watch_window,
    resolve_schedule,
    _SCHEDULE_DEFAULTS,
    _build_exit_conds,
)


UTC = timezone.utc
NYC = ZoneInfo("America/New_York")


def _nyc(year, month, day, hour, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=NYC)


def _strategy(**overrides):
    base = {
        "nav_premium_pct": 0.5,
        "max_qty_per_1btc_equity": 6,
        "leg_min_price": 0,
        "equity_drawdown_stop_pct": 5,
        "equity_sl_only_final_hours": 0,
        "equity_sl_except_final_hours": 0,
    }
    base.update(overrides)
    s = TuDyShoMonopteros()
    s.configure(base)
    return s


def _strangle_pos(schedule_id="mon_thu", call_strike=76000.0, put_strike=74000.0,
                  quantity=1.0):
    sched = dict(_SCHEDULE_DEFAULTS[schedule_id])
    sched["schedule_id"] = schedule_id
    expiry = "28MAY26"
    legs = [
        {"strike": call_strike, "is_call": True, "expiry": expiry, "side": "sell",
         "qty": quantity, "price_btc": 0.001, "entry_price": 0.001,
         "entry_price_usd": 75.0, "entry_spot": 75000.0,
         "entry_bid": 0.001, "entry_ask": 0.0012, "entry_mark": 0.0011,
         "entry_iv": 35.0, "entry_delta": 0.1, "fee_usd_open": 1.0},
        {"strike": put_strike, "is_call": False, "expiry": expiry, "side": "sell",
         "qty": quantity, "price_btc": 0.001, "entry_price": 0.001,
         "entry_price_usd": 75.0, "entry_spot": 75000.0,
         "entry_bid": 0.001, "entry_ask": 0.0012, "entry_mark": 0.0011,
         "entry_iv": 35.0, "entry_delta": -0.1, "fee_usd_open": 1.0},
    ]
    return OpenPosition(
        entry_time=datetime(2026, 5, 27, 20, 0, tzinfo=UTC),
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
            "schedule_id": schedule_id,
            "schedule_params": sched,
            "dte": sched["dte"],
            "stop_loss_pct": sched["stop_loss_pct"],
            "turbulence_threshold": sched["turbulence_threshold"],
            "proximity_stop_hours": sched["proximity_stop_hours"],
            "proximity_buffer_usd": sched["proximity_buffer_usd"],
            "premium_sl_except_final_hours": sched["premium_sl_except_final_hours"],
        },
    )


class TestResolveSchedule:
    def test_monday_early(self):
        dt = _nyc(2026, 7, 6, 2, 0)
        assert resolve_schedule(dt.weekday(), dt.time(), _SCHEDULE_DEFAULTS) == "mon_early"

    def test_monday_before_mon_thu_still_mon_early(self):
        # mon_thu entry is 14:00 NYC — 10:00 / 12:00 are still mon_early.
        dt = _nyc(2026, 7, 6, 10, 0)
        assert resolve_schedule(dt.weekday(), dt.time(), _SCHEDULE_DEFAULTS) == "mon_early"
        dt = _nyc(2026, 7, 6, 12, 0)
        assert resolve_schedule(dt.weekday(), dt.time(), _SCHEDULE_DEFAULTS) == "mon_early"

    def test_monday_1400_is_mon_thu(self):
        dt = _nyc(2026, 7, 6, 14, 0)
        assert resolve_schedule(dt.weekday(), dt.time(), _SCHEDULE_DEFAULTS) == "mon_thu"

    def test_tuesday_afternoon_is_mon_thu(self):
        dt = _nyc(2026, 7, 7, 14, 5)
        assert resolve_schedule(dt.weekday(), dt.time(), _SCHEDULE_DEFAULTS) == "mon_thu"

    def test_friday_1230_is_fri(self):
        dt = _nyc(2026, 7, 10, 12, 30)
        assert resolve_schedule(dt.weekday(), dt.time(), _SCHEDULE_DEFAULTS) == "fri"

    def test_friday_before_1230_none(self):
        dt = _nyc(2026, 7, 10, 12, 0)
        assert resolve_schedule(dt.weekday(), dt.time(), _SCHEDULE_DEFAULTS) is None

    def test_saturday_none(self):
        dt = _nyc(2026, 7, 11, 14, 0)
        assert resolve_schedule(dt.weekday(), dt.time(), _SCHEDULE_DEFAULTS) is None


class TestWatchWindow:
    def test_mon_thu_before_entry_blocked(self):
        now = _nyc(2026, 7, 7, 13, 0).astimezone(UTC)
        assert in_watch_window(_SCHEDULE_DEFAULTS["mon_thu"], now) is False

    def test_mon_thu_after_entry_in_window(self):
        now = _nyc(2026, 7, 7, 14, 5).astimezone(UTC)
        assert in_watch_window(_SCHEDULE_DEFAULTS["mon_thu"], now) is True

    def test_mon_early_watch_ends_at_07_utc(self):
        assert in_watch_window(
            _SCHEDULE_DEFAULTS["mon_early"],
            datetime(2026, 7, 6, 6, 30, tzinfo=UTC),
        ) is True
        assert in_watch_window(
            _SCHEDULE_DEFAULTS["mon_early"],
            datetime(2026, 7, 6, 7, 0, tzinfo=UTC),
        ) is False


class TestAug2026ScheduleDefaults:
    def test_release_identity(self):
        s = _strategy()
        desc = s.describe_params()
        assert desc["release_name"] == RELEASE_NAME
        assert desc["strategy_version"] == "1.0.0"
        assert "16dcb4f42c9a" in desc["live_slot_ref"]
        assert s.name == "tudysho_monopteros"
        assert s.DATE_RANGE == ("2025-04-11", "2026-08-31")
        assert s._max_concurrent == 1

    def test_aug2026_trade_params(self):
        s = _strategy()
        mt = s._schedules["mon_thu"]
        assert mt["entry_time"] == "14:00"
        assert mt["delta"] == 0.05
        assert mt["dte"] == 1
        assert mt["min_otm_pct"] == 2.2
        assert mt["turbulence_threshold"] == 70.0
        assert mt["stop_loss_pct"] == 0.0
        assert mt["proximity_buffer_usd"] == 500.0
        assert mt["proximity_stop_hours"] == 8.0

        me = s._schedules["mon_early"]
        assert me["entry_time"] == "00:05"
        assert me["dte"] == 0
        assert me["delta"] == 0.05
        assert me["min_otm_pct"] == 1.0
        assert me["stop_loss_pct"] == 6.0
        assert me["proximity_buffer_usd"] == 800.0
        assert me["watch_until_utc_midnight"] is False
        assert me["watch_until_utc_hour"] == 7

        fri = s._schedules["fri"]
        assert fri["entry_time"] == "12:30"
        assert fri["delta"] == 0.06
        assert fri["min_otm_pct"] == 2.4
        assert fri["turbulence_threshold"] == 99.0
        assert fri["stop_loss_pct"] == 3.5
        assert fri["proximity_buffer_usd"] == 1000.0
        assert fri["proximity_stop_hours"] == 8.0

    def test_product_sizing(self):
        s = _strategy()
        assert s._nav_premium_pct == 0.5
        assert s._max_qty_per_1btc_equity == 6.0
        assert s._equity_drawdown_stop_pct == 5.0


class TestPerScheduleDailyCap:
    def test_monday_mon_early_then_mon_thu_same_nyc_day(self):
        s = _strategy()
        s._turbulence = None

        s._last_trade_nyc_date["mon_early"] = "2026-07-06"
        mon_1400 = _nyc(2026, 7, 6, 14, 0).astimezone(UTC)
        state = SimpleNamespace(
            dt=mon_1400,
            spot=100_000.0,
            nav_usd=100_000.0,
            equity_usd=100_000.0,
            expiries=lambda: [],
            get_chain=lambda e: [],
        )
        assert resolve_schedule(
            _nyc(2026, 7, 6, 14, 0).weekday(),
            _nyc(2026, 7, 6, 14, 0).time(),
            s._schedules,
        ) == "mon_thu"
        assert s._maybe_open(state) is None
        assert s._last_trade_nyc_date["mon_thu"] is None

    def test_same_schedule_blocked_after_trade(self):
        s = _strategy()
        s._last_trade_nyc_date["mon_thu"] = "2026-07-07"
        tue = _nyc(2026, 7, 7, 14, 0).astimezone(UTC)
        state = SimpleNamespace(dt=tue, spot=100_000.0, get_chain=lambda e: [])
        assert s._maybe_open(state) is None


class TestExitCondWiring:
    def test_mon_thu_has_proximity_only_when_sl_off(self):
        # Aug-2026 mon_thu lock has stop_loss_pct=0 → proximity only.
        conds = _build_exit_conds(_SCHEDULE_DEFAULTS["mon_thu"])
        assert len(conds) == 1

    def test_fri_has_proximity_and_premium(self):
        conds = _build_exit_conds(_SCHEDULE_DEFAULTS["fri"])
        assert len(conds) == 2

    def test_proximity_uses_schedule_buffer(self):
        s = _strategy()
        pos = _strangle_pos("mon_thu")  # buffer 500
        s._positions = [pos]
        # Spot 400 below put — within 500 buffer → no proximity
        state = SimpleNamespace(
            dt=datetime(2026, 5, 28, 6, 0, tzinfo=UTC),
            spot=73600.0,
            get_option=lambda *args: None,
        )
        assert s.on_market_state(state) == []

        # Spot 600 below put — beyond buffer → proximity
        state.spot = 73400.0
        trades = s.on_market_state(state)
        assert len(trades) == 1
        assert trades[0].exit_reason == "strike_proximity_stop"
        assert trades[0].metadata["proximity_buffer_usd"] == 500.0

    def test_param_help_covers_grid_keys(self):
        assert set(TuDyShoMonopteros.PARAM_HELP) == set(TuDyShoMonopteros.PARAM_GRID)


class TestNavPremiumSizing:
    def _state(self, nav_usd, equity_usd, spot):
        return SimpleNamespace(nav_usd=nav_usd, equity_usd=equity_usd, spot=spot)

    def test_target_premium_from_nav(self):
        s = _strategy(nav_premium_pct=0.5, max_qty_per_1btc_equity=6)
        state = self._state(100_000.0, 100_000.0, 100_000.0)
        qty, meta = s._compute_quantity(state, premium_usd_per_contract=100.0)
        assert qty == 5.0
        assert meta["target_premium_usd"] == 500.0
        assert meta["max_contracts_applied"] == 6.0
        assert meta["premium_capped"] is False


class TestConcurrency:
    def test_no_new_entry_while_position_open(self):
        s = _strategy()
        s._positions = [_strangle_pos()]
        state = SimpleNamespace(
            dt=datetime(2026, 5, 27, 16, 0, tzinfo=UTC),
            spot=75000.0,
            get_option=lambda *args: SimpleNamespace(
                bid=0.001, ask=0.0012, mark=0.0011, spot=75000.0,
                bid_usd=75.0, ask_usd=90.0, mark_usd=82.5,
            ),
            expiries=lambda: [],
            get_chain=lambda e: [],
        )
        trades = s.on_market_state(state)
        assert trades == []
        assert len(s._positions) == 1

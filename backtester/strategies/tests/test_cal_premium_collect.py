"""
Unit tests for backtester/strategies/cal_premium_collect.py

Tests cover:
    - First entry on first Friday at 09:00 UTC opens both pairs ("both")
    - First entry opens only put pair when sides="puts"
    - First entry opens only call pair when sides="calls"
    - Weekly roll: short leg is closed and new one is opened
    - Weekly roll: long leg is kept when delta is within threshold
    - Weekly roll: long leg is rolled when delta drifts outside threshold
    - No roll fires on a non-Friday tick
    - on_end() closes all open positions
    - Two rolls in the same ISO week are de-duplicated (roll fires once)

Run:
    .venv/bin/python -m pytest backtester/strategies/tests/test_cal_premium_collect.py -v
"""
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from backtester.pricing import deribit_fee_per_leg
from backtester.strategies.cal_premium_collect import CalPremiumCollect

# ── Constants ────────────────────────────────────────────────────────────────

SPOT = 94_000.0

# Strikes
PUT_STRIKE = 88_000.0     # ~20-delta put below ATM
CALL_STRIKE = 100_000.0   # ~20-delta call above ATM

# Expiries (Deribit format)
LONG_EXPIRY = "24JUN26"   # ~35 DTE from entry date 20-MAY-26
SHORT_EXPIRY = "27MAY26"  # ~7 DTE from entry date 20-MAY-26

# Prices (BTC units, will be multiplied by SPOT for USD)
LONG_ASK_BTC = 0.005       # 470 USD per long contract
LONG_BID_BTC = 0.0045
SHORT_BID_BTC = 0.002      # 188 USD per short contract  (received)
SHORT_ASK_BTC = 0.0025

LONG_ASK_USD = LONG_ASK_BTC * SPOT
LONG_BID_USD = LONG_BID_BTC * SPOT
SHORT_BID_USD = SHORT_BID_BTC * SPOT
SHORT_ASK_USD = SHORT_ASK_BTC * SPOT


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_quote(strike, is_call, bid, ask, delta, expiry, spot=SPOT):
    obj = SimpleNamespace(
        strike=strike,
        is_call=is_call,
        expiry=expiry,
        bid=bid,
        ask=ask,
        mark=bid,
        delta=delta,
        spot=spot,
    )
    obj.bid_usd = bid * spot
    obj.ask_usd = ask * spot
    obj.mark_usd = bid * spot
    return obj


def _make_state(
    dt,
    spot=SPOT,
    long_put_delta=-0.20,
    long_call_delta=0.20,
    long_ask=LONG_ASK_BTC,
    long_bid=LONG_BID_BTC,
    short_bid=SHORT_BID_BTC,
    short_ask=SHORT_ASK_BTC,
    long_expiry=LONG_EXPIRY,
    short_expiry=SHORT_EXPIRY,
):
    """Build a minimal mock MarketState for cal_premium_collect tests."""
    state = MagicMock()
    state.dt = dt
    state.spot = spot
    state.spot_bars = []

    # Pre-build quotes
    put_long_q = _make_quote(PUT_STRIKE, False, long_bid, long_ask, long_put_delta, long_expiry)
    call_long_q = _make_quote(CALL_STRIKE, True, long_bid, long_ask, long_call_delta, long_expiry)
    put_short_q = _make_quote(PUT_STRIKE, False, short_bid, short_ask, -0.10, short_expiry)
    call_short_q = _make_quote(CALL_STRIKE, True, short_bid, short_ask, 0.10, short_expiry)

    def get_chain(expiry):
        if expiry == long_expiry:
            return [put_long_q, call_long_q]
        if expiry == short_expiry:
            return [put_short_q, call_short_q]
        return []

    def get_option(expiry, strike, is_call):
        if expiry == long_expiry:
            if strike == PUT_STRIKE and not is_call:
                return put_long_q
            if strike == CALL_STRIKE and is_call:
                return call_long_q
        if expiry == short_expiry:
            if strike == PUT_STRIKE and not is_call:
                return put_short_q
            if strike == CALL_STRIKE and is_call:
                return call_short_q
        return None

    def expiries():
        # LONG_EXPIRY: compute DTE dynamically from dt
        from backtester.expiry_utils import parse_expiry_date
        exp_date = parse_expiry_date(long_expiry)
        # Return the long expiry only if it's in the 29–90 DTE window
        today = dt.date()
        dte = (exp_date.date() - today).days if exp_date else 0
        result = []
        if 29 <= dte <= 90:
            result.append(long_expiry)
        # SHORT_EXPIRY: compute DTE dynamically
        exp_short = parse_expiry_date(short_expiry)
        if exp_short:
            dte_s = (exp_short.date() - today).days
            if 5 <= dte_s <= 9:
                result.append(short_expiry)
        return result

    state.expiries.side_effect = expiries
    state.get_chain.side_effect = get_chain
    state.get_option.side_effect = get_option

    return state


def _friday_09(year=2026, month=5, day=22):
    """Return a Friday 09:00 UTC datetime (default: 2026-05-22 — nearest Friday)."""
    # 2026-05-22 is a Friday
    return datetime(year, month, day, 9, 0, 0, tzinfo=timezone.utc)


def _make_strategy(**params):
    s = CalPremiumCollect()
    defaults = {
        "sides": "both",
        "target_delta": 0.20,
        "delta_drift_threshold": 0.10,
    }
    defaults.update(params)
    s.configure(defaults)
    return s


# ── Tests: first entry ────────────────────────────────────────────────────────

class TestFirstEntry:

    def test_opens_both_pairs_on_first_friday(self):
        s = _make_strategy(sides="both")
        state = _make_state(_friday_09())
        trades = s.on_market_state(state)
        assert len(s._positions) == 2
        pair_types = {p.metadata["pair_type"] for p in s._positions}
        assert pair_types == {"put", "call"}
        # New API: each open emits a side='open' Trade for fill linkage.
        assert len(trades) == 2
        assert all(t.side == "open" for t in trades)
        assert all(t.pnl == 0.0 for t in trades)

    def test_opens_only_put_pair_when_sides_puts(self):
        s = _make_strategy(sides="puts")
        state = _make_state(_friday_09())
        s.on_market_state(state)
        assert len(s._positions) == 1
        assert s._positions[0].metadata["pair_type"] == "put"

    def test_opens_only_call_pair_when_sides_calls(self):
        s = _make_strategy(sides="calls")
        state = _make_state(_friday_09())
        s.on_market_state(state)
        assert len(s._positions) == 1
        assert s._positions[0].metadata["pair_type"] == "call"

    def test_no_entry_outside_friday_09(self):
        s = _make_strategy()
        # Monday 09:00 — should not open
        monday = datetime(2026, 5, 18, 9, 0, tzinfo=timezone.utc)
        state = _make_state(monday)
        s.on_market_state(state)
        assert len(s._positions) == 0

    def test_no_entry_friday_wrong_hour(self):
        s = _make_strategy()
        # Friday 10:00 — not 09:00
        dt = datetime(2026, 5, 22, 10, 0, tzinfo=timezone.utc)
        state = _make_state(dt)
        s.on_market_state(state)
        assert len(s._positions) == 0

    def test_position_metadata_structure(self):
        s = _make_strategy(sides="puts")
        state = _make_state(_friday_09())
        s.on_market_state(state)
        pos = s._positions[0]
        md = pos.metadata
        assert md["pair_type"] == "put"
        assert md["is_call"] is False
        assert md["long_expiry"] == LONG_EXPIRY
        assert md["short_expiry"] == SHORT_EXPIRY
        assert md["long_strike"] == PUT_STRIKE
        assert md["short_strike"] == PUT_STRIKE
        assert md["short_entry_premium_usd"] == pytest.approx(SHORT_BID_USD, rel=1e-4)
        assert md["long_entry_premium_usd"] == pytest.approx(LONG_ASK_USD, rel=1e-4)
        assert md["direction"] == "sell"


# ── Tests: weekly roll ────────────────────────────────────────────────────────

class TestWeeklyRoll:

    # Dates: first entry Friday 22-MAY-26, next roll Friday 29-MAY-26
    FIRST_FRIDAY = datetime(2026, 5, 22, 9, 0, tzinfo=timezone.utc)
    NEXT_FRIDAY = datetime(2026, 5, 29, 9, 0, tzinfo=timezone.utc)
    # For the next Friday, use a short expiry with 7 DTE from 29-MAY
    NEXT_SHORT_EXPIRY = "5JUN26"  # 7 DTE from 29-MAY-26

    def _state_for_next_friday(self, long_delta=-0.20, long_expiry=LONG_EXPIRY):
        """State at next Friday 09:00 with fresh short expiry."""
        from backtester.expiry_utils import parse_expiry_date

        dt = self.NEXT_FRIDAY
        state = MagicMock()
        state.dt = dt
        state.spot = SPOT
        state.spot_bars = []

        put_long_q = _make_quote(PUT_STRIKE, False, LONG_BID_BTC, LONG_ASK_BTC, long_delta, long_expiry)
        call_long_q = _make_quote(CALL_STRIKE, True, LONG_BID_BTC, LONG_ASK_BTC, abs(long_delta), long_expiry)
        put_short_q = _make_quote(PUT_STRIKE, False, SHORT_BID_BTC, SHORT_ASK_BTC, -0.10, self.NEXT_SHORT_EXPIRY)
        call_short_q = _make_quote(CALL_STRIKE, True, SHORT_BID_BTC, SHORT_ASK_BTC, 0.10, self.NEXT_SHORT_EXPIRY)

        # Also need old short expiry quotes for close (SHORT_EXPIRY still present)
        old_put_short = _make_quote(PUT_STRIKE, False, SHORT_BID_BTC, SHORT_ASK_BTC, -0.10, SHORT_EXPIRY)
        old_call_short = _make_quote(CALL_STRIKE, True, SHORT_BID_BTC, SHORT_ASK_BTC, 0.10, SHORT_EXPIRY)

        def get_chain(expiry):
            if expiry == long_expiry:
                return [put_long_q, call_long_q]
            if expiry == self.NEXT_SHORT_EXPIRY:
                return [put_short_q, call_short_q]
            if expiry == SHORT_EXPIRY:
                return [old_put_short, old_call_short]
            return []

        def get_option(expiry, strike, is_call):
            for q in get_chain(expiry):
                if q.strike == strike and q.is_call == is_call:
                    return q
            return None

        def expiries():
            # long expiry in 29-90 DTE window from NEXT_FRIDAY
            today = dt.date()
            result = []
            exp_long = parse_expiry_date(long_expiry)
            if exp_long:
                dte_l = (exp_long.date() - today).days
                if 29 <= dte_l <= 90:
                    result.append(long_expiry)
            # NEW short expiry in 5–9 DTE window
            exp_short = parse_expiry_date(self.NEXT_SHORT_EXPIRY)
            if exp_short:
                dte_s = (exp_short.date() - today).days
                if 5 <= dte_s <= 9:
                    result.append(self.NEXT_SHORT_EXPIRY)
            # old short expiry (now ~0 DTE, should NOT appear in 5-9 window)
            return result

        state.expiries.side_effect = expiries
        state.get_chain.side_effect = get_chain
        state.get_option.side_effect = get_option
        return state

    def test_roll_produces_close_trade_and_new_position(self):
        s = _make_strategy(sides="puts")
        # First entry (yields one open trade)
        s.on_market_state(_make_state(self.FIRST_FRIDAY))
        assert len(s._positions) == 1
        first_pos = s._positions[0]
        assert first_pos.metadata["short_expiry"] == SHORT_EXPIRY

        # Roll: with delta within threshold, this is a partial close of
        # the short + add a new short, so we expect 2 trades (1 close, 1 open).
        roll_state = self._state_for_next_friday()
        trades = s.on_market_state(roll_state)

        close_trades = [t for t in trades if t.side == "close"]
        open_trades  = [t for t in trades if t.side == "open"]
        assert len(close_trades) == 1
        # Short expired before the roll date → reason is "expiry", not "roll"
        assert close_trades[0].exit_reason == "expiry"
        assert len(open_trades) == 1
        assert len(s._positions) == 1
        new_pos = s._positions[0]
        assert new_pos.metadata["short_expiry"] == self.NEXT_SHORT_EXPIRY

    def test_roll_keeps_long_when_delta_within_threshold(self):
        """Delta -0.20, threshold 0.10 → drift = 0.0 → long kept.

        Under the new API the long position is NOT rebuilt: the same
        OpenPosition is mutated (short replaced via partial_close + add_legs).
        The long's original entry premium and fee remain intact on the
        position; only the short rotates.
        """
        s = _make_strategy(sides="puts", target_delta=0.20, delta_drift_threshold=0.10)
        s.on_market_state(_make_state(self.FIRST_FRIDAY))
        first_pos = s._positions[0]
        first_pos_id = first_pos.metadata["pos_id"]

        # Roll: same delta, long should stay at same expiry/strike.
        roll_state = self._state_for_next_friday(long_delta=-0.20)
        s.on_market_state(roll_state)

        new_pos = s._positions[0]
        # Same underlying OpenPosition (mutated, not replaced).
        assert new_pos.metadata["pos_id"] == first_pos_id
        assert new_pos.metadata["long_expiry"] == LONG_EXPIRY
        assert new_pos.metadata["long_strike"] == PUT_STRIKE
        # fees_open: long_fee retained + new short_fee added.
        expected_short_fee = deribit_fee_per_leg(SPOT, SHORT_BID_USD)
        expected_long_fee  = deribit_fee_per_leg(SPOT, LONG_ASK_USD)
        assert new_pos.fees_open == pytest.approx(
            expected_long_fee + expected_short_fee, rel=1e-4
        )

    def test_roll_refreshes_long_when_delta_drifted(self):
        """Delta drifts to -0.05, threshold 0.10 → |0.05 - 0.20| = 0.15 > 0.10 → roll long."""
        s = _make_strategy(sides="puts", target_delta=0.20, delta_drift_threshold=0.10)
        s.on_market_state(_make_state(self.FIRST_FRIDAY))

        # Provide a new long expiry at a different strike to detect the roll
        NEW_LONG_EXPIRY = "28JUL26"  # also in 29-90 DTE range from NEXT_FRIDAY (29-MAY)

        state = MagicMock()
        state.dt = self.NEXT_FRIDAY
        state.spot = SPOT
        state.spot_bars = []

        from backtester.expiry_utils import parse_expiry_date

        # Long at new expiry: delta -0.20 (fresh target)
        put_new_long = _make_quote(PUT_STRIKE, False, LONG_BID_BTC, LONG_ASK_BTC, -0.20, NEW_LONG_EXPIRY)
        call_new_long = _make_quote(CALL_STRIKE, True, LONG_BID_BTC, LONG_ASK_BTC, 0.20, NEW_LONG_EXPIRY)
        # Old long at OLD expiry: delta drifted to -0.05
        put_old_long = _make_quote(PUT_STRIKE, False, LONG_BID_BTC, LONG_ASK_BTC, -0.05, LONG_EXPIRY)
        call_old_long = _make_quote(CALL_STRIKE, True, LONG_BID_BTC, LONG_ASK_BTC, 0.05, LONG_EXPIRY)
        # Short
        put_short_old = _make_quote(PUT_STRIKE, False, SHORT_BID_BTC, SHORT_ASK_BTC, -0.10, SHORT_EXPIRY)
        put_short_new = _make_quote(PUT_STRIKE, False, SHORT_BID_BTC, SHORT_ASK_BTC, -0.10, self.NEXT_SHORT_EXPIRY)

        def get_chain(expiry):
            if expiry == NEW_LONG_EXPIRY:
                return [put_new_long, call_new_long]
            if expiry == LONG_EXPIRY:
                return [put_old_long, call_old_long]
            if expiry == SHORT_EXPIRY:
                return [put_short_old]
            if expiry == self.NEXT_SHORT_EXPIRY:
                return [put_short_new]
            return []

        def get_option(expiry, strike, is_call):
            for q in get_chain(expiry):
                if q.strike == strike and q.is_call == is_call:
                    return q
            return None

        def expiries():
            today = self.NEXT_FRIDAY.date()
            result = []
            for exp_code in [NEW_LONG_EXPIRY, self.NEXT_SHORT_EXPIRY]:
                ed = parse_expiry_date(exp_code)
                if ed:
                    dte = (ed.date() - today).days
                    if 29 <= dte <= 90:
                        result.append(exp_code)
                    elif 5 <= dte <= 9:
                        result.append(exp_code)
            # Add short expiry check
            for exp_code in [self.NEXT_SHORT_EXPIRY]:
                ed = parse_expiry_date(exp_code)
                if ed:
                    dte = (ed.date() - today).days
                    if 5 <= dte <= 9 and exp_code not in result:
                        result.append(exp_code)
            return result

        state.expiries.side_effect = expiries
        state.get_chain.side_effect = get_chain
        state.get_option.side_effect = get_option

        s.on_market_state(state)

        new_pos = s._positions[0]
        # Long should have been rolled to the new expiry
        assert new_pos.metadata["long_expiry"] == NEW_LONG_EXPIRY

    def test_no_roll_on_non_friday(self):
        s = _make_strategy(sides="puts")
        s.on_market_state(_make_state(self.FIRST_FRIDAY))
        assert len(s._positions) == 1

        # Tuesday 09:00 — no roll
        tuesday = datetime(2026, 5, 26, 9, 0, tzinfo=timezone.utc)
        state = _make_state(tuesday)
        trades = s.on_market_state(state)
        assert trades == []
        assert len(s._positions) == 1  # still open, unchanged

    def test_roll_fires_once_per_iso_week(self):
        """Two ticks on the same Friday should only roll once."""
        s = _make_strategy(sides="puts")
        s.on_market_state(_make_state(self.FIRST_FRIDAY))

        roll_state = self._state_for_next_friday()
        trades1 = s.on_market_state(roll_state)
        # New API: roll yields 2 trades (close of short + open of new short).
        close_trades_1 = [t for t in trades1 if t.side == "close"]
        assert len(close_trades_1) == 1

        # Same Friday, minute later — should not roll again
        dt2 = self.NEXT_FRIDAY.replace(minute=0)  # same minute=0, same ISO week
        state2 = MagicMock()
        state2.dt = dt2
        state2.spot = SPOT
        state2.spot_bars = []
        state2.expiries.side_effect = roll_state.expiries.side_effect
        state2.get_chain.side_effect = roll_state.get_chain.side_effect
        state2.get_option.side_effect = roll_state.get_option.side_effect

        trades2 = s.on_market_state(state2)
        roll_trades2 = [t for t in trades2 if t.exit_reason == "roll"]
        assert roll_trades2 == []


# ── Tests: on_end ─────────────────────────────────────────────────────────────

class TestOnEnd:

    def test_on_end_closes_all_positions(self):
        s = _make_strategy(sides="both")
        s.on_market_state(_make_state(_friday_09()))
        assert len(s._positions) == 2

        end_state = _make_state(datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc))
        trades = s.on_end(end_state)
        assert len(trades) == 2
        assert all(t.exit_reason == "end_of_data" for t in trades)
        assert len(s._positions) == 0

    def test_on_end_clears_positions(self):
        s = _make_strategy(sides="puts")
        s.on_market_state(_make_state(_friday_09()))
        s.on_end(_make_state(datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)))
        assert s._positions == []


# ── Tests: reset ──────────────────────────────────────────────────────────────

class TestReset:

    def test_reset_clears_state(self):
        s = _make_strategy()
        s.on_market_state(_make_state(_friday_09()))
        assert len(s._positions) > 0
        s.reset()
        assert s._positions == []
        assert s._last_roll_isoweek is None

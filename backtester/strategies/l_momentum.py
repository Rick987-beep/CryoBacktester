#!/usr/bin/env python3
"""
l_momentum.py — Long directional BTC options gated by MTF spot momentum.

Enters a long call or put at a 4h UTC boundary when multi-timeframe BTC
spot momentum is aligned in the trade direction:

    Call signal: 4h spot chg ≥ +mom_4h_thr% AND 1h spot chg ≥ +mom_1h_thr%
    Put  signal: 4h spot chg ≤ −mom_4h_thr% AND 1h spot chg ≤ −mom_1h_thr%

Additional filters:
    - DTE = 4 or 5 at entry
    - Delta ∈ [0.30, 0.40] for calls, [−0.40, −0.30] for puts
    - Bid-ask spread ≤ spread_max_pct% of mark

One trade per 4h entry window.  Up to max_concurrent positions open at once.

Exit:
    take_profit  — current ask  ≥ entry_ask × tp_mult
    spot_stop    — BTC spot drops spot_stop_pct% for calls / rises for puts
                   (spot_stop_pct = 0.0 disables)
    time_gate    — after time_gate_h hours, bid < entry_ask × time_gate_min_gain
                   (time_gate_h = 0 disables)
    end_of_data  — force-close at end of replay; expiry handled by engine.

Research basis:
    IndicatorBench/research/long_tradable_options/KERNEL_STRATEGY.md
    Expected base rate (MTF-high kernel): ~87.2% | EV: +7.3% per trade
    Expected frequency: ~11 trades/month at default params
"""
import logging
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import parse_expiry_date
from backtester.indicators import IndicatorDep
from backtester.pricing import deribit_fee_per_leg
from backtester.strategy_base import OpenPosition, Trade, close_trade

logger = logging.getLogger(__name__)

_4H_ENTRY_HOURS = frozenset({0, 4, 8, 12, 16, 20})

# Delta range validated empirically (KERNEL_STRATEGY.md §"Delta at entry")
_DELTA_LO = 0.30
_DELTA_HI = 0.40


class LMomentum:
    """Long directional BTC options gated by multi-timeframe spot momentum."""

    name = "l_momentum"
    DATE_RANGE = ("2026-01-01", "2026-05-12")
    DESCRIPTION = (
        "Buys a call (4h/1h spot up) or put (4h/1h spot down) at 4h UTC boundaries. "
        "DTE 4–5, delta 0.30–0.40. TP at ask × tp_mult; directional spot stop; "
        "36h time gate. One trade per entry window, up to 8 concurrent positions."
    )

    indicator_deps = [
        IndicatorDep(name="spot_mom_4h", symbol="BTCUSDT", interval="4h", warmup_days=2),
        IndicatorDep(name="spot_mom_1h", symbol="BTCUSDT", interval="1h", warmup_days=2),
    ]

    # Reduced priority grid — 243 combos (3×3×3×3×3).
    # Fixed: spread_max_pct=10, dte_range=(4,5), delta=(0.30,0.40).
    # Recommended default: 4h≥1.5% + 1h≥0.5% → 87.2% base rate, EV+7.3%.
    PARAM_GRID = {
        "mom_4h_thr":    [1.0, 1.5, 2.0],
        "mom_1h_thr":    [0.3, 0.5, 1.0],
        "tp_mult":       [1.75, 2.0, 2.5],
        "spot_stop_pct": [1.5, 2.0, 0.0],   # 0.0 = disabled
        "time_gate_h":   [24, 36, 48],
    }

    def __init__(self):
        self._positions = []  # type: List[OpenPosition]
        self._mom_4h = None   # pd.Series from indicator
        self._mom_1h = None   # pd.Series from indicator
        # Params — defaults match the EV-proven spec
        self._mom_4h_thr = 1.5
        self._mom_1h_thr = 0.5
        self._spread_max_pct = 10.0
        self._tp_mult = 2.0
        self._spot_stop_pct = 2.0
        self._time_gate_h = 36
        self._time_gate_min_gain = 1.30
        self._max_concurrent = 8

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        self._mom_4h_thr = float(params.get("mom_4h_thr", 1.5))
        self._mom_1h_thr = float(params.get("mom_1h_thr", 0.5))
        self._spread_max_pct = float(params.get("spread_max_pct", 10.0))
        self._tp_mult = float(params.get("tp_mult", 2.0))
        self._spot_stop_pct = float(params.get("spot_stop_pct", 2.0))
        self._time_gate_h = int(params.get("time_gate_h", 36))
        self._time_gate_min_gain = float(params.get("time_gate_min_gain", 1.30))
        self._max_concurrent = int(params.get("max_concurrent", 8))
        self._positions = []

    def set_indicators(self, ind):
        # type: (Dict[str, Any]) -> None
        self._mom_4h = ind.get("spot_mom_4h")
        self._mom_1h = ind.get("spot_mom_1h")

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        trades = []

        # --- Check exits for every open position ---
        still_open = []
        for pos in self._positions:
            reason = self._check_exit(state, pos)
            if reason:
                trades.append(self._close(state, pos, reason))
            else:
                still_open.append(pos)
        self._positions = still_open

        # --- Entry: only at 4h boundary ticks, one new trade per window ---
        if (state.dt.minute == 0
                and state.dt.hour in _4H_ENTRY_HOURS
                and len(self._positions) < self._max_concurrent):
            self._try_entry(state)

        return trades

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        trades = [self._close(state, pos, "end_of_data") for pos in self._positions]
        self._positions = []
        return trades

    def reset(self):
        # type: () -> None
        self._positions = []

    # ------------------------------------------------------------------
    # Entry
    # ------------------------------------------------------------------

    def _try_entry(self, state):
        # type: (Any) -> None
        """Evaluate MTF momentum and open one qualifying trade if signal is present."""
        mom_4h = _lookup_mom(self._mom_4h, state.dt, 4)
        mom_1h = _lookup_mom(self._mom_1h, state.dt, 1)
        if mom_4h is None or mom_1h is None:
            return

        thr_4h = self._mom_4h_thr
        thr_1h = self._mom_1h_thr

        # Direction: both timeframes must agree
        if mom_4h >= thr_4h and mom_1h >= thr_1h:
            is_call = True    # bullish → long call
        elif mom_4h <= -thr_4h and mom_1h <= -thr_1h:
            is_call = False   # bearish → long put
        else:
            return  # mixed or weak signal → no entry

        target_delta = 0.35 if is_call else -0.35

        # Prefer DTE=5 over DTE=4 (marginally higher base rate in research)
        today = state.dt.date()
        eligible_expiries = []
        for exp in state.expiries():
            exp_date = parse_expiry_date(exp)
            if exp_date is None:
                continue
            dte = (exp_date.date() - today).days
            if dte in (4, 5):
                eligible_expiries.append((dte, exp))
        eligible_expiries.sort(key=lambda x: -x[0])  # DTE 5 first

        for _, expiry in eligible_expiries:
            chain = state.get_chain(expiry)
            if not chain:
                continue

            opts = [q for q in chain if q.is_call == is_call]
            best = select_by_delta(opts, target_delta)
            if best is None:
                continue

            # Delta range filter
            if abs(best.delta) < _DELTA_LO or abs(best.delta) > _DELTA_HI:
                continue

            # Spread filter
            if best.mark == 0.0 or best.ask == 0.0:
                continue
            spread_pct = (best.ask - best.bid) / best.mark * 100.0
            if spread_pct > self._spread_max_pct:
                continue

            # Open one position and stop searching
            entry_ask_usd = best.ask_usd
            entry_mark_usd = best.mark_usd
            fee_open = deribit_fee_per_leg(state.spot, entry_mark_usd)
            dte = (parse_expiry_date(expiry).date() - today).days

            pos = OpenPosition(
                entry_time=state.dt,
                entry_spot=state.spot,
                legs=[{
                    "expiry": expiry,
                    "strike": best.strike,
                    "is_call": is_call,
                    "side": "buy",
                    "qty": 1.0,
                    "entry_price": best.ask,      # BTC-denominated
                    "entry_price_usd": entry_ask_usd,
                }],
                entry_price_usd=entry_ask_usd,
                fees_open=fee_open,
                metadata={
                    "direction": "buy",
                    "is_call": is_call,
                    "expiry": expiry,
                    "strike": best.strike,
                    "entry_ask_usd": entry_ask_usd,
                    "mom_4h": mom_4h,
                    "mom_1h": mom_1h,
                    "dte": dte,
                    "delta": best.delta,
                    "spread_pct": round(spread_pct, 2),
                },
            )
            self._positions.append(pos)
            logger.debug(
                "LMomentum: OPEN %s %s DTE=%d delta=%.2f ask_usd=%.2f "
                "spread=%.1f%% mom4h=%.2f%% mom1h=%.2f%%",
                "CALL" if is_call else "PUT", expiry, dte, best.delta,
                entry_ask_usd, spread_pct, mom_4h, mom_1h,
            )
            return  # one trade per entry window

    # ------------------------------------------------------------------
    # Exit
    # ------------------------------------------------------------------

    def _check_exit(self, state, pos):
        # type: (Any, OpenPosition) -> Optional[str]
        """Return exit reason string or None to keep position open."""
        expiry = pos.metadata["expiry"]
        strike = pos.metadata["strike"]
        is_call = pos.metadata["is_call"]
        entry_ask_usd = pos.metadata["entry_ask_usd"]

        quote = state.get_option(expiry, strike, is_call)
        if quote is None:
            return None  # no quote this tick — hold

        # TP: current ask ≥ entry_ask × tp_mult
        if quote.ask_usd >= entry_ask_usd * self._tp_mult:
            return "take_profit"

        # Stop A: spot adverse excursion (disabled when spot_stop_pct == 0)
        if self._spot_stop_pct > 0.0 and pos.entry_spot > 0.0:
            spot_chg_pct = (state.spot - pos.entry_spot) / pos.entry_spot * 100.0
            if is_call and spot_chg_pct <= -self._spot_stop_pct:
                return "spot_stop"
            if not is_call and spot_chg_pct >= self._spot_stop_pct:
                return "spot_stop"

        # Stop B: time gate (disabled when time_gate_h == 0)
        if self._time_gate_h > 0:
            held_h = (state.dt - pos.entry_time).total_seconds() / 3600.0
            if held_h >= self._time_gate_h:
                if quote.bid_usd < entry_ask_usd * self._time_gate_min_gain:
                    return "time_gate"

        return None

    def _close(self, state, pos, reason):
        # type: (Any, OpenPosition, str) -> Trade
        """Exit at current bid; fees computed on mark (Deribit convention)."""
        expiry = pos.metadata["expiry"]
        strike = pos.metadata["strike"]
        is_call = pos.metadata["is_call"]

        quote = state.get_option(expiry, strike, is_call)
        if quote is not None and quote.bid > 0.0:
            exit_usd = quote.bid_usd
            fee_close = deribit_fee_per_leg(state.spot, quote.mark_usd)
        elif quote is not None and quote.mark > 0.0:
            # bid is zero but mark is valid — use mark as fallback exit price
            exit_usd = quote.mark_usd
            fee_close = deribit_fee_per_leg(state.spot, quote.mark_usd)
        else:
            exit_usd = 0.0
            fee_close = 0.0

        logger.debug(
            "LMomentum: CLOSE %s %s reason=%s exit_usd=%.2f",
            "CALL" if is_call else "PUT", expiry, reason, exit_usd,
        )
        return close_trade(state, pos, reason,
                           current_usd=exit_usd,
                           fees_close=fee_close)


# ------------------------------------------------------------------
# Module-level helper (no self needed — also used in tests)
# ------------------------------------------------------------------

def _lookup_mom(series, dt, interval_h):
    # type: (Any, datetime, int) -> Optional[float]
    """Return pct-change value for the bar that closed at ``dt``.

    Binance klines are labeled by bar-open time. The bar whose close
    equals ``dt`` has open_time = ``dt`` − ``interval_h`` hours.
    At a 4h boundary like 16:00 UTC, the bar open at 12:00 just closed.

    Returns None on missing data or NaN.
    """
    if series is None:
        return None
    bar_ts = dt.replace(minute=0, second=0, microsecond=0) - timedelta(hours=interval_h)
    try:
        val = series.loc[bar_ts]
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return None
        return float(val)
    except KeyError:
        return None

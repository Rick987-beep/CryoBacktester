#!/usr/bin/env python3
"""
engine.py — Single-pass grid runner for backtesting strategies.

Iterates market data once and evaluates all parameter combinations
simultaneously: each strategy instance receives the same MarketState
at every 5-min tick. This avoids re-loading data for each combo and
keeps memory usage flat regardless of grid size.

Two public entry points:

  run_grid()         — lightweight V1-compatible output.
                       Returns dict[param_tuple → list[(pnl, triggered,
                       exit_hour, entry_date)]]. Use for quick counting
                       or when you don’t need daily NAV tracking.

  run_grid_full()    — full output used by the CLI and GridResult.
                       Returns (df, keys, nav_daily_df, final_nav_df):
                       • df            — trade log DataFrame (one row per closed trade)
                       • keys          — list of param tuples (index into combo_idx)
                       • nav_daily_df  — daily NAV low/high/close per combo
                       • final_nav_df  — final NAV + realized/open PnL per combo

NAV tracking detail:
  Every tick, _open_unrealized_pnl() marks all open positions to market.
  It reads pos._last_reprice_usd (cached by _reprice_legs in strategy_base)
  to avoid calling _reprice_legs twice per position per tick — once during
  the strategy’s SL/TP exit check, and once here for NAV accounting.
  Falls back to a fresh _reprice_legs call if the cache is absent.

Usage:
    from backtester.engine import run_grid_full
    df, keys, nav_daily_df, final_nav_df, df_fills = run_grid_full(
        MyStrategy, MY_PARAM_GRID, replay
    )
"""
import itertools
import time as _time
from typing import Any, Dict, List, Optional, Tuple, Type

from backtester.config import cfg as _cfg
from backtester.pricing import fee_btc_per_contract as _fee_btc
from backtester.strategy_base import Trade, _reprice_legs


def _inject_indicators(strategy_cls, instances, replay, progress, status_cb=None):
    # type: (Type, List[Any], Any, bool, Any) -> None
    """Pre-compute indicators declared by the strategy and inject into all instances."""
    deps = getattr(strategy_cls, "indicator_deps", None)
    if not deps:
        return

    from datetime import timezone
    from backtester.indicators import build_indicators

    start_dt, end_dt = replay.date_range()

    if progress:
        names = [d.name for d in deps]
        print(f"Building indicators {names} ({start_dt.date()} → {end_dt.date()})...")

    ind = build_indicators(deps, start_dt, end_dt, status_cb=status_cb)

    for strategy in instances:
        if hasattr(strategy, "set_indicators"):
            strategy.set_indicators(ind)

    if progress:
        print(f"  Indicators ready: {sorted(ind.keys())}")

_progress_interval = _cfg.simulation.progress_interval


def _iter_open_positions(strategy):
    # type: (Any) -> List[Any]
    """Return a strategy's current open positions.

    Reads ``strategy._positions`` (the canonical List[OpenPosition] that all
    strategies must maintain).  Returns an empty list when the attribute is
    absent or not a list — makes the engine safe against mis-configured
    strategies rather than silently carrying a stale cache value.
    """
    positions = getattr(strategy, "_positions", None)
    if isinstance(positions, list):
        return positions
    return []


def _open_unrealized_pnl(strategy, state, pos_cache):
    # type: (Any, Any, Dict[int, float]) -> float
    """Mark all open positions to market.

    Reads pos._last_reprice_usd if the strategy already repriced this tick
    (set by _reprice_legs in strategy_base), avoiding a redundant second call.
    Falls back to calling _reprice_legs directly when the cache is stale/absent.
    Uses carry-forward when a leg cannot be repriced on this tick.
    """
    positions = _iter_open_positions(strategy)
    if not positions:
        pos_cache.clear()
        return 0.0

    live_ids = set(id(p) for p in positions)
    stale_ids = [pid for pid in pos_cache.keys() if pid not in live_ids]
    for pid in stale_ids:
        pos_cache.pop(pid, None)

    total = 0.0
    for pos in positions:
        pid = id(pos)
        # Use reprice result cached by _reprice_legs this tick if available.
        current_usd = pos._last_reprice_usd
        per_leg_vals = pos._last_reprice_legs
        if current_usd is None:
            current_usd = _reprice_legs(state, pos)
            per_leg_vals = pos._last_reprice_legs
        else:
            # Consume the cached value — reset so a stale value isn't reused
            # on a future tick where _reprice_legs was not called (e.g. expiry
            # check fired and bypassed the SL/TP path).
            pos._last_reprice_usd = None
            pos._last_reprice_legs = None
        if current_usd is None:
            pnl = pos_cache.get(pid)
            if pnl is None:
                # First unseen tick with missing quotes: assume flat mark.
                pnl = -float(pos.fees_open)
        else:
            # Leg-aware path: when ALL legs carry side + entry price, compute
            # per-leg PnL directly. This correctly handles mixed-side positions
            # (e.g. calendar spreads) where entry_price_usd is a net value that
            # can't be combined with the gross _reprice_legs total.
            _can_leg_aware = (
                per_leg_vals is not None
                and len(per_leg_vals) == len(pos.legs)
                and bool(pos.legs)
                and all(
                    leg.get("side") in ("buy", "sell")
                    and ("price_btc" in leg or "entry_price" in leg)
                    for leg in pos.legs
                )
            )
            if _can_leg_aware:
                pnl = 0.0
                for leg, cur_val in zip(pos.legs, per_leg_vals):
                    qty = float(leg.get("qty", 1.0))
                    entry_btc = float(leg.get("price_btc", leg.get("entry_price", 0.0)))
                    entry_spot_leg = float(leg.get("entry_spot", pos.entry_spot))
                    entry_usd = entry_btc * entry_spot_leg * qty
                    if leg["side"] == "sell":
                        pnl += entry_usd - cur_val
                    else:
                        pnl += cur_val - entry_usd
                pnl -= float(pos.fees_open)
            else:
                # Leg annotations absent for unrealized PnL — carry forward.
                pnl = pos_cache.get(pid, -float(pos.fees_open))

        pos_cache[pid] = pnl
        total += pnl

    return total


def _grid_combos(param_grid):
    # type: (Dict[str, List]) -> List[Dict[str, Any]]
    """Expand a parameter grid dict into a list of param dicts.

    Example:
        {"a": [1, 2], "b": [10, 20]} → [{"a":1,"b":10}, {"a":1,"b":20}, ...]
    """
    keys = sorted(param_grid.keys())
    values = [param_grid[k] for k in keys]
    combos = []
    for vals in itertools.product(*values):
        combos.append(dict(zip(keys, vals)))
    return combos


def _params_to_key(params):
    # type: (Dict[str, Any]) -> Tuple
    """Convert a params dict to a hashable tuple key for results dict."""
    return tuple(sorted(params.items()))


def _trade_to_tuple(trade):
    # type: (Trade) -> Tuple[float, bool, int, str]
    """Convert Trade to V1-compatible (pnl, triggered, exit_hour, entry_date)."""
    return (trade.pnl, trade.triggered, trade.exit_hour, trade.entry_date)


def run_single(strategy_cls, params, replay):
    # type: (Type, Dict[str, Any], Any) -> List[Trade]
    """Run a single parameter combo and return Trade objects.

    Useful for debugging or inspecting individual trade details.
    """
    strategy = strategy_cls()
    strategy.configure(params)

    trades = []
    last_state = None
    realized_pnl = 0.0
    pos_pnl_cache = {}  # type: Dict[int, float]
    account_size = float(_cfg.simulation.account_size_usd)
    for state in replay:
        state.equity_usd = account_size + realized_pnl
        open_pnl = _open_unrealized_pnl(strategy, state, pos_pnl_cache)
        state.nav_usd = state.equity_usd + open_pnl
        result = strategy.on_market_state(state)
        for trade in result:
            trades.append(trade)
            if getattr(trade, "side", "close") == "close":
                realized_pnl += float(trade.pnl)
        last_state = state

    if last_state is not None:
        trades.extend(strategy.on_end(last_state))

    return trades


def run_grid(
    strategy_cls,       # type: Type
    param_grid,         # type: Dict[str, List]
    replay,             # type: Any
    extra_params=None,  # type: Optional[Dict[str, Any]]
    progress=True,      # type: bool
):
    # type: (...) -> Dict[Tuple, List[Tuple[float, bool, int, str]]]
    """Run all parameter combos in a single pass over market data.

    Creates one strategy instance per combo, iterates market data once,
    and feeds each MarketState to all instances simultaneously.

    Args:
        strategy_cls: Strategy class (must have configure/on_market_state/on_end/reset).
        param_grid: Dict of param_name → list of values.
        replay: MarketReplay instance (iterable of MarketState).
        extra_params: Optional fixed params merged into every combo
                      (e.g. {"pricing_mode": "real"}).
        progress: Print progress updates.

    Returns:
        Dict of param_tuple → list of (pnl, triggered, exit_hour, entry_date).
        Compatible with V1 metrics.compute_stats().
    """
    combos = _grid_combos(param_grid)
    n_combos = len(combos)

    if progress:
        print(f"Running {n_combos} parameter combos...")

    # Create and configure one strategy instance per combo
    instances = []  # type: List[Any]
    keys = []       # type: List[Tuple]
    for params in combos:
        full_params = dict(params)
        if extra_params:
            full_params.update(extra_params)
        strategy = strategy_cls()
        strategy.configure(full_params)
        instances.append(strategy)
        keys.append(_params_to_key(params))

    # Inject pre-computed indicators if strategy declares dependencies
    _inject_indicators(strategy_cls, instances, replay, progress)

    # Results: key → list of V1-compatible tuples
    results = {k: [] for k in keys}

    # Single-pass: iterate market data once
    t0 = _time.time()
    n_states = 0
    last_state = None
    total_states = len(replay)
    _last_print = t0
    _print_interval = 10.0  # seconds between progress lines

    for state in replay:
        n_states += 1
        for i, strategy in enumerate(instances):
            trades = strategy.on_market_state(state)
            for trade in trades:
                results[keys[i]].append(_trade_to_tuple(trade))
        last_state = state

        if progress:
            _now = _time.time()
            if _now - _last_print >= _print_interval:
                elapsed = _now - t0
                pct = 100.0 * n_states / total_states if total_states else 0.0
                print(f"  {n_states}/{total_states} states ({pct:.0f}%) — {elapsed:.1f}s elapsed...")
                _last_print = _now

    # Force-close any remaining positions
    if last_state is not None:
        for i, strategy in enumerate(instances):
            trades = strategy.on_end(last_state)
            for trade in trades:
                results[keys[i]].append(_trade_to_tuple(trade))

    elapsed = _time.time() - t0
    total_trades = sum(len(v) for v in results.values())

    if progress:
        print(
            f"Grid complete: {n_combos} combos × {n_states} states "
            f"= {total_trades:,} trades in {elapsed:.1f}s"
        )

    return results


def run_grid_full(
    strategy_cls,       # type: Type
    param_grid,         # type: Dict[str, List]
    replay,             # type: Any
    extra_params=None,  # type: Optional[Dict[str, Any]]
    progress=True,      # type: bool
    progress_cb=None,   # type: Optional[Any]  # Callable[[int, int, str], None] | None
    progress_cb_interval=50,  # type: int  # call progress_cb every N states
    status_cb=None,     # type: Optional[Any]  # Callable[[str, str], None] | None
):
    """Run all parameter combos in a single pass over market data.

    Accumulates trades into flat lists, then builds a memory-efficient
    pandas DataFrame (~10× less RAM than keeping Trade objects alive).

    Args:
        strategy_cls: Strategy class (configure/on_market_state/on_end/reset).
        param_grid:   Dict of param_name → list of values.
        replay:       MarketReplay instance (iterable of MarketState).
        extra_params: Optional fixed params merged into every combo.
        progress:     Print progress updates.

    Returns:
        Tuple of (df, keys, nav_daily_df, final_nav_df, df_fills):
        - df:       pandas DataFrame, one row per closed trade.
                    Column "combo_idx" (int16/int32) is an index into keys.
        - keys:     List[Tuple], where keys[i] is the param tuple for combo_idx i.
        - nav_daily_df: one row per combo/day with nav_low/nav_high/nav_close.
        - final_nav_df: one row per combo with final_nav, realized_pnl, open_pnl.
        - df_fills: one row per leg per event (open/close) across all combos.
    """
    import pandas as pd

    combos = _grid_combos(param_grid)
    n_combos = len(combos)

    if progress:
        print(f"Running {n_combos} parameter combos...")

    instances = []  # type: List[Any]
    keys = []       # type: List[Tuple]
    for params in combos:
        full_params = dict(params)
        if extra_params:
            full_params.update(extra_params)
        strategy = strategy_cls()
        strategy.configure(full_params)
        instances.append(strategy)
        keys.append(_params_to_key(params))

    # Inject pre-computed indicators if strategy declares dependencies
    _inject_indicators(strategy_cls, instances, replay, progress, status_cb=status_cb)

    if status_cb is not None:
        status_cb("backtesting", "Running backtest…")

    # Flat lists — Trade objects are decomposed immediately and discarded
    _combo_idx = []
    _entry_time = []
    _exit_time = []
    _entry_spot = []
    _exit_spot = []
    _entry_price_usd = []
    _exit_price_usd = []
    _fees = []
    _pnl = []
    _triggered = []
    _exit_reason = []
    _exit_hour = []
    _entry_date = []
    _status = []

    # Fill-level lists — one row per leg per event (open/close)
    _f_combo_idx = []
    _f_trade_idx = []
    _f_open_idx = []   # trade_idx of the matching open event
    _f_ts = []
    _f_event = []
    _f_contract = []
    _f_side = []
    _f_qty = []
    _f_price_btc = []   # option price in BTC per contract (always positive)
    _f_amount_btc = []  # signed BTC cash flow: +sell / -buy
    _f_fee_btc = []     # fee in BTC (always positive, deducted)
    _f_spot = []        # BTC/USD index at fill time
    _f_amount_usd = []  # derived: amount_btc × spot
    _f_fee_usd = []     # derived: fee_btc × spot
    _f_balance_usd = []  # running USD cash balance per combo after this fill
    _f_exit_reason = []
    _f_status = []

    # Per-combo pos_id → open trade_idx mapping (for open_idx linkage)
    _pos_open_idx = [{} for _ in range(n_combos)]  # type: List[Dict[int, int]]

    # Per-combo trade counter for stable trade_idx
    _trade_count = [0] * n_combos
    account_size = float(_cfg.simulation.account_size_usd)
    realized_pnl = [0.0] * n_combos
    last_open_pnl = [0.0] * n_combos
    pos_pnl_cache = [{} for _ in range(n_combos)]  # type: List[Dict[int, float]]
    running_balance = [account_size] * n_combos  # type: List[float]

    current_day = [None] * n_combos        # type: List[Optional[str]]
    day_low = [0.0] * n_combos
    day_high = [0.0] * n_combos
    day_close = [0.0] * n_combos
    day_realized_close = [0.0] * n_combos

    _nav_combo_idx = []
    _nav_date = []
    _nav_low = []
    _nav_high = []
    _nav_close = []
    _nav_realized_close = []

    def _append(i, trade):
        _combo_idx.append(i)
        _entry_time.append(trade.entry_time)
        _exit_time.append(trade.exit_time)
        _entry_spot.append(trade.entry_spot)
        _exit_spot.append(trade.exit_spot)
        _entry_price_usd.append(trade.entry_price_usd)
        _exit_price_usd.append(trade.exit_price_usd)
        _fees.append(trade.fees)
        _pnl.append(trade.pnl)
        _triggered.append(trade.triggered)
        _exit_reason.append(trade.exit_reason)
        _exit_hour.append(trade.exit_hour)
        _entry_date.append(trade.entry_date)
        _status.append(getattr(trade, 'status', 0))

    def _append_fills(i, trade):
        """Expand a Trade into per-leg fill rows (BTC-native).

        Each fill row records: price_btc (per contract), amount_btc (signed),
        fee_btc (positive, from fee model), and spot for USD derivation.

        For side=="open" Trades: emits open rows only.
        For side=="close" Trades: emits close rows only. The strategy must
            yield an explicit side='open' Trade so open fills are present.
        For side=="close" Trades with metadata["partial_close"]==True:
            emits close rows only for the closed legs; the pos_id → open_idx
            mapping is RETAINED so the surviving legs' eventual close still
            links back to the original open.
        Silently skips if the trade has no 'legs' in metadata.

        Leg dict expected keys:
            price_btc       — BTC price per contract at open (fallback: entry_price)
            exit_price_btc  — BTC price per contract at close (strategy annotates)
            qty             — contracts (default: 1.0)
            side            — "buy" | "sell"
            fee_btc_open    — override per-contract fee at open (e.g. 0 for free rolls)
            fee_btc_close   — override per-contract fee at close (e.g. 0 for expiry)
            strike, is_call, expiry  — for contract name construction
        """
        legs = trade.metadata.get("legs")
        if not legs:
            return

        _trade_count[i] += 1
        tidx = _trade_count[i]

        trade_side = getattr(trade, 'side', 'close')
        trade_status = getattr(trade, 'status', 0)
        pos_id = trade.metadata.get('pos_id')

        def _emit(leg, open_tidx, ts, spot, event, reason, status, is_close):
            strike = leg.get("strike", 0)
            expiry = leg.get("expiry", "")
            opt_type = "C" if leg.get("is_call") else "P"
            contract = leg.get("contract") or f"BTC-{expiry}-{int(strike)}-{opt_type}"
            leg_side = leg.get("side", "buy")
            fill_side = ("buy" if leg_side == "sell" else "sell") if is_close else leg_side
            qty = float(leg.get("qty", 1.0))

            if is_close:
                # Per-leg open_idx tracking: leg["_open_idx"] is set on the open
                # fill (see below) and read here. This handles multi-vintage
                # positions where legs were added at different times via
                # add_legs() — each leg points to its own open fill regardless
                # of the position-level pos_id.
                _leg_open_idx = leg.get("_open_idx")
                if _leg_open_idx is not None:
                    open_tidx = _leg_open_idx

                price_btc = float(leg.get("exit_price_btc", 0.0))
                if price_btc == 0.0:
                    # fallback: derive from USD annotation (exit_price_usd is per-contract)
                    _ex_usd = leg.get("exit_price_usd")
                    if _ex_usd is not None and spot > 0 and qty > 0:
                        price_btc = float(_ex_usd) / spot
                fee_override = leg.get("fee_btc_close")
            else:
                price_btc = float(leg.get("price_btc", leg.get("entry_price", 0.0)))
                fee_override = leg.get("fee_btc_open")

            if fee_override is not None:
                fee_btc = float(fee_override) * qty
            else:
                fee_btc = _fee_btc(price_btc) * qty

            sign = 1.0 if fill_side == "sell" else -1.0
            amount_btc = sign * qty * price_btc
            spot_f = float(spot)
            amount_usd = amount_btc * spot_f
            fee_usd = fee_btc * spot_f
            running_balance[i] += amount_usd - fee_usd
            _f_combo_idx.append(i)
            _f_trade_idx.append(tidx)
            _f_open_idx.append(open_tidx)
            _f_ts.append(ts)
            _f_event.append(event)
            _f_contract.append(contract)
            _f_side.append(fill_side)
            _f_qty.append(qty)
            _f_price_btc.append(price_btc)
            _f_amount_btc.append(amount_btc)
            _f_fee_btc.append(fee_btc)
            _f_spot.append(spot_f)
            _f_amount_usd.append(amount_usd)
            _f_fee_usd.append(fee_usd)
            _f_balance_usd.append(running_balance[i])
            _f_exit_reason.append(reason)
            _f_status.append(status)

            # Record open trade_idx ON the leg so close fills can link
            # back without needing the position-level pos_id map.
            # Also stamp the entry spot so leg-aware PnL in close_position /
            # partial_close uses the correct spot for legs added later
            # (e.g. via add_legs at a different timestamp).
            if not is_close:
                leg["_open_idx"] = tidx
                leg["entry_spot"] = float(spot)

        if trade_side == 'open':
            if pos_id is not None:
                _pos_open_idx[i][pos_id] = tidx
            for leg in legs:
                _emit(leg, tidx, trade.entry_time, trade.entry_spot,
                      "open", "", trade_status, is_close=False)
            return

        # side == 'close'
        is_partial = bool(trade.metadata.get('partial_close'))
        if pos_id is not None:
            if is_partial:
                # Retain mapping — survivors' eventual close still needs it.
                _open_tidx = _pos_open_idx[i].get(pos_id, None)
            else:
                _open_tidx = _pos_open_idx[i].pop(pos_id, None)
        else:
            _open_tidx = None
        if _open_tidx is None:
            _open_tidx = tidx

        for leg in legs:
            _emit(leg, _open_tidx, trade.exit_time, trade.exit_spot,
                  "close", trade.exit_reason or "", trade_status, is_close=True)

    t0 = _time.time()
    n_states = 0
    last_state = None
    total_states = len(replay)
    _last_print = t0
    _print_interval = 10.0  # seconds between progress lines

    for state in replay:
        n_states += 1
        day_key = state.dt.strftime("%Y-%m-%d")
        for i, strategy in enumerate(instances):
            state.equity_usd = account_size + realized_pnl[i]
            open_pnl = _open_unrealized_pnl(strategy, state, pos_pnl_cache[i])
            state.nav_usd = state.equity_usd + open_pnl
            for trade in strategy.on_market_state(state):
                _append_fills(i, trade)
                if getattr(trade, 'side', 'close') == 'close':
                    _append(i, trade)
                    realized_pnl[i] += float(trade.pnl)

            open_pnl = _open_unrealized_pnl(strategy, state, pos_pnl_cache[i])
            last_open_pnl[i] = open_pnl
            nav = account_size + realized_pnl[i] + open_pnl

            if current_day[i] != day_key:
                if current_day[i] is not None:
                    _nav_combo_idx.append(i)
                    _nav_date.append(current_day[i])
                    _nav_low.append(day_low[i])
                    _nav_high.append(day_high[i])
                    _nav_close.append(day_close[i])
                    _nav_realized_close.append(day_realized_close[i])
                current_day[i] = day_key
                day_low[i] = nav
                day_high[i] = nav
                day_close[i] = nav
                day_realized_close[i] = realized_pnl[i]
            else:
                if nav < day_low[i]:
                    day_low[i] = nav
                if nav > day_high[i]:
                    day_high[i] = nav
                day_close[i] = nav
                day_realized_close[i] = realized_pnl[i]
        last_state = state

        if progress:
            _now = _time.time()
            if _now - _last_print >= _print_interval:
                elapsed = _now - t0
                pct = 100.0 * n_states / total_states if total_states else 0.0
                print(f"  {n_states}/{total_states} states ({pct:.0f}%) — {elapsed:.1f}s elapsed...")
                _last_print = _now

        if progress_cb is not None and n_states % progress_cb_interval == 0:
            try:
                progress_cb(n_states, total_states, day_key)
            except Exception:
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    "progress_cb raised; ignoring", exc_info=True
                )

    if last_state is not None:
        for i, strategy in enumerate(instances):
            for trade in strategy.on_end(last_state):
                _append_fills(i, trade)
                if getattr(trade, 'side', 'close') == 'close':
                    _append(i, trade)
                    realized_pnl[i] += float(trade.pnl)

    # Flush trailing day rows for each combo
    for i in range(n_combos):
        if current_day[i] is None:
            continue
        _nav_combo_idx.append(i)
        _nav_date.append(current_day[i])
        _nav_low.append(day_low[i])
        _nav_high.append(day_high[i])
        _nav_close.append(day_close[i])
        _nav_realized_close.append(day_realized_close[i])

    elapsed = _time.time() - t0
    total_trades = len(_pnl)

    if progress:
        print(
            f"Grid complete: {n_combos} combos × {n_states} states "
            f"= {total_trades:,} trades in {elapsed:.1f}s"
        )

    # Build DataFrame with compact dtypes
    idx_dtype = "int16" if n_combos <= 32767 else "int32"
    df = pd.DataFrame({
        "combo_idx":       pd.array(_combo_idx, dtype=idx_dtype),
        "entry_time":      pd.to_datetime(_entry_time),
        "exit_time":       pd.to_datetime(_exit_time),
        "entry_spot":      pd.array(_entry_spot, dtype="float32"),
        "exit_spot":       pd.array(_exit_spot, dtype="float32"),
        "entry_price_usd": pd.array(_entry_price_usd, dtype="float32"),
        "exit_price_usd":  pd.array(_exit_price_usd, dtype="float32"),
        "fees":            pd.array(_fees, dtype="float32"),
        "pnl":             pd.array(_pnl, dtype="float32"),
        "triggered":       _triggered,
        "exit_reason":     pd.Categorical(_exit_reason),
        "exit_hour":       pd.array(_exit_hour, dtype="int16"),
        "entry_date":      _entry_date,
        "status":          pd.array(_status, dtype="uint16"),
    })

    nav_daily_df = pd.DataFrame({
        "combo_idx": pd.array(_nav_combo_idx, dtype=idx_dtype),
        "date": _nav_date,
        "nav_low": pd.array(_nav_low, dtype="float32"),
        "nav_high": pd.array(_nav_high, dtype="float32"),
        "nav_close": pd.array(_nav_close, dtype="float32"),
        "realized_close": pd.array(_nav_realized_close, dtype="float32"),
    })

    final_nav = [account_size + realized_pnl[i] + last_open_pnl[i] for i in range(n_combos)]
    final_nav_df = pd.DataFrame({
        "combo_idx": pd.array(range(n_combos), dtype=idx_dtype),
        "final_nav": pd.array(final_nav, dtype="float32"),
        "realized_pnl": pd.array(realized_pnl, dtype="float32"),
        "open_pnl": pd.array(last_open_pnl, dtype="float32"),
    })

    # Build fills DataFrame — one row per leg per event, compact categoricals
    if _f_combo_idx:
        df_fills = pd.DataFrame({
            "combo_idx":   pd.array(_f_combo_idx, dtype=idx_dtype),
            "trade_idx":   pd.array(_f_trade_idx, dtype="int32"),
            "open_idx":    pd.array(_f_open_idx, dtype="int32"),
            "ts":          pd.to_datetime(_f_ts),
            "event":       pd.Categorical(_f_event, categories=["open", "close"]),
            "contract":    pd.Categorical(_f_contract),
            "side":        pd.Categorical(_f_side, categories=["sell", "buy"]),
            "qty":         pd.array(_f_qty, dtype="float32"),
            "price_btc":   pd.array(_f_price_btc, dtype="float32"),
            "amount_btc":  pd.array(_f_amount_btc, dtype="float32"),
            "fee_btc":     pd.array(_f_fee_btc, dtype="float32"),
            "spot":        pd.array(_f_spot, dtype="float32"),
            "amount_usd":  pd.array(_f_amount_usd, dtype="float32"),
            "fee_usd":     pd.array(_f_fee_usd, dtype="float32"),
            "balance_usd": pd.array(_f_balance_usd, dtype="float32"),
            "exit_reason": pd.Categorical(_f_exit_reason),
            "status":      pd.array(_f_status, dtype="uint16"),
        })
    else:
        df_fills = pd.DataFrame(columns=[
            "combo_idx", "trade_idx", "open_idx", "ts", "event", "contract",
            "side", "qty", "price_btc", "amount_btc", "fee_btc", "spot",
            "amount_usd", "fee_usd", "balance_usd", "exit_reason", "status",
        ])

    return df, keys, nav_daily_df, final_nav_df, df_fills

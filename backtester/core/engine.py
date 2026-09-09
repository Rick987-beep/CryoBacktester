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

  Inner workers: run_grid_full(..., workers=N) shards already-expanded combos
  across spawn children when N>1 and MarketReplay can reload from parquet.
  workers=1 (the default once combo-count/host caps resolve) is bit-identical
  to the pre-parallel kernel — no Pool is created.

NAV tracking detail:
  Every tick, _open_unrealized_pnl() marks all open positions to market.
  It reads pos._last_reprice_usd (cached by _reprice_legs in strategy_base)
  to avoid calling _reprice_legs twice per position per tick — once during
  the strategy’s SL/TP exit check, and once here for NAV accounting.
  Falls back to a fresh _reprice_legs call if the cache is absent.

Usage:
    from backtester.core.engine import run_grid_full
    df, keys, nav_daily_df, final_nav_df, df_fills = run_grid_full(
        MyStrategy, MY_PARAM_GRID, replay
    )
"""
import itertools
import logging
import multiprocessing
import os
import queue as _queue
import threading
import time as _time
from typing import Any, Dict, List, Optional, Tuple, Type

_log = logging.getLogger(__name__)

from backtester.core.config import cfg as _cfg
from backtester.core.pricing import fee_btc_per_contract as _fee_btc
from backtester.core.strategy_base import Trade, _reprice_legs


def _freeze_indicator_params(params):
    # type: (Any) -> Tuple
    """Stable cache key for IndicatorDep.params (scalars only)."""
    if not params:
        return ()
    return tuple(sorted((str(k), params[k]) for k in params))


def _indicator_deps_for_instance(strategy_cls, instance):
    # type: (Type, Any) -> List[Any]
    """Prefer instance ``resolved_indicator_deps()`` after ``configure()``.

    Returns an empty list when the strategy has no indicator dependencies.
    ``resolved_indicator_deps`` returning ``None`` falls back to class-level
    ``indicator_deps`` (existing strategies).
    """
    resolver = getattr(instance, "resolved_indicator_deps", None)
    if callable(resolver):
        resolved = resolver()
        if resolved is not None:
            return list(resolved)
    deps = getattr(instance, "indicator_deps", None) or getattr(
        strategy_cls, "indicator_deps", None
    )
    return list(deps) if deps else []


def _dep_cache_key(dep):
    # type: (Any) -> Tuple
    return (
        dep.name,
        dep.symbol,
        dep.interval,
        int(getattr(dep, "warmup_days", 30)),
        _freeze_indicator_params(getattr(dep, "params", None)),
    )


def _compute_indicator_cache(strategy_cls, instances, replay, progress, status_cb=None):
    # type: (Type, List[Any], Any, bool, Any) -> Dict[Tuple, Any]
    """Build unique indicator series for ``instances``. Empty dict if none."""
    per_instance = [_indicator_deps_for_instance(strategy_cls, inst) for inst in instances]
    cache = {}  # type: Dict[Tuple, Any]
    if not any(per_instance):
        return cache

    from backtester.indicators import build_indicators

    start_dt, end_dt = replay.date_range()
    build_order = []  # type: List[Any]
    for deps in per_instance:
        for dep in deps:
            key = _dep_cache_key(dep)
            if key not in cache:
                cache[key] = None
                build_order.append(dep)

    if progress:
        names = sorted({d.name for deps in per_instance for d in deps})
        print(
            f"Building indicators {names} "
            f"({len(build_order)} unique, {start_dt.date()} → {end_dt.date()})..."
        )

    for dep in build_order:
        key = _dep_cache_key(dep)
        built = build_indicators([dep], start_dt, end_dt, status_cb=status_cb)
        cache[key] = built[dep.name]

    if progress:
        print(f"  Indicators ready: {len(cache)} unique series")
    return cache


def _apply_indicator_cache(strategy_cls, instances, cache):
    # type: (Type, List[Any], Optional[Dict[Tuple, Any]]) -> None
    if not cache:
        return
    for strategy in instances:
        deps = _indicator_deps_for_instance(strategy_cls, strategy)
        if not deps or not hasattr(strategy, "set_indicators"):
            continue
        strategy.set_indicators({dep.name: cache[_dep_cache_key(dep)] for dep in deps})


def _indicator_cache_for_combos(strategy_cls, combos, extra_params, replay, progress, status_cb=None):
    # type: (Type, List[Dict[str, Any]], Optional[Dict[str, Any]], Any, bool, Any) -> Optional[Dict[Tuple, Any]]
    """Parent-side unique series so spawn children skip a 4× kline rebuild."""
    probes = []
    seen = set()
    for params in combos:
        full_params = dict(params)
        if extra_params:
            full_params.update(extra_params)
        inst = strategy_cls()
        inst.configure(full_params)
        deps = _indicator_deps_for_instance(strategy_cls, inst)
        key = tuple(_dep_cache_key(d) for d in deps)
        if key in seen:
            continue
        seen.add(key)
        probes.append(inst)
    if not probes:
        return None
    cache = _compute_indicator_cache(
        strategy_cls, probes, replay, progress, status_cb,
    )
    return cache or None


def _inject_indicators(strategy_cls, instances, replay, progress, status_cb=None, indicator_cache=None):
    # type: (Type, List[Any], Any, bool, Any, Optional[Dict[Tuple, Any]]) -> None
    """Pre-compute indicators and inject into each instance.

    Unique ``(name, symbol, interval, warmup, params)`` tuples are built once
    so PARAM_GRID can vary indicator knobs without recomputing identical
    series. Each instance still receives a dict keyed by indicator name.
    Pass ``indicator_cache`` from the parent spawn path to skip rebuilds.
    """
    if indicator_cache is None:
        indicator_cache = _compute_indicator_cache(
            strategy_cls, instances, replay, progress, status_cb,
        )
    _apply_indicator_cache(strategy_cls, instances, indicator_cache)

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


def _overlay_mark_pnl(strategy, state):
    # type: (Any, Any) -> float
    """Optional strategy overlays (perp / sticky wing) marked into NAV.

    Strategies may expose ``perp_mark_pnl(spot)`` and/or ``wing_mark_pnl(state)``.
    Missing methods are treated as zero so legacy strategies stay unchanged.
    """
    total = 0.0
    perp = getattr(strategy, "perp_mark_pnl", None)
    if callable(perp):
        try:
            total += float(perp(float(getattr(state, "spot", 0.0) or 0.0)))
        except Exception:
            pass
    wing = getattr(strategy, "wing_mark_pnl", None)
    if callable(wing):
        try:
            total += float(wing(state))
        except Exception:
            pass
    return total


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


def _effective_params_for_key(params, strategy):
    # type: (Dict[str, Any], Any) -> Dict[str, Any]
    """Merge ``describe_params()`` over grid params for combo-key labeling.

    Strategies may lock or rewrite knobs in ``configure()`` (e.g. Mode C
    take-profit).  Combo keys / GUI labels should show the *effective* values,
    not the raw PARAM_GRID placeholders.
    """
    out = dict(params)
    describe = getattr(strategy, "describe_params", None)
    if not callable(describe):
        return out
    try:
        described = describe() or {}
    except Exception:
        return out
    if not isinstance(described, dict):
        return out
    for key in list(out.keys()):
        if key in described:
            out[key] = described[key]
    return out


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
        state.nav_usd = state.equity_usd + open_pnl + _overlay_mark_pnl(strategy, state)
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
        keys.append(_params_to_key(_effective_params_for_key(params, strategy)))

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


def _effective_inner_workers(n_combos, requested, replay, strategy_cls):
    # type: (int, Optional[int], Any, Type) -> int
    """Resolve inner workers; 1 means the legacy single-process kernel."""
    from backtester.core.grid_workers import (
        replay_reload_spec,
        resolve_workers_from_cfg,
        strategy_is_spawnable,
    )

    sharing = os.environ.get("CRYOBT_GRID_SHARE", "1") != "0"
    n_workers = resolve_workers_from_cfg(
        n_combos, requested, sharing=sharing,
    )
    if n_workers <= 1:
        return 1
    if replay_reload_spec(replay) is None:
        _log.info("grid workers: replay has no reload paths; staying single-process")
        return 1
    if not strategy_is_spawnable(strategy_cls):
        _log.info("grid workers: strategy class is not picklable; staying single-process")
        return 1
    return n_workers


def _ensure_child_logging():
    # type: () -> None
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        )


_SHARD_PROGRESS_QUEUE = None  # set in spawn children via Pool initializer


def _init_shard_worker(q):
    # type: (Any) -> None
    global _SHARD_PROGRESS_QUEUE
    _ensure_child_logging()
    _SHARD_PROGRESS_QUEUE = q


def _assert_spawnable_parent():
    # type: () -> None
    """Pool/Process spawn re-imports __main__. stdin/`python -c` cannot be re-run."""
    import sys

    main = sys.modules.get("__main__")
    path = getattr(main, "__file__", None)
    if not path or os.path.basename(str(path)) in ("<stdin>", "<string>"):
        raise RuntimeError(
            "inner grid workers cannot spawn from python -c or stdin "
            "(children re-import __main__ and then respawn forever). "
            "Run via python -m backtester.run, python -m backtester.job, or pytest."
        )


def _shard_proc(payload, progress_queue, result_queue):
    # type: (Dict[str, Any], Any, Any) -> None
    """Process target: one shard, result on ``result_queue``. No Pool respawn."""
    _init_shard_worker(progress_queue)
    shard_i = int(payload.get("shard_i") or 0)
    try:
        result_queue.put((shard_i, True, _run_grid_shard(payload)))
    except Exception as exc:
        try:
            result_queue.put((shard_i, False, exc))
        except Exception:
            pass
        raise


def _terminate_procs(procs):
    # type: (List[Any]) -> None
    for p in procs:
        if p.is_alive():
            try:
                p.terminate()
            except Exception:
                pass
    deadline = _time.time() + 3.0
    for p in procs:
        remaining = deadline - _time.time()
        try:
            p.join(timeout=max(0.05, remaining))
        except Exception:
            pass
    for p in procs:
        if p.is_alive():
            try:
                p.kill()
            except Exception:
                pass
            try:
                p.join(timeout=1.0)
            except Exception:
                pass


def _run_grid_shard(payload):
    # type: (Dict[str, Any]) -> Tuple
    """Spawn child: attach shared replay or reload from paths, run one shard."""
    from backtester.core.market_replay import MarketReplay

    _ensure_child_logging()
    shard_i = int(payload.get("shard_i") or 0)
    n_shards = int(payload.get("n_shards") or 1)
    combos = payload["combos"]
    offset = int(payload.get("combo_offset") or 0)
    q = payload.get("progress_queue") or _SHARD_PROGRESS_QUEUE
    share_meta = payload.get("share_meta")
    spec = payload.get("spec")
    pid = os.getpid()
    _log.info(
        "grid-w%s/%s start pid=%s n_combos=%s combo_off=%s share=%s",
        shard_i + 1, n_shards, pid, len(combos), offset, share_meta is not None,
    )
    print(
        f"[grid-w{shard_i + 1}/{n_shards}] start pid={pid} n_combos={len(combos)} "
        f"combo_off={offset} share={share_meta is not None}",
        flush=True,
    )
    t0 = _time.time()
    replay = None
    try:
        if share_meta is not None:
            replay = MarketReplay.from_shared_meta(share_meta)
        else:
            replay = MarketReplay(
                spec["snapshot_path"],
                spec["spot_track_path"],
                expiry_filter=spec.get("expiry_filter"),
                start=spec.get("start"),
                end=spec.get("end"),
                step_minutes=spec.get("step_minutes", 5),
            )

        def _cb(current, total, day_iso):
            if q is None or shard_i != 0:
                return
            try:
                q.put(("progress", current, total, day_iso))
            except Exception:
                pass

        result = _run_grid_full_combos(
            payload["strategy_cls"],
            combos,
            replay,
            extra_params=payload.get("extra_params"),
            progress=bool(payload.get("progress")),
            progress_cb=_cb if q is not None else None,
            progress_cb_interval=int(payload.get("progress_cb_interval") or 50),
            status_cb=None,
            indicator_cache=payload.get("indicator_cache"),
        )
        elapsed = _time.time() - t0
        _log.info(
            "grid-w%s/%s done pid=%s elapsed=%.1fs trades=%s",
            shard_i + 1, n_shards, pid, elapsed, len(result[0]),
        )
        print(
            f"[grid-w{shard_i + 1}/{n_shards}] done pid={pid} "
            f"elapsed={elapsed:.1f}s trades={len(result[0])}",
            flush=True,
        )
        if q is not None:
            try:
                q.put(("shard_done", shard_i, pid, _time.time() - t0, len(result[0])))
            except Exception:
                pass
        return result
    except Exception:
        _log.exception("grid-w%s/%s failed pid=%s", shard_i + 1, n_shards, pid)
        raise
    finally:
        for shm in getattr(replay, "_shm_holders", None) or []:
            try:
                shm.close()
            except Exception:
                pass


def _run_grid_full_spawn(
    strategy_cls,
    combos,
    replay,
    extra_params,
    progress,
    n_workers,
    status_cb=None,
    progress_cb=None,
    progress_cb_interval=50,
):
    # type: (Type, List[Dict[str, Any]], Any, Optional[Dict[str, Any]], bool, int, Optional[Any], Optional[Any], int) -> Tuple
    from backtester.core.grid_workers import (
        merge_grid_results,
        pack_replay_shared,
        partition_combos,
        replay_reload_spec,
        shard_offsets,
        unlink_replay_shared,
    )

    _assert_spawnable_parent()
    spec = replay_reload_spec(replay)
    share = os.environ.get("CRYOBT_GRID_SHARE", "1") != "0"
    share_meta = None
    holders = []
    indicator_cache = None
    try:
        indicator_cache = _indicator_cache_for_combos(
            strategy_cls, combos, extra_params, replay, progress, status_cb,
        )
    except Exception:
        _log.exception("parent indicator cache failed; children will rebuild")
        indicator_cache = None
    if share:
        try:
            share_meta, holders = pack_replay_shared(replay)
            _log.info("grid spawn: packed %s shared arrays", len(share_meta.get("arrays") or {}))
        except Exception:
            _log.exception("shared replay pack failed; falling back to duplicate-load")
            share_meta = None
            holders = []
    if share_meta is None and spec is None:
        _log.info("grid spawn: no share and no reload paths; single-process")
        return _run_grid_full_combos(
            strategy_cls, combos, replay,
            extra_params=extra_params, progress=progress,
            progress_cb=progress_cb, progress_cb_interval=progress_cb_interval,
            status_cb=status_cb,
        )
    parts = partition_combos(combos, n_workers)
    offsets = shard_offsets(parts)
    ctx = multiprocessing.get_context("spawn")
    progress_queue = ctx.Queue()
    result_queue = ctx.Queue()
    payloads = [
        {
            "strategy_cls": strategy_cls,
            "combos": part,
            "extra_params": extra_params,
            "spec": spec,
            "share_meta": share_meta,
            "progress": bool(progress) and i == 0,
            "progress_cb_interval": progress_cb_interval,
            "shard_i": i,
            "n_shards": len(parts),
            "combo_offset": offsets[i],
            "indicator_cache": indicator_cache,
        }
        for i, part in enumerate(parts)
    ]
    _log.info(
        "grid spawn: %s combos → %s shards sizes=%s share=%s",
        len(combos), len(parts), [len(p) for p in parts], share_meta is not None,
    )
    if progress:
        print(
            f"  spawn {len(parts)} shards sizes={[len(p) for p in parts]} "
            f"share={share_meta is not None} indicator_cache={indicator_cache is not None}",
            flush=True,
        )
    if status_cb is not None:
        status_cb("backtesting", f"Running backtest ({len(parts)} workers)…")

    stop_listen = threading.Event()

    def _listen():
        while True:
            try:
                msg = progress_queue.get(timeout=0.2)
            except _queue.Empty:
                if stop_listen.is_set():
                    break
                continue
            if msg is None:
                break
            kind = msg[0]
            if kind == "progress":
                _cur, _tot, _day = msg[1], msg[2], msg[3]
                if progress_cb is not None:
                    try:
                        progress_cb(_cur, _tot, _day)
                    except Exception:
                        _log.warning("progress_cb raised; ignoring", exc_info=True)
            elif kind == "shard_done":
                _log.info(
                    "grid shard %s done pid=%s elapsed=%.1fs trades=%s",
                    msg[1] + 1, msg[2], msg[3], msg[4],
                )

    listener = threading.Thread(target=_listen, name="grid-progress", daemon=True)
    listener.start()

    shard_timeout = os.environ.get("CRYOBT_GRID_SHARD_TIMEOUT")
    shard_timeout_s = float(shard_timeout) if shard_timeout else None
    procs = []  # type: List[Any]
    shards = [None] * len(payloads)  # type: List[Optional[Tuple]]
    try:
        for payload in payloads:
            p = ctx.Process(
                target=_shard_proc,
                args=(payload, progress_queue, result_queue),
                name=f"grid-w{int(payload['shard_i']) + 1}",
                daemon=False,
            )
            p.start()
            procs.append(p)
            _log.info("grid parent spawned %s pid=%s", p.name, p.pid)
            if progress:
                print(f"  spawned {p.name} pid={p.pid}", flush=True)

        outstanding = set(range(len(procs)))
        t_wait = _time.time()
        last_beat = t_wait
        first_result_s = 30.0
        while outstanding:
            try:
                idx, ok, body = result_queue.get(timeout=0.2)
            except _queue.Empty:
                idx = None
            else:
                outstanding.discard(int(idx))
                if ok:
                    shards[int(idx)] = body
                    _log.info(
                        "grid parent collected shard %s/%s remaining=%s",
                        int(idx) + 1, len(procs), len(outstanding),
                    )
                    if progress:
                        print(
                            f"  collected shard {int(idx) + 1}/{len(procs)} "
                            f"remaining={len(outstanding)}",
                            flush=True,
                        )
                else:
                    raise RuntimeError(
                        f"grid worker shard {int(idx) + 1} failed: {body!r}"
                    ) from (body if isinstance(body, BaseException) else None)

            now = _time.time()
            if now - last_beat >= 10.0:
                _log.info(
                    "grid parent waiting shards=%s elapsed=%.0fs pids=%s",
                    sorted(i + 1 for i in outstanding),
                    now - t_wait,
                    [procs[i].pid for i in sorted(outstanding)],
                )
                last_beat = now
            if shard_timeout_s is not None and (now - t_wait) > shard_timeout_s:
                raise TimeoutError(
                    f"grid shards {sorted(i + 1 for i in outstanding)} still "
                    f"running after {shard_timeout_s:.0f}s"
                )
            if (now - t_wait) > first_result_s and len(outstanding) == len(procs):
                dead = [
                    f"{procs[i].name} pid={procs[i].pid} exit={procs[i].exitcode}"
                    for i in outstanding
                    if procs[i].exitcode not in (None, 0)
                ]
                if dead:
                    raise RuntimeError(
                        "grid workers died before returning a shard: " + "; ".join(dead)
                    )

            for i in list(outstanding):
                p = procs[i]
                if p.exitcode is None:
                    continue
                if p.exitcode != 0:
                    # Exception may already be on the queue; wait one more drain.
                    try:
                        idx2, ok2, body2 = result_queue.get(timeout=0.5)
                    except _queue.Empty:
                        raise RuntimeError(
                            f"grid worker {p.name} pid={p.pid} exited {p.exitcode} "
                            "without a result"
                        )
                    outstanding.discard(int(idx2))
                    if not ok2:
                        raise RuntimeError(
                            f"grid worker shard {int(idx2) + 1} failed: {body2!r}"
                        ) from (body2 if isinstance(body2, BaseException) else None)
                    shards[int(idx2)] = body2

        for p in procs:
            p.join()
        procs = []
    except KeyboardInterrupt:
        _log.warning("grid spawn interrupted; terminating workers")
        _terminate_procs(procs)
        raise
    except BaseException:
        _log.exception("grid spawn abort; terminating workers")
        _terminate_procs(procs)
        raise
    finally:
        try:
            progress_queue.put(None)
        except Exception:
            pass
        stop_listen.set()
        listener.join(timeout=2.0)
        for q in (progress_queue, result_queue):
            try:
                q.close()
                q.join_thread()
            except Exception:
                pass
        unlink_replay_shared(holders)

    master_keys = []
    for _df, keys, *_rest in shards:
        master_keys.extend(keys)
    merged = merge_grid_results(shards, offsets, master_keys)
    _log.info(
        "grid spawn merge: keys=%s trades=%s",
        len(merged[1]), len(merged[0]),
    )
    return merged


def run_grid_full(
    strategy_cls,       # type: Type
    param_grid,         # type: Dict[str, List]
    replay,             # type: Any
    extra_params=None,  # type: Optional[Dict[str, Any]]
    progress=True,      # type: bool
    progress_cb=None,   # type: Optional[Any]  # Callable[[int, int, str], None] | None
    progress_cb_interval=50,  # type: int  # call progress_cb every N states
    status_cb=None,     # type: Optional[Any]  # Callable[[str, str], None] | None
    workers=None,       # type: Optional[int]
):
    """Run all parameter combos in a single pass over market data.

    Accumulates trades into flat lists, then builds a memory-efficient
    pandas DataFrame (~10× less RAM than keeping Trade objects alive).

    ``workers is None`` auto-sizes from host + combo count. ``workers=1``
    (or any resolved value of 1) is the legacy single-process path — no
    child processes are created. ``workers>1`` shards *expanded combos*
    across spawn children sharing a read-only MarketReplay (or duplicate-
    load if ``CRYOBT_GRID_SHARE=0``). Fake / in-memory replays stay on
    the workers=1 path.

    Args:
        strategy_cls: Strategy class (configure/on_market_state/on_end/reset).
        param_grid:   Dict of param_name → list of values.
        replay:       MarketReplay instance (iterable of MarketState).
        extra_params: Optional fixed params merged into every combo.
        progress:     Print progress updates.
        workers:      Requested inner processes. None = auto. 1 = no spawn.

    Returns:
        Tuple of (df, keys, nav_daily_df, final_nav_df, df_fills):
        - df:       pandas DataFrame, one row per closed trade.
                    Column "combo_idx" (int16/int32) is an index into keys.
        - keys:     List[Tuple], where keys[i] is the param tuple for combo_idx i.
        - nav_daily_df: one row per combo/day with nav_low/nav_high/nav_close.
        - final_nav_df: one row per combo with final_nav, realized_pnl, open_pnl.
        - df_fills: one row per leg per event (open/close) across all combos.
    """
    combos = _grid_combos(param_grid)
    n_combos = len(combos)
    n_workers = _effective_inner_workers(n_combos, workers, replay, strategy_cls)

    if progress:
        print(f"Running {n_combos} parameter combos...")
        if n_workers > 1:
            share = os.environ.get("CRYOBT_GRID_SHARE", "1") != "0"
            mode = "shared replay" if share else "duplicate-load"
            print(f"  inner workers: {n_workers} (spawn, {mode})")

    if n_workers <= 1:
        out = _run_grid_full_combos(
            strategy_cls,
            combos,
            replay,
            extra_params=extra_params,
            progress=progress,
            progress_cb=progress_cb,
            progress_cb_interval=progress_cb_interval,
            status_cb=status_cb,
        )
    else:
        out = _run_grid_full_spawn(
            strategy_cls,
            combos,
            replay,
            extra_params=extra_params,
            progress=progress,
            n_workers=n_workers,
            status_cb=status_cb,
            progress_cb=progress_cb,
            progress_cb_interval=progress_cb_interval,
        )
    from backtester.core.grid_workers import worker_run_meta

    df = out[0]
    df.attrs["grid_workers"] = worker_run_meta(
        workers, n_workers,
        sharing=os.environ.get("CRYOBT_GRID_SHARE", "1") != "0",
    )
    return out


def _run_grid_full_combos(
    strategy_cls,       # type: Type
    combos,             # type: List[Dict[str, Any]]
    replay,             # type: Any
    extra_params=None,  # type: Optional[Dict[str, Any]]
    progress=True,      # type: bool
    progress_cb=None,   # type: Optional[Any]
    progress_cb_interval=50,  # type: int
    status_cb=None,     # type: Optional[Any]
    indicator_cache=None,  # type: Optional[Dict[Tuple, Any]]
):
    """Single-process kernel: one replay pass over an expanded combo list."""
    import pandas as pd

    n_combos = len(combos)

    instances = []  # type: List[Any]
    keys = []       # type: List[Tuple]
    for params in combos:
        full_params = dict(params)
        if extra_params:
            full_params.update(extra_params)
        strategy = strategy_cls()
        strategy.configure(full_params)
        instances.append(strategy)
        keys.append(_params_to_key(_effective_params_for_key(params, strategy)))

    # Inject pre-computed indicators if strategy declares dependencies
    _inject_indicators(
        strategy_cls, instances, replay, progress,
        status_cb=status_cb, indicator_cache=indicator_cache,
    )

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
    _f_comment = []
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

        def _emit(leg, open_tidx, ts, spot, event, reason, status, is_close, comment=""):
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
            _f_comment.append(comment)
            _f_status.append(status)

            # Record open trade_idx ON the leg so close fills can link
            # back without needing the position-level pos_id map.
            # Also stamp the entry spot so leg-aware PnL in close_position /
            # partial_close uses the correct spot for legs added later
            # (e.g. via add_legs at a different timestamp).
            if not is_close:
                leg["_open_idx"] = tidx
                leg["entry_spot"] = float(spot)

        comment = str(trade.metadata.get("comment", "") or "")

        if trade_side == 'open':
            if pos_id is not None:
                _pos_open_idx[i][pos_id] = tidx
            for leg in legs:
                _emit(leg, tidx, trade.entry_time, trade.entry_spot,
                      "open", "", trade_status, is_close=False, comment=comment)
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
                  "close", trade.exit_reason or "", trade_status,
                  is_close=True, comment=comment)

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
            overlay = _overlay_mark_pnl(strategy, state)
            state.nav_usd = state.equity_usd + open_pnl + overlay
            for trade in strategy.on_market_state(state):
                _append_fills(i, trade)
                if getattr(trade, 'side', 'close') == 'close':
                    _append(i, trade)
                    realized_pnl[i] += float(trade.pnl)

            open_pnl = _open_unrealized_pnl(strategy, state, pos_pnl_cache[i])
            overlay = _overlay_mark_pnl(strategy, state)
            last_open_pnl[i] = open_pnl + overlay
            nav = account_size + realized_pnl[i] + last_open_pnl[i]

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

    if progress_cb is not None and n_states:
        try:
            progress_cb(n_states, total_states, day_key)
        except Exception:
            _log.warning("progress_cb raised; ignoring", exc_info=True)

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
            "comment":     pd.array(_f_comment, dtype="object"),
            "status":      pd.array(_f_status, dtype="uint16"),
        })
    else:
        df_fills = pd.DataFrame(columns=[
            "combo_idx", "trade_idx", "open_idx", "ts", "event", "contract",
            "side", "qty", "price_btc", "amount_btc", "fee_btc", "spot",
            "amount_usd", "fee_usd", "balance_usd", "exit_reason", "comment",
            "status",
        ])

    extra = _collect_extra_parquets(instances, keys)
    if extra:
        df.attrs["extra_parquets"] = extra

    return df, keys, nav_daily_df, final_nav_df, df_fills


def _collect_extra_parquets(instances, keys):
    """Duck-typed combo sidecars. Does not change the run_grid_full 5-tuple."""
    rows = []
    for i, strat in enumerate(instances):
        fn = getattr(strat, "investor_greeks_sidecar", None)
        if not callable(fn):
            continue
        try:
            row = fn()
        except Exception:
            _log.warning(
                "investor_greeks_sidecar failed for combo %s", i, exc_info=True,
            )
            continue
        if not row:
            continue
        out = dict(row)
        out["combo_idx"] = i
        if i < len(keys):
            for k, v in keys[i]:
                out[k] = v
        rows.append(out)
    if not rows:
        return {}
    import pandas as pd
    return {"investor_greeks.parquet": pd.DataFrame(rows)}

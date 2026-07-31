#!/usr/bin/env python3
"""
run.py — CLI entry point for backtester V2.

Usage:
    python -m backtester.run
    python -m backtester.run --strategy straddle
    python -m backtester.run --strategy put_sell
    python -m backtester.run --strategy straddle --output report.html
"""
import argparse
import os
import sys
import time
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backtester.core.market_replay import MarketReplay
from backtester.core.engine import run_grid_full
from backtester.core.results import GridResult
from backtester.reporting.html_report import generate_html
from backtester.research.walk_forward import run_walk_forward
from backtester.research.experiment import load_experiment
from backtester.core.config import cfg as _cfg
from backtester.core.paths import runs_dir
from workspace.catalog import family_for, strategies_dict

# ── Strategy Registry (façade) ───────────────────────────────────
#
# Canonical registration lives in workspace.catalog.  Stable strategy
# IDs must never be renamed (bundles, favourites, livecompare).
# Legacy strategies live in backtester/archive/strategies_to_be_fixed/

STRATEGIES = strategies_dict()

DEFAULT_OPTIONS = _cfg.data.options_parquet
DEFAULT_SPOT = _cfg.data.spot_parquet


# ── run_backtest() — callable by worker + tests ───────────────────

def run_backtest(
    strategy_key,
    param_grid,
    date_range,
    account_size,
    bundles_root,
    options_path=None,
    spot_path=None,
    progress_cb=None,
    status_cb=None,
    source="cli",
):
    """Run a discovery backtest and write both an HTML report and a run bundle.

    Args:
        strategy_key:  Key in STRATEGIES dict.
        param_grid:    {param: [values]} dict.
        date_range:    (date_from, date_to) strings or (None, None) for strategy default.
        account_size:  Virtual account size in USD.
        bundles_root:  Directory for HTML reports and .bundle/ dirs.
        options_path:  Override parquet path (default: config).
        spot_path:     Override parquet path (default: config).
        progress_cb:   Optional callable(current, total, date_iso) — called every 50 states.
        status_cb:     Optional callable(phase, msg) — called at phase transitions:
                       "loading_data", "building_indicators", "backtesting".
        source:        "cli" | "ui" recorded in bundle meta.

    Returns:
        pathlib.Path pointing to the .bundle/ directory.
    """
    import pathlib

    strategy_cls = STRATEGIES[strategy_key]
    opts = options_path or DEFAULT_OPTIONS
    spot = spot_path or DEFAULT_SPOT

    date_from, date_to = date_range if date_range else (None, None)
    if date_from is None and date_to is None:
        date_from, date_to = getattr(strategy_cls, "DATE_RANGE", (None, None))

    if status_cb is not None:
        status_cb("loading_data", "Loading price data…")

    t0 = time.time()
    replay = MarketReplay(opts, spot, start=date_from, end=date_to)

    t1 = time.time()
    df, keys, nav_daily_df, final_nav_df, df_fills = run_grid_full(
        strategy_cls, param_grid, replay,
        progress_cb=progress_cb,
        status_cb=status_cb,
    )
    grid_time = time.time() - t1

    first_dt, last_dt = replay.time_range
    actual_date_range = (first_dt.strftime("%Y-%m-%d"), last_dt.strftime("%Y-%m-%d"))

    result = GridResult(
        df, keys, nav_daily_df, final_nav_df,
        param_grid=param_grid,
        account_size=float(account_size),
        date_range=actual_date_range,
        df_fills=df_fills,
    )

    html = generate_html(
        strategy_name=strategy_cls.name,
        result=result,
        n_intervals=len(replay._timestamps),
        runtime_s=grid_time,
        strategy_description=getattr(strategy_cls, "DESCRIPTION", ""),
    )

    reports_dir = pathlib.Path(bundles_root)
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    html_path = reports_dir / f"{strategy_key}_{ts}.html"
    html_path.write_text(html)

    # Write bundle
    _ui_state_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "ui", "state"
    )
    from backtester.ui.services.store_service import StoreService
    _store = StoreService(str(_ui_state_dir), str(reports_dir))
    bundle_path = _store.write_bundle(
        result, strategy=strategy_key, runtime_s=grid_time, source=source,
        strategy_cls=strategy_cls, family=family_for(strategy_key),
    )
    _store.register_bundle(bundle_path)

    return bundle_path


# ── Main ─────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backtester V2")
    parser.add_argument("--strategy", default="short_str_turb_dyn",
                        choices=list(STRATEGIES.keys()))
    parser.add_argument("--options", default=DEFAULT_OPTIONS)
    parser.add_argument("--spot", default=DEFAULT_SPOT)
    parser.add_argument("--output", default=None)
    parser.add_argument("--robustness", action="store_true",
                        help="Include robustness analysis section in report (distribution, "
                             "marginal charts, all-combos table). Off by default for "
                             "fast discovery runs.")
    parser.add_argument("--wfo", action="store_true",
                        help="Run walk-forward validation and append a WFO section to the "
                             "report.  Uses DATE_RANGE from the strategy class.")
    parser.add_argument("--is-days", type=int, default=45, metavar="N",
                        help="In-sample window length in calendar days (default: 45).")
    parser.add_argument("--oos-days", type=int, default=15, metavar="N",
                        help="Out-of-sample window length in calendar days (default: 15).")
    parser.add_argument("--step-days", type=int, default=15, metavar="N",
                        help="Window shift per step in calendar days (default: 15).")
    parser.add_argument("--experiment", default=None, metavar="NAME",
                        help="Experiment name (backtester/experiments/<name>.toml). "
                             "Use with --mode sensitivity or --mode wfo.")
    parser.add_argument("--mode", default="discovery",
                        choices=["discovery", "sensitivity", "wfo"],
                        help="Run mode: discovery (full PARAM_GRID), sensitivity "
                             "(experiment grid around best params), wfo (walk-forward).")
    parser.add_argument("--no-bundle", action="store_true",
                        help="Skip writing a run bundle (no .bundle/ dir next to the HTML).")
    args = parser.parse_args()

    # ── Resolve strategy, param_grid, and WFO window params ───────
    if args.experiment:
        exp = load_experiment(args.experiment)
        strategy_cls = STRATEGIES[exp.strategy]
        if args.mode == "sensitivity":
            param_grid = exp.build_sensitivity_grid()
            args.robustness = True   # always include robustness for sensitivity runs
        else:
            param_grid = strategy_cls.PARAM_GRID
        wfo_is_days   = exp.wfo_is_days
        wfo_oos_days  = exp.wfo_oos_days
        wfo_step_days = exp.wfo_step_days
    else:
        strategy_cls = STRATEGIES[args.strategy]
        param_grid    = strategy_cls.PARAM_GRID
        wfo_is_days   = args.is_days
        wfo_oos_days  = args.oos_days
        wfo_step_days = args.step_days

    print(f"\n{'='*60}")
    print(f"  Backtester V2 — {strategy_cls.name}")
    print(f"{'='*60}")

    # Load data
    t0 = time.time()
    date_range_filter = getattr(strategy_cls, "DATE_RANGE", (None, None))
    replay = MarketReplay(args.options, args.spot,
                         start=date_range_filter[0], end=date_range_filter[1])
    print(f"  Data loaded: {len(replay._timestamps):,} intervals in {time.time()-t0:.1f}s")

    # Run grid
    t1 = time.time()
    df, keys, nav_daily_df, final_nav_df, df_fills = run_grid_full(
        strategy_cls, param_grid, replay
    )
    grid_time = time.time() - t1

    n_combos = len(keys)
    total_trades = len(df)
    print(f"  {n_combos:,} combos, {total_trades:,} trades in {grid_time:.1f}s")

    # Date range from spot data
    first_dt = datetime.fromtimestamp(
        int(replay._spot_ts[0]) / 1_000_000, tz=timezone.utc)
    last_dt = datetime.fromtimestamp(
        int(replay._spot_ts[-1]) / 1_000_000, tz=timezone.utc)
    date_range = (first_dt.strftime("%Y-%m-%d"), last_dt.strftime("%Y-%m-%d"))

    # Generate GridResult
    account_size = float(_cfg.simulation.account_size_usd)
    result = GridResult(
        df, keys, nav_daily_df, final_nav_df,
        param_grid=param_grid,
        account_size=account_size,
        date_range=date_range,
        df_fills=df_fills,
    )

    # Walk-forward validation (optional)
    wfo_result = None
    if args.wfo or args.mode == "wfo":
        wfo_result = run_walk_forward(
            strategy_cls=strategy_cls,
            options_path=args.options,
            spot_path=args.spot,
            is_days=wfo_is_days,
            oos_days=wfo_oos_days,
            step_days=wfo_step_days,
            account_size=account_size,
        )

    html = generate_html(
        strategy_name=strategy_cls.name,
        result=result,
        n_intervals=len(replay._timestamps),
        runtime_s=grid_time,
        strategy_description=getattr(strategy_cls, "DESCRIPTION", ""),
        robustness=args.robustness,
        wfo_result=wfo_result,
        status_labels=getattr(strategy_cls, "TRADE_STATUS", getattr(strategy_cls, "STATUS_LABELS", None)),
    )

    reports_dir = str(runs_dir())
    os.makedirs(reports_dir, exist_ok=True)
    report_stem = args.experiment or args.strategy
    if args.mode != "discovery":
        report_stem = f"{report_stem}_{args.mode}"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = args.output or os.path.join(reports_dir, f"{report_stem}_{ts}.html")
    with open(output_path, "w") as f:
        f.write(html)

    print(f"\n  Report: {output_path}")

    # ── Write run bundle (for UI) ──────────────────────────────────
    if not getattr(args, "no_bundle", False):
        try:
            from backtester.ui.services.store_service import StoreService
            _ui_state_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "ui", "state"
            )
            _store = StoreService(_ui_state_dir, reports_dir)
            _fam_key = exp.strategy if args.experiment else args.strategy
            _bundle_path = _store.write_bundle(
                result,
                strategy=report_stem,
                runtime_s=grid_time,
                source="cli",
                wfo_result=wfo_result,
                strategy_cls=strategy_cls,
                family=family_for(_fam_key),
            )
            _store.register_bundle(_bundle_path)
            print(f"  Bundle: {_bundle_path}")
        except Exception as _bundle_exc:
            print(f"  Bundle: skipped ({_bundle_exc})")

    print(f"  Total:  {time.time()-t0:.1f}s\n")


def _fmt_val(v):
    if isinstance(v, float) and v != int(v):
        return f"{v:.2f}"
    return str(int(v) if isinstance(v, float) else v)


if __name__ == "__main__":
    main()


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

from backtester.market_replay import MarketReplay
from backtester.engine import run_grid_full
from backtester.results import GridResult
from backtester.reporting_v2 import generate_html
from backtester.walk_forward import run_walk_forward
from backtester.experiment import load_experiment
from backtester.strategies.l_straddle_index_move import LStraddleIndexMove
from backtester.strategies.daily_put_sell import DailyPutSell
from backtester.strategies.short_generic import ShortGeneric
from backtester.strategies.deltaswipswap import DeltaSwipSwap
from backtester.strategies.short_strangle_weekly_cap import ShortStrangleWeeklyCap
from backtester.strategies.batman_calendar import BatmanCalendar
from backtester.strategies.short_str_turb_dyn import ShortStrTurbDyn
from backtester.strategies.ss_turb_dyn_sl import SsTurbDynSl
from backtester.strategies.ss_turb_dyn_mk2 import SsTurbDynMk2
from backtester.strategies.bt_supertrend_lc import BtSupertrendLc
from backtester.strategies.long_gamma_whitelist import LongGammaWhitelist
from backtester.strategies.preopen_straddle import PreopenStraddle
from backtester.strategies.hedged_put_sell import HedgedPutSell
from backtester.config import cfg as _cfg

# ── Strategy Registry ────────────────────────────────────────────

STRATEGIES = {
    "straddle": LStraddleIndexMove,
    "l_straddle_index_move": LStraddleIndexMove,
    "put_sell": DailyPutSell,
    "delta_strangle_tp": ShortGeneric,
    "short_option":       ShortGeneric,
    "short_generic":      ShortGeneric,
    "deltaswipswap": DeltaSwipSwap,
    "weekly_strangle_cap": ShortStrangleWeeklyCap,
    "batman_calendar": BatmanCalendar,
    "short_str_turb_dyn": ShortStrTurbDyn,
    "ss_turb_dyn_sl": SsTurbDynSl,
    "ss_turb_dyn_mk2": SsTurbDynMk2,
    "bt_supertrend_lc": BtSupertrendLc,
    "long_gamma_whitelist": LongGammaWhitelist,
    "preopen_straddle": PreopenStraddle,
    "hedged_put_sell": HedgedPutSell,
}

DEFAULT_OPTIONS = _cfg.data.options_parquet
DEFAULT_SPOT = _cfg.data.spot_parquet


# ── Main ─────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backtester V2")
    parser.add_argument("--strategy", default="straddle",
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

    reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
    os.makedirs(reports_dir, exist_ok=True)
    report_stem = args.experiment or args.strategy
    if args.mode != "discovery":
        report_stem = f"{report_stem}_{args.mode}"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = args.output or os.path.join(reports_dir, f"{report_stem}_{ts}.html")
    with open(output_path, "w") as f:
        f.write(html)

    print(f"\n  Report: {output_path}")
    print(f"  Total:  {time.time()-t0:.1f}s\n")


def _fmt_val(v):
    if isinstance(v, float) and v != int(v):
        return f"{v:.2f}"
    return str(int(v) if isinstance(v, float) else v)


if __name__ == "__main__":
    main()


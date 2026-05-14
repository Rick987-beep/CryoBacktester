"""
run_ui_test.py — Quick 36-combo test run for UI development / manual tests.

Usage:
    python -m backtester.run_ui_test

Produces a run bundle in backtester/reports/ with 36 combos over the last
~90 days of data.  Typical runtime: 10-25 s on a developer Mac.

DO NOT use for research — this grid is trimmed purely for fast manual testing.
"""
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
from backtester.config import cfg as _cfg
from backtester.strategies.short_generic import ShortGeneric

# ── Trimmed grid: 2 × 3 × 2 × 3 = 36 combos ─────────────────────────────────
_TEST_GRID = {
    "leg_type":         ["strangle"],
    "dte":              [1],
    "delta":            [0.24],
    "entry_hour":       [3, 9],
    "stop_loss_pct":    [0, 4.0, 6.0],
    "take_profit_pct":  [0, 0.5],
    "max_hold_hours":   [0],
    "skip_weekends":    [1],
    "min_otm_pct":      [4, 5, 6],
}

# ~90-day window inside the strategy's full date range
_DATE_FROM = "2026-01-15"
_DATE_TO   = "2026-04-21"


def main():
    t0 = time.time()
    options_path = _cfg.data.options_parquet
    spot_path    = _cfg.data.spot_parquet

    print(f"\n{'='*55}")
    print(f"  UI Test Run — short_generic (36 combos, ~90 days)")
    print(f"{'='*55}")

    replay = MarketReplay(options_path, spot_path,
                         start=_DATE_FROM, end=_DATE_TO)
    print(f"  Loaded {len(replay._timestamps):,} intervals")

    t1 = time.time()
    df, keys, nav_daily_df, final_nav_df, df_fills = run_grid_full(
        ShortGeneric, _TEST_GRID, replay, progress=True
    )
    grid_time = time.time() - t1

    date_range = (_DATE_FROM, _DATE_TO)
    account_size = float(_cfg.simulation.account_size_usd)

    result = GridResult(
        df, keys, nav_daily_df, final_nav_df,
        param_grid=_TEST_GRID,
        account_size=account_size,
        date_range=date_range,
        df_fills=df_fills,
    )

    reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
    os.makedirs(reports_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    html = generate_html(
        strategy_name=f"{ShortGeneric.name} [ui-test]",
        result=result,
        n_intervals=len(replay._timestamps),
        runtime_s=grid_time,
        strategy_description="36-combo test grid for UI development.",
    )
    html_path = os.path.join(reports_dir, f"short_generic_uitest_{ts}.html")
    with open(html_path, "w") as f:
        f.write(html)
    print(f"\n  Report: {html_path}")

    # Write bundle for UI
    try:
        from backtester.ui.services.store_service import StoreService
        _ui_state_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "ui", "state"
        )
        _store = StoreService(_ui_state_dir, reports_dir)
        bundle_path = _store.write_bundle(
            result, strategy="short_generic_uitest",
            runtime_s=grid_time, source="cli"
        )
        _store.register_bundle(bundle_path)
        print(f"  Bundle: {bundle_path}")
    except Exception as exc:
        print(f"  Bundle: skipped ({exc})")

    print(f"  Total:  {time.time()-t0:.1f}s\n")


if __name__ == "__main__":
    main()

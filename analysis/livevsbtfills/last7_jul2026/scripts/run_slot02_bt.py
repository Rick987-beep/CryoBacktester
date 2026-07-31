#!/usr/bin/env python3
"""Run tudysho_eisbach with current slot-02 trade params (single combo)."""
from __future__ import annotations

import json
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PKG = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtester.core.engine import run_grid_full
from backtester.core.market_replay import MarketReplay
from backtester.core.config import cfg
from backtester.strategies.tudysho_eisbach import TuDyShoEisbach

SLOT02_PARAMS = {
    "nav_premium_pct": 0.8,
    "max_qty_per_1btc_equity": 12,
    "leg_min_price": 0,
    "equity_drawdown_stop_pct": 0,
    "equity_sl_only_final_hours": 0,
    "equity_sl_except_final_hours": 0,
    "schedule_mon_thu": {
        "entry_time": "13:00",
        "dte": 1,
        "delta": 0.05,
        "min_otm_pct": 2.6,
        "turbulence_threshold": 60.0,
        "stop_loss_pct": 4.0,
        "proximity_stop_hours": 8.0,
        "proximity_buffer_usd": 500.0,
        "premium_sl_except_final_hours": 8.0,
        "watch_until_utc_midnight": True,
    },
    "schedule_mon_early": {
        "turbulence_threshold": 999.0,
    },
    "schedule_fri": {
        "entry_time": "12:00",
        "dte": 1,
        "delta": 0.1,
        "min_otm_pct": 2.4,
        "turbulence_threshold": 99.0,
        "stop_loss_pct": 4.0,
        "proximity_stop_hours": 4.0,
        "proximity_buffer_usd": 0.0,
        "premium_sl_except_final_hours": 4.0,
        "watch_until_utc_midnight": True,
    },
}

DATE_FROM = "2026-07-19"
DATE_TO = "2026-07-29"
ACCOUNT = 100_000.0


def _param_grid_single(params: dict) -> dict:
    return {k: [v] for k, v in params.items()}


def _write_bundle(df, keys, nav_daily_df, final_nav_df, df_fills, runtime_s: float) -> Path:
    reports = ROOT / "backtester/reports"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    bundle = reports / f"tudysho_eisbach_slot02_{ts}.bundle"
    bundle.mkdir(parents=True, exist_ok=True)

    df.to_parquet(bundle / "trade_log.parquet", index=False)
    nav_daily_df.to_parquet(bundle / "nav_daily.parquet", index=False)
    final_nav_df.to_parquet(bundle / "final_nav.parquet", index=False)
    if df_fills is not None and not df_fills.empty:
        df_fills.to_parquet(bundle / "fills.parquet", index=False)

    keys_serial = [[[k, v] for k, v in key] for key in keys]
    meta = {
        "strategy": "tudysho_eisbach",
        "param_grid": _param_grid_single(SLOT02_PARAMS),
        "keys": keys_serial,
        "date_range": [DATE_FROM, DATE_TO],
        "account_size": ACCOUNT,
        "runtime_s": runtime_s,
        "source": "analysis_slot02_last7",
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "n_combos": len(keys),
        "n_trades": int(len(df)),
        "note": "Slot-02 live params Jul 2026; mon_early disabled via turb=999",
    }
    (bundle / "meta.json").write_text(json.dumps(meta, indent=2, default=str))

    strat_dir = bundle / "strategy"
    strat_dir.mkdir(exist_ok=True)
    src = ROOT / "backtester/strategies/tudysho_eisbach.py"
    shutil.copy2(src, strat_dir / src.name)
    return bundle


def main() -> None:
    t0 = time.time()
    replay = MarketReplay(
        cfg.data.options_parquet,
        cfg.data.spot_parquet,
        start=DATE_FROM,
        end=DATE_TO,
    )
    df, keys, nav_daily_df, final_nav_df, df_fills = run_grid_full(
        TuDyShoEisbach,
        _param_grid_single(SLOT02_PARAMS),
        replay,
    )
    runtime = time.time() - t0
    bundle = _write_bundle(df, keys, nav_daily_df, final_nav_df, df_fills, runtime)

    meta_path = PKG / "metadata.json"
    base = json.loads(meta_path.read_text())
    base["backtest"]["bundle"] = str(bundle.relative_to(ROOT))
    base["backtest"]["n_trades"] = int(len(df))
    meta_path.write_text(json.dumps(base, indent=2) + "\n")
    print(f"Trades: {len(df)}  runtime: {runtime:.1f}s")
    print(f"Bundle: {bundle}")


if __name__ == "__main__":
    main()

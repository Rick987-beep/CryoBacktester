"""Run backtester single-combo grid for resolved live params."""
from __future__ import annotations

import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from backtester.compare.io_utils import log_stage
from backtester.core.config import cfg
from backtester.core.engine import run_grid_full
from backtester.core.market_replay import MarketReplay
from backtester.run import STRATEGIES


def run(
    bt_strategy: str,
    param_grid: Dict[str, List[Any]],
    date_from: str,
    date_to: str,
    account_size: float,
    bundles_root: Path,
    label: str = "livecompare",
) -> Path:
    strategy_cls = STRATEGIES.get(bt_strategy)
    if strategy_cls is None:
        raise ValueError(f"Unknown BT strategy: {bt_strategy}")

    t0 = time.time()
    replay = MarketReplay(
        cfg.data.options_parquet,
        cfg.data.spot_parquet,
        start=date_from,
        end=date_to,
    )
    df, keys, nav_daily_df, final_nav_df, df_fills = run_grid_full(
        strategy_cls, param_grid, replay,
    )
    runtime = time.time() - t0

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    bundle = bundles_root / f"{bt_strategy}_{label}_slot_{ts}.bundle"
    bundle.mkdir(parents=True, exist_ok=True)
    df.to_parquet(bundle / "trade_log.parquet", index=False)
    nav_daily_df.to_parquet(bundle / "nav_daily.parquet", index=False)
    final_nav_df.to_parquet(bundle / "final_nav.parquet", index=False)
    if df_fills is not None and not df_fills.empty:
        df_fills.to_parquet(bundle / "fills.parquet", index=False)

    keys_serial = [[[k, v] for k, v in key] for key in keys]
    meta = {
        "strategy": bt_strategy,
        "param_grid": param_grid,
        "keys": keys_serial,
        "date_range": [date_from, date_to],
        "account_size": account_size,
        "runtime_s": runtime,
        "source": label,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "n_combos": len(keys),
        "n_trades": int(len(df)),
    }
    (bundle / "meta.json").write_text(json.dumps(meta, indent=2, default=str))

    strat_dir = bundle / "strategy"
    strat_dir.mkdir(exist_ok=True)
    mod = Path(strategy_cls.__module__.replace(".", "/") + ".py")
    src = Path(__file__).resolve().parents[1] / mod.name
    # copy from strategies package
    src = Path(__file__).resolve().parents[1] / "strategies" / f"{bt_strategy}.py"
    if src.exists():
        shutil.copy2(src, strat_dir / src.name)

    log_stage("run_bt_done", bundle=str(bundle), n_trades=len(df), runtime_s=round(runtime, 2))
    return bundle

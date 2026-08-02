#!/usr/bin/env python3
"""Offline QA: VRP fire rates and gaps over theta_engine_v8 DATE_RANGE."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backtester.indicators.hist_data import load_klines
from backtester.indicators.vol_context import build_vol_context
from workspace.strategies.theta_engine.v8 import ThetaEngineV8


def main() -> int:
    start_s, end_s = ThetaEngineV8.DATE_RANGE
    start = datetime.fromisoformat(start_s).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(end_s).replace(tzinfo=timezone.utc)

    print(f"DATE_RANGE {start_s} → {end_s}")
    df = load_klines("BTCUSDT", "1d", start, end, warmup_days=60)
    panel = build_vol_context(df)
    # Restrict to analysis window
    panel = panel.loc[(panel.index >= start) & (panel.index <= end)]
    weekdays = panel[panel.index.dayofweek < 5].copy()

    print(f"weekday rows: {len(weekdays)}")
    print(f"dvol coverage: {weekdays['dvol'].notna().mean():.1%}")
    print(f"rv30 coverage: {weekdays['rv30'].notna().mean():.1%}")
    print(f"vrp  coverage: {weekdays['vrp'].notna().mean():.1%}")
    print()
    print(weekdays[["dvol", "rv30", "vrp"]].describe().round(2).to_string())
    print()

    for thr in (0.0, 3.0, 5.0):
        mask = weekdays["vrp"] >= thr
        fire = weekdays.loc[mask]
        if fire.empty:
            print(f"VRP>={thr}: 0% fire")
            continue
        # gaps between successive fire days (calendar)
        gaps = fire.index.to_series().diff().dt.days.dropna()
        print(
            f"VRP>={thr}: fire={mask.mean():.1%} n={mask.sum()} "
            f"gap_mean={gaps.mean():.1f} gap_max={gaps.max():.0f}"
        )

    out = Path("analysis") / "theta_engine_v8_vrp_qa.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    weekdays.to_parquet(out)
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

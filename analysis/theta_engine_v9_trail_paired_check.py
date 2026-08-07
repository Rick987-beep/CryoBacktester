#!/usr/bin/env python3
"""Paired exit check for theta_engine_v9 trail candidate (tests 1 + 5).

Compares on ``fav_sharpe_rich4_f5_1600`` only (same DATE_RANGE as v9)::

    fixed TP=0.60   — locked Mode C control
    fixed TP=0.65   — ablation: is trail just a higher fixed TP?
    trail arm=0.65 / giveback=0.05 — fav#114 candidate

(1) Pair trades that share the same entry_time across combos;
    report mean ΔPnL (trail − fixed) with bootstrap 95% CI.
(5) Aggregate trail vs fixed 0.60 and vs fixed 0.65.

Usage::

    PYTHONPATH=. python analysis/theta_engine_v9_trail_paired_check.py
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from backtester.core.config import cfg as _cfg
from backtester.core.engine import run_grid_full
from backtester.core.market_replay import MarketReplay
from backtester.core.paths import runs_dir
from workspace.strategies.theta_engine.v9 import ThetaEngineV9

ENTRY = "fav_sharpe_rich4_f5_1600"
N_BOOT = 5000
RNG = np.random.default_rng(42)

BOOK = {
    "delta": [0.25],
    "min_dte": [90],
    "hedge_delta": [0.0],
    "hedge_qty_mult": [0],
    "hold_days": [0],
    "stop_loss_pct": [3.0],
    "trail_arm": [0.65],
    "trail_giveback": [0.05],
    "tp_age_early": [14],
    "tp_age_late": [45],
    "tp_early": [0.70],
    "tp_mid": [0.50],
    "tp_late": [0.35],
    "max_concurrent": [20],
    "qty_per_1btc_equity": [0.2],
    "launch_accel": [0],
    "launch_size_mult": [1.0],
    "entry_policy": [ENTRY],
}


def _base_grid(**overrides: Any) -> Dict[str, List]:
    g = {k: list(v) for k, v in BOOK.items()}
    g.update(overrides)
    return g


def _combo_name(key: Tuple) -> Optional[str]:
    d = dict(key)
    mode = str(d.get("exit_mode"))
    tp = float(d.get("take_profit_pct", 0.0))
    if mode == "fixed" and abs(tp - 0.60) < 1e-9:
        return "fixed_0.60"
    if mode == "fixed" and abs(tp - 0.65) < 1e-9:
        return "fixed_0.65"
    if (
        mode == "trail"
        and abs(tp) < 1e-9
        and abs(float(d.get("trail_arm", 0)) - 0.65) < 1e-9
        and abs(float(d.get("trail_giveback", 0)) - 0.05) < 1e-9
    ):
        return "trail_0.65_0.05"
    return None


def _pair_delta(
    a: pd.DataFrame, b: pd.DataFrame, name_a: str, name_b: str
) -> pd.DataFrame:
    """Inner-join on entry_time (+ cumcount); Δ = b.pnl − a.pnl."""
    cols = ["entry_time", "exit_time", "pnl", "exit_reason", "entry_price_usd", "fees"]
    left = a[cols].copy()
    right = b[cols].copy()
    left["entry_time"] = pd.to_datetime(left["entry_time"], utc=True)
    right["entry_time"] = pd.to_datetime(right["entry_time"], utc=True)
    left["_k"] = left.groupby("entry_time").cumcount()
    right["_k"] = right.groupby("entry_time").cumcount()
    m = left.merge(
        right,
        on=["entry_time", "_k"],
        how="inner",
        suffixes=("_" + name_a, "_" + name_b),
    )
    m["delta_pnl"] = m["pnl_" + name_b] - m["pnl_" + name_a]
    return m


def _bootstrap_mean_ci(x: np.ndarray, n_boot: int = N_BOOT) -> Tuple[float, float, float]:
    x = np.asarray(x, dtype=float)
    if len(x) == 0:
        return float("nan"), float("nan"), float("nan")
    mean = float(x.mean())
    boots = RNG.choice(x, size=(n_boot, len(x)), replace=True).mean(axis=1)
    lo, hi = np.quantile(boots, [0.025, 0.975])
    return mean, float(lo), float(hi)


def _agg(df: pd.DataFrame, nav: pd.DataFrame, idx: int) -> Dict[str, float]:
    sub = df[df.combo_idx == idx]
    nsub = nav[nav.combo_idx == idx].sort_values("date")
    s = nsub["nav_close"].astype(float)
    rets = s.pct_change().dropna()
    sharpe = (
        float(rets.mean() / rets.std() * np.sqrt(365))
        if len(rets) > 2 and rets.std() > 0
        else float("nan")
    )
    dd = float(((s - s.cummax()) / s.cummax()).min()) if len(s) else float("nan")
    return {
        "n": float(len(sub)),
        "pnl": float(sub["pnl"].sum()) if len(sub) else 0.0,
        "sharpe": sharpe,
        "max_dd": dd,
        "final_nav": float(s.iloc[-1]) if len(s) else float("nan"),
        "mean_pnl": float(sub["pnl"].mean()) if len(sub) else 0.0,
    }


def main() -> int:
    print("=" * 60)
    print("  v9 trail paired check (1 + 5) —", ENTRY)
    print("  DATE_RANGE", ThetaEngineV9.DATE_RANGE[0], "→", ThetaEngineV9.DATE_RANGE[1])
    print("=" * 60)

    t0 = time.time()
    replay = MarketReplay(
        _cfg.data.options_parquet,
        _cfg.data.spot_parquet,
        start=ThetaEngineV9.DATE_RANGE[0],
        end=ThetaEngineV9.DATE_RANGE[1],
    )
    print(f"  Data: {len(replay._timestamps):,} intervals")

    # Cartesian includes unused cells (fixed@0, trail@0.60/0.65); keep 3.
    grid = _base_grid(
        exit_mode=["fixed", "trail"],
        take_profit_pct=[0.0, 0.60, 0.65],
    )
    df, keys, nav_daily, final_nav, fills = run_grid_full(
        ThetaEngineV9, grid, replay
    )

    idx_by_name: Dict[str, int] = {}
    for i, key in enumerate(keys):
        name = _combo_name(key)
        if name is not None:
            idx_by_name[name] = i
    print("  Kept combos:", idx_by_name)

    keep_names = {"fixed_0.60", "fixed_0.65", "trail_0.65_0.05"}
    missing = keep_names - set(idx_by_name)
    if missing:
        raise SystemExit(f"missing combos: {missing}; keys={[dict(k) for k in keys]}")

    print("\n--- (5) Aggregate vs fixed 0.60 / fixed 0.65 ---")
    aggs = {name: _agg(df, nav_daily, idx) for name, idx in idx_by_name.items()}
    ctrl = aggs["fixed_0.60"]
    print(
        f"  {'name':18} {'n':>4} {'pnl':>10} {'Δpnl':>9} {'sharpe':>7} "
        f"{'Δsh':>6} {'maxDD':>8} {'final':>10}"
    )
    for name in ("fixed_0.60", "fixed_0.65", "trail_0.65_0.05"):
        a = aggs[name]
        print(
            f"  {name:18} {a['n']:4.0f} {a['pnl']:10.0f} {a['pnl']-ctrl['pnl']:+9.0f} "
            f"{a['sharpe']:7.2f} {a['sharpe']-ctrl['sharpe']:+6.2f} "
            f"{a['max_dd']:8.1%} {a['final_nav']:10.0f}"
        )

    trail = df[df.combo_idx == idx_by_name["trail_0.65_0.05"]]
    f60 = df[df.combo_idx == idx_by_name["fixed_0.60"]]
    f65 = df[df.combo_idx == idx_by_name["fixed_0.65"]]

    print("\n--- (1) Paired ΔPnL (trail − fixed), matched on entry_time ---")
    for name, base in (("fixed_0.60", f60), ("fixed_0.65", f65)):
        paired = _pair_delta(base, trail, "base", "trail")
        d = paired["delta_pnl"].to_numpy()
        mean, lo, hi = _bootstrap_mean_ci(d)
        pct_pos = float((d > 0).mean()) if len(d) else float("nan")
        print(
            f"  trail vs {name}: paired={len(d)}  "
            f"(base n={len(base)}, trail n={len(trail)}, "
            f"unpaired_base={len(base)-len(d)}, unpaired_trail={len(trail)-len(d)})"
        )
        print(
            f"    mean ΔPnL={mean:+.2f}  bootstrap 95% CI [{lo:+.2f}, {hi:+.2f}]  "
            f"%Δ>0={pct_pos:.1%}  median Δ={float(np.median(d)):+.2f}"
        )
        print(
            "    trail exits:",
            paired["exit_reason_trail"].value_counts().to_dict(),
        )
        print(
            "    base exits:",
            paired["exit_reason_base"].value_counts().to_dict(),
        )

    print("\n--- Verdict helpers ---")
    p60 = _pair_delta(f60, trail, "base", "trail")["delta_pnl"].to_numpy()
    p65 = _pair_delta(f65, trail, "base", "trail")["delta_pnl"].to_numpy()
    m60, lo60, hi60 = _bootstrap_mean_ci(p60)
    m65, lo65, hi65 = _bootstrap_mean_ci(p65)
    beat_60 = lo60 > 0
    beat_65 = lo65 > 0
    print(f"  CI(trail−fixed0.60) entirely > 0? {beat_60}  ({lo60:+.2f},{hi60:+.2f})")
    print(f"  CI(trail−fixed0.65) entirely > 0? {beat_65}  ({lo65:+.2f},{hi65:+.2f})")
    if beat_60 and beat_65:
        print("  → Keep digging (WFO/OOS): trail beats both fixed levels on paired PnL.")
    elif beat_60 and not beat_65:
        print(
            "  → Trail ≈ higher fixed TP: beats 0.60 but not 0.65 — "
            "prefer fixed 0.65 (simpler) unless OOS says otherwise."
        )
    elif not beat_60:
        print("  → Do not dig deeper on trail: paired edge vs locked control not reliable.")
    else:
        print("  → Mixed; inspect aggregates above.")

    out_dir = Path(runs_dir())
    paired60 = _pair_delta(f60, trail, "base", "trail")
    paired65 = _pair_delta(f65, trail, "base", "trail")
    csv60 = out_dir / "theta_engine_v9_trail_vs_fixed060_paired.csv"
    csv65 = out_dir / "theta_engine_v9_trail_vs_fixed065_paired.csv"
    paired60.to_csv(csv60, index=False)
    paired65.to_csv(csv65, index=False)
    out = out_dir / "theta_engine_v9_trail_paired_check.txt"
    out.write_text(
        "\n".join(
            [
                f"entry={ENTRY}",
                f"date_range={ThetaEngineV9.DATE_RANGE}",
                f"aggs={aggs}",
                f"paired_vs_060: n={len(p60)} mean={m60} ci=({lo60},{hi60})",
                f"paired_vs_065: n={len(p65)} mean={m65} ci=({lo65},{hi65})",
                f"elapsed_s={time.time()-t0:.1f}",
            ]
        )
        + "\n"
    )
    print(f"\n  Wrote {out}")
    print(f"  Wrote {csv60.name}, {csv65.name}")
    print(f"  Total: {time.time()-t0:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

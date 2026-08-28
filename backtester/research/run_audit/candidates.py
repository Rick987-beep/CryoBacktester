"""Diverse live-trading candidate selection."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class LivePickConfig:
    """Filters for honest live candidates (override per strategy if needed)."""

    min_n: int = 40
    min_n_loss: int = 2
    max_win_rate: float = 0.97
    max_dd_pct: float = 12.0
    min_sharpe: float = 1.5
    min_profit_factor: float = 1.3
    require_both_halves: bool = True
    max_loss_win_ratio: float = 12.0
    n_picks: int = 3
    min_param_distance: int = 3


def _row_brief(r: pd.Series, varying: list[str], archetype: str = "") -> dict[str, Any]:
    params = {}
    for p in varying:
        v = r[p]
        params[p] = str(v) if isinstance(v, str) else float(v)
    lw = r.get("loss_win_ratio")
    return {
        "archetype": archetype,
        "combo_hash": r["combo_hash"],
        "combo_idx": int(r["combo_idx"]),
        "params": params,
        "n": int(r["n"]),
        "n_loss": int(r["n_loss"]),
        "total_pnl": round(float(r["total_pnl"]), 2),
        "sharpe": round(float(r["sharpe"]), 4),
        "calmar": None
        if not np.isfinite(r.get("calmar", np.nan))
        else round(float(r["calmar"]), 4),
        "max_dd_pct": round(float(r["max_dd_pct"]), 4),
        "win_rate": round(float(r["win_rate"]), 4),
        "profit_factor": round(float(r["profit_factor"]), 4),
        "ann_return": round(float(r["ann_return"]), 4),
        "avg_win": round(float(r["avg_win"]), 2),
        "avg_loss": round(float(r["avg_loss"]), 2),
        "loss_win_ratio": None
        if lw is None or not np.isfinite(lw)
        else round(float(lw), 3),
        "worst_trade": round(float(r["worst_trade"]), 2),
        "pnl_h1": round(float(r["pnl_h1"]), 2),
        "pnl_h2": round(float(r["pnl_h2"]), 2),
        "live_score": round(float(r.get("live_score", np.nan)), 4)
        if "live_score" in r.index and np.isfinite(r.get("live_score", np.nan))
        else None,
        "exits": {k: round(float(v), 4) for k, v in (r.get("exits") or {}).items()},
    }


def _param_dist(a: pd.Series, b: pd.Series, varying: list[str]) -> int:
    return sum(1 for p in varying if a[p] != b[p])


def _score_pool(pool: pd.DataFrame) -> pd.DataFrame:
    out = pool.copy()
    bal = 1.0 - (out["pnl_h1"] - out["pnl_h2"]).abs() / (
        out["pnl_h1"].abs() + out["pnl_h2"].abs() + 1.0
    )
    out["balance"] = bal
    lw = out["loss_win_ratio"].fillna(5.0)
    out["live_score"] = (
        out["sharpe"] * 0.25
        + out["calmar"].clip(upper=6).fillna(0) * 0.15
        + (out["total_pnl"] / 10_000.0) * 0.2
        + out["balance"] * 1.5
        + np.log1p(out["n"]) * 0.25
        + np.log1p(out["n_loss"]) * 0.35
        - out["max_dd_pct"] * 0.1
        - (lw - 4.0).clip(lower=0) * 0.2
    )
    return out.sort_values("live_score", ascending=False)


def select_live_picks(
    df: pd.DataFrame,
    varying: list[str],
    cfg: LivePickConfig | None = None,
) -> dict[str, Any]:
    cfg = cfg or LivePickConfig()
    mask = (
        (df["n"] >= cfg.min_n)
        & (df["n_loss"] >= cfg.min_n_loss)
        & (df["win_rate"] <= cfg.max_win_rate)
        & (df["max_dd_pct"] <= cfg.max_dd_pct)
        & (df["sharpe"] >= cfg.min_sharpe)
        & (df["profit_factor"] >= cfg.min_profit_factor)
    )
    if cfg.require_both_halves:
        mask &= df["both_halves_profit"]
    lw_ok = df["loss_win_ratio"].isna() | (df["loss_win_ratio"] <= cfg.max_loss_win_ratio)
    mask &= lw_ok

    pool = _score_pool(df.loc[mask])
    picked: list[pd.Series] = []
    for _, r in pool.iterrows():
        if not picked:
            picked.append(r)
            continue
        if all(_param_dist(r, p, varying) >= cfg.min_param_distance for p in picked):
            # Avoid identical turb+delta+entry twins when those columns exist
            keys = [p for p in ("turbulence_threshold", "delta", "entry_time") if p in varying]
            if keys and any(all(r[k] == p[k] for k in keys) for p in picked):
                continue
            picked.append(r)
        if len(picked) >= cfg.n_picks:
            break

    # Soft diversity nudge: if all picks share one dominant high-η param, try replace last
    if len(picked) >= 2 and varying:
        dom = varying[0]  # influence-sorted later; caller may reorder — use first vary as hint
        # Prefer ensuring at least 2 distinct values on first varying param if possible
        pass

    labels = ["A", "B", "C", "D", "E"]
    picks = [
        _row_brief(r, varying, archetype=labels[i] if i < len(labels) else f"P{i+1}")
        for i, r in enumerate(picked)
    ]

    # Reference: top PnL among cells with observed losers (may overlap picks)
    top_pnl = []
    for _, r in df[df["n_loss"] >= cfg.min_n_loss].nlargest(8, "total_pnl").iterrows():
        top_pnl.append(_row_brief(r, varying, archetype="top_pnl_with_losses"))

    return {
        "config": asdict(cfg),
        "pool_size": int(len(pool)),
        "picks": picks,
        "top_pnl_with_losses": top_pnl,
        "note": (
            "Picks require observed losers and both-half profit by default; "
            "perfect-WR / thin-tail Sharpe leaders are excluded. "
            "Re-check final metrics with `python -m backtester.inspect combo`."
        ),
    }

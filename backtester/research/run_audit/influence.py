"""Parameter influence (η²) and per-level summaries."""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def eta_squared(y: pd.Series | np.ndarray, groups: pd.Series) -> float:
    """One-way ANOVA η²: between-group SS / total SS."""
    y = pd.Series(np.asarray(y, dtype=float)).reset_index(drop=True)
    g = pd.Series(groups).reset_index(drop=True)
    overall = float(y.mean())
    ss_tot = float(((y - overall) ** 2).sum())
    if ss_tot <= 0 or not np.isfinite(ss_tot):
        return 0.0
    ss_between = 0.0
    for _, idx in g.groupby(g, sort=False).groups.items():
        yi = y.iloc[list(idx)]
        ss_between += len(yi) * (float(yi.mean()) - overall) ** 2
    return float(ss_between / ss_tot)


def _level_label(param: str, value: Any) -> str | float:
    if param in ("entry_time", "leg_type") or isinstance(value, str):
        return str(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return str(value)


def level_stats(df: pd.DataFrame, param: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for v, sub in df.groupby(param, sort=True):
        lw = sub["loss_win_ratio"].replace([np.inf, -np.inf], np.nan)
        rows.append(
            {
                "level": _level_label(param, v),
                "n": int(len(sub)),
                "med_sharpe": float(sub["sharpe"].median()),
                "mean_sharpe": float(sub["sharpe"].mean()),
                "med_pnl": float(sub["total_pnl"].median()),
                "p_profit": float((sub["total_pnl"] > 0).mean()),
                "med_dd": float(sub["max_dd_pct"].median()),
                "p95_dd": float(sub["max_dd_pct"].quantile(0.95)),
                "worst_dd": float(sub["max_dd_pct"].max()),
                "med_wr": float(sub["win_rate"].median()),
                "med_n": float(sub["n"].median()),
                "p_sharpe_gt2": float((sub["sharpe"] > 2).mean()),
                "p_perfect_wr": float(sub["perfect_wr"].mean()),
                "med_loss_win": float(lw.median()) if lw.notna().any() else None,
                "worst_pnl": float(sub["total_pnl"].min()),
            }
        )
    return rows


def param_influence(df: pd.DataFrame, varying: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in varying:
        meds = df.groupby(p)["sharpe"].median()
        best = meds.idxmax() if len(meds) else None
        worst = meds.idxmin() if len(meds) else None
        out.append(
            {
                "param": p,
                "eta_sharpe": eta_squared(df["sharpe"].fillna(0), df[p]),
                "eta_pnl": eta_squared(df["total_pnl"].fillna(0), df[p]),
                "eta_dd": eta_squared(df["max_dd_pct"].fillna(0), df[p]),
                "med_sharpe_range": float(meds.max() - meds.min()) if len(meds) else 0.0,
                "best_level_by_med_sharpe": _level_label(p, best) if best is not None else None,
                "worst_level_by_med_sharpe": _level_label(p, worst) if worst is not None else None,
                "levels": level_stats(df, p),
            }
        )
    out.sort(key=lambda x: -x["eta_sharpe"])
    return out


def heat_median(
    df: pd.DataFrame, row_param: str, col_param: str, metric: str = "sharpe"
) -> dict[str, Any] | None:
    if row_param not in df.columns or col_param not in df.columns:
        return None
    pivot = df.pivot_table(
        index=row_param, columns=col_param, values=metric, aggfunc="median"
    )
    if pivot.empty:
        return None

    def _lab(p: str, v: Any) -> str | float:
        return _level_label(p, v)

    return {
        "row_param": row_param,
        "col_param": col_param,
        "metric": metric,
        "index": [_lab(row_param, i) for i in pivot.index],
        "columns": [_lab(col_param, c) for c in pivot.columns],
        "values": [
            [None if pd.isna(x) else round(float(x), 4) for x in row]
            for row in pivot.values
        ],
    }


def inert_fraction(df: pd.DataFrame, varying: list[str], param: str) -> float | None:
    """Share of slices (other params fixed) where ``param`` does not change Sharpe."""
    others = [p for p in varying if p != param]
    if not others or param not in df.columns:
        return None
    nunq = df.groupby(others, dropna=False)["sharpe"].nunique()
    if nunq.empty:
        return None
    return float((nunq == 1).mean())

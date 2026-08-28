"""Curve-fit / multiplicity diagnostics."""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

try:
    from scipy.stats import spearmanr
except ImportError:  # pragma: no cover
    spearmanr = None  # type: ignore


def economic_fingerprint(df: pd.DataFrame) -> pd.Series:
    return (
        df["total_pnl"].round(2).astype(str)
        + "|"
        + df["n"].astype(int).astype(str)
        + "|"
        + df["win_rate"].round(6).astype(str)
    )


def grid_summary(df: pd.DataFrame, half_split_mid: str) -> dict[str, Any]:
    n = len(df)
    fp = economic_fingerprint(df)
    n_unique = int(fp.nunique())
    top100 = df.nlargest(min(100, n), "sharpe").copy()
    top100_fp = (
        top100["total_pnl"].round(0).astype(int).astype(str)
        + "|"
        + top100["n"].astype(int).astype(str)
    )
    mask = df["n"] >= 20
    rho = None
    if spearmanr is not None and mask.sum() >= 5:
        rho_v, _ = spearmanr(df.loc[mask, "pnl_h1"], df.loc[mask, "pnl_h2"])
        rho = float(rho_v) if np.isfinite(rho_v) else None

    top_h1 = df.nlargest(min(100, n), "pnl_h1")
    h1_mean = float(top_h1["pnl_h1"].mean()) if len(top_h1) else 0.0
    h2_mean = float(top_h1["pnl_h2"].mean()) if len(top_h1) else 0.0
    decay = float(1.0 - h2_mean / h1_mean) if h1_mean > 1e-9 else None

    class_sizes = fp.value_counts()
    return {
        "n_combos": n,
        "n_profit": int((df["total_pnl"] > 0).sum()),
        "pct_profit": round(float((df["total_pnl"] > 0).mean()), 4),
        "n_sharpe_gt2": int((df["sharpe"] > 2).sum()),
        "n_sharpe_gt3": int((df["sharpe"] > 3).sum()),
        "n_sharpe_gt4": int((df["sharpe"] > 4).sum()),
        "n_perfect_wr": int(df["perfect_wr"].sum()),
        "pct_perfect_wr": round(float(df["perfect_wr"].mean()), 4),
        "n_wr_ge_95": int((df["win_rate"] >= 0.95).sum()),
        "median_sharpe": round(float(df["sharpe"].median()), 4),
        "median_pnl": round(float(df["total_pnl"].median()), 2),
        "median_dd": round(float(df["max_dd_pct"].median()), 3),
        "median_n": round(float(df["n"].median()), 1),
        "median_wr": round(float(df["win_rate"].median()), 4),
        "p95_sharpe": round(float(df["sharpe"].quantile(0.95)), 4),
        "p99_sharpe": round(float(df["sharpe"].quantile(0.99)), 4),
        "max_sharpe": round(float(df["sharpe"].max()), 4),
        "min_pnl": round(float(df["total_pnl"].min()), 2),
        "max_pnl": round(float(df["total_pnl"].max()), 2),
        "n_unique_economic_outcomes": n_unique,
        "effective_grid_shrink": round(1.0 - n_unique / max(n, 1), 4),
        "unique_outcome_fps_top100_sharpe": int(top100_fp.nunique()),
        "largest_duplicate_class": int(class_sizes.iloc[0]) if len(class_sizes) else 0,
        "median_duplicate_class": float(class_sizes.median()) if len(class_sizes) else 0.0,
        "half_split_mid": half_split_mid,
        "spearman_h1_h2_pnl": None if rho is None else round(rho, 4),
        "top100_h1_mean_decay_to_h2": None if decay is None else round(decay, 4),
        "frac_top100_h1_still_profit_h2": round(
            float((top_h1["pnl_h2"] > 0).mean()) if len(top_h1) else 0.0, 4
        ),
        "pct_both_halves_profit": round(float(df["both_halves_profit"].mean()), 4),
        "top_decile_perfect_wr_share": round(
            float(df.nlargest(max(1, n // 10), "sharpe")["perfect_wr"].mean()), 4
        ),
    }


def neighbor_plateau(
    df: pd.DataFrame,
    varying: list[str],
    param_grid: dict[str, list],
    *,
    top_n: int = 40,
    tol: float = 0.25,
) -> dict[str, Any]:
    """Among top-N Sharpe cells, fraction of 1-step neighbors within ``tol`` relative Sharpe."""
    if not varying or df.empty:
        return {"top_n": 0, "median_frac_within_tol": None, "sample": []}

    def coerce(p: str, v: Any) -> Any:
        levels = list(param_grid.get(p) or [])
        for L in levels:
            if p in ("entry_time", "leg_type") or isinstance(L, str):
                if str(L) == str(v):
                    return str(L)
            else:
                try:
                    if float(L) == float(v):
                        return float(L)
                except (TypeError, ValueError):
                    continue
        return str(v) if isinstance(v, str) else float(v)

    lookup: dict[tuple, float] = {}
    for _, r in df.iterrows():
        key = tuple(coerce(p, r[p]) for p in varying)
        lookup[key] = float(r["sharpe"])

    sample: list[dict[str, Any]] = []
    fracs: list[float] = []
    for _, r in df.nlargest(min(top_n, len(df)), "sharpe").iterrows():
        tup = tuple(coerce(p, r[p]) for p in varying)
        neigh: list[float] = []
        for i, p in enumerate(varying):
            levels = param_grid.get(p) or []
            levels_c = [
                str(x) if (p in ("entry_time", "leg_type") or isinstance(x, str)) else float(x)
                for x in levels
            ]
            cur = tup[i]
            if cur not in levels_c:
                continue
            j = levels_c.index(cur)
            for jj in (j - 1, j + 1):
                if 0 <= jj < len(levels_c):
                    nt = list(tup)
                    nt[i] = levels_c[jj]
                    hit = lookup.get(tuple(nt))
                    if hit is not None:
                        neigh.append(hit)
        if neigh:
            frac = float(
                np.mean(
                    [
                        abs(s - r["sharpe"]) / max(abs(r["sharpe"]), 1e-9) < tol
                        for s in neigh
                    ]
                )
            )
            fracs.append(frac)
            sample.append(
                {
                    "combo_hash": r["combo_hash"],
                    "sharpe": round(float(r["sharpe"]), 4),
                    "n_neighbors": len(neigh),
                    "frac_within_tol": round(frac, 4),
                    "med_neigh_sharpe": round(float(np.median(neigh)), 4),
                }
            )

    return {
        "top_n": len(sample),
        "tol": tol,
        "median_frac_within_tol": None
        if not fracs
        else round(float(np.median(fracs)), 4),
        "sample": sample[:10],
    }


def curve_fit_verdict(summary: dict[str, Any], plateau: dict[str, Any]) -> dict[str, Any]:
    """Heuristic label + evidence bullets from computed stats."""
    shrink = float(summary.get("effective_grid_shrink") or 0)
    perfect = float(summary.get("pct_perfect_wr") or 0)
    top_perf = float(summary.get("top_decile_perfect_wr_share") or 0)
    rho = summary.get("spearman_h1_h2_pnl")
    pct_profit = float(summary.get("pct_profit") or 0)

    score = 0
    if shrink >= 0.7:
        score += 2
    elif shrink >= 0.4:
        score += 1
    if perfect >= 0.25 or top_perf >= 0.5:
        score += 2
    elif perfect >= 0.1:
        score += 1
    if rho is not None and rho < 0.5:
        score += 1
    if pct_profit >= 0.9:
        score += 1

    if score >= 5:
        level = "HIGH"
    elif score >= 3:
        level = "MODERATE"
    else:
        level = "LOW"

    evidence = [
        (
            f"Only {summary.get('n_unique_economic_outcomes')} unique economic outcomes "
            f"out of {summary.get('n_combos')} "
            f"({100 * shrink:.0f}% duplicates)."
        ),
        (
            f"{summary.get('n_perfect_wr')} cells ({100 * perfect:.0f}%) have WR=100%; "
            f"{100 * top_perf:.0f}% of the top Sharpe decile are perfect-WR."
        ),
        (
            f"Half-sample PnL Spearman = {rho}; "
            f"top-100 H1→H2 mean PnL decay = {summary.get('top100_h1_mean_decay_to_h2')}; "
            f"mid={summary.get('half_split_mid')}."
        ),
        (
            f"{100 * pct_profit:.1f}% of combos are profitable — "
            "a pre-narrowed island inflates multiple-testing."
        ),
    ]
    med_plat = plateau.get("median_frac_within_tol")
    if med_plat is not None:
        evidence.append(
            f"Neighbor plateau (top Sharpe): median {100 * med_plat:.0f}% of "
            f"1-step neighbors stay within {100 * float(plateau.get('tol', 0.25)):.0f}% Sharpe."
        )
    return {"level": level, "score": score, "evidence": evidence}

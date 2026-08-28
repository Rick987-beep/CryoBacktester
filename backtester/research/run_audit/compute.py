"""Orchestrate a full run audit → JSON-serialisable dict."""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from backtester.inspect.resolve import ResolvedRun
from backtester.research.run_audit.candidates import LivePickConfig, select_live_picks
from backtester.research.run_audit.curve_fit import (
    curve_fit_verdict,
    grid_summary,
    neighbor_plateau,
)
from backtester.research.run_audit.danger import danger_rank, danger_verdict
from backtester.research.run_audit.frame import build_combo_frame
from backtester.research.run_audit.influence import (
    heat_median,
    inert_fraction,
    param_influence,
)


def _jsonable(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, (np.floating, float)):
        x = float(obj)
        if not np.isfinite(x):
            return None
        return x
    if isinstance(obj, (np.integer, int)):
        return int(obj)
    if isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    if isinstance(obj, str):
        return obj
    if pd.isna(obj):
        return None
    return str(obj)


def audit_run(
    run: ResolvedRun,
    *,
    live_cfg: LivePickConfig | None = None,
    heat_row: str | None = None,
    heat_col: str | None = None,
) -> dict[str, Any]:
    """Compute the full audit pack for a resolved run."""
    df, bits = build_combo_frame(run)
    varying = bits["varying_params"]
    param_grid = bits["param_grid"]

    influence = param_influence(df, varying)
    # Prefer heat on top-2 influence params
    if heat_row is None and len(varying) >= 1:
        heat_row = influence[0]["param"] if influence else varying[0]
    if heat_col is None and len(varying) >= 2:
        heat_col = influence[1]["param"] if len(influence) > 1 else varying[1]

    heats = {}
    if heat_row and heat_col and heat_row != heat_col:
        h = heat_median(df, heat_row, heat_col, "sharpe")
        if h:
            heats["sharpe"] = h
        h2 = heat_median(df, heat_row, heat_col, "total_pnl")
        if h2:
            heats["total_pnl"] = h2

    summary = grid_summary(df, bits["half_split_mid"])
    inert = {
        p: inert_fraction(df, varying, p)
        for p in varying
        if inert_fraction(df, varying, p) is not None
    }
    for p, frac in inert.items():
        if frac is not None:
            summary[f"inert_fraction_{p}"] = round(float(frac), 4)

    plateau = neighbor_plateau(df, varying, param_grid)
    cf_verdict = curve_fit_verdict(summary, plateau)
    ranked = danger_rank(df, varying)
    d_verdict = danger_verdict(df, varying, ranked)
    live = select_live_picks(df, varying, live_cfg)

    # Sharpe histogram
    edges = [-2, -1, 0, 1, 2, 3, 4, 5, 6, 99]
    labels, counts = [], []
    for a, b in zip(edges[:-1], edges[1:]):
        labels.append(f"{a}–{b}" if b < 90 else f"{a}+")
        counts.append(int(((df["sharpe"] >= a) & (df["sharpe"] < b)).sum()))

    # Top-10 by Sharpe / PnL for context (not live recommendations)
    def _top(metric: str, n: int = 10) -> list[dict[str, Any]]:
        rows = []
        for _, r in df.nlargest(n, metric).iterrows():
            rows.append(
                {
                    "combo_hash": r["combo_hash"],
                    "combo_idx": int(r["combo_idx"]),
                    "params": {
                        p: (str(r[p]) if isinstance(r[p], str) else float(r[p]))
                        for p in varying
                    },
                    "n": int(r["n"]),
                    "n_loss": int(r["n_loss"]),
                    "total_pnl": round(float(r["total_pnl"]), 2),
                    "sharpe": round(float(r["sharpe"]), 4),
                    "max_dd_pct": round(float(r["max_dd_pct"]), 4),
                    "win_rate": round(float(r["win_rate"]), 4),
                    "perfect_wr": bool(r["perfect_wr"]),
                }
            )
        return rows

    pack = {
        "schema_version": 1,
        "meta": {
            "run_id": run.run_id,
            "bundle": run.bundle_name,
            "bundle_path": str(run.bundle_path),
            "strategy": run.strategy,
            "family": run.family,
            "date_from": bits["date_from"],
            "date_to": bits["date_to"],
            "n_combos": bits["n_combos"],
            "account_size": bits["capital"],
            "varying_params": varying,
            "fixed_params": bits["fixed_params"],
            "half_split_mid": bits["half_split_mid"],
            "exit_reasons": bits["exit_reasons"],
            "git_sha": run.git_sha,
        },
        "questions": {
            "influence": (
                "Which parameter has what influence on the results?"
            ),
            "danger": 'Which setting is the most "dangerous"?',
            "curve_fit": "How much curve-fitting is going on in this run?",
            "live": (
                "Which 2 or three combos do you suggest for live trading "
                "(they should be very different from each other if possible)?"
            ),
        },
        "grid_summary": summary,
        "influence": influence,
        "influence_bar": [
            {
                "param": x["param"],
                "eta_sharpe": round(x["eta_sharpe"], 4),
                "eta_pnl": round(x["eta_pnl"], 4),
                "eta_dd": round(x["eta_dd"], 4),
            }
            for x in influence
        ],
        "heats": heats,
        "sharpe_hist": {"labels": labels, "counts": counts},
        "danger_rank": ranked,
        "danger_verdict": d_verdict,
        "curve_fit": {
            "verdict": cf_verdict,
            "neighbor_plateau": plateau,
        },
        "live_candidates": live,
        "top10_sharpe": _top("sharpe"),
        "top10_pnl": _top("total_pnl"),
        "inert_fractions": {k: round(float(v), 4) for k, v in inert.items() if v is not None},
    }
    return _jsonable(pack)

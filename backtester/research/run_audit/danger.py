"""Danger ranking for grid settings and product shapes."""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from backtester.research.run_audit.influence import level_stats


def danger_rank(df: pd.DataFrame, varying: list[str], *, top_k: int = 12) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for p in varying:
        for row in level_stats(df, p):
            med_lw = row.get("med_loss_win") or 0.0
            if med_lw != med_lw:  # NaN
                med_lw = 0.0
            # Real left-tail / bad expectancy — do NOT boost for perfect WR
            # (that is a separate "mirage / product shape" flag).
            score = (
                float(row["p95_dd"]) / 10.0
                + max(0.0, -float(row["med_sharpe"]))
                + max(0.0, float(med_lw) - 3.0) * 0.5
                + (1.0 - float(row["p_profit"]))
            )
            ranked.append({**row, "param": p, "danger_score": round(score, 4)})
    ranked.sort(key=lambda x: -x["danger_score"])
    return ranked[:top_k]


def danger_verdict(df: pd.DataFrame, varying: list[str], ranked: list[dict[str, Any]]) -> dict[str, Any]:
    """Narrative hooks: worst setting by p95 DD / asymmetry, and thin-tail shape."""
    # Prefer "real" left-tail settings over perfect-WR mirages for the headline setting.
    real = [r for r in ranked if float(r.get("p_perfect_wr") or 0) < 0.4]
    worst_setting = real[0] if real else (ranked[0] if ranked else None)

    mirage_levels = sorted(
        [
            r
            for p in varying
            for r in ({**row, "param": p} for row in level_stats(df, p))
            if float(r.get("p_perfect_wr") or 0) >= 0.5
        ],
        key=lambda x: -float(x.get("p_perfect_wr") or 0),
    )[:5]

    # Highest median loss/win among levels with enough losers
    worst_lw = None
    for p in varying:
        for row in level_stats(df, p):
            lw = row.get("med_loss_win")
            if lw is None or not np.isfinite(lw):
                continue
            if float(row.get("p_perfect_wr") or 0) >= 0.5:
                continue
            if worst_lw is None or lw > worst_lw["med_loss_win"]:
                worst_lw = {"param": p, **row}

    pct_perfect = float(df["perfect_wr"].mean())
    top_dec = df.nlargest(max(1, len(df) // 10), "sharpe")
    shape = {
        "pct_perfect_wr": round(pct_perfect, 4),
        "top_decile_perfect_wr_share": round(float(top_dec["perfect_wr"].mean()), 4),
        "top_decile_med_wr": round(float(top_dec["win_rate"].median()), 4),
        "top_decile_med_n": round(float(top_dec["n"].median()), 1),
        "all_med_loss_win": (
            None
            if not df["loss_win_ratio"].notna().any()
            else round(float(df["loss_win_ratio"].median()), 3)
        ),
    }

    headline_parts = []
    if worst_setting:
        headline_parts.append(
            f"Most dangerous *setting*: {worst_setting['param']}={worst_setting['level']} "
            f"(danger_score={worst_setting['danger_score']})."
        )
    if pct_perfect >= 0.15 or float(top_dec["perfect_wr"].mean()) >= 0.5:
        headline_parts.append(
            "Most dangerous *product shape*: high WR / perfect-WR cells with unobserved left tail."
        )

    return {
        "headline": " ".join(headline_parts) if headline_parts else "No clear danger flag.",
        "worst_setting": worst_setting,
        "worst_loss_win_level": worst_lw,
        "mirage_levels": mirage_levels,
        "thin_tail_shape": shape,
    }

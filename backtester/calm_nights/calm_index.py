"""Continuous calmness indices (0–1) and causal morning score."""

from __future__ import annotations

import numpy as np
import pandas as pd


def causal_percentile_calm(
    series: pd.Series,
    *,
    window: int = 60,
    min_periods: int = 20,
) -> pd.Series:
    """Causal calm index: higher = calmer vs prior *window* slot-A days."""
    values = series.astype(float).to_numpy()
    out = np.full(len(values), np.nan)
    for i in range(len(values)):
        start = max(0, i - window)
        hist = values[start:i]
        hist = hist[~np.isnan(hist)]
        if len(hist) < min_periods:
            continue
        val = values[i]
        if np.isnan(val):
            continue
        calmer_frac = float(np.mean(hist > val))
        out[i] = np.clip(calmer_frac, 0.0, 1.0)
    return pd.Series(out, index=series.index, name=series.name)


def extrusion_calm_index(
    range_norm: pd.Series,
    path_norm: pd.Series | None = None,
    *,
    range_weight: float = 0.7,
    path_weight: float = 0.3,
    window: int = 60,
    min_periods: int = 20,
) -> pd.Series:
    """Composite 0–1 calm index from normalized extrusion (higher = calmer)."""
    m_range = causal_percentile_calm(
        range_norm, window=window, min_periods=min_periods
    )
    if path_norm is None:
        return m_range.rename("morning_calm_index")

    m_path = causal_percentile_calm(
        path_norm, window=window, min_periods=min_periods
    )
    w_sum = range_weight + path_weight
    composite = (range_weight * m_range + path_weight * m_path) / w_sum
    return composite.clip(0.0, 1.0).rename("morning_calm_index")

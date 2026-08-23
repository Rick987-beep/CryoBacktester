"""Front-expiry 25Δ mark IV and rolling rank for short-DTE entry sizing.

Builds a daily panel (UTC midnight index) from options snapshot parquets at a
fixed entry clock. For each day:

* pick the exact-``dte`` expiry (same rule as ``select_expiry``)
* record call / put 25Δ ``mark_iv`` (%)
* rolling percentile rank over ``rank_lookback`` days

Strategies look up ``call_iv_rank_60`` or ``put_iv_rank_60`` after skew side
is chosen.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from backtester.core.paths import market_data_dir
from backtester.indicators.options_snapshot import iv_25d_for_entry_day
from backtester.indicators.vol_context import dvol_rank

logger = logging.getLogger(__name__)


def build_front_25d_iv_rank(
    df_raw: pd.DataFrame,
    *,
    options_dir: Optional[Path] = None,
    entry_hour: int = 16,
    entry_minute: int = 0,
    dte: int = 1,
    delta: float = 0.25,
    rank_lookback: int = 60,
    **_params,
) -> pd.DataFrame:
    """Daily panel: call/put 25Δ IV and rolling ranks."""
    cols = [
        "call_iv_25d", "put_iv_25d",
        "call_iv_rank_60", "put_iv_rank_60",
    ]
    if df_raw.empty:
        return pd.DataFrame(columns=cols)

    bars = df_raw.sort_index()
    if bars.index.tz is None:
        bars = bars.copy()
        bars.index = bars.index.tz_localize("UTC")
    daily_index = bars.index.normalize().unique()

    root = Path(options_dir) if options_dir is not None else market_data_dir()
    rows: list[dict[str, float]] = []
    for day_ts in daily_index:
        entry_day = day_ts.date()
        call_iv, put_iv = iv_25d_for_entry_day(
            entry_day,
            root,
            entry_hour=entry_hour,
            entry_minute=entry_minute,
            dte=dte,
            delta=delta,
        )
        rows.append({
            "call_iv_25d": call_iv,
            "put_iv_25d": put_iv,
        })

    out = pd.DataFrame(rows, index=daily_index)
    out.index.name = "day"
    out["call_iv_rank_60"] = dvol_rank(out["call_iv_25d"], lookback=rank_lookback)
    out["put_iv_rank_60"] = dvol_rank(out["put_iv_25d"], lookback=rank_lookback)
    return out


def lookup_front_25d_iv_rank(
    panel: Optional[pd.DataFrame],
    dt: datetime,
    is_call: bool,
) -> dict[str, float]:
    """Causal asof lookup for the skew-selected side's IV rank."""
    iv_key = "call_iv_25d" if is_call else "put_iv_25d"
    rank_key = "call_iv_rank_60" if is_call else "put_iv_rank_60"
    empty = {iv_key: float("nan"), "iv_rank_60": float("nan")}
    if panel is None or panel.empty:
        return empty
    ts = pd.Timestamp(dt)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    day = ts.normalize()
    idx = panel.index[panel.index <= day]
    if len(idx) == 0:
        return empty
    row = panel.loc[idx[-1]]
    iv_val = float(row.get(iv_key, np.nan))
    rank_val = float(row.get(rank_key, np.nan))
    if math.isnan(rank_val):
        return {iv_key: iv_val, "iv_rank_60": float("nan")}
    return {iv_key: iv_val, "iv_rank_60": rank_val}

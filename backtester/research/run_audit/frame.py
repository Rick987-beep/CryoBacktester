"""Build a per-combo analysis frame from a resolved run bundle."""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from backtester.core.results import _all_combo_stats
from backtester.inspect.load import read_nav
from backtester.inspect.resolve import ResolvedRun, keys_from_meta, params_from_key
from backtester.ui.services.store_service import key_hash


def varying_and_fixed(param_grid: dict[str, list] | None) -> tuple[list[str], dict[str, Any]]:
    grid = param_grid or {}
    varying = [k for k, v in grid.items() if isinstance(v, list) and len(v) > 1]
    fixed = {
        k: (v[0] if isinstance(v, list) and v else v)
        for k, v in grid.items()
        if not (isinstance(v, list) and len(v) > 1)
    }
    return varying, fixed


def build_combo_frame(run: ResolvedRun) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load trades + NAV, compute inspect-aligned metrics, return wide frame.

    Returns ``(df, meta_bits)`` where ``df`` is one row per combo with params,
    compact metrics, loss counts, exit mixes, and half-period PnL.
    """
    meta = run.meta
    keys = keys_from_meta(meta)
    if not keys:
        raise ValueError(f"run {run.bundle_name} has no combo keys in meta.json")

    capital = float(meta.get("account_size") or 100_000.0)
    date_from = run.date_from or (meta.get("date_range") or [None, None])[0]
    date_to = run.date_to or (meta.get("date_range") or [None, None])[1]
    if not date_from or not date_to:
        raise ValueError("run meta missing date_range / date_from / date_to")

    trade_path = run.bundle_path / "trade_log.parquet"
    if not trade_path.is_file():
        raise FileNotFoundError(f"missing trade_log.parquet in {run.bundle_path}")

    tl = pd.read_parquet(
        trade_path,
        columns=["combo_idx", "pnl", "exit_reason", "entry_time", "entry_date"],
    )
    if tl.empty:
        raise ValueError("trade_log.parquet is empty")
    tl["pnl"] = tl["pnl"].astype(np.float64)
    tl["entry_time"] = pd.to_datetime(tl["entry_time"], utc=True)

    nav = read_nav(run, combo_idxs=None)
    stats_by_key = _all_combo_stats(
        tl,
        keys,
        capital=capital,
        nav_daily_df=nav if not nav.empty else None,
        date_from=date_from,
        date_to=date_to,
    )

    # Trade enrichments
    wins = tl.loc[tl["pnl"] > 0].groupby("combo_idx")["pnl"]
    losses = tl.loc[tl["pnl"] < 0].groupby("combo_idx")["pnl"]
    n_win = wins.count()
    n_loss = losses.count()
    avg_win = wins.mean()
    avg_loss = losses.mean()
    worst = tl.groupby("combo_idx")["pnl"].min()
    best = tl.groupby("combo_idx")["pnl"].max()

    er = tl.groupby(["combo_idx", "exit_reason"]).size().unstack(fill_value=0)
    er_pct = er.div(er.sum(axis=1), axis=0) if not er.empty else er

    mid = pd.Timestamp(f"{date_from}T00:00:00Z") + (
        pd.Timestamp(f"{date_to}T00:00:00Z") - pd.Timestamp(f"{date_from}T00:00:00Z")
    ) / 2
    tl = tl.copy()
    tl["half"] = np.where(tl["entry_time"] < mid, "H1", "H2")
    half = tl.groupby(["combo_idx", "half"])["pnl"].sum().unstack(fill_value=0.0)
    if "H1" not in half.columns:
        half["H1"] = 0.0
    if "H2" not in half.columns:
        half["H2"] = 0.0

    rows: list[dict[str, Any]] = []
    for i, key in enumerate(keys):
        params = params_from_key(key)
        st = stats_by_key.get(key) or {}
        nw = float(n_win.get(i, 0) or 0)
        nl = float(n_loss.get(i, 0) or 0)
        aw = float(avg_win.get(i, 0) or 0)
        al = float(avg_loss.get(i, 0) or 0)
        lw = None
        if aw > 0 and nl > 0:
            lw = float(-al / aw) if al < 0 else None
        exits = {}
        if not er_pct.empty and i in er_pct.index:
            exits = {str(c): float(er_pct.loc[i, c]) for c in er_pct.columns}
        rows.append(
            {
                "combo_idx": i,
                "combo_hash": key_hash(key),
                **params,
                "n": int(st.get("n") or 0),
                "total_pnl": float(st.get("total_pnl") or 0.0),
                "win_rate": float(st.get("win_rate") or 0.0),
                "profit_factor": float(st.get("profit_factor") or 0.0),
                "sharpe": float(st.get("sharpe") or 0.0),
                "max_dd_pct": float(st.get("max_dd_pct") or 0.0),
                "ann_return": float(st.get("ann_return") or 0.0),
                "n_win": int(nw),
                "n_loss": int(nl),
                "avg_win": aw,
                "avg_loss": al,
                "loss_win_ratio": lw if lw is not None else np.nan,
                "worst_trade": float(worst.get(i, 0) or 0),
                "best_trade": float(best.get(i, 0) or 0),
                "pnl_h1": float(half.loc[i, "H1"]) if i in half.index else 0.0,
                "pnl_h2": float(half.loc[i, "H2"]) if i in half.index else 0.0,
                "exits": exits,
            }
        )

    df = pd.DataFrame(rows)
    # Calmar from ann_return / DD (inspect uses equity_metrics; this is close enough
    # for ranking — live picks re-checked via inspect when available).
    dd_frac = (df["max_dd_pct"] / 100.0).replace(0, np.nan)
    df["calmar"] = df["ann_return"] / dd_frac
    df["both_halves_profit"] = (df["pnl_h1"] > 0) & (df["pnl_h2"] > 0)
    df["perfect_wr"] = df["win_rate"] >= 1.0 - 1e-12

    varying, fixed = varying_and_fixed(meta.get("param_grid"))
    # Drop vary keys that aren't columns (shouldn't happen)
    varying = [v for v in varying if v in df.columns]
    meta_bits = {
        "capital": capital,
        "date_from": date_from,
        "date_to": date_to,
        "half_split_mid": str(mid.date()),
        "varying_params": varying,
        "fixed_params": fixed,
        "param_grid": meta.get("param_grid") or {},
        "n_combos": len(df),
        "exit_reasons": sorted({k for d in df["exits"] for k in d}),
    }
    return df, meta_bits

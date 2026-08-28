"""Filtered parquet access and per-combo metrics (no full GridResult)."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from backtester.core.results import _all_combo_stats, equity_metrics
from backtester.inspect.resolve import ResolvedCombo, ResolvedRun, keys_from_meta
from backtester.ui.services.store_service import key_hash

COMPACT_METRIC_KEYS = (
    "n",
    "total_pnl",
    "win_rate",
    "profit_factor",
    "sharpe",
    "calmar",
    "max_dd_pct",
    "ann_return",
    "score",
)

TRADE_SCHEMA = [
    "combo_idx",
    "entry_time",
    "exit_time",
    "entry_spot",
    "exit_spot",
    "entry_price_usd",
    "exit_price_usd",
    "fees",
    "pnl",
    "triggered",
    "exit_reason",
    "exit_hour",
    "entry_date",
    "status",
]

FILL_SCHEMA = [
    "combo_idx",
    "trade_idx",
    "open_idx",
    "ts",
    "event",
    "contract",
    "side",
    "qty",
    "price_btc",
    "amount_btc",
    "fee_btc",
    "spot",
    "amount_usd",
    "fee_usd",
    "balance_usd",
    "exit_reason",
    "comment",
    "status",
]

NAV_SCHEMA = [
    "combo_idx",
    "date",
    "nav_low",
    "nav_high",
    "nav_close",
    "realized_close",
]


def _read_parquet_filtered(
    path: Path,
    combo_idxs: list[int] | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    if combo_idxs is None:
        return pd.read_parquet(path, columns=columns)

    # Predicate pushdown when the file has combo_idx
    try:
        table = pq.read_table(
            path,
            columns=columns,
            filters=[("combo_idx", "in", list(combo_idxs))],
        )
        return table.to_pandas()
    except Exception:
        df = pd.read_parquet(path, columns=columns)
        if "combo_idx" in df.columns:
            return df[df["combo_idx"].isin(combo_idxs)].reset_index(drop=True)
        return df


def read_trades(run: ResolvedRun, combo_idxs: list[int]) -> pd.DataFrame:
    return _read_parquet_filtered(run.bundle_path / "trade_log.parquet", combo_idxs)


def read_fills(run: ResolvedRun, combo_idxs: list[int]) -> pd.DataFrame:
    return _read_parquet_filtered(run.bundle_path / "fills.parquet", combo_idxs)


def read_nav(run: ResolvedRun, combo_idxs: list[int] | None = None) -> pd.DataFrame:
    return _read_parquet_filtered(run.bundle_path / "nav_daily.parquet", combo_idxs)


def read_final_nav(run: ResolvedRun) -> pd.DataFrame:
    path = run.bundle_path / "final_nav.parquet"
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_parquet(path)


def list_sidecar_files(run: ResolvedRun) -> list[str]:
    named = list(run.meta.get("sidecars") or [])
    extra = []
    for p in sorted(run.bundle_path.glob("*.parquet")):
        if p.name in (
            "trade_log.parquet",
            "nav_daily.parquet",
            "final_nav.parquet",
            "fills.parquet",
        ):
            continue
        if p.name not in named:
            extra.append(p.name)
    return named + extra


def schema_info(kind: str) -> dict[str, Any]:
    kind = kind.lower()
    mapping = {
        "trades": TRADE_SCHEMA,
        "trade_log": TRADE_SCHEMA,
        "fills": FILL_SCHEMA,
        "nav": NAV_SCHEMA,
        "nav_daily": NAV_SCHEMA,
        "greeks": [
            "combo_idx",
            "(strategy-defined columns in investor_greeks.parquet)",
        ],
    }
    if kind not in mapping:
        raise ValueError(
            f"unknown schema {kind!r}; choose trades|fills|nav|greeks"
        )
    return {"kind": kind, "columns": mapping[kind]}


def filter_trades(
    df: pd.DataFrame,
    *,
    since: str | None = None,
    until: str | None = None,
    pnl_lt: float | None = None,
    pnl_gt: float | None = None,
    exit_reason: str | None = None,
    trade_idx: int | None = None,
    pos_id: int | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df
    out = df
    # trade_idx on trade_log = 0-based row within the combo (before other filters)
    if trade_idx is not None:
        if "trade_idx" in out.columns:
            out = out[out["trade_idx"] == trade_idx]
        else:
            if 0 <= trade_idx < len(out):
                out = out.iloc[[trade_idx]]
            else:
                return out.iloc[0:0].reset_index(drop=True)
    if since:
        col = "entry_date" if "entry_date" in out.columns else "entry_time"
        out = out[out[col].astype(str) >= since]
    if until:
        col = "entry_date" if "entry_date" in out.columns else "entry_time"
        out = out[out[col].astype(str) <= until]
    if pnl_lt is not None and "pnl" in out.columns:
        out = out[out["pnl"] < pnl_lt]
    if pnl_gt is not None and "pnl" in out.columns:
        out = out[out["pnl"] > pnl_gt]
    if exit_reason and "exit_reason" in out.columns:
        out = out[out["exit_reason"].astype(str) == exit_reason]
    if pos_id is not None:
        for col in ("pos_id", "open_idx"):
            if col in out.columns:
                out = out[out[col] == pos_id]
                break
    return out.reset_index(drop=True)


def filter_fills(
    df: pd.DataFrame,
    *,
    since: str | None = None,
    until: str | None = None,
    exit_reason: str | None = None,
    trade_idx: int | None = None,
    pos_id: int | None = None,
    event: str | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df
    out = df
    if since and "ts" in out.columns:
        out = out[out["ts"].astype(str) >= since]
    if until and "ts" in out.columns:
        out = out[out["ts"].astype(str) <= until]
    if exit_reason and "exit_reason" in out.columns:
        out = out[out["exit_reason"].astype(str) == exit_reason]
    if trade_idx is not None and "trade_idx" in out.columns:
        out = out[out["trade_idx"] == trade_idx]
    if pos_id is not None and "open_idx" in out.columns:
        out = out[out["open_idx"] == pos_id]
    if event and "event" in out.columns:
        out = out[out["event"].astype(str) == event]
    return out.reset_index(drop=True)


def metrics_for_combos(
    run: ResolvedRun,
    combos: list[ResolvedCombo],
    *,
    full: bool = False,
    scores: dict[str, float] | None = None,
) -> list[dict[str, Any]]:
    """Compute stats for *combos* only via filtered parquets."""
    if not combos:
        return []
    idxs = [c.combo_idx for c in combos]
    df = read_trades(run, idxs)
    nav = read_nav(run, idxs)
    date_from = run.date_from or (run.meta.get("date_range") or [None, None])[0]
    date_to = run.date_to or (
        (run.meta.get("date_range") or [None, None])[1]
        if run.meta.get("date_range")
        else None
    )
    capital = float(run.meta.get("account_size") or 10000.0)

    # Remap keys list to only requested combos but keep original combo_idx in frames
    # _all_combo_stats iterates enumerate(keys) as combo_idx — so keys must be
    # aligned to the max combo_idx present, or we remap.
    keys_full = keys_from_meta(run.meta)
    # Build a dense key list covering 0..max(idx) with placeholders for gaps
    max_idx = max(idxs)
    keys_aligned: list[tuple] = []
    for i in range(max_idx + 1):
        if i < len(keys_full):
            keys_aligned.append(keys_full[i])
        else:
            keys_aligned.append((("_missing", i),))

    # Drop rows for idxs we don't want (already filtered) — stats skip empty idxs
    all_stats = _all_combo_stats(
        df,
        keys_aligned,
        capital=capital,
        nav_daily_df=nav if not nav.empty else None,
        date_from=date_from,
        date_to=date_to,
    )

    rows: list[dict[str, Any]] = []
    for combo in combos:
        key = combo.key
        stats = dict(all_stats.get(key) or {})
        # Calmar from equity_metrics on this combo alone
        df_c = df[df["combo_idx"] == combo.combo_idx] if not df.empty else df
        nav_c = (
            nav[nav["combo_idx"] == combo.combo_idx] if not nav.empty else None
        )
        eq = equity_metrics(
            df_c if df_c is not None and not df_c.empty else None,
            capital=capital,
            nav_daily_combo=nav_c,
            date_from=date_from,
            date_to=date_to,
        )
        calmar = float(eq.get("calmar") or 0.0) if eq else 0.0
        stats["calmar"] = calmar
        score = None
        if scores and combo.combo_hash in scores:
            score = scores[combo.combo_hash]
        stats["score"] = score

        if full:
            metrics = {k: _jsonable(v) for k, v in stats.items()}
            if eq:
                for k in ("sortino", "consec_wins", "consec_losses", "max_drawdown"):
                    if k in eq:
                        metrics[k] = _jsonable(eq[k])
        else:
            metrics = {
                k: _jsonable(stats.get(k))
                for k in COMPACT_METRIC_KEYS
            }
        rows.append(
            {
                **combo.summary(),
                "metrics": metrics,
            }
        )
    return rows


def top_combo_idxs_by_pnl(run: ResolvedRun, n: int) -> list[int]:
    """Cheap ranking from final_nav.parquet (no trade-log load)."""
    fn = read_final_nav(run)
    if fn.empty or "combo_idx" not in fn.columns:
        return list(range(min(n, len(keys_from_meta(run.meta)))))
    col = "realized_pnl" if "realized_pnl" in fn.columns else "final_nav"
    ranked = fn.sort_values(col, ascending=False)
    return [int(x) for x in ranked["combo_idx"].head(n).tolist()]


def combo_hashes_for_run(run: ResolvedRun) -> list[dict[str, Any]]:
    out = []
    for i, key in enumerate(keys_from_meta(run.meta)):
        out.append(
            {
                "combo_idx": i,
                "combo_hash": key_hash(key),
                "params": {k: v for k, v in key},
            }
        )
    return out


def df_records(df: pd.DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    if df.empty:
        return []
    view = df if limit is None else df.head(limit)
    # Timestamps → ISO strings
    out = view.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].astype(str)
    return [
        {k: _jsonable(v) for k, v in row.items()}
        for row in out.to_dict(orient="records")
    ]


def write_frame(df: pd.DataFrame, path: Path, fmt: str) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt = fmt.lower()
    if fmt == "csv":
        df.to_csv(path, index=False)
    elif fmt == "parquet":
        df.to_parquet(path, index=False)
    elif fmt == "json":
        path.write_text(df.to_json(orient="records", date_format="iso", indent=2))
    else:
        raise ValueError(f"unknown format {fmt!r}")


def _jsonable(v: Any) -> Any:
    if v is None:
        return None
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            pass
    if isinstance(v, (float, int, str, bool)):
        if isinstance(v, float) and (v != v):  # NaN
            return None
        return v
    return str(v)

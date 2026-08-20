#!/usr/bin/env python3
"""v14 loser skip analysis — entry-time features on run 727 baselines.

Builds ``analysis/v14_loser_skip_run727/``:
  manifest.json, verdict.json, data/*.parquet, report.html

Usage::

    PYTHONPATH=. .venv/bin/python analysis/v14_loser_skip_run727.py
"""
from __future__ import annotations

import base64
import io
import json
import math
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from scipy import stats

from backtester.core.config import cfg
from backtester.core.expiry_utils import parse_expiry_date
from backtester.core.paths import runs_dir
from backtester.indicators.hist_data import load_klines
from backtester.indicators.pipeline import build_indicators
from backtester.indicators import IndicatorDep
from backtester.indicators.vol_context import build_vol_context, lookup_vol_context
from backtester.ui.services.store_service import key_hash
from workspace.strategies.theta_shared import SKEW_DTE, SKEW_MIN_RUNGS, SKEW_RR_DELTA

# ── Constants ────────────────────────────────────────────────────────────────

OUT_DIR = Path("analysis/v14_loser_skip_run727")
BUNDLE_NAME = "theta_engine_v14_20260820_115925.bundle"
RUN_ID = 727
DATE_FROM = "2025-08-17"
DATE_TO = "2026-08-18"
CAPITAL = 100_000.0
N_LOSERS_POWERED = 20
FDR_ALPHA = 0.10
N_PERM = 1000
MIN_SKIPPED_LOSERS = 8
MIN_LOSER_USD_FRAC = 0.20
PERM_SEED = 42

BOOKS: Dict[str, Dict[str, Any]] = {
    "eff2523b17b8": {
        "display": "RichForce2 15 skew6",
        "role": "#1",
        "skew_source": "skew6",
    },
    "0b4573c70c01": {
        "display": "Daily16",
        "role": "fan-out",
        "skew_source": "skew6",
    },
    "c8573c839903": {
        "display": "RichForce2 16 front",
        "role": "fan-out",
        "skew_source": "front",
    },
    "760386167875": {
        "display": "RichDay15",
        "role": "fan-out",
        "skew_source": "skew6",
    },
}

METRICS = (
    "vrp",
    "dvol_rank_60",
    "short_mark_iv",
    "front_vrp",
    "otm_pct",
    "rr_abs",
    "rr_gap",
    "turbulence",
    "trend_regime",
)

METRIC_DEFS = {
    "vrp": "DVOL − RV30 (vol points, %)",
    "dvol_rank_60": "60d DVOL percentile (0–1)",
    "short_mark_iv": "Sold 25Δ mark IV (%)",
    "front_vrp": "short_mark_iv − DVOL",
    "otm_pct": "Strike distance from spot (%)",
    "rr_abs": "|25Δ RR| at book selector",
    "rr_gap": "|rr_front − rr_skew6|",
    "turbulence": "Pipeline turbulence.composite (0–100, shifted)",
    "trend_regime": "Closed-bar regime (+1/0/−1)",
}

_CONTRACT_RE = re.compile(
    r"^BTC-(?P<expiry>\d{1,2}[A-Z]{3}\d{2})-(?P<strike>\d+)-(?P<cp>[CP])$"
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _git_sha() -> Optional[str]:
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=Path(__file__).resolve().parents[1],
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
    except Exception:
        return None


def parse_contract(contract: str) -> Tuple[str, float, bool]:
    m = _CONTRACT_RE.match(str(contract))
    if not m:
        raise ValueError(f"bad contract: {contract}")
    return m.group("expiry"), float(m.group("strike")), m.group("cp") == "C"


def _select_by_delta_df(df: pd.DataFrame, target: float) -> Optional[pd.Series]:
    if df.empty:
        return None
    cands = df[df["delta"] != 0.0]
    if cands.empty:
        cands = df
    if cands.empty:
        return None
    idx = (cands["delta"] - target).abs().idxmin()
    return cands.loc[idx]


def _rr_25d(snap: pd.DataFrame, expiry: str) -> Optional[float]:
    chain = snap[snap["expiry"] == expiry]
    if chain.empty:
        return None
    call = _select_by_delta_df(chain[chain["is_call"]], +SKEW_RR_DELTA)
    put = _select_by_delta_df(chain[~chain["is_call"]], -SKEW_RR_DELTA)
    if call is None or put is None:
        return None
    civ = float(call["mark_iv"] or 0.0)
    piv = float(put["mark_iv"] or 0.0)
    if civ <= 0 or piv <= 0:
        return None
    return civ - piv


def _listed_by_dte(snap: pd.DataFrame, today) -> List[Tuple[str, int]]:
    rows: List[Tuple[str, int]] = []
    for exp in snap["expiry"].unique():
        exp_dt = parse_expiry_date(str(exp))
        if exp_dt is None:
            continue
        dte = (exp_dt.date() - today).days
        if dte >= 1:
            rows.append((str(exp), dte))
    rows.sort(key=lambda x: x[1])
    return rows


def _skew6_rr(snap: pd.DataFrame, today) -> Optional[float]:
    listed = _listed_by_dte(snap, today)
    if not listed:
        return None
    best_i = min(range(len(listed)), key=lambda i: (abs(listed[i][1] - SKEW_DTE), listed[i][1]))
    idxs = [best_i]
    if best_i > 0:
        idxs.insert(0, best_i - 1)
    if best_i + 1 < len(listed):
        idxs.append(best_i + 1)
    vals: List[float] = []
    for i in idxs:
        rr = _rr_25d(snap, listed[i][0])
        if rr is not None:
            vals.append(rr)
    if len(vals) < SKEW_MIN_RUNGS:
        return None
    return float(np.mean(vals))


def _lookup_asof(series_or_df: pd.DataFrame | pd.Series, ts: pd.Timestamp, col: Optional[str] = None):
    if series_or_df is None or len(series_or_df) == 0:
        return float("nan")
    idx = series_or_df.index
    if getattr(idx, "tz", None) is None:
        # leave as-is; compare after normalize
        pass
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    # Align naive indices to UTC for comparison
    if getattr(idx, "tz", None) is None:
        # treat as UTC
        valid = idx[idx <= ts.tz_localize(None) if False else idx]
        # simpler: convert idx
        try:
            idx2 = idx.tz_localize("UTC") if idx.tz is None else idx.tz_convert("UTC")
        except TypeError:
            idx2 = idx
        mask = idx2 <= ts
        if not mask.any():
            return float("nan")
        loc = series_or_df.iloc[int(np.where(mask)[0][-1])]
    else:
        idx2 = idx.tz_convert("UTC")
        mask = idx2 <= ts
        if not mask.any():
            return float("nan")
        loc = series_or_df.iloc[int(np.where(mask)[0][-1])]
    if col is not None:
        return float(loc[col]) if pd.notna(loc[col]) else float("nan")
    if isinstance(loc, pd.Series) and col is None:
        return float(loc.iloc[0]) if len(loc) else float("nan")
    return float(loc) if pd.notna(loc) else float("nan")


def _bh_fdr(pvals: Sequence[float]) -> np.ndarray:
    arr = np.asarray(pvals, dtype=float)
    out = np.full_like(arr, np.nan)
    ok = np.isfinite(arr)
    if ok.sum() == 0:
        return out
    out[ok] = stats.false_discovery_control(arr[ok], method="bh")
    return out


def _rank_biserial(x: np.ndarray, y: np.ndarray) -> float:
    """Effect size from Mann–Whitney: positive ⇒ x (losers) stochastically larger."""
    x = x[np.isfinite(x)]
    y = y[np.isfinite(y)]
    if len(x) < 2 or len(y) < 2:
        return float("nan")
    u, _ = stats.mannwhitneyu(x, y, alternative="two-sided")
    return float(1.0 - (2.0 * u) / (len(x) * len(y)))


def _max_dd(cum: np.ndarray) -> float:
    if len(cum) == 0:
        return 0.0
    peak = np.maximum.accumulate(cum)
    dd = cum - peak
    return float(dd.min())


def _approx_sharpe(pnls: np.ndarray) -> float:
    """Crude trade-level Sharpe (not annualised day NAV)."""
    if len(pnls) < 2:
        return float("nan")
    mu = pnls.mean()
    sd = pnls.std(ddof=1)
    if sd <= 1e-12:
        return float("nan")
    return float(mu / sd * math.sqrt(len(pnls)))


# ── Stage 1: join trades ─────────────────────────────────────────────────────

def resolve_combo_idx(meta: dict) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for i, key in enumerate(meta["keys"]):
        h = key_hash(tuple(map(tuple, key)))
        if h in BOOKS:
            out[h] = i
    missing = set(BOOKS) - set(out)
    if missing:
        raise RuntimeError(f"hashes not in meta.keys: {missing}")
    return out


def load_baseline_trades(bundle: Path) -> pd.DataFrame:
    meta = json.loads((bundle / "meta.json").read_text())
    hash_to_idx = resolve_combo_idx(meta)
    idx_to_hash = {v: k for k, v in hash_to_idx.items()}
    idxs = list(hash_to_idx.values())

    tl = pd.read_parquet(bundle / "trade_log.parquet")
    fills = pd.read_parquet(bundle / "fills.parquet")
    tl = tl[tl["combo_idx"].isin(idxs)].copy()
    opens = fills[(fills["event"] == "open") & (fills["combo_idx"].isin(idxs))].copy()

    # Pair open fill ↔ close trade on combo_idx + entry_time
    opens = opens.rename(columns={"ts": "entry_time", "spot": "fill_spot"})
    merged = tl.merge(
        opens[
            [
                "combo_idx",
                "entry_time",
                "open_idx",
                "contract",
                "side",
                "qty",
                "price_btc",
                "fill_spot",
                "amount_usd",
                "fee_usd",
                "balance_usd",
            ]
        ],
        on=["combo_idx", "entry_time"],
        how="inner",
        validate="one_to_one",
    )
    if len(merged) != len(tl):
        raise RuntimeError(
            f"join mismatch: trade_log={len(tl)} merged={len(merged)}"
        )

    parsed = [parse_contract(c) for c in merged["contract"].astype(str)]
    merged["expiry"] = [p[0] for p in parsed]
    merged["strike"] = [p[1] for p in parsed]
    merged["is_call"] = [p[2] for p in parsed]
    merged["leg_type"] = np.where(merged["is_call"], "call", "put")
    merged["combo_hash"] = merged["combo_idx"].map(idx_to_hash)
    merged["book"] = merged["combo_hash"].map(lambda h: BOOKS[h]["display"])
    merged["role"] = merged["combo_hash"].map(lambda h: BOOKS[h]["role"])
    merged["skew_source"] = merged["combo_hash"].map(lambda h: BOOKS[h]["skew_source"])

    merged["loser"] = merged["pnl"] < 0
    # Tail loser = worst quintile of PnL within book
    merged["tail_loser"] = False
    for h, g in merged.groupby("combo_hash"):
        q20 = g["pnl"].quantile(0.20)
        merged.loc[g.index, "tail_loser"] = g["pnl"] <= q20

    merged["powered"] = False
    for h, g in merged.groupby("combo_hash"):
        n_lose = int(g["loser"].sum())
        merged.loc[g.index, "powered"] = n_lose >= N_LOSERS_POWERED
        merged.loc[g.index, "n_losers_book"] = n_lose

    return merged.sort_values(["combo_hash", "entry_time"]).reset_index(drop=True)


# ── Stage 2: features ────────────────────────────────────────────────────────

def load_entry_chains(entry_times: Sequence[pd.Timestamp]) -> Dict[int, pd.DataFrame]:
    """Load option rows only at the given entry timestamps."""
    ts_us = sorted({int(pd.Timestamp(t).timestamp() * 1_000_000) for t in entry_times})
    days = sorted({pd.Timestamp(t).strftime("%Y-%m-%d") for t in entry_times})
    root = cfg.data.options_parquet
    day_files = [
        str(Path(root) / f"options_{d}.parquet")
        for d in days
        if (Path(root) / f"options_{d}.parquet").exists()
    ]
    if not day_files:
        raise FileNotFoundError("no options day files for entry dates")

    fmt = ds.ParquetFileFormat(dictionary_columns=[])
    dataset = ds.dataset(day_files, format=fmt)
    cols = [
        "timestamp", "expiry", "strike", "is_call",
        "bid_price", "ask_price", "mark_price", "mark_iv", "delta",
    ]
    filt = pc.field("timestamp").isin(ts_us)
    batches = []
    for batch in dataset.to_batches(columns=cols, filter=filt):
        exp_idx = batch.schema.get_field_index("expiry")
        if exp_idx >= 0 and pa.types.is_dictionary(batch.schema.field("expiry").type):
            batch = batch.set_column(
                exp_idx, "expiry", pc.cast(batch.column(exp_idx), pa.string())
            )
        batches.append(batch)
    table = pa.Table.from_batches(batches)
    df = table.to_pandas()
    by_ts: Dict[int, pd.DataFrame] = {}
    for ts, g in df.groupby("timestamp"):
        by_ts[int(ts)] = g.reset_index(drop=True)
    return by_ts


def build_panels(start: datetime, end: datetime) -> Dict[str, Any]:
    daily = load_klines("BTCUSDT", "1d", start, end, warmup_days=60)
    vol = build_vol_context(daily)
    inds = build_indicators(
        [
            IndicatorDep(name="turbulence", symbol="BTCUSDT", interval="15m"),
            IndicatorDep(name="trend_regime", symbol="BTCUSDT", interval="1d"),
        ],
        start,
        end,
    )
    return {
        "vol": vol,
        "turbulence": inds["turbulence"],
        "trend_regime": inds["trend_regime"],
    }


def enrich_features(trades: pd.DataFrame, panels: Dict[str, Any]) -> pd.DataFrame:
    chains = load_entry_chains(trades["entry_time"].tolist())
    rows = []
    for _, tr in trades.iterrows():
        ts = pd.Timestamp(tr["entry_time"])
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        ts_us = int(ts.timestamp() * 1_000_000)
        snap = chains.get(ts_us)
        spot = float(tr["entry_spot"])
        strike = float(tr["strike"])
        is_call = bool(tr["is_call"])
        expiry = str(tr["expiry"])

        # otm_pct from fill alone
        if is_call:
            otm = (strike - spot) / spot * 100.0
        else:
            otm = (spot - strike) / spot * 100.0

        short_iv = float("nan")
        rr_front = float("nan")
        rr_skew6 = float("nan")
        spread_pct = float("nan")
        if snap is not None and not snap.empty:
            q = snap[
                (snap["expiry"] == expiry)
                & (np.isclose(snap["strike"].astype(float), strike))
                & (snap["is_call"] == is_call)
            ]
            if len(q):
                short_iv = float(q.iloc[0]["mark_iv"])
                bid = float(q.iloc[0]["bid_price"] or 0.0)
                ask = float(q.iloc[0]["ask_price"] or 0.0)
                mid = 0.5 * (bid + ask) if (bid > 0 and ask > 0) else float("nan")
                if mid and mid > 0:
                    spread_pct = (ask - bid) / mid * 100.0
            rf = _rr_25d(snap, expiry)
            if rf is not None:
                rr_front = rf
            rs = _skew6_rr(snap, ts.date())
            if rs is not None:
                rr_skew6 = rs

        vol = lookup_vol_context(panels["vol"], ts.to_pydatetime())
        dvol = vol["dvol"]
        front_vrp = (
            short_iv - dvol
            if np.isfinite(short_iv) and np.isfinite(dvol)
            else float("nan")
        )

        # Book selector RR
        skew_src = tr["skew_source"]
        rr_comp = rr_skew6 if skew_src == "skew6" else rr_front
        rr_abs = abs(rr_comp) if np.isfinite(rr_comp) else float("nan")
        rr_gap = (
            abs(rr_front - rr_skew6)
            if np.isfinite(rr_front) and np.isfinite(rr_skew6)
            else float("nan")
        )

        # Turbulence: asof last closed hour (pipeline already shift(1))
        turb_df = panels["turbulence"]
        hour_ts = ts.floor("h")
        turbulence = float("nan")
        if turb_df is not None and len(turb_df):
            idx = turb_df.index
            if idx.tz is None:
                idx = idx.tz_localize("UTC")
            else:
                idx = idx.tz_convert("UTC")
            mask = idx <= hour_ts
            if mask.any():
                row = turb_df.iloc[int(np.where(mask)[0][-1])]
                turbulence = float(row["composite"]) if pd.notna(row["composite"]) else float("nan")

        # Trend regime closed-bar
        tr_df = panels["trend_regime"]
        trend = float("nan")
        if tr_df is not None and len(tr_df):
            day = ts.normalize()
            idx = tr_df.index
            if idx.tz is None:
                idx = idx.tz_localize("UTC")
            else:
                idx = idx.tz_convert("UTC")
            mask = idx <= day
            if mask.any():
                row = tr_df.iloc[int(np.where(mask)[0][-1])]
                trend = float(row["regime"]) if pd.notna(row["regime"]) else float("nan")

        trend_against = False
        if np.isfinite(trend):
            if is_call and trend > 0:
                trend_against = True
            if (not is_call) and trend < 0:
                trend_against = True

        rows.append(
            {
                "vrp": vol["vrp"],
                "dvol": dvol,
                "rv30": vol["rv30"],
                "dvol_rank_60": vol["dvol_rank_60"],
                "short_mark_iv": short_iv,
                "front_vrp": front_vrp,
                "otm_pct": otm,
                "rr_front": rr_front,
                "rr_skew6": rr_skew6,
                "rr_abs": rr_abs,
                "rr_gap": rr_gap,
                "turbulence": turbulence,
                "trend_regime": trend,
                "trend_against_short": trend_against,
                "spread_pct": spread_pct,
            }
        )

    feat = pd.DataFrame(rows)
    out = pd.concat([trades.reset_index(drop=True), feat], axis=1)
    return out


# ── Stage 3: univariate ──────────────────────────────────────────────────────

def univariate_table(trades: pd.DataFrame) -> pd.DataFrame:
    records = []
    books = list(trades["combo_hash"].unique()) + ["pooled"]
    for book in books:
        if book == "pooled":
            g = trades
            label = "pooled"
            display = "pooled"
        else:
            g = trades[trades["combo_hash"] == book]
            label = book
            display = BOOKS[book]["display"]
        losers = g[g["loser"]]
        winners = g[~g["loser"]]
        powered = bool(g["powered"].iloc[0]) if book != "pooled" else (
            int(g.groupby("combo_hash")["loser"].sum().ge(N_LOSERS_POWERED).sum()) >= 1
        )
        for m in METRICS:
            lx = losers[m].to_numpy(dtype=float)
            wx = winners[m].to_numpy(dtype=float)
            lx_f = lx[np.isfinite(lx)]
            wx_f = wx[np.isfinite(wx)]
            p = float("nan")
            rbis = float("nan")
            if len(lx_f) >= 3 and len(wx_f) >= 3:
                try:
                    _, p = stats.mannwhitneyu(lx_f, wx_f, alternative="two-sided")
                    rbis = _rank_biserial(lx_f, wx_f)
                except ValueError:
                    pass
            records.append(
                {
                    "book": display,
                    "combo_hash": label,
                    "metric": m,
                    "powered": powered,
                    "n_losers": int(len(lx_f)),
                    "n_winners": int(len(wx_f)),
                    "loser_median": float(np.nanmedian(lx)) if len(lx_f) else float("nan"),
                    "winner_median": float(np.nanmedian(wx)) if len(wx_f) else float("nan"),
                    "loser_p10": float(np.nanpercentile(lx_f, 10)) if len(lx_f) else float("nan"),
                    "loser_p90": float(np.nanpercentile(lx_f, 90)) if len(lx_f) else float("nan"),
                    "winner_p10": float(np.nanpercentile(wx_f, 10)) if len(wx_f) else float("nan"),
                    "winner_p90": float(np.nanpercentile(wx_f, 90)) if len(wx_f) else float("nan"),
                    "mw_pvalue": p,
                    "rank_biserial": rbis,
                }
            )
    uni = pd.DataFrame(records)

    # BH-FDR on #1 only
    h1 = "eff2523b17b8"
    mask = uni["combo_hash"] == h1
    uni.loc[mask, "fdr_q"] = _bh_fdr(uni.loc[mask, "mw_pvalue"].to_numpy())
    uni["fdr_pass"] = False
    uni.loc[mask, "fdr_pass"] = uni.loc[mask, "fdr_q"] < FDR_ALPHA
    # other books: no nomination
    uni.loc[~mask, "fdr_q"] = np.nan
    return uni


# ── Stage 4: skip hunt ───────────────────────────────────────────────────────

@dataclass
class Rule:
    metric: str
    direction: str  # "ge" or "le"
    threshold: float
    metric2: Optional[str] = None
    direction2: Optional[str] = None
    threshold2: Optional[float] = None

    def key(self) -> str:
        s = f"{self.metric}_{self.direction}_{self.threshold:.6g}"
        if self.metric2:
            s += f"__AND__{self.metric2}_{self.direction2}_{self.threshold2:.6g}"
        return s

    def mask(self, df: pd.DataFrame) -> pd.Series:
        x = df[self.metric]
        m = (x >= self.threshold) if self.direction == "ge" else (x <= self.threshold)
        m = m & x.notna()
        if self.metric2 is not None:
            y = df[self.metric2]
            m2 = (y >= self.threshold2) if self.direction2 == "ge" else (y <= self.threshold2)
            m = m & m2 & y.notna()
        return m


def _eval_rule(df: pd.DataFrame, rule: Rule) -> Dict[str, Any]:
    sk = rule.mask(df)
    losers = df["loser"]
    n_losers = int(losers.sum())
    n_winners = int((~losers).sum())
    loser_usd = float((-df.loc[losers, "pnl"]).clip(lower=0).sum())
    # Actual loser $ = sum of negative pnls (absolute)
    loser_usd = float((-df.loc[losers, "pnl"]).sum())
    winner_usd = float(df.loc[~losers, "pnl"].sum())

    sk_l = sk & losers
    sk_w = sk & ~losers
    n_sk_l = int(sk_l.sum())
    n_sk_w = int(sk_w.sum())
    usd_sk_l = float((-df.loc[sk_l, "pnl"]).sum()) if n_sk_l else 0.0
    usd_sk_w = float(df.loc[sk_w, "pnl"].sum()) if n_sk_w else 0.0

    frac_l = usd_sk_l / loser_usd if loser_usd > 1e-9 else 0.0
    frac_w = usd_sk_w / winner_usd if winner_usd > 1e-9 else 0.0
    lift = frac_l / frac_w if frac_w > 1e-9 else (999.0 if frac_l > 0 else 0.0)

    kept = df.loc[~sk]
    leftover_pnl = float(kept["pnl"].sum())
    leftover_sharpe = _approx_sharpe(kept["pnl"].to_numpy(dtype=float))
    # simple DD on daily cumsum of remaining realized pnl
    if len(kept):
        daily = kept.groupby(kept["entry_time"].dt.normalize())["pnl"].sum().sort_index()
        cum = CAPITAL + daily.cumsum().to_numpy(dtype=float)
        leftover_dd = _max_dd(cum)
    else:
        leftover_dd = 0.0

    base_pnl = float(df["pnl"].sum())
    anecdote = n_sk_l < MIN_SKIPPED_LOSERS or frac_l < MIN_LOSER_USD_FRAC

    return {
        "rule_key": rule.key(),
        "metric": rule.metric,
        "direction": rule.direction,
        "threshold": rule.threshold,
        "metric2": rule.metric2 or "",
        "direction2": rule.direction2 or "",
        "threshold2": rule.threshold2 if rule.threshold2 is not None else float("nan"),
        "n_skipped": int(sk.sum()),
        "n_losers_skipped": n_sk_l,
        "n_winners_skipped": n_sk_w,
        "loser_usd_skipped": usd_sk_l,
        "winner_usd_skipped": usd_sk_w,
        "frac_loser_usd": frac_l,
        "frac_winner_usd": frac_w,
        "lift": float(lift),
        "leftover_pnl": leftover_pnl,
        "base_pnl": base_pnl,
        "delta_pnl": leftover_pnl - base_pnl,
        "leftover_sharpe": leftover_sharpe,
        "leftover_dd": leftover_dd,
        "anecdote": anecdote,
        "abs_floor_ok": (n_sk_l >= MIN_SKIPPED_LOSERS) and (frac_l >= MIN_LOSER_USD_FRAC),
    }


def _candidate_rules(
    df: pd.DataFrame, metrics: Sequence[str], *, pairs: bool = True
) -> List[Rule]:
    """One-sided quantile cuts; optional AND of two metrics."""
    rules: List[Rule] = []
    qs = [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50, 0.60, 0.70, 0.75, 0.80, 0.85, 0.90]
    single: List[Rule] = []
    for m in metrics:
        x = df[m].dropna()
        if len(x) < 10:
            continue
        for q in qs:
            thr = float(np.quantile(x, q))
            single.append(Rule(m, "ge", thr))
            single.append(Rule(m, "le", thr))
        # fixed turbulence bands
        if m == "turbulence":
            for thr in (35.0, 50.0, 65.0, 80.0):
                single.append(Rule(m, "ge", thr))
        if m == "trend_regime":
            single.append(Rule(m, "ge", 0.5))
            single.append(Rule(m, "le", -0.5))
    # Dedup by key
    seen = set()
    for r in single:
        if r.key() in seen:
            continue
        seen.add(r.key())
        rules.append(r)

    # AND of two (only if ≥2 metrics)
    if pairs and len(metrics) >= 2:
        # Use coarser quantile grid for pairs
        coarse = [0.2, 0.3, 0.5, 0.7, 0.8]
        pair_singles: Dict[str, List[Rule]] = {m: [] for m in metrics}
        for m in metrics:
            x = df[m].dropna()
            if len(x) < 10:
                continue
            for q in coarse:
                thr = float(np.quantile(x, q))
                pair_singles[m].append(Rule(m, "ge", thr))
                pair_singles[m].append(Rule(m, "le", thr))
        mets = [m for m in metrics if pair_singles[m]]
        for i, m1 in enumerate(mets):
            for m2 in mets[i + 1 :]:
                for r1 in pair_singles[m1]:
                    for r2 in pair_singles[m2]:
                        rules.append(
                            Rule(
                                r1.metric, r1.direction, r1.threshold,
                                r2.metric, r2.direction, r2.threshold,
                            )
                        )
    return rules


def _best_lift(df: pd.DataFrame, rules: Sequence[Rule]) -> float:
    best = 0.0
    for r in rules:
        ev = _eval_rule(df, r)
        if ev["abs_floor_ok"] and ev["lift"] > best:
            best = ev["lift"]
    return best


def _time_split_lifts(df: pd.DataFrame, rule: Rule) -> Tuple[float, float]:
    mid = df["entry_time"].quantile(0.5)
    a = df[df["entry_time"] <= mid]
    b = df[df["entry_time"] > mid]
    la = _eval_rule(a, rule)["lift"] if len(a) >= 10 else float("nan")
    lb = _eval_rule(b, rule)["lift"] if len(b) >= 10 else float("nan")
    return la, lb


def _threshold_robust(df: pd.DataFrame, rule: Rule, all_rules: Sequence[Rule]) -> bool:
    """Nearby cuts (± one grid step on same metric/direction) still lift > 1."""
    same = [
        r for r in all_rules
        if r.metric == rule.metric
        and r.direction == rule.direction
        and r.metric2 is None
        and rule.metric2 is None
    ]
    if not same:
        return True
    thrs = sorted({r.threshold for r in same})
    try:
        i = thrs.index(rule.threshold)
    except ValueError:
        # find nearest
        i = int(np.argmin([abs(t - rule.threshold) for t in thrs]))
    neighbors = []
    if i > 0:
        neighbors.append(thrs[i - 1])
    if i + 1 < len(thrs):
        neighbors.append(thrs[i + 1])
    if not neighbors:
        # continuous ±20%
        neighbors = [rule.threshold * 0.8, rule.threshold * 1.2]
    for thr in neighbors:
        r2 = Rule(rule.metric, rule.direction, thr)
        if _eval_rule(df, r2)["lift"] <= 1.0:
            return False
    return True


def skip_hunt(trades: pd.DataFrame, uni: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    h1 = "eff2523b17b8"
    df1 = trades[trades["combo_hash"] == h1].copy()
    n_losers_1 = int(df1["loser"].sum())
    powered_1 = n_losers_1 >= N_LOSERS_POWERED

    fdr_metrics = (
        uni[(uni["combo_hash"] == h1) & (uni["fdr_pass"])]["metric"].tolist()
    )
    # Promotion hunt only when powered; FDR list still reported either way.
    fdr_for_hunt = fdr_metrics if powered_1 else []

    all_rows: List[Dict[str, Any]] = []
    verdict: Dict[str, Any] = {
        "status": "underpowered" if not powered_1 else "no_skip",
        "n_losers_h1": n_losers_1,
        "powered_h1": powered_1,
        "fdr_metrics": fdr_for_hunt,
        "fdr_metrics_descriptive": fdr_metrics,
        "candidates": [],
        "reason": "",
        "permutation_p95_lift": float("nan"),
    }

    if not powered_1:
        extra = ""
        if fdr_metrics:
            extra = (
                f" Descriptive note: {fdr_metrics} passed BH-FDR on #1 but cannot "
                f"be promoted with only {n_losers_1} losers."
            )
        verdict["reason"] = (
            f"#1 has only {n_losers_1} losers (<{N_LOSERS_POWERED}); "
            f"underpowered for skip promotion." + extra
        )
        # Still evaluate descriptive rules for the report (all tagged anecdote /
        # non-promotable). No FDR hunt, no permutation promotion.
        report_rules = _candidate_rules(df1, list(METRICS), pairs=False)
        for h, meta in BOOKS.items():
            g = trades[trades["combo_hash"] == h]
            for r in report_rules:
                ev = _eval_rule(g, r)
                ev.update(
                    {
                        "book": meta["display"],
                        "combo_hash": h,
                        "role": meta["role"],
                        "powered": bool(g["powered"].iloc[0]),
                        "fdr_metric": r.metric in fdr_metrics,
                        "perm_percentile": float("nan"),
                        "lift_h1": float("nan"),
                        "lift_h2": float("nan"),
                        "time_split_ok": False,
                        "threshold_robust": False,
                        "replication_ok": False,
                        "promoted": False,
                    }
                )
                all_rows.append(ev)
        return pd.DataFrame(all_rows), verdict

    fdr_metrics = fdr_for_hunt
    if not fdr_metrics:
        verdict["reason"] = (
            "No entry-time metric separated losers from winners on #1 after BH-FDR < 0.10. "
            "Losers look like winners at open; skip is the overnight path."
        )
        fdr_metrics_for_hunt: List[str] = []
    else:
        fdr_metrics_for_hunt = fdr_metrics

    # Always evaluate a broad rule set for the report (anecdote table),
    # but only FDR-passers can be promoted.
    report_metrics = list(METRICS)
    report_rules = _candidate_rules(df1, report_metrics)
    hunt_rules = _candidate_rules(df1, fdr_metrics_for_hunt) if fdr_metrics_for_hunt else []

    # Evaluate report rules on #1 + each fan-out book
    for h, meta in BOOKS.items():
        g = trades[trades["combo_hash"] == h]
        for r in report_rules:
            ev = _eval_rule(g, r)
            ev.update(
                {
                    "book": meta["display"],
                    "combo_hash": h,
                    "role": meta["role"],
                    "powered": bool(g["powered"].iloc[0]),
                    "fdr_metric": r.metric in fdr_metrics,
                    "perm_percentile": float("nan"),
                    "lift_h1": float("nan"),
                    "lift_h2": float("nan"),
                    "time_split_ok": False,
                    "threshold_robust": False,
                    "replication_ok": False,
                    "promoted": False,
                }
            )
            all_rows.append(ev)

    # Permutation null on hunt rules only
    perm_p95 = float("nan")
    if hunt_rules:
        rng = np.random.default_rng(PERM_SEED)
        best_null = np.empty(N_PERM, dtype=float)
        labels = df1["loser"].to_numpy().copy()
        for i in range(N_PERM):
            shuf = df1.copy()
            shuf["loser"] = rng.permutation(labels)
            best_null[i] = _best_lift(shuf, hunt_rules)
        perm_p95 = float(np.quantile(best_null, 0.95))
        verdict["permutation_p95_lift"] = perm_p95

        # Score hunt rules on #1 for promotion
        candidates = []
        for r in hunt_rules:
            ev = _eval_rule(df1, r)
            if not ev["abs_floor_ok"]:
                continue
            if ev["lift"] < perm_p95:
                continue
            la, lb = _time_split_lifts(df1, r)
            time_ok = (
                np.isfinite(la) and np.isfinite(lb) and (la > 1.0) and (lb > 1.0)
            )
            thr_ok = _threshold_robust(df1, r, hunt_rules) if r.metric2 is None else True
            # Replication: must not reverse on another powered fan-out
            repl_ok = True
            for h, meta in BOOKS.items():
                if h == h1:
                    continue
                g = trades[trades["combo_hash"] == h]
                if not bool(g["powered"].iloc[0]):
                    continue
                lift_o = _eval_rule(g, r)["lift"]
                if lift_o < 1.0:
                    repl_ok = False
                    break
            # Leftover not "same hill": require delta_pnl > 0 (skipped net losers)
            hill_ok = ev["delta_pnl"] > 0

            if time_ok and thr_ok and repl_ok and hill_ok:
                candidates.append(
                    {
                        "rule_key": r.key(),
                        "metric": r.metric,
                        "direction": r.direction,
                        "threshold": r.threshold,
                        "metric2": r.metric2,
                        "direction2": r.direction2,
                        "threshold2": r.threshold2,
                        "lift": ev["lift"],
                        "n_losers_skipped": ev["n_losers_skipped"],
                        "frac_loser_usd": ev["frac_loser_usd"],
                        "frac_winner_usd": ev["frac_winner_usd"],
                        "delta_pnl": ev["delta_pnl"],
                        "leftover_pnl": ev["leftover_pnl"],
                        "perm_p95": perm_p95,
                        "lift_h1": la,
                        "lift_h2": lb,
                    }
                )

            # Annotate matching rows in all_rows
            for row in all_rows:
                if row["combo_hash"] == h1 and row["rule_key"] == r.key():
                    row["perm_percentile"] = (
                        float((best_null < ev["lift"]).mean()) if hunt_rules else float("nan")
                    )
                    row["lift_h1"] = la
                    row["lift_h2"] = lb
                    row["time_split_ok"] = time_ok
                    row["threshold_robust"] = thr_ok
                    row["replication_ok"] = repl_ok
                    row["promoted"] = bool(
                        time_ok and thr_ok and repl_ok and hill_ok and ev["abs_floor_ok"]
                        and ev["lift"] >= perm_p95
                    )

        # Keep top 2 by lift
        candidates.sort(key=lambda c: c["lift"], reverse=True)
        candidates = candidates[:2]
        if candidates:
            verdict["status"] = "candidate"
            verdict["candidates"] = candidates
            verdict["reason"] = (
                f"{len(candidates)} skip rule(s) passed FDR, permutation, floors, "
                f"time-split, threshold, and replication gates on #1."
            )
        else:
            verdict["reason"] = (
                f"FDR-passers existed ({fdr_metrics}) but no rule cleared permutation "
                f"(p95 lift={perm_p95:.2f}), absolute floors, time-split, and replication."
            )
    elif not fdr_metrics:
        # powered but no FDR passers — keep no_skip reason already set above
        pass

    rules_df = pd.DataFrame(all_rows)
    return rules_df, verdict


# ── Stage 5: write bundle + HTML ─────────────────────────────────────────────

def _fig_to_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight", facecolor="white")
    import matplotlib.pyplot as plt

    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _inventory(trades: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for h, meta in BOOKS.items():
        g = trades[trades["combo_hash"] == h]
        losers = g[g["loser"]]
        rows.append(
            {
                "book": meta["display"],
                "role": meta["role"],
                "hash": h,
                "n": len(g),
                "n_losers": int(losers.shape[0]),
                "win_rate": float((~g["loser"]).mean()),
                "total_pnl": float(g["pnl"].sum()),
                "loser_usd": float((-losers["pnl"]).sum()),
                "max_loss": float(g["pnl"].min()),
                "pct_call": float((g["leg_type"] == "call").mean()),
                "powered": bool(g["powered"].iloc[0]),
            }
        )
    return pd.DataFrame(rows)


def write_html(
    out_dir: Path,
    trades: pd.DataFrame,
    uni: pd.DataFrame,
    rules: pd.DataFrame,
    verdict: Dict[str, Any],
    inventory: pd.DataFrame,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    charts: Dict[str, str] = {}

    # Feature overlap: boxplots for #1
    h1 = trades[trades["combo_hash"] == "eff2523b17b8"]
    n_m = len(METRICS)
    fig, axes = plt.subplots(3, 3, figsize=(12, 10))
    axes = axes.ravel()
    for i, m in enumerate(METRICS):
        ax = axes[i]
        w = h1.loc[~h1["loser"], m].dropna()
        l = h1.loc[h1["loser"], m].dropna()
        ax.boxplot(
            [w, l],
            tick_labels=["win", "lose"],
            showfliers=False,
            widths=0.55,
        )
        ax.set_title(m, fontsize=10)
        ax.grid(True, axis="y", alpha=0.3)
        # FDR badge
        row = uni[(uni["combo_hash"] == "eff2523b17b8") & (uni["metric"] == m)]
        if len(row) and bool(row.iloc[0]["fdr_pass"]):
            ax.set_facecolor("#e8f5e9")
    fig.suptitle(
        "#1 RichForce2 15 skew6 — entry metrics (win vs lose)\n"
        "green panel = FDR-pass on #1 · Source: run 727 · 2025-08-17→2026-08-18",
        fontsize=11,
    )
    fig.tight_layout()
    charts["overlap"] = _fig_to_b64(fig)

    # Skip cost vs benefit (#1 rules, non-anecdote or top by lift)
    if rules is not None and len(rules):
        r1 = rules[rules["combo_hash"] == "eff2523b17b8"].copy()
        r1 = r1.sort_values("lift", ascending=False).head(40)
        fig, ax = plt.subplots(figsize=(8, 6))
        colors = [
            "#14804a" if p else ("#c0392b" if a else "#1e6fbf")
            for p, a in zip(r1.get("promoted", False), r1["anecdote"])
        ]
        ax.scatter(
            r1["winner_usd_skipped"],
            r1["loser_usd_skipped"],
            c=colors,
            alpha=0.75,
            s=28,
        )
        ax.set_xlabel("Winner $ skipped")
        ax.set_ylabel("Loser $ skipped")
        ax.set_title("Skip cost vs benefit on #1 (top-40 by lift)")
        ax.grid(True, alpha=0.3)
        # legend proxies
        ax.scatter([], [], c="#14804a", label="promoted")
        ax.scatter([], [], c="#c0392b", label="anecdote (failed floor)")
        ax.scatter([], [], c="#1e6fbf", label="other")
        ax.legend(fontsize=8)
        fig.tight_layout()
        charts["skip_scatter"] = _fig_to_b64(fig)

        # Counterfactual equity for best promoted or best non-anecdote
        cand = r1[r1.get("promoted", False) == True]  # noqa: E712
        if cand.empty:
            cand = r1[~r1["anecdote"]].head(1)
        if len(cand):
            best = cand.iloc[0]
            # rebuild rule
            rule = Rule(
                best["metric"],
                best["direction"],
                float(best["threshold"]),
                best["metric2"] or None if best["metric2"] else None,
                best["direction2"] or None if best["metric2"] else None,
                float(best["threshold2"]) if best["metric2"] and np.isfinite(best["threshold2"]) else None,
            )
            sk = rule.mask(h1)
            base = h1.sort_values("entry_time")
            daily_base = base.groupby(base["entry_time"].dt.normalize())["pnl"].sum()
            kept = base.loc[~sk]
            daily_kept = kept.groupby(kept["entry_time"].dt.normalize())["pnl"].sum()
            idx = daily_base.index.union(daily_kept.index).sort_values()
            cum_b = CAPITAL + daily_base.reindex(idx, fill_value=0).cumsum()
            cum_k = CAPITAL + daily_kept.reindex(idx, fill_value=0).cumsum()
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.plot(idx, cum_b, label="baseline #1", color="#1e6fbf")
            ax.plot(idx, cum_k, label=f"skip {best['rule_key'][:40]}", color="#14804a")
            ax.set_ylabel("NAV ($)")
            ax.set_xlabel("Date")
            ax.set_title("#1 counterfactual cumulative PnL (trade-list, not full engine replay)")
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)
            fig.tight_layout()
            charts["counterfactual"] = _fig_to_b64(fig)

    # Build HTML
    inv_rows = ""
    for _, r in inventory.iterrows():
        inv_rows += (
            f"<tr><td>{r['book']}</td><td>{r['role']}</td><td>{r['n']}</td>"
            f"<td>{r['n_losers']}</td><td>{r['win_rate']:.1%}</td>"
            f"<td>${r['total_pnl']:,.0f}</td><td>${r['loser_usd']:,.0f}</td>"
            f"<td>${r['max_loss']:,.0f}</td><td>{r['pct_call']:.0%} call</td>"
            f"<td>{'powered' if r['powered'] else 'underpowered'}</td></tr>"
        )

    uni_h1 = uni[uni["combo_hash"] == "eff2523b17b8"].sort_values("mw_pvalue")
    uni_rows = ""
    for _, r in uni_h1.iterrows():
        badge = "PASS" if r["fdr_pass"] else "—"
        uni_rows += (
            f"<tr><td>{r['metric']}</td>"
            f"<td>{r['loser_median']:.3g}</td><td>{r['winner_median']:.3g}</td>"
            f"<td>{r['rank_biserial']:.3f}</td><td>{r['mw_pvalue']:.3g}</td>"
            f"<td>{r['fdr_q']:.3g}</td><td><b>{badge}</b></td></tr>"
        )

    # Top anecdote / rules table
    rule_rows = ""
    if rules is not None and len(rules):
        show = rules[rules["combo_hash"] == "eff2523b17b8"].sort_values(
            "lift", ascending=False
        ).head(15)
        for _, r in show.iterrows():
            tag = (
                "PROMOTED"
                if r.get("promoted")
                else ("anecdote" if r["anecdote"] else "fail gates")
            )
            rule_rows += (
                f"<tr><td style='font-size:11px'>{r['rule_key'][:48]}</td>"
                f"<td>{r['n_losers_skipped']}</td><td>{r['n_winners_skipped']}</td>"
                f"<td>${r['loser_usd_skipped']:,.0f}</td>"
                f"<td>${r['winner_usd_skipped']:,.0f}</td>"
                f"<td>{r['lift']:.2f}</td><td>{tag}</td></tr>"
            )

    status = verdict.get("status", "?")
    reason = verdict.get("reason", "")
    cands = verdict.get("candidates") or []
    cand_html = ""
    if cands:
        for c in cands:
            cand_html += (
                f"<li><code>{c['rule_key']}</code> — lift={c['lift']:.2f}, "
                f"losers skipped={c['n_losers_skipped']}, "
                f"ΔPnL=${c['delta_pnl']:,.0f}</li>"
            )
    else:
        cand_html = "<li>None</li>"

    def img(key: str, caption: str) -> str:
        if key not in charts:
            return ""
        return (
            f'<figure><img src="data:image/png;base64,{charts[key]}" '
            f'style="max-width:100%;height:auto"/>'
            f"<figcaption>{caption}</figcaption></figure>"
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<title>v14 loser skip — run 727</title>
<style>
  :root {{ --navy:#0d1b2a; --blue:#1e6fbf; --bg:#f0f2f7; --text:#1a1a2e; --muted:#6a7a8c; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         margin:0; background:var(--bg); color:var(--text); }}
  header {{ background:var(--navy); color:#fff; padding:1.2rem 1.5rem; }}
  header h1 {{ margin:0 0 .3rem; font-size:1.35rem; }}
  header p {{ margin:0; color:#a8c0d8; font-size:.9rem; }}
  main {{ max-width:1100px; margin:0 auto; padding:1.2rem 1rem 3rem; }}
  section {{ background:#fff; border:1px solid #dde3ec; border-radius:10px;
             padding:1rem 1.2rem; margin-bottom:1rem; }}
  h2 {{ margin:0 0 .7rem; font-size:1.05rem; color:var(--navy); }}
  .badge {{ display:inline-block; padding:.2rem .55rem; border-radius:6px;
            font-weight:700; font-size:.85rem; }}
  .badge.candidate {{ background:#e8f5e9; color:#14804a; }}
  .badge.no_skip {{ background:#fff3e0; color:#e67e22; }}
  .badge.underpowered {{ background:#fdecea; color:#c0392b; }}
  table {{ width:100%; border-collapse:collapse; font-size:.88rem; }}
  th, td {{ padding:.4rem .5rem; border-bottom:1px solid #eef0f3; text-align:left; }}
  th {{ color:var(--muted); font-weight:600; font-size:.75rem; text-transform:uppercase; }}
  figcaption {{ color:var(--muted); font-size:.8rem; margin-top:.35rem; }}
  code {{ background:#f4f6f9; padding:.1rem .3rem; border-radius:4px; font-size:.82rem; }}
  .reason {{ margin:.6rem 0; line-height:1.45; }}
</style>
</head>
<body>
<header>
  <h1>v14 loser skip analysis</h1>
  <p>Run 727 · four baseline books · entry-time causal metrics · 2025-08-17 → 2026-08-18</p>
</header>
<main>
<section>
  <h2>1. Verdict</h2>
  <p><span class="badge {status}">{status}</span></p>
  <p class="reason">{reason}</p>
  <p><b>Candidates:</b></p>
  <ul>{cand_html}</ul>
  <p style="color:var(--muted);font-size:.85rem">
    Gates: n_losers≥{N_LOSERS_POWERED} on #1 · BH-FDR&lt;{FDR_ALPHA} · permutation top 5% ·
    ≥{MIN_SKIPPED_LOSERS} losers and ≥{MIN_LOSER_USD_FRAC:.0%} loser $ ·
    time-split lift&gt;1 · threshold neighbours · no reverse on powered fan-out.
    8→6 is labelled anecdote, not a suggestion.
  </p>
</section>

<section>
  <h2>2. Loser inventory</h2>
  <table>
    <thead><tr>
      <th>Book</th><th>Role</th><th>N</th><th>Losers</th><th>WR</th>
      <th>PnL</th><th>Loser $</th><th>Max loss</th><th>Side</th><th>Power</th>
    </tr></thead>
    <tbody>{inv_rows}</tbody>
  </table>
</section>

<section>
  <h2>3. Entry-time overlap (#1)</h2>
  {img("overlap", "Boxplots win vs lose on #1. Green face = metric passed BH-FDR on #1.")}
  <table>
    <thead><tr>
      <th>Metric</th><th>Lose med</th><th>Win med</th><th>Rank-biserial</th>
      <th>MW p</th><th>FDR q</th><th>FDR</th>
    </tr></thead>
    <tbody>{uni_rows}</tbody>
  </table>
</section>

<section>
  <h2>4. Skip cost vs benefit</h2>
  {img("skip_scatter", "Each point is a 1- or 2-metric skip rule evaluated on #1.")}
  <table>
    <thead><tr>
      <th>Rule</th><th>Lose n</th><th>Win n</th><th>Lose $</th><th>Win $</th>
      <th>Lift</th><th>Tag</th>
    </tr></thead>
    <tbody>{rule_rows}</tbody>
  </table>
</section>

<section>
  <h2>5. #1 counterfactual</h2>
  {img("counterfactual", "Cumulative NAV from remaining trades after applying best rule (if any). Not a full engine replay.")}
  <p style="color:var(--muted);font-size:.85rem">
    Machine bundle: <code>analysis/v14_loser_skip_run727/</code> ·
    read <code>verdict.json</code> for the next experiment.
  </p>
</section>
</main>
</body>
</html>
"""
    (out_dir / "report.html").write_text(html, encoding="utf-8")


def main() -> int:
    print("=" * 60)
    print("  v14 loser skip — run 727")
    print("=" * 60)

    bundle = Path(runs_dir()) / BUNDLE_NAME
    if not bundle.exists():
        raise FileNotFoundError(bundle)

    out = OUT_DIR
    (out / "data").mkdir(parents=True, exist_ok=True)

    print("1) join trades…")
    trades = load_baseline_trades(bundle)
    inv = _inventory(trades)
    print(inv.to_string(index=False))

    print("2) build indicator panels…")
    start = datetime.fromisoformat(DATE_FROM).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(DATE_TO).replace(tzinfo=timezone.utc)
    panels = build_panels(start, end)

    print("3) enrich entry features…")
    trades = enrich_features(trades, panels)
    print(
        "feature coverage:",
        {m: float(trades[m].notna().mean()) for m in METRICS},
    )

    print("4) univariate + FDR…")
    uni = univariate_table(trades)
    h1_uni = uni[uni["combo_hash"] == "eff2523b17b8"][
        ["metric", "mw_pvalue", "fdr_q", "fdr_pass", "rank_biserial"]
    ]
    print(h1_uni.to_string(index=False))

    print("5) skip hunt + permutation…")
    rules, verdict = skip_hunt(trades, uni)
    print("verdict:", json.dumps({k: verdict[k] for k in ("status", "reason", "fdr_metrics")}, indent=2))

    print("6) write bundle…")
    manifest = {
        "run_id": RUN_ID,
        "bundle": str(bundle),
        "date_from": DATE_FROM,
        "date_to": DATE_TO,
        "capital": CAPITAL,
        "books": {
            h: {
                **meta,
                "combo_idx": int(
                    trades.loc[trades["combo_hash"] == h, "combo_idx"].iloc[0]
                ),
            }
            for h, meta in BOOKS.items()
        },
        "metrics": METRIC_DEFS,
        "gates": {
            "n_losers_powered": N_LOSERS_POWERED,
            "fdr_alpha": FDR_ALPHA,
            "n_perm": N_PERM,
            "min_skipped_losers": MIN_SKIPPED_LOSERS,
            "min_loser_usd_frac": MIN_LOSER_USD_FRAC,
        },
        "git_sha": _git_sha(),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (out / "verdict.json").write_text(json.dumps(verdict, indent=2, default=str), encoding="utf-8")

    trades.to_parquet(out / "data" / "trades.parquet", index=False)
    trades.to_csv(out / "data" / "trades.csv", index=False)
    uni.to_parquet(out / "data" / "univariate.parquet", index=False)
    if len(rules):
        rules.to_parquet(out / "data" / "skip_rules.parquet", index=False)
    else:
        pd.DataFrame().to_parquet(out / "data" / "skip_rules.parquet", index=False)
    trades[trades["loser"]].to_parquet(out / "data" / "losers.parquet", index=False)
    inv.to_csv(out / "data" / "inventory.csv", index=False)

    print("7) HTML report…")
    write_html(out, trades, uni, rules, verdict, inv)

    print(f"\nDone → {out.resolve()}")
    print(f"  report: {out / 'report.html'}")
    print(f"  verdict: {verdict['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

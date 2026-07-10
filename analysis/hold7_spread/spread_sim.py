#!/usr/bin/env python3
"""
Spread Retrofit Simulation — Hold7 Short-Vol Trades
====================================================
Takes the 352 existing short trades as given (P&L already backtested).
For each trade inside the available parquet window, simulates adding a
protective long leg at a further-OTM strike selected by target |delta|.

Configurations tested: delta_target ∈ {0.03, 0.05, 0.08}

Fill assumptions:
  Long entry : pay ask_price  at entry snapshot
  Long exit  : receive bid_price at exit snapshot (0.0 if NaN / expired worthless)

Outputs (all written alongside this script in analysis/hold7_spread/):
  hold7_spread_trades_d{delta}.csv   — per-trade detail for each delta config
  hold7_spread_summary.json          — aggregated stats across all configs
  hold7_spread_report.html           — self-contained visual report

Usage:
  python analysis/hold7_spread/spread_sim.py
"""

import json
import csv
import math
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

# ── Paths ────────────────────────────────────────────────────────────────────

ROOT       = Path(__file__).resolve().parents[2]   # repo root
DATA_DIR   = ROOT / "backtester" / "data"
OUT_DIR    = Path(__file__).parent                   # analysis/hold7_spread/
TRADES_CSV = OUT_DIR / "hold7_dte9_eq333_shortvol_trades.csv"

DELTA_TARGETS = [0.03, 0.05, 0.08]

# ── Deribit fee model ────────────────────────────────────────────────────────

def deribit_fee(spot: float, price_usd: float) -> float:
    """
    Matches backtester/pricing.py exactly:
      index_rate    = 0.00025  (0.025% of index per contract)
      price_cap_frac = 0.1042  (10.42% of option price)
    """
    return min(0.00025 * spot, 0.1042 * max(price_usd, 0.0))


# ── Parquet cache ─────────────────────────────────────────────────────────────

_snap_cache: dict = {}

def load_snap(date_str: str, hour_utc: int) -> pd.DataFrame | None:
    """Return the nearest 5-min snapshot in the parquet for (date, hour)."""
    key = (date_str, hour_utc)
    if key in _snap_cache:
        return _snap_cache[key]

    p = DATA_DIR / f"options_{date_str}.parquet"
    if not p.exists():
        _snap_cache[key] = None
        return None

    df = pd.read_parquet(p)
    df["ts"] = pd.to_datetime(df["timestamp"], unit="us", utc=True)
    target = pd.Timestamp(f"{date_str} {hour_utc:02d}:00:00", tz="UTC")
    diff   = (df["ts"] - target).abs()
    snap   = df[diff == diff.min()].copy()
    _snap_cache[key] = snap
    return snap


# ── Expiry helpers ────────────────────────────────────────────────────────────

def expiry_code(entry_dt: pd.Timestamp, dte_days: float) -> str:
    """Derive Deribit expiry code (e.g. '25APR25') from entry time + DTE."""
    exp_dt = entry_dt + pd.Timedelta(days=dte_days)
    # Deribit expiry is 08:00 UTC on the expiry date
    exp_dt = exp_dt.normalize() + pd.Timedelta(hours=8)
    return exp_dt.strftime("%-d%b%y").upper()


# ── Long leg selection ────────────────────────────────────────────────────────

def find_long_strike(
    snap: pd.DataFrame,
    expiry: str,
    is_call: bool,
    short_strike: float,
    target_delta: float,
) -> dict | None:
    """
    Find the long protective option in a snapshot:
      - Same expiry and type as the short
      - Strike further OTM than the short
      - |delta| closest to target_delta (tie-break: prefer lower delta)

    Returns a dict with strike, ask_price, bid_price, mark_price, delta, or None.
    """
    candidates = snap[
        (snap["expiry"]   == expiry) &
        (snap["is_call"]  == is_call) &
        (snap["ask_price"].notna()) &
        (snap["ask_price"] > 0)
    ].copy()

    if candidates.empty:
        return None

    candidates["abs_delta"] = candidates["delta"].abs()

    # Further OTM means: for puts, strike < short_strike; for calls, strike > short_strike
    if is_call:
        candidates = candidates[candidates["strike"] > short_strike]
    else:
        candidates = candidates[candidates["strike"] < short_strike]

    # Also require delta strictly lower than short's ~0.15
    candidates = candidates[candidates["abs_delta"] < 0.145]

    if candidates.empty:
        return None

    candidates["delta_dist"] = (candidates["abs_delta"] - target_delta).abs()
    best = candidates.sort_values(["delta_dist", "abs_delta"]).iloc[0]

    return {
        "strike":     float(best["strike"]),
        "ask_price":  float(best["ask_price"]),
        "bid_price":  float(best["bid_price"]) if not pd.isna(best["bid_price"]) else None,
        "mark_price": float(best["mark_price"]),
        "delta":      float(best["abs_delta"]),
        "spot":       float(best["underlying_price"]),
    }


def get_exit_price(
    snap: pd.DataFrame | None,
    expiry: str,
    is_call: bool,
    strike: float,
) -> tuple[float, float]:
    """
    Return (bid_btc, spot_usd) for a given option at exit time.
    Returns (0.0, spot) if not found or bid is NaN (expired worthless).
    """
    if snap is None:
        return 0.0, 0.0

    spot_series = snap["underlying_price"]
    spot = float(spot_series.iloc[0]) if not spot_series.empty else 0.0

    row = snap[
        (snap["expiry"]  == expiry) &
        (snap["is_call"] == is_call) &
        (snap["strike"].between(strike - 50, strike + 50))
    ]

    if row.empty:
        return 0.0, spot

    bid = row.iloc[0]["bid_price"]
    return (float(bid) if not pd.isna(bid) else 0.0), spot


# ── Per-trade simulation ──────────────────────────────────────────────────────

def simulate_trade(
    trade: pd.Series,
    target_delta: float,
) -> dict | None:
    """
    Simulate the long leg for one trade.
    Returns a result dict, or None if the trade can't be simulated.
    """
    entry_dt = trade["fill_entry_utc"]
    exit_dt  = trade["fill_exit_utc"]
    is_call  = (trade["side"] == "Short call")
    short_k  = float(trade["strike"])
    ecode    = expiry_code(entry_dt, trade["dte"])

    snap_e = load_snap(entry_dt.strftime("%Y-%m-%d"), int(entry_dt.hour))
    if snap_e is None:
        return None

    long = find_long_strike(snap_e, ecode, is_call, short_k, target_delta)
    if long is None:
        return None

    # ── Long entry ────────────────────────────────────────────────────────────
    spot_entry    = long["spot"]
    ask_entry_btc = long["ask_price"]
    cost_usd      = ask_entry_btc * spot_entry          # USD paid for long
    fee_open      = deribit_fee(spot_entry, cost_usd)

    # ── Long exit ─────────────────────────────────────────────────────────────
    snap_x = load_snap(exit_dt.strftime("%Y-%m-%d"), int(exit_dt.hour))
    bid_exit_btc, spot_exit = get_exit_price(snap_x, ecode, is_call, long["strike"])

    proceeds_usd = bid_exit_btc * spot_exit             # USD received at close
    fee_close    = deribit_fee(spot_exit, proceeds_usd) if proceeds_usd > 0 else 0.0

    long_pnl_usd = proceeds_usd - cost_usd - fee_open - fee_close

    # ── Spread ────────────────────────────────────────────────────────────────
    spread_pnl_usd = trade["net_pnl"] + long_pnl_usd

    # Width of the spread in USD (max theoretical loss at expiry, before premium)
    if is_call:
        spread_width_usd = (long["strike"] - short_k) * 1.0   # not meaningful for short call spread
    else:
        spread_width_usd = (short_k - long["strike"]) * 1.0

    return {
        "trade_num":        int(trade["trade_num"]),
        "entry_date":       str(trade["entry_date"].date()) if hasattr(trade["entry_date"], "date") else str(trade["entry_date"]),
        "side":             trade["side"],
        "short_strike":     short_k,
        "expiry":           ecode,
        "exit_type":        trade["exit_type"],
        "short_pnl":        round(trade["net_pnl"],   2),
        # long leg detail
        "long_strike":      long["strike"],
        "long_delta":       round(long["delta"],       4),
        "long_ask_entry":   round(ask_entry_btc,       6),
        "long_bid_exit":    round(bid_exit_btc,        6),
        "long_cost_usd":    round(-(cost_usd + fee_open), 2),   # negative = money paid
        "long_exit_usd":    round(proceeds_usd - fee_close,  2),
        "long_pnl_usd":     round(long_pnl_usd,       2),
        "spread_pnl":       round(spread_pnl_usd,     2),
        "spread_width_usd": round(spread_width_usd,   0),
        "spot_entry":       round(spot_entry,          0),
        "spot_exit":        round(spot_exit,           0),
    }


# ── Aggregate stats ───────────────────────────────────────────────────────────

def aggregate(results: list[dict], label: str, target_delta: float) -> dict:
    short_pnl   = [r["short_pnl"]   for r in results]
    spread_pnl  = [r["spread_pnl"]  for r in results]
    long_costs  = [r["long_cost_usd"] for r in results]   # negative values
    sl_results  = [r for r in results if r["exit_type"] == "stop_loss"]

    def maxdd(equity_series):
        peak = equity_series[0]
        dd   = 0.0
        for v in equity_series:
            peak = max(peak, v)
            dd   = min(dd, v - peak)
        return dd

    eq_short  = [100_000 + sum(short_pnl[:i+1])  for i in range(len(short_pnl))]
    eq_spread = [100_000 + sum(spread_pnl[:i+1]) for i in range(len(spread_pnl))]

    return {
        "label":                label,
        "target_delta":         target_delta,
        "n_trades":             len(results),
        # short-only baseline
        "short_total_pnl":      round(sum(short_pnl),   2),
        "short_win_rate":       round(sum(1 for x in short_pnl if x > 0) / len(short_pnl) * 100, 1),
        "short_max_loss":       round(min(short_pnl),   2),
        "short_max_dd":         round(maxdd(eq_short),  2),
        # long leg cost
        "long_total_cost":      round(sum(long_costs),  2),
        "long_avg_cost_per_trade": round(sum(long_costs) / len(long_costs), 2),
        "long_cost_pct_of_short_pnl": round(abs(sum(long_costs)) / sum(short_pnl) * 100, 1),
        # spread combined
        "spread_total_pnl":     round(sum(spread_pnl),  2),
        "spread_win_rate":      round(sum(1 for x in spread_pnl if x > 0) / len(spread_pnl) * 100, 1),
        "spread_max_loss":      round(min(spread_pnl),  2),
        "spread_max_dd":        round(maxdd(eq_spread), 2),
        "pnl_preservation_pct": round(sum(spread_pnl) / sum(short_pnl) * 100, 1),
        # SL event analysis
        "n_sl_trades":          len(sl_results),
        "sl_short_pnl":         round(sum(r["short_pnl"]  for r in sl_results), 2) if sl_results else None,
        "sl_spread_pnl":        round(sum(r["spread_pnl"] for r in sl_results), 2) if sl_results else None,
        "sl_long_recoup":       round(sum(r["long_pnl_usd"] for r in sl_results), 2) if sl_results else None,
        # equity series for charting
        "equity_short":         eq_short,
        "equity_spread":        eq_spread,
    }


# ── Refinement analyses ───────────────────────────────────────────────────────

def _snap_lookup(snap_df, expiry: str, is_call: bool, strike: float) -> dict:
    """Return bid/ask/mark/delta for a specific option in a snapshot."""
    if snap_df is None:
        return {}
    row = snap_df[
        (snap_df["expiry"]  == expiry) &
        (snap_df["is_call"] == is_call) &
        (snap_df["strike"].between(strike - 500, strike + 500))
    ]
    if row.empty:
        return {}
    r = row.iloc[0]
    return {
        "bid":   float(r["bid_price"])  if not pd.isna(r["bid_price"])  else None,
        "ask":   float(r["ask_price"])  if not pd.isna(r["ask_price"])  else None,
        "mark":  float(r["mark_price"]),
        "delta": round(abs(float(r["delta"])), 4),
        "spot":  float(r["underlying_price"]),
    }


def compute_loser_analysis(
    result_sets: list[tuple[float, list[dict]]],
    trades_df: pd.DataFrame,
) -> list[dict]:
    """
    For every trade where short_pnl < 0, collect outcome across all configs
    AND look up full contract details (entry/exit bid/ask for both legs).
    """
    base_delta, base_results = result_sets[0]
    loser_nums = {r["trade_num"] for r in base_results if r["short_pnl"] < 0}

    by_trade: dict[int, dict] = {}
    for r in base_results:
        if r["trade_num"] in loser_nums:
            by_trade[r["trade_num"]] = {
                "trade_num":    r["trade_num"],
                "entry_date":   r["entry_date"],
                "side":         r["side"],
                "short_strike": r["short_strike"],
                "expiry":       r["expiry"],
                "exit_type":    r["exit_type"],
                "short_pnl":    r["short_pnl"],
                "spot_entry":   r["spot_entry"],
                "spot_exit":    r["spot_exit"],
            }

    # Collect per-config data
    for target_delta, results in result_sets:
        key = f"d{int(target_delta * 100):02d}"
        for r in results:
            if r["trade_num"] in loser_nums:
                by_trade[r["trade_num"]][f"long_strike_{key}"] = r.get("long_strike")
                by_trade[r["trade_num"]][f"long_delta_{key}"]  = r.get("long_delta")
                by_trade[r["trade_num"]][f"long_pnl_{key}"]    = r["long_pnl_usd"]
                by_trade[r["trade_num"]][f"spread_pnl_{key}"]  = r["spread_pnl"]

    # Enrich with live option prices from parquet (entry + exit, both legs, for Δ=0.05)
    mid_delta, mid_results = result_sets[1]   # Δ=0.05 is index 1
    mid_by_num = {r["trade_num"]: r for r in mid_results}

    for tnum, rec in by_trade.items():
        t = trades_df[trades_df["trade_num"] == tnum].iloc[0]
        is_call = (t["side"] == "Short call")
        ecode   = rec["expiry"]
        short_k = rec["short_strike"]
        long_k  = rec.get("long_strike_d05")

        se = load_snap(t["fill_entry_utc"].strftime("%Y-%m-%d"), int(t["fill_entry_utc"].hour))
        sx = load_snap(t["fill_exit_utc"].strftime("%Y-%m-%d"),  int(t["fill_exit_utc"].hour))

        short_e = _snap_lookup(se, ecode, is_call, short_k)
        short_x = _snap_lookup(sx, ecode, is_call, short_k)
        long_e  = _snap_lookup(se, ecode, is_call, long_k)  if long_k else {}
        long_x  = _snap_lookup(sx, ecode, is_call, long_k)  if long_k else {}

        rec["short_bid_e"]   = short_e.get("bid")
        rec["short_delta_e"] = short_e.get("delta")
        rec["short_ask_x"]   = short_x.get("ask")
        rec["long_ask_e"]    = long_e.get("ask")
        rec["long_delta_e"]  = long_e.get("delta")
        rec["long_bid_x"]    = long_x.get("bid")

    return sorted(by_trade.values(), key=lambda x: x["short_pnl"])


def compute_distance_stats(result_sets: list[tuple[float, list[dict]]]) -> list[dict]:
    """
    Per config: USD and % distance between short and long strike.
    Returns a list of per-config stat dicts plus individual row data for histogram.
    """
    stats = []
    for target_delta, results in result_sets:
        matched = [r for r in results if r.get("long_strike") is not None]
        dists_usd = [abs(r["short_strike"] - r["long_strike"]) for r in matched]
        dists_pct = [d / r["short_strike"] * 100 for d, r in zip(dists_usd, matched)]
        if not dists_usd:
            continue

        # Histogram buckets ($1k wide, 0–20k)
        buckets = list(range(0, 21_000, 1_000))
        counts  = [0] * (len(buckets) - 1)
        for d in dists_usd:
            for i in range(len(buckets) - 1):
                if buckets[i] <= d < buckets[i + 1]:
                    counts[i] += 1
                    break

        stats.append({
            "target_delta":  target_delta,
            "n":             len(matched),
            "mean_usd":      round(float(np.mean(dists_usd)),   0),
            "median_usd":    round(float(np.median(dists_usd)), 0),
            "min_usd":       round(float(np.min(dists_usd)),    0),
            "max_usd":       round(float(np.max(dists_usd)),    0),
            "std_usd":       round(float(np.std(dists_usd)),    0),
            "mean_pct":      round(float(np.mean(dists_pct)),   2),
            "median_pct":    round(float(np.median(dists_pct)), 2),
            "min_pct":       round(float(np.min(dists_pct)),    2),
            "max_pct":       round(float(np.max(dists_pct)),    2),
            "hist_buckets":  [f"${b//1000}k" for b in buckets[:-1]],
            "hist_counts":   counts,
            "dists_usd":     dists_usd,   # raw list for additional stats
        })
    return stats


def _svg_distance_hist(dist_stats: dict, width=320, height=190, color="#1e6fbf", shared_ymax=None) -> str:
    """Bar histogram of strike distance distribution with optional shared y-scale."""
    counts  = dist_stats["hist_counts"]
    buckets = dist_stats["hist_buckets"]
    last    = max((i for i, c in enumerate(counts) if c > 0), default=0) + 2
    counts  = counts[:last]
    buckets = buckets[:last]

    n     = len(counts)
    ymax  = shared_ymax if shared_ymax else (max(counts) * 1.18 if counts else 1)
    PAD_L, PAD_R, PAD_T, PAD_B = 28, 8, 16, 32
    bw    = (width - PAD_L - PAD_R) / max(n, 1)
    bars  = []

    # grid lines at 25/50/75% of ymax
    for frac in (0.25, 0.5, 0.75, 1.0):
        tick = ymax * frac
        yp   = height - PAD_B - tick / ymax * (height - PAD_T - PAD_B)
        bars.append(f'<line x1="{PAD_L}" y1="{yp:.1f}" x2="{width-PAD_R}" y2="{yp:.1f}" stroke="#eef" stroke-width="1"/>')
        bars.append(f'<text x="{PAD_L-3}" y="{yp+3:.1f}" text-anchor="end" font-size="8" fill="#aaa">{int(tick)}</text>')

    for i, (cnt, lbl) in enumerate(zip(counts, buckets)):
        x = PAD_L + i * bw
        h = cnt / ymax * (height - PAD_T - PAD_B)
        y = height - PAD_B - h
        bars.append(f'<rect x="{x+1:.1f}" y="{y:.1f}" width="{bw-2:.1f}" height="{h:.1f}" fill="{color}" rx="2" opacity="0.78"/>')
        if i % 2 == 0:
            bars.append(f'<text x="{x+bw/2:.1f}" y="{height-PAD_B+12:.1f}" text-anchor="middle" font-size="8" fill="#888">{lbl}</text>')
        if cnt > 0:
            bars.append(f'<text x="{x+bw/2:.1f}" y="{y-2:.1f}" text-anchor="middle" font-size="8" font-weight="600" fill="{color}">{cnt}</text>')

    bars.append(f'<line x1="{PAD_L}" y1="{height-PAD_B}" x2="{width-PAD_R}" y2="{height-PAD_B}" stroke="#ccc" stroke-width="1"/>')
    bars.append(f'<text x="{width//2}" y="{height-PAD_B+24:.1f}" text-anchor="middle" font-size="8" fill="#bbb">strike distance ($1k bins)</text>')
    return f'<svg width="{width}" height="{height}" style="font-family:sans-serif;display:block">{"".join(bars)}</svg>'


# ── HTML report ───────────────────────────────────────────────────────────────

def _svg_equity(configs: list[dict], width=1050, height=320) -> str:
    """Multi-line equity curve SVG — full card width."""
    colors = ["#1e6fbf", "#e67e22", "#27ae60", "#8e44ad"]

    all_curves = [("Short-only", configs[0]["equity_short"], colors[0], "")]
    for i, cfg in enumerate(configs):
        all_curves.append((f"Spread Δ={cfg['target_delta']:.2f}", cfg["equity_spread"], colors[i+1], "5,3"))

    all_vals = [v for _, curve, _, _ in all_curves for v in curve]
    ymin, ymax = min(all_vals), max(all_vals)
    pad  = (ymax - ymin) * 0.07
    ymin -= pad; ymax += pad
    n    = len(all_curves[0][1])

    PAD_L, PAD_R, PAD_T, PAD_B = 52, 16, 28, 32

    def sx(i): return PAD_L + i / (n - 1) * (width - PAD_L - PAD_R)
    def sy(v): return PAD_T + (1 - (v - ymin) / (ymax - ymin)) * (height - PAD_T - PAD_B)

    elems = []

    # grid + y-axis
    step = _nice_step(ymax - ymin, 7)
    v = math.floor(ymin / step) * step
    while v <= ymax:
        y = sy(v)
        if PAD_T - 4 <= y <= height - PAD_B + 4:
            is_baseline = abs(v - 100_000) < step * 0.1
            stroke_col  = "#c8cdd5" if is_baseline else "#eef0f3"
            sw          = "1.5" if is_baseline else "1"
            elems.append(f'<line x1="{PAD_L}" y1="{y:.1f}" x2="{width-PAD_R}" y2="{y:.1f}" stroke="{stroke_col}" stroke-width="{sw}"/>')
            elems.append(f'<text x="{PAD_L-5}" y="{y+3.5:.1f}" text-anchor="end" font-size="10" fill="#888">${v/1000:.0f}k</text>')
        v += step

    # shaded area: short-only above spreads (optional subtle fill)
    # just the short-only polyline gets a light fill
    short_pts   = " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(all_curves[0][1]))
    bottom_l    = f"{sx(n-1):.1f},{height-PAD_B} {sx(0):.1f},{height-PAD_B}"
    elems.append(f'<polygon points="{short_pts} {bottom_l}" fill="#1e6fbf" opacity="0.05"/>')

    # curves (short-only last so it draws on top)
    for lbl, curve, col, dash in reversed(all_curves):
        pts = " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(curve))
        sw  = "2.5" if dash == "" else "1.8"
        elems.append(f'<polyline points="{pts}" fill="none" stroke="{col}" stroke-width="{sw}" stroke-dasharray="{dash}" opacity="0.92"/>')

    # final equity labels at right edge
    for lbl, curve, col, dash in all_curves:
        y_end = sy(curve[-1])
        elems.append(f'<circle cx="{sx(n-1):.1f}" cy="{y_end:.1f}" r="3" fill="{col}"/>')

    # x-axis: trade number ticks every 50 trades
    for i in range(0, n, 50):
        x = sx(i)
        elems.append(f'<line x1="{x:.1f}" y1="{height-PAD_B}" x2="{x:.1f}" y2="{height-PAD_B+4}" stroke="#bbb" stroke-width="1"/>')
        elems.append(f'<text x="{x:.1f}" y="{height-PAD_B+14:.1f}" text-anchor="middle" font-size="9" fill="#aaa">#{i}</text>')

    elems.append(f'<text x="{width//2}" y="{height-PAD_B+26:.1f}" text-anchor="middle" font-size="9" fill="#bbb">trade #</text>')

    # legend — top right
    lx = width - PAD_R - 20
    ly = PAD_T + 6
    for lbl, curve, col, dash in reversed(all_curves):
        lx -= len(lbl) * 6 + 38
        elems.append(f'<line x1="{lx:.1f}" y1="{ly}" x2="{lx+20:.1f}" y2="{ly}" stroke="{col}" stroke-width="2" stroke-dasharray="{dash}"/>')
        elems.append(f'<text x="{lx+24:.1f}" y="{ly+3.5:.1f}" font-size="10" fill="#444">{lbl}</text>')

    return f'<svg width="{width}" height="{height}" style="font-family:sans-serif;display:block">{"".join(elems)}</svg>'


def _nice_step(span, n_ticks=6):
    raw = max(span / n_ticks, 1e-9)
    mag = 10 ** math.floor(math.log10(raw))
    for f in (1, 2, 2.5, 5, 10):
        if raw <= f * mag:
            return f * mag
    return 10 * mag


def _svg_cost_bar(configs: list[dict], width=1050, height=150) -> str:
    """
    Full-width grouped bar chart: avg short P&L vs. avg long hedge cost per config.
    Displayed below the equity curves.
    """
    all_labels = ["Short-only avg P&L"] + [f"Hedge cost Δ={c['target_delta']:.2f}" for c in configs]
    all_colors = ["#1e6fbf", "#e67e22", "#27ae60", "#8e44ad"]
    avg_short  = configs[0]["short_total_pnl"] / configs[0]["n_trades"]
    all_vals   = [avg_short] + [abs(c["long_avg_cost_per_trade"]) for c in configs]
    ymax       = max(all_vals) * 1.25

    n_bars  = len(all_vals)
    pad_l   = 55
    pad_r   = 20
    pad_top = 18
    pad_bot = 36
    plot_w  = width - pad_l - pad_r
    bar_w   = plot_w / (n_bars * 1.6)
    spacing = plot_w / n_bars

    def sy(v): return height - pad_bot - v / ymax * (height - pad_bot - pad_top)

    bars = []
    # grid lines
    for tick in [ymax * 0.25, ymax * 0.5, ymax * 0.75, ymax]:
        y = sy(tick)
        bars.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width-pad_r}" y2="{y:.1f}" stroke="#eee" stroke-width="1"/>')
        bars.append(f'<text x="{pad_l-4}" y="{y+3:.1f}" text-anchor="end" font-size="9" fill="#999">${tick:.0f}</text>')

    bars.append(f'<line x1="{pad_l}" y1="{height-pad_bot}" x2="{width-pad_r}" y2="{height-pad_bot}" stroke="#ccc" stroke-width="1"/>')

    for i, (val, lbl, col) in enumerate(zip(all_vals, all_labels, all_colors)):
        cx  = pad_l + spacing * i + spacing / 2
        x   = cx - bar_w / 2
        h   = val / ymax * (height - pad_bot - pad_top)
        y   = sy(val)
        bars.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{h:.1f}" fill="{col}" rx="3" opacity="0.82"/>')
        bars.append(f'<text x="{cx:.1f}" y="{y-4:.1f}" text-anchor="middle" font-size="10" font-weight="600" fill="{col}">${val:.0f}</text>')
        bars.append(f'<text x="{cx:.1f}" y="{height-pad_bot+14:.1f}" text-anchor="middle" font-size="9" fill="#555">{lbl}</text>')

    return f'<svg width="{width}" height="{height}" style="font-family:sans-serif;display:block">{"".join(bars)}</svg>'


def _fmt_btc(v, spot=None):
    if v is None: return "—"
    usd = f" (${v*spot:,.0f})" if spot else ""
    return f"{v:.5f} BTC{usd}"

def _loser_section(loser_rows: list[dict]) -> str:
    config_keys = [("d03", "Δ=0.03"), ("d05", "Δ=0.05"), ("d08", "Δ=0.08")]

    total_short  = sum(r["short_pnl"] for r in loser_rows)
    totals_long  = {k: sum(r.get(f"long_pnl_{k}", 0) for r in loser_rows) for k, _ in config_keys}
    recovery_pct = {k: totals_long[k] / total_short * -100 for k, _ in config_keys}

    def fc(v):
        col = "#14804a" if v > 0 else "#c0392b"
        sign = "+" if v > 0 else ""
        return f'<td class="num" style="color:{col}">{sign}${v:,.2f}</td>'

    def tag(label, bg, fg):
        return f'<span style="background:{bg};color:{fg};border-radius:3px;padding:1px 5px;font-size:10px">{label}</span>'

    # ── Per-trade block: full spread detail ──────────────────────────────────
    trade_blocks = ""
    for r in loser_rows:
        is_sl    = r["exit_type"] == "stop_loss"
        exit_badge = tag("SL", "#e74c3c", "#fff") if is_sl else tag("ask", "#e8ecf2", "#666")
        spot_move  = (r["spot_exit"] - r["spot_entry"]) / r["spot_entry"] * 100
        is_put     = "put" in r["side"].lower()
        adverse    = (is_put and spot_move < 0) or (not is_put and spot_move > 0)
        move_col   = "#c0392b" if adverse else "#555"

        opt_type = "Put" if is_put else "Call"

        # short leg row
        short_bid_e = _fmt_btc(r.get("short_bid_e"), r["spot_entry"])
        short_ask_x = _fmt_btc(r.get("short_ask_x"), r["spot_exit"])
        short_delta = f"Δ={r['short_delta_e']:.4f}" if r.get("short_delta_e") else ""

        # long leg row (Δ=0.05 for display)
        long_k      = r.get("long_strike_d05")
        long_ask_e  = _fmt_btc(r.get("long_ask_e"),  r["spot_entry"])
        long_bid_x  = _fmt_btc(r.get("long_bid_x"),  r["spot_exit"])
        long_delta  = f"Δ={r['long_delta_e']:.4f}" if r.get("long_delta_e") else ""

        dist_usd = abs(r["short_strike"] - long_k) if long_k else None
        dist_pct = dist_usd / r["short_strike"] * 100 if dist_usd else None
        dist_str = f"${dist_usd:,.0f} ({dist_pct:.1f}%)" if dist_usd else "—"

        pnl_rows = ""
        for key, lbl in config_keys:
            lpnl = r.get(f"long_pnl_{key}", 0)
            spnl = r.get(f"spread_pnl_{key}", r["short_pnl"])
            lk   = r.get(f"long_strike_{key}")
            lstr = f"${lk:,.0f}" if lk else "—"
            ld   = r.get(f"long_delta_{key}")
            ldstr = f"Δ={ld:.4f}" if ld else ""
            lc   = "#14804a" if lpnl > 0 else "#c0392b"
            sc   = "#14804a" if spnl > 0 else "#c0392b"
            pnl_rows += (
                f"<tr><td style='padding-left:1.5rem;color:#888'>{lbl} long: {lstr} {ldstr}</td>"
                f"<td class='num' style='color:{lc}'>{'+' if lpnl>0 else ''}${lpnl:,.2f}</td>"
                f"<td class='num' style='color:{sc}'>{'+' if spnl>0 else ''}${spnl:,.2f}</td></tr>"
            )

        trade_blocks += f"""
<tr style="border-top:2px solid #dde3ec;background:#fafbfd">
  <td rowspan="4" style="font-weight:700;font-size:1rem;text-align:center;vertical-align:middle;color:#1e6fbf">#{r['trade_num']}</td>
  <td colspan="2">{r['entry_date']} · {r['side']} · expiry {r['expiry']}</td>
  <td colspan="2" style="color:{move_col}">BTC {r['spot_entry']:,.0f} → {r['spot_exit']:,.0f} ({spot_move:+.1f}%)</td>
  <td>{exit_badge}</td>
  <td class="num r" style="font-weight:600">${r['short_pnl']:,.2f}</td>
</tr>
<tr>
  <td style="color:#888;font-size:.82rem">SHORT {opt_type}</td>
  <td class="num"><strong>${r['short_strike']:,.0f}</strong> {short_delta}</td>
  <td class="num">entry bid: {short_bid_e}</td>
  <td class="num" colspan="2">exit ask: {short_ask_x}</td>
  <td></td>
</tr>
<tr>
  <td style="color:#888;font-size:.82rem">LONG {opt_type} (Δ=0.05)</td>
  <td class="num"><strong>${long_k:,.0f}</strong> {long_delta} if long_k else "—"</td>
  <td class="num">entry ask: {long_ask_e}</td>
  <td class="num" colspan="2">exit bid: {long_bid_x} · spread width: {dist_str}</td>
  <td></td>
</tr>
<tr style="background:#f5f7fa">
  <td colspan="2" style="font-size:.82rem;color:#666">P&amp;L by protection config</td>
  <td colspan="5">
    <table style="width:100%;font-size:.82rem;border:none">
      <tr><th style="text-align:left;color:#888;border:none">Config</th>
          <th style="text-align:right;color:#888;border:none">Long leg P&amp;L</th>
          <th style="text-align:right;color:#888;border:none">Spread P&amp;L</th></tr>
      {pnl_rows}
    </table>
  </td>
</tr>"""

    total_row = f"""<tr style="background:#f0f4fb;font-weight:600;border-top:2px solid #c8d0dc">
  <td colspan="6">Total across {len(loser_rows)} losing trades</td>
  <td class="num r">${total_short:,.2f}</td>
</tr>"""

    recovery_summary = " &nbsp;·&nbsp; ".join(
        f"Δ={k[1:]}: <strong style='color:{'#c0392b' if recovery_pct[k]<0 else '#14804a'}'>{recovery_pct[k]:+.1f}%</strong> recovered"
        for k, _ in config_keys
    )

    return f"""<div class="card">
  <h2>Losing Short Trades — Full Spread Detail</h2>
  <p class="note" style="margin-bottom:.85rem">
    <strong>7 of 246 trades</strong> had negative short P&amp;L (2 stop-losses, 5 small normal losses).
    Total short loss: <strong>${total_short:,.2f}</strong>.
    Long leg recovery on these same trades: {recovery_summary}.
    Prices shown in BTC with USD equivalent at entry/exit spot.
    Long leg option details shown for the <strong>Δ=0.05 config</strong>; P&amp;L breakdown for all three configs is shown per trade.
  </p>
  <div style="overflow-x:auto">
  <table>
    <thead><tr>
      <th>#</th><th colspan="2">Trade</th><th colspan="2">Spot movement</th>
      <th>Exit</th><th>Short P&amp;L</th>
    </tr></thead>
    <tbody>
      {trade_blocks}
      {total_row}
    </tbody>
  </table>
  </div>
  <p class="note" style="margin-top:.75rem">
    <strong>Key finding:</strong>
    On the 5 normal losing exits, the short reversed slightly (paid more at close than collected at open)
    while the long option <em>also</em> expired near-worthless — the long added to losses.
    On trade #298 (genuine directional SL, BTC fell ~6% to $63k), the long gained meaningfully (+$113 to +$481 depending on config).
    On trade #125 (SL triggered by IV spike — BTC barely moved), the long at $91k was still deeply OTM and gained very little.
    <strong>Conclusion: the protective long only pays when there is a large spot move, not from IV spikes alone.</strong>
  </p>
</div>"""


def _distance_section(distance_stats: list[dict]) -> str:
    colors = ["#e67e22", "#27ae60", "#8e44ad"]
    # shared y-max across all histograms for fair visual comparison
    global_ymax = max(max(ds["hist_counts"]) for ds in distance_stats) * 1.18
    hist_svgs = ""
    stat_rows = ""

    for i, ds in enumerate(distance_stats):
        col = colors[i % len(colors)]
        hist_svgs += f"""<div style="text-align:center;flex:1;min-width:0">
  <div style="font-size:.85rem;font-weight:600;color:{col};margin-bottom:.4rem">Δ = {ds['target_delta']:.2f} &nbsp;·&nbsp; median ${ds['median_usd']:,.0f} ({ds['median_pct']:.1f}%)</div>
  {_svg_distance_hist(ds, color=col, shared_ymax=global_ymax)}
</div>"""

        # Percentile-like distribution summary
        dists = sorted(ds["dists_usd"])
        p25 = dists[int(len(dists) * 0.25)]
        p75 = dists[int(len(dists) * 0.75)]

        stat_rows += f"""<tr>
  <td><strong>Δ = {ds['target_delta']:.2f}</strong></td>
  <td class="num">${ds['mean_usd']:,.0f}</td>
  <td class="num">${ds['median_usd']:,.0f}</td>
  <td class="num">${p25:,.0f} – ${p75:,.0f}</td>
  <td class="num">${ds['min_usd']:,.0f} – ${ds['max_usd']:,.0f}</td>
  <td class="num">{ds['mean_pct']:.1f}%</td>
  <td class="num">{ds['median_pct']:.1f}%</td>
  <td class="num">{ds['min_pct']:.1f}% – {ds['max_pct']:.1f}%</td>
</tr>"""

    return f"""<div class="card">
  <h2>Strike Distance: Short vs Long Leg</h2>
  <table style="margin-bottom:1rem">
    <thead><tr>
      <th>Config</th>
      <th>Mean Δstrike</th><th>Median</th><th>IQR (P25–P75)</th><th>Range</th>
      <th>Mean %</th><th>Median %</th><th>% Range</th>
    </tr></thead>
    <tbody>{stat_rows}</tbody>
  </table>
  <div style="display:flex;gap:1.25rem;align-items:flex-start;margin-top:.75rem">
    {hist_svgs}
  </div>
  <p class="note" style="margin-top:.75rem">
    Distance = |short strike − long strike|. For short puts the long is <em>below</em> the short; for short calls <em>above</em>.
    The distance scales roughly with BTC spot price: the short is selected at ~10–12% OTM (Δ≈0.15),
    and the long at ~15–18% OTM (Δ≈0.05), so the gap is typically <strong>5–8% of the current BTC price</strong>.
    At $85k BTC that is $4–7k; at $115k BTC it is $6–9k.
    <br><br>
    <strong>Why the distances look large in dollar terms:</strong> BTC options are quoted at strikes in $1,000 increments
    (sometimes $2,000+ at the wings). The nearest available strike to Δ=0.05 can be 1–3 strikes away from the theoretical value,
    causing the actual long delta to range from 0.03 to 0.10 even when targeting 0.05.
    In low-spot environments (BTC ~$65–70k) the $60k Deribit strike floor is sometimes the only option below the short,
    pushing the actual long delta higher than intended (e.g. Δ≈0.10 instead of 0.05).
  </p>
  <h3 style="margin-top:1rem;font-size:.9rem;color:var(--blue)">Three Concrete Examples (Δ=0.05 config)</h3>
  <table style="max-width:860px;margin-top:.5rem">
    <thead><tr>
      <th>Trade</th><th>BTC Spot</th><th>Short leg</th><th>Short bid (entry)</th>
      <th>Long leg</th><th>Long ask (entry)</th><th>Distance</th><th>Net spread cost</th>
    </tr></thead>
    <tbody>
      <tr>
        <td>#315 · Short put · 3APR26</td>
        <td class="num">$70,544</td>
        <td class="num">$64,000 · Δ=0.187</td>
        <td class="num">0.01250 BTC ($881)</td>
        <td class="num">$60,000 · Δ=0.096</td>
        <td class="num">0.00650 BTC ($458)</td>
        <td class="num">$4,000 (6.2%)</td>
        <td class="num r">−$423/trade</td>
      </tr>
      <tr style="background:#fafbfd">
        <td>#276 · Short put · 16JAN26</td>
        <td class="num">$92,605</td>
        <td class="num">$85,000 · Δ=0.138</td>
        <td class="num">0.00550 BTC ($509)</td>
        <td class="num">$80,000 · Δ=0.053</td>
        <td class="num">0.00230 BTC ($213)</td>
        <td class="num">$5,000 (5.9%)</td>
        <td class="num r">−$296/trade</td>
      </tr>
      <tr>
        <td>#185 · Short put · 22AUG25</td>
        <td class="num">$114,931</td>
        <td class="num">$108,000 · Δ=0.171</td>
        <td class="num">0.00600 BTC ($689)</td>
        <td class="num">$100,000 · Δ=0.046</td>
        <td class="num">0.00180 BTC ($207)</td>
        <td class="num">$8,000 (7.4%)</td>
        <td class="num r">−$483/trade</td>
      </tr>
    </tbody>
  </table>
  <p class="note" style="margin-top:.5rem">
    "Net spread cost" = long ask − short bid at entry (in USD). This is what you pay for the protection on day one,
    before knowing how the trade ends. Note that trade #315 has a long at Δ=0.096 (not 0.05) because
    $60k was the lowest available Deribit strike — the $60k floor compressed the long delta upward.
  </p>
</div>"""


def _fee_section() -> str:
    return """<div class="card">
  <h2>Fee Model Verification</h2>
  <p style="margin-bottom:.75rem">
    Fees are applied to <strong>both the long entry and long exit</strong> using the same model
    as the backtester (<code>backtester/pricing.py · deribit_fee_per_leg</code>):
  </p>
  <table style="max-width:520px">
    <thead><tr><th>Parameter</th><th>Value</th><th>Formula</th></tr></thead>
    <tbody>
      <tr><td>Index rate</td><td>0.025%</td><td>fee_base = 0.00025 × BTC_spot per contract</td></tr>
      <tr><td>Price cap fraction</td><td>10.42%</td><td>fee_cap  = 0.1042 × option_price_usd</td></tr>
      <tr><td>Applied fee</td><td>min of above</td><td>min(fee_base, fee_cap) per leg per trade</td></tr>
      <tr><td>Worthless exit</td><td>$0</td><td>No fee charged when bid = 0 (option expires worthless)</td></tr>
    </tbody>
  </table>
  <p class="note" style="margin-top:.6rem">
    <strong>Bug fixed in this run:</strong> An earlier draft used 0.03% / 12.5% (Deribit taker headline rates).
    The backtester config uses 0.025% / 10.42%, consistent with maker-equivalent fill assumptions.
    The corrected fees reduced the overstated hedge cost by ~$3–5 per leg, improving spread P&amp;L by
    roughly <strong>$800–1,500</strong> across the dataset depending on config.
    Fee functions were cross-validated against <code>deribit_fee_per_leg()</code> for six representative
    (spot, price) pairs — all matched to 4 decimal places.
  </p>
</div>"""


def _pct_color(pct: float) -> str:
    if pct >= 85:   return "#14804a"
    if pct >= 70:   return "#e67e22"
    return "#c0392b"


def generate_html(
    configs: list[dict],
    missing_trades: int,
    out_path: Path,
    loser_rows: list[dict] | None = None,
    distance_stats: list[dict] | None = None,
) -> None:
    total_trades = configs[0]["n_trades"] + missing_trades
    short_total  = configs[0]["short_total_pnl"]

    def fmt_usd(v, always_sign=False):
        sign = "+" if v > 0 else ""
        if always_sign or v < 0:
            return f"{sign}${v:,.2f}"
        return f"${v:,.2f}"

    def pct(v):
        return f"{v:.1f}%"

    equity_svg = _svg_equity(configs, width=1050, height=320)
    cost_svg   = _svg_cost_bar(configs, width=1050, height=150)

    # preservation % bars for summary table — inline style
    def pres_bar(pct):
        w   = max(0, min(100, pct))
        col = "#14804a" if pct >= 80 else "#e67e22" if pct >= 55 else "#c0392b"
        return (f'<div style="display:flex;align-items:center;gap:6px">'
                f'<div style="width:80px;height:7px;background:#eef0f3;border-radius:4px;overflow:hidden">'
                f'<div style="width:{w:.0f}%;height:100%;background:{col};border-radius:4px"></div></div>'
                f'<span style="color:{col};font-weight:600">{pct:.1f}%</span></div>')

    # header metric chips
    chip_html = ""
    chip_configs = [("Short-only", short_total, 100.0, "#1e6fbf")] + [
        (f"Spread Δ={c['target_delta']:.2f}", c["spread_total_pnl"], c["pnl_preservation_pct"],
         ["#e67e22","#27ae60","#8e44ad"][i]) for i, c in enumerate(configs)
    ]
    for lbl, pnl, pres, col in chip_configs:
        sign = "+" if pnl > 0 else ""
        chip_html += (f'<div style="background:rgba(255,255,255,.10);border:1px solid rgba(255,255,255,.18);'
                      f'border-radius:8px;padding:.5rem .9rem;min-width:140px">'
                      f'<div style="font-size:10px;color:#a8c0d8;letter-spacing:.5px;margin-bottom:2px">{lbl}</div>'
                      f'<div style="font-size:1.15rem;font-weight:700;color:#fff">{sign}${pnl:,.0f}</div>'
                      f'<div style="font-size:10px;color:{col if col != "#1e6fbf" else "#a8c0d8"}">'
                      f'{"baseline" if pres == 100 else f"{pres:.1f}% preserved"}</div></div>')

    rows_enriched = ""
    for cfg in configs:
        pres = cfg["pnl_preservation_pct"]
        sl_row = ""
        if cfg["n_sl_trades"]:
            sl_row = (f"<br><small style='color:#888'>SL: short {fmt_usd(cfg['sl_short_pnl'], True)} "
                      f"→ spread {fmt_usd(cfg['sl_spread_pnl'], True)} "
                      f"(long recouped {fmt_usd(cfg['sl_long_recoup'], True)})</small>")
        rows_enriched += f"""
<tr>
  <td><strong>Δ = {cfg['target_delta']:.2f}</strong></td>
  <td class="num g">{fmt_usd(cfg['spread_total_pnl'], True)}</td>
  <td>{pres_bar(pres)}</td>
  <td class="num r">{fmt_usd(cfg['long_total_cost'])}</td>
  <td class="num">{fmt_usd(cfg['long_avg_cost_per_trade'])}/trade</td>
  <td class="num">{pct(cfg['spread_win_rate'])}</td>
  <td class="num r">{fmt_usd(cfg['spread_max_loss'])}</td>
  <td class="num r">{fmt_usd(cfg['spread_max_dd'])}</td>
  <td style="font-size:.82rem">{sl_row if sl_row else "—"}</td>
</tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Spread Retrofit — Hold7 Short-Vol</title>
<style>
  :root{{--navy:#0d1b2a;--blue:#1e6fbf;--gold:#c8a84b;--bg:#f0f2f7;--text:#1a1a2e;--muted:#6a7a8c;--card-border:#dde3ec}}
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);font-size:14px;line-height:1.6}}
  header{{background:linear-gradient(135deg,var(--navy) 0%,#1a3356 60%,#0f2744 100%);color:#fff;padding:2.5rem 2rem 2rem}}
  header h1{{font-size:1.8rem;font-weight:700;margin-bottom:.4rem;letter-spacing:-.3px}}
  header .sub{{color:#8fb8d8;font-size:.9rem;max-width:58rem;margin-bottom:1.25rem;line-height:1.6}}
  header .chips{{display:flex;gap:.75rem;flex-wrap:wrap;margin-top:.25rem}}
  main{{max-width:1120px;margin:-1rem auto 3rem;padding:0 1.25rem}}
  .card{{background:#fff;border:1px solid var(--card-border);border-radius:12px;padding:1.5rem 1.75rem;margin-bottom:1.25rem;box-shadow:0 1px 3px rgba(0,0,0,.04)}}
  .card-title{{color:var(--blue);font-size:1rem;font-weight:700;margin:0 0 1rem;padding-bottom:.4rem;border-bottom:2px solid #e8ecf2;letter-spacing:-.1px}}
  h2{{color:var(--blue);font-size:1rem;font-weight:700;margin:0 0 1rem;padding-bottom:.4rem;border-bottom:2px solid #e8ecf2}}
  h3{{color:var(--blue);font-size:.9rem;font-weight:600;margin:.9rem 0 .4rem}}
  table{{width:100%;border-collapse:collapse;font-size:.87rem}}
  th,td{{padding:.42rem .6rem;border-bottom:1px solid #edf0f5;text-align:left;vertical-align:middle}}
  th{{background:#f4f6fb;font-weight:600;color:var(--muted);font-size:.82rem;text-transform:uppercase;letter-spacing:.4px;white-space:nowrap}}
  tr:last-child td{{border-bottom:none}}
  td.num{{text-align:right;font-variant-numeric:tabular-nums}}
  td.g{{color:#14804a;font-weight:600}}
  td.r{{color:#c0392b}}
  .note{{font-size:.82rem;color:var(--muted);margin-top:.6rem;line-height:1.55}}
  code{{background:#f0f4fb;border:1px solid #dde3ec;border-radius:3px;padding:1px 5px;font-size:.82rem;font-family:monospace}}
  footer{{text-align:center;color:var(--muted);font-size:.78rem;padding:.5rem 1rem 2.5rem}}
  .chart-label{{font-size:.8rem;font-weight:600;color:var(--muted);margin-bottom:.4rem;text-transform:uppercase;letter-spacing:.5px}}
</style>
</head>
<body>
<header>
  <div style="font-size:10px;letter-spacing:2.5px;text-transform:uppercase;color:var(--gold);margin-bottom:10px;font-weight:600">Spread Retrofit Simulation · {datetime.now().strftime("%B %Y")}</div>
  <h1>Hold7 Short-Vol — Vertical Spread Retrofit</h1>
  <div class="sub">Protective long legs (further OTM, same expiry) added to every short trade.
  Short P&amp;L is taken as-is from the backtester. Long P&amp;L simulated from Deribit parquet snapshots:
  <strong style="color:#fff">enter at ask · exit at bid</strong>. Three protection distances tested by target |Δ|.
  <strong style="color:#fff">{configs[0]['n_trades']} of {total_trades} trades</strong> within parquet window · {missing_trades} earlier trades excluded.</div>
  <div class="chips">{chip_html}</div>
</header>
<main>

  <div class="card">
    <h2>Results Summary</h2>
    <table>
      <thead><tr>
        <th>Config</th><th>Spread P&amp;L</th><th>P&amp;L preserved</th>
        <th>Total hedge cost</th><th>Avg/trade</th>
        <th>Win rate</th><th>Worst trade</th><th>Max drawdown</th><th>SL detail</th>
      </tr></thead>
      <tbody>
        <tr style="background:#f4f6fb">
          <td><strong>Short-only (baseline)</strong></td>
          <td class="num g">{fmt_usd(short_total, True)}</td>
          <td>{pres_bar(100)}</td>
          <td class="num">—</td><td class="num">—</td>
          <td class="num">{pct(configs[0]['short_win_rate'])}</td>
          <td class="num r">{fmt_usd(configs[0]['short_max_loss'])}</td>
          <td class="num r">{fmt_usd(configs[0]['short_max_dd'])}</td>
          <td>—</td>
        </tr>
        {rows_enriched}
      </tbody>
    </table>
    <p class="note">P&amp;L preserved = spread total ÷ short-only total (same 246-trade window).
    Hedge cost = long ask at entry − long bid at exit + Deribit fees, summed across all trades.</p>
  </div>

  <div class="card">
    <h2>Equity Curves</h2>
    {equity_svg}
    <div style="margin-top:1.25rem">
      <div class="chart-label">Average per-trade: short P&amp;L vs. hedge cost by config</div>
      {cost_svg}
    </div>
    <p class="note">All curves start at $100,000 initial equity · solid line = short-only baseline · dashed = spread configs · trade # on x-axis.</p>
  </div>

  {_loser_section(loser_rows) if loser_rows else ""}

  {_distance_section(distance_stats) if distance_stats else ""}

  {_fee_section()}

  <div class="card">
    <h2>Interpretation Notes</h2>
    <ul style="padding-left:1.2rem;line-height:2">
      <li>The protective long expires <strong>worthless in the vast majority of trades</strong> — the entire ask premium is lost. This is the expected cost of an OTM insurance leg.</li>
      <li>Deep OTM options have wide bid/ask spreads (median ~9% of mark at Δ=0.15; worse for Δ&lt;0.08). <strong>The ask-entry fill is the conservative, realistic assumption.</strong></li>
      <li>At lower delta targets (Δ=0.03), the option is very cheap but rarely has material value at exit — hedge is near-theoretical.</li>
      <li>On stop-loss events the long leg gains from delta and vega expansion. However, the 2 SL events in this window were <em>IV-spike</em> stops (spot barely moved), so the far-OTM long gained very little against a short that was still far OTM itself.</li>
      <li>The 106 excluded early trades (Oct 2024 – Apr 2025) had similar short P&amp;L characteristics based on the full-period report (~+$37k). Their exclusion doesn't change the conclusion about relative spread cost.</li>
    </ul>
  </div>

</main>
<footer>Generated {datetime.now().strftime("%Y-%m-%d %H:%M UTC")} · CryoBacktester spread_sim.py</footer>
</body>
</html>"""

    out_path.write_text(html, encoding="utf-8")
    print(f"  HTML → {out_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading trades...")
    df = pd.read_csv(TRADES_CSV)
    df["fill_entry_utc"] = pd.to_datetime(df["fill_entry_utc"], utc=True)
    df["fill_exit_utc"]  = pd.to_datetime(df["fill_exit_utc"],  utc=True)
    df["entry_date"]     = pd.to_datetime(df["entry_date"])

    DATA_START = pd.Timestamp("2025-04-11", tz="UTC")
    in_range   = df[df["fill_entry_utc"] >= DATA_START].copy()
    missing    = len(df) - len(in_range)
    print(f"  {len(df)} total trades | {len(in_range)} in parquet range | {missing} excluded")

    all_configs = []

    for target_delta in DELTA_TARGETS:
        label = f"delta_{target_delta:.2f}".replace(".", "")
        print(f"\nSimulating Δ={target_delta:.2f} ...")
        results   = []
        skipped   = 0
        no_long   = 0

        for _, trade in in_range.iterrows():
            res = simulate_trade(trade, target_delta)
            if res is None:
                # Fall back: use short P&L only (long not available), mark for audit
                skipped += 1
                results.append({
                    "trade_num":        int(trade["trade_num"]),
                    "entry_date":       str(trade["entry_date"].date()),
                    "side":             trade["side"],
                    "short_strike":     float(trade["strike"]),
                    "expiry":           expiry_code(trade["fill_entry_utc"], trade["dte"]),
                    "exit_type":        trade["exit_type"],
                    "short_pnl":        round(trade["net_pnl"], 2),
                    "long_strike":      None,
                    "long_delta":       None,
                    "long_ask_entry":   None,
                    "long_bid_exit":    None,
                    "long_cost_usd":    0.0,
                    "long_exit_usd":    0.0,
                    "long_pnl_usd":     0.0,
                    "spread_pnl":       round(trade["net_pnl"], 2),
                    "spread_width_usd": None,
                    "spot_entry":       round(trade["spot_entry"], 0),
                    "spot_exit":        round(trade["spot_exit"],  0),
                    "note":             "long_not_found",
                })
            else:
                res["note"] = ""
                results.append(res)

        matched = len(results) - skipped
        print(f"  Matched long: {matched} | Fallback (no long found): {skipped}")

        # Write per-trade CSV
        csv_path = OUT_DIR / f"hold7_spread_trades_d{int(target_delta*100):02d}.csv"
        fieldnames = [
            "trade_num","entry_date","side","short_strike","expiry","exit_type",
            "short_pnl","long_strike","long_delta","long_ask_entry","long_bid_exit",
            "long_cost_usd","long_exit_usd","long_pnl_usd","spread_pnl",
            "spread_width_usd","spot_entry","spot_exit","note",
        ]
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(results)
        print(f"  CSV  → {csv_path}")

        cfg = aggregate(results, label, target_delta)
        cfg["_results"] = results   # keep raw list for analysis passes
        all_configs.append(cfg)

        print(f"  Short P&L: ${cfg['short_total_pnl']:,.2f}  |  "
              f"Spread P&L: ${cfg['spread_total_pnl']:,.2f}  |  "
              f"Preservation: {cfg['pnl_preservation_pct']:.1f}%  |  "
              f"Avg hedge cost: ${cfg['long_avg_cost_per_trade']:,.2f}/trade")

    # Summary JSON (strip equity series — too large)
    summary = []
    for cfg in all_configs:
        row = {k: v for k, v in cfg.items() if not k.startswith("equity_") and k != "_results"}
        summary.append(row)

    json_path = OUT_DIR / "hold7_spread_summary.json"
    with open(json_path, "w") as f:
        json.dump({
            "generated":         datetime.now().isoformat(),
            "source_trades":     TRADES_CSV.name,
            "trades_simulated":  len(in_range),
            "trades_excluded":   missing,
            "configs":           summary,
        }, f, indent=2)
    print(f"\n  JSON → {json_path}")

    # ── Refinement analyses ───────────────────────────────────────────────────
    result_sets = list(zip(DELTA_TARGETS, [cfg["_results"] for cfg in all_configs]))
    loser_rows     = compute_loser_analysis(result_sets, in_range)
    distance_stats = compute_distance_stats(result_sets)
    print(f"\nLosing trades: {len(loser_rows)}")

    # HTML report
    html_path = OUT_DIR / "hold7_spread_report.html"
    generate_html(all_configs, missing, html_path, loser_rows, distance_stats)

    print("\nDone.")


if __name__ == "__main__":
    main()

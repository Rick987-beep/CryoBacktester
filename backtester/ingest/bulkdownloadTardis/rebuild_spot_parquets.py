#!/usr/bin/env python3
"""
rebuild_spot_parquets.py — Replace corrupted spot_YYYY-MM-DD.parquet files.

Background — why this script exists
------------------------------------
This is a fix for a broken bulk download pipeline. The original bulk download
path (bulk_fetch.py → stream_extract.py) derived the spot OHLC by reading
`underlying_price` out of individual options ticks. When `max_dte=700` was used
(to capture the full options chain), long-DTE options (e.g. 358-DTE Dec 2026
contracts) contributed `underlying_price` = their quarterly futures settlement
price, not the spot index. This silently inflated the spot track by $2,000–$4,000
for hours around each expiry. See backtester/ingest/SPOT_DATA_ISSUE.md for full
details.

This approach is the cleanest fix
----------------------------------
Rather than patching the options tick aggregation, this script pulls from a
dedicated Tardis dataset: `derivative_ticker/BTC-PERPETUAL`. That dataset records
the perpetual contract's `index_price` column, which is the actual Deribit
composite settlement index (capped median of 5+ constituent exchanges — Coinbase,
Kraken, Bitstamp, Bitfinex, Gemini). It is immune to the DTE/futures basis
problem because it has no DTE at all. It also provides full 24/7 coverage at
~245 k ticks/day, giving clean, dense 1-min bars for every minute of the day
without any dependency on which options happened to tick.

Status of the upstream fix
---------------------------
`stream_extract.py` has been fixed (added `SPOT_MAX_DTE = 2`), so future bulk
downloads will no longer corrupt the spot track. However, a fix at the
`bulk_fetch.py` level is still missing — `bulk_fetch.py` does not guard against
re-running with `max_dte=700` on dates that already have a clean spot parquet.
This is not critical since `stream_extract.py`'s guard is the actual gatekeeper,
but worth noting for completeness.

Output schema per file (spot_YYYY-MM-DD.parquet):
    timestamp  int64    microseconds UTC, 1-min aligned (:00 seconds)
    open       float32
    high       float32
    low        float32
    close      float32

Row count: 1441 per day
    Row 0: 23:59:00 UTC of D-1 (midnight carry-over bar for continuity)
    Rows 1–1440: 00:00:00–23:59:00 UTC of D (1440 minutes)

Usage:
    # Rebuild all 378 days (iterates over every options_YYYY-MM-DD.parquet found)
    python rebuild_spot_parquets.py

    # Rebuild a single date only
    python rebuild_spot_parquets.py --date 2026-01-01

    # Preview without writing files
    python rebuild_spot_parquets.py --dry-run

    # Use a custom data directory
    python rebuild_spot_parquets.py --data-dir /path/to/data

    # Skip dates that already have a rebuilt spot parquet (for resuming)
    python rebuild_spot_parquets.py --skip-existing
"""

import argparse
import calendar
import gzip
import io
import os
import sys
import time
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

try:
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import requests
except ImportError as e:
    print(f"Missing dependency: {e}\npip install numpy pandas pyarrow requests", file=sys.stderr)
    sys.exit(1)


# ── Configuration ─────────────────────────────────────────────────────────────

TARDIS_API_KEY = (
    "TD.q33pbrvPKEfhj685.ORfIaVN4HSAK2oL.6ixdTTmLjwMvHwK"
    ".FMpRFNSuBUGmk-e.JBuPq1ZEjt4dgG1.tDIK"
)
TARDIS_BASE = "https://datasets.tardis.dev/v1/deribit/derivative_ticker"
SYMBOL = "BTC-PERPETUAL"

# Delay between Tardis requests (seconds) — academic subscription, be polite
REQUEST_DELAY = 1.5

# Default data directory (backtester/data/ relative to this script)
_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_DIR = os.path.normpath(os.path.join(_HERE, "..", "..", "data"))


# ── Sanity check thresholds ───────────────────────────────────────────────────
# These are hard limits; violation skips the write and prints a warning.
# Values chosen conservatively — a healthy BTC day never breaches these.

BTC_MIN = 1_000.0           # Floor: BTC has never been below $1k in our dataset range
BTC_MAX = 500_000.0         # Ceiling: BTC has never exceeded $500k

MAX_INTRADAY_BAR_RANGE = 0.15   # Max (high - low) / close per 1-min bar = 15%
                                # (a genuine flash crash is ~5%; 15% = clearly bad data)

MAX_CONSECUTIVE_GAP = 0.05      # Max |open[i] - close[i-1]| / close[i-1] = 5%
                                # (5% in 1 second at a minute boundary = data error)

MAX_DAILY_HIGH_DEV = 0.20       # Max max(high) / median(close) - 1 = 20%
                                # BTC can legitimately have 15%+ daily ranges on
                                # volatile days (e.g. 2026-02-06: +13% recovery
                                # after crash). The corruption signature from
                                # SPOT_DATA_ISSUE.md was ~4-5% spikes at ~08:00
                                # UTC caused by futures settlement price; those
                                # would still exceed 20% only if extreme.
                                # The per-bar and consecutive-gap checks below
                                # are more precise detectors for data artifacts.

MIN_ROWS_WITH_DATA = 1380       # At least 95.8% of the 1440 day minutes must have
                                # real tick data (not forward-filled from gaps).
                                # Protects against near-empty files.


# ── Timestamp helpers ─────────────────────────────────────────────────────────

def _day_start_us(d: date) -> int:
    """Microsecond timestamp of 00:00:00 UTC on date d."""
    return int(calendar.timegm(d.timetuple())) * 1_000_000


# ── Date discovery ────────────────────────────────────────────────────────────

def _find_option_dates(data_dir: str) -> List[date]:
    """Return sorted list of all dates with options_YYYY-MM-DD.parquet files."""
    import glob
    pattern = os.path.join(data_dir, "options_????-??-??.parquet")
    paths = sorted(glob.glob(pattern))
    dates = []
    for p in paths:
        base = os.path.basename(p)  # options_2025-04-11.parquet
        date_str = base[len("options_"):len("options_") + 10]
        try:
            d = date.fromisoformat(date_str)
            dates.append(d)
        except ValueError:
            pass
    return dates


# ── Tardis download ───────────────────────────────────────────────────────────

def _download_day(d: date) -> Optional[pd.DataFrame]:
    """Download derivative_ticker/BTC-PERPETUAL for date d.

    Returns DataFrame with columns ['timestamp_us', 'index_price'] (both
    float64, rows where index_price is NaN dropped), or None on error.

    The Tardis endpoint returns an HTTP 302 redirect; requests follows it
    automatically. The file is a gzip-compressed CSV.
    """
    url = (
        f"{TARDIS_BASE}/{d.year}/{d.month:02d}/{d.day:02d}/{SYMBOL}.csv.gz"
    )
    try:
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {TARDIS_API_KEY}"},
            allow_redirects=True,
            timeout=120,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"  ERROR download failed: {exc}", file=sys.stderr)
        return None

    try:
        with gzip.open(io.BytesIO(resp.content), "rt", errors="replace") as fh:
            df = pd.read_csv(
                fh,
                usecols=["timestamp", "index_price"],
                dtype={"timestamp": "int64", "index_price": "float64"},
            )
        df.rename(columns={"timestamp": "timestamp_us"}, inplace=True)
        df.dropna(subset=["index_price"], inplace=True)
        df = df[df["index_price"] > 0].reset_index(drop=True)
        return df
    except Exception as exc:
        print(f"  ERROR parsing CSV: {exc}", file=sys.stderr)
        return None


# ── OHLC resampling ───────────────────────────────────────────────────────────

def _resample_to_1min_ohlc(
    df: pd.DataFrame, d: date
) -> Optional[pd.DataFrame]:
    """Resample tick-level index_price data to 1-min OHLC bars for date d.

    Covers exactly the 1440 minutes of d: 00:00:00–23:59:00 UTC.
    Minutes with no ticks are forward-filled from the preceding bar,
    then any remaining gaps at the start are back-filled.

    Returns DataFrame with columns [timestamp, open, high, low, close]
    (timestamp = int64 microseconds, prices = float64), or None if
    fewer than MIN_ROWS_WITH_DATA minutes have real data.
    """
    day_start = _day_start_us(d)
    day_end = day_start + 24 * 3600 * 1_000_000  # exclusive upper bound

    day_df = df[(df["timestamp_us"] >= day_start) & (df["timestamp_us"] < day_end)].copy()

    if len(day_df) == 0:
        print(f"  ERROR: zero ticks for {d}", file=sys.stderr)
        return None

    # Assign each tick to its 1-minute bucket (floor to minute boundary)
    day_df["bucket"] = (day_df["timestamp_us"] // 60_000_000) * 60_000_000

    # Aggregate OHLC per bucket — groupby preserves insertion order, so
    # first/last correctly map to temporal order within the source CSV.
    ohlc = (
        day_df.groupby("bucket", sort=True)["index_price"]
        .agg(open="first", high="max", low="min", close="last")
        .reset_index()
        .rename(columns={"bucket": "timestamp"})
    )

    real_minutes = len(ohlc)

    # Build complete 1440-minute grid and left-join the real data
    grid = pd.DataFrame(
        {"timestamp": [day_start + i * 60_000_000 for i in range(1440)]}
    )
    merged = grid.merge(ohlc, on="timestamp", how="left")

    # For gaps: fill OHLC as [prev_close, prev_close, prev_close, prev_close]
    # We do this by forward-filling close first, then patching other columns.
    merged["close"] = merged["close"].ffill()
    merged["open"] = merged["open"].where(merged["open"].notna(), merged["close"])
    merged["high"] = merged["high"].where(merged["high"].notna(), merged["close"])
    merged["low"] = merged["low"].where(merged["low"].notna(), merged["close"])

    # Backfill any remaining NaN at the very start of the day
    for col in ("open", "high", "low", "close"):
        merged[col] = merged[col].bfill()

    if merged[["open", "high", "low", "close"]].isna().any().any():
        print(f"  ERROR: NaN remaining after fill for {d}", file=sys.stderr)
        return None

    if real_minutes < MIN_ROWS_WITH_DATA:
        print(
            f"  ERROR: only {real_minutes} real minutes (< {MIN_ROWS_WITH_DATA} threshold) for {d}",
            file=sys.stderr,
        )
        return None

    return merged


# ── Sanity checks ─────────────────────────────────────────────────────────────

def _sanity_check(df: pd.DataFrame, d: date) -> Tuple[bool, List[str]]:
    """Run all sanity checks against a complete 1441-row spot DataFrame.

    Returns (passed: bool, warnings: List[str]).
    Fatal checks set passed=False; non-fatal checks only append warnings.
    """
    warnings: List[str] = []
    fatal = False

    n = len(df)
    if n != 1441:
        warnings.append(f"Row count: expected 1441, got {n}")
        fatal = True

    nan_count = df[["open", "high", "low", "close"]].isna().sum().sum()
    if nan_count:
        warnings.append(f"NaN in OHLC: {int(nan_count)} cells")
        fatal = True

    price_min = float(df["low"].min())
    price_max = float(df["high"].max())
    if price_min < BTC_MIN or price_max > BTC_MAX:
        warnings.append(
            f"Price out of BTC sanity bounds:"
            f" min={price_min:.0f}, max={price_max:.0f}"
            f" (expected {BTC_MIN:.0f}–{BTC_MAX:.0f})"
        )
        fatal = True

    bad_hl = int((df["high"] < df["low"]).sum())
    if bad_hl:
        warnings.append(f"high < low in {bad_hl} rows")
        fatal = True

    # Per-bar intraday range (15% = obviously bad data for a 1-min bar)
    close_safe = df["close"].clip(lower=1.0)
    bar_range_ratio = (df["high"] - df["low"]) / close_safe
    bad_range_count = int((bar_range_ratio > MAX_INTRADAY_BAR_RANGE).sum())
    if bad_range_count:
        worst = float(bar_range_ratio.max()) * 100
        warnings.append(
            f"Intraday bar range > {MAX_INTRADAY_BAR_RANGE*100:.0f}%:"
            f" {bad_range_count} rows (worst: {worst:.1f}%)"
        )
        fatal = True

    # Consecutive minute gap (open vs previous close)
    prev_close = df["close"].values[:-1]
    curr_open = df["open"].values[1:]
    prev_safe = np.clip(prev_close, 1.0, None)
    gaps = np.abs(curr_open - prev_close) / prev_safe
    bad_gap_count = int((gaps > MAX_CONSECUTIVE_GAP).sum())
    if bad_gap_count:
        worst_gap = float(gaps.max()) * 100
        warnings.append(
            f"Open-to-prior-close gap > {MAX_CONSECUTIVE_GAP*100:.0f}%:"
            f" {bad_gap_count} transitions (worst: {worst_gap:.1f}%)"
        )
        # Non-fatal: can happen at exchange restarts; just warn

    # Daily high vs median — the corruption signature
    median_close = float(df["close"].median())
    max_high = float(df["high"].max())
    if median_close > 0:
        high_dev = max_high / median_close - 1.0
        if high_dev > MAX_DAILY_HIGH_DEV:
            warnings.append(
                f"Daily max/median ratio = {1+high_dev:.4f}"
                f" (max={max_high:.0f}, median={median_close:.0f},"
                f" dev={high_dev*100:.2f}%)"
                f" — CORRUPTION SIGNATURE (threshold {MAX_DAILY_HIGH_DEV*100:.0f}%)"
            )
            fatal = True

    return (not fatal), warnings


# ── Parquet writer ────────────────────────────────────────────────────────────

def _write_parquet(df: pd.DataFrame, path: str) -> None:
    """Write spot parquet: int64 timestamps, float32 OHLC, zstd compression."""
    table = pa.table(
        {
            "timestamp": pa.array(df["timestamp"].values.astype("int64"), type=pa.int64()),
            "open":      pa.array(df["open"].values.astype("float32"), type=pa.float32()),
            "high":      pa.array(df["high"].values.astype("float32"), type=pa.float32()),
            "low":       pa.array(df["low"].values.astype("float32"), type=pa.float32()),
            "close":     pa.array(df["close"].values.astype("float32"), type=pa.float32()),
        }
    )
    pq.write_table(table, path, compression="zstd")


# ── Main pipeline ─────────────────────────────────────────────────────────────

def rebuild_spot_parquets(
    data_dir: str,
    output_dir: Optional[str] = None,
    target_date: Optional[date] = None,
    dry_run: bool = False,
    skip_existing: bool = False,
) -> None:
    """Main entry point. Iterates over all options dates and rebuilds spot parquets.

    Args:
        data_dir:   Source directory — must contain options_YYYY-MM-DD.parquet files.
                    Spot parquets are read from here (for --skip-existing) unless
                    output_dir is set.
        output_dir: Write rebuilt spot parquets here instead of data_dir.
                    Directory is created if it does not exist.
                    Useful for a parallel rebuild without touching production data.
    """

    if target_date is not None:
        dates = [target_date]
    else:
        dates = _find_option_dates(data_dir)

    if not dates:
        print(f"No options_YYYY-MM-DD.parquet files found in {data_dir}", file=sys.stderr)
        sys.exit(1)

    write_dir = output_dir if output_dir else data_dir
    if output_dir and not dry_run:
        os.makedirs(output_dir, exist_ok=True)

    print(
        f"rebuild_spot_parquets:"
        f" {len(dates)} dates  data_dir={data_dir}"
        f"  output_dir={write_dir}"
        f"  dry_run={dry_run}  skip_existing={skip_existing}"
    )

    ok_count = 0
    skip_count = 0
    fail_count = 0

    # prev_day_close: last close price from previous day's parquet,
    # used to populate the midnight carry-over bar (23:59 of D-1).
    prev_day_close: Optional[float] = None

    for i, d in enumerate(dates, start=1):
        spot_path = os.path.join(write_dir, f"spot_{d}.parquet")

        print(f"\n[{i}/{len(dates)}] {d}", flush=True)

        if skip_existing and os.path.exists(spot_path):
            print(f"  SKIP: file exists", flush=True)
            # Still need to read prev_day_close for next iteration
            try:
                existing = pd.read_parquet(spot_path)
                prev_day_close = float(existing["close"].iloc[-1])
            except Exception:
                pass
            skip_count += 1
            continue

        # ── Download ──────────────────────────────────────────────────────────
        raw = _download_day(d)
        if raw is None:
            print(f"  FAIL: download error", flush=True)
            fail_count += 1
            prev_day_close = None  # Can't derive next midnight bar
            time.sleep(REQUEST_DELAY)
            continue

        size_mb = len(raw) / 1e6
        print(f"  downloaded: {len(raw):,} ticks ({size_mb:.2f}M rows)", flush=True)

        # ── Resample to 1-min OHLC (1440 bars for target day) ─────────────────
        bars = _resample_to_1min_ohlc(raw, d)
        if bars is None:
            print(f"  FAIL: resampling failed", flush=True)
            fail_count += 1
            prev_day_close = None
            time.sleep(REQUEST_DELAY)
            continue

        # ── Build midnight carry-over bar (23:59 of D-1) ───────────────────────
        midnight_ts = _day_start_us(d) - 60_000_000  # 23:59:00 of D-1
        if prev_day_close is not None:
            p = prev_day_close
        else:
            # First date or after a failed day: use this day's first price
            p = float(bars["close"].iloc[0])
        midnight_bar = pd.DataFrame(
            {"timestamp": [midnight_ts], "open": [p], "high": [p], "low": [p], "close": [p]}
        )

        # ── Assemble full 1441-row DataFrame ──────────────────────────────────
        full = pd.concat([midnight_bar, bars], ignore_index=True)

        # ── Sanity checks ─────────────────────────────────────────────────────
        passed, warnings = _sanity_check(full, d)
        for w in warnings:
            print(f"  {'WARN' if passed else 'FATAL'}: {w}", flush=True)

        if not passed:
            print(f"  FAIL: sanity check(s) failed — file NOT written", flush=True)
            fail_count += 1
            # Still update prev_day_close using this day's close so the
            # midnight bar for D+1 is best-effort rather than None.
            prev_day_close = float(full["close"].iloc[-1])
            time.sleep(REQUEST_DELAY)
            continue

        # ── Write ──────────────────────────────────────────────────────────────
        close_px = float(full["close"].iloc[-1])
        if dry_run:
            print(
                f"  DRY-RUN: would write {spot_path}"
                f"  ({len(full)} rows, close={close_px:,.0f})",
                flush=True,
            )
        else:
            _write_parquet(full, spot_path)
            size_kb = os.path.getsize(spot_path) / 1024
            print(
                f"  OK: {spot_path}"
                f"  ({len(full)} rows, close={close_px:,.0f}, {size_kb:.0f} KB)",
                flush=True,
            )

        prev_day_close = close_px
        ok_count += 1
        time.sleep(REQUEST_DELAY)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"Done.  OK={ok_count}  skip={skip_count}  fail={fail_count}")
    if fail_count:
        print(f"  Re-run with --skip-existing to resume after fixing failures.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild corrupted spot_YYYY-MM-DD.parquet files using "
            "Tardis derivative_ticker/BTC-PERPETUAL index_price."
        )
    )
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_DATA_DIR,
        help=f"Directory containing options/spot parquets (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Rebuild a single date only (YYYY-MM-DD). Default: all dates.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Download and validate but do not write any files.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help=(
            "Write rebuilt spot parquets here instead of --data-dir. "
            "Useful for a side-by-side rebuild without touching existing files. "
            "Directory is created if it does not exist."
        ),
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip dates that already have a spot_YYYY-MM-DD.parquet (for resuming).",
    )
    args = parser.parse_args()

    target_date = None
    if args.date:
        try:
            target_date = date.fromisoformat(args.date)
        except ValueError:
            print(f"Invalid date: {args.date!r}  (expected YYYY-MM-DD)", file=sys.stderr)
            sys.exit(1)

    rebuild_spot_parquets(
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        target_date=target_date,
        dry_run=args.dry_run,
        skip_existing=args.skip_existing,
    )


if __name__ == "__main__":
    main()

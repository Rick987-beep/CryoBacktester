#!/usr/bin/env python3
"""
check_data_completeness.py — Read-only data completeness audit for backtester/data/.

Builds a single continuous timeline across ALL days in the range, then
reports every missing 5-min slot — including gaps that cross midnight.

Checks:
    1. Missing options or spot parquets for any day in the range
    2. Low row count (< MIN_ROWS rows) per day
    3. Every gap in the continuous options timestamp series:
         - any gap > 5 min (one missing slot) is listed
         - gaps > GAP_WARN_MINUTES are flagged prominently

Cross-day gaps (e.g. data ends at 23:30 on day N and resumes at 16:55 on
day N+1) are caught automatically because the timeline is global.

Usage:
    .venv/bin/python backtester/ingest/check_data_completeness.py
    .venv/bin/python backtester/ingest/check_data_completeness.py --from 2025-10-01
    .venv/bin/python backtester/ingest/check_data_completeness.py --large   # only gaps > 2h
"""
import argparse
import os
import sys
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

try:
    import numpy as np
    import pyarrow.parquet as pq
except ImportError:
    print("pip install numpy pyarrow", file=sys.stderr)
    sys.exit(1)

# ── Configuration ─────────────────────────────────────────────────────────────

DATA_DIR         = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DEFAULT_FROM     = "2025-04-11"   # first Tardis day available
DEFAULT_TO       = "2026-05-02"   # last recorder day available
INTERVAL_MIN     = 5              # expected tick spacing in minutes
GAP_WARN_MIN     = 120            # gaps > this are flagged as large  (⚠)
MIN_ROWS         = 10_000         # fewer rows than this → low-data flag


# ── Helpers ───────────────────────────────────────────────────────────────────

def date_range(from_str, to_str):
    # type: (str, str) -> List[str]
    start = datetime.strptime(from_str, "%Y-%m-%d")
    end   = datetime.strptime(to_str,   "%Y-%m-%d")
    dates, cur = [], start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return dates


def find_options_file(data_dir, date_str):
    # type: (str, str) -> Optional[str]
    path = os.path.join(data_dir, f"options_{date_str}.parquet")
    return path if os.path.exists(path) else None


def find_spot_file(data_dir, date_str):
    # type: (str, str) -> Optional[str]
    for prefix in ("spot_track", "spot"):
        path = os.path.join(data_dir, f"{prefix}_{date_str}.parquet")
        if os.path.exists(path):
            return path
    return None


def ts_us_to_str(ts_us):
    # type: (int) -> str
    """Convert microsecond timestamp to 'YYYY-MM-DD HH:MM UTC' string."""
    return datetime.utcfromtimestamp(ts_us / 1_000_000).strftime("%Y-%m-%d %H:%M")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(from_date, to_date, data_dir, large_only):
    # type: (str, str, str, bool) -> int

    dates = date_range(from_date, to_date)
    interval_us = INTERVAL_MIN * 60 * 1_000_000   # 5 min in microseconds

    print(f"Checking {len(dates)} days  ({from_date} → {to_date})")
    print(f"Data dir: {data_dir}")
    print(f"Reporting: {'only gaps > ' + str(GAP_WARN_MIN) + 'min' if large_only else 'every gap > ' + str(INTERVAL_MIN) + 'min'}")
    print()

    # ── Pass 1: per-day file checks + row counts ──────────────────────────────
    n_missing_opts  = 0
    n_missing_spot  = 0
    n_low_rows      = 0
    per_day_rows    = {}   # date_str -> int

    for date_str in dates:
        opts_path = find_options_file(data_dir, date_str)
        spot_path = find_spot_file(data_dir, date_str)

        if opts_path is None:
            print(f"  ⚠ {date_str}: MISSING options parquet")
            n_missing_opts += 1
            continue

        if spot_path is None:
            print(f"  ⚠ {date_str}: MISSING spot parquet")
            n_missing_spot += 1

        try:
            table = pq.read_table(opts_path, columns=["timestamp"])
            n_rows = len(table)
        except Exception as exc:
            print(f"  ⚠ {date_str}: READ ERROR — {exc}")
            continue

        per_day_rows[date_str] = n_rows
        if n_rows < MIN_ROWS:
            print(f"  ⚠ {date_str}: LOW ROWS — {n_rows:,} (< {MIN_ROWS:,})")
            n_low_rows += 1

    print()

    # ── Pass 2: build global timeline across all present days ─────────────────
    print("Building global timeline (loading all timestamps)...")
    all_intervals = []   # list of int (5-min bucket in microseconds)

    for date_str in dates:
        opts_path = find_options_file(data_dir, date_str)
        if opts_path is None:
            continue
        try:
            table = pq.read_table(opts_path, columns=["timestamp"])
        except Exception:
            continue

        ts = np.array(table.column("timestamp").to_pylist(), dtype=np.int64)
        # Snap each timestamp down to its 5-min bucket
        buckets = (ts // interval_us) * interval_us
        buckets = np.unique(buckets)
        all_intervals.append(buckets)

    if not all_intervals:
        print("No data loaded — nothing to check.")
        return 1

    all_intervals_arr = np.unique(np.concatenate(all_intervals))
    all_intervals_arr.sort()
    print(f"  {len(all_intervals_arr):,} unique 5-min intervals found across all days")
    print()

    # ── Pass 3: find every gap in the global sorted interval list ─────────────
    gaps = []   # list of (from_us, to_us, gap_min)
    for i in range(1, len(all_intervals_arr)):
        diff_us  = int(all_intervals_arr[i] - all_intervals_arr[i - 1])
        diff_min = diff_us // 60_000_000
        if diff_min > INTERVAL_MIN:
            gaps.append((int(all_intervals_arr[i - 1]), int(all_intervals_arr[i]), diff_min))

    # ── Report ────────────────────────────────────────────────────────────────
    n_large = sum(1 for _, _, m in gaps if m > GAP_WARN_MIN)
    n_small = len(gaps) - n_large

    print(f"  Gaps found: {len(gaps)} total  "
          f"({n_large} large > {GAP_WARN_MIN}min,  {n_small} small ≤ {GAP_WARN_MIN}min)")
    print()

    if gaps:
        # Sort by size descending for readability
        gaps_sorted = sorted(gaps, key=lambda x: x[2], reverse=True)

        if large_only:
            to_show = [(f, t, m) for f, t, m in gaps_sorted if m > GAP_WARN_MIN]
        else:
            to_show = gaps_sorted

        if to_show:
            col_w = 19
            print(f"  {'From':<{col_w}}  {'To':<{col_w}}  {'Gap':>8}  {'Flag'}")
            print(f"  {'-'*col_w}  {'-'*col_w}  {'-'*8}  {'-'*4}")
            for (from_us, to_us, gap_min) in to_show:
                h, m = divmod(gap_min, 60)
                dur  = f"{h}h {m:02d}m" if h else f"{m}min"
                flag = "⚠  LARGE" if gap_min > GAP_WARN_MIN else ""
                print(f"  {ts_us_to_str(from_us):<{col_w}}  "
                      f"{ts_us_to_str(to_us):<{col_w}}  "
                      f"{dur:>8}  {flag}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print(f"  Days checked:          {len(dates):>6}")
    print(f"  Missing options:       {n_missing_opts:>6}")
    print(f"  Missing spot:          {n_missing_spot:>6}")
    print(f"  Low-row days:          {n_low_rows:>6}  (< {MIN_ROWS:,} rows)")
    print(f"  Total gaps:            {len(gaps):>6}")
    print(f"    of which large:      {n_large:>6}  (> {GAP_WARN_MIN}min)")
    print(f"    of which small:      {n_small:>6}  (≤ {GAP_WARN_MIN}min)")
    print("=" * 60)

    has_issues = n_missing_opts or n_missing_spot or n_low_rows or n_large
    if not has_issues and not gaps:
        print("\n  All clean — no issues found.")
    elif not has_issues and gaps:
        print(f"\n  No large gaps or missing files — {len(gaps)} small maintenance gaps only.")

    return 1 if (n_missing_opts or n_missing_spot or n_large) else 0


def main():
    # type: () -> None
    parser = argparse.ArgumentParser(
        description="Read-only completeness audit of backtester/data/ parquets"
    )
    parser.add_argument("--from", dest="from_date", default=DEFAULT_FROM,
                        help=f"Start date YYYY-MM-DD (default: {DEFAULT_FROM})")
    parser.add_argument("--to",   dest="to_date",   default=DEFAULT_TO,
                        help=f"End date YYYY-MM-DD (default: {DEFAULT_TO})")
    parser.add_argument("--data-dir", default=DATA_DIR,
                        help=f"Data directory (default: {DATA_DIR})")
    parser.add_argument("--large", dest="large_only", action="store_true",
                        help=f"Only show gaps > {GAP_WARN_MIN}min (suppress small ones)")
    args = parser.parse_args()

    sys.exit(run(args.from_date, args.to_date, args.data_dir, args.large_only))


if __name__ == "__main__":
    main()

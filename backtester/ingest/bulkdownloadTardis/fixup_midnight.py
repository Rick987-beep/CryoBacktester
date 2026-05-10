#!/usr/bin/env python3
"""
fixup_midnight.py — Seed sparse 00:00 snapshots from the previous day's 23:55 state.

Background
----------
stream_extract.py processes each day in isolation. At the first 5-min boundary
(00:00 UTC), last_quote is nearly empty — only the handful of instruments that
happened to tick in the first few seconds of the file. The 00:05 snapshot is
already dense because enough ticks have trickled in by then.

This script runs a single FORWARD-ORDER pass over all finished options parquets
and for each day D:
    1. Reads the last-boundary rows (23:55) from options_{D-1}.parquet
    2. Identifies instruments absent from D's 00:00 boundary
    3. Appends those missing instruments to D's 00:00 with their D-1 state
    4. Rewrites options_{D}.parquet in-place

Result: every day's 00:00 snapshot is fully populated with the previous day's
closing state, matching what a live system would see at midnight.

Properties
----------
- Idempotent: running twice produces identical output (already-seeded instruments
  are already present at 00:00, so nothing is added the second time)
- Safe: never modifies any boundary other than 00:00; all other rows untouched
- Edge case: if D-1 parquet doesn't exist (first available day, or a gap), D's
  00:00 is left as-is — no error, just a note in the log
- Does NOT touch spot parquets

Usage
-----
    # Run after all workers finish
    python fixup_midnight.py --data-dir /bulk/data

    # Preview what would change without rewriting anything
    python fixup_midnight.py --data-dir /bulk/data --dry-run

    # Fix a single day only
    python fixup_midnight.py --data-dir /bulk/data --date 2026-03-10
"""

import argparse
import glob
import os
import sys
from datetime import date, timedelta
from typing import List, Optional, Tuple

try:
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("pip install numpy pandas pyarrow", file=sys.stderr)
    sys.exit(1)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
INTERVAL_US = 5 * 60 * 1_000_000   # 5 minutes in microseconds


# ── Helpers ───────────────────────────────────────────────────────────────────

def _day_start_us(d):
    # type: (date) -> int
    """Microsecond timestamp of 00:00:00 UTC on date d."""
    import calendar
    return int(calendar.timegm(d.timetuple())) * 1_000_000


def _opts_path(data_dir, date_str):
    # type: (str, str) -> str
    return os.path.join(data_dir, f"options_{date_str}.parquet")


def _find_all_dates(data_dir):
    # type: (str) -> List[str]
    """Return all options_YYYY-MM-DD.parquet date strings, sorted chronologically."""
    pattern = os.path.join(data_dir, "options_????-??-??.parquet")
    paths = glob.glob(pattern)
    dates = []
    for p in paths:
        name = os.path.basename(p)
        # options_YYYY-MM-DD.parquet
        date_str = name[len("options_"):-len(".parquet")]
        try:
            date.fromisoformat(date_str)
            dates.append(date_str)
        except ValueError:
            continue
    return sorted(dates)


# ── Core fixup for one day ────────────────────────────────────────────────────

def fixup_day(date_str, data_dir=DATA_DIR, dry_run=False):
    # type: (str, str, bool) -> Tuple[int, int]
    """Seed D's 00:00 snapshot from D-1's 23:55 state.

    Args:
        date_str:  Target day YYYY-MM-DD.
        data_dir:  Directory containing the parquets.
        dry_run:   If True, compute what would change but don't rewrite.

    Returns:
        (instruments_added, instruments_already_present)
        instruments_added == 0 means either D-1 was missing or already fully seeded.
    """
    d = date.fromisoformat(date_str)
    d_prev = d - timedelta(days=1)
    prev_str = d_prev.isoformat()

    target_path = _opts_path(data_dir, date_str)
    prev_path   = _opts_path(data_dir, prev_str)

    if not os.path.exists(target_path):
        print(f"[fixup] {date_str}  SKIP — target parquet not found", flush=True)
        return 0, 0

    if not os.path.exists(prev_path):
        print(f"[fixup] {date_str}  SKIP — no previous day ({prev_str}) parquet", flush=True)
        return 0, 0

    # ── Load target day ───────────────────────────────────────────────────────
    target_df = pq.read_table(target_path).to_pandas()

    day_start = _day_start_us(d)

    # Rows already at 00:00
    at_midnight = target_df[target_df["timestamp"] == day_start]
    midnight_keys = set(
        zip(at_midnight["expiry"], at_midnight["strike"], at_midnight["is_call"])
    )

    # ── Load previous day's last boundary ────────────────────────────────────
    prev_df = pq.read_table(prev_path).to_pandas()
    last_ts = int(prev_df["timestamp"].max())
    seed_rows = prev_df[prev_df["timestamp"] == last_ts].copy()

    if seed_rows.empty:
        print(f"[fixup] {date_str}  SKIP — previous day has no data", flush=True)
        return 0, 0

    # ── Find missing instruments ──────────────────────────────────────────────
    seed_keys = set(
        zip(seed_rows["expiry"], seed_rows["strike"], seed_rows["is_call"])
    )
    already_present = len(midnight_keys & seed_keys)
    missing_keys    = seed_keys - midnight_keys

    if not missing_keys:
        print(
            f"[fixup] {date_str}  already complete"
            f"  ({len(midnight_keys)} instruments at 00:00, none missing)",
            flush=True,
        )
        return 0, already_present

    # Build the rows to inject: same values as 23:55 of D-1, timestamp → D 00:00
    expiry_arr   = seed_rows["expiry"].values
    strike_arr   = seed_rows["strike"].values
    is_call_arr  = seed_rows["is_call"].values

    # Vectorised mask for missing instruments
    missing_mask = np.zeros(len(seed_rows), dtype=bool)
    for i, (exp, strike, ic) in enumerate(zip(expiry_arr, strike_arr, is_call_arr)):
        if (exp, float(strike), bool(ic)) in missing_keys:
            missing_mask[i] = True

    inject = seed_rows[missing_mask].copy()
    inject["timestamp"] = day_start

    n_added = len(inject)

    print(
        f"[fixup] {date_str}"
        f"  prev_last={pd.Timestamp(last_ts, unit='us', tz='UTC').strftime('%H:%M')}"
        f"  midnight_had={len(midnight_keys)}"
        f"  seeding={n_added}"
        f"  already_present={already_present}"
        + ("  (dry-run)" if dry_run else ""),
        flush=True,
    )

    if dry_run:
        return n_added, already_present

    # ── Merge and rewrite ─────────────────────────────────────────────────────
    merged = pd.concat([target_df, inject], ignore_index=True)
    merged.sort_values(
        ["timestamp", "expiry", "strike", "is_call"], inplace=True
    )
    merged.reset_index(drop=True, inplace=True)

    # Rewrite with same schema
    out_table = pa.table({
        "timestamp":        pa.array(merged["timestamp"].tolist(),         type=pa.int64()),
        "expiry":           pa.array(merged["expiry"].tolist(),            type=pa.dictionary(pa.int8(), pa.string())),
        "strike":           pa.array(merged["strike"].tolist(),            type=pa.float32()),
        "is_call":          pa.array(merged["is_call"].tolist(),           type=pa.bool_()),
        "underlying_price": pa.array(merged["underlying_price"].tolist(),  type=pa.float32()),
        "bid_price":        pa.array(merged["bid_price"].tolist(),         type=pa.float32()),
        "ask_price":        pa.array(merged["ask_price"].tolist(),         type=pa.float32()),
        "mark_price":       pa.array(merged["mark_price"].tolist(),        type=pa.float32()),
        "mark_iv":          pa.array(merged["mark_iv"].tolist(),           type=pa.float32()),
        "delta":            pa.array(merged["delta"].tolist(),             type=pa.float32()),
    })
    pq.write_table(out_table, target_path, compression="zstd")

    return n_added, already_present


# ── Batch runner ──────────────────────────────────────────────────────────────

def fixup_all(data_dir=DATA_DIR, dry_run=False, single_date=None):
    # type: (str, bool, Optional[str]) -> None
    """Run the midnight fixup forward pass over all (or one) parquet(s).

    Args:
        data_dir:    Directory containing options_*.parquet files.
        dry_run:     Report without rewriting.
        single_date: If set, fix only this one day (YYYY-MM-DD).
    """
    if single_date:
        dates = [single_date]
    else:
        dates = _find_all_dates(data_dir)

    if not dates:
        print(f"No options_*.parquet files found in {data_dir}", file=sys.stderr)
        sys.exit(1)

    print(
        f"\n{'='*60}\n"
        f"fixup_midnight  |  {len(dates)} days"
        + ("  [DRY RUN]" if dry_run else "")
        + f"\ndata_dir={data_dir}\n"
        f"{'='*60}\n",
        flush=True,
    )

    total_added = 0
    days_fixed  = 0
    days_skipped = 0

    for date_str in dates:
        added, _ = fixup_day(date_str, data_dir=data_dir, dry_run=dry_run)
        total_added += added
        if added > 0:
            days_fixed += 1
        else:
            days_skipped += 1

    print(
        f"\n{'='*60}\n"
        f"Done.  days_fixed={days_fixed}  days_skipped={days_skipped}"
        f"  total_rows_added={total_added:,}\n"
        + ("(dry-run — nothing written)\n" if dry_run else "")
        + f"{'='*60}\n",
        flush=True,
    )

    if dry_run and total_added > 0:
        sys.exit(1)   # signal to caller that there is work to do


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    # type: () -> None
    parser = argparse.ArgumentParser(
        description="Seed sparse 00:00 snapshots from previous day's 23:55 state"
    )
    parser.add_argument(
        "--data-dir", default=DATA_DIR,
        help="Directory containing options_*.parquet files",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report what would change without rewriting any files",
    )
    parser.add_argument(
        "--date",
        help="Fix a single day only (YYYY-MM-DD). Default: fix all days.",
    )
    args = parser.parse_args()

    fixup_all(
        data_dir=args.data_dir,
        dry_run=args.dry_run,
        single_date=args.date,
    )


if __name__ == "__main__":
    main()

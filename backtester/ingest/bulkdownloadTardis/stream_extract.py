#!/usr/bin/env python3
"""
stream_extract.py — Stream one day's OPTIONS.csv.gz directly to snapshot parquets.

Processes the raw Tardis file line by line — never loads it into memory.
Maintains a last-known-quote dict per instrument, flushed at each 5-min boundary.
Builds 1-min spot OHLC bars inline from the underlying_price column.

No intermediate tick parquet is written. Output is two compact files:
    data/options_YYYY-MM-DD.parquet   (~1–2 MB)   — 5-min option snapshots
    data/spot_YYYY-MM-DD.parquet      (~20 KB)    — 1-min spot OHLC bars

These are the files that market_replay.py consumes directly.

Usage:
    python stream_extract.py --date 2025-03-01
    python stream_extract.py --date 2025-03-01 --gz-path /tmp/OPTIONS.csv.gz
    python stream_extract.py --date 2025-03-01 --max-dte 28 --data-dir ./data

    # As a module (called from bulk_fetch.py):
    from stream_extract import stream_extract
    opts_path, spot_path = stream_extract("2025-03-01", data_dir="/bulk/data")
"""

import argparse
import gzip
import math
import os
import re
import sys
import time
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Tuple

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("pip install pyarrow", file=sys.stderr)
    sys.exit(1)

# ── Constants ────────────────────────────────────────────────────────────────

INTERVAL_MIN = 5
INTERVAL_US = INTERVAL_MIN * 60 * 1_000_000     # 5-min in microseconds
SPOT_INTERVAL_US = 60 * 1_000_000                # 1-min in microseconds
MAX_DTE_DEFAULT = 700

# Only use near-expiry options to source the 1-min spot OHLC.
# Long-DTE options (e.g. 358-DTE Dec contracts) have underlying_price equal
# to the quarterly futures settlement price, not the spot index — causing
# large corrupted spikes in the spot track.  DTE ≤ 2 ensures we only use
# same-day and next-day options whose underlying_price ≈ actual spot.
# See backtester/ingest/SPOT_DATA_ISSUE.md for root-cause details.
SPOT_MAX_DTE = 2

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

_MONTH = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_EXPIRY_RE = re.compile(r"^(\d{1,2})([A-Z]{3})(\d{2})$")
_NAN = float("nan")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _parse_expiry_date(expiry_str):
    # type: (str) -> Optional[date]
    """Parse Deribit expiry string to date. '9MAR26' -> date(2026, 3, 9)."""
    m = _EXPIRY_RE.match(expiry_str)
    if not m:
        return None
    month = _MONTH.get(m.group(2))
    if month is None:
        return None
    return date(2000 + int(m.group(3)), month, int(m.group(1)))


def _sf(val):
    # type: (str) -> float
    """Safe string-to-float. Returns nan for empty/missing fields."""
    return float(val) if val else _NAN


def _day_start_us(trade_date):
    # type: (date) -> int
    """Return microsecond timestamp of 00:00:00 UTC on trade_date."""
    return int(datetime(
        trade_date.year, trade_date.month, trade_date.day,
        tzinfo=timezone.utc,
    ).timestamp() * 1_000_000)


# ── Core streaming function ──────────────────────────────────────────────────

def stream_extract(
    date_str,           # type: str
    gz_path=None,       # type: Optional[str]
    max_dte=MAX_DTE_DEFAULT,  # type: int
    data_dir=DATA_DIR,  # type: str
):
    # type: (...) -> Tuple[str, str]
    """Stream OPTIONS.csv.gz for one day into two snapshot parquets.

    Reads the raw gzip CSV line-by-line, maintaining a last-known-quote dict
    per (expiry, strike, is_call) instrument. At each 5-min UTC boundary, the
    full instrument state is flushed to the snapshot accumulator. 1-min spot
    OHLC bars are built inline from the underlying_price column.

    The 00:00 snapshot will be sparse (few instruments ticked in the first
    seconds of the day). Use fixup_midnight.py afterwards to seed it from the
    previous day's 23:55 state.

    Args:
        date_str:  Trade date YYYY-MM-DD. Used for DTE calculation and output filenames.
        gz_path:   Path to OPTIONS.csv.gz. Defaults to data_dir/options_chain_{date_str}.csv.gz.
        max_dte:   Max calendar days-to-expiry to include (inclusive). Default 28.
        data_dir:  Directory for output parquets.

    Returns:
        (options_parquet_path, spot_parquet_path)

    Raises:
        FileNotFoundError: If gz_path does not exist.
        ValueError: If no matching rows found after scanning the whole file.
    """
    os.makedirs(data_dir, exist_ok=True)

    trade_date = datetime.strptime(date_str, "%Y-%m-%d").date()

    if gz_path is None:
        gz_path = os.path.join(data_dir, f"options_chain_{date_str}.csv.gz")
    if not os.path.exists(gz_path):
        raise FileNotFoundError(f"Raw file not found: {gz_path}")

    opts_out = os.path.join(data_dir, f"options_{date_str}.parquet")
    spot_out = os.path.join(data_dir, f"spot_{date_str}.parquet")

    day_start_us = _day_start_us(trade_date)
    last_boundary_us = day_start_us + 24 * 3600 * 1_000_000 - INTERVAL_US  # 23:55:00

    # ── Per-instrument state ─────────────────────────────────────────────────
    # last_quote[(expiry, strike, is_call)] = [underlying_px, bid, ask, mark, mark_iv, delta]
    last_quote = {}  # type: Dict[Tuple[str, float, bool], List[float]]

    # Snapshot accumulators — one list per column, appended at each flush
    snap_ts         = []  # type: List[int]
    snap_expiry     = []  # type: List[str]
    snap_strike     = []  # type: List[float]
    snap_is_call    = []  # type: List[bool]
    snap_underlying = []  # type: List[float]
    snap_bid        = []  # type: List[float]
    snap_ask        = []  # type: List[float]
    snap_mark       = []  # type: List[float]
    snap_iv         = []  # type: List[float]
    snap_delta      = []  # type: List[float]

    # 1-min spot OHLC: bucket_us -> [open, high, low, close]
    spot_bars = {}  # type: Dict[int, List[float]]

    # DTE cache: expiry_str -> int DTE, or None (unparseable), or -1 (out of range)
    dte_cache = {}  # type: Dict[str, Optional[int]]

    # Boundary pointer — starts at 00:00 of the trade date
    next_boundary_us = day_start_us

    def _flush(boundary_us):
        # type: (int) -> None
        """Append one snapshot row per tracked instrument at this boundary."""
        for (expiry, strike, is_call), vals in last_quote.items():
            snap_ts.append(boundary_us)
            snap_expiry.append(expiry)
            snap_strike.append(strike)
            snap_is_call.append(is_call)
            snap_underlying.append(vals[0])
            snap_bid.append(vals[1])
            snap_ask.append(vals[2])
            snap_mark.append(vals[3])
            snap_iv.append(vals[4])
            snap_delta.append(vals[5])

    # ── Stream the gzip CSV ──────────────────────────────────────────────────
    t0 = time.time()
    total = matched = 0
    gz_size = os.path.getsize(gz_path)
    print(
        f"[stream_extract] {date_str}  max_dte={max_dte}"
        f"  source: {gz_size / 1024**3:.2f} GB",
        flush=True,
    )

    with gzip.open(gz_path, "rt", errors="replace") as f:
        header = f.readline().strip().split(",")
        col = {name: i for i, name in enumerate(header)}

        # Resolve column indices once — avoids dict lookup per row
        i_sym   = col["symbol"]
        i_ts    = col["timestamp"]
        i_bid   = col["bid_price"]
        i_ask   = col["ask_price"]
        i_mark  = col["mark_price"]
        i_iv    = col["mark_iv"]
        i_spot  = col["underlying_price"]
        i_delta = col["delta"]

        for raw_line in f:
            total += 1
            fields = raw_line.split(",")
            sym = fields[i_sym]

            # ── Filter: BTC options only, symbol BTC-EXPIRY-STRIKE-C/P ──────
            if not sym.startswith("BTC-"):
                continue
            parts = sym.split("-")
            if len(parts) != 4:
                continue

            expiry_str = parts[1]

            # ── DTE filter ───────────────────────────────────────────────────
            if expiry_str not in dte_cache:
                exp_date = _parse_expiry_date(expiry_str)
                if exp_date is None:
                    dte_cache[expiry_str] = None
                else:
                    dte_cache[expiry_str] = (exp_date - trade_date).days
            dte = dte_cache[expiry_str]
            if dte is None or dte < 0 or dte > max_dte:
                continue

            matched += 1
            ts = int(fields[i_ts])
            strike = float(parts[2])
            is_call = parts[3].rstrip() == "C"

            # ── Advance boundaries ───────────────────────────────────────────
            # Emit snapshot for every 5-min mark that this tick has passed,
            # BEFORE updating last_quote with current tick's values.
            # Snapshot at boundary T captures the state of all ticks with ts < T.
            while ts >= next_boundary_us:
                _flush(next_boundary_us)
                next_boundary_us += INTERVAL_US

            # ── Update last-known quote for this instrument ──────────────────
            spot_val = _sf(fields[i_spot])
            last_quote[(expiry_str, strike, is_call)] = [
                spot_val,
                _sf(fields[i_bid]),
                _sf(fields[i_ask]),
                _sf(fields[i_mark]),
                _sf(fields[i_iv]),
                _sf(fields[i_delta]),
            ]

            # ── 1-min spot OHLC ──────────────────────────────────────────────
            # Gate spot OHLC on SPOT_MAX_DTE: only near-expiry options have
            # underlying_price ≈ actual spot index.  Long-DTE options use the
            # quarterly futures settlement price, which corrupts the spot track.
            if not math.isnan(spot_val) and dte is not None and dte <= SPOT_MAX_DTE:
                bucket = (ts // SPOT_INTERVAL_US) * SPOT_INTERVAL_US
                bar = spot_bars.get(bucket)
                if bar is None:
                    spot_bars[bucket] = [spot_val, spot_val, spot_val, spot_val]
                else:
                    if spot_val > bar[1]:
                        bar[1] = spot_val
                    if spot_val < bar[2]:
                        bar[2] = spot_val
                    bar[3] = spot_val

            if matched % 1_000_000 == 0:
                print(
                    f"  {matched:>9,} matched / {total:>12,} scanned"
                    f"  ({time.time() - t0:.0f}s)",
                    flush=True,
                )

    # ── EOD flush: emit any remaining boundaries up through 23:55 ────────────
    # If the file's last tick is before the final boundary (e.g. data ends at 23:54),
    # we still need to emit those trailing boundaries so the day has full coverage.
    while next_boundary_us <= last_boundary_us:
        _flush(next_boundary_us)
        next_boundary_us += INTERVAL_US

    elapsed = time.time() - t0
    n_rows = len(snap_ts)
    print(
        f"  Scan done: {matched:,} matched / {total:,} total"
        f"  →  {n_rows:,} snapshot rows  ({elapsed:.0f}s)",
        flush=True,
    )

    if n_rows == 0:
        raise ValueError(f"No BTC option rows with DTE ≤ {max_dte} found in {gz_path}")

    # ── Write options snapshot parquet ───────────────────────────────────────
    opts_table = pa.table({
        "timestamp":        pa.array(snap_ts,         type=pa.int64()),
        "expiry":           pa.array(snap_expiry,     type=pa.dictionary(pa.int8(), pa.string())),
        "strike":           pa.array(snap_strike,     type=pa.float32()),
        "is_call":          pa.array(snap_is_call,    type=pa.bool_()),
        "underlying_price": pa.array(snap_underlying, type=pa.float32()),
        "bid_price":        pa.array(snap_bid,        type=pa.float32()),
        "ask_price":        pa.array(snap_ask,        type=pa.float32()),
        "mark_price":       pa.array(snap_mark,       type=pa.float32()),
        "mark_iv":          pa.array(snap_iv,         type=pa.float32()),
        "delta":            pa.array(snap_delta,      type=pa.float32()),
    })
    pq.write_table(opts_table, opts_out, compression="zstd")
    opts_mb = os.path.getsize(opts_out) / 1024 ** 2
    print(
        f"  Options: {opts_out}"
        f"  ({opts_mb:.1f} MB, {n_rows:,} rows)",
        flush=True,
    )

    # ── Write spot OHLC parquet ───────────────────────────────────────────────
    if not spot_bars:
        raise ValueError(f"No spot price data found in {gz_path}")

    spot_sorted = sorted(spot_bars.items())
    spot_table = pa.table({
        "timestamp": pa.array([b       for b, _ in spot_sorted], type=pa.int64()),
        "open":      pa.array([v[0]    for _, v in spot_sorted], type=pa.float32()),
        "high":      pa.array([v[1]    for _, v in spot_sorted], type=pa.float32()),
        "low":       pa.array([v[2]    for _, v in spot_sorted], type=pa.float32()),
        "close":     pa.array([v[3]    for _, v in spot_sorted], type=pa.float32()),
    })
    pq.write_table(spot_table, spot_out, compression="zstd")
    spot_kb = os.path.getsize(spot_out) / 1024
    print(
        f"  Spot:    {spot_out}"
        f"  ({spot_kb:.0f} KB, {len(spot_sorted):,} 1-min bars)",
        flush=True,
    )

    return opts_out, spot_out


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    # type: () -> None
    parser = argparse.ArgumentParser(
        description="Stream one day's OPTIONS.csv.gz → snapshot parquets"
    )
    parser.add_argument("--date", required=True, help="Trade date YYYY-MM-DD")
    parser.add_argument(
        "--gz-path",
        default=None,
        help="Path to OPTIONS.csv.gz (default: DATA_DIR/options_chain_DATE.csv.gz)",
    )
    parser.add_argument(
        "--max-dte",
        type=int,
        default=MAX_DTE_DEFAULT,
        help=f"Max calendar DTE to include (default {MAX_DTE_DEFAULT})",
    )
    parser.add_argument(
        "--data-dir",
        default=DATA_DIR,
        help="Output directory (default: data/ next to this script)",
    )
    args = parser.parse_args()

    stream_extract(
        date_str=args.date,
        gz_path=args.gz_path,
        max_dte=args.max_dte,
        data_dir=args.data_dir,
    )
    print("Done.")


if __name__ == "__main__":
    main()

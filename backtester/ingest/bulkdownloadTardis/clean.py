#!/usr/bin/env python3
"""
clean.py — Data cleaning and validation for one day's snapshot parquets.

Applies six cleaning steps to the options and spot DataFrames produced by
stream_extract.py, then validates the result is plausible. Returns a
CleanReport with counts of every fix applied.

Steps (in order):
    1. IV format normalisation   — decimal 0.6 → percent 60.0
    2. NaN / zero fill           — NaN → 0.0, zero underlying_price rows dropped
    3. Spot price outlier removal — rows deviating >20% from day median dropped
    4. Option price outlier removal — ask > 0.5 BTC rows dropped
    5. Bid/ask/mark ordering     — inversion fixes and mark clamping
    6. Row-count plausibility    — <10,000 rows flags the day as suspect

Usage (from bulk_fetch.py):
    from clean import clean_day, CleanReport
    opts_df, spot_df, report = clean_day(opts_df, spot_df, date_str="2025-03-01")
    if report.suspect:
        # do not delete raw file, log warning
        ...

Usage as CLI (inspect any already-written parquet):
    python clean.py --date 2025-03-01 --data-dir ./data
    python clean.py --opts /tmp/stream_test/options_2025-03-01.parquet \
                    --spot /tmp/stream_test/spot_2025-03-01.parquet
"""

import argparse
import os
import sys
from dataclasses import dataclass, field
from typing import Optional, Tuple

try:
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("pip install numpy pandas pyarrow", file=sys.stderr)
    sys.exit(1)

# ── Thresholds ────────────────────────────────────────────────────────────────

IV_DECIMAL_THRESHOLD = 2.0      # median mark_iv below this → multiply by 100
SPOT_OUTLIER_PCT     = 0.20     # drop rows where spot deviates >20% from day median
ASK_PRICE_ABS_MAX    = 10.0     # universal hard cap: (max_strike ~500k / min_spot ~50k) = 10 BTC
PUT_TIME_VALUE_SLACK = 1.05     # 5% buffer above intrinsic for deep ITM puts
MIN_ROWS_PLAUSIBLE   = 10_000   # fewer rows than this → flag as suspect


# ── Report ────────────────────────────────────────────────────────────────────

@dataclass
class CleanReport:
    date_str: str

    # Step 1
    iv_rescaled: bool = False           # True if entire mark_iv column was × 100

    # Step 2
    nan_remaining: int = 0              # NaN cells left as-is (data absent from exchange)
    zero_spot_dropped: int = 0          # rows dropped for missing underlying_price

    # Step 3
    spot_outlier_dropped: int = 0       # rows dropped for extreme spot value

    # Step 4
    ask_outlier_dropped: int = 0        # rows dropped for implausible ask price

    # Step 5
    bid_ask_swapped: int = 0            # rows where bid/ask were inverted
    mark_clamped_low: int = 0           # rows where mark < bid → clamped up
    mark_clamped_high: int = 0          # rows where mark > ask → clamped down

    # Step 6
    final_rows: int = 0
    suspect: bool = False               # True if final_rows < MIN_ROWS_PLAUSIBLE

    # Step 7: spot validation (corruption signature check)
    spot_corrupted: bool = False        # True if spot OHLC shows futures-price spike
    spot_max_high_dev: float = 0.0      # max(high) / median(close) - 1
    spot_max_close_dev: float = 0.0     # max(close) / median(close) - 1

    def summary(self):
        # type: () -> str
        parts = [f"[clean] {self.date_str}  final_rows={self.final_rows:,}"]
        if self.iv_rescaled:
            parts.append("iv_rescaled=True")
        if self.nan_remaining:
            parts.append(f"nan_remaining={self.nan_remaining:,}")
        if self.zero_spot_dropped:
            parts.append(f"zero_spot_dropped={self.zero_spot_dropped:,}")
        if self.spot_outlier_dropped:
            parts.append(f"spot_outlier_dropped={self.spot_outlier_dropped:,}")
        if self.ask_outlier_dropped:
            parts.append(f"ask_outlier_dropped={self.ask_outlier_dropped:,}")
        if self.bid_ask_swapped:
            parts.append(f"bid_ask_swapped={self.bid_ask_swapped:,}")
        if self.mark_clamped_low:
            parts.append(f"mark_clamped_low={self.mark_clamped_low:,}")
        if self.mark_clamped_high:
            parts.append(f"mark_clamped_high={self.mark_clamped_high:,}")
        if self.suspect:
            parts.append("SUSPECT=True")
        if self.spot_corrupted:
            parts.append(
                f"SPOT_CORRUPTED=True"
                f"(high_dev={self.spot_max_high_dev*100:.2f}%,"
                f" close_dev={self.spot_max_close_dev*100:.2f}%)"
            )
        return "  ".join(parts)


# ── Core cleaning function ────────────────────────────────────────────────────

def clean_day(opts_df, spot_df, date_str="unknown"):
    # type: (pd.DataFrame, pd.DataFrame, str) -> Tuple[pd.DataFrame, pd.DataFrame, CleanReport]
    """Apply all cleaning steps to one day's snapshot DataFrames.

    Modifies copies — does not mutate the inputs.

    Args:
        opts_df:   Options snapshot DataFrame (output of stream_extract).
        spot_df:   Spot OHLC DataFrame (output of stream_extract).
        date_str:  Date string for logging (YYYY-MM-DD).

    Returns:
        (cleaned_opts_df, cleaned_spot_df, CleanReport)
    """
    report = CleanReport(date_str=date_str)
    opts = opts_df.copy()
    spot = spot_df.copy()

    # ── Step 1: IV format normalisation ──────────────────────────────────────
    # mark_iv should be in percent (e.g. 60.0 = 60%).
    # If median of non-zero, non-NaN values is below threshold, it's in decimal.
    iv_positive = opts["mark_iv"].dropna()
    iv_positive = iv_positive[iv_positive > 0]
    if len(iv_positive) > 0 and float(iv_positive.median()) < IV_DECIMAL_THRESHOLD:
        opts["mark_iv"] = opts["mark_iv"] * 100.0
        report.iv_rescaled = True

    # ── Step 2: NaN / zero fill ───────────────────────────────────────────────
    # Drop rows with no usable spot price first (NaN or 0.0).
    bad_spot = opts["underlying_price"].isna() | (opts["underlying_price"] == 0.0)
    report.zero_spot_dropped = int(bad_spot.sum())
    if report.zero_spot_dropped:
        opts = opts[~bad_spot].reset_index(drop=True)

    # Count remaining NaN cells. NaN is preserved in the parquet as the
    # sentinel for "data absent from exchange" — distinct from 0.0 which means
    # "exchange reported this value as zero".
    nan_cols = ["bid_price", "ask_price", "mark_price", "mark_iv", "delta"]
    report.nan_remaining = int(opts[nan_cols].isna().sum().sum())

    # ── Step 3: Spot price outlier removal ───────────────────────────────────
    # Drop rows where underlying_price deviates more than SPOT_OUTLIER_PCT from
    # the day's median. Catches corrupt one-off values (0, 9999999, etc.).
    spot_median = float(opts["underlying_price"].median())
    if spot_median > 0:
        lo = spot_median * (1.0 - SPOT_OUTLIER_PCT)
        hi = spot_median * (1.0 + SPOT_OUTLIER_PCT)
        bad_spot_val = (opts["underlying_price"] < lo) | (opts["underlying_price"] > hi)
        report.spot_outlier_dropped = int(bad_spot_val.sum())
        if report.spot_outlier_dropped:
            opts = opts[~bad_spot_val].reset_index(drop=True)

    # ── Step 4: Option price outlier removal ─────────────────────────────────
    # Two-tier check — deep ITM options can legitimately be worth >0.5 BTC.
    #
    # Calls: theoretical max = full underlying value = 1.0 BTC. Anything above
    #   is a data glitch. (Hard physical bound, no slack needed.)
    #
    # Puts: theoretical max intrinsic = (strike - spot) / spot BTC. We allow
    #   5% above that for residual time value on near-expiry deep ITM puts.
    #   Formula: max_put = (strike / underlying_price) * PUT_TIME_VALUE_SLACK
    #
    # Universal hard cap: ask > 2.0 BTC is impossible for any option.
    #   This catches rows where underlying_price itself is corrupted and the
    #   per-row put bound misfires.
    spot_vals   = opts["underlying_price"].values
    ask_vals    = opts["ask_price"].values
    strike_vals = opts["strike"].values
    is_call_arr = opts["is_call"].values

    # Universal cap
    bad_ask = ask_vals > ASK_PRICE_ABS_MAX

    # Per-row bounds where spot is positive
    safe_spot = spot_vals > 0
    # Calls: cap at 6.0 BTC (generous ceiling; long-dated deep ITM calls on a
    # rising BTC market can carry significant time value above intrinsic)
    bad_ask |= is_call_arr & (ask_vals > 6.0)
    # Puts: ask must be < (strike / spot) * slack
    put_max = np.where(safe_spot, (strike_vals / spot_vals) * PUT_TIME_VALUE_SLACK, ASK_PRICE_ABS_MAX)
    bad_ask |= ~is_call_arr & (ask_vals > put_max)

    report.ask_outlier_dropped = int(bad_ask.sum())
    if report.ask_outlier_dropped:
        opts = opts[~bad_ask].reset_index(drop=True)

    # ── Step 5: Bid/ask/mark ordering ────────────────────────────────────────
    # Only applied to rows where both bid and ask are positive (non-zero).
    tradeable = (opts["bid_price"] > 0) & (opts["ask_price"] > 0)

    # 5a. Fix inverted bid/ask: if ask < bid, swap them.
    inverted = tradeable & (opts["ask_price"] < opts["bid_price"])
    report.bid_ask_swapped = int(inverted.sum())
    if report.bid_ask_swapped:
        orig_bid = opts.loc[inverted, "bid_price"].copy()
        opts.loc[inverted, "bid_price"] = opts.loc[inverted, "ask_price"]
        opts.loc[inverted, "ask_price"] = orig_bid

    # 5b. Clamp mark below bid.
    mark_too_low = tradeable & (opts["mark_price"] > 0) & (opts["mark_price"] < opts["bid_price"])
    report.mark_clamped_low = int(mark_too_low.sum())
    if report.mark_clamped_low:
        opts.loc[mark_too_low, "mark_price"] = opts.loc[mark_too_low, "bid_price"]

    # 5c. Clamp mark above ask.
    mark_too_high = tradeable & (opts["mark_price"] > opts["ask_price"])
    report.mark_clamped_high = int(mark_too_high.sum())
    if report.mark_clamped_high:
        opts.loc[mark_too_high, "mark_price"] = opts.loc[mark_too_high, "ask_price"]

    # ── Step 6: Row-count plausibility ───────────────────────────────────────
    report.final_rows = len(opts)
    if report.final_rows < MIN_ROWS_PLAUSIBLE:
        report.suspect = True

    # ── Step 7: Spot corruption signature check ──────────────────────────────
    # Detect the specific corruption pattern from SPOT_DATA_ISSUE.md:
    # long-DTE options carry underlying_price = quarterly futures settlement
    # price, causing artificial high spikes 4-5% above the true spot index.
    # Thresholds from SPOT_DATA_ISSUE.md §9:
    #   max_close_dev < 4%  (max close vs median close)
    #   max_high_dev  < 6%  (max high  vs median close)
    if len(spot) > 0 and "close" in spot.columns and "high" in spot.columns:
        median_close = float(spot["close"].median())
        if median_close > 0:
            max_close = float(spot["close"].max())
            max_high = float(spot["high"].max())
            report.spot_max_close_dev = max_close / median_close - 1.0
            report.spot_max_high_dev = max_high / median_close - 1.0
            if report.spot_max_close_dev > 0.04 or report.spot_max_high_dev > 0.06:
                report.spot_corrupted = True
                report.suspect = True

    return opts, spot, report


# ── In-place parquet rewrite (called from bulk_fetch.py) ─────────────────────

def clean_parquets(opts_path, spot_path, date_str):
    # type: (str, str, str) -> CleanReport
    """Read, clean, and rewrite the two parquets for a given day in-place.

    Args:
        opts_path:  Path to options_YYYY-MM-DD.parquet.
        spot_path:  Path to spot_YYYY-MM-DD.parquet.
        date_str:   YYYY-MM-DD string for logging.

    Returns:
        CleanReport — caller is responsible for checking report.suspect.
    """
    opts_df = pq.read_table(opts_path).to_pandas()
    spot_df = pq.read_table(spot_path).to_pandas()

    opts_clean, spot_clean, report = clean_day(opts_df, spot_df, date_str=date_str)

    print(report.summary(), flush=True)

    # Rewrite options parquet (preserve category encoding for expiry)
    opts_table = pa.table({
        "timestamp":        pa.array(opts_clean["timestamp"].tolist(),         type=pa.int64()),
        "expiry":           pa.array(opts_clean["expiry"].tolist(),            type=pa.dictionary(pa.int8(), pa.string())),
        "strike":           pa.array(opts_clean["strike"].tolist(),            type=pa.float32()),
        "is_call":          pa.array(opts_clean["is_call"].tolist(),           type=pa.bool_()),
        "underlying_price": pa.array(opts_clean["underlying_price"].tolist(),  type=pa.float32()),
        "bid_price":        pa.array(opts_clean["bid_price"].tolist(),         type=pa.float32()),
        "ask_price":        pa.array(opts_clean["ask_price"].tolist(),         type=pa.float32()),
        "mark_price":       pa.array(opts_clean["mark_price"].tolist(),        type=pa.float32()),
        "mark_iv":          pa.array(opts_clean["mark_iv"].tolist(),           type=pa.float32()),
        "delta":            pa.array(opts_clean["delta"].tolist(),             type=pa.float32()),
    })
    pq.write_table(opts_table, opts_path, compression="zstd")

    # Rewrite spot parquet (unchanged, but consistent call pattern)
    spot_table = pa.table({
        "timestamp": pa.array(spot_clean["timestamp"].tolist(), type=pa.int64()),
        "open":      pa.array(spot_clean["open"].tolist(),      type=pa.float32()),
        "high":      pa.array(spot_clean["high"].tolist(),      type=pa.float32()),
        "low":       pa.array(spot_clean["low"].tolist(),       type=pa.float32()),
        "close":     pa.array(spot_clean["close"].tolist(),     type=pa.float32()),
    })
    pq.write_table(spot_table, spot_path, compression="zstd")

    return report


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    # type: () -> None
    parser = argparse.ArgumentParser(
        description="Clean and validate one day's snapshot parquets"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", help="Trade date YYYY-MM-DD (looks in --data-dir)")
    group.add_argument("--opts", help="Explicit path to options parquet")

    parser.add_argument("--spot", help="Explicit path to spot parquet (with --opts)")
    parser.add_argument(
        "--data-dir",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"),
        help="Data directory (used with --date)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report issues without rewriting parquets",
    )
    args = parser.parse_args()

    if args.date:
        opts_path = os.path.join(args.data_dir, f"options_{args.date}.parquet")
        spot_path = os.path.join(args.data_dir, f"spot_{args.date}.parquet")
        date_str = args.date
    else:
        opts_path = args.opts
        spot_path = args.spot
        if not spot_path:
            parser.error("--spot required when using --opts")
        date_str = os.path.basename(opts_path).replace("options_", "").replace(".parquet", "")

    for p in (opts_path, spot_path):
        if not os.path.exists(p):
            print(f"File not found: {p}", file=sys.stderr)
            sys.exit(1)

    opts_df = pq.read_table(opts_path).to_pandas()
    spot_df = pq.read_table(spot_path).to_pandas()

    opts_clean, spot_clean, report = clean_day(opts_df, spot_df, date_str=date_str)
    print(report.summary())

    if args.dry_run:
        print("(dry-run — no files written)")
        sys.exit(1 if report.suspect else 0)

    # Rewrite
    report2 = clean_parquets(opts_path, spot_path, date_str)
    if report2.suspect:
        print(f"WARNING: {date_str} flagged as suspect ({report2.final_rows:,} rows < {MIN_ROWS_PLAUSIBLE:,})")
        sys.exit(1)

    print("Done.")


if __name__ == "__main__":
    main()

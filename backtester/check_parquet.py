"""
Parquet data quality checker for backtester option snapshots.

Usage:
    python -m backtester.check_parquet [OPTIONS] [PARQUET_FILE]

    PARQUET_FILE  Path to options parquet (default: backtester/data/options_*.parquet,
                  uses the most recent match)

Options:
    --detail      Run deeper per-issue breakdowns (slower)

Checks performed:
    1. NaN / negative field counts
    2. mark=0 with non-zero bid or ask  (corruption indicator)
    3. Crossed book  (bid > ask)
    4. ask/mark and bid/mark ratio outliers  (thin-book / stale quotes)
    5. Fully frozen instruments  (price never changes)
    6. Cross-strike ask_usd jumps > 5× adjacent strike  (pricing discontinuities)
"""

import argparse
import glob
import os
from typing import Optional

import pandas as pd


# ── helpers ──────────────────────────────────────────────────────────────────

def _load(path: str) -> pd.DataFrame:
    print(f"Loading {path} ...")
    df = pd.read_parquet(path)
    print(f"  {len(df):,} rows, {df['expiry'].nunique()} expiries, "
          f"{df[['expiry','strike','is_call']].drop_duplicates().shape[0]} unique instruments\n")
    return df


def _section(title: str) -> None:
    print(f"{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ── checks ───────────────────────────────────────────────────────────────────

def check_nulls_and_negatives(df: pd.DataFrame) -> None:
    _section("1. NaN / negative fields")
    for col in ("bid_price", "ask_price", "mark_price", "underlying_price"):
        n_nan = df[col].isna().sum()
        n_neg = (df[col].fillna(0) < 0).sum()
        flag = "  ← WARNING" if (n_nan or n_neg) else ""
        print(f"  {col:<20} NaN={n_nan:>8,}   neg={n_neg:>8,}{flag}")
    # delta: negative is normal for puts — only flag NaN
    n_nan = df["delta"].isna().sum()
    flag = "  ← WARNING" if n_nan else ""
    print(f"  {'delta':<20} NaN={n_nan:>8,}   neg= (normal for puts){flag}")
    print()


def check_mark_zero(df: pd.DataFrame, detail: bool) -> None:
    _section("2. mark=0 with non-zero bid/ask  (corruption indicator)")
    mz_ask = df[(df["mark_price"] == 0) & (df["ask_price"].fillna(0) > 0)]
    mz_bid = df[(df["mark_price"] == 0) & (df["bid_price"].fillna(0) > 0)]
    print(f"  mark=0 && ask>0 : {len(mz_ask):>8,} rows  "
          f"({mz_ask[['expiry','strike','is_call']].drop_duplicates().shape[0]} unique instruments)")
    print(f"  mark=0 && bid>0 : {len(mz_bid):>8,} rows  "
          f"({mz_bid[['expiry','strike','is_call']].drop_duplicates().shape[0]} unique instruments)")

    if detail and len(mz_ask):
        mz_ask = mz_ask.copy()
        mz_ask["ask_usd"] = mz_ask["ask_price"] * mz_ask["underlying_price"]
        high = mz_ask[mz_ask["ask_usd"] > 1_000]
        low  = mz_ask[mz_ask["ask_usd"] <= 1_000]
        print(f"\n  ask_usd > $1,000 ({len(high)} rows, "
              f"{high[['expiry','strike','is_call']].drop_duplicates().shape[0]} instruments):")
        if len(high):
            uniq = high.drop_duplicates(["expiry","strike","is_call"])
            print(uniq[["expiry","strike","is_call","ask_price","mark_price",
                         "underlying_price","delta","ask_usd"]].to_string(index=False))
        print(f"\n  ask_usd <= $1,000 ({len(low)} rows) — ask_usd stats:")
        print(low["ask_usd"].describe().to_string())
        print("  Sample:")
        print(low.drop_duplicates(["expiry","strike","is_call"]).head(6)
              [["expiry","strike","is_call","ask_price","mark_price",
                "underlying_price","delta","ask_usd"]].to_string(index=False))
    print()


def check_crossed_book(df: pd.DataFrame) -> None:
    _section("3. Crossed book  (bid > ask)")
    both = df[
        df["bid_price"].notna() & df["ask_price"].notna() &
        (df["bid_price"] > 0) & (df["ask_price"] > 0)
    ]
    crossed = both[both["bid_price"] > both["ask_price"]]
    n_instr = crossed[["expiry","strike","is_call"]].drop_duplicates().shape[0]
    flag = "  ← WARNING" if len(crossed) else "  ✓ clean"
    print(f"  Crossed rows: {len(crossed):>8,}  ({n_instr} unique instruments){flag}")
    print()


def check_ratio_outliers(df: pd.DataFrame, detail: bool) -> None:
    _section("4. bid/ask vs mark ratio outliers")
    has_mark = df[df["mark_price"] > 0].copy()

    has_ask = has_mark[has_mark["ask_price"].fillna(0) > 0].copy()
    has_bid = has_mark[has_mark["bid_price"].fillna(0) > 0].copy()
    has_ask["ask_mark_ratio"] = has_ask["ask_price"] / has_ask["mark_price"]
    has_bid["bid_mark_ratio"] = has_bid["bid_price"] / has_bid["mark_price"]

    ask_high = has_ask[has_ask["ask_mark_ratio"] > 3]
    ask_low  = has_ask[has_ask["ask_mark_ratio"] < 0.3]
    bid_high = has_bid[has_bid["bid_mark_ratio"] > 3]
    bid_low  = has_bid[has_bid["bid_mark_ratio"] < 0.1]  # very wide spread

    for label, sub, thr in [
        ("ask > 3× mark ", ask_high, "← possible error"),
        ("ask < 0.3× mark", ask_low,  "← suspiciously cheap ask"),
        ("bid > 3× mark ", bid_high, "← possible error"),
        ("bid < 0.1× mark", bid_low,  "  (normal for deep OTM)"),
    ]:
        n_instr = sub[["expiry","strike","is_call"]].drop_duplicates().shape[0]
        print(f"  {label}: {len(sub):>8,} rows  ({n_instr} instruments)  {thr}")

    if detail:
        for label, sub in [("ask>3×mark", ask_high), ("ask<0.3×mark", ask_low),
                            ("bid>3×mark", bid_high)]:
            if len(sub):
                agg = sub.groupby(["expiry","strike","is_call"]).agg(
                    n_rows=("ask_price" if "ask" in label else "bid_price", "count"),
                    ratio_max=(
                        "ask_mark_ratio" if "ask" in label else "bid_mark_ratio", "max"
                    ),
                    delta=("delta", "first"),
                ).reset_index().sort_values("ratio_max", ascending=False)
                print(f"\n  {label} — instruments sorted by worst ratio:")
                print(agg.to_string(index=False))
    print()


def check_frozen(df: pd.DataFrame, detail: bool) -> None:
    _section("5. Fully frozen instruments  (mark+ask constant across all timestamps)")
    agg = df.groupby(["expiry","strike","is_call"], observed=True).agg(
        mark_nuniq=("mark_price", "nunique"),
        ask_nuniq =("ask_price",  "nunique"),
    ).reset_index()
    frozen = agg[(agg["mark_nuniq"] == 1) & (agg["ask_nuniq"] == 1)]
    n_total = len(agg)
    print(f"  Frozen: {len(frozen):>5} of {n_total} instruments  "
          f"({'%.1f' % (100*len(frozen)/n_total)}%)")
    if detail and len(frozen):
        sample = (
            df.merge(frozen[["expiry","strike","is_call"]], on=["expiry","strike","is_call"])
            .drop_duplicates(["expiry","strike","is_call"])
        )
        sample = sample.copy()
        sample["ask_usd"]  = sample["ask_price"]  * sample["underlying_price"]
        sample["mark_usd"] = sample["mark_price"] * sample["underlying_price"]
        print(sample[["expiry","strike","is_call","bid_price","ask_price",
                       "mark_price","delta","mark_usd","ask_usd"]]
              .sort_values(["expiry","strike"]).to_string(index=False))
    print()


def check_strike_jumps(df: pd.DataFrame, detail: bool) -> None:
    _section("6. Cross-strike ask_usd jumps > 5× adjacent strike")
    # Only consider rows where ask is clean (mark > 0 guards against mark=0 garbage)
    df_ask = df[(df["ask_price"].fillna(0) > 0) & (df["mark_price"] > 0)].copy()
    df_ask["ask_usd"] = df_ask["ask_price"] * df_ask["underlying_price"]

    jump_rows: list[dict] = []
    for (ts, exp, is_call), grp in df_ask.groupby(["timestamp", "expiry", "is_call"], observed=True):
        grp   = grp.sort_values("strike")
        asks  = grp["ask_usd"].values
        strs  = grp["strike"].values
        dlts  = grp["delta"].values
        marks = grp["mark_price"].values
        for i in range(len(asks)):
            nbrs = []
            if i > 0:         nbrs.append(asks[i - 1])
            if i < len(asks) - 1: nbrs.append(asks[i + 1])
            if not nbrs:
                continue
            nbr = sorted(nbrs)[len(nbrs) // 2]
            if nbr > 0 and asks[i] > nbr * 5:
                jump_rows.append({
                    "expiry": exp, "is_call": is_call, "strike": strs[i],
                    "delta": dlts[i], "ask_usd": asks[i],
                    "nbr_ask_usd": nbr, "ratio": asks[i] / nbr,
                    "mark_price": marks[i],
                })

    jdf = pd.DataFrame(jump_rows) if jump_rows else pd.DataFrame()
    print(f"  Total jump instances: {len(jdf)}")
    if len(jdf):
        n_instr = jdf[["expiry","strike","is_call"]].drop_duplicates().shape[0]
        print(f"  Unique instruments:   {n_instr}")
        if detail:
            worst = (jdf.sort_values("ratio", ascending=False)
                     .drop_duplicates(["expiry","strike","is_call"])
                     .head(20))
            print(worst[["expiry","strike","is_call","delta",
                          "ask_usd","nbr_ask_usd","ratio","mark_price"]]
                  .to_string(index=False))
    print()


# ── main ─────────────────────────────────────────────────────────────────────

def _find_default_parquet() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    pattern = os.path.join(here, "snapshots", "options_*.parquet")
    matches = sorted(glob.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No options parquet found at {pattern}")
    return matches[-1]  # most recent by filename sort


def main(argv: Optional[list] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run data quality checks on a backtester options parquet snapshot.",
    )
    parser.add_argument(
        "parquet",
        nargs="?",
        default=None,
        help="Path to options parquet file (default: most recent in backtester/data/)",
    )
    parser.add_argument(
        "--detail",
        action="store_true",
        help="Run deeper per-issue breakdowns (shows instrument lists, slower)",
    )
    args = parser.parse_args(argv)

    path = args.parquet or _find_default_parquet()
    df   = _load(path)

    check_nulls_and_negatives(df)
    check_mark_zero(df, detail=args.detail)
    check_crossed_book(df)
    check_ratio_outliers(df, detail=args.detail)
    check_frozen(df, detail=args.detail)
    check_strike_jumps(df, detail=args.detail)

    print("=== check complete ===")


if __name__ == "__main__":
    main()

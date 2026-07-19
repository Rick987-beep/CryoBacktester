"""
CLI: ingest Binance OHLC klines into indicators/data/ via hist_data.load_klines.

Example:
  python -m indicators.ingest_klines --symbol BTCUSDT --interval 1d --start 2020-01-01
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from indicators.hist_data import KLINE_DIR, load_klines


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Ingest Binance klines into local parquet cache")
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--interval", default="1d")
    p.add_argument(
        "--start",
        default="2020-01-01",
        help="UTC start date YYYY-MM-DD (default 2020-01-01)",
    )
    p.add_argument(
        "--end",
        default=None,
        help="UTC end date YYYY-MM-DD (default: now)",
    )
    p.add_argument(
        "--warmup-days",
        type=int,
        default=0,
        help="Extra history before --start (default 0; start already includes history)",
    )
    args = p.parse_args(argv)

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    if args.end:
        end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    else:
        end = datetime.now(tz=timezone.utc)

    df = load_klines(
        symbol=args.symbol,
        interval=args.interval,
        start=start,
        end=end,
        warmup_days=args.warmup_days,
    )
    path = KLINE_DIR / f"{args.symbol.upper()}_{args.interval}.parquet"
    print(f"symbol={args.symbol.upper()} interval={args.interval}")
    print(f"path={path}")
    print(f"rows={len(df)}")
    if not df.empty:
        print(f"range={df.index.min()} → {df.index.max()}")
    else:
        print("range=(empty)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

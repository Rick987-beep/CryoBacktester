#!/usr/bin/env python3
"""Copy macro calendar parquets from CryoQuant macro_store into backtester/data."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def sync(
    source: Path,
    dest: Path,
) -> int:
    if not source.is_dir():
        raise SystemExit(f"source not found: {source}")
    dest.mkdir(parents=True, exist_ok=True)
    count = 0
    for src in source.rglob("*.parquet"):
        rel = src.relative_to(source)
        out = dest / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, out)
        count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync CryoQuant macro calendar into backtester")
    parser.add_argument(
        "--source",
        default=str(
            Path.home() / "CryoQuant" / "data" / "macro" / "economic_events" / "us_scheduled"
        ),
    )
    parser.add_argument(
        "--dest",
        default=str(
            Path(__file__).resolve().parents[1]
            / "data"
            / "macro"
            / "economic_events"
            / "us_scheduled"
        ),
    )
    args = parser.parse_args()
    n = sync(Path(args.source), Path(args.dest))
    print(f"copied {n} parquet files to {args.dest}")


if __name__ == "__main__":
    main()

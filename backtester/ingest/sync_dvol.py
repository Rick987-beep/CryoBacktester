#!/usr/bin/env python3
"""Copy Deribit BTC_DVOL parquets from CryoQuant macro_store into the data plane."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from backtester.core.paths import dvol_dir, repo_root


def sync(source: Path, dest: Path) -> int:
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
    parser = argparse.ArgumentParser(
        description="Sync CryoQuant BTC_DVOL into CryoBacktester data/macro"
    )
    parser.add_argument(
        "--source",
        default=str(
            Path.home() / "CryoQuant" / "data" / "macro" / "deribit" / "BTC_DVOL"
        ),
    )
    parser.add_argument(
        "--dest",
        default=str(dvol_dir()),
    )
    args = parser.parse_args()
    n = sync(Path(args.source), Path(args.dest))
    print(f"copied {n} parquet files to {args.dest}")
    print(f"(repo root: {repo_root()})")


if __name__ == "__main__":
    main()

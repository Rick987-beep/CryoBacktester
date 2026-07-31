"""Live vs backtester comparison CLI."""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

from backtester.compare.models import RunSpec
from backtester.compare.pipeline import run_pipeline


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_ct_root() -> Path:
    env = os.environ.get("CT_ROOT")
    if env:
        return Path(env)
    candidate = _repo_root().parent / "CryoTrader"
    return candidate


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(
        prog="python -m backtester.compare",
        description="Compare CryoTrader live slot fills to backtester",
    )
    sub = ap.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Run full comparison pipeline")
    run_p.add_argument("--slot", required=True, help="Live slot number e.g. 02")
    run_p.add_argument("--last-n", type=int, default=7, help="Last N live fills")
    run_p.add_argument("--from", dest="date_from", default=None)
    run_p.add_argument("--to", dest="date_to", default=None)
    run_p.add_argument("--cryotrader-root", type=Path, default=None)
    run_p.add_argument("--out", type=Path, default=None)
    run_p.add_argument("--skip-pull", action="store_true", help="Reuse blotter in out dir or cache")
    run_p.add_argument("--skip-bt", action="store_true", help="Skip BT run (reuse bundle)")
    run_p.add_argument("--sizing", choices=["bt_default", "live_mirror"], default="bt_default")

    args = ap.parse_args(argv)

    if args.command == "run":
        repo = _repo_root()
        ct = args.cryotrader_root or _default_ct_root()
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        out = args.out or (
            repo / "analysis/livecompare/runs" / f"{ts}_slot{args.slot.zfill(2)}_last{args.last_n}"
        )
        out = out.resolve()
        spec = RunSpec(
            slot=args.slot,
            cryotrader_root=ct,
            repo_root=repo,
            out_dir=out,
            last_n=args.last_n,
            date_from=args.date_from,
            date_to=args.date_to,
            skip_pull=args.skip_pull,
            skip_bt=args.skip_bt,
            sizing_mode=args.sizing,
        )
        out_path = run_pipeline(spec)
        print(f"\nDone. Outputs in: {out_path}")
        print(f"  summary.md  report.html  manifest.json  data/comparison.csv")


if __name__ == "__main__":
    main()

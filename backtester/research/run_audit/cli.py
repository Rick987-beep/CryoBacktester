"""CLI: ``python -m backtester.research.run_audit RUN``."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from backtester.core.paths import repo_root
from backtester.inspect import AmbiguousMatch, NotFound, ResolveError
from backtester.inspect.format import emit, emit_error
from backtester.inspect.resolve import default_store, resolve_run
from backtester.research.run_audit.candidates import LivePickConfig
from backtester.research.run_audit.compute import audit_run
from backtester.research.run_audit.render import write_html


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m backtester.research.run_audit",
        description=(
            "Audit an existing backtester run: parameter influence, danger, "
            "curve-fit diagnostics, and diverse live candidates."
        ),
    )
    p.add_argument("run", help="run_id | bundle path | dirname | unique fragment")
    p.add_argument(
        "--state-dir",
        type=Path,
        default=None,
        help="UI state dir with ui_state.db (default: backtester/ui/state)",
    )
    p.add_argument(
        "--bundles-root",
        type=Path,
        default=None,
        help="Runs / bundles root (default: data/runs or CRYOBT_RUNS)",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Write audit.json (+ report.html) here (default: analysis/run_audit/<bundle_stem>)",
    )
    p.add_argument(
        "--html",
        action="store_true",
        help="Also write report.html via the section kit",
    )
    p.add_argument(
        "--stdout",
        action="store_true",
        help="Print full JSON pack to stdout (default: write files, print summary path)",
    )
    p.add_argument("--n-picks", type=int, default=3)
    p.add_argument("--min-n", type=int, default=40)
    p.add_argument("--min-n-loss", type=int, default=2)
    p.add_argument("--max-win-rate", type=float, default=0.97)
    p.add_argument("--max-dd-pct", type=float, default=12.0)
    p.add_argument("--min-sharpe", type=float, default=1.5)
    p.add_argument("--min-profit-factor", type=float, default=1.3)
    p.add_argument(
        "--allow-perfect-wr",
        action="store_true",
        help="Do not cap max win_rate (disables default 0.97 honest filter)",
    )
    p.add_argument(
        "--no-both-halves",
        action="store_true",
        help="Do not require both calendar halves profitable",
    )
    p.add_argument("--heat-row", default=None, help="Heatmap row param (default: top η²)")
    p.add_argument("--heat-col", default=None, help="Heatmap col param (default: 2nd η²)")
    return p


def default_out_dir(bundle_name: str) -> Path:
    stem = bundle_name.removesuffix(".bundle")
    return repo_root() / "analysis" / "run_audit" / stem


def run_audit_cli(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    store = default_store(args.state_dir, args.bundles_root)
    try:
        run = resolve_run(store, args.run)
    except AmbiguousMatch as e:
        emit_error(e)
        return 2
    except (NotFound, ResolveError) as e:
        emit_error(e)
        return 1

    max_wr = 1.0 if args.allow_perfect_wr else args.max_win_rate
    live_cfg = LivePickConfig(
        min_n=args.min_n,
        min_n_loss=args.min_n_loss,
        max_win_rate=max_wr,
        max_dd_pct=args.max_dd_pct,
        min_sharpe=args.min_sharpe,
        min_profit_factor=args.min_profit_factor,
        require_both_halves=not args.no_both_halves,
        n_picks=args.n_picks,
    )
    pack = audit_run(
        run,
        live_cfg=live_cfg,
        heat_row=args.heat_row,
        heat_col=args.heat_col,
    )

    out_dir = args.out_dir or default_out_dir(run.bundle_name)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "audit.json"
    json_path.write_text(json.dumps(pack, indent=2, default=str), encoding="utf-8")

    html_path = None
    if args.html:
        html_path = write_html(pack, out_dir / "report.html")

    if args.stdout:
        emit(pack, table=False)
    else:
        summary: dict[str, Any] = {
            "ok": True,
            "run_id": run.run_id,
            "bundle": run.bundle_name,
            "out_dir": str(out_dir),
            "audit_json": str(json_path),
            "report_html": str(html_path) if html_path else None,
            "curve_fit_level": (pack.get("curve_fit") or {}).get("verdict", {}).get("level"),
            "influence_top": (pack.get("influence_bar") or [{}])[0].get("param"),
            "danger_headline": (pack.get("danger_verdict") or {}).get("headline"),
            "n_live_picks": len((pack.get("live_candidates") or {}).get("picks") or []),
            "live_hashes": [
                p.get("combo_hash")
                for p in ((pack.get("live_candidates") or {}).get("picks") or [])
            ],
        }
        emit(summary, table=False)

    return 0


def main(argv: list[str] | None = None) -> int:
    return run_audit_cli(argv)


if __name__ == "__main__":
    sys.exit(main())

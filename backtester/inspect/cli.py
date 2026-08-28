"""CLI for ``python -m backtester.inspect``."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from backtester.inspect import AmbiguousMatch, NotFound, ResolveError
from backtester.inspect import load as L
from backtester.inspect.format import emit, emit_error
from backtester.inspect.resolve import (
    ResolvedCombo,
    default_store,
    favourite_for,
    filter_combos,
    find_hash_across_runs,
    parse_param_filters,
    resolve_combo,
    resolve_run,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m backtester.inspect",
        description="Locate backtester runs/combos and dump metrics / trade logs (fast path).",
    )
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
        "--table",
        action="store_true",
        help="Human table on stdout instead of JSON",
    )
    sub = p.add_subparsers(dest="command", required=True)

    runs = sub.add_parser("runs", help="List runs from the UI store")
    runs.add_argument("--strategy", default=None)
    runs.add_argument("--family", default=None)
    runs.add_argument("--since", default=None, help="created_at >= YYYY-MM-DD")
    runs.add_argument("--limit", type=int, default=50)

    show = sub.add_parser("show", help="Show one run (meta summary)")
    show.add_argument("run", help="run_id | bundle path | dirname | unique fragment")

    combos = sub.add_parser("combos", help="List / filter combos in a run")
    combos.add_argument("run")
    combos.add_argument("--hash", dest="combo_hash", default=None)
    combos.add_argument("--idx", dest="combo_idx", type=int, default=None)
    combos.add_argument("--param", action="append", default=None, help="k=v (repeatable)")
    combos.add_argument("--q", default=None, help="substring over params JSON / hash")
    combos.add_argument("--top", type=int, default=None, help="Top N by realized_pnl")
    combos.add_argument("--metrics", action="store_true", help="Include compact metrics")
    combos.add_argument("--full", action="store_true", help="Full metrics (with --metrics)")
    combos.add_argument("--limit", type=int, default=40)

    combo = sub.add_parser("combo", help="Params + metrics for one combo")
    combo.add_argument("run")
    combo.add_argument("combo", nargs="?", default=None, help="hash | #idx | bare idx")
    combo.add_argument("--param", action="append", default=None)
    combo.add_argument("--full", action="store_true")

    trades = sub.add_parser("trades", help="Dump / filter trade log for a combo")
    _add_combo_args(trades)
    _add_trade_filters(trades)
    trades.add_argument("--out", type=Path, default=None)
    trades.add_argument("--format", choices=["json", "csv", "parquet"], default="json")
    trades.add_argument("--limit", type=int, default=None)

    fills = sub.add_parser("fills", help="Dump / filter fills for a combo")
    _add_combo_args(fills)
    fills.add_argument("--since", default=None)
    fills.add_argument("--until", default=None)
    fills.add_argument("--exit-reason", default=None)
    fills.add_argument("--trade-idx", type=int, default=None)
    fills.add_argument("--pos-id", type=int, default=None)
    fills.add_argument("--event", choices=["open", "close"], default=None)
    fills.add_argument("--out", type=Path, default=None)
    fills.add_argument("--format", choices=["json", "csv", "parquet"], default="json")
    fills.add_argument("--limit", type=int, default=None)

    schema = sub.add_parser("schema", help="Print column schema")
    schema.add_argument("kind", choices=["trades", "fills", "nav", "greeks"])

    favs = sub.add_parser("favs", help="List starred combos")
    favs.add_argument("--strategy", default=None)
    favs.add_argument("--limit", type=int, default=100)

    find = sub.add_parser("find", help="Find combo_hash across runs")
    find.add_argument("combo_hash")
    find.add_argument("--limit", type=int, default=40)

    audit = sub.add_parser(
        "audit",
        help="Grid quality audit (influence / danger / curve-fit / live picks)",
    )
    audit.add_argument("run", help="run_id | bundle path | dirname | unique fragment")
    audit.add_argument("--out-dir", type=Path, default=None)
    audit.add_argument("--html", action="store_true")
    audit.add_argument("--stdout", action="store_true")
    audit.add_argument("--n-picks", type=int, default=3)
    audit.add_argument("--min-n", type=int, default=40)
    audit.add_argument("--min-n-loss", type=int, default=2)
    audit.add_argument("--max-win-rate", type=float, default=0.97)
    audit.add_argument("--max-dd-pct", type=float, default=12.0)
    audit.add_argument("--min-sharpe", type=float, default=1.5)
    audit.add_argument("--min-profit-factor", type=float, default=1.3)
    audit.add_argument("--allow-perfect-wr", action="store_true")
    audit.add_argument("--no-both-halves", action="store_true")
    audit.add_argument("--heat-row", default=None)
    audit.add_argument("--heat-col", default=None)

    return p


def _add_combo_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("run")
    p.add_argument("combo", nargs="?", default=None, help="hash | #idx | bare idx")
    p.add_argument("--param", action="append", default=None)


def _add_trade_filters(p: argparse.ArgumentParser) -> None:
    p.add_argument("--since", default=None)
    p.add_argument("--until", default=None)
    p.add_argument("--pnl-lt", type=float, default=None)
    p.add_argument("--pnl-gt", type=float, default=None)
    p.add_argument("--exit-reason", default=None)
    p.add_argument("--trade-idx", type=int, default=None)
    p.add_argument("--pos-id", type=int, default=None)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "audit":
        return _cmd_audit(args)

    table = bool(args.table)
    try:
        store = default_store(args.state_dir, args.bundles_root)
        payload = _dispatch(store, args)
        emit(payload, table=table)
        return 0
    except AmbiguousMatch as exc:
        emit_error(exc, table=table)
        return 2
    except NotFound as exc:
        emit_error(exc, table=table)
        return 1
    except ResolveError as exc:
        emit_error(exc, table=table)
        return 1
    except ValueError as exc:
        emit_error(exc, table=table)
        return 1
    except BrokenPipeError:
        return 0


def _cmd_audit(args) -> int:
    """Delegate to ``backtester.research.run_audit`` with matching flags."""
    from backtester.research.run_audit.cli import run_audit_cli

    forwarded: list[str] = [args.run]
    if args.state_dir is not None:
        forwarded.extend(["--state-dir", str(args.state_dir)])
    if args.bundles_root is not None:
        forwarded.extend(["--bundles-root", str(args.bundles_root)])
    if args.out_dir is not None:
        forwarded.extend(["--out-dir", str(args.out_dir)])
    if args.html:
        forwarded.append("--html")
    if args.stdout:
        forwarded.append("--stdout")
    forwarded.extend(["--n-picks", str(args.n_picks)])
    forwarded.extend(["--min-n", str(args.min_n)])
    forwarded.extend(["--min-n-loss", str(args.min_n_loss)])
    forwarded.extend(["--max-win-rate", str(args.max_win_rate)])
    forwarded.extend(["--max-dd-pct", str(args.max_dd_pct)])
    forwarded.extend(["--min-sharpe", str(args.min_sharpe)])
    forwarded.extend(["--min-profit-factor", str(args.min_profit_factor)])
    if args.allow_perfect_wr:
        forwarded.append("--allow-perfect-wr")
    if args.no_both_halves:
        forwarded.append("--no-both-halves")
    if args.heat_row:
        forwarded.extend(["--heat-row", args.heat_row])
    if args.heat_col:
        forwarded.extend(["--heat-col", args.heat_col])
    return run_audit_cli(forwarded)


def _dispatch(store, args) -> Any:
    cmd = args.command
    if cmd == "runs":
        return _cmd_runs(store, args)
    if cmd == "show":
        return _cmd_show(store, args)
    if cmd == "combos":
        return _cmd_combos(store, args)
    if cmd == "combo":
        return _cmd_combo(store, args)
    if cmd == "trades":
        return _cmd_trades(store, args)
    if cmd == "fills":
        return _cmd_fills(store, args)
    if cmd == "schema":
        return L.schema_info(args.kind)
    if cmd == "favs":
        return _cmd_favs(store, args)
    if cmd == "find":
        return {
            "combo_hash": args.combo_hash,
            "matches": find_hash_across_runs(store, args.combo_hash, limit=args.limit),
        }
    raise ValueError(f"unknown command {cmd}")


def _cmd_runs(store, args) -> dict:
    store.scan_bundles()
    rows = []
    for r in store.list_runs():
        if args.strategy and r.strategy != args.strategy:
            continue
        if args.family and (r.family or "") != args.family:
            continue
        if args.since and (r.created_at or "") < args.since:
            continue
        rows.append(
            {
                "run_id": r.id,
                "strategy": r.strategy,
                "family": r.family,
                "bundle": Path(r.bundle_path).name,
                "date_from": r.date_from,
                "date_to": r.date_to,
                "n_combos": r.n_combos,
                "n_trades": r.n_trades,
                "created_at": r.created_at,
                "label": r.label,
                "pinned": bool(r.pinned),
            }
        )
        if len(rows) >= args.limit:
            break
    return {"n": len(rows), "rows": rows}


def _cmd_show(store, args) -> dict:
    run = resolve_run(store, args.run)
    payload = run.summary()
    payload["sidecars"] = L.list_sidecar_files(run)
    payload["files"] = sorted(
        p.name for p in run.bundle_path.iterdir() if p.is_file()
    )
    return payload


def _resolve_combo_args(store, args):
    run = resolve_run(store, args.run)
    params = parse_param_filters(getattr(args, "param", None))
    combo_token = getattr(args, "combo", None)
    if params and not combo_token:
        combo = resolve_combo(run, params=params)
    elif combo_token:
        combo = resolve_combo(run, combo_token, params=params or None)
    else:
        raise NotFound("provide combo hash/#idx or --param k=v")
    return run, combo


def _cmd_combos(store, args) -> dict:
    run = resolve_run(store, args.run)
    params = parse_param_filters(args.param)
    if args.top:
        idxs = L.top_combo_idxs_by_pnl(run, args.top)
        combos: list[ResolvedCombo] = []
        for i in idxs:
            combos.extend(filter_combos(run, combo_idx=i))
    else:
        combos = filter_combos(
            run,
            combo_hash=args.combo_hash,
            combo_idx=args.combo_idx,
            params=params or None,
            q=args.q,
        )
        combos = combos[: args.limit]

    if args.metrics or args.full:
        rows = L.metrics_for_combos(run, combos, full=bool(args.full))
    else:
        rows = [c.summary() for c in combos]
    return {
        "run_id": run.run_id,
        "bundle": run.bundle_name,
        "n": len(rows),
        "rows": rows,
    }


def _cmd_combo(store, args) -> dict:
    run, combo = _resolve_combo_args(store, args)
    metrics_rows = L.metrics_for_combos(run, [combo], full=bool(args.full))
    row = metrics_rows[0] if metrics_rows else {**combo.summary(), "metrics": {}}
    fav = favourite_for(store, run, combo)
    if fav is not None:
        row["favourite"] = {
            "name": fav.name or None,
            "note": fav.note or None,
            "score": fav.score,
            "sharpe": fav.sharpe,
            "total_pnl": fav.total_pnl,
        }
    else:
        row["favourite"] = None
    row["run_id"] = run.run_id
    row["bundle"] = run.bundle_name
    row["strategy"] = run.strategy
    row["date_from"] = run.date_from
    row["date_to"] = run.date_to
    return row


def _cmd_trades(store, args) -> dict:
    run, combo = _resolve_combo_args(store, args)
    df = L.read_trades(run, [combo.combo_idx])
    df = L.filter_trades(
        df,
        since=args.since,
        until=args.until,
        pnl_lt=args.pnl_lt,
        pnl_gt=args.pnl_gt,
        exit_reason=args.exit_reason,
        trade_idx=args.trade_idx,
        pos_id=args.pos_id,
    )
    return _dump_frame(
        df,
        run=run,
        combo=combo,
        kind="trades",
        out=args.out,
        fmt=args.format,
        limit=args.limit,
    )


def _cmd_fills(store, args) -> dict:
    run, combo = _resolve_combo_args(store, args)
    df = L.read_fills(run, [combo.combo_idx])
    if df.empty:
        return {
            "run_id": run.run_id,
            "bundle": run.bundle_name,
            "combo_hash": combo.combo_hash,
            "combo_idx": combo.combo_idx,
            "kind": "fills",
            "n": 0,
            "schema": L.FILL_SCHEMA,
            "rows": [],
            "warning": "fills.parquet missing or empty",
        }
    df = L.filter_fills(
        df,
        since=args.since,
        until=args.until,
        exit_reason=args.exit_reason,
        trade_idx=args.trade_idx,
        pos_id=args.pos_id,
        event=args.event,
    )
    return _dump_frame(
        df,
        run=run,
        combo=combo,
        kind="fills",
        out=args.out,
        fmt=args.format,
        limit=args.limit,
    )


def _dump_frame(df, *, run, combo: ResolvedCombo, kind: str, out, fmt, limit) -> dict:
    schema = L.TRADE_SCHEMA if kind == "trades" else L.FILL_SCHEMA
    n = int(len(df))
    written = None
    if out is not None:
        L.write_frame(df, out, fmt)
        written = str(out)

    # Cap stdout rows when writing a file (agents still get a sample + schema).
    row_limit = limit
    if out is not None and row_limit is None:
        row_limit = 20
    rows = L.df_records(df, limit=row_limit)
    return {
        "run_id": run.run_id,
        "bundle": run.bundle_name,
        "combo_hash": combo.combo_hash,
        "combo_idx": combo.combo_idx,
        "kind": kind,
        "n": n,
        "schema": list(df.columns) if not df.empty else schema,
        "written": written,
        "rows": rows,
    }


def _cmd_favs(store, args) -> dict:
    rows = []
    for fav in store.list_favourites():
        if args.strategy and fav.strategy != args.strategy:
            continue
        run = store.get_run(fav.run_id)
        rows.append(
            {
                "run_id": fav.run_id,
                "bundle": Path(run.bundle_path).name if run else None,
                "strategy": fav.strategy,
                "combo_hash": fav.combo_hash,
                "name": fav.name,
                "note": fav.note,
                "score": fav.score,
                "sharpe": fav.sharpe,
                "total_pnl": fav.total_pnl,
                "params_str": fav.params_str,
            }
        )
        if len(rows) >= args.limit:
            break
    return {"n": len(rows), "rows": rows}


if __name__ == "__main__":
    sys.exit(main())

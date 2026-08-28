"""JSON / table output helpers for ``backtester.inspect``."""
from __future__ import annotations

import json
import sys
from typing import Any


def emit(payload: Any, *, table: bool = False, stream=None) -> None:
    stream = stream or sys.stdout
    if table:
        _emit_table(payload, stream)
    else:
        json.dump(payload, stream, indent=2, default=str)
        stream.write("\n")


def emit_error(exc: BaseException, *, table: bool = False) -> None:
    """Write a structured error to stderr (exit codes set by the CLI)."""
    payload: dict[str, Any] = {
        "error": type(exc).__name__,
        "message": str(exc),
    }
    candidates = getattr(exc, "candidates", None)
    if candidates is not None:
        payload["candidates"] = candidates
    emit(payload, table=table, stream=sys.stderr)


def _emit_table(payload: Any, stream) -> None:
    if isinstance(payload, dict) and "error" in payload:
        stream.write(f"ERROR: {payload.get('error')}: {payload.get('message', '')}\n")
        if "candidates" in payload:
            for c in payload["candidates"]:
                stream.write(f"  - {c}\n")
        return

    if isinstance(payload, list):
        if not payload:
            stream.write("(empty)\n")
            return
        if all(isinstance(x, dict) for x in payload):
            flat_rows = [_flatten(r) for r in payload]
            cols = _union_keys(flat_rows)
            _print_rows(flat_rows, cols, stream)
            return
        for x in payload:
            stream.write(f"{x}\n")
        return

    if isinstance(payload, dict):
        if "rows" in payload and isinstance(payload["rows"], list):
            meta = {k: v for k, v in payload.items() if k != "rows"}
            if meta:
                stream.write(json.dumps(meta, indent=2, default=str) + "\n")
            _emit_table(payload["rows"], stream)
            return
        flat = _flatten(payload)
        for k, v in flat.items():
            stream.write(f"{k}: {v}\n")
        return

    stream.write(f"{payload}\n")


def _flatten(d: dict, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in d.items():
        key = f"{prefix}{k}" if not prefix else f"{prefix}.{k}"
        if isinstance(v, dict) and k in ("metrics", "params", "favourite"):
            out.update(_flatten(v, key))
        elif isinstance(v, (list, dict)):
            out[key] = json.dumps(v, default=str)
        else:
            out[key] = v
    return out


def _union_keys(rows: list[dict]) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k)
                keys.append(k)
    return keys


def _print_rows(rows: list[dict], cols: list[str], stream) -> None:
    prefer = [
        "run_id",
        "bundle",
        "strategy",
        "combo_idx",
        "combo_hash",
        "n_combos",
        "date_from",
        "date_to",
        "metrics.total_pnl",
        "metrics.sharpe",
        "metrics.win_rate",
        "metrics.max_dd_pct",
        "favourite_name",
        "name",
    ]
    show = [c for c in prefer if c in cols]
    if not show:
        show = cols[:8]
    widths = {
        c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in show
    }
    stream.write("  ".join(c.ljust(widths[c]) for c in show) + "\n")
    stream.write("  ".join("-" * widths[c] for c in show) + "\n")
    for r in rows:
        stream.write(
            "  ".join(str(r.get(c, "")).ljust(widths[c]) for c in show) + "\n"
        )

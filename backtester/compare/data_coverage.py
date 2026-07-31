"""Check parquet data coverage for the comparison window."""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import List, Tuple

from backtester.compare.models import ParityWarning, RunSpec, WarningCode
from backtester.compare.io_utils import log_stage


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def check(spec: RunSpec, date_from: str, date_to: str) -> Tuple[dict, List[ParityWarning]]:
    data_dir = _repo_root() / "backtester/data"
    warnings: List[ParityWarning] = []
    d0 = date.fromisoformat(date_from)
    d1 = date.fromisoformat(date_to)
    days = []
    d = d0
    while d <= d1:
        opts = data_dir / f"options_{d.isoformat()}.parquet"
        spot = data_dir / f"spot_track_{d.isoformat()}.parquet"
        inc_opts = _repo_root() / f"analysis/deribit_btc_options_jul2026/data/options_{d.isoformat()}_INCOMPLETE.parquet"
        row = {
            "date": d.isoformat(),
            "options": opts.exists(),
            "spot": spot.exists(),
            "incomplete_options": inc_opts.exists(),
        }
        if not opts.exists() and inc_opts.exists():
            warnings.append(ParityWarning(
                code=WarningCode.DATA_INCOMPLETE,
                severity="warn",
                message=f"Options data for {d} is incomplete only",
                context={"path": str(inc_opts)},
            ))
        if not opts.exists() and not inc_opts.exists():
            warnings.append(ParityWarning(
                code=WarningCode.DATA_GAP,
                severity="error",
                message=f"Missing options parquet for {d}",
            ))
        days.append(row)
        d += timedelta(days=1)

    coverage = {"date_from": date_from, "date_to": date_to, "days": days}
    log_stage("data_coverage", n_days=len(days), gaps=sum(1 for w in warnings if w.code == WarningCode.DATA_GAP))
    return coverage, warnings

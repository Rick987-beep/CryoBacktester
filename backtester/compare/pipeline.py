"""Orchestrate live vs backtester comparison pipeline."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from backtester.compare.bt_trades import load_bundle
from backtester.compare.data_coverage import check as check_data_coverage
from backtester.compare.forensics import build_forensics, write_forensics_jsonl
from backtester.compare.io_utils import log_stage, write_json
from backtester.compare.live_trades import (
    load_blotter_rows,
    rows_to_dataframe,
    select_window,
)
from backtester.compare.match_trades import match
from backtester.compare.models import RunSpec
from backtester.compare.pull_blotter import pull_blotter
from backtester.compare.report import write_report_html, write_summary
from backtester.compare.resolve_config import resolve
from backtester.compare.run_bt import run as run_bt


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _window_dates(live_df: pd.DataFrame, pad_days: int = 2) -> tuple[str, str]:
    d0 = pd.to_datetime(live_df.entry_date.min())
    d1 = pd.to_datetime(live_df.entry_date.max()) + timedelta(days=pad_days)
    return d0.strftime("%Y-%m-%d"), d1.strftime("%Y-%m-%d")


def run_pipeline(spec: RunSpec) -> Path:
    spec.out_dir.mkdir(parents=True, exist_ok=True)
    data_dir = spec.out_dir / "data"
    data_dir.mkdir(exist_ok=True)

    # 1. Resolve config
    resolved, parity_warnings = resolve(spec)
    write_json(spec.out_dir / "resolved_config.json", resolved)
    write_json(
        spec.out_dir / "warnings.json",
        {"warnings": [w.to_dict() for w in parity_warnings]},
    )

    # 2. Pull or reuse blotter
    blotter = spec.out_dir / "data" / "blotter.jsonl"
    if spec.skip_pull and blotter.exists():
        log_stage("pull_blotter_skipped", path=str(blotter))
    elif spec.skip_pull:
        # fallback to cached copy from prior livevsbtfills pull
        cached = _repo_root() / "analysis/livevsbtfills/data/slot-02.jsonl"
        if cached.exists():
            blotter.write_text(cached.read_text())
            log_stage("pull_blotter_cached", source=str(cached))
        else:
            pull_blotter(spec)
            blotter = spec.out_dir / "data" / "blotter.jsonl"
    else:
        pull_blotter(spec)
        blotter = spec.out_dir / "data" / "blotter.jsonl"

    # 3. Live trades
    all_rows = load_blotter_rows(blotter)
    window_rows = select_window(all_rows, spec.last_n, spec.date_from, spec.date_to)
    flat_params = resolved.get("bt_params_flat", {})
    live_df = rows_to_dataframe(window_rows, flat_params)
    live_df.to_csv(data_dir / "live_trades.csv", index=False)

    date_from, date_to = _window_dates(live_df)
    if spec.date_from:
        date_from = spec.date_from
    if spec.date_to:
        date_to = spec.date_to

    # 4. Data coverage
    coverage, cov_warnings = check_data_coverage(spec, date_from, date_to)
    parity_warnings.extend(cov_warnings)
    write_json(spec.out_dir / "data_coverage.json", coverage)

    # 5. Run BT
    from backtester.core.paths import runs_dir
    bundles_root = runs_dir()
    if spec.skip_bt:
        existing_manifest = spec.out_dir / "manifest.json"
        bundle_rel = None
        if existing_manifest.exists():
            from backtester.compare.io_utils import read_json
            bundle_rel = read_json(existing_manifest).get("bundle")
        if not bundle_rel:
            raise ValueError("--skip-bt requires existing manifest.json with bundle in out dir")
        bundle = Path(bundle_rel)
        if not bundle.is_absolute():
            bundle = _repo_root() / bundle
    else:
        bundle = run_bt(
            bt_strategy=resolved["bt_strategy"],
            param_grid=resolved["param_grid"],
            date_from=date_from,
            date_to=date_to,
            account_size=spec.account_size_usd,
            bundles_root=bundles_root,
            label=f"slot{spec.slot_padded}",
        )

    bt_df = load_bundle(bundle, resolved["bt_strategy"])
    bt_df.to_csv(data_dir / "bt_trades.csv", index=False)

    # 6. Match + forensics
    comparison = match(live_df, bt_df)
    comparison.to_csv(data_dir / "comparison.csv", index=False)
    forensics = build_forensics(live_df)
    forensics.to_csv(data_dir / "forensics.csv", index=False)
    write_forensics_jsonl(forensics, data_dir / "forensics.jsonl")

    repo = _repo_root()
    try:
        bundle_rel = str(bundle.relative_to(repo))
    except ValueError:
        bundle_rel = str(bundle)
    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "slot": spec.slot_padded,
        "last_n": spec.last_n,
        "date_from": date_from,
        "date_to": date_to,
        "live_strategy": resolved["live_strategy"],
        "bt_strategy": resolved["bt_strategy"],
        "bundle": bundle_rel,
        "sizing_mode": spec.sizing_mode,
        "n_live": len(live_df),
        "n_bt": len(bt_df),
        "comparability_counts": comparison.comparability.value_counts().to_dict(),
        "warnings": [w.to_dict() for w in parity_warnings],
        "resolved_config_hash": resolved.get("slot_toml_hash"),
    }
    write_json(spec.out_dir / "manifest.json", manifest)

    write_summary(manifest, comparison, forensics, spec.out_dir / "summary.md")
    write_report_html(manifest, comparison, forensics, spec.out_dir / "report.html")

    # Update LATEST pointer
    latest = repo / "analysis/livecompare/LATEST"
    try:
        latest.write_text(str(spec.out_dir.relative_to(repo)) + "\n")
    except ValueError:
        latest.write_text(str(spec.out_dir) + "\n")

    log_stage("pipeline_done", out=str(spec.out_dir), n_live=len(live_df), n_bt=len(bt_df))
    return spec.out_dir

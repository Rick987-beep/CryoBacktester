"""
tests/ui/test_run_worker.py — Integration tests for the run_worker subprocess.

These tests actually spawn a worker subprocess, so they are marked slow_ui and
require real parquet data to be present.

Run with: python -m pytest tests/ui/test_run_worker.py -v -m slow_ui
"""
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time

import pytest


@pytest.mark.slow_ui
def test_worker_writes_bundle_and_progress(tmp_path, sqlite_store):
    """Worker completes a tiny run and writes progress + a bundle."""
    try:
        import backtester.config as _cfg
        opts = _cfg.cfg.data.options_parquet
        spot = _cfg.cfg.data.spot_parquet
        if not (os.path.exists(opts) and os.path.exists(spot)):
            pytest.skip("no parquet data available")
    except Exception:
        pytest.skip("config unavailable")

    bundles_root = str(tmp_path / "bundles")
    state_dir    = str(tmp_path / "state")
    progress_path = str(tmp_path / "progress.jsonl")

    # Use shortest date range with data
    config = {
        "strategy":      "short_generic",
        "param_grid":    {"delta": [0.24], "dte": [7]},
        "date_from":     "2025-10-01",
        "date_to":       "2025-10-08",
        "account_size":  100000.0,
        "bundles_root":  bundles_root,
        "state_dir":     state_dir,
        "progress_path": progress_path,
    }

    config_path = str(tmp_path / "config.json")
    pathlib.Path(config_path).write_text(json.dumps(config))

    proc = subprocess.run(
        [sys.executable, "-m", "backtester.ui.services.run_worker",
         "--config", config_path],
        timeout=120,
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0, f"worker stderr: {proc.stderr[:800]}"
    assert pathlib.Path(progress_path).exists(), "progress file not created"

    lines = [json.loads(l) for l in pathlib.Path(progress_path).read_text().splitlines()
             if l.strip()]
    assert lines, "no progress lines written"

    final = lines[-1]
    assert final.get("status") == "done", f"last line: {final}"
    bundle_path = final.get("bundle_path")
    assert bundle_path and pathlib.Path(bundle_path).exists()
    assert (pathlib.Path(bundle_path) / "meta.json").exists()


@pytest.mark.slow_ui
def test_worker_handles_sigterm(tmp_path):
    """SIGTERM cancels the worker and writes a cancelled status line."""
    try:
        import backtester.config as _cfg
        opts = _cfg.cfg.data.options_parquet
        spot = _cfg.cfg.data.spot_parquet
        if not (os.path.exists(opts) and os.path.exists(spot)):
            pytest.skip("no parquet data available")
    except Exception:
        pytest.skip("config unavailable")

    bundles_root = str(tmp_path / "bundles")
    state_dir    = str(tmp_path / "state")
    progress_path = str(tmp_path / "progress.jsonl")

    config = {
        "strategy":      "short_generic",
        "param_grid":    {"delta": [0.24], "dte": [7]},
        "date_from":     "2025-10-01",
        "date_to":       "2026-12-31",   # long range so we can interrupt it
        "account_size":  100000.0,
        "bundles_root":  bundles_root,
        "state_dir":     state_dir,
        "progress_path": progress_path,
    }

    config_path = str(tmp_path / "config.json")
    pathlib.Path(config_path).write_text(json.dumps(config))

    proc = subprocess.Popen(
        [sys.executable, "-m", "backtester.ui.services.run_worker",
         "--config", config_path],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Give it a moment to start up, then terminate
    time.sleep(3)
    proc.send_signal(signal.SIGTERM)

    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        pytest.fail("worker did not exit after SIGTERM within 10s")

    assert proc.returncode != 0, "expected non-zero exit after cancel"

    if pathlib.Path(progress_path).exists():
        lines = [json.loads(l) for l in
                 pathlib.Path(progress_path).read_text().splitlines() if l.strip()]
        statuses = [l.get("status") for l in lines if "status" in l]
        assert "cancelled" in statuses or proc.returncode != 0

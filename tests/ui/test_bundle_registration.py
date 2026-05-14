"""
tests/ui/test_bundle_registration.py — scan_bundles registers pre-existing bundles.
"""
import json
import pathlib
import pytest
from datetime import datetime, timezone


def _write_fake_bundle(bundles_root: pathlib.Path, strategy: str, ts: str):
    """Create a minimal .bundle dir with meta.json + required parquets."""
    import pandas as pd
    import numpy as np

    bundle_dir = bundles_root / f"{strategy}_{ts}.bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    meta = {
        "strategy":     strategy,
        "param_grid":   {"delta": [0.24]},
        "keys":         [[[["delta", 0.24]]]],
        "date_range":   ["2025-10-01", "2025-10-08"],
        "account_size": 10000.0,
        "runtime_s":    1.0,
        "source":       "test",
        "created_at":   f"{ts[:8]}T{ts[9:]}Z".replace("_", "T"),
        "n_combos":     1,
        "n_trades":     2,
        "git_sha":      "abc1234",
        "git_dirty":    False,
        "config_hash":  "xyz",
    }
    (bundle_dir / "meta.json").write_text(json.dumps(meta))

    # Minimal trade_log parquet
    df = pd.DataFrame([{
        "combo_idx": 0,
        "entry_time": "2025-10-01",
        "exit_time":  "2025-10-02",
        "pnl":        50.0,
        "entry_spot": 30000.0,
        "exit_spot":  30000.0,
        "entry_price_usd": 40.0,
        "exit_price_usd":  40.0,
        "fees": 1.0,
        "triggered": True,
        "exit_reason": "tp",
        "exit_hour": 6,
        "entry_date": "2025-10-01",
        "status": 0,
    }])
    df.to_parquet(bundle_dir / "trade_log.parquet", index=False)

    nav = pd.DataFrame([{
        "combo_idx": 0, "date": "2025-10-01",
        "nav_low": 9950.0, "nav_high": 10050.0, "nav_close": 10000.0,
    }])
    nav.to_parquet(bundle_dir / "nav_daily.parquet", index=False)

    final = pd.DataFrame([{
        "combo_idx": 0, "final_nav": 10050.0,
        "realized_pnl": 50.0, "open_pnl": 0.0,
    }])
    final.to_parquet(bundle_dir / "final_nav.parquet", index=False)

    return bundle_dir


def test_scan_bundles_registers_dropped_bundle(tmp_path):
    """scan_bundles finds and registers a pre-existing .bundle dir."""
    bundles_root = tmp_path / "reports"
    state_dir    = tmp_path / "ui_state"
    bundles_root.mkdir()
    state_dir.mkdir()

    # Write a bundle before StoreService is created
    bundle_dir = _write_fake_bundle(bundles_root, "short_generic", "20251001_090000")

    from backtester.ui.services.store_service import StoreService
    store = StoreService(str(state_dir), str(bundles_root))

    # scan_bundles should pick it up
    store.scan_bundles()

    rows = store.list_runs()
    assert len(rows) >= 1
    assert any(r.strategy == "short_generic" for r in rows)


def test_register_bundle_idempotent(tmp_path):
    """register_bundle called twice returns the same run_id without error."""
    bundles_root = tmp_path / "reports"
    state_dir    = tmp_path / "ui_state"
    bundles_root.mkdir()
    state_dir.mkdir()

    bundle_dir = _write_fake_bundle(bundles_root, "short_generic", "20251001_100000")

    from backtester.ui.services.store_service import StoreService
    store = StoreService(str(state_dir), str(bundles_root))

    run_id_1 = store.register_bundle(bundle_dir)
    run_id_2 = store.register_bundle(bundle_dir)

    assert run_id_1 == run_id_2

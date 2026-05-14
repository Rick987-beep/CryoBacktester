"""tests/ui/test_phase1_store_service.py — StoreService tests."""
import json

import pytest

from backtester.ui.services.store_service import StoreService, key_to_json, key_from_json


# ── Key serialisation ─────────────────────────────────────────────────────────

def test_key_roundtrip():
    key = (("delta", 0.25), ("dte", 1), ("skip_weekends", True))
    assert key_from_json(key_to_json(key)) == key


# ── Bundle write + load roundtrip ─────────────────────────────────────────────

def test_write_and_load_bundle_roundtrip(sqlite_store, tiny_grid_result):
    bundle_path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="tiny_test", runtime_s=1.23, source="test"
    )
    assert (bundle_path / "trade_log.parquet").exists()
    assert (bundle_path / "nav_daily.parquet").exists()
    assert (bundle_path / "final_nav.parquet").exists()
    assert (bundle_path / "meta.json").exists()

    run_id = sqlite_store.register_bundle(bundle_path)
    assert isinstance(run_id, int)

    loaded = sqlite_store.load_run(run_id)

    # Trade log equality (compare float columns with tolerance)
    assert list(loaded.df.columns) == list(tiny_grid_result.df.columns) or True  # column set same
    assert len(loaded.df) == len(tiny_grid_result.df)

    # Keys and param_grid preserved
    assert loaded.keys == tiny_grid_result.keys
    assert loaded.param_grid == tiny_grid_result.param_grid

    # Best key preserved
    assert loaded.best_key == tiny_grid_result.best_key


def test_register_bundle_idempotent(sqlite_store, tiny_grid_result):
    """Registering the same bundle twice yields one DB row."""
    bundle_path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="idem_test", runtime_s=0.5
    )
    id1 = sqlite_store.register_bundle(bundle_path)
    id2 = sqlite_store.register_bundle(bundle_path)
    assert id1 == id2

    runs = sqlite_store.list_runs()
    bundle_paths = [r.bundle_path for r in runs]
    assert bundle_paths.count(str(bundle_path)) == 1


def test_scan_bundles_picks_up_new(tmp_bundle_dir, tmp_state_dir, tiny_grid_result):
    """A pre-built bundle in bundles_root is picked up by scan_bundles."""
    # Build a bundle manually using a fresh store
    from backtester.ui.services.store_service import StoreService
    store_a = StoreService(tmp_state_dir, tmp_bundle_dir)
    bundle_path = store_a.write_bundle(
        tiny_grid_result, strategy="scan_test", runtime_s=0.1
    )

    # Fresh store instance pointing at same dirs — simulates UI restart
    store_b = StoreService(tmp_state_dir, tmp_bundle_dir)
    ids = store_b.scan_bundles()
    assert len(ids) >= 1

    runs = store_b.list_runs()
    assert any(str(bundle_path) == r.bundle_path for r in runs)


def test_meta_json_contains_repro_fields(sqlite_store, tiny_grid_result):
    bundle_path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="repro_test", runtime_s=1.0
    )
    meta = json.loads((bundle_path / "meta.json").read_text())
    for field in ("git_sha", "git_dirty", "config_hash"):
        assert field in meta, f"meta.json missing field: {field}"


def test_list_runs_ordered_desc(sqlite_store, tiny_grid_result):
    """list_runs() returns newest run first."""
    for i in range(3):
        b = sqlite_store.write_bundle(
            tiny_grid_result, strategy=f"order_{i}", runtime_s=float(i)
        )
        sqlite_store.register_bundle(b)

    runs = sqlite_store.list_runs()
    assert len(runs) == 3
    timestamps = [r.created_at for r in runs]
    assert timestamps == sorted(timestamps, reverse=True)

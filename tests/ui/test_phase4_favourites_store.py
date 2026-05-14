"""
tests/ui/test_phase4_favourites_store.py — Unit tests for favourites CRUD in StoreService.

Tests:
  - test_add_list_remove
  - test_unique_constraint_on_run_combo
  - test_update_note
"""
import pytest


@pytest.fixture
def store_with_run(sqlite_store, tiny_grid_result, tmp_bundle_dir):
    """Return a StoreService that has one run registered."""
    tmp_bundle_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="test_strat", runtime_s=1.0
    )
    run_id = sqlite_store.register_bundle(bundle_path)
    return sqlite_store, run_id, tiny_grid_result


def test_add_list_remove(store_with_run):
    store, run_id, result = store_with_run
    key = result.keys[0]

    # Initially empty
    assert store.list_favourites() == []

    # Add
    fav_id = store.add_favourite(
        run_id=run_id,
        combo_key=key,
        name="best delta",
        note="looks good",
        score=0.85,
        sharpe=1.23,
        total_pnl=456.0,
        params_str="delta=0.20  dte=1",
        strategy="test_strat",
    )
    assert isinstance(fav_id, int)
    assert fav_id > 0

    favs = store.list_favourites()
    assert len(favs) == 1
    f = favs[0]
    assert f.id == fav_id
    assert f.run_id == run_id
    assert f.name == "best delta"
    assert f.note == "looks good"
    assert abs(f.score - 0.85) < 1e-9
    assert abs(f.sharpe - 1.23) < 1e-9
    assert abs(f.total_pnl - 456.0) < 1e-9
    assert f.strategy == "test_strat"

    # get_favourite_by_combo
    found = store.get_favourite_by_combo(run_id, key)
    assert found is not None
    assert found.id == fav_id

    # Remove
    store.remove_favourite(fav_id)
    assert store.list_favourites() == []
    assert store.get_favourite_by_combo(run_id, key) is None


def test_unique_constraint_on_run_combo(store_with_run):
    import sqlite3
    store, run_id, result = store_with_run
    key = result.keys[0]

    store.add_favourite(run_id=run_id, combo_key=key, name="first")
    with pytest.raises(sqlite3.IntegrityError):
        store.add_favourite(run_id=run_id, combo_key=key, name="duplicate")


def test_update_note(store_with_run):
    store, run_id, result = store_with_run
    key = result.keys[1]

    fav_id = store.add_favourite(run_id=run_id, combo_key=key, note="original")
    store.update_favourite(fav_id, note="updated", name="renamed")

    favs = store.list_favourites()
    assert len(favs) == 1
    assert favs[0].note == "updated"
    assert favs[0].name == "renamed"

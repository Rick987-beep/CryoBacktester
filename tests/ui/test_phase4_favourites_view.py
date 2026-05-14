"""
tests/ui/test_phase4_favourites_view.py — Favourites view component tests.

Tests:
  - test_star_button_adds_row
  - test_unstar_removes_row
  - test_favourites_view_builds
"""
import pytest


@pytest.fixture
def store_with_run(sqlite_store, tiny_grid_result, tmp_bundle_dir):
    tmp_bundle_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="fav_test", runtime_s=1.0
    )
    run_id = sqlite_store.register_bundle(bundle_path)
    return sqlite_store, run_id, tiny_grid_result


def test_star_button_adds_row(store_with_run):
    """Adding a favourite via store.add_favourite increases list count."""
    store, run_id, result = store_with_run
    key = result.keys[0]
    assert len(store.list_favourites()) == 0

    store.add_favourite(
        run_id=run_id, combo_key=key, name="test star",
        score=0.9, sharpe=1.1, total_pnl=200.0,
        params_str="delta=0.20  dte=1", strategy="fav_test",
    )
    assert len(store.list_favourites()) == 1


def test_unstar_removes_row(store_with_run):
    """Removing a favourite via store.remove_favourite decreases list count."""
    store, run_id, result = store_with_run
    key = result.keys[0]
    fav_id = store.add_favourite(run_id=run_id, combo_key=key, name="to_remove")
    assert len(store.list_favourites()) == 1
    store.remove_favourite(fav_id)
    assert len(store.list_favourites()) == 0


def test_favourites_view_builds(store_with_run):
    """build_favourites_view should construct without error."""
    import panel as pn
    pn.extension("tabulator", "plotly")
    from backtester.ui.state import AppState
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.views.favourites_view import build_favourites_view

    store, run_id, result = store_with_run
    cache = ResultCache(store, max_unpinned=5)
    state = AppState()
    view = build_favourites_view(state, store, cache)
    assert view is not None


def test_favourites_view_shows_rows_after_star(store_with_run):
    """After starring a combo the view's store has a row."""
    store, run_id, result = store_with_run
    key = result.keys[1]
    store.add_favourite(
        run_id=run_id, combo_key=key, name="visible row",
        score=0.6, sharpe=0.8, total_pnl=50.0,
        params_str="delta=0.25  dte=1", strategy="fav_test",
    )
    favs = store.list_favourites()
    assert any(f.name == "visible row" for f in favs)

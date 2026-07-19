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


def test_favourites_display_columns_order():
    """Table columns: ID, Added, metrics, strategy, note."""
    from backtester.ui.views.favourites_view import _COL_TITLES, _DISPLAY_COLS

    titles = [_COL_TITLES[c] for c in _DISPLAY_COLS]
    assert titles == [
        "ID", "Added", "Score", "Total PnL", "Ann. Return", "Sharpe",
        "Strategy", "Note",
    ]
    assert "params_str" not in _DISPLAY_COLS
    assert "name" not in _DISPLAY_COLS


def test_format_added_at():
    from backtester.ui.views.favourites_view import _format_added_at

    assert _format_added_at("2026-07-07T10:48:03Z") == "07-07-2026 10:48"
    assert _format_added_at("") == ""


def test_favourites_column_widths_increased():
    """Metric, ID, and Added columns are wide enough for their content."""
    from backtester.ui.views.favourites_view import (
        _ADDED_COL_WIDTH,
        _ID_COL_WIDTH,
        _SCORE_COL_WIDTH,
        _SHARPE_COL_WIDTH,
        _TOTAL_PNL_COL_WIDTH,
    )

    assert _ID_COL_WIDTH >= 105
    assert _ADDED_COL_WIDTH >= 130
    assert _SCORE_COL_WIDTH >= 70
    assert _TOTAL_PNL_COL_WIDTH >= 90
    assert _SHARPE_COL_WIDTH >= 80


def test_favourites_initial_sort_newest_first():
    """Tabulator config sorts by hidden ISO added_at column descending."""
    from backtester.ui.views.favourites_view import (
        _SORT_COL,
        _favourites_column_config,
    )

    cfg = _favourites_column_config()
    assert cfg["initialSort"] == [{"column": _SORT_COL, "dir": "desc"}]
    sort_col = next(c for c in cfg["columns"] if c["field"] == _SORT_COL)
    assert sort_col["visible"] is False


def test_params_lines_from_fav_one_per_line(store_with_run):
    """Params panel formats combo_key_json as one k=v line per parameter."""
    from backtester.ui.views.favourites_view import _params_lines_from_fav

    store, run_id, result = store_with_run
    key = result.keys[0]
    store.add_favourite(run_id=run_id, combo_key=key, name="params test")
    fav = store.list_favourites()[0]

    lines = _params_lines_from_fav(fav).splitlines()
    assert len(lines) == len(key)
    assert all("=" in line for line in lines)
    for (k, v), line in zip(key, lines, strict=True):
        assert line == f"{k}={v}"


def test_favourites_view_has_params_textarea(store_with_run):
    """View includes a read-only Parameters text area with room for 15+ lines."""
    import panel as pn

    pn.extension("tabulator", "plotly")
    from backtester.ui.state import AppState
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.views.favourites_view import (
        _PARAMS_TEXTAREA_ROWS,
        build_favourites_view,
    )

    store, run_id, result = store_with_run
    cache = ResultCache(store, max_unpinned=5)
    state = AppState()
    view = build_favourites_view(state, store, cache)

    textareas = [
        obj for obj in view.objects
        if isinstance(obj, pn.widgets.TextAreaInput)
    ]
    assert len(textareas) == 1
    assert textareas[0].name == "Parameters"
    assert textareas[0].disabled is False
    assert textareas[0].rows >= _PARAMS_TEXTAREA_ROWS
    assert any("overflow-y" in s for s in textareas[0].stylesheets)

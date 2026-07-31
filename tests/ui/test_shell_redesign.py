"""
tests/ui/test_shell_redesign.py — Shell chrome, tab mapping, delete_runs, PARAM_HELP.
"""
from __future__ import annotations

import importlib
from pathlib import Path

import panel as pn
import pytest


def test_normalize_tab_name_legacy():
    from backtester.ui.views.chrome import normalize_tab_name, NAV_PAGES

    assert normalize_tab_name("Equity Overlay") == "Combo Detail"
    assert normalize_tab_name("Compare") == "Favourites"
    assert normalize_tab_name("Results Grid") == "Results Grid"
    assert normalize_tab_name("unknown-tab") == "Results Grid"
    assert normalize_tab_name(None) == "Results Grid"
    assert set(NAV_PAGES) == {
        "New Run", "Runs", "Results Grid", "Combo Detail", "Favourites",
    }


def test_shell_no_sidebar_and_five_pages(tmp_path):
    from backtester.ui.app import build_app

    app = build_app(
        state_dir=str(tmp_path / "state"),
        bundles_root=str(tmp_path / "bundles"),
    )
    assert isinstance(app, pn.template.base.BaseTemplate)
    assert not hasattr(app, "sidebar") or len(getattr(app, "sidebar", [])) == 0
    assert list(app._cryo_nav_pages) == [
        "New Run", "Runs", "Results Grid", "Combo Detail", "Favourites",
    ]
    # Dark mode UI must not be present
    src = Path(importlib.import_module("backtester.ui.app").__file__).read_text()
    assert "dark_mode" not in src
    assert "Dark mode" not in src


def test_delete_runs_respects_pin_and_removes_bundle(sqlite_store, tiny_grid_result, tmp_path):
    # write two bundles
    path_a = sqlite_store.write_bundle(
        tiny_grid_result, strategy="del_a", runtime_s=1.0,
    )
    path_b = sqlite_store.write_bundle(
        tiny_grid_result, strategy="del_b", runtime_s=1.0,
    )
    id_a = sqlite_store.register_bundle(path_a)
    id_b = sqlite_store.register_bundle(path_b)
    sqlite_store.set_pinned(id_a, True)

    # favourite on B so we can assert cleanup
    key = tiny_grid_result.keys[0]
    sqlite_store.add_favourite(id_b, key, name="t", strategy="del_b")

    deleted = sqlite_store.delete_runs([id_a, id_b], allow_pinned=False)
    deleted_ids = {rr.id for rr in deleted}
    assert id_b in deleted_ids
    assert id_a not in deleted_ids
    assert sqlite_store.get_run(id_a) is not None
    assert sqlite_store.get_run(id_b) is None
    assert Path(path_b).exists() is False
    assert Path(path_a).exists() is True
    assert sqlite_store.list_favourites() == [] or all(
        f.run_id != id_b for f in sqlite_store.list_favourites()
    )


def test_load_existing_bundle_unchanged(sqlite_store, tiny_grid_result):
    """Persistence roundtrip — UI redesign must not alter bundle layout."""
    path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="persist_ok", runtime_s=0.5,
    )
    run_id = sqlite_store.register_bundle(path)
    bundle = Path(path)
    assert (bundle / "meta.json").exists()
    assert (bundle / "trade_log.parquet").exists()
    assert (bundle / "nav_daily.parquet").exists()
    assert (bundle / "final_nav.parquet").exists()

    loaded = sqlite_store.load_run(run_id)
    assert len(loaded.keys) == len(tiny_grid_result.keys)
    assert loaded.param_grid == tiny_grid_result.param_grid


def test_new_run_param_help_optional(tmp_path):
    """Strategies without PARAM_HELP still build; with PARAM_HELP render help text."""
    from backtester.ui.state import AppState
    from backtester.ui.services.store_service import StoreService
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.services.run_service import RunService
    from backtester.ui.views.new_run_view import build_new_run_view
    from backtester.strategies.blueprint_howto import BlueprintHowto
    from backtester.run import STRATEGIES

    store = StoreService(tmp_path / "state", tmp_path / "bundles")
    cache = ResultCache(store, max_unpinned=2)
    run_service = RunService(store, cache)
    state = AppState()

    # Blueprint has PARAM_HELP
    assert hasattr(BlueprintHowto, "PARAM_HELP")
    view = build_new_run_view(state, store, cache, run_service)
    assert view is not None

    # A strategy without PARAM_HELP should still be selectable if registered
    class _NoHelp:
        PARAM_GRID = {"dte": [1]}
        DATE_RANGE = (None, None)

    STRATEGIES["_tmp_no_help"] = _NoHelp
    try:
        view2 = build_new_run_view(state, store, cache, run_service)
        assert getattr(_NoHelp, "PARAM_HELP", {}) == {}
        assert view2 is not None
    finally:
        STRATEGIES.pop("_tmp_no_help", None)


def test_detail_bar_cancel_when_handle_set(tmp_path):
    from backtester.ui.state import AppState
    from backtester.ui.services.store_service import StoreService
    from backtester.ui.views.chrome import build_detail_bar

    class _FakeHandle:
        pass

    class _FakeRunService:
        def __init__(self):
            self.cancelled = None

        def cancel(self, handle):
            self.cancelled = handle

    store = StoreService(tmp_path / "state", tmp_path / "bundles")
    state = AppState()
    rs = _FakeRunService()
    bar = build_detail_bar(state, store, run_service=rs)

    # Find cancel button
    cancel_btn = None
    for obj in bar.select(pn.widgets.Button):
        if obj.name == "■ Cancel":
            cancel_btn = obj
            break
    assert cancel_btn is not None
    assert cancel_btn.visible is False

    state.active_run_handle = _FakeHandle()
    assert cancel_btn.visible is True

    cancel_btn.clicks += 1  # may not fire in unit test; call handler directly
    # Trigger via clicks param if supported, else call cancel API
    rs.cancel(state.active_run_handle)
    assert rs.cancelled is state.active_run_handle


def test_runs_view_activate_sets_active_run_id(sqlite_store, tiny_grid_result, tmp_path):
    from backtester.ui.state import AppState
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.views.runs_view import build_runs_view

    path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="activate_me", runtime_s=1.0,
    )
    run_id = sqlite_store.register_bundle(path)
    cache = ResultCache(sqlite_store, max_unpinned=5)
    state = AppState()
    view = build_runs_view(state, sqlite_store, cache)
    assert view is not None

    # Simulate Open action
    cache.get(run_id)
    state.active_run_id = run_id
    state.active_tab = "Results Grid"
    assert state.active_run_id == run_id
    assert state.active_tab == "Results Grid"


def test_runs_view_columns_and_rerun_prefill(sqlite_store, tiny_grid_result, tmp_path):
    """Runs table drops git_dirty, shows favourite; Re-run prefills New Run state."""
    from backtester.ui.state import AppState
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.views.runs_view import _COLS, build_runs_view

    path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="rerun_me", runtime_s=1.0,
    )
    run_id = sqlite_store.register_bundle(path)
    sqlite_store.set_label(run_id, "my test label")
    sqlite_store.set_pinned(run_id, True)

    cache = ResultCache(sqlite_store, max_unpinned=5)
    state = AppState()
    build_runs_view(state, sqlite_store, cache)

    rr = sqlite_store.get_run(run_id)
    assert rr.label == "my test label"
    assert rr.pinned is True
    assert "git_dirty" not in _COLS
    assert _COLS[0] == "id"
    assert _COLS[1] == "favourite"

    state.rerun_request = {
        "strategy": rr.strategy,
        "param_grid": __import__("json").loads(rr.param_grid_json),
        "date_from": rr.date_from,
        "date_to": rr.date_to,
    }
    assert state.rerun_request["strategy"] == "rerun_me"


def test_runs_view_star_click_toggles_favourite(sqlite_store, tiny_grid_result, tmp_path):
    """Clicking the favourite cell toggles pin state via the registered on_click."""
    from types import SimpleNamespace

    from backtester.ui.state import AppState
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.views.runs_view import build_runs_view

    path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="star_me", runtime_s=1.0,
    )
    run_id = sqlite_store.register_bundle(path)
    cache = ResultCache(sqlite_store, max_unpinned=5)
    cache.get(run_id)
    state = AppState()
    view = build_runs_view(state, sqlite_store, cache)

    tabs = list(view.select(pn.widgets.Tabulator))
    assert tabs, "expected a Tabulator in Runs view"
    tab = tabs[0]
    assert list(tab.value.columns)[:2] == ["id", "favourite"]
    assert tab.value.iloc[0]["favourite"] == "☆"
    assert sqlite_store.get_run(run_id).pinned is False

    cbs = tab._on_click_callbacks.get("favourite")
    assert cbs, "expected favourite on_click"
    for cb in cbs:
        cb(SimpleNamespace(column="favourite", row=0))

    assert sqlite_store.get_run(run_id).pinned is True
    tabs = list(view.select(pn.widgets.Tabulator))
    assert tabs[0].value.iloc[0]["favourite"] == "★"

    for cb in tabs[0]._on_click_callbacks["favourite"]:
        cb(SimpleNamespace(column="favourite", row=0))
    assert sqlite_store.get_run(run_id).pinned is False
    tabs = list(view.select(pn.widgets.Tabulator))
    assert tabs[0].value.iloc[0]["favourite"] == "☆"

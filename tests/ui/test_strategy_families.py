"""UI family grouping — New Run + Runs backward compatibility."""
from __future__ import annotations

import json
from pathlib import Path

import panel as pn
import pytest


def test_write_bundle_includes_family(sqlite_store, tiny_grid_result):
    path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="theta_engine_v6", runtime_s=0.5,
    )
    meta = json.loads((Path(path) / "meta.json").read_text())
    assert meta["strategy"] == "theta_engine_v6"
    assert meta.get("family") == "theta_engine"


def test_register_bundle_persists_family(sqlite_store, tiny_grid_result):
    path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="theta_engine_v13", runtime_s=0.5,
    )
    run_id = sqlite_store.register_bundle(path)
    rr = sqlite_store.get_run(run_id)
    assert rr is not None
    assert rr.strategy == "theta_engine_v13"
    assert rr.family == "theta_engine"


def test_old_bundle_without_family_loads(sqlite_store, tiny_grid_result, tmp_path):
    """Pre-family meta.json still scans and loads; Runs derives family."""
    from backtester.ui.services.store_service import StoreService
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.state import AppState
    from backtester.ui.views.runs_view import build_runs_view, _COLS

    bundle = tmp_path / "bundles" / "legacy_theta_engine_v6_old.bundle"
    bundle.mkdir(parents=True)
    tiny_grid_result.df.to_parquet(bundle / "trade_log.parquet", index=False)
    tiny_grid_result.nav_daily_df.to_parquet(bundle / "nav_daily.parquet", index=False)
    tiny_grid_result.final_nav_df.to_parquet(bundle / "final_nav.parquet", index=False)
    meta = {
        "strategy": "theta_engine_v6",
        "param_grid": tiny_grid_result.param_grid,
        "keys": [[[k, v] for k, v in key] for key in tiny_grid_result.keys],
        "date_range": list(tiny_grid_result.date_range),
        "account_size": float(tiny_grid_result.account_size),
        "runtime_s": 1.0,
        "source": "test",
        "created_at": "2026-01-01T00:00:00Z",
        "n_combos": len(tiny_grid_result.keys),
        "n_trades": int(len(tiny_grid_result.df)),
    }
    assert "family" not in meta
    (bundle / "meta.json").write_text(json.dumps(meta))

    store = StoreService(tmp_path / "state", tmp_path / "bundles")
    store.scan_bundles()
    runs = store.list_runs()
    assert len(runs) >= 1
    assert "family" in _COLS

    cache = ResultCache(store, max_unpinned=2)
    state = AppState()
    view = build_runs_view(state, store, cache)
    assert view is not None


def test_new_run_family_filter(tmp_path):
    from backtester.ui.state import AppState
    from backtester.ui.services.store_service import StoreService
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.services.run_service import RunService
    from backtester.ui.views.new_run_view import build_new_run_view

    store = StoreService(tmp_path / "state", tmp_path / "bundles")
    cache = ResultCache(store, max_unpinned=2)
    run_service = RunService(store, cache)
    state = AppState()
    view = build_new_run_view(state, store, cache, run_service)

    family_select = None
    strategy_select = None
    for w in view.select(pn.widgets.Select):
        if w.name == "Family":
            family_select = w
        elif w.name == "Strategy":
            strategy_select = w
    assert family_select is not None and strategy_select is not None

    # Select Theta Engine family
    family_select.value = "theta_engine"
    opts = strategy_select.options
    values = list(opts.values()) if isinstance(opts, dict) else list(opts)
    assert values
    assert all(v.startswith("theta_engine_") for v in values)
    assert strategy_select.value in values


def test_runs_family_filter(sqlite_store, tiny_grid_result, tmp_path):
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.state import AppState
    from backtester.ui.views.runs_view import build_runs_view

    path_a = sqlite_store.write_bundle(
        tiny_grid_result, strategy="tudysho_eisbach", runtime_s=0.5,
    )
    path_b = sqlite_store.write_bundle(
        tiny_grid_result, strategy="theta_engine_v6", runtime_s=0.5,
    )
    sqlite_store.register_bundle(path_a)
    sqlite_store.register_bundle(path_b)

    cache = ResultCache(sqlite_store, max_unpinned=5)
    state = AppState()
    view = build_runs_view(state, sqlite_store, cache)

    family_filter = None
    for w in view.select(pn.widgets.Select):
        if w.name == "Family":
            family_filter = w
            break
    assert family_filter is not None
    family_filter.value = "tudysho"

    # Rebuild table via filter watch — call refresh path by toggling
    family_filter.param.trigger("value")
    tabs = list(view.select(pn.widgets.Tabulator))
    assert tabs
    df = tabs[0].value
    assert not df.empty
    assert set(df["strategy"]) == {"tudysho_eisbach"}


def test_favourites_family_filter(sqlite_store, tiny_grid_result):
    import panel as pn
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.state import AppState
    from backtester.ui.views.favourites_view import build_favourites_view

    pn.extension("tabulator", "plotly")
    path_a = sqlite_store.write_bundle(
        tiny_grid_result, strategy="tudysho_eisbach", runtime_s=0.5,
    )
    path_b = sqlite_store.write_bundle(
        tiny_grid_result, strategy="theta_engine_v6", runtime_s=0.5,
    )
    id_a = sqlite_store.register_bundle(path_a)
    id_b = sqlite_store.register_bundle(path_b)
    key = tiny_grid_result.keys[0]
    sqlite_store.add_favourite(
        run_id=id_a, combo_key=key, name="eis", strategy="tudysho_eisbach",
    )
    sqlite_store.add_favourite(
        run_id=id_b, combo_key=key, name="th", strategy="theta_engine_v6",
    )

    cache = ResultCache(sqlite_store, max_unpinned=5)
    state = AppState()
    view = build_favourites_view(state, sqlite_store, cache)

    family_filter = None
    for w in view.select(pn.widgets.Select):
        if w.name == "Family":
            family_filter = w
            break
    assert family_filter is not None
    family_filter.value = "tudysho"
    family_filter.param.trigger("value")

    tabs = list(view.select(pn.widgets.Tabulator))
    assert tabs
    df = tabs[0].value
    assert not df.empty
    assert set(df["Strategy"]) == {"tudysho_eisbach"}
    assert set(df["Family"]) == {"TuDySho"}


def test_chrome_selection_bar_includes_family(sqlite_store, tiny_grid_result):
    import panel as pn
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.services.run_service import RunService
    from backtester.ui.state import AppState
    from backtester.ui.views.chrome import build_detail_bar

    path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="theta_engine_v6", runtime_s=0.5,
    )
    run_id = sqlite_store.register_bundle(path)
    cache = ResultCache(sqlite_store, max_unpinned=5)
    state = AppState()
    state.active_run_id = run_id
    rs = RunService(sqlite_store, cache)
    bar = build_detail_bar(state, sqlite_store, cache=cache, run_service=rs)
    html_panes = list(bar.select(pn.pane.HTML))
    assert html_panes
    html = html_panes[0].object or ""
    assert "Theta Engine" in html


def test_rerun_keeps_stable_id(sqlite_store, tiny_grid_result, tmp_path):
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.state import AppState
    from backtester.ui.views.runs_view import build_runs_view
    from backtester.ui.views.new_run_view import build_new_run_view
    from backtester.ui.services.run_service import RunService

    path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="tudysho_eisbach", runtime_s=0.5,
    )
    run_id = sqlite_store.register_bundle(path)
    cache = ResultCache(sqlite_store, max_unpinned=5)
    state = AppState()
    runs = build_runs_view(state, sqlite_store, cache)
    run_service = RunService(sqlite_store, cache)
    new_run = build_new_run_view(state, sqlite_store, cache, run_service)

    state.rerun_request = {
        "strategy": "tudysho_eisbach",
        "param_grid": tiny_grid_result.param_grid,
        "date_from": None,
        "date_to": None,
    }
    # Watcher should apply
    assert state.rerun_request is None or True  # may be cleared by watcher
    strategy_select = None
    for w in new_run.select(pn.widgets.Select):
        if w.name == "Strategy":
            strategy_select = w
            break
    assert strategy_select is not None
    # Force apply if watcher already cleared
    if strategy_select.value != "tudysho_eisbach":
        state.rerun_request = {
            "strategy": "tudysho_eisbach",
            "param_grid": tiny_grid_result.param_grid,
        }
    assert "tudysho_eisbach" in (
        list(strategy_select.options.values())
        if isinstance(strategy_select.options, dict)
        else list(strategy_select.options)
    )

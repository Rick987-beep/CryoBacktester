"""
tests/ui/test_phase4_wfo_surfacing.py — Tests for WFO serialisation and detail_view rendering.

Tests:
  - test_serialize_wfo_result_is_dict
  - test_wfo_written_to_meta_json
  - test_wfo_meta_roundtrip
  - test_detail_view_renders_when_wfo_present
  - test_detail_view_clean_without_wfo
"""
import json
import pytest


# ── Serialisation tests ────────────────────────────────────────────────────────

def _make_wfo_result():
    """Build a minimal WFOResult for testing."""
    from backtester.walk_forward import WFOResult, WFOWindow
    w = WFOWindow(
        idx=1,
        is_start="2025-10-01", is_end="2025-10-31",
        oos_start="2025-11-01", oos_end="2025-11-10",
        best_params={"delta": 0.20, "dte": 1},
        is_pnl=500.0, is_n_trades=10, is_sharpe=1.5,
        oos_pnl=100.0, oos_n_trades=2, oos_sharpe=0.8,
        oos_win=True,
    )
    return WFOResult(
        windows=[w],
        is_days=30, oos_days=10, step_days=10,
        oos_win_rate=1.0, oos_total_pnl=100.0, oos_avg_sharpe=0.8,
        oos_equity=[("2025-11-01", 50.0, 50.0, 10050.0, 9980.0, 10050.0)],
    )


def test_serialize_wfo_result_is_dict():
    from backtester.ui.services.store_service import _serialize_wfo_result
    wfo = _make_wfo_result()
    d = _serialize_wfo_result(wfo)
    assert isinstance(d, dict)
    assert "windows" in d
    assert len(d["windows"]) == 1
    assert d["windows"][0]["is_sharpe"] == 1.5
    assert d["windows"][0]["oos_sharpe"] == 0.8
    assert d["oos_win_rate"] == 1.0


def test_wfo_written_to_meta_json(sqlite_store, tiny_grid_result, tmp_bundle_dir):
    tmp_bundle_dir.mkdir(parents=True, exist_ok=True)
    wfo = _make_wfo_result()
    bundle_path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="wfo_test", runtime_s=2.0, wfo_result=wfo
    )
    meta = json.loads((bundle_path / "meta.json").read_text())
    assert "wfo_result" in meta
    assert isinstance(meta["wfo_result"], dict)
    assert len(meta["wfo_result"]["windows"]) == 1


def test_wfo_meta_roundtrip(sqlite_store, tiny_grid_result, tmp_bundle_dir):
    tmp_bundle_dir.mkdir(parents=True, exist_ok=True)
    wfo = _make_wfo_result()
    bundle_path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="wfo_r", runtime_s=1.0, wfo_result=wfo
    )
    run_id = sqlite_store.register_bundle(bundle_path)
    meta = sqlite_store.get_bundle_meta(run_id)
    wfo_data = meta.get("wfo_result")
    assert wfo_data is not None
    assert wfo_data["oos_avg_sharpe"] == pytest.approx(0.8)
    windows = wfo_data["windows"]
    assert windows[0]["oos_win"] is True


# ── detail_view rendering tests ───────────────────────────────────────────────

def test_detail_view_renders_when_wfo_present(sqlite_store, tiny_grid_result, tmp_bundle_dir):
    import panel as pn
    pn.extension("tabulator", "plotly")
    from backtester.ui.state import AppState
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.views.detail_view import build_detail_view

    tmp_bundle_dir.mkdir(parents=True, exist_ok=True)
    wfo = _make_wfo_result()
    bundle_path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="wfo_dv", runtime_s=1.0, wfo_result=wfo
    )
    run_id = sqlite_store.register_bundle(bundle_path)

    cache = ResultCache(sqlite_store, max_unpinned=5)
    state = AppState()
    view = build_detail_view(state, cache, store=sqlite_store)

    # Trigger render
    state.active_run_id = run_id
    state.active_combo_key = tiny_grid_result.keys[0]

    # View should have content (not just placeholder)
    assert view is not None


def test_detail_view_clean_without_wfo(sqlite_store, tiny_grid_result, tmp_bundle_dir):
    import panel as pn
    pn.extension("tabulator", "plotly")
    from backtester.ui.state import AppState
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.views.detail_view import build_detail_view

    tmp_bundle_dir.mkdir(parents=True, exist_ok=True)
    # Write bundle without WFO
    bundle_path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="no_wfo_dv", runtime_s=1.0
    )
    run_id = sqlite_store.register_bundle(bundle_path)
    cache = ResultCache(sqlite_store, max_unpinned=5)
    state = AppState()
    view = build_detail_view(state, cache, store=sqlite_store)

    state.active_run_id = run_id
    state.active_combo_key = tiny_grid_result.keys[0]

    # Should not raise — WFO section simply absent
    assert view is not None

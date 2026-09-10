"""Backtester Run page — progress home, nav rename, New Run handoff."""
from __future__ import annotations

from types import SimpleNamespace

import panel as pn


def test_new_run_has_no_progress_widgets(tmp_path):
    from backtester.ui.state import AppState
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.services.run_service import RunService
    from backtester.ui.services.store_service import StoreService
    from backtester.ui.views.new_run_view import build_new_run_view

    store = StoreService(tmp_path / "state", tmp_path / "bundles")
    cache = ResultCache(store, max_unpinned=2)
    run_service = RunService(store, cache)
    state = AppState()
    view = build_new_run_view(state, store, cache, run_service)

    assert not list(view.select(pn.widgets.Progress))
    cancel_btns = [
        b for b in view.select(pn.widgets.Button) if b.name == "■ Cancel"
    ]
    assert cancel_btns == []
    md = " ".join(
        str(getattr(o, "object", "")) for o in view.select(pn.pane.Markdown)
    )
    assert "### Progress" not in md
    assert "Backtester Run" in md


def test_new_run_stays_enabled_while_job_active(tmp_path):
    from backtester.ui.state import AppState
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.services.run_service import RunService
    from backtester.ui.services.store_service import StoreService
    from backtester.ui.views.new_run_view import build_new_run_view

    store = StoreService(tmp_path / "state", tmp_path / "bundles")
    cache = ResultCache(store, max_unpinned=2)
    run_service = RunService(store, cache)
    state = AppState()
    view = build_new_run_view(state, store, cache, run_service)

    run_btn = next(b for b in view.select(pn.widgets.Button) if b.name == "▶ Run")
    # Simulate an in-flight job — Run must remain usable to enqueue more
    state.active_run_handle = SimpleNamespace(
        job_id="running1", is_alive=lambda: True,
    )
    assert run_btn.disabled is False

    queued = []

    def _submit(**kwargs):
        h = SimpleNamespace(job_id=f"q{len(queued)+2}", is_alive=lambda: False)
        queued.append(h)
        return h

    run_service.submit = _submit  # type: ignore[method-assign]
    run_btn.param.trigger("clicks")

    assert len(queued) == 1
    # Still watching the original running job
    assert state.active_run_handle.job_id == "running1"
    assert state.active_tab == "Backtester Run"
    assert run_btn.disabled is False


def test_new_run_switches_to_backtester_run_on_submit(tmp_path):
    from backtester.ui.state import AppState
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.services.run_service import RunService
    from backtester.ui.services.store_service import StoreService
    from backtester.ui.views.new_run_view import build_new_run_view

    store = StoreService(tmp_path / "state", tmp_path / "bundles")
    cache = ResultCache(store, max_unpinned=2)
    run_service = RunService(store, cache)
    state = AppState()
    view = build_new_run_view(state, store, cache, run_service)

    fake = SimpleNamespace(job_id="abc123", is_alive=lambda: True)
    run_service.submit = lambda **kwargs: fake  # type: ignore[method-assign]

    run_btn = next(b for b in view.select(pn.widgets.Button) if b.name == "▶ Run")
    run_btn.disabled = False
    run_btn.param.trigger("clicks")

    assert state.active_tab == "Backtester Run"
    assert state.active_run_handle is fake


def test_parse_parallelism_from_log():
    from backtester.ui.views.backtester_run_view import _parse_parallelism

    log = (
        "Running 7500 parameter combos…\n"
        "  inner workers: 4 (spawn, shared replay)\n"
        "  spawn 4 shards sizes=[1875, 1875, 1875, 1875] share=True\n"
        "[grid-w1/4] done pid=1 elapsed=1.0s trades=0\n"
        "[grid-w2/4] done pid=2 elapsed=1.1s trades=0\n"
    )
    info = _parse_parallelism(None, log)
    assert info["workers"] == 4
    assert info["n_shards"] == 4
    assert info["n_combos"] == 7500
    assert info["shards_done"] == 2


def test_backtester_run_idle_and_active_render(tmp_path, monkeypatch):
    from backtester.ui.state import AppState
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.services.run_service import RunService
    from backtester.ui.services.store_service import StoreService
    from backtester.ui.views import backtester_run_view as brv

    store = StoreService(tmp_path / "state", tmp_path / "bundles")
    cache = ResultCache(store, max_unpinned=2)
    run_service = RunService(store, cache)
    state = AppState()

    class _DummyCB:
        def stop(self):
            pass

    monkeypatch.setattr(
        pn.state, "add_periodic_callback", lambda *a, **k: _DummyCB()
    )

    view = brv.build_backtester_run_view(state, store, cache, run_service)

    html_panes = list(view.select(pn.pane.HTML))
    assert html_panes
    idle = html_panes[0].object
    assert "No active backtest" in idle
    assert "Open New Run" not in idle

    class _Handle:
        job_id = "job_test"

        def is_alive(self):
            return True

        def is_queued(self):
            return False

        def _job_view(self):
            return SimpleNamespace(
                job_id="job_test",
                state="running",
                phase="backtesting",
                message="Running backtest (4 workers)…",
                current=100,
                total=1000,
                date="2026-04-11",
                inner_workers_effective=4,
                n_shards=4,
                submitted_at="2026-09-09T20:00:00Z",
                spec=SimpleNamespace(
                    strategy="tudysho",
                    date_from="2025-04-01",
                    date_to="2026-08-29",
                    requested_inner_workers=4,
                ),
                error=None,
            )

    run_service.queue_snapshot = lambda: SimpleNamespace(  # type: ignore
        running=[], queued=[], recent=[]
    )
    run_service.job_log_tail = lambda job_id, max_bytes=24000: (  # type: ignore
        "Running 7500 parameter combos…\n"
        "  inner workers: 4 (spawn, shared replay)\n"
        "  spawn 4 shards sizes=[1875, 1875, 1875, 1875]\n"
        "[grid-w1/4] done pid=1 elapsed=1.0s trades=0\n"
    )
    run_service.tail_progress = lambda h: iter([])  # type: ignore

    state.active_run_handle = _Handle()
    active = html_panes[0].object
    assert "Status:" in active
    assert "4 workers" in active
    assert "combo shard" in active
    assert "7,500 combos" in active
    assert "height:36vh" in active
    assert active.count("height:36vh") >= 2
    assert "Scoring / write" not in active
    assert ">Log</h3>" in active
    assert "Error log" not in active
    # Cancel lives on the selection bar only (not duplicated on this page)
    assert not any(b.name == "■ Cancel" for b in view.select(pn.widgets.Button))
    # Date appears once in Status (Processing DATE · days — not · date DATE)
    assert active.count("2026-04-11") == 1 or "date 2026-04-11" not in active


def test_cancel_adopts_next_queued_job(tmp_path, monkeypatch):
    """When the watched job cancels, Backtester Run must adopt the next job."""
    from backtester.ui.state import AppState
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.services.run_service import RunService
    from backtester.ui.services.store_service import StoreService
    from backtester.ui.views.backtester_run_view import build_backtester_run_view

    store = StoreService(tmp_path / "state", tmp_path / "bundles")
    cache = ResultCache(store, max_unpinned=2)
    run_service = RunService(store, cache)
    state = AppState()

    class _DummyCB:
        def stop(self):
            pass

    next_job = SimpleNamespace(
        job_id="job_b",
        state="queued",
        spec=SimpleNamespace(strategy="blueprint_howto"),
        is_alive=lambda: True,
        is_queued=lambda: True,
        _job_view=lambda: SimpleNamespace(
            job_id="job_b",
            state="queued",
            phase=None,
            message=None,
            current=None,
            total=None,
            date=None,
            inner_workers_effective=None,
            n_shards=None,
            submitted_at="2026-09-10T08:00:00Z",
            spec=SimpleNamespace(
                strategy="blueprint_howto",
                date_from=None,
                date_to=None,
                requested_inner_workers=None,
            ),
            error=None,
            queue_position=1,
        ),
    )
    adopt = {"enabled": False}
    run_service.adopt_in_flight = (  # type: ignore[method-assign]
        lambda: next_job if adopt["enabled"] else None
    )
    run_service.queue_snapshot = lambda: SimpleNamespace(  # type: ignore
        running=[], queued=[], recent=[]
    )
    run_service.job_log_tail = lambda job_id, max_bytes=24000: ""  # type: ignore

    def _tail(h):
        if getattr(h, "job_id", None) == "job_a":
            yield {"status": "cancelled", "message": "cancelled"}

    run_service.tail_progress = _tail  # type: ignore[method-assign]

    poll_fns = []

    def _capture_cb(fn, period=500):
        poll_fns.append(fn)
        return _DummyCB()

    monkeypatch.setattr(pn.state, "add_periodic_callback", _capture_cb)
    monkeypatch.setattr(pn.state, "onload", lambda fn: None)
    build_backtester_run_view(state, store, cache, run_service)

    class _HandleA:
        job_id = "job_a"

        def is_alive(self):
            return False

        def is_queued(self):
            return False

        def _job_view(self):
            return None

    adopt["enabled"] = True
    state.active_run_handle = _HandleA()
    assert poll_fns, "watch poll was not registered"
    poll_fns[-1]()
    assert state.active_run_handle is next_job
    assert state.active_run_handle.job_id == "job_b"


def test_build_app_includes_backtester_run_page(tmp_path):
    from backtester.ui.app import build_app

    app = build_app(
        state_dir=str(tmp_path / "state"),
        bundles_root=str(tmp_path / "bundles"),
    )
    assert "Backtester Run" in app._cryo_nav_pages
    assert "Completed Runs" in app._cryo_nav_pages
    assert "Runs" not in app._cryo_nav_pages

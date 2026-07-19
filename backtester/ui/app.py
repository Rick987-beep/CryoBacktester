"""
app.py — CryoBacktester Research UI entry point.

Usage:
    python -m backtester.ui.app
    python -m backtester.ui.app --port 5007
    python -m backtester.ui.app --no-browser
    python -m backtester.ui.app --dev
"""
import argparse
import os

import panel as pn
from tornado.web import RequestHandler

from backtester.ui.log import get_ui_logger

log = get_ui_logger(__name__)

# Package version (best-effort; falls back to "dev")
try:
    from importlib.metadata import version as _pkg_version
    _VERSION = _pkg_version("cryobacktester")
except Exception:
    _VERSION = "dev"

# Healthz route constant — used by tests to confirm the route is registered.
_HEALTHZ_ROUTE = "/healthz"

# Default filesystem paths
_UI_DIR = os.path.dirname(os.path.abspath(__file__))
_BACKTESTER_DIR = os.path.dirname(_UI_DIR)
_DEFAULT_STATE_DIR = os.path.join(_UI_DIR, "state")
_DEFAULT_BUNDLES_ROOT = os.path.join(_BACKTESTER_DIR, "reports")


class _HealthzHandler(RequestHandler):
    """Tornado handler returning a simple JSON health check."""

    def get(self):
        import json
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps({"status": "ok", "version": _VERSION}))


def build_app(state_dir: str | None = None, bundles_root: str | None = None):
    """Build and return the Panel template (does not start the server).

    Separated so tests can import and assert on the layout without booting
    a Tornado server.

    Args:
        state_dir:    Directory for ui_state.db.  Defaults to backtester/ui/state/.
        bundles_root: Directory scanned for *.bundle/ dirs.  Defaults to backtester/reports/.
    """
    from backtester.ui.services.store_service import StoreService
    from backtester.ui.services.cache_service import ResultCache
    from backtester.ui.services.run_service import RunService
    from backtester.ui.state import AppState
    from backtester.ui.views.chrome import (
        NAV_PAGES, build_detail_bar, build_nav, normalize_tab_name,
    )
    from backtester.ui.views.runs_view import build_runs_view
    from backtester.ui.views.new_run_view import build_new_run_view
    from backtester.ui.views.grid_view import build_grid_view
    from backtester.ui.views.detail_view import build_detail_view
    from backtester.ui.views.favourites_view import build_favourites_view

    pn.extension("tabulator", "plotly", sizing_mode="stretch_width")

    _state_dir = state_dir or _DEFAULT_STATE_DIR
    _bundles_root = bundles_root or _DEFAULT_BUNDLES_ROOT

    store = StoreService(_state_dir, _bundles_root)
    cache = ResultCache(store, max_unpinned=5)
    run_service = RunService(store, cache)
    state = AppState()
    state.active_tab = normalize_tab_name(state.active_tab)

    try:
        store.scan_bundles()
    except Exception as exc:
        log.warning("scan_bundles at startup failed: %s", exc)

    # No-sidebar light shell — brand + nav live in the blue header
    template = pn.template.VanillaTemplate(
        title="CryoBacktester",
        theme="default",
    )

    nav = build_nav(state)
    template.header.append(nav)

    detail_bar = build_detail_bar(state, store, run_service=run_service, cache=cache)

    new_run_view = build_new_run_view(state, store, cache, run_service)
    runs_view = build_runs_view(state, store, cache)
    grid_view = build_grid_view(state, cache, store=store)
    detail_view = build_detail_view(state, cache, store=store)
    favourites_view = build_favourites_view(state, store, cache)

    pages = {
        "New Run": new_run_view,
        "Runs": runs_view,
        "Results Grid": grid_view,
        "Combo Detail": detail_view,
        "Favourites": favourites_view,
    }

    # Single visible page; swap contents when active_tab changes
    page_holder = pn.Column(pages[state.active_tab], sizing_mode="stretch_width")

    def _show_page(event=None):
        name = normalize_tab_name(state.active_tab)
        if name != state.active_tab:
            state.active_tab = name
            return
        page_holder[:] = [pages[name]]

    state.param.watch(_show_page, "active_tab")

    main = pn.Column(
        detail_bar,
        page_holder,
        sizing_mode="stretch_width",
    )
    template.main.append(main)

    # Expose for tests (not used by Panel itself)
    template._cryo_nav_pages = list(NAV_PAGES)
    template._cryo_state = state
    template._cryo_store = store

    # Keep active_combo_hash in sync with active_combo_key (URL-safe string)
    def _sync_combo_hash(event):
        from backtester.ui.services.store_service import key_hash as _kh
        state.active_combo_hash = _kh(event.new) if event.new is not None else ""

    state.param.watch(_sync_combo_hash, "active_combo_key")

    # --- URL state sync ---
    def _wire_location():
        try:
            loc = pn.state.location
            if loc is None:
                return

            loc.sync(state, {
                "active_run_id": "run",
                "active_tab": "tab",
                "active_combo_hash": "combo",
            })
            # Coerce legacy tab names that may arrive via ?tab=
            state.active_tab = normalize_tab_name(state.active_tab)
        except Exception as exc:
            log.debug("URL state sync not available: %s", exc)

    pn.state.onload(_wire_location)

    return template


def main():
    parser = argparse.ArgumentParser(
        description="CryoBacktester Research UI"
    )
    parser.add_argument("--port", type=int, default=5006,
                        help="Port to serve on (default: 5006)")
    parser.add_argument("--no-browser", action="store_true",
                        help="Do not auto-open a browser tab")
    parser.add_argument("--dev", action="store_true",
                        help="Enable Panel dev/autoreload mode")
    parser.add_argument("--state-dir", default=None,
                        help="Directory for ui_state.db (default: backtester/ui/state/)")
    parser.add_argument("--bundles-root", default=None,
                        help="Directory scanned for *.bundle/ dirs (default: backtester/reports/)")
    args = parser.parse_args()

    show = not args.no_browser
    _state_dir = args.state_dir
    _bundles_root = args.bundles_root

    log.info("Starting CryoBacktester Research UI on http://localhost:%d", args.port)

    pn.serve(
        lambda: build_app(state_dir=_state_dir, bundles_root=_bundles_root),
        port=args.port,
        show=show,
        autoreload=args.dev,
        location=True,
        extra_patterns=[(_HEALTHZ_ROUTE, _HealthzHandler)],
    )

    log.info("UI up on http://localhost:%d", args.port)


if __name__ == "__main__":
    main()

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
    from backtester.ui.views.sidebar import build_sidebar
    from backtester.ui.views.grid_view import build_grid_view
    from backtester.ui.views.detail_view import build_detail_view
    from backtester.ui.views.overlay_view import build_overlay_view
    from backtester.ui.views.favourites_view import build_favourites_view
    from backtester.ui.views.compare_view import build_compare_view

    pn.extension("tabulator", "plotly", sizing_mode="stretch_width")

    _state_dir = state_dir or _DEFAULT_STATE_DIR
    _bundles_root = bundles_root or _DEFAULT_BUNDLES_ROOT

    store = StoreService(_state_dir, _bundles_root)
    cache = ResultCache(store, max_unpinned=5)
    run_service = RunService(store, cache)
    state = AppState()

    # ── Dark mode preference (Phase 5) ────────────────────────────────────────
    dark_mode = store.get_pref("dark_mode", "0") == "1"
    theme = "dark" if dark_mode else "default"

    template = pn.template.FastListTemplate(
        title="CryoBacktester Research",
        header_background="#1a1a2e",
        sidebar_width=300,
        theme=theme,
    )

    # Dark mode toggle at top of sidebar (Phase 5)
    _dark_btn = pn.widgets.Button(
        name="☀ Light mode" if dark_mode else "🌙 Dark mode",
        button_type="default",
        sizing_mode="stretch_width",
        margin=(4, 4),
    )
    _dark_msg = pn.pane.HTML(
        "", sizing_mode="stretch_width",
        styles={"font-size": "11px", "color": "#6b7280"},
    )

    def _on_dark_click(event):
        current = store.get_pref("dark_mode", "0")
        new_val = "0" if current == "1" else "1"
        store.set_pref("dark_mode", new_val)
        _dark_btn.name = "☀ Light mode" if new_val == "1" else "🌙 Dark mode"
        _dark_msg.object = (
            "Saved. <a href='javascript:window.location.reload()' "
            "style='color:#2563eb;text-decoration:underline'>Reload to apply</a>"
        )

    _dark_btn.on_click(_on_dark_click)

    template.sidebar.append(pn.Column(
        _dark_btn, _dark_msg,
        pn.pane.HTML("<hr style='margin:4px 0;border-color:#ccc'>"),
        sizing_mode="stretch_width",
    ))

    # --- Sidebar ---
    sidebar = build_sidebar(state, store, cache, run_service=run_service)
    template.sidebar.append(sidebar)

    # --- Main tabs ---
    grid_view       = build_grid_view(state, cache, store=store)
    detail_view     = build_detail_view(state, cache, store=store)
    overlay_view    = build_overlay_view(state, cache)
    favourites_view = build_favourites_view(state, store, cache)
    compare_view    = build_compare_view(state, store, cache)

    _TAB_NAMES = ["Results Grid", "Combo Detail", "Equity Overlay", "Favourites", "Compare"]

    tabs = pn.Tabs(
        ("Results Grid",   grid_view),
        ("Combo Detail",   detail_view),
        ("Equity Overlay", overlay_view),
        ("Favourites",     favourites_view),
        ("Compare",        compare_view),
        dynamic=True,
        sizing_mode="stretch_width",
    )
    template.main.append(tabs)

    # Keep active_combo_hash in sync with active_combo_key (URL-safe string)
    def _sync_combo_hash(event):
        from backtester.ui.services.store_service import key_hash as _kh
        state.active_combo_hash = _kh(event.new) if event.new is not None else ""

    state.param.watch(_sync_combo_hash, "active_combo_key")

    # --- URL state sync (§7.7) ---
    # Sync active_tab ↔ tabs.active index
    def _tabs_to_state(event):
        idx = event.new
        if 0 <= idx < len(_TAB_NAMES):
            state.active_tab = _TAB_NAMES[idx]

    tabs.param.watch(_tabs_to_state, "active")

    def _state_tab_to_tabs(event):
        name = event.new
        if name in _TAB_NAMES:
            tabs.active = _TAB_NAMES.index(name)

    state.param.watch(_state_tab_to_tabs, "active_tab")

    # Wire pn.state.location when the server is ready (best-effort)
    def _wire_location():
        try:
            loc = pn.state.location
            if loc is None:
                return
            # Two-way bind: URL ?run=<int> ↔ state.active_run_id
            #               URL ?tab=<str> ↔ state.active_tab
            #               URL ?combo=<hash_str> ↔ state.active_combo_hash (read-only useful)
            loc.sync(state, {"active_run_id": "run",
                             "active_tab": "tab",
                             "active_combo_hash": "combo"})
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

    # Pass a factory function so build_app is re-evaluated per session.
    # This ensures user preferences (e.g. dark mode) are read fresh on each reload.
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

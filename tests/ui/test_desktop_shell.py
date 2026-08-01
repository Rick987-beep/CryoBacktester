"""
tests/ui/test_desktop_shell.py — Unit tests for desktop shell helpers.

Covers InstanceLock, wait_for_healthz, quit confirmation wiring, and
desktop argparse help. Does not start a real WKWebView window.
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from backtester.ui.desktop import InstanceLock, handle_window_closing
from backtester.ui.server_utils import wait_for_healthz


# ── InstanceLock ─────────────────────────────────────────────────────────────

@pytest.mark.skipif(__import__("os").name != "posix", reason="fcntl flock is POSIX")
def test_instance_lock_exclusive(tmp_path):
    path = str(tmp_path / "desktop.lock")
    a = InstanceLock(path)
    b = InstanceLock(path)
    a.acquire()
    with pytest.raises(RuntimeError, match="already running"):
        b.acquire()
    a.release()
    b.acquire()
    b.release()


@pytest.mark.skipif(__import__("os").name != "posix", reason="fcntl flock is POSIX")
def test_instance_lock_reacquire_after_release(tmp_path):
    path = str(tmp_path / "desktop.lock")
    lock = InstanceLock(path)
    lock.acquire()
    lock.release()
    lock.acquire()
    lock.release()


# ── wait_for_healthz ─────────────────────────────────────────────────────────

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            body = json.dumps({"status": "ok", "version": "test"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_args):
        pass


def test_wait_for_healthz_success():
    server = HTTPServer(("127.0.0.1", 0), _HealthHandler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        body = wait_for_healthz(port, timeout_s=5.0)
        assert body["status"] == "ok"
        assert body["version"] == "test"
    finally:
        server.shutdown()


def test_wait_for_healthz_timeout():
    with pytest.raises(TimeoutError, match="healthz not ready"):
        wait_for_healthz(1, timeout_s=0.3)


# ── Quit confirmation ────────────────────────────────────────────────────────

class _FakeRunService:
    def __init__(self, n: int):
        self._n = n
        self.shutdown_calls = 0

    def running_worker_count(self) -> int:
        return self._n

    def shutdown_all(self, timeout_s: float = 2.0) -> None:
        self.shutdown_calls += 1
        self._n = 0


def test_handle_window_closing_no_workers_allows_close():
    svc = _FakeRunService(0)
    assert handle_window_closing(svc, confirm_fn=lambda _m: False) is True
    assert svc.shutdown_calls == 0


def test_handle_window_closing_cancel_keeps_window():
    svc = _FakeRunService(2)
    assert handle_window_closing(svc, confirm_fn=lambda _m: False) is False
    assert svc.shutdown_calls == 0
    assert svc.running_worker_count() == 2


def test_handle_window_closing_confirm_shuts_down():
    svc = _FakeRunService(1)
    assert handle_window_closing(svc, confirm_fn=lambda _m: True) is True
    assert svc.shutdown_calls == 1


def test_handle_window_closing_none_run_service():
    assert handle_window_closing(None, confirm_fn=lambda _m: False) is True


# ── CLI / imports ────────────────────────────────────────────────────────────

def test_desktop_module_imports():
    import backtester.ui.desktop as desktop
    assert callable(desktop.main)
    assert callable(desktop.handle_window_closing)


def test_desktop_help_exits_zero():
    from backtester.ui.desktop import _build_arg_parser
    p = _build_arg_parser()
    args = p.parse_args([])
    assert args.port == 5006


def test_build_app_exposes_run_service():
    from backtester.ui.app import build_app
    from backtester.ui.services.run_service import RunService

    app = build_app()
    assert isinstance(app._cryo_run_service, RunService)


def test_app_main_does_not_use_show_true():
    """Regression: eager show=True caused empty localhost browser tabs."""
    import ast
    import inspect
    import backtester.ui.app as app_mod

    src = inspect.getsource(app_mod.main)
    tree = ast.parse(src)
    serve_shows = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = ""
            if isinstance(func, ast.Attribute):
                name = func.attr
            elif isinstance(func, ast.Name):
                name = func.id
            if name == "serve":
                for kw in node.keywords:
                    if kw.arg == "show":
                        serve_shows.append(ast.literal_eval(kw.value))
    assert serve_shows == [False], f"pn.serve show= expected [False], got {serve_shows}"


def test_ui_url_and_websocket_origins_aligned():
    """Regression: 127.0.0.1 URL + default localhost-only WS → blank UI shell."""
    from backtester.ui.server_utils import UI_HOST, ui_base_url, ui_websocket_origins

    assert UI_HOST == "localhost"
    assert ui_base_url(5006) == "http://localhost:5006/"
    origins = ui_websocket_origins(5006)
    assert "localhost:5006" in origins
    assert "127.0.0.1:5006" in origins


def test_desktop_and_app_pass_websocket_origin():
    """Both entry points must pass websocket_origin into pn.serve."""
    import ast
    import inspect
    import backtester.ui.app as app_mod
    import backtester.ui.desktop as desktop_mod

    def _serve_ws_origins(fn) -> bool:
        src = inspect.getsource(fn)
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = ""
            if isinstance(node.func, ast.Attribute):
                name = node.func.attr
            elif isinstance(node.func, ast.Name):
                name = node.func.id
            if name != "serve":
                continue
            for kw in node.keywords:
                if kw.arg == "websocket_origin":
                    return True
        return False

    assert _serve_ws_origins(app_mod.main)
    assert _serve_ws_origins(desktop_mod.main)


@pytest.mark.slow_ui
def test_panel_session_hydrates_nav_with_localhost_url():
    """pull_session over localhost must see nav RadioButtonGroup (not blank shell)."""
    import threading

    from bokeh.client import pull_session
    import panel as pn

    from backtester.ui.app import _HEALTHZ_ROUTE, _HealthzHandler, build_app
    from backtester.ui.server_utils import (
        UI_HOST,
        ui_base_url,
        ui_websocket_origins,
        wait_for_healthz,
    )

    port = 5133
    template = build_app(state_dir="/tmp/cryo-hydrate-test")
    origins = ui_websocket_origins(port)

    def _serve():
        pn.serve(
            template,
            port=port,
            address="127.0.0.1",
            show=False,
            websocket_origin=origins,
            location=True,
            extra_patterns=[(_HEALTHZ_ROUTE, _HealthzHandler)],
        )

    threading.Thread(target=_serve, daemon=True).start()
    wait_for_healthz(port, timeout_s=20.0, host=UI_HOST)

    session = pull_session(url=ui_base_url(port))
    try:
        found = []
        stack = list(session.document.roots)
        while stack:
            m = stack.pop()
            if type(m).__name__ == "RadioButtonGroup":
                found = list(getattr(m, "labels", None) or [])
                break
            for attr in ("children", "tabs", "center", "header", "main", "sidebar"):
                child = getattr(m, attr, None)
                if child is None:
                    continue
                if isinstance(child, list):
                    stack.extend(child)
                else:
                    stack.append(child)
        assert found == [
            "New Run",
            "Runs",
            "Results Grid",
            "Combo Detail",
            "Favourites",
        ], f"nav not hydrated: {found!r}"
    finally:
        session.close()

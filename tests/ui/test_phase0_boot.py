"""
tests/ui/test_phase0_boot.py — Phase 0 automated tests.

Fast tests (< 1 s each):
  test_ui_module_imports      — import succeeds, build_app() returns a Panel object.
  test_app_has_healthz_handler — _HEALTHZ_ROUTE constant exists and handler is callable.

Slow test (marked slow_ui, skipped by default):
  test_app_boots_on_random_port — starts a real Tornado server, hits /healthz,
                                  confirms 200 + JSON body.
"""
import importlib
import pytest


# ── Fast tests ────────────────────────────────────────────────────────────────

def test_ui_module_imports():
    """backtester.ui.app is importable and build_app() returns a Panel component."""
    import panel as pn
    app_mod = importlib.import_module("backtester.ui.app")
    result = app_mod.build_app()
    # Panel templates are subclasses of pn.template.base.BaseTemplate
    assert isinstance(result, pn.template.base.BaseTemplate), (
        f"build_app() returned {type(result)}, expected a Panel template"
    )


def test_app_has_healthz_handler():
    """_HEALTHZ_ROUTE is defined as a string and _HealthzHandler is importable."""
    from backtester.ui.app import _HEALTHZ_ROUTE, _HealthzHandler
    assert isinstance(_HEALTHZ_ROUTE, str) and _HEALTHZ_ROUTE.startswith("/")
    # Handler must be a class (Tornado RequestHandler subclass)
    from tornado.web import RequestHandler
    assert issubclass(_HealthzHandler, RequestHandler)


# ── Slow / integration test (skipped by default) ─────────────────────────────

@pytest.mark.slow_ui
def test_app_boots_on_random_port():
    """Start a real Panel server on a free port, hit /healthz, then shut down."""
    import json
    import socket
    import threading
    import time
    import urllib.request

    import panel as pn
    from backtester.ui.app import build_app, _HEALTHZ_ROUTE, _HealthzHandler

    # Find a free port
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    app = build_app()

    server_started = threading.Event()
    server_ref = {}

    def _serve():
        srv = pn.serve(
            app,
            port=port,
            show=False,
            threaded=False,
            extra_patterns=[(_HEALTHZ_ROUTE, _HealthzHandler)],
        )
        server_ref["srv"] = srv
        server_started.set()

    t = threading.Thread(target=_serve, daemon=True)
    t.start()

    # Poll until healthz responds (up to 10 s)
    deadline = time.time() + 10
    last_exc = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/healthz", timeout=1
            ) as resp:
                body = json.loads(resp.read())
                assert resp.status == 200
                assert body["status"] == "ok"
                assert "version" in body
                return  # success
        except Exception as exc:
            last_exc = exc
            time.sleep(0.2)

    pytest.fail(f"Healthz endpoint never became available: {last_exc}")

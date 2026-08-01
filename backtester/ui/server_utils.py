"""
server_utils.py — Shared helpers for Panel UI entry points.

wait_for_healthz — poll /healthz until the Tornado server is ready.
ui_base_url / ui_websocket_origins — keep HTTP URL and Bokeh WS Origin in sync.

Panel defaults allow_websocket_origin to ``localhost`` only. Opening
``http://127.0.0.1`` then yields a blank shell (template chrome, no widgets).
Always open ``localhost`` and whitelist both loopback hostnames.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

# Canonical loopback hostname for the Research UI (matches Panel WS default).
UI_HOST = "localhost"


def ui_base_url(port: int, host: str = UI_HOST) -> str:
    """HTTP URL opened by the desktop window / browser."""
    return f"http://{host}:{port}/"


def ui_websocket_origins(port: int) -> list[str]:
    """Origins Bokeh must accept for the Panel WebSocket session.

    Include both loopback names so a stray 127.0.0.1 open still works.
    """
    return [f"localhost:{port}", f"127.0.0.1:{port}"]


def wait_for_healthz(
    port: int,
    timeout_s: float = 30.0,
    host: str = UI_HOST,
) -> dict:
    """Block until ``http://{host}:{port}/healthz`` returns status ok.

    Returns:
        Parsed JSON body from /healthz.

    Raises:
        TimeoutError: if the endpoint is not ready within ``timeout_s``.
    """
    url = f"http://{host}:{port}/healthz"
    deadline = time.monotonic() + timeout_s
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as resp:
                body = json.loads(resp.read().decode("utf-8"))
                if resp.status == 200 and body.get("status") == "ok":
                    return body
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
            last_exc = exc
        time.sleep(0.1)
    raise TimeoutError(
        f"UI healthz not ready at {url} within {timeout_s:.0f}s "
        f"(last error: {last_exc})"
    )

"""
desktop.py — Native desktop shell for the CryoBacktester Research UI.

Starts Panel with show=False, waits for /healthz, then opens a single
pywebview (WKWebView) window. No system browser, no Terminal.app required
when launched via the macOS .app wrapper.

Usage:
    python -m backtester.ui.desktop
    python -m backtester.ui.desktop --port 5007
    python -m backtester.ui.desktop --state-dir /tmp/cryo-ui-state

Quit: if backtest workers are still running, a confirmation dialog is shown.
Cancel keeps the window open; confirm stops workers and exits.
"""
from __future__ import annotations

import argparse
import os
import signal
import socket
import sys
import threading

from backtester.ui.log import get_ui_logger

log = get_ui_logger(__name__)

_DEFAULT_PORT = 5006
_LOCK_NAME = "desktop.lock"


class InstanceLock:
    """Exclusive file lock so only one desktop UI runs at a time.

    Uses fcntl.flock on POSIX. The lock is released automatically if the
    process crashes (kernel drops the flock).
    """

    def __init__(self, lock_path: str):
        self.lock_path = lock_path
        self._fh = None

    def acquire(self) -> None:
        import fcntl

        os.makedirs(os.path.dirname(self.lock_path) or ".", exist_ok=True)
        self._fh = open(self.lock_path, "a+")
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._fh.close()
            self._fh = None
            raise RuntimeError(
                f"CryoBacktester desktop UI is already running "
                f"(lock: {self.lock_path})"
            ) from exc
        self._fh.seek(0)
        self._fh.truncate()
        self._fh.write(str(os.getpid()))
        self._fh.flush()

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            import fcntl
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            self._fh.close()
        except Exception:
            pass
        self._fh = None


def _port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    """True if something already accepts TCP on host:port (IPv4 loopback)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0


def handle_window_closing(run_service, confirm_fn) -> bool:
    """Return True to allow close, False to keep the window open.

    ``confirm_fn(message) -> bool`` is injected so unit tests can mock the
    native dialog without starting WKWebView.
    """
    n = run_service.running_worker_count() if run_service is not None else 0
    if n <= 0:
        return True
    msg = (
        f"{n} backtest(s) still running. Quit anyway?\n\n"
        "Confirming will stop all running workers."
    )
    if not confirm_fn(msg):
        return False
    run_service.shutdown_all()
    return True


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="CryoBacktester Research UI (native desktop window)",
    )
    p.add_argument(
        "--port", type=int, default=_DEFAULT_PORT,
        help=f"Port for the local Panel server (default: {_DEFAULT_PORT})",
    )
    p.add_argument(
        "--state-dir", default=None,
        help="Directory for ui_state.db and the desktop lock file "
             "(default: backtester/ui/state/)",
    )
    p.add_argument(
        "--bundles-root", default=None,
        help="Directory scanned for *.bundle/ dirs (default: data/runs/)",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)

    try:
        import webview
    except ImportError:
        log.error(
            "pywebview is required for the desktop UI. "
            "Install with: pip install pywebview"
        )
        print(
            "error: pywebview is not installed. "
            "Run: pip install pywebview",
            file=sys.stderr,
        )
        return 1

    from backtester.ui.app import (
        _DEFAULT_STATE_DIR,
        _HEALTHZ_ROUTE,
        _HealthzHandler,
        build_app,
    )
    from backtester.ui.server_utils import (
        UI_HOST,
        ui_base_url,
        ui_websocket_origins,
        wait_for_healthz,
    )
    import panel as pn

    state_dir = args.state_dir or _DEFAULT_STATE_DIR
    lock = InstanceLock(os.path.join(state_dir, _LOCK_NAME))
    try:
        lock.acquire()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if _port_in_use(args.port):
        lock.release()
        print(
            f"error: port {args.port} is already in use. "
            f"Stop the other process or pass --port <free-port>.",
            file=sys.stderr,
        )
        return 1

    # build_app once so we hold the RunService for quit / signals
    template = build_app(state_dir=state_dir, bundles_root=args.bundles_root)
    run_service = getattr(template, "_cryo_run_service", None)

    def _on_signal(signum, _frame):
        log.info("desktop: received signal %s — shutting down workers", signum)
        if run_service is not None:
            run_service.shutdown_all()
        lock.release()
        os._exit(0)

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    ws_origins = ui_websocket_origins(args.port)

    def _serve():
        # Bind IPv4 loopback; open UI_HOST (localhost) in the window.
        # websocket_origin must include that host or Bokeh leaves a blank
        # shell (header only, no widgets).
        pn.serve(
            template,
            port=args.port,
            address="127.0.0.1",
            show=False,
            autoreload=False,
            location=True,
            websocket_origin=ws_origins,
            extra_patterns=[(_HEALTHZ_ROUTE, _HealthzHandler)],
        )

    server_thread = threading.Thread(target=_serve, name="panel-serve", daemon=True)
    server_thread.start()

    try:
        wait_for_healthz(args.port, timeout_s=30.0, host=UI_HOST)
    except TimeoutError as exc:
        log.error("desktop: %s", exc)
        print(f"error: {exc}", file=sys.stderr)
        if run_service is not None:
            run_service.shutdown_all()
        lock.release()
        return 1

    url = ui_base_url(args.port)
    log.info("desktop: opening native window at %s (ws origins=%s)", url, ws_origins)

    window = webview.create_window(
        "CryoBacktester",
        url,
        width=1400,
        height=900,
        text_select=True,
    )

    def _confirm(message: str) -> bool:
        # Native modal; returns True if user confirms.
        return bool(window.create_confirmation_dialog("Quit CryoBacktester?", message))

    def _on_closing():
        allow = handle_window_closing(run_service, _confirm)
        if not allow:
            return False  # cancel close
        return True

    window.events.closing += _on_closing

    try:
        webview.start()
    finally:
        if run_service is not None:
            run_service.shutdown_all()
        lock.release()
        log.info("desktop: exited")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

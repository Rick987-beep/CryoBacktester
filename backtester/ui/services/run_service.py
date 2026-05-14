"""
run_service.py — Launch and manage backtest worker subprocesses.

RunHandle   — data class wrapping an in-flight subprocess.
RunService  — spawns workers, tails progress, cancels.
"""
import atexit
import json
import os
import subprocess
import sys
import time
import uuid
from typing import Iterator

from backtester.ui.log import get_ui_logger

log = get_ui_logger(__name__)

_WORKER_MODULE = "backtester.ui.services.run_worker"


class RunHandle:
    """Holds all state for one running (or recently completed) backtest."""

    def __init__(self, proc, progress_path, config_path):
        self.proc = proc
        self.pid = proc.pid
        self.progress_path = progress_path
        self.config_path = config_path
        self._last_pos = 0   # byte offset for tail_progress

    def is_alive(self) -> bool:
        return self.proc.poll() is None

    def exit_code(self):
        return self.proc.poll()


class RunService:
    """Manages backtest worker subprocesses.

    Args:
        store:   StoreService (for registering completed bundles).
        cache:   ResultCache (for loading bundles after registration).
    """

    def __init__(self, store, cache):
        self._store = store
        self._cache = cache
        self._handles: list[RunHandle] = []
        atexit.register(self._cleanup)

    def submit(
        self,
        strategy_key: str,
        param_grid: dict,
        date_range: tuple,
        account_size: float | None = None,
    ) -> RunHandle:
        """Spawn a worker subprocess.

        Args:
            strategy_key: Key in STRATEGIES dict.
            param_grid:   {param: [values]}.
            date_range:   (date_from, date_to) — either may be None.
            account_size: USD account size (default: from config).

        Returns:
            RunHandle for the in-flight run.
        """
        from backtester.config import cfg as _cfg
        from backtester.run import DEFAULT_OPTIONS, DEFAULT_SPOT

        if account_size is None:
            account_size = float(_cfg.simulation.account_size_usd)

        run_id = uuid.uuid4().hex
        tmp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "..", "..", "..", "logs")
        # Normalize to repo root logs/
        _repo_root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))))
        tmp_dir = os.path.join(_repo_root, "logs")
        os.makedirs(tmp_dir, exist_ok=True)

        progress_path = os.path.join(tmp_dir, f"ui-worker-{run_id}-progress.jsonl")
        config_path   = os.path.join(tmp_dir, f"ui-worker-{run_id}-config.json")

        # Resolve bundles_root (same as DEFAULT_BUNDLES_ROOT in app.py)
        _ui_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        _backtester_dir = os.path.dirname(_ui_dir)
        bundles_root = os.path.join(_backtester_dir, "reports")

        date_from, date_to = date_range if date_range else (None, None)

        config = {
            "strategy":      strategy_key,
            "param_grid":    param_grid,
            "date_from":     date_from,
            "date_to":       date_to,
            "account_size":  account_size,
            "bundles_root":  bundles_root,
            "state_dir":     str(self._store._state_dir),
            "progress_path": progress_path,
        }
        with open(config_path, "w") as f:
            json.dump(config, f)

        cmd = [sys.executable, "-m", _WORKER_MODULE, "--config", config_path]
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        log.info("run_service: spawned worker pid=%d strategy=%s", proc.pid, strategy_key)

        handle = RunHandle(proc, progress_path, config_path)
        self._handles.append(handle)
        return handle

    def tail_progress(self, handle: RunHandle) -> Iterator[dict]:
        """Yield new JSON lines from handle.progress_path (file-tail style).

        Each call resumes from where it left off.  Yields as many complete
        lines as are available; yields nothing if no new data.
        """
        if not os.path.exists(handle.progress_path):
            return

        with open(handle.progress_path, "rb") as f:
            f.seek(handle._last_pos)
            data = f.read()
            handle._last_pos += len(data)

        for raw in data.decode("utf-8", errors="replace").splitlines():
            raw = raw.strip()
            if not raw:
                continue
            try:
                yield json.loads(raw)
            except json.JSONDecodeError:
                log.debug("run_service: bad JSON in progress: %r", raw)

    def cancel(self, handle: RunHandle):
        """Send SIGTERM to the worker; SIGKILL after 2 s if still alive."""
        if not handle.is_alive():
            return
        log.info("run_service: cancelling worker pid=%d", handle.pid)
        handle.proc.terminate()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and handle.is_alive():
            time.sleep(0.05)
        if handle.is_alive():
            handle.proc.kill()
            log.warning("run_service: SIGKILL sent to pid=%d", handle.pid)

    def await_result(self, handle: RunHandle) -> int | None:
        """Block until the worker exits.

        Returns the registered run_id on success, None on cancel/error.
        """
        handle.proc.wait()
        for line in self.tail_progress(handle):
            if line.get("status") == "done":
                bundle_path = line.get("bundle_path")
                if bundle_path:
                    try:
                        run_id = self._store.register_bundle(bundle_path)
                        self._cache.get(run_id)
                        return run_id
                    except Exception as exc:
                        log.error("run_service: failed to register bundle: %s", exc)
                return None
        return None

    def _cleanup(self):
        """Terminate all still-running workers on interpreter exit."""
        for handle in self._handles:
            if handle.is_alive():
                try:
                    handle.proc.terminate()
                except Exception:
                    pass

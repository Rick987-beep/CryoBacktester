"""
run_service.py — GUI client of jobd.

New Run enqueues a ``JobSpec`` (same path as ``python -m backtester.run
--detach``). Progress is polled from ``JobStore`` files. Cancel goes through
``QueueClient``. Closing the UI must **not** kill jobs — ``shutdown_all``
only reaps leftover local test children (``submit_cmd``).
"""
from __future__ import annotations

import atexit
import os
import signal
import subprocess
import time
from typing import Iterator

from backtester.job.api import JobSpec, JobStore, JobView, QueueClient
from backtester.ui.log import get_ui_logger

log = get_ui_logger(__name__)


class RunHandle:
    """One in-flight (or recently finished) backtest.

    Either a jobd job (``job_id``) or a local test child (``proc`` from
    ``submit_cmd``).
    """

    def __init__(
        self,
        *,
        proc=None,
        job_id: str | None = None,
        job_store: JobStore | None = None,
    ):
        self.proc = proc
        self.job_id = job_id
        self._job_store = job_store
        self._last_progress_key = None
        self._terminal_emitted = False
        self._pid = proc.pid if proc is not None else None

    @property
    def pid(self):
        if self._pid is not None:
            return self._pid
        view = self._job_view()
        return None if view is None else view.pid

    def _job_view(self) -> JobView | None:
        if self._job_store is None or not self.job_id:
            return None
        return self._job_store.get(self.job_id)

    def is_queued(self) -> bool:
        view = self._job_view()
        return view is not None and view.state == "queued"

    def is_alive(self) -> bool:
        if self.proc is not None:
            return self.proc.poll() is None
        view = self._job_view()
        if view is None:
            return False
        return view.state in ("queued", "running")

    def exit_code(self):
        if self.proc is not None:
            return self.proc.poll()
        view = self._job_view()
        if view is None:
            return None
        if view.state in ("queued", "running"):
            return None
        if view.state == "done":
            return 0
        return 1


class RunService:
    """Enqueue / observe / cancel backtests. Does not own job processes.

    Args:
        store:   StoreService (for registering completed bundles).
        cache:   ResultCache (for loading bundles after registration).
        jobs_root: JobStore root (default: ``jobs_dir()`` / ``CRYOBT_JOBS``).
    """

    def __init__(self, store, cache, jobs_root=None):
        self._store = store
        self._cache = cache
        self._handles: list[RunHandle] = []
        self._client = QueueClient(jobs_root)
        atexit.register(self.shutdown_all)

    def running_worker_count(self) -> int:
        """Local test children plus queued/running jobd jobs.

        Used by the desktop quit dialog. Jobs are **not** stopped on quit.
        """
        return self.local_worker_count() + self.in_flight_job_count()

    def local_worker_count(self) -> int:
        return sum(1 for h in self._handles if h.proc is not None and h.is_alive())

    def in_flight_job_count(self) -> int:
        try:
            snap = self._client.snapshot()
        except Exception as exc:
            log.debug("run_service: snapshot failed: %s", exc)
            return sum(1 for h in self._handles if h.job_id and h.is_alive())
        return len(snap.running) + len(snap.queued)

    def _spawn(self, cmd: list[str]) -> subprocess.Popen:
        """Spawn ``cmd`` in a new session (local test children only)."""
        kwargs: dict = {
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
        }
        if os.name == "posix":
            kwargs["start_new_session"] = True
        return subprocess.Popen(cmd, **kwargs)

    def _stop_handle(self, handle: RunHandle, timeout_s: float = 2.0) -> None:
        """SIGTERM a *local* process group, then SIGKILL if still alive."""
        if handle.proc is None or not handle.is_alive():
            return

        def _signal(sig: int) -> None:
            try:
                if os.name == "posix":
                    os.killpg(handle.pid, sig)
                elif sig == signal.SIGTERM:
                    handle.proc.terminate()
                else:
                    handle.proc.kill()
            except ProcessLookupError:
                return
            except OSError as exc:
                log.debug("run_service: signal %s pid=%d failed: %s", sig, handle.pid, exc)
                try:
                    if sig == signal.SIGTERM:
                        handle.proc.terminate()
                    else:
                        handle.proc.kill()
                except Exception:
                    pass

        _signal(signal.SIGTERM)
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline and handle.is_alive():
            time.sleep(0.05)
        if handle.is_alive():
            log.warning("run_service: SIGKILL sent to pid=%d", handle.pid)
            _signal(signal.SIGKILL)
            try:
                handle.proc.wait(timeout=1.0)
            except Exception:
                pass

    def submit(
        self,
        strategy_key: str,
        param_grid: dict,
        date_range: tuple,
        account_size: float | None = None,
        *,
        options_path: str | None = None,
        spot_path: str | None = None,
        workers: int | None = None,
    ) -> RunHandle:
        """Enqueue a jobd job (same as CLI ``--detach``).

        Args:
            strategy_key: Key in STRATEGIES dict.
            param_grid:   {param: [values]}.
            date_range:   (date_from, date_to) — either may be None.
            account_size: USD account size (default: from config).
            options_path / spot_path: test overrides for tiny parquet.
            workers: inner combo-shard workers (None = auto).

        Returns:
            RunHandle for the in-flight job.
        """
        from backtester.core.config import cfg as _cfg

        if account_size is None:
            account_size = float(_cfg.simulation.account_size_usd)

        date_from, date_to = date_range if date_range else (None, None)
        spec = JobSpec(
            strategy=strategy_key,
            param_grid=param_grid,
            date_from=date_from,
            date_to=date_to,
            account_size=account_size,
            requested_inner_workers=workers,
            options_path=options_path,
            spot_path=spot_path,
            source="ui",
        )
        view = self._client.enqueue(spec)
        log.info(
            "run_service: enqueued job_id=%s strategy=%s",
            view.job_id, strategy_key,
        )
        handle = RunHandle(job_id=view.job_id, job_store=self._client.store)
        self._handles.append(handle)
        return handle

    def submit_cmd(self, cmd: list[str]) -> RunHandle:
        """Spawn an arbitrary command as a tracked *local* child (tests)."""
        proc = self._spawn(cmd)
        handle = RunHandle(proc=proc)
        self._handles.append(handle)
        log.info("run_service: spawned cmd worker pid=%d cmd=%s", proc.pid, cmd[:3])
        return handle

    def adopt_in_flight(self) -> RunHandle | None:
        """Attach the first running (else queued) job so the UI can reconnect."""
        snap = self._client.snapshot()
        views = list(snap.running) + list(snap.queued)
        if not views:
            return None
        job_id = views[0].job_id
        existing = next((h for h in self._handles if h.job_id == job_id), None)
        if existing is not None:
            return existing
        handle = RunHandle(job_id=job_id, job_store=self._client.store)
        self._handles.append(handle)
        log.info("run_service: adopted in-flight job_id=%s", job_id)
        return handle

    def import_finished_jobs(self) -> list[int]:
        """Register recent done job bundles into the UI store (idempotent)."""
        run_ids: list[int] = []
        try:
            snap = self._client.snapshot()
        except Exception as exc:
            log.debug("run_service: import snapshot failed: %s", exc)
            return run_ids
        for view in snap.recent:
            if view.state != "done" or not view.bundle_path:
                continue
            try:
                run_ids.append(self._store.register_bundle(view.bundle_path))
            except Exception as exc:
                log.debug(
                    "run_service: skip import job_id=%s: %s",
                    view.job_id, exc,
                )
        return run_ids

    def tail_progress(self, handle: RunHandle) -> Iterator[dict]:
        """Yield new progress dicts by polling ``status.json``."""
        if handle.job_id:
            yield from self._tail_job(handle)

    def _tail_job(self, handle: RunHandle) -> Iterator[dict]:
        view = handle._job_view()
        if view is None:
            if not handle._terminal_emitted:
                handle._terminal_emitted = True
                yield {"status": "error", "message": "job missing"}
            return
        key = (
            view.state,
            view.phase,
            view.current,
            view.total,
            view.date,
            view.bundle_path,
            view.error,
            view.queue_position,
        )
        if key == handle._last_progress_key:
            return
        handle._last_progress_key = key

        if view.state == "queued":
            pos = view.queue_position
            msg = f"Queued (position {pos})" if pos else "Queued"
            yield {"phase": "queued", "msg": msg}
            return

        if view.phase:
            yield {"phase": view.phase, "msg": view.phase}
        if view.current is not None and view.total is not None:
            yield {
                "current": view.current,
                "total": view.total,
                "date": view.date,
                "ts": view.heartbeat_ts,
            }

        if handle._terminal_emitted:
            return
        if view.state == "done":
            handle._terminal_emitted = True
            yield {"status": "done", "bundle_path": view.bundle_path}
        elif view.state in ("error", "cancelled", "dead"):
            handle._terminal_emitted = True
            status = "cancelled" if view.state == "cancelled" else "error"
            yield {"status": status, "message": view.error or view.state}

    def cancel(self, handle: RunHandle):
        """Cancel a jobd job, or SIGTERM a local test child."""
        if handle.job_id:
            log.info("run_service: cancelling job_id=%s", handle.job_id)
            self._client.cancel(handle.job_id)
            return
        if not handle.is_alive():
            return
        log.info("run_service: cancelling local worker pid=%d", handle.pid)
        self._stop_handle(handle, timeout_s=2.0)

    def shutdown_all(self, timeout_s: float = 2.0) -> None:
        """Reap local test children only. Never cancel jobd jobs."""
        for handle in list(self._handles):
            if handle.proc is None:
                continue
            if handle.is_alive():
                try:
                    log.info("run_service: shutdown_all stopping pid=%d", handle.pid)
                    self._stop_handle(handle, timeout_s=timeout_s)
                except Exception as exc:
                    log.debug(
                        "run_service: shutdown_all error pid=%s: %s",
                        getattr(handle, "pid", "?"), exc,
                    )

    def await_result(self, handle: RunHandle) -> int | None:
        """Block until the job/worker exits.

        Returns the registered run_id on success, None on cancel/error.
        """
        if handle.job_id:
            while handle.is_alive():
                time.sleep(0.05)
            view = handle._job_view()
            if view and view.state == "done" and view.bundle_path:
                try:
                    run_id = self._store.register_bundle(view.bundle_path)
                    self._cache.get(run_id)
                    return run_id
                except Exception as exc:
                    log.error("run_service: failed to register bundle: %s", exc)
                    return None
            return None

        if handle.proc is not None:
            handle.proc.wait()
        return None

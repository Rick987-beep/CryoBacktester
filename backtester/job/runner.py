"""Job process: heartbeat files, then stub sleep or run_backtest, then exit.

Tests set ``CRYOBT_JOB_STUB=1`` and ``CRYOBT_JOB_STUB_SECS`` to a fraction of
a second. Never use a full-grid strategy in unit tests.
"""
from __future__ import annotations

import os
import signal
import sys
import time
from datetime import datetime, timezone

from backtester.core.paths import jobs_dir
from backtester.job.api import JobStore, atomic_write_json

_CANCELLED = False


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _caffeinate(pid: int) -> None:
    if sys.platform != "darwin":
        return
    try:
        import subprocess

        subprocess.Popen(
            ["caffeinate", "-dims", "-w", str(pid)],
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError:
        pass


def _install_cancel(store: JobStore, job_id: str) -> None:
    def _handler(signum, frame):
        global _CANCELLED
        _CANCELLED = True
        store.write_status(
            job_id,
            {"state": "cancelled", "pid": os.getpid(), "heartbeat_ts": _now()},
        )
        store.write_result(job_id, {"state": "cancelled", "bundle_path": None})
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def _heartbeat(store: JobStore, job_id: str, extra: dict | None = None) -> None:
    payload = {
        "state": "running",
        "pid": os.getpid(),
        "heartbeat_ts": _now(),
    }
    if extra:
        payload.update(extra)
    store.write_status(job_id, payload)


def _run_stub(store: JobStore, job_id: str) -> None:
    """Heartbeat until CRYOBT_JOB_STUB_SECS (default 0.5s)."""
    secs = float(os.environ.get("CRYOBT_JOB_STUB_SECS", "0.5"))
    t0 = time.time()
    n = 0
    while time.time() - t0 < secs:
        if _CANCELLED:
            return
        n += 1
        _heartbeat(
            store, job_id,
            {"phase": "backtesting", "current": n, "total": 10, "date": "stub"},
        )
        time.sleep(min(0.05, secs / 5))


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if not args:
        print("usage: python -m backtester.job.runner JOB_ID", file=sys.stderr)
        return 2
    job_id = args[0]
    root = os.environ.get("CRYOBT_JOBS") or str(jobs_dir())
    store = JobStore(root)
    pid = os.getpid()
    _install_cancel(store, job_id)
    _caffeinate(pid)
    atomic_write_json(store.job_dir(job_id) / "runtime.json", {"pid": pid, "started_at": _now()})
    _heartbeat(store, job_id, {"phase": "starting"})
    try:
        if os.environ.get("CRYOBT_JOB_STUB") != "1":
            raise RuntimeError(
                "job runner refusing a real backtest in this build step; "
                "set CRYOBT_JOB_STUB=1 or call run_backtest from E2E later"
            )
        _run_stub(store, job_id)
        if not _CANCELLED:
            store.write_status(
                job_id,
                {"state": "done", "pid": pid, "heartbeat_ts": _now(), "phase": "done"},
            )
            store.write_result(job_id, {"state": "done", "bundle_path": None, "stub": True})
    except SystemExit:
        raise
    except Exception as exc:
        store.write_status(
            job_id,
            {"state": "error", "pid": pid, "heartbeat_ts": _now(), "error": str(exc)},
        )
        store.write_result(job_id, {"state": "error", "error": str(exc), "bundle_path": None})
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

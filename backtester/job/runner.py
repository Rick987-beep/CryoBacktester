"""Job process: heartbeat files, then stub sleep or run_backtest, then exit.

Tests set ``CRYOBT_JOB_STUB=1`` and ``CRYOBT_JOB_STUB_SECS`` to a fraction of
a second. Never use a full-grid strategy in unit tests. E2E uses ``job_smoke``
plus a tiny parquet fixture.
"""
from __future__ import annotations

import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from backtester.core.paths import jobs_dir
from backtester.job.api import JobSpec, JobStore, atomic_write_json, read_json

_CANCELLED = False
_LAST_HB = 0.0
_HB_MIN_INTERVAL = 0.25


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


def _heartbeat(store: JobStore, job_id: str, extra: dict | None = None, *, force: bool = False) -> None:
    global _LAST_HB
    now = time.time()
    if not force and (now - _LAST_HB) < _HB_MIN_INTERVAL:
        return
    _LAST_HB = now
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
            force=True,
        )
        time.sleep(min(0.05, secs / 5))


def _load_spec(store: JobStore, job_id: str) -> JobSpec:
    blob = read_json(store.job_dir(job_id) / "spec.json")
    return JobSpec.from_dict(blob)


def _run_real(store: JobStore, job_id: str):
    # type: (JobStore, str) -> tuple[Path, dict]
    from backtester.job.smoke import register_smoke
    from backtester.run import run_backtest

    register_smoke()
    spec = _load_spec(store, job_id)
    out_root = store.job_dir(job_id) / "out"
    out_root.mkdir(parents=True, exist_ok=True)
    os.environ["CRYOBT_UI_STATE"] = str(store.job_dir(job_id) / "ui_state")
    os.environ.setdefault("CRYOBT_RUNS", str(out_root))

    def progress_cb(current, total, day_iso):
        if _CANCELLED:
            raise KeyboardInterrupt("job cancelled")
        _heartbeat(
            store, job_id,
            {
                "phase": "backtesting",
                "current": int(current),
                "total": int(total),
                "date": str(day_iso),
            },
        )

    def status_cb(phase, msg):
        if _CANCELLED:
            raise KeyboardInterrupt("job cancelled")
        _heartbeat(store, job_id, {"phase": str(phase), "message": str(msg)}, force=True)

    bundle = run_backtest(
        spec.strategy,
        spec.param_grid,
        (spec.date_from, spec.date_to),
        spec.account_size,
        str(out_root),
        options_path=spec.options_path,
        spot_path=spec.spot_path,
        progress_cb=progress_cb,
        status_cb=status_cb,
        source=spec.source or "job",
        workers=spec.requested_inner_workers,
    )
    extra = {}
    meta_path = Path(bundle) / "meta.json"
    if meta_path.is_file():
        meta = read_json(meta_path)
        if meta.get("grid_workers_effective") is not None:
            extra["inner_workers_effective"] = meta["grid_workers_effective"]
    return Path(bundle), extra


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
    _heartbeat(store, job_id, {"phase": "starting"}, force=True)
    try:
        if os.environ.get("CRYOBT_JOB_STUB") == "1":
            _run_stub(store, job_id)
            bundle = None
            extra = {"stub": True}
        else:
            bundle, extra = _run_real(store, job_id)
        if not _CANCELLED:
            payload = {
                "state": "done",
                "pid": pid,
                "heartbeat_ts": _now(),
                "phase": "done",
                "bundle_path": str(bundle) if bundle else None,
            }
            payload.update({k: v for k, v in extra.items() if k != "stub"})
            if extra.get("stub"):
                payload["stub"] = True
            store.write_status(job_id, payload)
            store.write_result(
                job_id,
                {
                    "state": "done",
                    "bundle_path": str(bundle) if bundle else None,
                    **({"stub": True} if extra.get("stub") else {}),
                },
            )
    except KeyboardInterrupt:
        store.write_status(
            job_id,
            {"state": "cancelled", "pid": pid, "heartbeat_ts": _now()},
        )
        store.write_result(job_id, {"state": "cancelled", "bundle_path": None})
        return 0
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

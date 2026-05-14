"""
run_worker.py — Subprocess worker for UI-initiated backtests.

Entry point: python -m backtester.ui.services.run_worker --config <path>

Reads a JSON config from --config file:
    {
        "strategy":      "short_generic",
        "param_grid":    {"delta": [0.24], ...},
        "date_from":     "2025-10-01",   (or null)
        "date_to":       "2025-12-31",   (or null)
        "account_size":  100000.0,
        "bundles_root":  "/path/to/backtester/reports",
        "state_dir":     "/path/to/backtester/ui/state",
        "progress_path": "/tmp/progress_<uuid>.jsonl"
    }

Writes JSON lines to progress_path:
    {"ts": "2025-10-01T09:00:00", "current": 50, "total": 1200, "date": "2025-10-01"}
    ...
    {"status": "done",      "bundle_path": "/path/to/bundle.bundle/"}
    {"status": "error",     "message": "..."}
    {"status": "cancelled"}
"""
import argparse
import json
import logging
import os
import signal
import sys
import time

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backtester.ui.log import get_ui_logger

log = get_ui_logger(__name__)

_CANCELLED = False


def _install_signal_handler(progress_path):
    """Write a 'cancelled' status line and set the global flag on SIGTERM."""
    def _handler(signum, frame):
        global _CANCELLED
        _CANCELLED = True
        _write_line(progress_path, {"status": "cancelled"})
        sys.exit(1)

    signal.signal(signal.SIGTERM, _handler)


def _write_line(path, obj):
    """Append one JSON line to the progress file (best-effort)."""
    try:
        with open(path, "a") as f:
            f.write(json.dumps(obj) + "\n")
            f.flush()
    except Exception as exc:
        log.warning("run_worker: could not write to progress file: %s", exc)


def main():
    parser = argparse.ArgumentParser(description="CryoBacktester run worker")
    parser.add_argument("--config", required=True,
                        help="Path to JSON config file")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)

    progress_path = cfg["progress_path"]
    _install_signal_handler(progress_path)

    strategy_key  = cfg["strategy"]
    param_grid    = cfg["param_grid"]
    date_from     = cfg.get("date_from")
    date_to       = cfg.get("date_to")
    account_size  = float(cfg.get("account_size", 100000.0))
    bundles_root  = cfg["bundles_root"]

    # Worker logs to its own file (per spec §7.9)
    pid = os.getpid()
    log_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)))), "..", "..", "logs"
    )
    # Normalize to repo root logs/
    _repo_root = PROJECT_ROOT
    log_dir = os.path.join(_repo_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    _worker_log = os.path.join(log_dir, f"ui-worker-{pid}.log")
    _file_h = logging.FileHandler(_worker_log)
    _file_h.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s — %(message)s"
    ))
    logging.getLogger().addHandler(_file_h)

    log.info("run_worker pid=%d starting: strategy=%s", pid, strategy_key)

    from datetime import datetime, timezone

    def _progress_cb(current, total, date_iso):
        if _CANCELLED:
            return
        _write_line(progress_path, {
            "ts": datetime.now(timezone.utc).isoformat(),
            "current": current,
            "total": total,
            "date": date_iso,
        })

    try:
        from backtester.run import run_backtest

        bundle_path = run_backtest(
            strategy_key=strategy_key,
            param_grid=param_grid,
            date_range=(date_from, date_to),
            account_size=account_size,
            bundles_root=bundles_root,
            progress_cb=_progress_cb,
            source="ui",
        )

        _write_line(progress_path, {
            "status": "done",
            "bundle_path": str(bundle_path),
        })
        log.info("run_worker pid=%d done: bundle=%s", pid, bundle_path)
        sys.exit(0)

    except Exception as exc:
        log.exception("run_worker pid=%d error", pid)
        _write_line(progress_path, {
            "status": "error",
            "message": str(exc),
        })
        sys.exit(2)


if __name__ == "__main__":
    main()

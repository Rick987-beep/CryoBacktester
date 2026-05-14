"""
log.py — Logging helper for backtester.ui.

Usage:
    from backtester.ui.log import get_ui_logger
    log = get_ui_logger(__name__)

Writes to:
  - stderr (level from CRYOTRADER_UI_LOG_LEVEL env var, default INFO)
  - logs/ui.log (5 MB × 3 rotations, same level)

Worker processes call get_ui_logger with a different name and get
   logs/ui-worker-<pid>.log instead (see run_worker.py).
"""
import logging
import os
from logging.handlers import RotatingFileHandler

_LOG_LEVEL_DEFAULT = "INFO"
_LOGS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "logs",
)
_UI_LOG_FILE = os.path.join(_LOGS_DIR, "ui.log")

# Track whether the root ui logger has already been configured so we don't
# add duplicate handlers on repeated imports (common in Panel's hot-reload).
_configured_names: set = set()


def get_ui_logger(name: str) -> logging.Logger:
    """Return a logger for *name*, configured with file + stderr handlers.

    Safe to call multiple times with the same name — handlers are only added once.
    """
    log = logging.getLogger(name)
    if name in _configured_names:
        return log

    level_str = os.environ.get("CRYOTRADER_UI_LOG_LEVEL", _LOG_LEVEL_DEFAULT).upper()
    level = getattr(logging, level_str, logging.INFO)
    log.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # stderr handler
    if not any(isinstance(h, logging.StreamHandler) for h in log.handlers):
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        sh.setLevel(level)
        log.addHandler(sh)

    # rotating file handler
    os.makedirs(_LOGS_DIR, exist_ok=True)
    if not any(isinstance(h, RotatingFileHandler) for h in log.handlers):
        fh = RotatingFileHandler(_UI_LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3)
        fh.setFormatter(fmt)
        fh.setLevel(level)
        log.addHandler(fh)

    log.propagate = False
    _configured_names.add(name)
    return log

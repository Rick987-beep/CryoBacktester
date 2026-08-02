"""Data-plane path resolution.

Defaults live under ``{repo}/data/``. Override with env vars for trader boxes / NAS.
"""
from __future__ import annotations

import os
from pathlib import Path

_CORE_DIR = Path(__file__).resolve().parent
_BACKTESTER_ROOT = _CORE_DIR.parent
_REPO_ROOT = _BACKTESTER_ROOT.parent


def repo_root() -> Path:
    return _REPO_ROOT


def backtester_root() -> Path:
    return _BACKTESTER_ROOT


def _env_path(*names: str) -> Path | None:
    for name in names:
        raw = os.environ.get(name)
        if raw:
            return Path(raw).expanduser().resolve()
    return None


def market_data_dir() -> Path:
    return _env_path("CRYOBT_MARKET_DATA") or (_REPO_ROOT / "data" / "market")


def kline_cache_dir() -> Path:
    return _env_path("CRYOBT_KLINE_DIR", "CRYOTRADER_KLINE_DIR") or (
        _REPO_ROOT / "data" / "klines"
    )


def macro_calendar_dir() -> Path:
    return _env_path("CRYOBT_MACRO_CALENDAR") or (
        market_data_dir() / "macro" / "economic_events" / "us_scheduled"
    )


def macro_dir() -> Path:
    """Root for synced macro series (DVOL, calendars, …)."""
    return _env_path("CRYOBT_MACRO") or (_REPO_ROOT / "data" / "macro")


def dvol_dir() -> Path:
    """Synced Deribit BTC_DVOL hive partitions (year=/month=.parquet)."""
    return _env_path("CRYOBT_DVOL") or (macro_dir() / "deribit" / "BTC_DVOL")


def tardis_raw_dir() -> Path:
    return _env_path("CRYOBT_TARDIS_RAW") or (_REPO_ROOT / "data" / "tardis_raw")


def runs_dir() -> Path:
    return _env_path("CRYOBT_RUNS") or (_REPO_ROOT / "data" / "runs")


def data_archive_dir() -> Path:
    return _env_path("CRYOBT_DATA_ARCHIVE") or (_REPO_ROOT / "data" / "archive")

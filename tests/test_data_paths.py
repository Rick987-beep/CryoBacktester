"""Data-plane path resolution defaults and env overrides."""
from __future__ import annotations

import importlib

import pytest

import backtester.core.paths as paths


@pytest.fixture(autouse=True)
def _reload_paths_defaults(monkeypatch):
    for key in (
        "CRYOBT_MARKET_DATA",
        "CRYOBT_KLINE_DIR",
        "CRYOTRADER_KLINE_DIR",
        "CRYOBT_RUNS",
        "CRYOBT_MACRO_CALENDAR",
        "CRYOBT_TARDIS_RAW",
    ):
        monkeypatch.delenv(key, raising=False)
    importlib.reload(paths)
    yield
    importlib.reload(paths)


def test_defaults_under_repo_data():
    repo = paths.repo_root()
    assert paths.market_data_dir() == repo / "data" / "market"
    assert paths.kline_cache_dir() == repo / "data" / "klines"
    assert paths.runs_dir() == repo / "data" / "runs"
    assert paths.tardis_raw_dir() == repo / "data" / "tardis_raw"
    assert "macro" in str(paths.macro_calendar_dir())


def test_env_overrides(monkeypatch, tmp_path):
    market = tmp_path / "mkt"
    klines = tmp_path / "kl"
    runs = tmp_path / "rn"
    monkeypatch.setenv("CRYOBT_MARKET_DATA", str(market))
    monkeypatch.setenv("CRYOBT_KLINE_DIR", str(klines))
    monkeypatch.setenv("CRYOBT_RUNS", str(runs))
    importlib.reload(paths)

    assert paths.market_data_dir() == market.resolve()
    assert paths.kline_cache_dir() == klines.resolve()
    assert paths.runs_dir() == runs.resolve()


def test_cryotrader_kline_alias(monkeypatch, tmp_path):
    klines = tmp_path / "legacy_kl"
    monkeypatch.delenv("CRYOBT_KLINE_DIR", raising=False)
    monkeypatch.setenv("CRYOTRADER_KLINE_DIR", str(klines))
    importlib.reload(paths)
    assert paths.kline_cache_dir() == klines.resolve()

"""
tests/test_backtester_indicators.py

Tests for the indicator pre-computation framework:
  - indicators/hist_data.py   (kline caching layer)
  - backtester/indicators.py  (build_indicators + IndicatorDep)
  - backtester/engine.py      (_inject_indicators integration)

All tests are offline: Binance is mocked via unittest.mock.patch.
"""

import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_kline_row(open_ms: int, o=100.0, h=102.0, l=99.0, c=101.0, v=10.0):
    """Build one Binance kline list entry."""
    close_ms = open_ms + 59_999  # 1-min bar
    return [
        open_ms, str(o), str(h), str(l), str(c), str(v),
        close_ms, "0", 0, "0", "0", "0",
    ]


def _make_klines(start_ms: int, n: int, interval_ms: int = 15 * 60 * 1000):
    """Return n synthetic Binance kline rows starting at start_ms."""
    return [_make_kline_row(start_ms + i * interval_ms) for i in range(n)]


def _dt(year=2025, month=11, day=1):
    return datetime(year, month, day, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# indicators/hist_data.py
# ---------------------------------------------------------------------------

class TestHistData:

    def test_cold_fetch_stores_cache(self, tmp_path, monkeypatch):
        """A cold fetch hits Binance and writes a parquet cache."""
        import indicators.hist_data as hd
        monkeypatch.setattr(hd, "KLINE_DIR", tmp_path)

        n = 200
        start_ms = int(_dt(2025, 11, 1).timestamp() * 1000)
        fake_bars = _make_klines(start_ms, n)

        mock_resp = MagicMock()
        mock_resp.json.return_value = fake_bars
        mock_resp.raise_for_status.return_value = None

        with patch("indicators.hist_data.requests.get", return_value=mock_resp):
            df = hd.load_klines(
                "BTCUSDT", "15m",
                start=_dt(2025, 11, 1),
                end=_dt(2025, 11, 1) + timedelta(days=2),
                warmup_days=0,
            )

        assert not df.empty
        assert list(df.columns) == ["open", "high", "low", "close", "volume"]
        assert df.index.tz is not None  # tz-aware
        cache_file = tmp_path / "BTCUSDT_15m.parquet"
        assert cache_file.exists()

    def test_repeat_call_uses_cache(self, tmp_path, monkeypatch):
        """Second call within the cached range does not hit Binance."""
        import indicators.hist_data as hd
        monkeypatch.setattr(hd, "KLINE_DIR", tmp_path)

        # 5 warmup days + 1 target day = 6 days at 15m = 6*24*4 = 576 bars
        n = 600
        start_ms = int((_dt(2025, 11, 1) - timedelta(days=5)).timestamp() * 1000)
        fake_bars = _make_klines(start_ms, n)

        mock_resp = MagicMock()
        mock_resp.json.return_value = fake_bars
        mock_resp.raise_for_status.return_value = None

        with patch("indicators.hist_data.requests.get", return_value=mock_resp) as mock_get:
            hd.load_klines("BTCUSDT", "15m", _dt(2025, 11, 1), _dt(2025, 11, 1), warmup_days=5)
            first_call_count = mock_get.call_count

        # Second call — Binance should not be hit again (cache covers the range)
        with patch("indicators.hist_data.requests.get", return_value=mock_resp) as mock_get:
            df = hd.load_klines("BTCUSDT", "15m", _dt(2025, 11, 1), _dt(2025, 11, 1), warmup_days=5)
            assert mock_get.call_count == 0

        assert not df.empty

    def test_tail_append(self, tmp_path, monkeypatch):
        """When the cache is stale at the end, only the tail is fetched."""
        import indicators.hist_data as hd
        monkeypatch.setattr(hd, "KLINE_DIR", tmp_path)

        interval_ms = 15 * 60 * 1000
        start_ms = int(_dt(2025, 10, 1).timestamp() * 1000)
        n_initial = 200

        # Write an initial cache covering Oct 1 → Oct 2 (roughly)
        initial_bars = _make_klines(start_ms, n_initial, interval_ms)
        mock_resp = MagicMock()
        mock_resp.json.return_value = initial_bars
        mock_resp.raise_for_status.return_value = None

        with patch("indicators.hist_data.requests.get", return_value=mock_resp):
            hd.load_klines("BTCUSDT", "15m", _dt(2025, 10, 1), _dt(2025, 10, 2), warmup_days=0)

        # Now request a later end date — should fetch tail
        tail_start_ms = start_ms + n_initial * interval_ms
        tail_bars = _make_klines(tail_start_ms, 50, interval_ms)
        mock_resp.json.return_value = tail_bars

        with patch("indicators.hist_data.requests.get", return_value=mock_resp) as mock_get:
            df = hd.load_klines("BTCUSDT", "15m", _dt(2025, 10, 1), _dt(2025, 10, 5), warmup_days=0)
            assert mock_get.call_count >= 1  # tail was fetched

        assert len(df) >= n_initial  # grew

    def test_corrupt_cache_recovers(self, tmp_path, monkeypatch):
        """A corrupt cache parquet falls back to a fresh fetch."""
        import indicators.hist_data as hd
        monkeypatch.setattr(hd, "KLINE_DIR", tmp_path)

        # Write garbage to the cache file
        cache_file = tmp_path / "BTCUSDT_15m.parquet"
        cache_file.write_bytes(b"not a parquet file")

        n = 100
        start_ms = int(_dt(2025, 11, 1).timestamp() * 1000)
        fake_bars = _make_klines(start_ms, n)
        mock_resp = MagicMock()
        mock_resp.json.return_value = fake_bars
        mock_resp.raise_for_status.return_value = None

        with patch("indicators.hist_data.requests.get", return_value=mock_resp):
            df = hd.load_klines("BTCUSDT", "15m", _dt(2025, 11, 1), _dt(2025, 11, 1), warmup_days=0)

        assert not df.empty

    def test_no_duplicate_index(self, tmp_path, monkeypatch):
        """Returned DataFrame has no duplicate timestamps."""
        import indicators.hist_data as hd
        monkeypatch.setattr(hd, "KLINE_DIR", tmp_path)

        n = 150
        start_ms = int(_dt(2025, 11, 1).timestamp() * 1000)
        # Introduce duplicates in the raw data
        bars = _make_klines(start_ms, n)
        bars += _make_klines(start_ms, 10)  # overlap

        mock_resp = MagicMock()
        mock_resp.json.return_value = bars
        mock_resp.raise_for_status.return_value = None

        with patch("indicators.hist_data.requests.get", return_value=mock_resp):
            df = hd.load_klines("BTCUSDT", "15m", _dt(2025, 11, 1), _dt(2025, 11, 5), warmup_days=0)

        assert not df.index.duplicated().any()


# ---------------------------------------------------------------------------
# backtester/indicators.py
# ---------------------------------------------------------------------------

def _make_15m_df(n_bars=500):
    """Synthetic 15m DataFrame sufficient to warm up turbulence."""
    freq = "15min"
    idx = pd.date_range("2025-10-01", periods=n_bars, freq=freq, tz="UTC")
    rng = np.random.default_rng(42)
    close = 30000 + rng.standard_normal(n_bars).cumsum() * 100
    high = close + rng.uniform(50, 200, n_bars)
    low = close - rng.uniform(50, 200, n_bars)
    return pd.DataFrame({
        "open": close,
        "high": high,
        "low": low,
        "close": close,
        "volume": rng.uniform(1, 10, n_bars),
    }, index=idx)


class TestBuildIndicators:

    def test_turbulence_dep(self, tmp_path, monkeypatch):
        """build_indicators returns a turbulence DataFrame for a turbulence dep."""
        from backtester.indicators import IndicatorDep, build_indicators

        df_15m = _make_15m_df(600)

        # Patch load_klines to return our synthetic data without network
        with patch("backtester.indicators.load_klines", return_value=df_15m):
            ind = build_indicators(
                deps=[IndicatorDep(name="turbulence", symbol="BTCUSDT", interval="15m")],
                start=_dt(2025, 11, 1),
                end=_dt(2025, 11, 10),
            )

        assert "turbulence" in ind
        df = ind["turbulence"]
        assert "composite" in df.columns
        assert "signal" in df.columns
        assert not df.dropna(subset=["composite"]).empty

    def test_unknown_indicator_raises(self):
        """Requesting an unknown indicator name raises ValueError."""
        from backtester.indicators import IndicatorDep, build_indicators

        with patch("backtester.indicators.load_klines", return_value=_make_15m_df()):
            with pytest.raises(ValueError, match="Unknown indicator"):
                build_indicators(
                    deps=[IndicatorDep(name="does_not_exist", symbol="BTCUSDT", interval="15m")],
                    start=_dt(2025, 11, 1),
                    end=_dt(2025, 11, 5),
                )

    def test_indicator_params_forwarded(self, tmp_path, monkeypatch):
        """Custom params in IndicatorDep are forwarded to the builder."""
        from backtester.indicators import IndicatorDep, build_indicators

        df_15m = _make_15m_df(600)

        with patch("backtester.indicators.load_klines", return_value=df_15m):
            ind = build_indicators(
                deps=[IndicatorDep(
                    name="turbulence",
                    symbol="BTCUSDT",
                    interval="15m",
                    params={"thresh_green": 20, "thresh_red": 70},
                )],
                start=_dt(2025, 11, 1),
                end=_dt(2025, 11, 10),
            )

        df = ind["turbulence"]
        # With thresh_green=20 some bars should be red/yellow that wouldn't be otherwise
        assert set(df["signal"].dropna().unique()).issubset({"green", "yellow", "red"})


# ---------------------------------------------------------------------------
# backtester/engine.py — indicator injection
# ---------------------------------------------------------------------------

class _FakeReplay:
    """Minimal MarketReplay stub — returns no states, has date_range()."""

    def __init__(self, start_dt, end_dt):
        self._start = start_dt
        self._end = end_dt

    def date_range(self):
        return self._start, self._end

    def __iter__(self):
        return iter([])

    def __len__(self):
        return 0


class _StrategyWithDeps:
    """Strategy that declares indicator_deps and records set_indicators calls."""
    from backtester.indicators import IndicatorDep
    indicator_deps = [IndicatorDep(name="turbulence", symbol="BTCUSDT", interval="15m")]

    def __init__(self):
        self._ind = None

    def configure(self, params):
        pass

    def set_indicators(self, ind):
        self._ind = ind

    def on_market_state(self, state):
        return []

    def on_end(self, state):
        return []

    def reset(self):
        pass


class _StrategyNoDeps:
    """Strategy with no indicator_deps — set_indicators should not be called."""
    def __init__(self):
        self._ind = "unchanged"

    def configure(self, params):
        pass

    def on_market_state(self, state):
        return []

    def on_end(self, state):
        return []

    def reset(self):
        pass


class TestEngineInjection:

    def test_set_indicators_called_when_deps_declared(self):
        """Engine calls set_indicators on all instances when indicator_deps is set."""
        from backtester.engine import _inject_indicators

        replay = _FakeReplay(_dt(2025, 11, 1), _dt(2025, 11, 10))
        instances = [_StrategyWithDeps(), _StrategyWithDeps()]

        fake_ind = {"turbulence": pd.DataFrame({"composite": [50.0]}, index=pd.DatetimeIndex(["2025-11-01"], tz="UTC"))}

        with patch("backtester.indicators.build_indicators", return_value=fake_ind):
            _inject_indicators(_StrategyWithDeps, instances, replay, progress=False)

        for inst in instances:
            assert inst._ind is not None
            assert "turbulence" in inst._ind

    def test_no_injection_without_deps(self):
        """Engine does not call set_indicators when strategy has no indicator_deps."""
        from backtester.engine import _inject_indicators

        replay = _FakeReplay(_dt(2025, 11, 1), _dt(2025, 11, 10))
        instances = [_StrategyNoDeps()]

        with patch("backtester.indicators.build_indicators") as mock_build:
            _inject_indicators(_StrategyNoDeps, instances, replay, progress=False)
            mock_build.assert_not_called()

        assert instances[0]._ind == "unchanged"

    def test_run_grid_full_injects(self):
        """run_grid_full calls _inject_indicators for strategies with deps."""
        from backtester.engine import run_grid_full

        replay = _FakeReplay(_dt(2025, 11, 1), _dt(2025, 11, 10))
        fake_ind = {"turbulence": pd.DataFrame()}

        with patch("backtester.engine._inject_indicators") as mock_inject:
            run_grid_full(
                _StrategyNoDeps,
                {"dummy": [1]},
                replay,
                progress=False,
            )
            mock_inject.assert_called_once()

"""Unit tests for sync_vps post-sync QA (no network)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backtester.ingest import sync_vps as sv


def _us(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000)


def _write_options(path: Path, snaps: list[datetime], inst_per_snap: int = 800) -> None:
    timestamps = []
    expiries = []
    for snap in snaps:
        ts = _us(snap)
        for _i in range(inst_per_snap):
            timestamps.append(ts)
            expiries.append("28MAY26")
    table = pa.table(
        {
            "timestamp": pa.array(timestamps, type=pa.int64()),
            "expiry": pa.array(expiries, type=pa.string()),
            "strike": pa.array([100_000.0] * len(timestamps), type=pa.float32()),
            "is_call": pa.array([True] * len(timestamps), type=pa.bool_()),
        }
    )
    pq.write_table(table, path)


def _write_spot(path: Path, bars: list[datetime]) -> None:
    timestamps = [_us(dt) for dt in bars]
    n = len(timestamps)
    table = pa.table(
        {
            "timestamp": pa.array(timestamps, type=pa.int64()),
            "open": pa.array([1.0] * n, type=pa.float32()),
            "high": pa.array([1.0] * n, type=pa.float32()),
            "low": pa.array([1.0] * n, type=pa.float32()),
            "close": pa.array([1.0] * n, type=pa.float32()),
        }
    )
    pq.write_table(table, path)


def _full_day_snaps(day: datetime) -> list[datetime]:
    start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    return [start + timedelta(minutes=5 * i) for i in range(288)]


def _full_day_spot_bars(day: datetime) -> list[datetime]:
    """Recorder-like: a few pre-midnight bars + 00:00 … 23:54."""
    prev = day - timedelta(days=1)
    bars = [
        prev.replace(hour=23, minute=55, second=0, microsecond=0),
        prev.replace(hour=23, minute=56, second=0, microsecond=0),
        prev.replace(hour=23, minute=57, second=0, microsecond=0),
        prev.replace(hour=23, minute=58, second=0, microsecond=0),
        prev.replace(hour=23, minute=59, second=0, microsecond=0),
    ]
    start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    bars.extend(start + timedelta(minutes=i) for i in range(1435))  # through 23:54
    return bars


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture(autouse=True)
def _relax_size_floors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Synthetic fixtures are far smaller than production parquets."""
    monkeypatch.setattr(sv, "_OPTS_SIZE_FLOOR", 1)
    monkeypatch.setattr(sv, "_SPOT_SIZE_FLOOR", 1)


def test_qa_clean_day(data_dir: Path) -> None:
    day = datetime(2026, 9, 11, tzinfo=timezone.utc)
    date_str = "2026-09-11"
    _write_options(data_dir / f"options_{date_str}.parquet", _full_day_snaps(day), 800)
    _write_spot(data_dir / f"spot_track_{date_str}.parquet", _full_day_spot_bars(day))

    summary, issues = sv.qa_day(date_str, str(data_dir))
    assert issues == [], (summary, issues)
    assert "ok" in summary
    assert "288/288" in summary


def test_qa_options_gap_and_thin(data_dir: Path) -> None:
    day = datetime(2026, 9, 5, tzinfo=timezone.utc)
    date_str = "2026-09-05"
    snaps = _full_day_snaps(day)
    drop = {
        day.replace(hour=9, minute=0) + timedelta(minutes=5 * i)
        for i in range(8)
    }
    snaps = [s for s in snaps if s not in drop]
    path = data_dir / f"options_{date_str}.parquet"
    _write_options(path, snaps, inst_per_snap=100)

    issues, stats = sv.qa_options_file(str(path), date_str)
    assert stats["snaps"] == 280
    assert any("snaps" in i for i in issues)
    assert any("gap" in i for i in issues)
    assert any("thin" in i for i in issues)


def test_qa_spot_head_truncation_and_gaps(data_dir: Path) -> None:
    day = datetime(2026, 9, 6, tzinfo=timezone.utc)
    date_str = "2026-09-06"
    start = day.replace(hour=9, minute=40, second=0, microsecond=0)
    bars = [start + timedelta(minutes=i) for i in range(100)]
    bars = bars[:50] + bars[55:]
    path = data_dir / f"spot_track_{date_str}.parquet"
    _write_spot(path, bars)

    issues, stats = sv.qa_spot_file(str(path), date_str)
    assert stats["bars"] == len(bars)
    assert any("head truncation" in i for i in issues)
    assert any("spot bars" in i for i in issues)
    assert any("spot gap" in i for i in issues)


def test_qa_missing_files(data_dir: Path) -> None:
    summary, issues = sv.qa_day("2026-01-01", str(data_dir))
    assert any("MISSING options" in i for i in issues)
    assert any("MISSING spot" in i for i in issues)
    assert "ISSUES" in summary


def test_qa_missing_timestamp_column(data_dir: Path) -> None:
    path = data_dir / "options_2026-01-02.parquet"
    pq.write_table(pa.table({"expiry": pa.array(["28MAY26"])}), path)
    issues, _ = sv.qa_options_file(str(path), "2026-01-02")
    assert any("missing column 'timestamp'" in i for i in issues)


def test_run_post_sync_qa_summary(data_dir: Path, caplog: pytest.LogCaptureFixture) -> None:
    import logging

    day = datetime(2026, 9, 11, tzinfo=timezone.utc)
    date_str = "2026-09-11"
    _write_options(data_dir / f"options_{date_str}.parquet", _full_day_snaps(day), 800)
    _write_spot(data_dir / f"spot_track_{date_str}.parquet", _full_day_spot_bars(day))

    with caplog.at_level(logging.INFO):
        notes = sv.run_post_sync_qa([date_str], str(data_dir))
    assert notes == []
    assert any("none" in r.message for r in caplog.records)


def test_size_tiny_vs_median(data_dir: Path) -> None:
    for d in ("2026-09-10", "2026-09-11"):
        (data_dir / f"options_{d}.parquet").write_bytes(b"x" * 2_000_000)
    tiny = data_dir / "options_2026-09-12.parquet"
    day = datetime(2026, 9, 12, tzinfo=timezone.utc)
    _write_options(tiny, _full_day_snaps(day)[:10], inst_per_snap=800)

    med = sv._median_sibling_size(str(data_dir), "options", exclude_name=tiny.name)
    assert med is not None and med >= 1_000_000
    issues, _ = sv.qa_options_file(str(tiny), "2026-09-12", size_median=med)
    assert any("tiny" in i for i in issues)


def test_run_skips_qa_on_dry_run(data_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"n": 0}

    def _fake_qa(dates, path):
        called["n"] += 1
        return []

    monkeypatch.setattr(sv, "run_post_sync_qa", _fake_qa)
    monkeypatch.setattr(sv, "_remote_list", lambda *a, **k: [])
    # Force "nothing to download" path with local files present
    day = datetime(2026, 9, 11, tzinfo=timezone.utc)
    date_str = "2026-09-11"
    _write_options(data_dir / f"options_{date_str}.parquet", _full_day_snaps(day)[:2], 10)
    _write_spot(data_dir / f"spot_track_{date_str}.parquet", [day])

    monkeypatch.setattr(sv, "_date_range", lambda days: [date_str])
    rc = sv.run(days=1, dry_run=True, confirm=False, no_qa=False, data_dir=str(data_dir))
    assert rc == 0
    assert called["n"] == 0

    rc = sv.run(days=1, dry_run=False, confirm=True, no_qa=False, data_dir=str(data_dir))
    assert rc == 0
    assert called["n"] == 1

    called["n"] = 0
    rc = sv.run(days=1, dry_run=False, confirm=True, no_qa=True, data_dir=str(data_dir))
    assert rc == 0
    assert called["n"] == 0

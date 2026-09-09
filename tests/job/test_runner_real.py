"""In-process real runner: job_smoke + tiny parquet. No jobd. No full grids."""
from __future__ import annotations

from pathlib import Path

from backtester.job.api import JobSpec, JobStore
from backtester.job.runner import main as runner_main
from tests.test_engine_workers_parity import _write_tiny_replay


def test_real_runner_writes_bundle(tmp_path, monkeypatch):
    monkeypatch.delenv("CRYOBT_JOB_STUB", raising=False)
    monkeypatch.setenv("CRYOBT_JOBS", str(tmp_path))
    monkeypatch.setenv("CRYOBT_JOB_SMOKE_HOLD", "0")
    mkt = tmp_path / "mkt"
    mkt.mkdir()
    opt, spot = _write_tiny_replay(mkt)
    store = JobStore(tmp_path)
    job_id = "smoke1"
    store.write_spec(
        job_id,
        JobSpec(
            strategy="job_smoke",
            param_grid={"x": list(range(8))},
            account_size=100_000.0,
            requested_inner_workers=1,
            options_path=opt,
            spot_path=spot,
            source="test",
        ),
    )
    rc = runner_main([job_id])
    assert rc == 0
    view = store.get(job_id)
    assert view is not None
    assert view.state == "done"
    assert view.bundle_path
    bundle = Path(view.bundle_path)
    assert bundle.is_dir()
    assert (bundle / "meta.json").is_file()
    assert (bundle / "trade_log.parquet").is_file()
    assert (tmp_path / job_id / "ui_state").exists() or (
        tmp_path / job_id / "out"
    ).is_dir()
    # 8 combos × 1 trade each
    import pandas as pd

    trades = pd.read_parquet(bundle / "trade_log.parquet")
    assert len(trades) == 8


def test_real_runner_unknown_strategy_errors(tmp_path, monkeypatch):
    monkeypatch.delenv("CRYOBT_JOB_STUB", raising=False)
    monkeypatch.setenv("CRYOBT_JOBS", str(tmp_path))
    store = JobStore(tmp_path)
    job_id = "bad1"
    store.write_spec(
        job_id,
        JobSpec(strategy="does_not_exist", param_grid={"x": [0]}, source="test"),
    )
    rc = runner_main([job_id])
    assert rc == 1
    view = store.get(job_id)
    assert view.state == "error"
    assert view.bundle_path is None
    assert view.error

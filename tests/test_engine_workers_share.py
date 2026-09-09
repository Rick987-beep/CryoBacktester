"""A3: shared read-only MarketReplay backing across parent and spawn children."""
from __future__ import annotations

import numpy as np
import pytest

from backtester.core.grid_workers import pack_replay_shared, unlink_replay_shared
from backtester.core.market_replay import MarketReplay
from tests.test_engine_workers_parity import (
    _ParamPnlStrategy,
    _assert_same_grid,
    _write_tiny_replay,
)


def test_pack_attach_writeable_and_same_values(tmp_path):
    opt, spot = _write_tiny_replay(tmp_path)
    parent = MarketReplay(opt, spot)
    assert parent._opt_bid.flags.writeable is False

    meta, holders = pack_replay_shared(parent)
    try:
        child = MarketReplay.from_shared_meta(meta)
        assert parent._opt_bid.flags.writeable is False
        assert child._opt_bid.flags.writeable is False
        np.testing.assert_array_equal(parent._opt_bid, child._opt_bid)
        np.testing.assert_array_equal(parent._spot_close, child._spot_close)
        assert meta["arrays"]["_opt_bid"]["name"]
        with pytest.raises(ValueError):
            child._opt_bid[0] = 9.0
        with pytest.raises(ValueError):
            parent._opt_bid[0] = 9.0
        assert float(child._opt_bid[0]) == float(parent._opt_bid[0])
    finally:
        unlink_replay_shared(holders)
    # Parent must still own its arrays after shm is unlinked (CLI reads time_range).
    assert float(parent._opt_bid[0]) == pytest.approx(0.01)
    start, end = parent.time_range
    assert start is not None and end is not None
    assert len(parent._timestamps) == 8


def test_spawn_uses_shared_not_parquet_reload(tmp_path, monkeypatch):
    from backtester.core.engine import run_grid_full
    import backtester.core.engine as eng

    opt, spot = _write_tiny_replay(tmp_path)
    replay = MarketReplay(opt, spot)
    monkeypatch.setattr(eng, "_effective_inner_workers", lambda *a, **k: 2)

    real_init = MarketReplay.__init__

    def boom(self, *a, **k):
        raise AssertionError("child must not reload parquet when sharing")

    monkeypatch.setattr(MarketReplay, "__init__", boom)
    df, keys, *_ = run_grid_full(
        _ParamPnlStrategy,
        {"x": list(range(16))},
        replay,
        progress=False,
        workers=2,
    )
    assert len(keys) == 16
    assert list(df["pnl"].astype(int)) == list(range(16))
    monkeypatch.setattr(MarketReplay, "__init__", real_init)


def test_shared_spawn_matches_workers_1(tmp_path, monkeypatch):
    from backtester.core.engine import run_grid_full
    import backtester.core.engine as eng

    opt, spot = _write_tiny_replay(tmp_path)
    grid = {"x": list(range(16))}
    ref = run_grid_full(
        _ParamPnlStrategy, grid, MarketReplay(opt, spot), progress=False, workers=1,
    )
    monkeypatch.setattr(eng, "_effective_inner_workers", lambda *a, **k: 2)
    replay = MarketReplay(opt, spot)
    orig_ts0 = int(replay._timestamps[0])
    got = run_grid_full(
        _ParamPnlStrategy, grid, replay, progress=False, workers=2,
    )
    _assert_same_grid(ref, got)
    assert int(replay._timestamps[0]) == orig_ts0
    assert len(replay.time_range) == 2

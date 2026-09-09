"""Spawn handover: progress, cancel/timeout, no leftover children, parent replay."""
from __future__ import annotations

import multiprocessing
import os
import time

import pytest

from backtester.core.engine import run_grid_full
from backtester.core.market_replay import MarketReplay
from tests.test_engine_workers_parity import _ParamPnlStrategy, _write_tiny_replay


class _HangStrategy:
    """Sleeps forever on combo x=0 so the parent timeout can fire."""

    name = "hang_shard"

    def configure(self, params):
        self.x = int(params["x"])

    def on_market_state(self, state):
        if self.x == 0:
            time.sleep(120)
        return []

    def on_end(self, state):
        return []

    def reset(self):
        pass

    def describe_params(self):
        return {"x": self.x}


def test_stdin_parent_refuses_spawn(monkeypatch):
    import sys
    import types

    import backtester.core.engine as eng

    fake = types.ModuleType("__main__")
    monkeypatch.setitem(sys.modules, "__main__", fake)
    with pytest.raises(RuntimeError, match="cannot spawn"):
        eng._assert_spawnable_parent()


def test_spawn_progress_cb_from_shard0(tmp_path, monkeypatch):
    import backtester.core.engine as eng

    opt, spot = _write_tiny_replay(tmp_path)
    monkeypatch.setattr(eng, "_effective_inner_workers", lambda *a, **k: 2)
    seen = []

    def _cb(current, total, day):
        seen.append((int(current), int(total), str(day)))

    df, keys, *_ = run_grid_full(
        _ParamPnlStrategy,
        {"x": list(range(16))},
        MarketReplay(opt, spot),
        progress=False,
        progress_cb=_cb,
        progress_cb_interval=1,
        workers=2,
    )
    assert len(keys) == 16
    assert seen, "parent must receive progress from shard 0"
    assert seen[-1][0] == seen[-1][1]
    assert seen[-1][1] > 0


def test_shard_timeout_terminates_workers(tmp_path, monkeypatch):
    import backtester.core.engine as eng

    opt, spot = _write_tiny_replay(tmp_path)
    monkeypatch.setattr(eng, "_effective_inner_workers", lambda *a, **k: 2)
    monkeypatch.setenv("CRYOBT_GRID_SHARD_TIMEOUT", "2")
    t0 = time.perf_counter()
    with pytest.raises(TimeoutError, match="still running"):
        run_grid_full(
            _HangStrategy,
            {"x": list(range(16))},
            MarketReplay(opt, spot),
            progress=False,
            workers=2,
        )
    elapsed = time.perf_counter() - t0
    assert elapsed < 15, f"timeout path hung ({elapsed:.1f}s)"
    time.sleep(0.4)
    leftover = [p for p in multiprocessing.active_children() if p.is_alive()]
    assert leftover == [], leftover


def test_child_raise_leaves_no_workers(tmp_path, monkeypatch):
    import backtester.core.engine as eng
    from tests.test_engine_workers_parity import _BoomStrategy

    opt, spot = _write_tiny_replay(tmp_path)
    monkeypatch.setattr(eng, "_effective_inner_workers", lambda *a, **k: 2)
    with pytest.raises(Exception, match="intentional shard failure"):
        run_grid_full(
            _BoomStrategy,
            {"x": list(range(16))},
            MarketReplay(opt, spot),
            progress=False,
            workers=2,
        )
    time.sleep(0.4)
    leftover = [p for p in multiprocessing.active_children() if p.is_alive()]
    assert leftover == [], leftover

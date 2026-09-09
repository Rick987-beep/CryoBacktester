"""A5: inner workers=4 should be ≥2× workers=1 on a CPU-bound mid grid.

Skipped unless CRYOBT_RUN_PERF=1. Not required on every CI runner.
"""
from __future__ import annotations

import os
import time

import pytest

from backtester.core.engine import run_grid_full
from backtester.core.market_replay import MarketReplay
from tests.test_engine_workers_parity import _write_tiny_replay


class _CpuBurnStrategy:
    """Module-level picklable burner — GIL-bound Python work per tick."""

    name = "cpu_burn"
    BURN = 80_000

    def configure(self, params):
        self.x = int(params["x"])
        self._acc = 0.0

    def reset(self):
        self._acc = 0.0

    def describe_params(self):
        return {"x": self.x}

    def on_market_state(self, state):
        acc = 0.0
        x = self.x
        for i in range(self.BURN):
            acc += (i * x) % 17
        self._acc = acc
        return []

    def on_end(self, state):
        return []


@pytest.mark.perf
def test_four_workers_speedup_at_least_2x(tmp_path, monkeypatch):
    if os.environ.get("CRYOBT_RUN_PERF") != "1":
        pytest.skip("set CRYOBT_RUN_PERF=1 to run the inner-worker perf gate")
    if (os.cpu_count() or 1) < 4:
        pytest.skip("need ≥4 logical CPUs")

    import backtester.core.engine as eng

    opt, spot = _write_tiny_replay(tmp_path, n_ticks=24)
    grid = {"x": list(range(64))}
    replay_1 = MarketReplay(opt, spot)
    t0 = time.perf_counter()
    run_grid_full(_CpuBurnStrategy, grid, replay_1, progress=False, workers=1)
    wall_1 = time.perf_counter() - t0

    monkeypatch.setattr(eng, "_effective_inner_workers", lambda *a, **k: 4)
    replay_4 = MarketReplay(opt, spot)
    t1 = time.perf_counter()
    run_grid_full(_CpuBurnStrategy, grid, replay_4, progress=False, workers=4)
    wall_4 = time.perf_counter() - t1

    speedup = wall_1 / wall_4 if wall_4 > 0 else 0.0
    print(f"\nperf gate: wall_1={wall_1:.2f}s wall_4={wall_4:.2f}s speedup={speedup:.2f}×")
    assert wall_1 > 0.5, f"fixture too light ({wall_1:.2f}s); increase BURN"
    assert speedup >= 2.0, (
        f"inner workers=4 speedup {speedup:.2f}× < 2.0 "
        f"(wall_1={wall_1:.2f}s, wall_4={wall_4:.2f}s)"
    )

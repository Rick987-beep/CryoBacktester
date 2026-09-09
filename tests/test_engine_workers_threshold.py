"""workers=1 and in-memory replays must not construct a process Pool."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from backtester.core.engine import run_grid_full
from backtester.core.strategy_base import Trade


class _TinyPnlStrategy:
    """Module-level so pickle would work — still must not spawn on fake replay."""

    name = "tiny_pnl"

    def configure(self, params):
        self.x = int(params.get("x", 0))

    def on_market_state(self, state):
        return []

    def on_end(self, state):
        return [
            Trade(
                entry_time=state.dt,
                exit_time=state.dt,
                entry_spot=float(state.spot),
                exit_spot=float(state.spot),
                entry_price_usd=0.0,
                exit_price_usd=0.0,
                fees=0.0,
                pnl=float(self.x),
                triggered=False,
                exit_reason="end",
                exit_hour=0,
                entry_date=state.dt.strftime("%Y-%m-%d"),
                side="close",
            )
        ]

    def reset(self):
        pass

    def describe_params(self):
        return {"x": self.x}


class _FakeReplay:
    def __init__(self, states):
        self._states = states

    def __len__(self):
        return len(self._states)

    def __iter__(self):
        return iter(self._states)


def _fake_replay():
    t0 = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    s = SimpleNamespace(dt=t0, spot=100_000.0, equity_usd=0.0, nav_usd=0.0)
    s.get_option = lambda *a, **k: None
    return _FakeReplay([s])


def test_workers_1_does_not_construct_pool(monkeypatch):
    import backtester.core.engine as eng

    def boom(*_a, **_k):
        raise AssertionError("Pool must not be constructed when workers=1")

    monkeypatch.setattr(
        eng.multiprocessing,
        "get_context",
        lambda *_a, **_k: SimpleNamespace(Pool=boom),
    )
    df, keys, _nav, final, _fills = run_grid_full(
        _TinyPnlStrategy,
        {"x": [1, 2, 3]},
        _fake_replay(),
        progress=False,
        workers=1,
    )
    assert len(keys) == 3
    assert list(df["pnl"].astype(float)) == [1.0, 2.0, 3.0]
    assert len(final) == 3


def test_fake_replay_never_spawns_even_if_requested(monkeypatch):
    import backtester.core.engine as eng

    def boom(*_a, **_k):
        raise AssertionError("in-memory replay must stay single-process")

    monkeypatch.setattr(
        eng.multiprocessing,
        "get_context",
        lambda *_a, **_k: SimpleNamespace(Pool=boom),
    )
    df, keys, *_ = run_grid_full(
        _TinyPnlStrategy,
        {"x": list(range(16))},
        _fake_replay(),
        progress=False,
        workers=8,
    )
    assert len(keys) == 16
    assert len(df) == 16

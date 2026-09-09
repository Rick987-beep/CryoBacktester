"""Unit tests for inner-worker policy, partition, merge, freeze."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backtester.core.grid_workers import (
    MachineProfile,
    auto_worker_cap,
    freeze_arrays,
    merge_grid_results,
    partition_combos,
    remap_combo_idx,
    resolve_workers,
    shard_offsets,
)


def _profile(**kwargs) -> MachineProfile:
    base = dict(
        logical_cpus=10,
        physical_cpus=10,
        performance_cpus=4,
        total_ram_gb=16.0,
        available_ram_gb=8.0,
        on_battery=False,
    )
    base.update(kwargs)
    return MachineProfile(**base)


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({}, 4),  # 16GB Air AC sharing: all 4 P-cores
        ({"on_battery": True}, 2),
        ({"available_ram_gb": 2.0}, 2),
        ({"available_ram_gb": 1.0}, 1),
        ({"total_ram_gb": 8.0, "available_ram_gb": 6.0}, 1),
        ({"performance_cpus": 12, "total_ram_gb": 64.0, "available_ram_gb": 32.0}, 11),
        ({"performance_cpus": 2, "physical_cpus": 2, "logical_cpus": 2}, 2),
        ({"peers": 2}, 2),
    ],
)
def test_auto_worker_cap_profiles(kwargs, expected):
    peers = kwargs.pop("peers", 1)
    cap = auto_worker_cap(_profile(**kwargs), sharing=True, peers=peers)
    assert cap == expected


def test_auto_worker_cap_duplicate_load_stricter():
    cap = auto_worker_cap(_profile(), sharing=False)
    assert cap <= 2


@pytest.mark.parametrize(
    "n,requested,expected",
    [
        (1, 4, 1),
        (2, 4, 1),
        (7, 4, 1),
        (8, 4, 1),  # 8//8 = 1
        (16, 4, 2),
        (24, 4, 3),
        (32, 4, 4),
        (1728, 1, 1),
        (1728, 8, 4),  # host_cap 4 on Air-like profile
        (3, 3, 1),
        (0, 4, 1),
    ],
)
def test_resolve_workers_table(n, requested, expected):
    got = resolve_workers(n, requested, _profile())
    assert got == expected


def test_resolve_workers_none_uses_host_cap(monkeypatch):
    monkeypatch.delenv("CRYOBT_GRID_WORKERS", raising=False)
    assert resolve_workers(100, None, _profile()) == 4


def test_resolve_workers_env_override(monkeypatch):
    monkeypatch.setenv("CRYOBT_GRID_WORKERS", "1")
    assert resolve_workers(100, None, _profile()) == 1


def test_resolve_workers_rejects_negative():
    with pytest.raises(ValueError):
        resolve_workers(-1, 4, _profile())
    with pytest.raises(ValueError):
        resolve_workers(10, -2, _profile())


def test_partition_covers_exactly():
    combos = [{"i": i} for i in range(10)]
    parts = partition_combos(combos, 3)
    flat = [c for p in parts for c in p]
    assert flat == combos
    assert sum(len(p) for p in parts) == 10
    ids = [id(c) for p in parts for c in p]
    assert ids == [id(c) for c in combos]


def test_partition_n_workers_gt_n():
    combos = [{"i": 0}, {"i": 1}]
    parts = partition_combos(combos, 5)
    assert len(parts) == 2
    assert [c["i"] for p in parts for c in p] == [0, 1]


def test_partition_rejects_zero_workers():
    with pytest.raises(ValueError):
        partition_combos([{"a": 1}], 0)


def test_shard_offsets():
    parts = partition_combos([{"i": i} for i in range(10)], 3)
    offs = shard_offsets(parts)
    assert offs[0] == 0
    assert offs[1] == len(parts[0])
    assert offs[2] == len(parts[0]) + len(parts[1])


def test_remap_and_merge():
    df0 = pd.DataFrame({"combo_idx": [0, 1], "pnl": [1.0, 2.0], "entry_time": [1, 2]})
    df1 = pd.DataFrame({"combo_idx": [0], "pnl": [3.0], "entry_time": [3]})
    nav0 = pd.DataFrame({"combo_idx": [0, 1], "date": ["a", "b"]})
    nav1 = pd.DataFrame({"combo_idx": [0], "date": ["c"]})
    fin0 = pd.DataFrame({"combo_idx": [0, 1], "final_nav": [1.0, 2.0]})
    fin1 = pd.DataFrame({"combo_idx": [0], "final_nav": [3.0]})
    fill0 = pd.DataFrame({"combo_idx": [0], "ts": [10]})
    fill1 = pd.DataFrame({"combo_idx": [0], "ts": [11]})
    shards = [
        (df0, [("a", 1), ("a", 2)], nav0, fin0, fill0),
        (df1, [("a", 3)], nav1, fin1, fill1),
    ]
    keys = [("a", 1), ("a", 2), ("a", 3)]
    df, keys_out, nav, final, fills = merge_grid_results(shards, [0, 2], keys)
    assert keys_out == keys
    assert list(df["combo_idx"]) == [0, 1, 2]
    assert list(df["pnl"]) == [1.0, 2.0, 3.0]
    assert list(final["combo_idx"]) == [0, 1, 2]
    assert list(fills["combo_idx"]) == [0, 2]


def test_merge_empty_shard():
    df0 = pd.DataFrame({"combo_idx": [0], "pnl": [1.0]})
    empty = pd.DataFrame(columns=["combo_idx", "pnl"])
    shards = [
        (df0, [("k", 0)], df0.copy(), df0.copy(), empty),
        (empty, [], empty, empty, empty),
    ]
    df, keys, *_ = merge_grid_results(shards, [0, 1], [("k", 0)])
    assert list(df["combo_idx"]) == [0]
    assert keys == [("k", 0)]


def test_freeze_arrays_write_raises():
    class Box:
        pass

    obj = Box()
    obj._opt_bid = np.ones(4, dtype=np.float32)
    freeze_arrays(obj, names=["_opt_bid"])
    assert obj._opt_bid.flags.writeable is False
    with pytest.raises(ValueError):
        obj._opt_bid[0] = 9.0
    assert float(obj._opt_bid[0]) == 1.0


def test_freeze_idempotent():
    class Box:
        pass

    obj = Box()
    obj._opt_bid = np.ones(2, dtype=np.float32)
    freeze_arrays(obj, names=["_opt_bid"])
    freeze_arrays(obj, names=["_opt_bid"])
    assert obj._opt_bid.flags.writeable is False

"""Inner combo-shard helpers: host probe, worker cap, partition, merge.

The engine kernel stays single-process when ``resolve_workers`` returns 1.
Inner workers (when >1) are children of ``run_backtest`` / a job runner —
never of the GUI.
"""
from __future__ import annotations

import logging
import os
import pickle
import platform
import subprocess
from dataclasses import dataclass
from typing import Any, Sequence

import pandas as pd

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class MachineProfile:
    logical_cpus: int
    physical_cpus: int
    performance_cpus: int
    total_ram_gb: float
    available_ram_gb: float
    on_battery: bool


_PROFILE_CACHE: MachineProfile | None = None

_ARRAY_ATTRS = (
    "_opt_timestamps",
    "_opt_expiry_idx",
    "_opt_strike",
    "_opt_is_call",
    "_opt_bid",
    "_opt_ask",
    "_opt_mark",
    "_opt_mark_iv",
    "_opt_delta",
    "_ts_sorted",
    "_ts_starts",
    "_ts_lens",
    "_timestamps",
    "_spot_ts",
    "_spot_open",
    "_spot_high",
    "_spot_low",
    "_spot_close",
)


def _sysctl_int(name: str) -> int | None:
    try:
        out = subprocess.check_output(["sysctl", "-n", name], text=True, timeout=2)
        return int(out.strip())
    except Exception:
        return None


def _ram_gb() -> tuple[float, float]:
    """Return (total_gb, available_gb). available falls back to total*0.5."""
    total = 0.0
    if platform.system() == "Darwin":
        raw = _sysctl_int("hw.memsize")
        if raw:
            total = raw / (1024 ** 3)
    else:
        try:
            pages = os.sysconf("SC_PHYS_PAGES")
            page = os.sysconf("SC_PAGE_SIZE")
            total = (pages * page) / (1024 ** 3)
        except (ValueError, OSError):
            total = 0.0
    avail = total * 0.5
    if platform.system() == "Linux":
        try:
            with open("/proc/meminfo") as fh:
                for line in fh:
                    if line.startswith("MemAvailable:"):
                        avail = int(line.split()[1]) / (1024 ** 2)
                        break
        except OSError:
            pass
    return (total or 8.0, max(0.0, avail))


def _on_battery() -> bool:
    if platform.system() != "Darwin":
        return False
    try:
        out = subprocess.check_output(["pmset", "-g", "batt"], text=True, timeout=2)
    except Exception:
        return False
    return "Battery Power" in out or "discharging" in out.lower()


def probe_machine() -> MachineProfile:
    """Cache-once host probe. No microbenchmarks."""
    global _PROFILE_CACHE
    if _PROFILE_CACHE is not None:
        return _PROFILE_CACHE
    logical = os.cpu_count() or 1
    physical = logical
    if platform.system() == "Darwin":
        physical = _sysctl_int("hw.physicalcpu") or logical
        perf = _sysctl_int("hw.perflevel0.physicalcpu") or physical
    else:
        perf = physical
    total, avail = _ram_gb()
    _PROFILE_CACHE = MachineProfile(
        logical_cpus=int(logical),
        physical_cpus=int(physical),
        performance_cpus=int(perf),
        total_ram_gb=float(total),
        available_ram_gb=float(avail),
        on_battery=_on_battery(),
    )
    return _PROFILE_CACHE


def reset_machine_profile_cache() -> None:
    """Test helper."""
    global _PROFILE_CACHE
    _PROFILE_CACHE = None


def auto_worker_cap(
    profile: MachineProfile,
    *,
    hard_cap: int = 16,
    sharing: bool = True,
    peers: int = 1,
) -> int:
    """Host-based max inner workers before combo-size limits."""
    cpu = max(1, int(profile.performance_cpus))
    leave_one = (not sharing) or profile.on_battery or cpu >= 8
    cpu_budget = max(1, cpu - (1 if leave_one and cpu >= 4 else 0))

    if not sharing:
        if profile.total_ram_gb < 8:
            mem_budget = 1
        elif profile.total_ram_gb < 24:
            mem_budget = 2
        elif profile.total_ram_gb < 48:
            mem_budget = 4
        else:
            mem_budget = 8
    else:
        if profile.total_ram_gb <= 8:
            mem_budget = 1
        elif profile.total_ram_gb < 12:
            mem_budget = 2
        else:
            mem_budget = hard_cap

    if profile.available_ram_gb < 1.5:
        mem_budget = 1
    elif profile.available_ram_gb < 3.0:
        mem_budget = min(mem_budget, 2)

    cap = min(cpu_budget, mem_budget, max(1, int(hard_cap)))
    cap = max(1, cap // max(1, int(peers)))
    if profile.on_battery:
        cap = min(cap, 2)
    return max(1, cap)


def resolve_workers(
    n_combos: int,
    requested: int | None,
    profile: MachineProfile,
    *,
    hard_cap: int = 16,
    min_combos_parallel: int = 8,
    min_combos_per_worker: int = 8,
    sharing: bool = True,
    peers: int = 1,
) -> int:
    """Effective inner workers. 1 => single-process path (no spawn)."""
    if n_combos < 0:
        raise ValueError("n_combos must be >= 0")
    if requested is not None and requested < 0:
        raise ValueError("requested workers must be >= 0")

    host_cap = auto_worker_cap(
        profile, hard_cap=hard_cap, sharing=sharing, peers=peers
    )
    if requested is None:
        env = os.environ.get("CRYOBT_GRID_WORKERS")
        requested = int(env) if env is not None else host_cap
    if requested <= 1 or n_combos < min_combos_parallel:
        return 1
    by_size = max(1, n_combos // min_combos_per_worker)
    return max(1, min(requested, host_cap, n_combos, by_size))


def resolve_workers_from_cfg(
    n_combos: int,
    requested: int | None,
    *,
    profile: MachineProfile | None = None,
    sharing: bool = True,
    peers: int | None = None,
) -> int:
    from backtester.core.config import cfg

    sim = cfg.simulation
    if peers is None:
        peers = int(os.environ.get("CRYOBT_JOB_PEERS", "1"))
    return resolve_workers(
        n_combos,
        requested,
        profile or probe_machine(),
        hard_cap=int(getattr(sim, "grid_workers_hard_cap", 16)),
        min_combos_parallel=int(getattr(sim, "grid_min_combos_parallel", 8)),
        min_combos_per_worker=int(getattr(sim, "grid_min_combos_per_worker", 8)),
        sharing=sharing,
        peers=peers,
    )


def partition_combos(combos: Sequence[dict], n_workers: int) -> list[list[dict]]:
    """Contiguous slices of an already-expanded combo list.

    flatten(partitions) == list(combos). Never rebuild a sub-PARAM_GRID.
    """
    if n_workers < 1:
        raise ValueError("n_workers must be >= 1")
    items = list(combos)
    n = len(items)
    if n == 0:
        return [[] for _ in range(n_workers)]
    n_workers = min(n_workers, n)
    base, extra = divmod(n, n_workers)
    out: list[list[dict]] = []
    i = 0
    for w in range(n_workers):
        size = base + (1 if w < extra else 0)
        out.append(items[i : i + size])
        i += size
    return out


def shard_offsets(partitions: Sequence[Sequence[Any]]) -> list[int]:
    offsets = []
    acc = 0
    for part in partitions:
        offsets.append(acc)
        acc += len(part)
    return offsets


def remap_combo_idx(df: pd.DataFrame, offset: int, col: str = "combo_idx") -> pd.DataFrame:
    if df is None or df.empty or col not in df.columns:
        return df.copy() if df is not None else pd.DataFrame()
    out = df.copy()
    out[col] = out[col].astype("int64") + int(offset)
    return out


def merge_grid_results(
    shard_results: Sequence[tuple],
    offsets: Sequence[int],
    master_keys: Sequence[tuple],
) -> tuple[pd.DataFrame, list[tuple], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Concatenate per-shard 5-tuples and remap combo_idx by shard offset."""
    if len(shard_results) != len(offsets):
        raise ValueError("shard_results and offsets length mismatch")

    trades: list[pd.DataFrame] = []
    navs: list[pd.DataFrame] = []
    finals: list[pd.DataFrame] = []
    fills: list[pd.DataFrame] = []
    extras: list[pd.DataFrame] = []

    for (df, _keys, nav, final, fill), off in zip(shard_results, offsets):
        trades.append(remap_combo_idx(df, off))
        navs.append(remap_combo_idx(nav, off))
        finals.append(remap_combo_idx(final, off))
        fills.append(remap_combo_idx(fill, off))
        extra = getattr(df, "attrs", {}).get("extra_parquets") or {}
        greeks = extra.get("investor_greeks.parquet")
        if greeks is not None and not getattr(greeks, "empty", True):
            extras.append(remap_combo_idx(greeks, off))

    def _cat(parts: list[pd.DataFrame]) -> pd.DataFrame:
        nonempty = [p for p in parts if p is not None and not p.empty]
        if not nonempty:
            return parts[0].copy() if parts else pd.DataFrame()
        out = pd.concat(nonempty, ignore_index=True)
        if "combo_idx" in out.columns:
            sort_cols = ["combo_idx"]
            for extra in ("entry_time", "ts", "date"):
                if extra in out.columns:
                    sort_cols.append(extra)
                    break
            out = out.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
        return out

    df = _cat(trades)
    nav_daily = _cat(navs)
    final_nav = _cat(finals)
    df_fills = _cat(fills)
    if extras:
        merged_greeks = _cat(extras)
        df.attrs["extra_parquets"] = {"investor_greeks.parquet": merged_greeks}
    keys = list(master_keys)
    return df, keys, nav_daily, final_nav, df_fills


def replay_reload_spec(replay: Any) -> dict[str, Any] | None:
    """Paths + filters to rebuild MarketReplay in a spawn child. None = cannot."""
    path = getattr(replay, "snapshot_path", None)
    spot = getattr(replay, "spot_track_path", None)
    if not path or not spot:
        return None
    if not os.path.exists(str(path)) or not os.path.exists(str(spot)):
        return None
    return {
        "snapshot_path": str(path),
        "spot_track_path": str(spot),
        "expiry_filter": getattr(replay, "expiry_filter", None),
        "start": getattr(replay, "start", None),
        "end": getattr(replay, "end", None),
        "step_minutes": int(getattr(replay, "step_minutes", 5) or 5),
    }


def strategy_is_spawnable(strategy_cls: Any) -> bool:
    try:
        pickle.dumps(strategy_cls)
        return True
    except Exception:
        return False


def worker_run_meta(
    requested: int | None,
    effective: int,
    *,
    sharing: bool = False,
) -> dict[str, Any]:
    """Bundle/CLI sidecar: requested vs host_cap vs effective + machine probe."""
    profile = probe_machine()
    host_cap = auto_worker_cap(profile, sharing=sharing)
    return {
        "grid_workers_requested": requested,
        "grid_workers_host_cap": int(host_cap),
        "grid_workers_effective": int(effective),
        "grid_machine": {
            "logical_cpus": int(profile.logical_cpus),
            "physical_cpus": int(profile.physical_cpus),
            "performance_cpus": int(profile.performance_cpus),
            "total_ram_gb": round(float(profile.total_ram_gb), 2),
            "available_ram_gb": round(float(profile.available_ram_gb), 2),
            "on_battery": bool(profile.on_battery),
        },
    }


def freeze_arrays(obj: Any, names: Sequence[str] = _ARRAY_ATTRS) -> None:
    """Set ndarray.flags.writeable = False on named attributes. Idempotent."""
    import numpy as np

    for name in names:
        arr = getattr(obj, name, None)
        if isinstance(arr, np.ndarray):
            arr.flags.writeable = False

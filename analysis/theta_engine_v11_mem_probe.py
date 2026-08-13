#!/usr/bin/env python3
"""Measure CryoBacktester RAM by stage: load peak vs steady arrays vs instances.

Not a production tool — one-shot probe for the "should we chunk history?" question.
"""

from __future__ import annotations

import gc
import json
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np

from backtester.core.config import cfg as _cfg
from backtester.core.engine import _inject_indicators
from workspace.strategies.theta_engine.v11 import ThetaEngineV11


def _rss_mb() -> float:
    """Current RSS in MiB (macOS: ru_maxrss is bytes; Linux: KiB)."""
    import resource

    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # ru_maxrss is a high-water mark, not current. Prefer ps for current.
    try:
        with open(f"/proc/{os.getpid()}/status") as f:  # Linux
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except FileNotFoundError:
        pass
    # macOS current RSS via ps
    out = os.popen(f"ps -o rss= -p {os.getpid()}").read().strip()
    if out:
        return float(out) / 1024.0
    # fallback: high-water (bytes on Darwin)
    return rss / (1024.0 * 1024.0)


def _nbytes_mb(obj: Any) -> float:
    if obj is None:
        return 0.0
    if isinstance(obj, np.ndarray):
        return float(obj.nbytes) / (1024.0 * 1024.0)
    if isinstance(obj, dict):
        # crude: keys+values as Python objects (~28B/int + dict overhead)
        return (sys.getsizeof(obj) + 28 * len(obj)) / (1024.0 * 1024.0)
    if isinstance(obj, list):
        return sys.getsizeof(obj) / (1024.0 * 1024.0)
    return 0.0


class _PeakSampler:
    def __init__(self, interval_s: float = 0.25) -> None:
        self.peak = _rss_mb()
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._run, args=(interval_s,), daemon=True)

    def start(self) -> None:
        self._t.start()

    def stop(self) -> float:
        self._stop.set()
        self._t.join(timeout=2.0)
        return self.peak

    def _run(self, interval_s: float) -> None:
        while not self._stop.wait(interval_s):
            cur = _rss_mb()
            if cur > self.peak:
                self.peak = cur


def _array_report(replay: Any) -> Dict[str, float]:
    names = [
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
    ]
    out: Dict[str, float] = {}
    for n in names:
        out[n] = round(_nbytes_mb(getattr(replay, n, None)), 1)
    out["_ts_to_idx"] = round(_nbytes_mb(getattr(replay, "_ts_to_idx", None)), 1)
    out["sum_opt_arrays"] = round(sum(out[k] for k in names if k.startswith("_opt_")), 1)
    return out


def probe(start: str, end: str, n_combos: int = 54, n_iter_states: int = 4000) -> Dict[str, Any]:
    from backtester.core.market_replay import MarketReplay

    gc.collect()
    baseline = _rss_mb()
    sampler = _PeakSampler()
    sampler.start()
    t0 = time.time()
    replay = MarketReplay(
        _cfg.data.options_parquet,
        _cfg.data.spot_parquet,
        start=start,
        end=end,
    )
    load_s = time.time() - t0
    load_peak = sampler.stop()
    after_load = _rss_mb()
    arrays = _array_report(replay)

    instances: List[Any] = []
    t1 = time.time()
    for _ in range(n_combos):
        s = ThetaEngineV11()
        s.configure({
            "delta": 0.25,
            "min_dte": 90,
            "hold_days": 0,
            "stop_loss_pct": 3.0,
            "take_profit_pct": 0.50,
            "max_concurrent": 20,
            "qty_per_1btc_equity": 0.2,
            "launch_accel": 0,
            "launch_size_mult": 1.0,
            "greek_limits_mode": "off",
            "perp_delta_hedge": 0,
            "perp_deadband_pct": 2.0,
            "option_hedge_mode": "sticky_budget",
            "wing_expiry_mode": "same",
            "wing_delta": 0.10,
            "wing_trigger": "dg",
            "wing_close_margin_pct": 3.0,
            "wing_min_hold_minutes": 60.0,
            "wing_cooldown_minutes": 60.0,
            "wing_cooldown_override_mult": 1.5,
            "wing_side_mode": "greek",
            "wing_delta_mode": "relative",
            "wing_delta_ratio": 0.5,
            "entry_policy": "fav_sharpe_rich4_f5_1600",
        })
        instances.append(s)
    after_instances = _rss_mb()
    _inject_indicators(ThetaEngineV11, instances, replay, progress=True)
    after_indicators = _rss_mb()
    inst_s = time.time() - t1

    # Iterate a prefix of states through all instances (working-set, not full run)
    n_states = 0
    t2 = time.time()
    capital = float(_cfg.simulation.account_size_usd)
    for state in replay:
        n_states += 1
        state.equity_usd = capital
        state.nav_usd = capital
        for strat in instances:
            list(strat.on_market_state(state))
        if n_states >= n_iter_states:
            break
    iter_s = time.time() - t2
    after_iter = _rss_mb()
    states_per_s = n_states / iter_s if iter_s > 0 else 0.0

    n_opt = len(replay._opt_timestamps)
    result = {
        "date_range": [start, end],
        "n_opt_rows": int(n_opt),
        "n_intervals": int(len(replay)),
        "n_combos": n_combos,
        "n_iter_states": n_states,
        "rss_mb": {
            "baseline": round(baseline, 1),
            "load_peak": round(load_peak, 1),
            "after_load": round(after_load, 1),
            "after_instances": round(after_instances, 1),
            "after_indicators": round(after_indicators, 1),
            "after_iter": round(after_iter, 1),
        },
        "delta_mb": {
            "load_steady": round(after_load - baseline, 1),
            "load_peak_over_steady": round(load_peak - after_load, 1),
            "instances": round(after_instances - after_load, 1),
            "indicators": round(after_indicators - after_instances, 1),
            "iter_working_set": round(after_iter - after_indicators, 1),
        },
        "arrays_mb": arrays,
        "dead_opt_timestamps_mb": arrays.get("_opt_timestamps"),
        "seconds": {
            "load": round(load_s, 1),
            "instances_plus_indicators": round(inst_s, 1),
            "iter": round(iter_s, 1),
            "states_per_s": round(states_per_s, 1),
        },
    }
    del instances
    del replay
    gc.collect()
    return result


def main() -> None:
    windows = [
        ("2026-07-01", "2026-08-01"),   # ~1 month
        ("2026-05-01", "2026-08-01"),   # ~3 months
        ("2026-02-01", "2026-08-01"),   # ~6 months
    ]
    # Full range last — only if previous window stayed under 6 GB RSS
    full = ("2025-04-11", "2026-08-12")
    rows: List[Dict[str, Any]] = []
    for start, end in windows:
        print(f"\n===== probe {start} → {end} =====")
        row = probe(start, end)
        print(json.dumps(row, indent=2))
        rows.append(row)
    last_steady = rows[-1]["rss_mb"]["after_load"]
    if last_steady < 4500:
        print(f"\n===== probe FULL {full[0]} → {full[1]} =====")
        rows.append(probe(*full))
        print(json.dumps(rows[-1], indent=2))
    else:
        print(f"\nSkipping full range (6m after_load={last_steady:.0f} MB)")

    out = ROOT / "analysis" / "theta_engine_v11_mem_probe.json"
    out.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()

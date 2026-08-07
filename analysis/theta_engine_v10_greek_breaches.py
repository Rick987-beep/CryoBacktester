#!/usr/bin/env python3
"""Run theta_engine_v10 PARAM_GRID and print cash-Greek breach telemetry.

Four cells: RichForce16 / Daily15 × greek_limits_mode off|size_to_budget
(Mode C TP locks applied inside the strategy).

Usage::

    PYTHONPATH=. .venv/bin/python analysis/theta_engine_v10_greek_breaches.py
"""

from __future__ import annotations

import time
from itertools import product
from typing import Any, Dict, List, Tuple

from backtester.core.config import cfg as _cfg
from backtester.core.engine import _inject_indicators, _open_unrealized_pnl
from backtester.core.market_replay import MarketReplay
from workspace.strategies.theta_engine._common import (
    BASELINE_DAILY15,
    BASELINE_DISPLAY,
    BASELINE_RICHFORCE16,
)
from workspace.strategies.theta_engine.v10 import ThetaEngineV10

CAPITAL = float(_cfg.simulation.account_size_usd)


def _combos(grid: Dict[str, List]) -> List[Dict[str, Any]]:
    keys = list(grid)
    return [dict(zip(keys, vals)) for vals in product(*(grid[k] for k in keys))]


def _run_with_telemetry(
    replay: MarketReplay,
    param_grid: Dict[str, List],
) -> List[Tuple[Dict[str, Any], Dict[str, Any], float, int]]:
    """Single-pass grid; return (params, risk_telemetry, realized_pnl, n_closes)."""
    combos = _combos(param_grid)
    instances = []
    for params in combos:
        s = ThetaEngineV10()
        s.configure(params)
        instances.append((params, s))

    _inject_indicators(ThetaEngineV10, [s for _, s in instances], replay, progress=True)

    realized = [0.0] * len(instances)
    n_closes = [0] * len(instances)
    pos_caches: List[Dict[int, float]] = [{} for _ in instances]
    last_state = None
    n_states = 0
    t0 = time.time()

    for state in replay:
        n_states += 1
        for i, (_, strat) in enumerate(instances):
            state.equity_usd = CAPITAL + realized[i]
            open_pnl = _open_unrealized_pnl(strat, state, pos_caches[i])
            state.nav_usd = state.equity_usd + open_pnl
            for trade in strat.on_market_state(state):
                if getattr(trade, "side", "close") == "close":
                    realized[i] += float(trade.pnl)
                    n_closes[i] += 1
        last_state = state
        if n_states % 5000 == 0:
            print(f"  … {n_states} states ({time.time() - t0:.0f}s)")

    if last_state is not None:
        for i, (_, strat) in enumerate(instances):
            for trade in strat.on_end(last_state):
                if getattr(trade, "side", "close") == "close":
                    realized[i] += float(trade.pnl)
                    n_closes[i] += 1

    out = []
    for i, (params, strat) in enumerate(instances):
        out.append((params, strat.risk_telemetry(), realized[i], n_closes[i]))
    print(f"Done {n_states} states, {len(combos)} combos in {time.time() - t0:.1f}s")
    return out


def _pct(num: int, den: int) -> float:
    return 100.0 * num / den if den else 0.0


def main() -> None:
    date_from, date_to = ThetaEngineV10.DATE_RANGE
    print(f"Loading {date_from} → {date_to}")
    replay = MarketReplay(
        _cfg.data.options_parquet,
        _cfg.data.spot_parquet,
        start=date_from,
        end=date_to,
    )
    print(f"Intervals: {len(replay._timestamps):,}")
    grid = ThetaEngineV10.PARAM_GRID
    print(f"Grid combos: {len(_combos(grid))}")
    rows = _run_with_telemetry(replay, grid)

    print()
    print("=" * 100)
    print("theta_engine_v10 — investor cash-Greek limit breaches (% of bars)")
    print("Bands: |D|<10% (short Γ) / 30% (long Γ); G%>−10; |V%|<0.2; T%>−1")
    print("=" * 100)
    hdr = (
        f"{'entry':12} {'mode':16} {'TP':4} {'closes':>6} {'PnL$':>10} "
        f"{'bars':>7} {'any%':>6} {'D%':>6} {'G%':>6} {'V%':>6} {'T%':>6} "
        f"{'max|V|':>8} {'minG':>8} {'skips':>5} {'sized':>5}"
    )
    print(hdr)
    print("-" * len(hdr))

    for params, tel, pnl, n_cl in rows:
        ep = params["entry_policy"]
        name = BASELINE_DISPLAY.get(ep, ep)[:12]
        mode = params["greek_limits_mode"]
        # TP from Mode C lock (strategy applied it); show expected
        tp = 0.60 if ep == BASELINE_RICHFORCE16 else 0.50
        bars = int(tel["risk_bars"])
        print(
            f"{name:12} {mode:16} {tp:4.2f} {n_cl:6d} {pnl:10.0f} "
            f"{bars:7d} "
            f"{_pct(tel['risk_breach_any'], bars):6.1f} "
            f"{_pct(tel['risk_breach_d'], bars):6.1f} "
            f"{_pct(tel['risk_breach_g'], bars):6.1f} "
            f"{_pct(tel['risk_breach_v'], bars):6.1f} "
            f"{_pct(tel['risk_breach_t'], bars):6.1f} "
            f"{tel['risk_max_abs_v']:8.3f} "
            f"{tel['risk_min_g']:8.3f} "
            f"{tel['risk_skips']:5d} "
            f"{tel['risk_sized_opens']:5d}"
        )

    print()
    print("any%/D%/G%/V%/T% = fraction of bars outside that band.")
    print("max|V| / minG = worst observed vega / gamma % of AUM over the run.")
    print("skips / sized = entry skips / opens with qty < raw under size_to_budget.")


if __name__ == "__main__":
    main()

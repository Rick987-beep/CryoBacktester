"""Shared helpers for theta_engine_v10 phase analysis scripts."""

from __future__ import annotations

import csv
import json
import subprocess
import time
from collections import defaultdict
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from backtester.core.config import cfg as _cfg
from backtester.core.engine import _inject_indicators, _open_unrealized_pnl
from backtester.core.market_replay import MarketReplay
from workspace.strategies.theta_engine._common import BASELINE_DISPLAY, BASELINE_RICHFORCE16
from workspace.strategies.theta_engine.v10 import ThetaEngineV10

CAPITAL = float(_cfg.simulation.account_size_usd)
ANALYSIS_ROOT = Path(__file__).resolve().parent


def combos(grid: Dict[str, List]) -> List[Dict[str, Any]]:
    keys = list(grid)
    return [dict(zip(keys, vals)) for vals in product(*(grid[k] for k in keys))]


def pct(num: float, den: float) -> float:
    return 100.0 * float(num) / float(den) if den else 0.0


def git_head() -> str:
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=str(ANALYSIS_ROOT.parent),
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
    except Exception:
        return "unknown"


def bucket_n_open(n: int) -> str:
    """Bucket concurrent open count for Phase 0 tables."""
    if n <= 0:
        return "0"
    if n == 1:
        return "1"
    if n <= 3:
        return "2-3"
    if n <= 5:
        return "4-5"
    if n <= 10:
        return "6-10"
    return "11+"


def max_dd_from_nav(navs: List[float]) -> float:
    """Max drawdown % from a NAV path (peak-to-trough)."""
    if not navs:
        return 0.0
    peak = navs[0]
    max_dd = 0.0
    for v in navs:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (peak - v) / peak * 100.0
            if dd > max_dd:
                max_dd = dd
    return max_dd


def ann_return_from_nav(navs: List[float], n_calendar_days: float) -> float:
    if not navs or navs[0] <= 0 or n_calendar_days <= 0:
        return 0.0
    total = navs[-1] / navs[0]
    years = n_calendar_days / 365.25
    if years <= 0:
        return 0.0
    if total <= 0:
        return -1.0
    return total ** (1.0 / years) - 1.0


def write_phase_dir(
    phase: str,
    *,
    summary_md: str,
    scorecard_rows: List[Dict[str, Any]],
    meta: Dict[str, Any],
    extras: Optional[Dict[str, Any]] = None,
) -> Path:
    """Write standard phase artifacts under analysis/theta_engine_v10_pN/."""
    out = ANALYSIS_ROOT / f"theta_engine_v10_{phase}"
    out.mkdir(parents=True, exist_ok=True)
    (out / "summary.md").write_text(summary_md, encoding="utf-8")
    (out / "README.md").write_text(
        f"# theta_engine_v10 {phase}\n\nSee `summary.md` and `scorecard.csv`.\n",
        encoding="utf-8",
    )
    meta = dict(meta)
    meta.setdefault("written_at", datetime.now(timezone.utc).isoformat())
    meta.setdefault("git_head", git_head())
    (out / "meta.json").write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")

    if scorecard_rows:
        keys = list(scorecard_rows[0].keys())
        with (out / "scorecard.csv").open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            for row in scorecard_rows:
                w.writerow(row)
    else:
        (out / "scorecard.csv").write_text("", encoding="utf-8")

    extras = extras or {}
    for name, payload in extras.items():
        path = out / name
        if isinstance(payload, str):
            path.write_text(payload, encoding="utf-8")
        elif isinstance(payload, list) and payload and isinstance(payload[0], dict):
            keys = list(payload[0].keys())
            with path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=keys)
                w.writeheader()
                for row in payload:
                    w.writerow(row)
        else:
            path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return out


def scorecard_row(
    params: Dict[str, Any],
    tel: Dict[str, Any],
    *,
    pnl: float,
    n_closes: int,
    ann_return: float,
    max_dd: float,
    sharpe: float = 0.0,
) -> Dict[str, Any]:
    ep = params.get("entry_policy", "")
    bars = int(tel.get("risk_bars") or 0)
    return {
        "entry": BASELINE_DISPLAY.get(ep, ep),
        "entry_policy": ep,
        "greek_limits_mode": params.get("greek_limits_mode", tel.get("greek_limits_mode")),
        "qty_per_1btc_equity": params.get("qty_per_1btc_equity"),
        "max_concurrent": params.get("max_concurrent"),
        "hold_days": params.get("hold_days", 0),
        "perp_delta_hedge": params.get("perp_delta_hedge", 0),
        "option_hedge_mode": params.get("option_hedge_mode", "none"),
        "wing_expiry_mode": params.get("wing_expiry_mode", "same"),
        "wing_delta": params.get("wing_delta", 0.10),
        "wing_trigger": params.get("wing_trigger", "dg"),
        "take_profit_pct": (
            params["take_profit_pct"]
            if params.get("take_profit_pct") is not None
            else (0.60 if ep == BASELINE_RICHFORCE16 else 0.50)
        ),
        "ann_return": round(ann_return, 6),
        "max_dd_pct": round(max_dd, 4),
        "sharpe": round(sharpe, 4),
        "pnl": round(pnl, 2),
        "n_closes": n_closes,
        "bars": bars,
        "breach_any_pct": round(pct(tel.get("risk_breach_any", 0), bars), 3),
        "breach_d_pct": round(pct(tel.get("risk_breach_d", 0), bars), 3),
        "breach_g_pct": round(pct(tel.get("risk_breach_g", 0), bars), 3),
        "breach_v_pct": round(pct(tel.get("risk_breach_v", 0), bars), 3),
        "breach_t_pct": round(pct(tel.get("risk_breach_t", 0), bars), 3),
        "max_abs_v": round(float(tel.get("risk_max_abs_v") or 0), 4),
        "min_g": round(float(tel.get("risk_min_g") or 0), 4),
        "skips": int(tel.get("risk_skips") or 0),
        "sized_opens": int(tel.get("risk_sized_opens") or 0),
        "perp_trades": int(tel.get("perp_trades") or 0),
        "perp_fees": round(float(tel.get("perp_fees") or 0), 2),
        "wing_adjusts": int(tel.get("wing_adjusts") or 0),
        "wing_opens": int(tel.get("wing_opens") or 0),
        "wing_premium_paid": round(float(tel.get("wing_premium_paid") or 0), 2),
    }


def run_grid_telemetry(
    replay: MarketReplay,
    param_grid: Optional[Dict[str, List]] = None,
    *,
    param_list: Optional[List[Dict[str, Any]]] = None,
    collect_n_open: bool = False,
) -> List[Dict[str, Any]]:
    """Single-pass grid; each result has params, tel, pnl, n_closes, nav_path, n_open_bins.

    Pass either ``param_grid`` (Cartesian product) or an explicit ``param_list``.
    """
    if param_list is not None:
        combo_list = list(param_list)
    elif param_grid is not None:
        combo_list = combos(param_grid)
    else:
        raise ValueError("run_grid_telemetry requires param_grid or param_list")
    instances: List[Tuple[Dict[str, Any], Any]] = []
    for params in combo_list:
        s = ThetaEngineV10()
        s.configure(params)
        instances.append((params, s))

    _inject_indicators(ThetaEngineV10, [s for _, s in instances], replay, progress=True)

    realized = [0.0] * len(instances)
    n_closes = [0] * len(instances)
    pos_caches: List[Dict[int, float]] = [{} for _ in instances]
    nav_paths: List[List[float]] = [[] for _ in instances]
    day_nav: List[Dict[str, float]] = [{} for _ in instances]
    n_open_stats: List[Dict[str, Dict[str, int]]] = [
        defaultdict(lambda: {"bars": 0, "breach_any": 0, "breach_v": 0, "breach_d": 0})
        for _ in instances
    ]
    last_state = None
    n_states = 0
    t0 = time.time()
    first_dt = None
    last_dt = None

    for state in replay:
        n_states += 1
        if first_dt is None:
            first_dt = state.dt
        last_dt = state.dt
        day = state.dt.strftime("%Y-%m-%d")
        for i, (_, strat) in enumerate(instances):
            state.equity_usd = CAPITAL + realized[i]
            open_pnl = _open_unrealized_pnl(strat, state, pos_caches[i])
            perp_pnl = float(getattr(strat, "perp_mark_pnl", lambda _s: 0.0)(state.spot))
            wing_pnl = float(
                getattr(strat, "wing_mark_pnl", lambda _st: 0.0)(state)
            )
            state.nav_usd = state.equity_usd + open_pnl + perp_pnl + wing_pnl
            before = dict(strat.risk_telemetry())
            for trade in strat.on_market_state(state):
                if getattr(trade, "side", "close") == "close":
                    realized[i] += float(trade.pnl)
                    n_closes[i] += 1
            after = strat.risk_telemetry()
            day_nav[i][day] = state.nav_usd
            if collect_n_open:
                n = len(strat._positions)
                b = bucket_n_open(n)
                n_open_stats[i][b]["bars"] += 1
                # attribute new breach ticks this bar
                if after.get("risk_breach_any", 0) > before.get("risk_breach_any", 0):
                    n_open_stats[i][b]["breach_any"] += 1
                if after.get("risk_breach_v", 0) > before.get("risk_breach_v", 0):
                    n_open_stats[i][b]["breach_v"] += 1
                if after.get("risk_breach_d", 0) > before.get("risk_breach_d", 0):
                    n_open_stats[i][b]["breach_d"] += 1
        last_state = state
        if n_states % 5000 == 0:
            print(f"  … {n_states} states ({time.time() - t0:.0f}s)")

    if last_state is not None:
        for i, (_, strat) in enumerate(instances):
            for trade in strat.on_end(last_state):
                if getattr(trade, "side", "close") == "close":
                    realized[i] += float(trade.pnl)
                    n_closes[i] += 1
            # final NAV
            open_pnl = _open_unrealized_pnl(strat, last_state, pos_caches[i])
            perp_pnl = float(getattr(strat, "perp_mark_pnl", lambda _s: 0.0)(last_state.spot))
            wing_pnl = float(
                getattr(strat, "wing_mark_pnl", lambda _st: 0.0)(last_state)
            )
            day = last_state.dt.strftime("%Y-%m-%d")
            day_nav[i][day] = CAPITAL + realized[i] + open_pnl + perp_pnl + wing_pnl

    cal_days = 1.0
    if first_dt and last_dt:
        cal_days = max((last_dt.date() - first_dt.date()).days, 1)

    out: List[Dict[str, Any]] = []
    for i, (params, strat) in enumerate(instances):
        navs = [day_nav[i][d] for d in sorted(day_nav[i])]
        if not navs:
            navs = [CAPITAL]
        # crude daily sharpe
        rets = []
        for a, b in zip(navs, navs[1:]):
            if a > 0:
                rets.append((b - a) / a)
        sharpe = 0.0
        if len(rets) > 1:
            import statistics
            m = statistics.mean(rets)
            s = statistics.pstdev(rets)
            if s > 1e-12:
                sharpe = m / s * (365 ** 0.5)
        final_spot = last_state.spot if last_state else 0
        wing_final = float(getattr(strat, "wing_mark_pnl", lambda _st: 0.0)(last_state)) if last_state else 0.0
        out.append({
            "params": params,
            "tel": strat.risk_telemetry(),
            "pnl": realized[i]
            + float(getattr(strat, "perp_mark_pnl", lambda _s: 0.0)(final_spot))
            + wing_final,
            "n_closes": n_closes[i],
            "navs": navs,
            "ann_return": ann_return_from_nav(navs, cal_days),
            "max_dd": max_dd_from_nav(navs),
            "sharpe": sharpe,
            "n_open_bins": {k: dict(v) for k, v in n_open_stats[i].items()},
            "cal_days": cal_days,
            "strategy": strat,
        })
    print(f"Done {n_states} states, {len(combo_list)} combos in {time.time() - t0:.1f}s")
    return out


def load_replay() -> MarketReplay:
    date_from, date_to = ThetaEngineV10.DATE_RANGE
    print(f"Loading {date_from} → {date_to}")
    replay = MarketReplay(
        _cfg.data.options_parquet,
        _cfg.data.spot_parquet,
        start=date_from,
        end=date_to,
    )
    print(f"Intervals: {len(replay._timestamps):,}")
    return replay


def base_book_params(**overrides: Any) -> Dict[str, List]:
    """Mode C book skeleton for phase grids."""
    g = {
        "delta": [0.25],
        "min_dte": [90],
        "hedge_delta": [0.0],
        "hedge_qty_mult": [0],
        "hold_days": [0],
        "stop_loss_pct": [3.0],
        "take_profit_pct": [0.50],
        "max_concurrent": [20],
        "qty_per_1btc_equity": [0.2],
        "launch_accel": [0],
        "launch_size_mult": [1.0],
        "greek_limits_mode": ["off"],
        "perp_delta_hedge": [0],
        "perp_deadband_pct": [2.0],
        "option_hedge_mode": ["none"],
        "wing_expiry_mode": ["same"],
        "wing_delta": [0.10],
        "wing_trigger": ["dg"],
        "entry_policy": list(ThetaEngineV10.PARAM_GRID["entry_policy"]),
    }
    g.update(overrides)
    return g

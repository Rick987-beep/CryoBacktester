#!/usr/bin/env python3
"""Soundness suite: is trail arm=0.65/gb=0.05 real edge or selection luck?

On ``fav_sharpe_rich4_f5_1600`` only.  Tests from the research plan:

  (2) DSR / multiple-testing haircut over prior exit search + local grid
  (3) Walk-forward: fixed TP=0.60 vs trail 0.65/0.05 (and a neighborhood WFO)
  (4) Chronological 70/30 holdout with IS re-selection + frozen candidate OOS
  (+) Local sensitivity hill check around the candidate (spike vs plateau)

Prior exit trials already spent on this entry (approx)::

    Mode C fixed TP sweep: 5
    Mode A coarse trail:  12
    Mode A high-arm:       9
    ─────────────────────────
    N_PRIOR              = 26

Usage::

    PYTHONPATH=. python analysis/theta_engine_v9_trail_verify.py
    PYTHONPATH=. python analysis/theta_engine_v9_trail_verify.py --skip-wfo
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from backtester.core.config import cfg as _cfg
from backtester.core.engine import run_grid_full
from backtester.core.market_replay import MarketReplay
from backtester.core.paths import runs_dir
from backtester.core.results import GridResult
from backtester.research.robustness import deflated_sharpe_ratio
from workspace.strategies.theta_engine.v9 import ThetaEngineV9

ENTRY = "fav_sharpe_rich4_f5_1600"
N_PRIOR = 26  # Mode C (5) + coarse trail (12) + high-arm (9) on this entry
CAPITAL = float(_cfg.simulation.account_size_usd)
DATE_START, DATE_END = ThetaEngineV9.DATE_RANGE

BOOK = {
    "delta": [0.25],
    "min_dte": [90],
    "hedge_delta": [0.0],
    "hedge_qty_mult": [0],
    "hold_days": [0],
    "stop_loss_pct": [3.0],
    "tp_age_early": [14],
    "tp_age_late": [45],
    "tp_early": [0.70],
    "tp_mid": [0.50],
    "tp_late": [0.35],
    "max_concurrent": [20],
    "qty_per_1btc_equity": [0.2],
    "launch_accel": [0],
    "launch_size_mult": [1.0],
    "entry_policy": [ENTRY],
}

# Local hill around the candidate (not the coarse losing region).
LOCAL_ARMS = [0.58, 0.60, 0.62, 0.65, 0.68, 0.70]
LOCAL_GBS = [0.03, 0.05, 0.07, 0.10]


def _grid(**overrides: Any) -> Dict[str, List]:
    g = {k: list(v) for k, v in BOOK.items()}
    g.update(overrides)
    return g


def _key_dict(key: Tuple) -> Dict[str, Any]:
    return dict(key)


def _is_trail_cand(d: Dict[str, Any], arm: float = 0.65, gb: float = 0.05) -> bool:
    return (
        str(d.get("exit_mode")) == "trail"
        and abs(float(d.get("take_profit_pct", 0))) < 1e-9
        and abs(float(d.get("trail_arm", 0)) - arm) < 1e-9
        and abs(float(d.get("trail_giveback", 0)) - gb) < 1e-9
    )


def _is_fixed(d: Dict[str, Any], tp: float) -> bool:
    return (
        str(d.get("exit_mode")) == "fixed"
        and abs(float(d.get("take_profit_pct", -1)) - tp) < 1e-9
    )


def _label(d: Dict[str, Any]) -> str:
    if str(d.get("exit_mode")) == "fixed":
        return "fixed_%.2f" % float(d["take_profit_pct"])
    return "trail_%.2f_%.2f" % (
        float(d["trail_arm"]),
        float(d["trail_giveback"]),
    )


def _agg_from_result(result: GridResult, key: Tuple) -> Dict[str, Any]:
    st = result.all_stats.get(key, {})
    return {
        "label": _label(_key_dict(key)),
        "n": int(st.get("n", 0)),
        "pnl": float(st.get("total_pnl", 0.0)),
        "sharpe": float(st.get("sharpe", 0.0)),
        "max_dd_pct": float(st.get("max_dd_pct", 0.0)),
        "ann_return": float(st.get("ann_return", 0.0) or 0.0),
        "score": float(result.scores.get(key, 0.0)),
    }


def _find_key(keys: List[Tuple], pred) -> Optional[Tuple]:
    for k in keys:
        if pred(_key_dict(k)):
            return k
    return None


def _run_grid(grid: Dict[str, List], replay: MarketReplay, title: str) -> GridResult:
    print(f"\n── {title} ──")
    t0 = time.time()
    df, keys, nav, final, fills = run_grid_full(ThetaEngineV9, grid, replay)
    result = GridResult(
        df,
        keys,
        nav,
        final,
        param_grid=grid,
        account_size=CAPITAL,
        date_range=tuple(ThetaEngineV9.DATE_RANGE),
        df_fills=fills,
    )
    print(f"  {len(keys)} combos, {len(df)} trades in {time.time()-t0:.1f}s")
    return result


def phase_local_and_dsr(replay: MarketReplay) -> Dict[str, Any]:
    """(+) Local hill + (2) DSR with multiple-testing budget."""
    trail_grid = _grid(
        exit_mode=["trail"],
        take_profit_pct=[0.0],
        trail_arm=LOCAL_ARMS,
        trail_giveback=LOCAL_GBS,
    )
    fixed_grid = _grid(
        exit_mode=["fixed"],
        take_profit_pct=[0.60, 0.65],
        trail_arm=[0.65],
        trail_giveback=[0.05],
    )
    trail_res = _run_grid(trail_grid, replay, "Local trail hill (6×4)")
    fixed_res = _run_grid(fixed_grid, replay, "Fixed controls 0.60 / 0.65")

    rows = []
    for key in trail_res.keys:
        d = _key_dict(key)
        a = _agg_from_result(trail_res, key)
        a.update(arm=float(d["trail_arm"]), gb=float(d["trail_giveback"]))
        rows.append(a)
    hill = pd.DataFrame(rows).sort_values("sharpe", ascending=False)

    cand_key = _find_key(trail_res.keys, lambda d: _is_trail_cand(d, 0.65, 0.05))
    ctrl_key = _find_key(fixed_res.keys, lambda d: _is_fixed(d, 0.60))
    assert cand_key and ctrl_key
    cand = _agg_from_result(trail_res, cand_key)
    ctrl = _agg_from_result(fixed_res, ctrl_key)
    best_local = hill.iloc[0].to_dict()

    # Rank of candidate within local hill
    h = hill.reset_index(drop=True)
    hit = h.index[((h.arm - 0.65).abs() < 1e-9) & ((h.gb - 0.05).abs() < 1e-9)]
    cand_rank = int(hit[0]) + 1 if len(hit) else -1

    # DSR on candidate trade PnLs
    cand_pnls = trail_res.df[trail_res.df.combo_idx == trail_res.key_to_idx[cand_key]][
        "pnl"
    ].to_numpy()
    n_local = len(LOCAL_ARMS) * len(LOCAL_GBS)
    dsr_prior_only = deflated_sharpe_ratio(cand_pnls, CAPITAL, N_PRIOR)
    dsr_with_local = deflated_sharpe_ratio(cand_pnls, CAPITAL, N_PRIOR + n_local)
    # Best-in-local DSR (selection over local grid only, + prior)
    best_key = None
    for key in trail_res.keys:
        d = _key_dict(key)
        if abs(d["trail_arm"] - best_local["arm"]) < 1e-9 and abs(
            d["trail_giveback"] - best_local["gb"]
        ) < 1e-9:
            best_key = key
            break
    best_pnls = trail_res.df[
        trail_res.df.combo_idx == trail_res.key_to_idx[best_key]
    ]["pnl"].to_numpy()
    dsr_best_local = deflated_sharpe_ratio(best_pnls, CAPITAL, N_PRIOR + n_local)

    print("\n  Local hill top-5 by Sharpe:")
    print(hill.head(5)[["arm", "gb", "n", "pnl", "sharpe", "max_dd_pct"]].to_string(index=False))
    print(
        f"\n  Candidate 0.65/0.05: sharpe={cand['sharpe']:.3f} pnl={cand['pnl']:.0f} "
        f"rank={cand_rank}/{n_local}  vs fixed0.60 sharpe={ctrl['sharpe']:.3f}"
    )
    print(
        f"  DSR(candidate | n_trials={N_PRIOR})={dsr_prior_only}  "
        f"DSR(+local {N_PRIOR}+{n_local})={dsr_with_local}"
    )
    print(
        f"  Best local arm={best_local['arm']} gb={best_local['gb']} "
        f"sharpe={best_local['sharpe']:.3f} DSR={dsr_best_local}"
    )

    # Spike diagnostic: sharpe gap vs neighbors of candidate
    neigh = hill[
        (hill.arm.sub(0.65).abs() <= 0.03 + 1e-9)
        | ((hill.arm == 0.65) & (hill.gb.sub(0.05).abs() <= 0.02 + 1e-9))
    ]
    print("  Neighbor band around candidate:")
    print(neigh.sort_values(["arm", "gb"])[["arm", "gb", "sharpe", "pnl"]].to_string(index=False))

    return {
        "hill": hill,
        "candidate": cand,
        "control": ctrl,
        "cand_rank": cand_rank,
        "n_local": n_local,
        "dsr_prior_only": dsr_prior_only,
        "dsr_with_local": dsr_with_local,
        "dsr_best_local": dsr_best_local,
        "best_local": best_local,
        "trail_res": trail_res,
        "fixed_res": fixed_res,
        "cand_key": cand_key,
        "ctrl_key": ctrl_key,
    }


def _split_date(frac: float = 0.70) -> str:
    t0 = datetime.strptime(DATE_START, "%Y-%m-%d").date()
    t1 = datetime.strptime(DATE_END, "%Y-%m-%d").date()
    span = (t1 - t0).days
    split = t0 + timedelta(days=int(span * frac))
    return split.strftime("%Y-%m-%d")


def phase_holdout() -> Dict[str, Any]:
    """(4) Chronological 70/30 holdout."""
    split = _split_date(0.70)
    # IS ends day before split; OOS from split
    t_split = datetime.strptime(split, "%Y-%m-%d").date()
    is_end = (t_split - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"\n── (4) Holdout IS [{DATE_START} → {is_end}]  OOS [{split} → {DATE_END}] ──")

    is_replay = MarketReplay(
        _cfg.data.options_parquet, _cfg.data.spot_parquet,
        start=DATE_START, end=is_end,
    )
    oos_replay = MarketReplay(
        _cfg.data.options_parquet, _cfg.data.spot_parquet,
        start=split, end=DATE_END,
    )

    # IS selection grid: fixed 0.60 + local trail neighborhood (smaller for speed)
    is_arms = [0.60, 0.65, 0.70]
    is_gbs = [0.03, 0.05, 0.08]
    # Run trail IS and fixed IS separately to avoid cartesian junk
    is_trail = _run_grid(
        _grid(
            exit_mode=["trail"], take_profit_pct=[0.0],
            trail_arm=is_arms, trail_giveback=is_gbs,
        ),
        is_replay,
        "Holdout IS trail neighborhood",
    )
    is_fixed = _run_grid(
        _grid(
            exit_mode=["fixed"], take_profit_pct=[0.60, 0.65],
            trail_arm=[0.65], trail_giveback=[0.05],
        ),
        is_replay,
        "Holdout IS fixed",
    )

    # Pick IS best by Sharpe across both
    candidates = []
    for key in is_trail.keys:
        candidates.append((key, _agg_from_result(is_trail, key), "trail"))
    for key in is_fixed.keys:
        candidates.append((key, _agg_from_result(is_fixed, key), "fixed"))
    candidates.sort(key=lambda x: x[1]["sharpe"], reverse=True)
    is_winner_key, is_winner_agg, is_winner_family = candidates[0]
    print(
        f"  IS winner: {is_winner_agg['label']} sharpe={is_winner_agg['sharpe']:.3f} "
        f"pnl={is_winner_agg['pnl']:.0f}"
    )

    # OOS: evaluate IS winner, frozen trail candidate, fixed 0.60
    oos_specs = [
        ("is_winner", {k: [v] for k, v in _key_dict(is_winner_key).items()}),
        (
            "trail_0.65_0.05",
            _grid(
                exit_mode=["trail"], take_profit_pct=[0.0],
                trail_arm=[0.65], trail_giveback=[0.05],
            ),
        ),
        (
            "fixed_0.60",
            _grid(
                exit_mode=["fixed"], take_profit_pct=[0.60],
                trail_arm=[0.65], trail_giveback=[0.05],
            ),
        ),
    ]
    oos_stats = {}
    for name, g in oos_specs:
        res = _run_grid(g, oos_replay, f"Holdout OOS {name}")
        key = res.keys[0]
        oos_stats[name] = _agg_from_result(res, key)
        oos_stats[name]["params"] = _key_dict(key)

    print("\n  OOS results:")
    for name, st in oos_stats.items():
        print(
            f"    {name:18} {st['label']:18} n={st['n']:3d} "
            f"pnl={st['pnl']:+8.0f} sharpe={st['sharpe']:.2f} dd={st['max_dd_pct']:.1f}%"
        )

    trail_beats_fixed_oos = (
        oos_stats["trail_0.65_0.05"]["pnl"] > oos_stats["fixed_0.60"]["pnl"]
        and oos_stats["trail_0.65_0.05"]["sharpe"] >= oos_stats["fixed_0.60"]["sharpe"] - 0.05
    )
    return {
        "split": split,
        "is_end": is_end,
        "is_winner": is_winner_agg,
        "is_winner_family": is_winner_family,
        "oos": oos_stats,
        "trail_beats_fixed_oos": trail_beats_fixed_oos,
    }


def phase_wfo_head_to_head() -> Dict[str, Any]:
    """(3) WFO: only fixed 0.60 vs trail 0.65/0.05."""
    print("\n── (3) WFO head-to-head fixed0.60 vs trail0.65/0.05 ──")
    # Two separate single-param grids can't be one cartesian cleanly with
    # different exit_mode semantics — use exit_mode×tp with one trail cell:
    # fixed@0.60 and trail@tp=0.
    grid = _grid(
        exit_mode=["fixed", "trail"],
        take_profit_pct=[0.0, 0.60],
        trail_arm=[0.65],
        trail_giveback=[0.05],
    )
    # This yields 4 cells; WFO will pick among all 4 including junk
    # (trail@0.60 ≡ trail@0, fixed@0). Collapse by running custom shortlist
    # via two 1-value grids is cleaner — use explicit 2-combo by only
    # allowing the meaningful pair through a tiny custom loop below.
    #
    # Hack: pass a grid that only has the two meaningful combos by using
    # exit_mode list length matching unique take_profit — engine cartesian
    # always multiplies. So implement mini-WFO here.

    from backtester.research.walk_forward import _build_windows, _daily_pnl_from_df, _simple_sharpe

    is_days, oos_days, step_days = 90, 30, 30
    t0 = datetime.strptime(DATE_START, "%Y-%m-%d").date()
    t1 = datetime.strptime(DATE_END, "%Y-%m-%d").date()
    windows = _build_windows(t0, t1, is_days, oos_days, step_days)
    print(f"  {len(windows)} windows (IS={is_days} OOS={oos_days} step={step_days})")

    configs = {
        "fixed_0.60": _grid(
            exit_mode=["fixed"], take_profit_pct=[0.60],
            trail_arm=[0.65], trail_giveback=[0.05],
        ),
        "trail_0.65_0.05": _grid(
            exit_mode=["trail"], take_profit_pct=[0.0],
            trail_arm=[0.65], trail_giveback=[0.05],
        ),
    }

    rows = []
    for i, (is_s, is_e, oos_s, oos_e) in enumerate(windows, 1):
        is_a, is_b = is_s.strftime("%Y-%m-%d"), is_e.strftime("%Y-%m-%d")
        oos_a, oos_b = oos_s.strftime("%Y-%m-%d"), oos_e.strftime("%Y-%m-%d")
        print(f"  Window {i}/{len(windows)}: IS[{is_a}→{is_b}] OOS[{oos_a}→{oos_b}]")
        is_replay = MarketReplay(
            _cfg.data.options_parquet, _cfg.data.spot_parquet, start=is_a, end=is_b
        )
        # Score each config on IS by GridResult sharpe (single combo each)
        is_scores = {}
        for name, g in configs.items():
            df, keys, nav, final, _ = run_grid_full(
                ThetaEngineV9, g, is_replay, progress=False
            )
            gr = GridResult(
                df, keys, nav, final, param_grid=g, account_size=CAPITAL,
                date_range=(is_a, is_b),
            )
            st = gr.all_stats[keys[0]]
            is_scores[name] = {
                "sharpe": float(st["sharpe"]),
                "pnl": float(st["total_pnl"]),
                "n": int(st["n"]),
            }
        # Pick IS winner by sharpe, tie-break pnl
        winner = max(
            is_scores.keys(),
            key=lambda n: (is_scores[n]["sharpe"], is_scores[n]["pnl"]),
        )
        # OOS both (for paired comparison) + record winner path
        oos_replay = MarketReplay(
            _cfg.data.options_parquet, _cfg.data.spot_parquet, start=oos_a, end=oos_b
        )
        oos_scores = {}
        for name, g in configs.items():
            df, keys, nav, final, _ = run_grid_full(
                ThetaEngineV9, g, oos_replay, progress=False
            )
            pnl = float(df["pnl"].sum()) if len(df) else 0.0
            daily = _daily_pnl_from_df(df, oos_a, oos_b)
            oos_scores[name] = {
                "pnl": pnl,
                "sharpe": _simple_sharpe(daily),
                "n": int(len(df)),
            }
        row = {
            "window": i,
            "is": f"{is_a}:{is_b}",
            "oos": f"{oos_a}:{oos_b}",
            "is_winner": winner,
            "is_fixed_sharpe": is_scores["fixed_0.60"]["sharpe"],
            "is_trail_sharpe": is_scores["trail_0.65_0.05"]["sharpe"],
            "oos_winner_pnl": oos_scores[winner]["pnl"],
            "oos_winner_sharpe": oos_scores[winner]["sharpe"],
            "oos_fixed_pnl": oos_scores["fixed_0.60"]["pnl"],
            "oos_trail_pnl": oos_scores["trail_0.65_0.05"]["pnl"],
            "oos_fixed_sharpe": oos_scores["fixed_0.60"]["sharpe"],
            "oos_trail_sharpe": oos_scores["trail_0.65_0.05"]["sharpe"],
            "oos_trail_minus_fixed_pnl": (
                oos_scores["trail_0.65_0.05"]["pnl"] - oos_scores["fixed_0.60"]["pnl"]
            ),
            "oos_win_deployed": oos_scores[winner]["pnl"] > 0,
        }
        rows.append(row)
        print(
            f"    IS→{winner}  OOS Δpnl(trail-fixed)="
            f"{row['oos_trail_minus_fixed_pnl']:+.0f}  "
            f"deployed_pnl={row['oos_winner_pnl']:+.0f}"
        )

    wfo = pd.DataFrame(rows)
    trail_is_wins = float((wfo.is_winner == "trail_0.65_0.05").mean())
    oos_delta = wfo.oos_trail_minus_fixed_pnl
    # bootstrap mean OOS delta across windows
    rng = np.random.default_rng(0)
    boots = rng.choice(oos_delta.to_numpy(), size=(5000, len(oos_delta)), replace=True).mean(1)
    lo, hi = np.quantile(boots, [0.025, 0.975])
    summary = {
        "n_windows": len(wfo),
        "trail_is_win_rate": trail_is_wins,
        "oos_deployed_win_rate": float(wfo.oos_win_deployed.mean()),
        "oos_deployed_pnl": float(wfo.oos_winner_pnl.sum()),
        "oos_trail_minus_fixed_mean": float(oos_delta.mean()),
        "oos_trail_minus_fixed_ci": (float(lo), float(hi)),
        "oos_trail_beats_fixed_frac": float((oos_delta > 0).mean()),
        "always_trail_oos_pnl": float(wfo.oos_trail_pnl.sum()),
        "always_fixed_oos_pnl": float(wfo.oos_fixed_pnl.sum()),
    }
    print("\n  WFO summary:")
    for k, v in summary.items():
        print(f"    {k}: {v}")
    return {"windows": wfo, "summary": summary}


def _verdict(local: Dict, holdout: Dict, wfo: Optional[Dict]) -> str:
    lines = []
    dsr = local["dsr_with_local"]
    dsr_ok = dsr is not None and dsr >= 0.95
    dsr_weak = dsr is not None and dsr < 0.5
    lines.append(
        f"DSR(with local budget)={dsr}  "
        f"[{'PASS ≥0.95' if dsr_ok else 'FAIL <0.5' if dsr_weak else 'WEAK/MID'}]"
    )
    lines.append(
        f"Local rank of 0.65/0.05: {local['cand_rank']}/{local['n_local']}  "
        f"(best local={local['best_local']['arm']}/{local['best_local']['gb']})"
    )
    lines.append(
        f"Holdout OOS trail vs fixed0.60: "
        f"pnl {holdout['oos']['trail_0.65_0.05']['pnl']:+.0f} vs "
        f"{holdout['oos']['fixed_0.60']['pnl']:+.0f}; "
        f"sharpe {holdout['oos']['trail_0.65_0.05']['sharpe']:.2f} vs "
        f"{holdout['oos']['fixed_0.60']['sharpe']:.2f}; "
        f"IS re-selected {holdout['is_winner']['label']}"
    )
    if wfo:
        s = wfo["summary"]
        lo, hi = s["oos_trail_minus_fixed_ci"]
        lines.append(
            f"WFO: trail IS-pick rate={s['trail_is_win_rate']:.0%}; "
            f"OOS mean Δpnl(trail-fixed)={s['oos_trail_minus_fixed_mean']:+.0f} "
            f"CI[{lo:+.0f},{hi:+.0f}]; "
            f"always-trail OOS pnl={s['always_trail_oos_pnl']:+.0f} vs "
            f"always-fixed={s['always_fixed_oos_pnl']:+.0f}"
        )
        ci_pos = lo > 0
    else:
        ci_pos = False

    # Decision rubric
    holdout_ok = holdout["trail_beats_fixed_oos"]
    hill_not_spike = local["cand_rank"] <= 5  # in top of local neighborhood

    if dsr_ok and holdout_ok and (wfo is None or ci_pos or wfo["summary"]["always_trail_oos_pnl"] > wfo["summary"]["always_fixed_oos_pnl"]):
        decision = "REAL EDGE (keep / promote to WFO-confirmed candidate)"
    elif holdout_ok and (wfo and wfo["summary"]["always_trail_oos_pnl"] > wfo["summary"]["always_fixed_oos_pnl"]) and not dsr_weak:
        decision = "SUGGESTIVE — OOS leans trail but multiple-testing not cleared; optional tiny live shadow only"
    elif not holdout_ok and (wfo is None or wfo["summary"]["always_trail_oos_pnl"] <= wfo["summary"]["always_fixed_oos_pnl"]):
        decision = "LUCK / NO EDGE — drop trail; keep fixed TP=0.60"
    else:
        decision = "INCONCLUSIVE / FRAGILE — do not promote; default to fixed TP=0.60"

    lines.append(f"DECISION: {decision}")
    if not hill_not_spike:
        lines.append("Note: candidate not near top of local hill → spike risk.")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-wfo", action="store_true")
    args = ap.parse_args()

    print("=" * 60)
    print("  theta_engine_v9 trail verification (luck vs edge)")
    print(f"  entry={ENTRY}  range={DATE_START}→{DATE_END}")
    print(f"  N_PRIOR exit trials on this entry ≈ {N_PRIOR}")
    print("=" * 60)
    t0 = time.time()

    replay = MarketReplay(
        _cfg.data.options_parquet,
        _cfg.data.spot_parquet,
        start=DATE_START,
        end=DATE_END,
    )
    print(f"  Full-range data: {len(replay._timestamps):,} intervals")

    local = phase_local_and_dsr(replay)
    holdout = phase_holdout()
    wfo = None if args.skip_wfo else phase_wfo_head_to_head()

    verdict = _verdict(local, holdout, wfo)
    print("\n" + "=" * 60)
    print(verdict)
    print("=" * 60)

    out_dir = Path(runs_dir())
    local["hill"].to_csv(out_dir / "theta_engine_v9_trail_local_hill.csv", index=False)
    if wfo is not None:
        wfo["windows"].to_csv(out_dir / "theta_engine_v9_trail_wfo_windows.csv", index=False)

    summary = {
        "entry": ENTRY,
        "date_range": [DATE_START, DATE_END],
        "n_prior": N_PRIOR,
        "candidate": local["candidate"],
        "control": local["control"],
        "cand_rank": local["cand_rank"],
        "dsr_prior_only": local["dsr_prior_only"],
        "dsr_with_local": local["dsr_with_local"],
        "dsr_best_local": local["dsr_best_local"],
        "best_local": {
            k: local["best_local"][k]
            for k in ("arm", "gb", "sharpe", "pnl", "n")
        },
        "holdout": {
            "split": holdout["split"],
            "is_winner": holdout["is_winner"],
            "oos": {
                k: {kk: vv for kk, vv in st.items() if kk != "params"}
                for k, st in holdout["oos"].items()
            },
            "trail_beats_fixed_oos": holdout["trail_beats_fixed_oos"],
        },
        "wfo": wfo["summary"] if wfo else None,
        "verdict": verdict,
        "elapsed_s": time.time() - t0,
    }
    path = out_dir / "theta_engine_v9_trail_verify.json"
    path.write_text(json.dumps(summary, indent=2, default=str))
    (out_dir / "theta_engine_v9_trail_verify_verdict.txt").write_text(verdict + "\n")
    print(f"\n  Wrote {path}")
    print(f"  Total: {time.time()-t0:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

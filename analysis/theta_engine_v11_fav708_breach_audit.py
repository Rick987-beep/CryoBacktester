#!/usr/bin/env python3
"""Bar-by-bar investor Greek audit for the three run-708 favourites.

Combos (after the sticky-wing expiry fix, 2025-04-11 → 2026-08-12):
  #1  RichForce16  dgv / 0.20 / next_listed   — compliance candidate
  #2  RichForce16  dg  / 0.10 / same          — D/G control
  #13 Daily15      dg  / 0.20 / same          — high-PnL stress twin

For every live bar (shorts open), records short-book-only vs full-book
(shorts + wing + perp) ``limits_ok`` checks, plus whether a wing is on.

Writes ``analysis/theta_engine_v11_fav708_breach_audit.json``.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtester.core.config import cfg as _cfg
from backtester.core.engine import _inject_indicators, _open_unrealized_pnl
from backtester.core.market_replay import MarketReplay
from backtester.core.portfolio_risk import DEFAULT_INVESTOR_LIMITS, limits_ok
from workspace.strategies.theta_engine._common import BASELINE_DAILY15, BASELINE_RICHFORCE16
from workspace.strategies.theta_engine.v11 import ThetaEngineV11

CAPITAL = float(_cfg.simulation.account_size_usd)
DATE_RANGE = ("2025-04-11", "2026-08-12")
OUT = ROOT / "analysis" / "theta_engine_v11_fav708_breach_audit.json"


def _base(**overrides: Any) -> Dict[str, Any]:
    g: Dict[str, Any] = {
        "delta": 0.25, "min_dte": 90, "hold_days": 0, "stop_loss_pct": 3.0,
        "take_profit_pct": 0.50, "max_concurrent": 20, "qty_per_1btc_equity": 0.2,
        "launch_accel": 0, "launch_size_mult": 1.0, "greek_limits_mode": "off",
        "perp_delta_hedge": 0, "perp_deadband_pct": 2.0,
        "option_hedge_mode": "sticky_budget",
        "wing_close_margin_pct": 3.0, "wing_min_hold_minutes": 60.0,
        "wing_cooldown_minutes": 60.0, "wing_cooldown_override_mult": 1.5,
        "wing_side_mode": "greek", "wing_delta_mode": "fixed", "wing_delta_ratio": 0.5,
    }
    g.update(overrides)
    return g


def _combos() -> List[Dict[str, Any]]:
    return [
        _base(
            label="#1 RF dgv/0.20/next",
            rank=1,
            entry_policy=BASELINE_RICHFORCE16,
            wing_trigger="dgv",
            wing_delta=0.20,
            wing_expiry_mode="next_listed",
        ),
        _base(
            label="#2 RF dg/0.10/same",
            rank=2,
            entry_policy=BASELINE_RICHFORCE16,
            wing_trigger="dg",
            wing_delta=0.10,
            wing_expiry_mode="same",
        ),
        _base(
            label="#13 D15 dg/0.20/same",
            rank=13,
            entry_policy=BASELINE_DAILY15,
            wing_trigger="dg",
            wing_delta=0.20,
            wing_expiry_mode="same",
        ),
    ]


def _dims(check) -> str:
    out = []
    if not check.delta_ok:
        out.append("D")
    if not check.gamma_ok:
        out.append("G")
    if not check.vega_ok:
        out.append("V")
    if not check.theta_ok:
        out.append("T")
    return "".join(out) or "-"


def _severity(greeks, check) -> float:
    ratios = []
    if not check.delta_ok and check.delta_band:
        ratios.append(abs(greeks.delta_pct) / check.delta_band)
    if not check.gamma_ok and DEFAULT_INVESTOR_LIMITS.gamma_pct_floor:
        ratios.append(greeks.gamma_pct / DEFAULT_INVESTOR_LIMITS.gamma_pct_floor)
    if not check.vega_ok and DEFAULT_INVESTOR_LIMITS.vega_pct_abs:
        ratios.append(abs(greeks.vega_pct) / DEFAULT_INVESTOR_LIMITS.vega_pct_abs)
    return max(ratios) if ratios else 0.0


def _pct(n: int, den: int) -> float:
    return 100.0 * float(n) / float(den) if den else 0.0


def main() -> None:
    date_from, date_to = DATE_RANGE
    lim = DEFAULT_INVESTOR_LIMITS
    print(f"Loading {date_from} → {date_to}")
    print(
        f"Investor limits: |D|<{lim.delta_pct_when_gamma_neg}% (γ<0) / "
        f"{lim.delta_pct_when_gamma_pos}% (γ>0),  "
        f"γ>{lim.gamma_pct_floor}%,  |V|<{lim.vega_pct_abs}%"
    )
    replay = MarketReplay(
        _cfg.data.options_parquet, _cfg.data.spot_parquet,
        start=date_from, end=date_to,
    )
    print(f"Intervals: {len(replay._timestamps):,}")

    combo_params = _combos()
    instances = []
    for p in combo_params:
        s = ThetaEngineV11()
        s.configure({k: v for k, v in p.items() if k not in ("label", "rank")})
        instances.append((p, s))
    _inject_indicators(ThetaEngineV11, [s for _, s in instances], replay, progress=True)

    n = len(instances)
    realized = [0.0] * n
    pos_caches: List[Dict[int, float]] = [{} for _ in range(n)]
    live = [0] * n
    full_ok = [0] * n
    short_ok = [0] * n
    recovered = [0] * n          # short breaching, full OK (hedge did its job)
    naked_breach = [0] * n       # full breaching, no wing
    hedged_breach = [0] * n      # full breaching, wing on
    wing_on = [0] * n
    short_d = [0] * n
    short_g = [0] * n
    short_v = [0] * n
    full_d = [0] * n
    full_g = [0] * n
    full_v = [0] * n
    worst: List[List[Dict[str, Any]]] = [[] for _ in range(n)]

    t0 = time.time()
    n_states = 0
    last_state = None
    for state in replay:
        n_states += 1
        last_state = state
        for i, (_, strat) in enumerate(instances):
            state.equity_usd = CAPITAL + realized[i]
            open_pnl = _open_unrealized_pnl(strat, state, pos_caches[i])
            overlay = float(strat.perp_mark_pnl(state.spot)) + float(strat.wing_mark_pnl(state))
            state.nav_usd = state.equity_usd + open_pnl + overlay
            for trade in strat.on_market_state(state):
                if getattr(trade, "side", "close") == "close":
                    realized[i] += float(trade.pnl)

            if strat.n_short_positions() == 0:
                continue
            live[i] += 1
            has_wing = strat._wing_position() is not None
            if has_wing:
                wing_on[i] += 1

            sg = strat._shorts_greeks_with_perp(state)
            sc = limits_ok(sg, DEFAULT_INVESTOR_LIMITS)
            fg = strat._portfolio_greeks_with_overlays(state)
            fc = limits_ok(fg, DEFAULT_INVESTOR_LIMITS)

            if sc.ok:
                short_ok[i] += 1
            else:
                if not sc.delta_ok:
                    short_d[i] += 1
                if not sc.gamma_ok:
                    short_g[i] += 1
                if not sc.vega_ok:
                    short_v[i] += 1
            if fc.ok:
                full_ok[i] += 1
                if not sc.ok:
                    recovered[i] += 1
            else:
                if has_wing:
                    hedged_breach[i] += 1
                else:
                    naked_breach[i] += 1
                if not fc.delta_ok:
                    full_d[i] += 1
                if not fc.gamma_ok:
                    full_g[i] += 1
                if not fc.vega_ok:
                    full_v[i] += 1
                sev = _severity(fg, fc)
                worst[i].append({
                    "dt": state.dt.isoformat(),
                    "sev": round(sev, 2),
                    "dims": _dims(fc),
                    "delta_pct": round(fg.delta_pct, 2),
                    "gamma_pct": round(fg.gamma_pct, 2),
                    "vega_pct": round(fg.vega_pct, 2),
                    "n_short": strat.n_short_positions(),
                    "has_wing": has_wing,
                    "short_ok": sc.ok,
                    "short_dims": _dims(sc),
                })
        if n_states % 20000 == 0:
            print(f"  … {n_states} states ({time.time() - t0:.0f}s)")

    if last_state is not None:
        for i, (_, strat) in enumerate(instances):
            for trade in strat.on_end(last_state):
                if getattr(trade, "side", "close") == "close":
                    realized[i] += float(trade.pnl)

    print(f"Done {n_states} states, {n} combos in {time.time() - t0:.1f}s\n")

    rows = []
    hdr = (
        f"{'combo':<24} {'live':>7} {'wing_on':>8} {'short_ok':>9} "
        f"{'full_ok':>8} {'recovered':>10} {'naked':>7} {'hedgedX':>8}"
    )
    print(hdr)
    print("-" * len(hdr))
    for i, (p, strat) in enumerate(instances):
        lb = live[i] or 1
        row = {
            "label": p["label"],
            "rank": p["rank"],
            "entry_policy": p["entry_policy"],
            "wing_trigger": p["wing_trigger"],
            "wing_delta": p["wing_delta"],
            "wing_expiry_mode": p["wing_expiry_mode"],
            "live_bars": live[i],
            "wing_on_pct": round(_pct(wing_on[i], lb), 2),
            "short_ok_pct": round(_pct(short_ok[i], lb), 2),
            "full_ok_pct": round(_pct(full_ok[i], lb), 2),
            "recovered_pct": round(_pct(recovered[i], lb), 2),
            "naked_breach_pct": round(_pct(naked_breach[i], lb), 2),
            "hedged_still_breach_pct": round(_pct(hedged_breach[i], lb), 2),
            "short_breach_d_pct": round(_pct(short_d[i], lb), 2),
            "short_breach_g_pct": round(_pct(short_g[i], lb), 2),
            "short_breach_v_pct": round(_pct(short_v[i], lb), 2),
            "full_breach_d_pct": round(_pct(full_d[i], lb), 2),
            "full_breach_g_pct": round(_pct(full_g[i], lb), 2),
            "full_breach_v_pct": round(_pct(full_v[i], lb), 2),
            "telemetry": strat.risk_telemetry(),
        }
        rows.append(row)
        print(
            f"{p['label']:<24} {live[i]:>7} {_pct(wing_on[i], lb):>7.1f}% "
            f"{_pct(short_ok[i], lb):>8.1f}% {_pct(full_ok[i], lb):>7.1f}% "
            f"{_pct(recovered[i], lb):>9.1f}% {_pct(naked_breach[i], lb):>6.1f}% "
            f"{_pct(hedged_breach[i], lb):>7.1f}%"
        )

    print("\n=== Per-Greek full-book breach % (live bars) ===")
    print(f"{'combo':<24} {'D':>8} {'G':>8} {'V':>8}   (short-only D/G/V)")
    for i, (p, _) in enumerate(instances):
        lb = live[i] or 1
        print(
            f"{p['label']:<24} {_pct(full_d[i], lb):7.1f}% {_pct(full_g[i], lb):7.1f}% "
            f"{_pct(full_v[i], lb):7.1f}%   "
            f"({_pct(short_d[i], lb):.1f}/{_pct(short_g[i], lb):.1f}/{_pct(short_v[i], lb):.1f})"
        )

    print("\n=== Severity (post-hedge, live bars) ===")
    for i, (p, _) in enumerate(instances):
        sevs = [r["sev"] for r in worst[i]]
        lb = live[i] or 1
        mild = sum(1 for s in sevs if 0 < s <= 2)
        moderate = sum(1 for s in sevs if 2 < s <= 10)
        severe = sum(1 for s in sevs if s > 10)
        print(
            f"{p['label']:<24} mild(<=2x)={_pct(mild, lb):5.1f}%  "
            f"mod(2-10x)={_pct(moderate, lb):5.1f}%  "
            f"severe(>10x)={_pct(severe, lb):5.1f}%  "
            f"max={max(sevs) if sevs else 0:.1f}x"
        )

    print("\n=== Worst 8 lapses per combo ===")
    for i, (p, _) in enumerate(instances):
        print(f"\n-- {p['label']} --")
        top = sorted(worst[i], key=lambda r: -r["sev"])[:8]
        for r in top:
            print(
                f"  {r['dt']}  sev={r['sev']:.1f}x  full={r['dims']} short={r['short_dims']}  "
                f"D%={r['delta_pct']:+.1f} G%={r['gamma_pct']:+.1f} V%={r['vega_pct']:+.1f}  "
                f"n_short={r['n_short']} wing={r['has_wing']}"
            )

    payload = {
        "date_range": list(DATE_RANGE),
        "limits": {
            "delta_pct_when_gamma_neg": lim.delta_pct_when_gamma_neg,
            "delta_pct_when_gamma_pos": lim.delta_pct_when_gamma_pos,
            "gamma_pct_floor": lim.gamma_pct_floor,
            "vega_pct_abs": lim.vega_pct_abs,
        },
        "combos": rows,
        "worst_top8": {
            instances[i][0]["label"]: sorted(worst[i], key=lambda r: -r["sev"])[:8]
            for i in range(n)
        },
    }
    OUT.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()

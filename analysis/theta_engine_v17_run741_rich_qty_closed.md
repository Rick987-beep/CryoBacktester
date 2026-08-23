# Run #741 — theta_engine_v17 rich qty discovery (CLOSED)

**Bundle:** `theta_engine_v17_20260822_090110` (UI run #741)  
**Window:** 2025-08-17 → 2026-08-18 · **216 combos**  
**Book:** RichForce2 16 front (`rf2_1600` / run-727 hash `c8573c839903`)

Also on the bundle: `data/runs/…/NOTES.md` and `meta.json` → `research_note` (gitignored).

## What was attempted

Scale **base entry quantity** down when the sold 1 DTE 25Δ leg looks expensive, hoping for **better headline metrics** (more PnL, less drawdown, higher Sharpe) — not Greek-limit compliance.

Richness → multiplicative factor on `qty_per_1btc_equity` base:

- Input 1: `front_vrp` = sold mark IV − DVOL (`vrp_lo` / `vrp_hi` / `alpha_vrp`)
- Input 2: `iv_rank_60` = 60d rank of sold-side 25Δ front IV (`rank_lo` / `rank_hi` / `alpha_rank`)
- Combine: `f = clip(f_vrp × f_rank, f_min, 1)`; `qty = round(q0 × f)`

Grid: `rich_mode` none|on × vrp_hi × alpha_vrp × rank_hi × alpha_rank × f_min (108 naked baseline cells + 108 sized cells). Skew side and 25Δ strike unchanged.

## Result

**Did not bring the desired effect.** Closed as a performance search.

| | Naked (`rich_mode=none`) | Best `on` | Median `on` |
|--|--:|--:|--:|
| PnL | +~$47k | +~$32k | +~$26k |
| Sharpe | ~3.25 | ~2.64 | ~2.30 |
| Max DD | ~−9.0% | still worse Calmar | ~−7.8% (mild only) |

- **0 / 108** `on` cells beat naked on PnL, Sharpe, or Calmar (ret/|DD|)
- Same 187 trades and ~88.8% WR — pure size cut, no selection change
- Cuts hit **winners harder than losers**
- Mild DD / Greek-breach relief only — not enough to offset return loss

Gentlest knobs lost least; aggressive `alpha_*` / early `rank_hi=0.85` lost most.

## Disposition

Default strategy grid is `rich_mode=none`. Logic + `V17_RICH_DISCOVERY_GRID` kept in `workspace/strategies/theta_engine/v17.py` as artefact. See also `AGENTS.md` research learnings and CHANGELOG 2026-08-22.

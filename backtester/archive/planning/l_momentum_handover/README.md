# l_momentum — Research Handover Package

**Strategy:** `l_momentum` — Long directional BTC options gated by multi-timeframe spot momentum  
**Backtest run:** 2026-01-01 → 2026-05-12 (131 days)  
**Run ID:** 15 | Bundle: `backtester/reports/l_momentum_20260516_100720.bundle`  
**Generated:** 2026-05-16  

---

## Contents

| File | Description |
|------|-------------|
| `starred_runs.md` | Parameters, metrics, and notes for the 3 starred combos |
| `starred_combos_summary.csv` | One-row-per-combo quick stats |
| `combo_A_trades.csv` | Full trade log — Combo A |
| `combo_B_trades.csv` | Full trade log — Combo B |
| `combo_C_trades.csv` | Full trade log — Combo C |
| `combo_A_equity.csv` | Daily equity curve — Combo A |
| `combo_B_equity.csv` | Daily equity curve — Combo B |
| `combo_C_equity.csv` | Daily equity curve — Combo C |
| `strategy_code.md` | Strategy source + entry/exit logic explanation |
| `l_momentum.py` | Copy of strategy source file |
| `coding_notes.md` | Implementation notes for AI agents |

---

## Quick read

- The three combos all share `tp_mult=2.5`, `spot_stop_pct=0.0` (stop disabled), `time_gate_h=48`
- They differ only in `mom_4h_thr` and `mom_1h_thr` (momentum filter strictness)
- All are profitable over the full period; recency gate (last 26 days) vetoed all combos — see `starred_runs.md §Notes`
- Primary research question for next step: **why did all combos underperform in the last ~26 days?**

---

## Research source

Original signal research: `IndicatorBench/research/long_tradable_options/KERNEL_STRATEGY.md`

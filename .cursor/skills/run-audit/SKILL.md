---
name: run-audit
description: >-
  Analyse existing CryoBacktester runs for grid quality: parameter influence
  (η²), dangerous settings, curve-fitting / multiplicity, and diverse live
  combo suggestions. Use when the user asks to analyse a run, autopsy a grid,
  ask what drives results, what is dangerous, how much overfitting, or which
  combos to take live. Prefer python -m backtester.research.run_audit (or
  inspect audit) — do not reinvent full-grid stats.
---

# Run audit

## When to use

User wants a **quality autopsy** of an existing backtester run (by run id,
bundle, or after browsing the UI), answering:

1. Which parameter has what influence on the results?
2. Which setting is the most "dangerous"?
3. How much curve-fitting is going on in this run?
4. Which 2 or three combos do you suggest for live trading (very different if possible)?

## Do not improvise the math

```bash
python -m backtester.research.run_audit RUN [--html] [--out-dir DIR]
# alias:
python -m backtester.inspect audit RUN [--html]
```

Resolve the run with **run-lookup** / `inspect show` first if the identity is
ambiguous. Never `StoreService.load_run()` for the full grid. The audit module
reads bundle parquets and uses the same `_all_combo_stats` path as inspect.

## Workflow

1. `python -m backtester.inspect show RUN` — lock identity / date range / n_combos
2. `python -m backtester.research.run_audit RUN --html` — write `audit.json` (+ `report.html`)
3. Read `analysis/run_audit/<bundle_stem>/audit.json` (or `--out-dir`)
4. Present findings: influence → danger → curve-fit → live picks
5. Re-check final pick metrics with `inspect combo RUN HASH` (authoritative UI numbers)
6. Deliverable: Cursor canvas **or** point at `report.html`; keep chat short

## Interpretation rules

| Signal | Meaning |
|--------|---------|
| High η² on one param | That axis drives the grid; discuss its levels first |
| `pct_perfect_wr` high / top-decile perfect WR | Thin-tail Sharpe mirage — do not recommend those cells live |
| `effective_grid_shrink` ≫ 0 | Many knobs are inert duplicates (often proximity / SL) |
| `curve_fit.verdict.level` HIGH | Pre-narrowed island + multiplicity; discount leaderboard |
| Live picks require `n_loss ≥ 2` by default | Observed left tail; override only with `--allow-perfect-wr` |
| Picks share turb+delta+entry | Rejected as twins — diversify on high-η² axes |
| Low-delta + near-max WR in picks | Agent judgment: may still be thin-tail; prefer mid-delta siblings when η²(delta) is Sharpe-inflating |

## Flexible documents

- JSON pack is the source of truth (`schema_version`)
- HTML is a **section kit** (`backtester.research.run_audit.render`): summary,
  influence, danger, curve_fit, live — omit/replace/add sections via
  `extra_sections` for strategy-specific appendices (wings, Greeks, …)
- Canvas: embed the same four answers; do not paste huge tables into chat

## Live filter knobs

Defaults match an “honest” screen. Override on the CLI when the strategy needs
it (`--min-n`, `--min-n-loss`, `--max-win-rate`, `--max-dd-pct`, …).

## See also

- [reference.md](reference.md) — pack schema, module map
- **run-lookup** skill — identity / trades / fills
- **livecompare** — live vs backtest after a pick is deployed

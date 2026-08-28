# Run audit — reference

## Commands

```bash
python -m backtester.research.run_audit 748 --html
python -m backtester.inspect audit 748 --html --out-dir /tmp/audit748
python -m backtester.research.run_audit RUN --stdout   # full JSON to stdout
```

Default output dir: `analysis/run_audit/<bundle_stem>/`

| File | Role |
|------|------|
| `audit.json` | Full pack (schema_version 1) |
| `report.html` | Optional section-kit document |

## Module map

```
backtester/research/run_audit/
  frame.py       # build combo frame via inspect resolve + _all_combo_stats
  influence.py   # η², level stats, heats, inert fractions
  danger.py      # danger ranking + verdict
  curve_fit.py   # duplicates, half-split, neighbor plateau, verdict
  candidates.py  # honest live picks + diversity
  compute.py     # audit_run(ResolvedRun) → pack
  render.py      # HTML section kit
  cli.py         # argparse entry
```

Identity resolution: `backtester.inspect.resolve` (same as run-lookup).

## Pack schema (v1) — key blocks

| Key | Contents |
|-----|----------|
| `meta` | run_id, bundle, strategy, dates, varying/fixed params |
| `questions` | The four mandatory analyst questions (verbatim) |
| `grid_summary` | % profit, Sharpe dist, unique outcomes, half-split stats |
| `influence` / `influence_bar` | Per-param η² + level tables |
| `heats` | Median Sharpe/PnL matrices (top η² axes by default) |
| `danger_rank` / `danger_verdict` | Ranked dangerous levels + headline |
| `curve_fit.verdict` | LOW/MODERATE/HIGH + evidence bullets |
| `live_candidates` | `picks` (diverse), `top_pnl_with_losses`, filter `config` |
| `top10_sharpe` / `top10_pnl` | Context only — not live recommendations |

## Metrics alignment

Compact Sharpe / max_dd / PnL / WR / PF come from `_all_combo_stats` (same as
`inspect combo`). Calmar on the frame is `ann_return / (max_dd_pct/100)` for
ranking; **re-fetch** pick metrics via `inspect combo` before quoting to the
user as final.

## Extending HTML

```python
from backtester.research.run_audit.render import render_html, section_summary

def section_wings(pack):
    return "<section id='wings'><h2>Wings</h2>...</section>"

html = render_html(pack, extra_sections=[("wings", section_wings)])
# Or replace defaults entirely with sections=[...]
```

## Curve-fit heuristic

Score accumulates from: high duplicate shrink, high perfect-WR share / top-decile
perfect WR, low H1↔H2 Spearman, and ≥90% cells profitable. Thresholds →
LOW / MODERATE / HIGH. Evidence bullets are always emitted for the agent narrative.

## Live pick defaults (`LivePickConfig`)

`min_n=40`, `min_n_loss=2`, `max_win_rate=0.97`, `max_dd_pct=12`, `min_sharpe=1.5`,
`min_profit_factor=1.3`, both halves profitable, greedy diversity with
`min_param_distance=3`.

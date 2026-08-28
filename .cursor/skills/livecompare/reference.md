# Livecompare reference

## Pipeline stages

1. `resolve_config` — CryoTrader slot TOML → BT strategy + param grid
2. `pull_blotter` — SCP `slot-NN.jsonl` from VPS
3. `data_coverage` — parquet availability check
4. `run_bt` — single-combo `tudysho_eisbach` (or mapped strategy)
5. `match_trades` — join on entry_date + schedule + comparability codes
6. `forensics` — partial fills, loss stops, config drift
7. `report` — summary.md + report.html

## Known parity gaps (not bugs)

| Gap | Live | BT |
|-----|------|-----|
| Fill model | mark fallback when bid=0 | bid > 0 required |
| Sizing | 0.28% / 4.36 / max 21 | 0.8% / 12 on $100k |
| mon_early | `enabled=false` in TOML | turb=999 workaround |
| SL label | `combined_mark_sl(metadata)` | `stop_loss` |

## Package layout

```
analysis/livecompare/          # config + runs output
backtester/compare/            # Python package
.cursor/skills/livecompare/    # agent skill
```

## Related

To locate an existing backtester run / combo / trade log (not live parity), use
`python -m backtester.inspect` and `.cursor/skills/run-lookup/` — do not glob
`data/runs/` or call `StoreService.load_run()` for lookup. For grid-quality
autopsy (influence / danger / curve-fit / live picks), use **run-audit**
(`.cursor/skills/run-audit/` / `python -m backtester.research.run_audit`).

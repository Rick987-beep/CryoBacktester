---
name: run-lookup
description: >-
  Locate CryoBacktester runs and combos and retrieve metrics, params, trade
  logs, and fills via python -m backtester.inspect. Use when the user or task
  mentions a run id, run bundle, combo hash, favourite, trade log, fills,
  params for a combo, or performance metrics from a past backtest — do not
  glob data/runs/ or call StoreService.load_run() for lookup.
---

# Run lookup

## When to use

Need a past backtester **run**, **combo(s)**, **metrics**, **params**, **trades**, or **fills**.

## Do not improvise

```bash
python -m backtester.inspect <command> …
```

Never:

- Glob `data/runs/` or open HTML reports to find combos
- Reimplement `key_hash`
- Call `StoreService.load_run()` for lookup (loads the whole grid)

If the CLI returns 0 or >1 matches → **stop** and show candidates. Do not pick.

## Identities

| Token | Meaning |
|-------|---------|
| `run_id` | SQLite id in `backtester/ui/state/ui_state.db` (e.g. `727`) |
| `bundle` | `{strategy}_{YYYYMMDD_HHMMSS}.bundle` under `data/runs/` |
| `combo_hash` | 12-char hash of the param tuple (preferred combo handle) |
| `combo_idx` | 0-based index **inside that run only** |

Same hash can appear in multiple runs — use `find` then pick a run.

## Commands

```bash
python -m backtester.inspect runs [--strategy STR] [--family FAM] [--since DATE]
python -m backtester.inspect show RUN
python -m backtester.inspect find HASH
python -m backtester.inspect combos RUN [--hash H] [--idx N] [--param k=v] [--q TEXT] [--top N] [--metrics]
python -m backtester.inspect combo RUN COMBO [--full]
python -m backtester.inspect trades RUN COMBO [--since D] [--until D] [--pnl-lt X] [--exit-reason R] [--out PATH]
python -m backtester.inspect fills RUN COMBO [--trade-idx N] [--event open|close]
python -m backtester.inspect schema trades|fills|nav|greeks
python -m backtester.inspect favs [--strategy STR]
```

`RUN` = id, bundle path/dirname, or unique fragment.  
`COMBO` = hash, `#idx`, bare idx, or unique `--param k=v`.

Default stdout is **JSON**. Add `--table` for humans.

## Workflow

1. `show` / `runs` / `find` → lock the run
2. `combos` / `combo` → metrics + params (1..N)
3. `trades` / `fills` → dump or filter rows (`schema` for columns)
4. Further stats: work on the dumped rows (script/shell). Do not load the full grid.
   For **grid-quality autopsy** (influence / danger / curve-fit / live picks), use
   the **run-audit** skill / `python -m backtester.research.run_audit RUN`.

## Notes

- Compact metrics: `n`, `total_pnl`, `win_rate`, `profit_factor`, `sharpe`, `calmar`, `max_dd_pct`, `ann_return`. `score` is null in the fast path (relative to full grid).
- `combo` includes UI **favourite** name/note when starred.
- Roots: `CRYOBT_RUNS` / `data/runs/`; state: `backtester/ui/state/`.

Bundle layout, trade columns, and metric definitions: [reference.md](reference.md).

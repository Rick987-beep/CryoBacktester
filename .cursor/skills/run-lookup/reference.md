# Run-lookup reference

Agent-facing detail for `python -m backtester.inspect` and CryoBacktester run bundles.
Read this when resolving identities, interpreting metrics, or extending the CLI.

## Package layout

```
backtester/inspect/           # product CLI (resolve + filtered parquet loads)
.cursor/skills/run-lookup/    # agent skill (when / how)
data/runs/*.bundle/           # on-disk run artifacts
backtester/ui/state/ui_state.db   # run_id index + favourites
```

## Do not

- Glob `data/runs/` or parse HTML reports to find combos
- Reimplement `key_hash` — use `backtester.ui.services.store_service.key_hash`
- Call `StoreService.load_run()` for lookup (rebuilds full `GridResult` for every combo)
- Treat `analysis/` extracted combo folders as the catalog (inspect does not search them)

## Identities

| Token | Source | Stable across machines? |
|-------|--------|-------------------------|
| `run_id` | `ui_state.db` `runs.id` | No (local index) |
| `bundle` name/path | `data/runs/` (`CRYOBT_RUNS`) | Yes if you copy the dir |
| `combo_hash` | `sha256(key_to_json(param_tuple))[:12]` | Yes for identical params |
| `combo_idx` | position in `meta.json` `keys` | Only within that run |

`combo_hash` is the preferred handle in AGENTS.md, favourites, and research notes.

## Bundle files

Written by `StoreService.write_bundle()`:

| Path | Role |
|------|------|
| `meta.json` | strategy, family, param_grid, keys, date_range, account_size, n_combos, n_trades, runtime_s, source, created_at, git_sha/git_dirty, config_hash, optional sidecars / strategy_source / wfo_result |
| `trade_log.parquet` | closed trades for all combos |
| `nav_daily.parquet` | daily NAV low/high/close (+ realized_close when present) |
| `final_nav.parquet` | final_nav, realized_pnl, open_pnl per combo_idx |
| `fills.parquet` | optional leg-level open/close fills |
| sidecar `*.parquet` | strategy extras (e.g. `investor_greeks.parquet`); listed in `meta.sidecars` |
| `strategy/*.py` | optional source snapshot at run time |

HTML: `data/runs/<stem>_<timestamp>.html` next to the bundle (not inside it).

### Trade log columns (typical)

`combo_idx`, `entry_time`, `exit_time`, `entry_spot`, `exit_spot`, `entry_price_usd`, `exit_price_usd`, `fees`, `pnl`, `triggered`, `exit_reason`, `exit_hour`, `entry_date`, `status`

### Fills columns (typical)

`combo_idx`, `trade_idx`, `open_idx`, `ts`, `event` (`open`/`close`), `contract`, `side`, `qty`, prices/fees in BTC and USD, `exit_reason`, `comment`, `status`

Use `python -m backtester.inspect schema trades|fills|nav|greeks` for the skill’s schema constants (greeks columns are strategy-defined).

## Metrics

Not persisted as a metrics parquet. Inspect recomputes for the requested combo(s) only via:

- `_all_combo_stats` on filtered trade_log + nav_daily
- `equity_metrics` for Calmar / Sortino / streaks when needed

Compact set: `n`, `total_pnl`, `win_rate`, `profit_factor`, `sharpe`, `calmar`, `max_dd_pct`, `ann_return`, `score`.

`score` is a **full-grid** percentile composite (`config.toml` `[scoring]` + optional recency). Fast path leaves it `null`; UI favourites may cache a score from when the combo was starred.

Default capital: `simulation.account_size_usd` in config (typically 100_000).

## CLI map

| Command | Purpose |
|---------|---------|
| `runs` | List indexed runs (strategy/family/since filters) |
| `show RUN` | Bundle meta summary + file list |
| `find HASH` | Favourites first, then meta scan across runs |
| `combos RUN` | Filter/list combos; `--metrics` / `--top N` |
| `combo RUN COMBO` | Params + metrics (+ favourite name/note) |
| `trades` / `fills` | Dump/filter rows; `--out` + `--format csv\|parquet\|json` |
| `schema` | Column lists |
| `favs` | Starred combos from SQLite |
| `audit RUN` | Grid quality autopsy → `backtester.research.run_audit` |

`RUN` = id | path | dirname | unique fragment.  
`COMBO` = hash | `#idx` | bare idx | unique `--param k=v`.

For influence / danger / curve-fit / live picks, use the **run-audit** skill
(`.cursor/skills/run-audit/`) rather than ad-hoc full-grid scripts.

Exit `2` + `candidates` → ambiguous; do not guess.

## Performance notes

- Cold `python -m …` pays ~0.5–0.7 s import; in-process resolve/metrics for one combo is tens of ms.
- `find` can meta-scan many runs — prefer an explicit run id when known.
- `runs` may call `scan_bundles()` to register new on-disk bundles into SQLite.

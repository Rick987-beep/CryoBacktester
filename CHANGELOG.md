# Changelog

All notable changes to CryoBacktester are documented here.

---

## [Unreleased] — 2026-05-14

### New Features

#### Interactive Research UI (`backtester/ui/`)
A Panel + Bokeh + Plotly web app for exploring backtest results without re-running the engine.

```bash
python -m backtester.ui.app              # http://localhost:5006
python -m backtester.ui.app --port 5007
python -m backtester.ui.app --dev        # autoreload
```

- **`app.py`** — entry point; serves the Panel app via Tornado.
- **`state.py`** — shared reactive `AppState` param object.
- **`log.py`** — UI-scoped logger.
- **`views/`** — one file per tab: `sidebar`, `grid_view`, `detail_view`, `overlay_view`, `favourites_view`, `compare_view`.
- **`services/`** — data-access layer: `store_service` (SQLite run index + bundle persistence), `cache_service` (LRU `ResultCache`), `equity_service`, `run_service`, `run_worker` (background backtest thread), `repro`, `toml_export`.
- **`charts/equity.py`** — Plotly equity + drawdown chart builders.
- **`state/ui_state.db`** — SQLite DB for starred combos and user preferences (gitignored).

#### `run.py` — `run_backtest()` public API
- New `run_backtest()` function callable by the UI worker and tests: accepts `strategy_key`, `param_grid`, `date_range`, `account_size`, `bundles_root`, optional `progress_cb`, and `source` label; writes both an HTML report and a run bundle, returning the bundle path.
- CLI (`main()`) now writes a `.bundle/` directory after each run (skippable with `--no-bundle`).

#### `engine.py` — `progress_cb` parameter
- `run_grid_full()` gains `progress_cb` (callable) and `progress_cb_interval` (default 50 states) parameters so the UI worker can stream live progress to the frontend.

#### `backtester/run_ui_test.py`
- Quick 36-combo test script (`python -m backtester.run_ui_test`) over a trimmed grid and 90-day window for fast UI development iteration. Not for research use.

#### Test suite expansion (`tests/`)
- 128 new UI unit tests across `tests/ui/` covering: boot, store/cache services, grid filter parser, equity charts, detail/overlay/favourites/compare views, TOML export, URL state, column presets, CSV export, range shorthand parser, run service, and run worker.
- `tests/test_engine_progress_cb.py` — two tests covering `progress_cb` invocation and error isolation.
- `pyproject.toml`: added `slow_ui` marker (excluded by default); `testpaths` now includes both `backtester/strategies/tests` and `tests/`.

### Changes

#### Dependencies (`requirements.txt`)
- Added `panel>=1.4,<2`, `bokeh>=3.4,<4`, `plotly>=5.20` for the interactive UI.

#### `.gitignore`
- Added `logs/` (runtime UI worker logs) and `backtester/ui/state/ui_state.db`.

#### `README.md`
- Added **Research UI** section with tab descriptions, filter syntax reference, and persistence notes.
- Updated repo structure diagram and testing docs to reflect `tests/ui/` and the new CLI flags.

#### `docs/upgrades/backtester-interactive-ui.md`
- Updated planning doc to reflect completed phases.

### Bug Fixes

#### `engine.py` — `progress_cb` signature incomplete
- `progress_cb` and `progress_cb_interval` were referenced in the loop body and called from `run.py` but were missing from `run_grid_full()`'s parameter list, causing a `TypeError` at runtime.
- Fixed undefined `logger` reference in the callback exception handler (replaced with inline `logging.getLogger(__name__)`).

---

## [7bc9c65] — 2026-05-13

### New Features

#### Fill-level trade log (`df_fills`)
- `engine.run_grid_full()` now returns a 5-tuple: `(df, keys, nav_daily_df, final_nav_df, df_fills)`.
- `df_fills` contains one row per leg per open/close event across all combos — enabling per-contract fill inspection.
- `_append_fills()` internal helper expands `Trade` objects into fill rows, with `pos_id`-based open/close linkage.
- `GridResult` accepts and stores `df_fills`; exposes `df_fills_best` (fills filtered & sorted for the best combo).
- HTML report now renders a fills table for the best combo when fill data is present.

#### New strategy: `hedged_put_sell`
- Sells OTM puts on a configurable weekday/hour schedule with cooldown and `max_concurrent` positions.
- Automatically opens a long put hedge when the short put's `abs(delta)` crosses `hedge_trigger_delta`; sizes the hedge to approximate delta-neutrality.
- Multiple hedge cycles per position are supported.
- Exits on take-profit or expiry; hedge is closed first.
- Registered in the strategy registry (`run.py`) under the name `hedged_put_sell`.

#### VPS data sync
- Added `backtester/ingest/sync_vps.py`: rsync-based script to pull daily parquets from the CryoTrader VPS.
- Added `.env.example` with SSH configuration template (`RECORDER_VPS_HOST`, `RECORDER_VPS_DATA_DIR`, `RECORDER_SSH_KEY`).

#### Trade status bitmask support
- `Trade` dataclass gains two new optional fields: `status: int` (strategy-defined bitmask) and `side: str` (`"open"` or `"close"`).
- `run.py` reads `TRADE_STATUS` / `STATUS_LABELS` from the strategy class and forwards it to `generate_html()` as `status_labels`.
- `reporting_v2.generate_html()` accepts `status_labels` and uses `_decode_status()` to render flag names in the fills table.

#### Docs
- Added `docs/upgrades/backtester-interactive-ui.md` (planning document for an interactive HTML report upgrade).

### Changes

#### `strategy_base.py` — bug fixes
- **`_reprice_legs()`**: reprice now correctly multiplies by `leg["qty"]` for all three price paths (ask, bid, mark fallback). Previously all multi-lot positions were mispriced at 1× quantity.
- **`close_short_strangle()`**: expiry settlement and live-reprice exit prices now scale by `quantity` from `pos.metadata`. Previously settlement P&L ignored position size.
- **`close_trade()`**: metadata passed to `Trade` now includes `legs` and `fees_open` automatically, enabling `_append_fills()` to reconstruct fill rows without strategy changes.

#### `reporting_v2.py` — visual redesign
- Full CSS overhaul: dark-blue (`#1565c0`) accent replacing the previous gray/green palette.
- Font stack updated to `Inter / Helvetica Neue`.
- Tables: zebra-striped rows, hover highlight, sticky blue header, 12 px body font.
- Metric labels styled with uppercase + letter-spacing.
- `best-box` uses a left-border accent strip instead of a filled background.
- Heatmap wrappers get a subtle box-shadow and rounded corners.
- `.fills-odd` / `.fills-even` CSS classes added for the new fills table.

#### `results.py`
- `GridResult.__init__()` accepts optional `df_fills` parameter (default `None`; backward-compatible).
- Docstring updated to document the new `df_fills` / `df_fills_best` attributes.

#### `run.py`
- Unpacks 5-tuple from `run_grid_full()`.
- Passes `df_fills` to `GridResult` and `status_labels` to `generate_html()`.

#### `walk_forward.py`
- Minor compatibility fix (unpacks 5-tuple from engine).

#### `strategies/short_str_turb_dyn.py`
- Refactored to emit explicit `side="open"` Trade events and attach `pos_id` / `skip_open_fill` metadata for accurate fill-log generation.

#### `strategies/deltaswipswap.py`
- Minor metadata update.

### Documentation

- Root `README.md` substantially expanded: includes the full workflow, research pipeline, CLI reference, and data ingestion docs previously living in `backtester/README.md`.
- `backtester/README.md` deleted (content merged into root `README.md`).

---

## [b46c5bf] — 2026-05-XX (initial public commit)

- Initial CryoBacktester repository: engine, market replay, results, reporting, six strategies, Tardis ingest pipeline.

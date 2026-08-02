# Changelog

All notable changes to CryoBacktester are documented here.

---

## Checkpoint — 2026-08-02: theta_engine_v8 smart entry (DVOL / VRP)

Add vol-context features and named entry policies so short-vol v8 can sell when
options look rich vs realized vol, with staged sensitivity + WFO research hooks.
Risk budget (qty / max_concurrent) stays fixed across policies.

### Data plane & indicators

- `backtester.ingest.sync_dvol` — copy CryoQuant `BTC_DVOL` hive into
  `data/macro/deribit/BTC_DVOL/` (env: `CRYOBT_DVOL` / `CRYOBT_MACRO`).
- `backtester.indicators.vol_context` — daily panel `dvol`, Parkinson `rv30`
  (causal shift +1), `vrp = dvol − rv30`, `dvol_rank_60`; registered in
  `pipeline._BUILDERS` as `vol_context`.
- Paths: `macro_dir()` / `dvol_dir()`; `data/macro/` gitignored like other blobs.

### theta_engine_v8 entry policies

- `indicator_deps` → `vol_context` (1d); entry metadata logs `entry_reason`
  (`schedule` / `rich` / `forced`) plus dvol/rv30/vrp.
- Named `entry_policy` axis (discovery): `daily_12`, `daily_1430`,
  `sched_mon_thu_1430`, `rich{0,3,5}_force3_1430`, `rich3_force4_1430`.
- Modes: `daily_clock`, `schedule_2x`, `rich_or_forced` (VRP gate + Mon/Thu
  force after N days). Explicit knobs + `entry_schedule` for sensitivity.
- UI favourites (not in discovery grid): Sharpe / PnL / mid / MWF shortlist
  (`fav_*` policies).
- `DATE_RANGE` extended through **2026-08-01**.

### Research tooling

- Experiment TOMLs: staged clock → schedule → VRP → force + WFO geometry
  (`workspace/experiments/theta_engine_v8_entry_*.toml`).
- `analysis/theta_engine_v8_entry_stages.py` — orchestrated A→B→C→WFO.
- `analysis/theta_engine_v8_star_favourites.py` — 4-combo UI run + stars.
- Experiment `type = "list"` deviations; WFO accepts optional `param_grid`.

```bash
python -m backtester.ingest.sync_dvol
python -m backtester.run --strategy theta_engine_v8
PYTHONPATH=. python analysis/theta_engine_v8_entry_stages.py
```

---

## Checkpoint — 2026-08-02: theta_engine_v8 (locked skew6 side)

Ship a lean daily short-vol theta strategy that hardwires the v7 side-selection
research, and lock v7 defaults to the best-Sharpe combo from that work.

### theta_engine_v8

- New strategy ID `theta_engine_v8` (catalog + shim
  `backtester/strategies/theta_engine_v8.py`).
- Side selection hardwired: **skew6 @ 14 DTE**, band 0 (`sign(S)` → put/call;
  exact `S==0` skips the day). No `side_mode` / skew DTE / band knobs.
- Keeps timed hold + optional launch accel; **drops** hedge wing and portfolio
  open-PnL equity stop (`hedge_qty_mult` must stay 0).
- Lean `PARAM_GRID`: book/exits + launch; Mon–Fri entry hardwired.
- Module helpers: `compute_skew6`, `skew_zone`, neighbor RR listing.
- Tests: `workspace/tests/test_theta_engine_v8.py`.

```bash
python -m backtester.run --strategy theta_engine_v8
```

### theta_engine_v7 research lock

After skew A/B, DTE/band, and hold×SL×TP sweeps (DATE_RANGE 2025-04-11 →
2026-07-29): default grid / experiment TOML lock to skew6, `skew_dte=14`,
band=0/skip, hold=0, SL=3.0, TP=0.5. v7 remains the configurable A/B artifact.

---

## Checkpoint — 2026-08-01: Research UI native desktop shell

Ship the Research UI as a local one-window macOS app (pywebview / WKWebView)
instead of Terminal + browser tabs. Harden worker process lifecycle so quitting
the UI does not leave orphaned backtests.

### Desktop shell

- Preferred entry: `python -m backtester.ui.desktop` (native window, no system
  browser). Browser CLI remains: `python -m backtester.ui.app`.
- Thin local launcher: `scripts/macos/CryoBacktester.app` (uses repo `.venv`;
  not a frozen binary). Optional `CRYOBT_ROOT` if the `.app` is moved.
- Single-instance flock (`desktop.lock`); quit confirmation when workers are
  still running; confirm → `shutdown_all`.
- Open `http://localhost:<port>/` with explicit Bokeh `websocket_origin` for
  both `localhost` and `127.0.0.1` (avoids blank shell: header only, no widgets).

### Worker lifecycle

- Workers spawn in a new POSIX session; cancel / quit / atexit share
  TERM → wait → KILL (`os.killpg` when available).
- `RunService.running_worker_count()` / `shutdown_all()` for desktop + signals.

### Browser CLI hygiene

- Always `pn.serve(..., show=False)`; open the browser once after `/healthz`
  (fixes empty `localhost` tabs from Bokeh’s eager `show=True`).

### Docs / deps

- `README.md`, `AGENTS.md`, `docs/upgrades/backtester-interactive-ui.md` updated.
- `pywebview>=5.0` added under Interactive UI in `requirements.txt`.

### Tests

- `tests/ui/test_run_service_lifecycle.py` — process group + stubborn-worker KILL
  (default CI, no parquet).
- `tests/ui/test_desktop_shell.py` — lock, quit policy, URL/WS origin alignment,
  `slow_ui` session hydration (nav RadioButtonGroup).

### Non-goals

- No py2app / Nuitka freeze; no Electron/Tauri; data plane stays outside the app.

---

## Checkpoint — 2026-07-31: product / workspace / data planes

Restructure the repo into three clear planes so shippable product code,
research “use” artifacts, and bulky data stop living on top of each other —
without renaming stable strategy IDs or breaking old runs/UI.

### Layout

| Plane | Path | Role |
|---|---|---|
| **Product** | `backtester/` | Engine, UI, ingest, indicator *code*, livecompare |
| **Workspace** | `workspace/` | Strategies by family, catalog, experiments, strategy tests |
| **Data** | `data/` | Market parquets, klines, run bundles, archive blobs (gitignored) |

Transitional symlinks keep old paths working (`backtester/data` → `data/market`,
`backtester/reports` → `data/runs`, `backtester/indicators/data` → `data/klines`).
Override roots via `backtester.core.paths` / env (`CRYOBT_MARKET_DATA`,
`CRYOBT_KLINE_DIR`, `CRYOBT_RUNS`, …).

### Strategy families

- New `workspace/catalog.py`: lightweight `Family` + `StrategySpec` registry
  (`tudysho`, `theta_engine`, `other`).
- Canonical strategy modules under `workspace/strategies/{family}/`.
- `backtester/strategies/*.py` are thin compatibility shims (stable import paths
  and IDs preserved for bundles, favourites, livecompare, experiments).
- git-crypt now encrypts `workspace/strategies/**` and most `workspace/tests/**`
  (shims and `blueprint_howto` stay plaintext).

### Research UI

- **New Run**: Family + Strategy selects driven by the catalog.
- **Runs** / **Favourites**: Family column + filter (family derived at display
  time from strategy ID; optional `family` in new bundle `meta.json`).
- **Selection bar**: quiet family label next to the selected run.

### Docs & skills

- `AGENTS.md`, `README.md`, `docs/strategy_howto.md`, `docs/git-crypt.md`,
  `workspace/README.md`, `data/README.md`, livecompare skill updated for the
  new layout and “how to add a family/strategy” checklist.

### Tests

- Strategy tests live under `workspace/tests/` (`pyproject.toml` `testpaths`
  updated).
- New coverage for catalog, import shims, path resolver, data-layout safety,
  and UI family filters.

### Non-goals (intentionally deferred)

- No SQLite `family` column; no strategy ID renames; no Postgres — bulk trade
  data remains in Parquet bundles; SQLite stays a thin run/favourites index.

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

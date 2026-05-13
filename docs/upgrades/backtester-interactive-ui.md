# Backtester Interactive UI — Spec & Implementation Plan

**Date:** 2026-04-23
**Status:** Proposed (revised 2026-04-23 after review)
**Audience:** AI coding agents implementing this upgrade
**Scope:** A local, rich, responsive desktop UI that replaces the "run → open static HTML → run again" loop for the CryoTrader backtester. macOS-first (developer runs on Mac), pure Python, direct in-process access to `GridResult` / `MarketReplay` / `engine.run_grid_full`.

---

## 1. Motivation / Problem

The current workflow (`backtester/run.py` → `reporting_v2.generate_html` → open HTML in browser) is slow and repetitive:

- Static HTML: can't sort, filter, pivot the grid result.
- Only top-N combos get equity curves & detailed metrics (see `GridResult.top_n_eq`); everything else is summary stats.
- Trade list is only shown for the best combo.
- No way to "save" a combo of interest for later comparison.
- Re-parameterising = editing `PARAM_GRID` in strategy file or an experiment TOML, re-running CLI, opening a new HTML. This kills iteration speed.

We want a **research cockpit**, modelled after tools like Vectorbt Pro's stats explorer, QuantConnect's result page, and Optuna Dashboard:

- Sort/filter/column-pick the full ranked combo list (not just top 20).
- Click any combo → see its equity curve, per-trade list, drawdown timeline.
- Multi-select combos → overlay equity curves on a comparison chart.
- Mark combos as **favourites** ("saved") with a note, persisted between sessions.
- Start a new backtest from the UI: pick a strategy, edit `PARAM_GRID` (or an experiment), run, get results live without leaving the app.

Non-goals (explicit):

- Not a production trading UI. The hub dashboard (`hub/hub_dashboard.py`) stays separate.
- Not a web-hosted multi-user service. Single-user, localhost-only.
- Not replacing the static HTML report — that stays as a shareable artefact for WFO / robustness runs.

---

## 2. UI framework choice

### Requirements

| Requirement | Weight |
|---|---|
| Direct access to in-process Python objects (no serialisation dance) | Must |
| First-class pandas DataFrame table with sort/filter/multi-select | Must |
| Interactive charts (equity curve, drawdown, zoom/pan, multi-series overlay) | Must |
| Runs as a local app on macOS with `python -m …` | Must |
| Ability to trigger long-running Python jobs (a grid run) with progress feedback | Must |
| Minimal extra dependencies; plays well with existing scientific stack (pandas, numpy, plotly-ish) | Should |
| No JS/TS/React build toolchain required | Should |
| Multi-tab / multi-view layout (sidebar + main pane) | Should |

### Candidates evaluated

| Framework | Verdict | Why |
|---|---|---|
| **Panel (HoloViz)** | **Chosen** | First-class pandas/DataFrame support. Tabulator widget is the best Python DataFrame table (server-side sort/filter, multi-select, pagination on 100k rows). Charts via Plotly / HoloViews / Bokeh. Runs via `panel serve` on localhost. Reactive via `.param.watch` or `pn.bind`. No JS build step. Pure Python. |
| Dash (Plotly) | Close second | Excellent charting. AG-Grid table is strong but the community edition lacks some filter features and the model is more callback-heavy. More boilerplate for the "sidebar + main + detail" layout we want. |
| Streamlit | Rejected | Whole-script re-run model kills responsiveness on large tables. Hard to keep multi-selected combos + detail panels in sync without manual `st.session_state` plumbing. |
| Marimo | Rejected | Reactive notebook is nice for exploration but weak for custom layouts and persisting favourites across sessions. |
| NiceGUI | Rejected | Nice ergonomics but DataFrame/table story is less mature than Panel's Tabulator. |
| PyQt / PySide (native) | Rejected | Overkill, slow iteration, no great pandas table without heavy custom models. Ships a large native dependency. |
| Textual (TUI) | Rejected | User explicitly asked for rich UI; terminal charts are a compromise. |

**Decision: Panel (HoloViz) 1.4+** with:

- `pn.widgets.Tabulator` for the combo grid table (server-side, selection-aware).
- **Plotly** for equity curves and drawdown charts (zoom / pan / hover). Already available on dev machines from the reporting layer; Panel renders Plotly natively via `pn.pane.Plotly`.
- **`subprocess.Popen`** runner for grid runs — the UI launches a worker script that writes progress + final result metadata to a JSONL file that the UI tails. Survives UI crashes, is trivially cancellable via `proc.terminate()`, and produces free logs. (Earlier revision used `ProcessPoolExecutor`; swapped because spawn-mode pickling + mid-run cancellation on macOS were fragile.)
- **SQLite** (`stdlib`) for the favourites / saved runs store.

This stack is pure pip-installable (`panel`, `plotly`, `bokeh` as Panel's dep) and needs no Node/JS tooling.

---

## 3. High-level architecture

```
+--------------------------------------------------------------+
|                        Panel app (UI)                        |
|  URL-addressable state: /?run=42&combo=<hash>&tab=detail     |
|                                                              |
|  Sidebar                   Main pane                         |
|  ┌─────────────┐           ┌──────────────────────────────┐  |
|  │ Strategy    │           │ Tab: Results Grid            │  |
|  │ selector    │           │   Tabulator(all_stats)       │  |
|  │ + param     │           │   → multi-select rows        │  |
|  │ editor      │           ├──────────────────────────────┤  |
|  │             │           │ Tab: Equity Overlay          │  |
|  │ [Run]       │           │   Plotly: selected combos    │  |
|  │ [Cancel]    │           ├──────────────────────────────┤  |
|  │             │           │ Tab: Combo Detail            │  |
|  │ Runs list   │           │   stats | equity | trades    │  |
|  │ (sqlite)    │           ├──────────────────────────────┤  |
|  │ Favourites  │           │ Tab: Favourites              │  |
|  └─────────────┘           └──────────────────────────────┘  |
|                                                              |
+-----------------------┬--------------------------------------+
                        │
                        ▼
+--------------------------------------------------------------+
|                 ui/services (thin Python API)                 |
|  - run_service: spawns `subprocess.Popen` worker that writes  |
|    progress + final meta to a JSONL file the UI tails         |
|  - store_service: sqlite + parquet bundles (shared with CLI)  |
|  - cache_service: pinned + LRU of in-memory GridResult        |
+-----------------------┬--------------------------------------+
                        │
                        ▼
+--------------------------------------------------------------+
|          Existing backtester core (unchanged)                |
|  MarketReplay → engine.run_grid_full → GridResult            |
+--------------------------------------------------------------+
```

Key constraint: the UI layer **only consumes** `GridResult` and the trade-log DataFrame. It must not reimplement statistics. Any new metric needed goes into `backtester/results.py` first.

---

## 4. Detailed UI specs

### 4.1 Results Grid tab

- Source: `GridResult.all_stats` rendered as a pandas DataFrame.
- Columns (configurable, persisted per strategy in sqlite):
  - `rank`, `score`, all param columns (from `GridResult.param_names`), `n` (trades),
    `total_pnl`, `sharpe`, `sortino` (if available), `profit_factor`,
    `max_dd_pct` / `max_intraday_dd_pct`, `win_rate`, `avg_pnl`, `cagr`,
    `calmar`, `monthly_consistency`.
- Sort: click column header. Default sort: `score` desc.
- Filter: per-column text/number filter row (Tabulator `header_filters=True`).
- Selection: multi-select with checkboxes. Soft cap 50 selected combos (warn above; Plotly handles more but legend gets unusable).
- Row actions: right-click (or row button column):
  - "Open detail" → switches to Combo Detail tab for that combo.
  - "Star" → adds to favourites.
  - "Copy params as TOML" → copies a snippet for the experiment file.
- Performance target: 10 000 rows renders < 500 ms, sort < 200 ms. Tabulator server-side pagination handles this; the full DataFrame is held in memory, pages of 200 are pushed.

### 4.2 Equity Overlay tab

- Plotly line chart.
- X-axis: shared `fan_dates` (from `GridResult.fan_dates` or reconstructed from `top_n_eq[key]["daily"]`).
- Y-axis: NAV or cumulative PnL (toggle).
- One line per selected combo. Legend entry shows rank + params short-form.
- Hover: shows date, NAV, drawdown-from-peak.
- Toggle: linear / log y-axis; drawdown-underwater subplot on/off.
- Edge case: if a combo is not in `top_n_eq` (selected combo is below top-N), the UI must **compute its equity on-demand** via `results.equity_metrics(df_c, capital, nav_daily_combo, date_from, date_to)` and cache it on the `GridResult` instance. No engine re-run.

### 4.3 Combo Detail tab

Three stacked panes for one focused combo:

1. **Stats card**: all scalar stats from `all_stats[key]` + `top_n_eq[key]`, formatted as a 2-column key/value grid.
2. **Equity + drawdown**: Plotly with two linked subplots (equity top, underwater drawdown bottom).
3. **Trades table**: `df_best`-equivalent DataFrame for this combo (`df[df["combo_idx"] == idx]`). Tabulator with columns `entry_time`, `exit_time`, `days_held`, `underlying_entry`, `strike`, `pnl`, `pnl_pct`, `exit_reason`. Clicking a row opens the **Trade Inspector** modal (§4.3.1).

#### 4.3.1 Trade inspector (modal)

For options strategies, bugs hide at the leg level (wrong sign, wrong strike, wrong fill). The modal shows:

- The full row from the trade log, including all leg columns that exist on `df` (leg symbols, strikes, entry/exit prices, signs, fees).
- A small mini-chart of the underlying spot during the trade's life.
- Any exit-reason / exit-condition metadata present on the row.

No engine changes — it just reads whatever columns the engine already puts on `df`.

### 4.4 Run Compare tab

Lets the user compare the best combo (or a chosen combo) of two different runs — the common research question "did my tweak help?":

- Pick Run A and Run B from the runs list (or Run A = current).
- For each, either take the top-ranked combo or pick a specific combo.
- Show: overlaid equity curves, side-by-side stats card with deltas, per-metric winner highlight.
- Export a one-liner summary (e.g. for pasting into notes / commit messages).

This is a full tab rather than a modal because users iterate on comparisons.

### 4.5 Favourites tab

- SQLite-backed store. See schema §5.
- Row per favourite: `name`, `strategy`, `params`, `score`, `total_pnl`, `sharpe`, `note`, `added_at`, `run_id`.
- Actions: open detail, re-run, unstar, edit note, export all as TOML.
- "Re-run" loads the same `strategy + param_grid` into the sidebar editor and pre-fills the Run button.

### 4.6 Sidebar: strategy & param editor + run button

- Strategy dropdown: keys of `backtester.run.STRATEGIES`.
- Param grid editor:
  - On strategy change, load `strategy_cls.PARAM_GRID` into an editable table (one row per param, values as CSV string).
  - Phase 3 input format: **CSV only** (e.g. `0.1, 0.2, 0.3`). Parser coerces to int/float/bool/str by sampling the strategy's defaults; invalid input is flagged inline and blocks Run.
  - Phase 5 adds **range shorthand** (`0.1..0.5:0.1`, `10..50:5`).
  - Also support loading an experiment: second dropdown listing `backtester/experiments/*.toml` (read-only in phase 1–4; write-back to experiments comes in a later phase, see §11/§12).
  - Date range override: two date pickers pre-filled with `strategy_cls.DATE_RANGE`.
- Run button: triggers `run_service.submit(strategy_key, param_grid, date_range)`. Disabled while another run is active.
- Progress widget: progress bar + current interval timestamp. The UI tails the worker's progress JSONL file on a 500 ms timer (`pn.state.add_periodic_callback`). Cancel button calls `proc.terminate()` on the worker.
- Runs list: completed `GridResult`s held in memory. **Pinned runs** (user toggles a pin icon) never evict; remaining slots use LRU (default 5, configurable). Each entry shows strategy, timestamp, n_combos, best score, pin state.

---

## 5. Persistence

All persistence is **local only**, in `backtester/ui/state/` (gitignored) and `backtester/reports/` (alongside existing HTML artefacts, see §5.2).

### 5.1 Favourites & saved runs — SQLite (`ui_state.db`)

```sql
CREATE TABLE runs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at       TEXT NOT NULL,   -- ISO8601 UTC
    strategy         TEXT NOT NULL,
    param_grid_json  TEXT NOT NULL,
    date_from        TEXT,
    date_to          TEXT,
    n_combos         INTEGER,
    n_trades         INTEGER,
    runtime_s        REAL,
    bundle_path      TEXT NOT NULL,   -- path to run bundle dir (see §5.2)
    pinned           INTEGER NOT NULL DEFAULT 0,   -- 0/1; pinned runs never LRU-evict
    label            TEXT,            -- user-editable
    git_sha          TEXT,            -- HEAD sha at run time
    git_dirty        INTEGER,         -- 0/1; was working tree dirty
    config_hash      TEXT             -- sha256 of backtester/config.toml
);

CREATE TABLE favourites (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id         INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    combo_key_json TEXT NOT NULL,       -- tuple(sorted dict items) as JSON
    name           TEXT NOT NULL,
    note           TEXT,
    score          REAL,
    total_pnl      REAL,
    sharpe         REAL,
    added_at       TEXT NOT NULL,
    UNIQUE(run_id, combo_key_json)
);
```

### 5.2 Run snapshot — unified run bundle (CLI + UI share)

A run bundle lives next to the existing HTML report:

`backtester/reports/<strategy>_<ts>.bundle/`
- `trade_log.parquet` — the engine's `df`
- `nav_daily.parquet` — the engine's `nav_daily_df`
- `final_nav.parquet` — the engine's `final_nav_df`
- `meta.json` — see schema below

**`meta.json` schema:**
```json
{
  "strategy": "short_generic",
  "param_grid": { "...": ["..."] },
  "keys": [[["name", "value"], ...], ...],
  "date_range": ["2025-01-01", "2026-03-31"],
  "account_size": 10000.0,
  "runtime_s": 12.4,
  "git_sha": "abc123...",
  "git_dirty": false,
  "config_hash": "sha256:...",
  "source": "cli" | "ui",
  "created_at": "2026-04-23T14:02:11Z"
}
```

**Both `python -m backtester.run` and the UI write this bundle.** The UI's SQLite `runs` table stores a row per bundle it knows about; on startup the UI scans `backtester/reports/*.bundle/` and registers any bundle it doesn't already have. This means CLI-produced runs automatically appear in the UI history — no silos.

Reconstructing a `GridResult` = load the 3 parquets + meta, call `GridResult(...)` again (~1 s on 10k combos).

The active (in-memory) `GridResult` is kept by reference. Pinned runs stay in memory; unpinned runs follow an LRU with configurable size.

---

## 6. Module / file layout

```
backtester/ui/
    __init__.py
    app.py                # Panel entry: `python -m backtester.ui.app`
    layout.py             # Sidebar + tab scaffolding
    state.py              # AppState dataclass (Param-based, reactive)
    views/
        __init__.py
        grid_view.py      # Results Grid tab (Tabulator)
        overlay_view.py   # Equity Overlay tab (Plotly)
        detail_view.py    # Combo Detail tab (stats + chart + trades + inspector modal)
        compare_view.py   # Run Compare tab
        favourites_view.py
        sidebar.py        # Strategy picker, param editor, run button
    services/
        __init__.py
        run_service.py    # subprocess.Popen worker + JSONL progress tail
        run_worker.py     # the worker script launched by run_service
        store_service.py  # sqlite + shared run bundles under backtester/reports/
        cache_service.py  # pinned + LRU of in-memory GridResult
        equity_service.py # on-demand equity_metrics() for non-top-N combos
        repro.py          # git sha + config hash helpers
    log.py                # thin wrapper around logging_setup.py for ui.log
    charts/
        __init__.py
        equity.py         # Plotly builders, shared
        drawdown.py
    state/                # gitignored runtime dir
        ui_state.db
        runs/
```

New runtime deps added to `requirements.txt`:
- `panel>=1.4`
- `bokeh>=3.4`       (Panel dep)
- `plotly>=5.20`     (already likely present via reporting stack)
- `pyarrow`          (already present)
- no others

CLI entry: `python -m backtester.ui.app [--port 5006] [--no-browser]`
Opens the default browser to `http://localhost:5006` by default.

---

## 7. Cross-cutting concerns

### 7.1 Progress reporting from engine

`engine.run_grid_full` currently runs silently. To feed the UI progress bar without affecting CLI users:

- Add an optional `progress_cb: Callable[[int, int, str], None] | None = None` kwarg (current interval, total intervals, current-day ISO date) to `run_grid_full`.
- The worker script (`run_worker.py`) passes a callback that appends JSON lines to `<bundle>/progress.jsonl`. The UI tails this file on a 500 ms timer. After the worker exits, it writes a final `{"status": "done" | "error", ...}` line.
- CLI callers pass `None` and behaviour is unchanged.

### 7.2 On-demand equity for non-top-N combos

Add `results.equity_metrics_for_key(grid_result, key)` that:

1. Returns `grid_result.top_n_eq[key]` if present.
2. Otherwise computes it using the combo's slice of `df` + `nav_daily_df` and memoises on a new `grid_result._lazy_eq` dict.

### 7.3 Serialising a combo key

Param tuples in `GridResult.keys` are `tuple(sorted dict items)`. For SQLite + UI roundtrip use:

```python
import json
def key_to_json(key: tuple) -> str: return json.dumps(list(key), separators=(",", ":"))
def key_from_json(s: str) -> tuple: return tuple((k, v) for k, v in json.loads(s))
```

### 7.4 Threading / responsiveness

Panel runs on Bokeh's Tornado server with an async event loop. The grid run **must** run in a separate **process** (not thread), because `engine.run_grid_full` is CPU-bound NumPy/pandas work and holds the GIL. We use `subprocess.Popen` with a dedicated worker script (`backtester.ui.services.run_worker`) rather than `ProcessPoolExecutor` — simpler semantics, clean logs, survives UI crashes.

### 7.5 Cancellation

The UI tracks `proc.pid`. Cancel = `proc.terminate()` (SIGTERM on macOS), falling back to `proc.kill()` after a 2 s grace period. The worker writes a final `{"status": "cancelled"}` line if it observes the signal; otherwise the UI infers cancellation from exit code. Engine has no internal cancellation hooks — acceptable for phase 1, trades are atomic per-interval so a partial run is simply discarded.

### 7.6 macOS specifics

- `subprocess.Popen` with a module-level worker script sidesteps the macOS `spawn` pickling concerns entirely.
- Panel auto-opens Safari by default; add `--browser=chrome` passthrough. No Mac-only API.
- App icon / menubar: out of scope phase 1.

### 7.7 URL-addressable state

Panel exposes `pn.state.location` which two-way-binds URL query params to app state. We sync three:

- `run` → active run id
- `combo` → selected combo hash (stable hash of the JSON-encoded key)
- `tab` → active main-pane tab name

This makes any view bookmarkable: `http://localhost:5006/?run=42&combo=ab12cd&tab=detail`. Trivial QoL, negligible implementation cost.

### 7.8 Reproducibility metadata

Every run bundle + SQLite row stores:

- `git_sha` — `git rev-parse HEAD` at run time (best-effort; `null` if not a git checkout).
- `git_dirty` — whether the working tree was dirty.
- `config_hash` — sha256 of `backtester/config.toml`.

Without this, "open a run from 3 weeks ago" silently shows numbers you can't reproduce. The UI surfaces these in the runs list and the stats card (subtle; red warning icon if `git_dirty`).

### 7.9 Logging

The UI uses the project's existing `logging_setup.py` (see repo root). A thin wrapper `backtester/ui/log.py` configures a rotating file handler writing to `logs/ui.log` (module name `backtester.ui`) plus stderr. Worker process logs to `logs/ui-worker-<pid>.log`. Log level controlled by `CRYOTRADER_UI_LOG_LEVEL` env var, default `INFO`. No new logging infrastructure is introduced.

---

## 8. Implementation plan

Each phase is a single PR-sized change. At the end of every phase **both automated tests pass and a manual live test is performed** before starting the next phase. A phase is not considered complete until the user has signed off on its live test.

### 8.0 Conventions & shared fixtures (read before starting any phase)

- **Python**: 3.11+. Type hints where they help readability; don't annotate trivial locals.
- **Imports**: absolute (`from backtester.ui.services import run_service`), not relative.
- **Logging**: every service module obtains `log = get_ui_logger(__name__)` (see §7.9).
- **No stats in UI code.** Any new scalar metric goes in `backtester/results.py` first (cf. §3 rule).
- **Test markers**: UI tests live in `tests/ui/`, no special marker. They must stay under 5 s total to keep the default suite (`pytest tests/ -v`) fast. Slow or server-boot tests get `@pytest.mark.slow_ui` and are skipped by default via `pyproject.toml` `addopts`.
- **Shared pytest fixtures** added in `tests/ui/conftest.py` and reused across phases:
  - `tiny_grid_result` → a real `GridResult` built from a 3-combo × 10-day synthetic trade log. ~20 ms to build; use everywhere instead of mocks.
  - `tmp_bundle_dir` → `tmp_path / "bundles"`, monkeypatches `backtester/reports/` bundle root to it.
  - `tmp_state_dir` → `tmp_path / "ui_state"`, monkeypatches `backtester/ui/state/` root.
  - `sqlite_store` → a `StoreService` bound to a throwaway DB under `tmp_state_dir`.
- **Manual-test data**: Phase 3+ live tests use strategy `short_generic` with an explicitly trimmed `PARAM_GRID` (see §8.3) so a full run completes in < 30 s on the developer's Mac. The trimmed grid is passed via the UI's param editor; the strategy file is never edited.

### 8.1 Phase 0 — scaffolding

**Goal:** empty Panel app boots on macOS, has a health endpoint, is served by `python -m backtester.ui.app`.

**Deliverables**

1. `backtester/ui/__init__.py` (empty).
2. `backtester/ui/log.py` — `get_ui_logger(name: str) -> logging.Logger` wrapping existing `logging_setup.configure_logging()`; adds a `RotatingFileHandler` at `logs/ui.log` (5 MB × 3 rotations) + stderr. Level via `CRYOTRADER_UI_LOG_LEVEL` env var, default `INFO`.
3. `backtester/ui/app.py`:
   - CLI: `python -m backtester.ui.app [--port 5006] [--no-browser] [--dev]`.
   - Builds a stub layout: Panel `FastListTemplate` with header "CryoTrader Research", empty sidebar, empty main pane with a single tab labelled "Results Grid (placeholder)".
   - Registers a custom `/healthz` Bokeh endpoint that returns `{"status": "ok", "version": <package version or "dev">}`. Use `pn.serve(..., extra_patterns=[("/healthz", HealthzHandler)])`.
   - Logs "UI up on http://localhost:<port>" on boot.
4. `requirements.txt`: add `panel>=1.4,<2`, `bokeh>=3.4,<4`. Verify `plotly` is already there (it is via reporting). No other new deps.
5. Pytest config: add `slow_ui` marker to `pyproject.toml` and include `-m "not live and not slow_ui"` in `addopts`.

**Automated tests** (`tests/ui/test_phase0_boot.py`)

- `test_ui_module_imports`: `import backtester.ui.app` succeeds, `app.build_app()` returns a Panel layout.
- `test_app_has_healthz_handler`: asserts `app._HEALTHZ_ROUTE` is defined at module level and callable.
- `@slow_ui test_app_boots_on_random_port`: starts server via `pn.serve(..., threaded=True, port=0, show=False)` on a background thread, polls `GET http://localhost:<port>/healthz` for up to 5 s, asserts `200` and body `{"status": "ok", ...}`, then calls `server.stop()`. Runs with `pytest -m slow_ui tests/ui/test_phase0_boot.py`.

**Manual live test** (run before declaring phase 0 done)

1. `python -m backtester.ui.app` — browser opens to localhost:5006, shows empty CryoTrader Research layout.
2. Check `logs/ui.log` contains the "UI up on …" line.
3. `curl http://localhost:5006/healthz` → `{"status":"ok","version":…}`.
4. Ctrl-C in the terminal stops the server cleanly (no stack trace).
5. **Sign-off**: user confirms steps 1–4 pass.

---

### 8.2 Phase 1 — static results view (read-only)

**Goal:** Display a real CLI-produced `GridResult` in an interactive table.

**Deliverables**

1. `backtester/ui/services/repro.py` — `git_sha()`, `git_dirty()`, `config_hash()` helpers (all return `None`/defaults if unavailable).
2. `backtester/ui/services/store_service.py`:
   - `StoreService(state_dir, bundles_root)` class, thin wrapper over SQLite (§5.1 schema) + filesystem.
   - `write_bundle(grid_result, strategy, runtime_s, source) -> Path` — writes 3 parquets + meta.json to `bundles_root/<strategy>_<ts>.bundle/`. Meta includes repro fields.
   - `register_bundle(bundle_path) -> int` — inserts a `runs` row, returns id. Idempotent on `bundle_path`.
   - `scan_bundles() -> list[int]` — scans `bundles_root/*.bundle/`, registers unseen, returns list of run ids.
   - `list_runs() -> list[RunRow]` — ordered by `created_at DESC`.
   - `load_run(run_id) -> GridResult` — reads parquets + meta, reconstructs `GridResult` via its normal constructor.
   - Thread-safe writes via a module-level `threading.Lock`.
3. `backtester/ui/services/cache_service.py`:
   - `ResultCache(store, max_unpinned=5)` — dict-backed LRU over `run_id -> GridResult`, plus a set of pinned ids that never evict.
   - `get(run_id)` loads lazily via `store.load_run` on miss.
   - `pin(run_id)` / `unpin(run_id)` / `pinned_ids()`.
4. **Modify** `backtester/run.py`: after the existing HTML write, call `StoreService.write_bundle(result, strategy, runtime_s, source="cli")`. Guard with a `--no-bundle` flag for edge cases.
5. `backtester/ui/state.py` — `AppState(param.Parameterized)` with reactive params: `active_run_id: int | None`, `selected_combo_keys: list[tuple]`, `active_tab: str`. Used by all views.
6. `backtester/ui/views/grid_view.py`:
   - `build_grid_view(state, cache) -> pn.Column` returns a Panel column containing a `pn.widgets.Tabulator`.
   - DataFrame source: `_grid_dataframe(result)` helper that flattens `result.all_stats` to one row per combo with columns listed in §4.1, plus a `rank` and `score` column, plus all param columns. Combo hash stored in a hidden column `_key_hash`.
   - `header_filters=True`, `pagination="remote"`, `page_size=200`, `selectable="checkbox"`.
   - On `param.watch` of `selection`, pushes selected `_key_hash` list into `state.selected_combo_keys`.
7. `backtester/ui/views/sidebar.py` — for phase 1, only a read-only runs list driven by `store.list_runs()`. Clicking a row sets `state.active_run_id`.
8. `backtester/ui/app.py` wires sidebar + grid view together; on `state.active_run_id` change, re-renders grid view with `cache.get(run_id)`.

**Automated tests** (`tests/ui/test_phase1_*.py`)

- `test_repro.py`
  - `test_git_sha_returns_hex_or_none`, `test_config_hash_stable` (hash twice, compare).
- `test_store_service.py`
  - `test_write_and_load_bundle_roundtrip` — uses `tiny_grid_result`; round-trips; asserts `df.equals(df_loaded)`, `keys == keys_loaded`, `best_key == best_key_loaded` after reconstruction.
  - `test_register_bundle_idempotent` — register same path twice, `runs` table still has 1 row.
  - `test_scan_bundles_picks_up_new` — drop a pre-built bundle into `tmp_bundle_dir`, `scan_bundles()` registers it.
  - `test_meta_json_contains_repro_fields` — asserts `git_sha`, `git_dirty`, `config_hash` keys exist.
- `test_cache_service.py`
  - `test_lru_evicts_after_max` — add 7 unpinned, assert only the 5 most recent remain in memory but all 7 are in SQLite.
  - `test_pinned_not_evicted` — pin id #1, add 6 others, assert id #1 still cached.
- `test_grid_view.py`
  - `test_grid_dataframe_shape` — `_grid_dataframe(tiny_grid_result)` has `n_combos` rows and contains `rank`, `score`, and every param column.
  - `test_grid_dataframe_sorted_by_score_desc`.
  - `test_selection_pushes_into_state` — simulate Tabulator `selection = [0, 2]`, assert `state.selected_combo_keys` has the matching keys.
- `test_cli_writes_bundle.py` (**slow_ui**) — runs `python -m backtester.run --strategy short_generic` against the tiny fixture replay (monkeypatch `MarketReplay` construction to return a stub) and asserts a new `.bundle/` dir appears and is readable by `StoreService.load_run`.

**Manual live test**

1. Run `python -m backtester.run --strategy short_generic` with the trimmed grid from §8.3 in a scratch copy of the strategy's `PARAM_GRID` (small enough to finish in 10–30 s). Confirm both the HTML report **and** a `.bundle/` dir appear under `backtester/reports/`.
2. Start `python -m backtester.ui.app`. Sidebar shows the new run.
3. Click the run → grid populates with ~N combos. User confirms:
   - sorting by `score`, `total_pnl`, `sharpe`, and any param column works.
   - column filters (type a value into the filter row) narrow the table.
   - selecting 3 rows via checkboxes shows a "3 selected" counter in the sidebar (can be a simple `pn.indicators.Number`).
4. Restart the UI; run still appears in the list (persistence works).
5. **Sign-off**: user confirms.

---

### 8.3 Phase 2 — combo detail + equity overlay

**Goal:** Click a combo → see its equity curve + trades. Select multiple → overlay.

**Deliverables**

1. `backtester/ui/services/equity_service.py`:
   - `equity_for_key(grid_result, key) -> dict | None`. Returns `grid_result.top_n_eq[key]` if present; else computes via `results.equity_metrics(...)` and caches on `grid_result._lazy_eq` dict (create attribute lazily).
   - `equity_many(grid_result, keys) -> dict[key, eq]` — calls the above per key; logs a debug line per compute.
2. `backtester/ui/charts/equity.py`:
   - `equity_figure(eq, title=None) -> plotly.graph_objects.Figure` — single line + underwater drawdown subplot. Annotates max-DD point.
   - `equity_overlay_figure(eqs: dict[label, eq], y_mode="nav" | "cumpnl") -> Figure` — multi-trace. Legend = labels. Hover-compare on x.
3. `backtester/ui/views/detail_view.py`:
   - Layout: top row stats card (`pn.pane.HTML` or `pn.GridBox` with key/value pairs), middle equity+drawdown Plotly, bottom trades Tabulator (`df[df["combo_idx"]==idx]`, columns per §4.3).
   - Row-click on trades table → opens **trade inspector modal** (§4.3.1) using `pn.Modal` (Panel 1.4) or fallback `pn.widgets.Card` overlay. Modal content: full row as key/value grid + Plotly mini-chart of spot between entry/exit (read `result.df` columns; we do NOT re-query market data here).
4. `backtester/ui/views/overlay_view.py`:
   - Binds to `state.selected_combo_keys`; builds `equity_overlay_figure` for those keys using `equity_service.equity_many`.
   - Toggle: NAV vs cumulative PnL; log y toggle; show/hide underwater subplot.
   - Empty state: "Select combos from the Results Grid tab."
5. URL state sync (§7.7) wired up: `state.active_tab`, `state.active_run_id`, `state.selected_combo_keys[0]` (first selection) ↔ `pn.state.location.search`.

**Automated tests** (`tests/ui/test_phase2_*.py`)

- `test_equity_service.py`
  - `test_returns_top_n_eq_unchanged` — key in top_n_eq → returns the same dict.
  - `test_computes_for_non_top_n` — build a `GridResult` with `cfg.simulation.top_n_report = 1`; query a non-top key; assert result has `daily`, `sortino`, `calmar` and is bitwise-equal to a direct `equity_metrics()` call.
  - `test_caches_on_result` — second call does not re-enter `results.equity_metrics` (monkeypatch a call counter).
- `test_charts_equity.py`
  - `test_equity_figure_has_two_subplots` — assert `len(fig.data) == 2` (equity + drawdown).
  - `test_overlay_has_trace_per_key` — 3 keys → 3 traces; each trace name contains param labels.
  - `test_overlay_y_mode_switch` — NAV vs cumpnl produce different y-arrays.
- `test_detail_view.py`
  - `test_stats_card_contains_key_metrics` — renders HTML; assert it contains `Sharpe`, `Total PnL`, `Max DD`.
  - `test_trades_table_filtered_to_combo` — row count equals `(df["combo_idx"]==idx).sum()`.
- `test_url_state_roundtrip.py`
  - `test_encode_decode_state` — encode `AppState` snapshot to URL query string and back; asserts equality.

**Manual live test**

1. From the run produced in phase 1's live test, click any row in the grid → "Open detail" (or auto-switch on double-click) → Combo Detail tab shows:
   - stats card populated,
   - equity + drawdown chart renders,
   - trades table has correct row count.
2. Click a trade row → inspector modal opens with all leg info.
3. Switch to Results Grid, select 5 rows, switch to Equity Overlay → 5 curves visible with a readable legend.
4. Select one combo that is **not in top 20** (scroll & select a mid-ranked one) → its curve still renders (on-demand compute). Verify `logs/ui.log` shows "equity_service computed for key …".
5. Toggle NAV / cumulative PnL, toggle log-y, toggle underwater — all should update live.
6. Copy the URL from the browser address bar, paste in a new tab → same combo + tab is shown (URL-addressable state works).
7. **Sign-off**: user confirms.

---

### 8.4 Phase 3 — live runs

**Goal:** Trigger a backtest from the UI, see progress live, get a loaded result.

**Deliverables**

1. **Modify** `backtester/engine.py`: add `progress_cb: Callable[[int, int, str], None] | None = None` kwarg to `run_grid_full`. Call it every `N` intervals (configurable, default 50) with `(current, total, date_iso)`. Never raise from a bad callback — wrap in try/except, log warning.
2. **Modify** `backtester/run.py`: factor the run body into `run_backtest(strategy_key, param_grid, date_range, account_size, bundles_root, progress_cb=None) -> Path` that returns the bundle path. CLI `main()` becomes a thin argparse shim around this function.
3. `backtester/ui/services/run_worker.py`:
   - `__main__` entry point; reads a JSON config from stdin or a `--config` file: `{strategy, param_grid, date_from, date_to, account_size, bundle_path, progress_path}`.
   - Imports `backtester.run.run_backtest`; passes a `progress_cb` that appends JSON lines `{"ts": iso, "current": i, "total": n, "date": day}` to `progress_path`.
   - On success, writes final line `{"status": "done", "bundle_path": "..."}`; on exception, writes `{"status": "error", "message": "..."}` and exits non-zero.
   - Installs a SIGTERM handler that writes `{"status": "cancelled"}` before exiting.
4. `backtester/ui/services/run_service.py`:
   - `RunService(store, cache)` class.
   - `submit(strategy_key, param_grid, date_range) -> RunHandle` — spawns `python -m backtester.ui.services.run_worker --config /tmp/<uuid>.json` via `subprocess.Popen`. Returns handle with `.pid`, `.progress_path`, `.proc`.
   - `tail_progress(handle) -> Iterator[dict]` — generator that yields new JSON lines (file-tail style using `os.stat` size).
   - `cancel(handle)` — `proc.terminate()`, then `proc.kill()` after 2 s.
   - `await_result(handle) -> int | None` — blocks until process exits; returns registered `run_id` on success, `None` on cancel/error.
5. `backtester/ui/views/sidebar.py` — **add**:
   - Strategy dropdown (`pn.widgets.Select`, options = keys of `backtester.run.STRATEGIES`).
   - Param grid editor: a `pn.widgets.Tabulator` or simple `pn.Column` of `pn.widgets.TextInput` rows, one per param in the strategy's `PARAM_GRID`. Each input holds a CSV string. On strategy change, re-populates from `PARAM_GRID`.
   - Parser `parse_param_csv(key: str, csv: str, sample: Any) -> list`: infers type from sample (int/float/bool/str); invalid input sets an inline error and disables the Run button.
   - Date pickers pre-filled from `DATE_RANGE`.
   - Run button → calls `run_service.submit(...)`, stores handle on `state.active_run_handle`. Disabled while a run is active.
   - Cancel button → `run_service.cancel(handle)`.
   - Progress widget: `pn.widgets.Progress` + `pn.pane.Markdown` showing current date. Updated by a `pn.state.add_periodic_callback(500 ms)` that reads new lines from `handle.progress_path`.
6. On run completion: `store.register_bundle(path)` (the worker already wrote it) → `cache.get(run_id)` → set `state.active_run_id`.

**Automated tests** (`tests/ui/test_phase3_*.py` and `tests/test_engine_progress_cb.py`)

- `tests/test_engine_progress_cb.py`
  - `test_callback_invoked_with_totals` — stub strategy, 100 intervals, callback list; asserts at least 1 call, last call `current == total`.
  - `test_bad_callback_does_not_break_run` — callback that raises → run completes successfully; warning logged.
- `tests/ui/test_param_csv_parser.py`
  - `test_parse_int_csv`, `test_parse_float_csv`, `test_parse_bool_csv`, `test_parse_mixed_fails_cleanly`, `test_empty_csv_returns_error`.
- `tests/ui/test_run_worker.py` (**slow_ui**)
  - `test_worker_writes_bundle_and_progress` — launches `run_worker` subprocess with a config pointing to a tiny synthetic replay (monkeypatch `MarketReplay` to yield 5 `MarketState`s). Asserts `progress.jsonl` has ≥ 1 progress line + a final `status=done` line; asserts bundle dir exists.
  - `test_worker_handles_sigterm` — launch worker on a 100-interval replay, send SIGTERM, expect `status=cancelled` in progress file.
- `tests/ui/test_run_service.py`
  - `test_submit_and_tail` (**slow_ui**) — submit; drain `tail_progress`; expect final `done` line and a registered run id.
- `tests/ui/test_bundle_registration.py`
  - Pre-drop a bundle dir under `tmp_bundle_dir`; `StoreService.scan_bundles` picks it up and the runs list UI contains it.

**Manual live test (small real backtest)**

Use `short_generic` with this trimmed grid, typed into the UI param editor (do NOT modify the strategy file):

```
leg_type         strangle
dte              1
delta            0.24
entry_hour       3, 9
stop_loss_pct    0, 4.0, 6.0
take_profit_pct  0, 0.5
max_hold_hours   0
skip_weekends    1
min_otm_pct      4, 5, 6
```

→ 2 × 3 × 2 × 3 = **36 combos** on the strategy's default ~120-day `DATE_RANGE`. Expected runtime: < 30 s on a dev Mac.

Steps:

1. Start UI cold. Pick strategy `short_generic`. Verify the param editor loads defaults.
2. Replace values per above. Verify the Run button enables only when all fields parse.
3. Type an obviously bad value in `delta` (`"abc"`) → Run disables, inline error shown.
4. Fix it, click Run:
   - progress bar advances,
   - date label updates at least every 2 s,
   - sidebar Cancel button enables.
5. Halfway through, click Cancel → progress halts; a toast/log entry says "cancelled". Previous active result (phase 1 run) remains loaded.
6. Click Run again, let it complete. On completion:
   - new run is auto-selected,
   - grid populates with 36 rows,
   - detail / overlay work as in phase 2.
7. Check `logs/ui.log` + `logs/ui-worker-<pid>.log` exist and have sensible content.
8. `ls backtester/reports/short_generic_*.bundle/` shows the new bundle with `meta.json` containing `git_sha`, `git_dirty`, `config_hash`, `source: "ui"`.
9. **Sign-off**: user confirms.

---

### 8.5 Phase 4 — favourites + run compare + WFO surfacing

**Goal:** Save interesting combos; compare two runs side-by-side; show IS/OOS deltas if present.

**Deliverables**

1. **SQLite**: apply the `favourites` table migration (§5.1) on `StoreService` init.
2. `backtester/ui/services/store_service.py` — new methods:
   - `add_favourite(run_id, combo_key, name, note) -> int`
   - `list_favourites() -> list[FavRow]`
   - `remove_favourite(fav_id)`
   - `update_favourite(fav_id, **fields)`
3. `backtester/ui/views/favourites_view.py` — Tabulator with columns `name`, `strategy`, `params`, `score`, `total_pnl`, `sharpe`, `note`, `added_at`. Row actions: **Open** (loads run, focuses combo), **Re-run** (preloads sidebar param editor with `{k: [v]}` for each param of the favourite — "re-run this exact combo"), **Unstar**, **Edit note**.
4. Grid view + detail view: add a star toggle button per combo.
5. `backtester/ui/views/compare_view.py`:
   - Two run dropdowns (Run A, Run B) + two combo selectors (default: best combo of each).
   - Shows overlay chart (reuses `equity_overlay_figure`) and a stats-delta table: columns `metric`, `A`, `B`, `Δ`, `winner`.
6. **TOML export**: `backtester/ui/services/toml_export.py` with `favourite_to_toml(fav) -> str` producing an `experiment`-style snippet. Clipboard via `pyperclip` (optional import; fallback: textarea dialog).
7. **WFO surfacing** (read-only):
   - Modify `backtester/run.py` so that when `--wfo` is passed, the bundle's `meta.json` gets a `wfo_result` key with the summary rows `run_walk_forward` already returns.
   - `detail_view.py`: if the active run has `meta["wfo_result"]`, render a compact IS/OOS delta table section above the trades table.

**Automated tests** (`tests/ui/test_phase4_*.py`)

- `test_favourites_store.py`
  - `test_add_list_remove`, `test_unique_constraint_on_run_combo`, `test_update_note`.
- `test_favourites_view.py`
  - `test_star_button_adds_row`, `test_unstar_removes_row`.
- `test_compare_view.py`
  - `test_compare_figure_has_two_traces` — two synthetic runs, assert the overlay figure has 2 traces named "A: …" and "B: …".
  - `test_stats_delta_table_signs` — if A.sharpe > B.sharpe, winner column for that row reads "A"; delta sign correct.
- `test_toml_export.py`
  - `test_roundtrip_via_tomllib` — generated TOML parses with `tomllib` back to the same dict.
- `test_wfo_surfacing.py`
  - `test_renders_when_present` — bundle meta with a `wfo_result` → detail view HTML contains "IS" and "OOS".
  - `test_absent_is_clean` — bundle meta without → no WFO section, no errors.

**Manual live test**

1. Load the phase 3 run. Star three combos from the grid.
2. Switch to Favourites tab: three rows visible. Edit the note on one.
3. Restart the UI: favourites still there.
4. Click Re-run on a favourite → sidebar param editor is prefilled with `{k: [v]}` for each param. Run it → 1-combo bundle produced and shown.
5. Run compare tab: pick the phase 3 run as A, the 1-combo re-run as B. Overlay shows two curves; stats-delta table populated; winner column sensible.
6. Run a WFO backtest from CLI: `python -m backtester.run --strategy short_generic --wfo --is-days 30 --oos-days 10 --step-days 10` (with the trimmed grid from §8.3 already in place). Verify UI picks it up, and the detail view shows the IS/OOS delta table.
7. Click "Copy params as TOML" on a favourite → paste in a scratch file; verify it parses (`python -c "import tomllib; tomllib.loads(open('x.toml').read())"`).
8. **Sign-off**: user confirms.

---

### 8.6 Phase 5 — polish

**Goal:** Ergonomics. No new features.

**Deliverables**

- Column chooser in the grid view (hide/show per column); preset saved to SQLite keyed by `(strategy, sha256(sorted_param_names))`; falls back to defaults when schema differs.
- Dark mode toggle (Panel `FastListTemplate` `theme="dark"`); preference saved per user.
- Keyboard shortcuts (Panel's `pn.state.onload` + JS snippet): `/` focus filter, `f` toggle star on focused row, `Esc` close modal / clear selection.
- Export filtered grid view to CSV (button in grid toolbar).
- Param-grid editor range shorthand: `0.1..0.5:0.1` → `[0.1, 0.2, 0.3, 0.4, 0.5]`; `10..50:5` → `[10, 15, 20, …, 50]`. Same validator path as CSV.
- "Prune runs" action: keep pinned, drop bundles older than N days. Dry-run preview first.

**Automated tests** (`tests/ui/test_phase5_*.py`)

- `test_column_preset.py` — save, load, mismatched schema → default.
- `test_range_shorthand_parser.py` — integer & float ranges; invalid step rejected.
- `test_csv_export.py` — filtered view exports only visible rows.

**Manual live test**

1. Hide 3 columns → save → reload UI → columns stay hidden.
2. Toggle dark mode; reload → persists.
3. `/` focuses filter; type; `Esc` clears.
4. In param editor, type `0.1..0.5:0.1` → preview shows `[0.1, 0.2, 0.3, 0.4, 0.5]`.
5. Prune runs (dry-run) → shows what it would delete; confirm → pinned survive.
6. **Sign-off**: user confirms.

---

### 8.7 Phase 6 — deferred / optional

Tracked but not planned in detail; revisit after phase 5 feedback:

- Full WFO visualiser (per-window IS vs OOS panels, stability plots across windows).
- Within-run diff view (two combos, same run; side-by-side + delta).
- 2-param heatmap via `grid_result.heatmap_pairs`.
- Experiment integration: write-back from UI → `backtester/experiments/*.toml`; promote favourite to named experiment. See §12.

Each gets its own spec document before implementation.

---

## 9. Test strategy

Fast Python tests (no browser automation) are the primary quality gate:

- `tests/ui/` new subtree. All pure-Python: services, state transformations, chart builders (assert Plotly figure JSON structure, not pixel output).
- Boot test starts a real Panel server on a random port in a thread, hits HTTP endpoints, shuts down. Kept small to stay under the 2 s budget in `pyproject.toml`.
- No Selenium / Playwright in phase 1. If phase 5 needs interaction tests, add an optional `tests/ui_e2e/` with `playwright` behind a `ui_e2e` pytest marker, skipped by default (mirroring the `live` marker convention).
- Existing backtester tests must stay green: adding `progress_cb` to `engine.run_grid_full` is a keyword-only optional — verified by running the full suite.
- **No import-hygiene lint test.** An earlier revision proposed grepping `backtester/ui/` for forbidden stats imports; dropped as over-engineered. The "UI only consumes `GridResult`" rule lives in this doc and in code review.

Acceptance criteria (manual, end of phase 4):

1. Starting from cold: `python -m backtester.ui.app` opens within 2 s.
2. Loading a 10 000-combo run into the grid renders in < 1 s.
3. Sorting any column finishes in < 300 ms (Tabulator server-side on pandas).
4. Selecting 10 combos and switching to Overlay renders the chart in < 800 ms, including any on-demand `equity_metrics` for non-top-N combos.
5. Starting a new run: progress bar updates at least once per second and total UI frame rate stays > 30 fps (observable, not auto-asserted).
6. Restart the app: favourites and run history reappear.

---

## 10. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Tabulator slow on 50 000+ rows | Server-side pagination enabled by default; pre-filter ≤ 10 k rows per view. |
| Plotly performance with many overlaid curves | Soft cap 50 selections (warn above); 50 × 400 points is well within Plotly's comfort zone. |
| Run bundles accumulating on disk | Bundles live in `backtester/reports/` alongside HTML. UI exposes a "prune runs older than N days / keep pinned" action. |
| Worker subprocess dies silently | Final line in `progress.jsonl` is the canonical status; UI also watches `proc.poll()`. On unexpected exit: toast + keep previous result active, preserve worker log. |
| Concurrent write to SQLite | Single-user app; `check_same_thread=False` + a module-level `threading.Lock` around writes is sufficient. |
| UI reimplements stats and drifts from `results.py` | Hard rule (§3): UI only reads `GridResult`. New metrics → `results.py` first. Enforced via code review. |
| Param schema drift between strategy versions | Column presets keyed by `(strategy, param_names_hash)` so schema changes fall back to defaults without breaking (§phase 5). |

---

## 11. Out of scope (for this upgrade)

- Multi-user hosting, auth, remote access.
- Running on the VPS alongside `ct-slot@XX`.
- Replacing `reporting_v2.generate_html` — static HTML reports stay for WFO and shareable artefacts.
- Editing strategy source code from the UI. The param grid editor only edits the grid values, not the strategy class.
- Writing back to `backtester/experiments/*.toml` from the UI — initially read-only. See §12.
- Live trading integration. This is a **research** UI.

---

## 12. Relationship to experiments

`backtester/experiments/*.toml` is the existing research-persistence mechanism (sensitivity grids + WFO window params). The UI's favourites (SQLite) are a parallel mechanism with different goals: favourites are a lightweight, per-combo research scratchpad; experiments are durable, git-trackable, CLI-driveable research artefacts.

**Initial focus (phase 1–5): experiments are read-only to the UI.** The dropdown loads an experiment's grid into the sidebar; the UI does not edit or create experiment files.

**Later (phase 6 / separate upgrade):** promote a favourite or a UI-edited param grid into a named `experiments/<name>.toml`. This is the right place to close the loop back to the CLI-driven WFO flow — but only after the UI core is proven.

## 13. Open questions (flag for user decision before phase 3)

1. Should the sidebar param editor support **adding / removing** param keys, or only editing values of the strategy's declared `PARAM_GRID` keys? Default: values-only (safer).
2. Should favourites sync to a file (e.g. `backtester/experiments/favourites.toml`) so they're git-trackable? Default: no, sqlite only, with an explicit "Export favourites to TOML" action.
3. Max combos hard cap per run triggered from the UI? Default: 5 000, warn above.

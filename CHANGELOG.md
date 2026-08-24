# Changelog

All notable changes to CryoBacktester are documented here.

---

## Checkpoint — 2026-08-24: v18 optional 1:1 wing_pct on stop baselines

Add theta_spreads-style further-OTM hedge to `theta_engine_v18`: equal qty,
same expiry, target width = `wing_pct × short strike`. No `max_width_usd` /
`min_width_usd`. `wing_pct=0` keeps the run-742 naked stop baselines
(Late1Eq5 / Full2Eq8). If `wing_pct>0` and no outer is listed, the short
is rolled back (no naked fill). Working grid = 2 stop books × 6 wing_pct
values (incl. 0).

```bash
python -m pytest workspace/tests/test_theta_engine_v18.py -v
python -m backtester.run --strategy theta_engine_v18
```

---

## Checkpoint — 2026-08-24: v18 Late1Eq5 / Full2Eq8 baselines (run 742)

Stop discovery on RichForce2 16 front (design A, 252 combos) in run 742
(`theta_engine_v18_20260824_072920`, 2025-08-18 → 2026-08-20). Credit SL
never beat naked; tight prox / tight equity hurt. Locked working
baselines via `stop_book`: **Late1Eq5** (`d550e3296f17` — prox 1@4 + eq
5%) and **Full2Eq8** (`731b1d03b15d` — prox 2@16 + eq 8%). Design-A grid
kept as `V18_STOP_DISCOVERY_GRID`. Investor D/G sidecar retained.

```bash
python -m pytest workspace/tests/test_theta_engine_v18.py -v
python -m backtester.run --strategy theta_engine_v18
```

---

## Checkpoint — 2026-08-22: v17 rich qty sizing closed (run 741)

Discovery on RichForce2 16 front: scale base qty down when the sold leg
looks expensive (`front_vrp` × `iv_rank_60`, multiplicative factor). Run
741 (`theta_engine_v17_20260822_090110`, 216 combos) — every `rich_mode=on`
cell lost PnL / Sharpe / Calmar to naked; WR unchanged; winners cut harder
than losers. **Closed** for performance. Default grid is `rich_mode=none`;
`V17_RICH_DISCOVERY_GRID` kept as artefact. Indicator
`front_25d_iv_rank` stays available.

```bash
python -m pytest workspace/tests/test_theta_engine_v17.py workspace/tests/test_front_25d_iv_rank.py -v
```

---

## Checkpoint — 2026-08-21: v17 investor D/G sidecar on short-DTE

`theta_engine_v17` forks `v14` with `track_investor_greeks` (default on).
Working grid is the run-727 RichForce2 16 front cell (`c8573c839903`).
Shorts-only vs full-book D/G after each live bar; vega is a meter.
Scorecard: `analysis/theta_engine_v17_investor_greeks.py`.
(`theta_engine_v15` was briefly misallocated to this work and removed —
stable ID is `theta_engine_v17`. `theta_engine_v16` remains the Mode C
premium-match wing.)

```bash
python -m pytest workspace/tests/ -v
python -m backtester.run --strategy theta_engine_v17
python analysis/theta_engine_v17_investor_greeks.py
```

---

## Checkpoint — 2026-08-16: v13 always-on strike-% / debit-% wing

Replace `wing_budget_usd` / `wing_delta` with `wing_strike_pct` (distance
from the short strike) and `wing_debit_pct` (fraction of short credit).
Long strike is always strictly further OTM; qty is 0.1–short qty (no
budget skip). 36-combo grid. Do not treat run 716/717 as this spec.

```bash
python -m pytest workspace/tests/ -v
```

---

## Checkpoint — 2026-08-15: v13 investor D/G sidecar

`theta_engine_v13` meters shorts-only vs full-book delta/gamma after each
bar (`track_investor_greeks`, default on). Vega is recorded, not gated.
`run_grid_full` collects `investor_greeks_sidecar()` into `df.attrs` (5-tuple
unchanged); `write_bundle` stores `investor_greeks.parquet` and
`meta.sidecars`. Scorecard: `analysis/theta_engine_v13_investor_greeks.py`.

```bash
python -m pytest workspace/tests/ tests/test_engine_sidecars.py tests/ui/test_store_service.py -v
python -m backtester.run --strategy theta_engine_v13
python analysis/theta_engine_v13_investor_greeks.py
```

---

## Checkpoint — 2026-08-15: theta_engine_v13 paired wing

Fork `theta_engine_base` into `theta_engine_v13`: whenever a Mode C short
opens, buy a same-right further-OTM long sized to `wing_budget_usd`
(0–250, never above). Knobs: `wing_delta` (0.01–0.10), `wing_expiry`
(`same` | `next_listed`). Short remains the boss (entry/size/TP/SL);
TP/SL use short-leg credit and MTM only. Budget 0 is the naked control.

```bash
python -m pytest workspace/tests/ -v
python -m backtester.run --strategy theta_engine_v13
```

---

## Checkpoint — 2026-08-15: theta_engine base / _common cleanup

Shared Mode C helpers (entry policies, skew6, net-credit SL/TP, weekday
clocks) live in `_common.py`. `base.py` is the strategy class only;
`v12.py` imports the same helpers and keeps the cover overlay. Dropped
dead discovery policies and the unused launch open-path
(`_do_open_with_min_dte` / `fictional_entry_date`). Equity fallback uses
`cfg.simulation.account_size_usd`. Historical v1–v11 are untouched.

```bash
python -m pytest workspace/tests/ -v
```

---

## Checkpoint — 2026-08-15: freeze Mode C as theta_engine_base

Canonical short-only lock is now `theta_engine_base`
(`workspace/strategies/theta_engine/base.py`): RichForce16 / Daily15, Mode C
book and TPs, no cover / perps / v9 trail-launch knobs. PARAM_GRID is the two
entry policies. Catalog ID `theta_engine_base` (frozen). Shared constants live
in `_common.py` (`MODE_C_BOOK`, `MODE_C_TP`, `MODE_C_DATE_RANGE`). Fork later
versions from `base.py`, not from v9 or v12.

```bash
python -m pytest workspace/tests/ -v
python -m backtester.run --strategy theta_engine_base
```

---

## Checkpoint — 2026-08-14: theta_engine_v12 long-option cover

Fork the v9 Mode C short-only book into `theta_engine_v12` and add a
**long-option cover** overlay. This is a clean start — not a continuation of
v10/v11 sticky wings or perps. Catalog ID `theta_engine_v12` + shim;
canonical code under `workspace/strategies/theta_engine/v12.py`.

### Short book (unchanged from v9)

RichForce16 / Daily15, Mode C fixed TP 0.60 / 0.50, SL=3, hold=0. Leftover
`hedge_delta` / `hedge_qty_mult` knobs removed.

### Cover overlay (`cover_*`)

- Trigger: shorts-only cash D/G vs `DEFAULT_INVESTOR_LIMITS`. Vega and theta
  are metered, not used.
- Size: smallest long qty (0.1 steps, search ceiling 50) so **full book**
  D/G is inside the bands. Skip the bar if no size works.
- Close: shorts inside `cover_inner_pct` (default 0.70) of the D/G limits,
  after `cover_min_hold_minutes` (default 60). Cooldown 60 minutes;
  `cover_severe_mult=1.5` bypasses it.
- Instrument: majority-qty short expiry or next listed; same type as the
  net short delta; strictly more OTM than the shorts, target 10Δ.
- Shorts keep entering on the v9 schedule. Cover is excluded from
  `max_concurrent`. Longs buy ask / sell bid; no SL/TP on the cover.

```bash
python -m pytest workspace/tests/ -v
python -m backtester.run --strategy theta_engine_v12
```

---

## Checkpoint — 2026-08-13: theta_engine_v11 smarter sticky wing

Fork Mode C (RichForce16 / Daily15) into `theta_engine_v11` and make the
sticky long wing *smarter about how* it responds once the investor cash-Greek
limits fire — not *whether* they fire. Limits remain `DEFAULT_INVESTOR_LIMITS`
(`|Δ|<10%` γ<0 else 30%, `γ>−10%`, `|V|<0.2%` of AUM). Catalog ID
`theta_engine_v11` + shim; canonical code under
`workspace/strategies/theta_engine/v11.py`.

### Wing response (vs v10)

- Timing: `wing_close_margin_pct`, `wing_min_hold_minutes`,
  `wing_cooldown_minutes`, `wing_cooldown_override_mult` (severity bypass).
- Instrument: `wing_side_mode=greek|count`, always-on OTM strike floor,
  `wing_delta_mode=fixed|relative`, breach-magnitude delta retry (step toward
  ATM if qty alone cannot satisfy).
- **Expiry:** sticky wings skip SL/TP but settle on `check_expiry`; cooldown
  is cleared on roll so a replacement can open the same bar (v10/early-v11
  left a zombie long that blocked new wings).
- Open/close still use the **short book only** (one slot, no top-up) so the
  wing cannot thrash off its own Greeks. That is the research state this
  checkpoint freezes — accumulate/resize is the next v11 pass.

### Perp overlay

`perp_delta_hedge` now emits blueprint `Trade` / fill rows (`BTC-PERPETUAL`,
open / adjust / flatten). Mark PnL stays on the engine overlay
(`perp_mark_pnl`); close `pnl=0` so NAV is not double-counted. Favourites in
the 704–708 grids still ran perp **off**.

### Research grids (GUI-visible `data/runs/`)

Locked from 704/706: `sticky_budget`, `wing_side_mode=greek`,
`wing_delta_mode=fixed`, margin/hold/cooldown 3/60/60, SL=3, `max_concurrent=20`,
`hold_days=0`. Date range `2025-04-11 → 2026-08-12`.

| Script | What |
|--------|------|
| `analysis/theta_engine_v11_wing_grid.py` | 64-combo wing knobs (run 704) |
| `analysis/theta_engine_v11_exit_grid*.py` | SL / max_concurrent / hold_days (705/706) |
| `analysis/theta_engine_v11_compliance_grid.py` | 48-combo trigger × delta × expiry × perp × entry (708, post-expiry-fix) |
| `analysis/theta_engine_v11_fav708_breach_audit.py` | Bar-by-bar short vs full-book limits on 708 favourites #1/#2/#13 |

708 #1 (RichForce16 `dgv`/0.20/`next_listed`) is the compliance-shaped
favourite on PnL/Sharpe, but full-book OK is only ~19% of live bars: the
single 0.1–0.6 wing never resizes while shorts grow to 11–20.

### Wing accumulate (same checkpoint, follow-on)

`wing_resize_mode=once|accumulate` + `wing_max_qty` (default 5.0). Accumulate
keeps one contract and **adds qty** (engine `add_legs` vintages) while the
*full* book is still in trigger-breach; min-hold spaces top-ups; flatten
still uses shorts + hysteresis. Same strike/expiry until expiry or shorts
recover — no call/put flip (that thrashed). `once` is the 708 slot
behaviour. Discovery PARAM_GRID and the 48-combo compliance grid lock
`accumulate`.

```bash
python -m pytest workspace/tests/ -v
python -m backtester.run --strategy theta_engine_v11
PYTHONPATH=. python analysis/theta_engine_v11_compliance_grid.py
```

---

## Checkpoint — 2026-08-07: theta_engine_v10 Greek risk + Mode C baselines

Ship portfolio cash-Greek metering, Mode C locked baselines (RichForce16 /
Daily15), and a phased research path (scale → perp → sticky wings) aimed at
~20% ann / ≤5% DD **and** investor Greek bands. Product PnL and mandate risk
remain in tension; artefacts under `analysis/theta_engine_v10_p*/`.

### Baselines (locked)

| Display | Policy ID | Rule |
|---------|-----------|------|
| **RichForce16** | `fav_sharpe_rich4_f5_1600` | rich-or-forced VRP≥4 else force 5d Mon/Thu @ 16:00; **TP 0.60** |
| **Daily15** | `fav_pnl_daily_1500` | Mon–Fri clock @ 15:00; **TP 0.50** |

Constants in `workspace/strategies/theta_engine/_common.py`. Combo keys now show
**effective** TP after configure (engine merges `describe_params()`), so the UI
no longer labels RichForce as 0.50 from a PARAM_GRID placeholder.

### Core: `portfolio_risk` + pricing Greeks

- `backtester/core/portfolio_risk.py` — cash D/G/V/T % of AUM (`nav_usd`);
  `DEFAULT_INVESTOR_LIMITS`, `limits_ok`, `max_qty_within_limits`.
- `pricing.py` — BS gamma / vega / theta helpers for risk metering.
- Docs: `docs/strategy_howto.md` §5a; blueprint comments point at v10.

### theta_engine_v9

- Mode C book fork (trail/schedule research paths; catalog + shim).
- Analysis helpers: `analysis/theta_engine_v9_trail_*.py`.

### theta_engine_v10

- Catalog ID `theta_engine_v10` + shim.
- `greek_limits_mode`: `off` | `size_to_budget` (may skip) | `scale` (shrink to
  residual, floor 0.1, never skip for budget).
- Optional **BTC perp** delta overlay (`perp_delta_hedge`, deadband); mark PnL
  in engine NAV via `perp_mark_pnl`.
- **Sticky long wings** (`option_hedge_mode=none|sticky_budget`): breach-gated
  inventory (triggers `dg` / `dgv` / `v`), Axis A `wing_expiry_mode` ×
  `wing_delta`; real `Trade` open/close fills + UI `comment` (blueprint pattern);
  longs exit at bid. Alias: `off` → `none`.
- Engine: `_effective_params_for_key` for honest combo labels; overlay mark PnL
  hook for perp/wing strategies.

### Experiments (scorecards under `analysis/`)

| Phase | Folder / script | Finding (short) |
|-------|-----------------|-----------------|
| P0 | `theta_engine_v10_p0/` | Breaches concentrate at high `n_open` |
| P1 | `…_p1/` | `scale` can near 20%/5% DD with 0 skips; Greeks still bad |
| P2 | `…_p2/` | Perp kills delta breach; return often &lt; 20% on P1 size |
| P3 | `…_p3/` | Early sticky (bar-flip) failed — do not use |
| P3b | `…_p3b/` | Breach-gated wings on naked Mode C; modest DG help, D still binding |

Shared runner: `analysis/theta_engine_v10_phase_lib.py`. GUI bundles via
`analysis/theta_engine_v10_p3b_gui_runs.py` → `data/runs/theta_engine_v10_*.bundle`.

```bash
python -m pytest workspace/tests/ tests/test_portfolio_risk.py -q
python -m backtester.run --strategy theta_engine_v10
PYTHONPATH=. python analysis/theta_engine_v10_p3b_gui_runs.py
```

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

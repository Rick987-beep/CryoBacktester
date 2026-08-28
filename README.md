# CryoBacktester

BTC options backtester using real Deribit historical tick data. Replays 5-minute option chain snapshots, evaluates parameter grids across strategies in a single data pass, and generates self-contained HTML reports with equity curves, composite scoring, heatmaps, and trade logs.

**This repo contains backtesting code only. No live trading, no exchange credentials, no production server.**

The companion live trading repo is [CryoTrader](https://github.com/Rick987-beep/CryoTrader). Strategies are occasionally ported from here to there — that is the only coupling.

---

## Table of Contents

1. [Quickstart](#quickstart)
2. [Repo Structure](#repo-structure)
3. [Data](#data)
4. [Market Replay](#market-replay)
5. [Strategy Logic](#strategy-logic)
6. [The Engine](#the-engine)
7. [The Research Pipeline](#the-research-pipeline)
8. [Research UI](#research-ui)
9. [Indicators](#indicators)
10. [Scoring Model](#scoring-model)
11. [HTML Reports](#html-reports)
12. [Experiment Files](#experiment-files)
13. [Configuration](#configuration)
14. [Strategies](#strategies)
15. [Adding a New Strategy](#adding-a-new-strategy)
16. [Testing](#testing)
17. [Performance Notes](#performance-notes)
18. [Fee Model](#fee-model)

---

## Quickstart

```bash
# 1. Create and activate virtual environment
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Run a strategy discovery grid (requires data in data/market/)
python -m backtester.run --strategy short_str_turb_dyn

# 3. Sensitivity analysis around a known-good candidate
python -m backtester.run --experiment short_str_turb_dyn_v1 --mode sensitivity

# 4. Walk-forward validation
python -m backtester.run --experiment short_str_turb_dyn_v1 --mode wfo

# 5. Launch the interactive Research UI (native window — preferred)
python -m backtester.ui.desktop
#    or open scripts/macos/CryoBacktester.app (Dock / Finder)

# 6. Run tests
python -m pytest tests/ workspace/tests/ -v
```

Reports and run bundles are written to `data/runs/` as self-contained HTML + `.bundle/` dirs.
The Research UI reads those same run bundles in a native window (or in the browser via `python -m backtester.ui.app`).

---

## Repo Structure

Three planes: **`backtester/`** (product), **`workspace/`** (strategies/experiments/tests), **`data/`** (market history, klines, run bundles). See `AGENTS.md`, `workspace/README.md`, `data/README.md`.


```
CryoBacktester/
├── backtester/                    # Package root (run from repo root)
│   ├── run.py                     # CLI entry point
│   ├── core/                      # Engine, market replay, pricing, results
│   │   ├── engine.py              # Single-pass grid runner — run_grid_full()
│   │   ├── market_replay.py       # Parquet loader → MarketState iterator
│   │   ├── strategy_base.py       # Strategy protocol, Trade/OpenPosition
│   │   ├── results.py             # GridResult: vectorised scoring, equity metrics
│   │   ├── pricing.py             # Deribit fee model, Black-Scholes helpers
│   │   ├── option_selection.py    # Option leg selection helpers
│   │   ├── expiry_utils.py        # Expiry date utilities
│   │   ├── market_hours.py        # US market hours / NYSE calendar
│   │   ├── config.py / config.toml
│   ├── research/                  # Sensitivity, WFO, robustness, run audit
│   │   ├── experiment.py
│   │   ├── walk_forward.py
│   │   ├── robustness.py          # Deflated Sharpe Ratio
│   │   └── run_audit/             # Grid quality autopsy (η², danger, curve-fit, live picks)
│   ├── inspect/                   # Fast run/combo lookup CLI (python -m backtester.inspect)
│   ├── reporting/                 # Self-contained HTML reports
│   │   ├── html_report.py
│   │   └── charts.py              # SVG chart primitives
│   ├── indicators/                # Indicator compute + build pipeline
│   │   ├── pipeline.py            # IndicatorDep / build_indicators
│   │   ├── hist_data.py           # On-disk Binance kline cache
│   │   ├── supertrend.py
│   │   ├── turbulence.py
│   │   ├── trend_regime.py
│   │   └── ingest_klines.py
│   ├── ui/                        # Interactive Research UI (Panel + Bokeh + Plotly)
│   │   ├── desktop.py             # Native window: python -m backtester.ui.desktop
│   │   ├── app.py                 # Browser CLI: python -m backtester.ui.app
│   │   ├── server_utils.py        # wait_for_healthz + URL/WS origin helpers
│   │   ├── state.py               # AppState param object (shared reactive state)
│   │   ├── log.py                 # UI-scoped logger
│   │   ├── views/                 # One file per screen
│   │   ├── services/              # Data access layer
│   │   ├── charts/
│   │   └── state/                 # SQLite DB + desktop.lock (gitignored)
│   ├── strategies/                # Compatibility shims → workspace.strategies
│   ├── calm_nights/               # Calm-nights helpers (cadysho)
│   ├── ingest/
│   │   ├── check_data_completeness.py
│   │   ├── check_parquet.py
│   │   └── tardis/                # Tardis bulk download pipeline
│   ├── data/                      # Parquet snapshots (gitignored)
│   ├── archive/                   # Archived data + legacy strategies (gitignored)
│   └── reports/                   # Generated HTML / run bundles (gitignored)
│
├── scripts/
│   └── macos/
│       ├── CryoBacktester.app     # Thin Dock launcher → .venv desktop UI
│       └── brand/                 # Cryo family icons + DESIGN.md
├── workspace/                     # USE plane — strategies, catalog, experiments
├── tests/                         # Integration + UI tests
│   └── ui/
├── docs/
├── analysis/                      # One-off analysis artifacts
├── handover/                      # External handover packages
└── pyproject.toml
```

**Gitignored directories (local work, not code):**
- `data/market/` — parquet snapshots (gitignored)
- `backtester/archive/` — archived parquets + planning + legacy strategies
- `data/runs/` — generated HTML reports and run bundles
- `backtester/ui/state/` — SQLite UI state DB + desktop.lock
- `backtester/indicators/data/` — cached kline data

---

## Data

### Format
Option data is stored as per-day parquet files in `data/market/`:
- `options_YYYY-MM-DD.parquet` — 5-minute option chain snapshots
- `spot_YYYY-MM-DD.parquet` — 1-minute BTC spot OHLC bars

All option prices are **BTC-denominated** (e.g. `0.0068 BTC`). USD value = `price × spot`.
`mark_iv` is stored as a **percentage** (e.g. `39.8` = 39.8% annualised vol). Divide by 100 before passing to Black-Scholes.

### Ingestion sources

**Tardis bulk download** (historic data, up to ~2 weeks lag):
```bash
python -m backtester.ingest.tardis.bulk_fetch
```
See `backtester/ingest/tardis/TARDIS_DATA_NOTES.md` for format details.  
For raw archive before subscription expiry, see `backtester/ingest/tardis/TARDIS_ARCHIVE_PLAN.md`.

**Sync from VPS live recorder** (done from the CryoTrader repo):
The live tick recorder runs as `ct-recorder` on the VPS and writes daily parquets.
Sync them using `backtester/ingest/tickrecorder/sync.py` in CryoTrader.

Both sources produce the same parquet schema — the engine sees no difference.

### Data paths

`backtester/core/config.toml` `[data]` section points to the data directory:
```toml
options_parquet = "data"
spot_parquet    = "data"
```
`MarketReplay` loads all `options_YYYY-MM-DD.parquet` and `spot_YYYY-MM-DD.parquet` files found in those directories.

---

## Market Replay

`backtester/core/market_replay.py` — converts parquet files into a time-stepped iterator that strategies consume.

**`MarketReplay`** loads all parquet files on construction. Strategies iterate over it:

```python
replay = MarketReplay("data/market", "data/market")  # or leave defaults from config/paths
for state in replay:
    trades = strategy.on_market_state(state)
```

**`MarketState`** — what a strategy sees at each 5-minute tick:

| Attribute / Method | Type | Description |
|---|---|---|
| `state.ts` | `datetime` | Timestamp of this snapshot (UTC) |
| `state.spot` | `float` | BTC mid-price at this snapshot |
| `state.get_option(strike, is_call, expiry)` | `OptionQuote \| None` | Fetch a specific option |
| `state.get_chain(expiry)` | `list[OptionQuote]` | All options for one expiry |
| `state.get_atm_strike(expiry)` | `float` | Nearest-to-spot strike |
| `state.expiries` | `list[str]` | Available expiry labels (sorted) |
| `state.spot_bars` | `list[SpotBar]` | 1-min OHLC bars since last snapshot |
| `state.spot_high_since(dt)` | `float` | Max spot since datetime (O(1)) |
| `state.spot_low_since(dt)` | `float` | Min spot since datetime (O(1)) |

**`OptionQuote`** fields: `strike`, `is_call`, `expiry`, `bid`, `ask`, `mark`, `mark_iv`, `delta`, `spot`, plus `.bid_usd`, `.ask_usd`, `.mark_usd` properties.

**Performance internals:**
- Option data stored as contiguous NumPy arrays (`float32` prices, `uint8` expiry index, `bool` is_call) — ~5× less RAM than Python dicts.
- Timestamp index built with `np.unique` for O(1) per-tick array slicing.
- `OptionQuote` objects built lazily, with a per-tick dict cache.
- `spot_high_since` / `spot_low_since` use pre-computed cummax/cummin arrays (O(1)).

---

## Strategy Logic

### Protocol

Every strategy implements the `Strategy` protocol from `strategy_base.py`. No base class is needed — structural typing only:

```python
class MyStrategy:
    name = "my_strategy"
    PARAM_GRID = {"delta": [0.1, 0.15, 0.2], "dte": [1, 2]}

    def configure(self, params: dict) -> None:
        """Apply one combo's parameters. Called before each grid run."""
        self.delta = params["delta"]
        self.dte   = params["dte"]
        self.pos   = None

    def on_market_state(self, state: MarketState) -> list[Trade]:
        """Called every 5-minute tick. Return list of closed trades."""
        ...

    def on_end(self, state: MarketState) -> list[Trade]:
        """Called once at end of data. Force-close any open position."""
        ...

    def reset(self) -> None:
        """Clear all state. Called between parameter combos."""
        ...

    def describe_params(self) -> dict:
        """Return current parameter values (used in reports)."""
        return {"delta": self.delta, "dte": self.dte}
```

### Trade dataclasses

**`OpenPosition`** — held by the strategy while a trade is open:
- `entry_time`, `entry_spot`, `legs`, `entry_price_usd`, `fees_open`, `metadata`
- `legs` is a list of dicts: `{strike, is_call, expiry, side, qty, entry_price}`

**`Trade`** — a completed trade returned to the engine:
- `entry_time`, `exit_time`, `entry_spot`, `exit_spot`
- `entry_price_usd`, `exit_price_usd`, `fees`, `pnl`
- `triggered` (bool), `exit_reason` (str), `entry_date`, `metadata`

### Composable entry/exit conditions

`strategy_base.py` provides factory functions for common conditions:

**Entry conditions** — `(MarketState) → bool`:
```python
time_window(start_hour, end_hour)   # only trade in this UTC hour range
weekday_only()                       # skip weekends
at_interval(every_n_ticks)          # fire every N ticks
```

**Exit conditions** — `(MarketState, OpenPosition) → str | None`:
```python
stop_loss_pct(pct, price_mode="mark")       # close if position value > pct% of entry
                                            # price_mode="mark": stable; use for SL
profit_target_pct(pct, price_mode="executable") # close if gain > pct% of entry
                                            # price_mode="executable": real bid/ask; use for TP
max_hold_hours(hours)               # close after N hours
max_hold_days(days)                 # close after N days
time_exit(hour)                     # close at specific UTC hour
index_move_trigger(pct)             # close if spot moved pct% since entry
                                    # (checks 1-min bars, not just 5-min close)
```

`price_mode` controls which price is used for SL/TP evaluation:
- `"mark"` — exchange model price; stable against wide bid/ask spreads. **Default for SL.**
- `"executable"` — ask for sell legs, bid for buy legs; only fires when you can actually get out. **Default for TP.**

**Configure them in `configure()`:**
```python
self._exit_conds = [stop_loss_pct(self._sl_pct, price_mode="mark")]
if self._tp_pct > 0:
    self._exit_conds.append(profit_target_pct(self._tp_pct, price_mode="executable"))
```

### Reprice caching

`price_legs(state, pos, mode)` marks all legs to market and returns the total position USD value.
Result is also cached in `pos._last_reprice_usd`. The engine reads this cache for NAV accounting
without repricing twice per tick.

`_reprice_legs(state, pos)` is a backward-compat alias for `price_legs(state, pos, mode="executable")`.

Available modes: `"mark"`, `"executable"`, `"bid"`, `"ask"`.

---

## The Engine

`backtester/core/engine.py` — runs all parameter combos in a **single pass** over the data.

```
run_grid_full(strategy_cls, param_grid, replay)
  → (df, keys, nav_daily_df, final_nav_df)
```

- `df` — trade log DataFrame, one row per closed trade across all combos
- `keys` — list of param tuples (index into the combo list)
- `nav_daily_df` — daily NAV low/high/close per combo
- `final_nav_df` — final NAV + realised/open PnL per combo

**How single-pass works:**
1. Expand `PARAM_GRID` into all combinations via `itertools.product`.
2. Instantiate one strategy instance per combo and call `configure(params)`.
3. Iterate `MarketReplay` once. At each tick, call `on_market_state(state)` on every instance.
4. Track open-position NAV per combo every tick using `_last_reprice_usd` cache.
5. After the last tick, call `on_end(state)` on every instance to force-close.

This means market data is loaded exactly once regardless of grid size.

---

## The Research Pipeline

Running a parameter grid and picking the best result is statistically dangerous — with enough combos you will find a "winner" by pure chance. The backtester is built around three explicit steps to combat this:

### Step 1 — Discovery
```bash
python -m backtester.run --strategy short_str_turb_dyn
```
Wide `PARAM_GRID` (hundreds of combos), full date range.
**Goal:** find which region of parameter space is profitable at all.
**Output:** discovery report with heatmaps, best-combo stats, Deflated Sharpe Ratio.

### Step 2 — Sensitivity
```bash
python -m backtester.run --experiment short_str_turb_dyn_v1 --mode sensitivity
```
Narrow grid centred on the Step 1 candidate (±10% / ±2h, 5 points per param).
**Goal:** is the candidate on a smooth hill or a spike?
**Output:** sensitivity report with marginal PnL charts and all-combos table.

### Step 3 — Walk-Forward Validation
```bash
python -m backtester.run --experiment short_str_turb_dyn_v1 --mode wfo
```
In-sample (IS) uses the wide `PARAM_GRID` (honest search space). Out-of-sample (OOS) is truly unseen.
**Goal:** does the region stay profitable on future data?
**Output:** WFO report with per-window table, stitched OOS equity curve, IS/OOS scatter.

### Why this separation matters
- `PARAM_GRID` in each strategy file is the wide, unbiased discovery grid. **Never narrow it post-hoc.**
- Experiment TOMLs in `backtester/experiments/` capture "what we think is good and why" — separately from the strategy definition.
- WFO uses the wide grid for its IS runs, so the IS optimiser has a real search problem, not a trivially narrow space around a known-good point.

### After a discovery run lands
Use the fast lookup + grid autopsy CLIs (do **not** reload the full `GridResult` in agents):

```bash
python -m backtester.inspect show 748
python -m backtester.research.run_audit 748 --html
# → analysis/run_audit/<bundle_stem>/audit.json (+ report.html)
```

Agent skills: `.cursor/skills/run-lookup/`, `.cursor/skills/run-audit/` (see `AGENTS.md`).

---

## Research UI

An interactive Panel-based app for exploring backtest results without re-running the engine.
The preferred launch is a **native desktop window** (pywebview / WKWebView on macOS) — one Dock icon, one window, no browser tabs.

```bash
# Native desktop (preferred)
python -m backtester.ui.desktop
python -m backtester.ui.desktop --port 5007

# Or open the thin local .app (uses this repo's .venv — not a frozen binary)
open scripts/macos/CryoBacktester.app
# Optional: symlink into ~/Applications and pin to Dock
# If you move the .app, set CRYOBT_ROOT to the repo path.
# Family icons + design language: scripts/macos/brand/ (see DESIGN.md)

# Browser / Terminal (dev / debugging)
python -m backtester.ui.app              # opens browser after /healthz is ready
python -m backtester.ui.app --port 5007
python -m backtester.ui.app --no-browser
python -m backtester.ui.app --dev        # autoreload on file changes
```

**Quit behaviour (desktop):** if a backtest worker is still running, a confirmation dialog appears. Cancel keeps the window open; confirm stops all workers (SIGTERM, then SIGKILL) and exits. Closing the window also releases the single-instance lock.

**Troubleshooting**

| Symptom | Fix |
|---|---|
| `desktop UI is already running` | Only one desktop instance is allowed; quit the other window first |
| `port 5006 is already in use` | Stop the other process (`lsof -i :5006`) or pass `--port` |
| `pywebview is not installed` | `pip install pywebview` (listed in `requirements.txt`) |
| `.app` says missing `.venv` | Create `.venv` in the repo, or set `CRYOBT_ROOT` if the app was moved |
| Blue header only / blank body | Must open `localhost` with matching Bokeh `websocket_origin` (fixed in desktop/app helpers). Quit stale UI and relaunch. |

### What it reads

The UI scans `data/runs/` for run bundles — directories created by `run.py`
(format: `<strategy>_<timestamp>.bundle/`) containing `meta.json`, `trade_log.parquet`,
`nav_daily.parquet`, and `final_nav.parquet`. It does **not** re-run the backtest engine.

### Tabs

| Tab | Description |
|---|---|
| **Results Grid** | All combos for the selected run — sortable, filterable, star/unstar |
| **Combo Detail** | Stats card + equity/drawdown chart + trade log for one focused combo |
| **Equity Overlay** | Multi-combo equity curves on one chart (select up to 50 combos) |
| **Favourites** | Starred combos across all runs; TOML export, re-run prefill, notes |
| **Compare** | Side-by-side metric table for selected combos |

### Results Grid filter syntax

Type filter expressions into the **Filter** box (space-separated, AND-combined):

| Expression | Effect |
|---|---|
| `sharpe>1.5` | Sharpe > 1.5 |
| `pnl:0..5000` | PnL between 0 and 5 000 |
| `max_dd_pct<=20` | Max drawdown ≤ 20 % |
| `exit_reason:trigger,expiry` | exit_reason is trigger or expiry |
| `strategy:short` | strategy contains "short" (substring) |
| `sharpe>1 pnl>0 max_dd_pct<30` | multiple AND filters |

Supported operators: `>` `>=` `<` `<=` `=` `!=`

### Persistence

User preferences (dark mode, column visibility presets per strategy) and starred combos
are stored in `backtester/ui/state/ui_state.db` (SQLite, gitignored). Created automatically
on first launch.

---

## Indicators

Pre-computed indicators are injected into strategy instances before the data pass begins. Strategies declare their dependencies via a class attribute:

```python
class MyStrategy:
    indicator_deps = [IndicatorDep(name="turbulence", ...)]
```

The engine calls `backtester/indicators/pipeline.py` → `build_indicators()` once before the grid run, then attaches the computed series to every strategy instance. **All indicator computation uses historic cached data only** — no live API calls inside the backtest loop.

### Available indicators

| Module | Purpose |
|---|---|
| `backtester/indicators/supertrend.py` | SuperTrend trend-direction signal |
| `backtester/indicators/turbulence.py` | Composite turbulence score (Parkinson RV, trend, burst, decay) |
| `backtester/indicators/hist_data.py` | On-disk Binance kline cache — loads/saves to `backtester/indicators/data/`, no live fetch at backtest time |

These files are separate copies from CryoTrader's `indicators/` and diverge independently.

---

## Scoring Model

After the engine completes, `GridResult` in `results.py` ranks all combos by a **composite score** (0 → 1): a weighted sum of per-metric percentile ranks across all eligible combos.

| Metric | Weight | Direction | What it captures |
|---|---|---|---|
| R² (equity linearity) | 15% | ↑ higher | Non-linear curves: sleeping giants and lucky streaks |
| Sharpe (annualised) | 15% | ↑ higher | Risk-adjusted return |
| Total PnL | 15% | ↑ higher | Absolute profitability |
| Max drawdown % (intraday) | 15% | ↓ lower | Worst peak-to-trough loss |
| Omega ratio | 10% | ↑ higher | Tail sensitivity beyond Sharpe |
| Ulcer Index | 10% | ↓ lower | Duration × severity of drawdowns |
| Monthly consistency | 10% | ↑ higher | Fraction of months ending positive |
| Profit factor | 10% | ↑ higher | Total gains / total losses |

Weights live in `config.toml` `[scoring]` — changing them requires no code edits.

**Max drawdown** is the intraday peak-to-trough measure (daily NAV low vs running high-watermark), which is strictly more conservative than EOD-close-based drawdown.

**Monthly consistency guard:** if the backtest spans fewer than 2 calendar months, consistency values are set to 0.5 (neutral) so this metric contributes no differentiation.

**Deflated Sharpe Ratio (DSR):** implemented in `robustness.py` per Bailey & López de Prado. Corrects the observed Sharpe for the number of parameter combos tested, non-normality of returns, and serial correlation. DSR < 1 means the result is likely noise.

---

## HTML Reports

Each run writes a self-contained HTML file to `data/runs/`. No server or external assets required — open directly in a browser.

**Report sections:**

| Section | Description |
|---|---|
| Risk summary bar | Best combo's key metrics at a glance (Sharpe, R², Omega, Ulcer, max DD) |
| Best-combo box | All parameters + all scoring metrics + Sortino, Calmar, DSR |
| Fan chart | Equity curves for top-20 combos with intraday high/low shading. Hover for params. |
| Leaderboard | Top-20 combos ranked by composite score |
| Heatmaps | Auto-generated for every 2-parameter pair |
| Robustness section | (`--robustness` or sensitivity mode) Distribution chart, marginal PnL charts, all-combos table |
| WFO section | (WFO mode) Per-window IS/OOS table, stitched OOS equity curve, IS vs OOS scatter |
| Trade log | Every entry/exit for the best combo |

Reports are gitignored — they are outputs, not code.

---

## Experiment Files

`backtester/experiments/<name>.toml` bridges Step 1 (discovery) and Steps 2–3. It captures a specific candidate without polluting the strategy file.

```toml
# backtester/experiments/short_str_turb_dyn_v1.toml
strategy = "short_str_turb_dyn"

[sensitivity]
steps = 5   # grid points per parameter

[sensitivity.best]
# Best combo found in Step 1 discovery
stop_loss_pct    = 150.0
take_profit_pct  = 0.50
turb_threshold   = 1.5

[sensitivity.deviation.stop_loss_pct]
type   = "pct"    # ±10% of 150 → [135, 142, 150, 157, 165]
amount = 10

[sensitivity.deviation.turb_threshold]
type   = "abs"    # ±0.5 → [1.0, 1.25, 1.5, 1.75, 2.0]
amount = 0.5

[wfo]
is_days   = 45
oos_days  = 15
step_days = 15
```

Deviation types: `"pct"` (±N% of best), `"abs"` (±N in natural units), `"fixed"` (held constant).

---

## Configuration

`backtester/core/config.toml` — application-level settings. Strategy-specific logic stays in strategy files.

Key sections:

| Section | Key settings |
|---|---|
| `[data]` | Paths to parquet files and directories |
| `[simulation]` | `account_size_usd`, `top_n_report` (top N combos in HTML) |
| `[pricing]` | `risk_free_rate`, `expiry_hour_utc`, `strike_step_usd`, vol clamps |
| `[repricing]` | Fallback pricing when bid/ask is 0 (mark × slip factor) |
| `[fees]` | Deribit fee model parameters |
| `[scoring]` | Metric weights for composite score |

---

## Strategies

Canonical strategy code lives under **`workspace/strategies/{family}/`**.
Register stable IDs in **`workspace/catalog.py`**. `backtester/strategies/` holds
compatibility shims only.

| Family | Examples |
|---|---|
| `tudysho` | tudysho, eisbach, monopteros, starnberg, stradysho, v1–v4 |
| `theta_engine` | v1–v6 |
| `other` | blueprint_howto, short_str_turb_dyn, cadysho, … |

Legacy unfinished ports live in `backtester/archive/strategies_to_be_fixed/`.

---

## Adding a New Strategy

The canonical reference is `workspace/strategies/other/blueprint_howto.py` — read it first.
Full step-by-step instructions are in `docs/strategy_howto.md`.

1. Create `workspace/strategies/<family>/my_strategy.py` implementing the `Strategy` protocol.

**Key imports:**
```python
from backtester.core.option_selection import select_by_delta
from backtester.core.expiry_utils import expiry_dt_utc, select_expiry
from backtester.core.pricing import deribit_fee_per_leg, EXPIRY_HOUR_UTC
from backtester.core.strategy_base import (
    OpenPosition, Trade, check_expiry, close_position,
    price_legs, profit_target_pct, stop_loss_pct, max_hold_hours,
)
```

**configure() — wire exit conditions:**
```python
def configure(self, params):
    self._sl_pct = float(params["stop_loss_pct"])
    self._tp_pct = float(params.get("take_profit_pct", 0.0))
    self._pos    = None
    # SL: mark mode — stable against wide spreads in thin books
    # TP: executable mode — only fires when you can actually get that price
    self._exit_conds = [stop_loss_pct(self._sl_pct, price_mode="mark")]
    if self._tp_pct > 0:
        self._exit_conds.append(profit_target_pct(self._tp_pct, price_mode="executable"))
```

**Required leg fields at open** (all mandatory):
```python
leg = {
    "strike":          float,   # USD
    "is_call":         bool,
    "expiry":          str,     # e.g. "28MAY26"
    "side":            "sell",  # or "buy" — drives price_legs() per-leg pricing
    "qty":             float,
    "price_btc":       float,   # fill price (bid for short, ask for long)
    "entry_price":     float,   # same as price_btc (alias)
    "entry_price_usd": float,   # price_btc × spot (per contract)
    "entry_spot":      float,   # spot at entry
    "entry_bid":       float,
    "entry_ask":       float,
    "entry_mark":      float,
    "entry_iv":        float,   # mark_iv from parquet — already % (34.4 = 34.4%)
    "entry_delta":     float,
    "fee_usd_open":    float,   # deribit_fee_per_leg(spot, entry_price_usd)
}
```

> **IV note:** `mark_iv` in the parquet is stored as a percentage (e.g. `34.4` = 34.4%).
> Store it as-is in the leg dict. Do NOT multiply by 100.

**Required leg fields before calling `close_position`:**
```python
leg["exit_price_btc"] = float   # fill price at close
leg["exit_price_usd"] = float   # exit_price_btc × exit_spot (per contract)
```

**pos.metadata — mandatory keys:**
```python
metadata = {
    "direction": "sell",   # or "buy" — required by stop_loss_pct / profit_target_pct
    "expiry":    expiry,   # expiry code string
    "expiry_dt": exp_dt,   # tz-aware datetime — used by check_expiry()
    "pos_id":    pos_id,   # monotonic int — links open fills to close fills in reports
}
```

2. Register in `workspace/catalog.py` (stable ID + family + status). Optionally add a shim under `backtester/strategies/`.

3. Run discovery:
```bash
python -m backtester.run --strategy my_strategy
```

4. Once you have a candidate, create `workspace/experiments/my_strategy_v1.toml` and run sensitivity + WFO.

**Key rule: keep `PARAM_GRID` wide and unbiased. Never narrow it after seeing results.**

---

## Testing

```bash
# Full test suite: UI tests + strategy tests
python -m pytest tests/ workspace/tests/ -v

# Strategy tests only (42 tests)
python -m pytest workspace/tests/ -v

# UI tests only
python -m pytest tests/ui/ -v

# Live/network tests (deselected by default, require network)
python -m pytest workspace/tests/ -m live -v
```

Tests live in two directories:
- `tests/ui/` — Panel UI unit tests (state, views, services, filter parser, etc.)
- `workspace/tests/` — per-strategy backtesting unit tests

`@pytest.mark.live` tests are excluded by default via `pyproject.toml` (`addopts = "-m 'not live'"`).
`@pytest.mark.slow_ui` marks tests that require a real Panel server and are also excluded by default.

---

## Performance Notes

On an M1 Mac with the full dataset (~109k intervals, ~87M option rows):

| Strategy | Combos | Trades | Time |
|---|---|---|---|
| `long_gamma_whitelist` | 432 | 12,312 | ~49s |
| `short_str_turb_dyn` | 12 | 904 | ~22s |

Key optimisations in the engine and market replay:
- **Single data pass** — all combos evaluated simultaneously; market data loaded once.
- **NumPy columnar storage** — option data in contiguous typed arrays (`float32`, `uint8`, `bool`). ~5× less RAM than Python dicts.
- **Timestamp index** — `np.unique` with `return_index/return_counts` for O(1) per-tick slicing.
- **Lazy `OptionQuote` construction** — built only when a strategy calls `get_option()`, with a per-tick dict cache.
- **O(1) excursion queries** — `spot_high_since()` / `spot_low_since()` via pre-computed cummax/cummin arrays.
- **Reprice caching** — `_reprice_legs` result stored on `pos._last_reprice_usd`; NAV tracker reads it rather than repricing twice (saves ~15% wall time on large grids).
- **LRU-cached expiry parsing** — `_parse_expiry_date` / `_expiry_dt_utc` cached; prevents 1.5M regex calls per run.

---

## Fee Model

Deribit taker fee model (per leg, per side):

```
fee = min(0.03% × index_price, 12.5% × option_mark_price)
```

At BTC ~$84k the index cap ≈ 0.00025 BTC/leg and typically binds for options above ~0.002 BTC. Implemented in `backtester/core/pricing.py`. Parameters configurable in `config.toml` `[fees]`.

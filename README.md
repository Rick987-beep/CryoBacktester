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

# 2. Run a strategy discovery grid (requires data in backtester/data/)
python -m backtester.run --strategy long_gamma_whitelist

# 3. Sensitivity analysis around a known-good candidate
python -m backtester.run --experiment short_str_turb_dyn_v1 --mode sensitivity

# 4. Walk-forward validation
python -m backtester.run --experiment short_str_turb_dyn_v1 --mode wfo

# 5. Launch the interactive Research UI
python -m backtester.ui.app

# 6. Run tests
python -m pytest tests/ backtester/strategies/tests/ -v
```

Reports are written to `backtester/reports/` as self-contained HTML files.
The Research UI reads those same run bundles interactively at http://localhost:5006.

---

## Repo Structure

```
CryoBacktester/
├── backtester/                    # Core backtesting engine (run from repo root)
│   ├── run.py                     # CLI entry point
│   ├── engine.py                  # Single-pass grid runner — run_grid_full()
│   ├── market_replay.py           # Parquet loader → MarketState iterator
│   ├── strategy_base.py           # Strategy protocol, Trade/OpenPosition dataclasses,
│   │                              # composable entry/exit condition factories
│   ├── results.py                 # GridResult: vectorised scoring, equity metrics
│   ├── robustness.py              # Deflated Sharpe Ratio (Bailey & López de Prado)
│   ├── walk_forward.py            # Walk-forward optimisation windows
│   ├── reporting_v2.py            # Self-contained HTML report generator
│   ├── reporting_charts.py        # SVG chart primitives
│   ├── experiment.py              # Sensitivity/WFO from TOML experiment files
│   ├── indicators.py              # Indicator pre-computation (supertrend, turbulence)
│   ├── pricing.py                 # Deribit fee model, Black-Scholes helpers
│   ├── bt_option_selection.py     # Option leg selection helpers
│   ├── expiry_utils.py            # Expiry date utilities
│   ├── config.py / config.toml   # Config loader + application settings
│   │
│   ├── ui/                        # Interactive Research UI (Panel + Bokeh + Plotly)
│   │   ├── app.py                 # Entry point: python -m backtester.ui.app
│   │   ├── state.py               # AppState param object (shared reactive state)
│   │   ├── log.py                 # UI-scoped logger
│   │   ├── views/                 # One file per tab
│   │   │   ├── sidebar.py         # Run list, strategy picker, param editor, run control
│   │   │   ├── grid_view.py       # Results Grid: sortable/filterable combo table
│   │   │   ├── detail_view.py     # Combo Detail: stats card + equity chart + trade log
│   │   │   ├── overlay_view.py    # Equity Overlay: multi-combo comparison chart
│   │   │   ├── favourites_view.py # Favourites: starred combos with TOML export
│   │   │   └── compare_view.py    # Compare: side-by-side combo metric table
│   │   ├── services/              # Data access layer
│   │   │   ├── store_service.py   # SQLite run index + bundle persistence
│   │   │   ├── cache_service.py   # LRU ResultCache (holds loaded GridResults)
│   │   │   ├── equity_service.py  # Equity curve extraction from GridResult
│   │   │   ├── run_service.py     # Async backtest execution bridge
│   │   │   ├── run_worker.py      # Background worker thread for live runs
│   │   │   ├── repro.py           # Reproducibility metadata helpers
│   │   │   └── toml_export.py     # Favourite → experiment TOML snippet
│   │   ├── charts/
│   │   │   └── equity.py          # Plotly equity + drawdown chart builders
│   │   └── state/
│   │       └── ui_state.db        # SQLite DB (gitignored)
│   │
│   ├── strategies/                # One file per strategy
│   │   └── tests/                 # Strategy unit tests
│   │
│   ├── experiments/               # TOML experiment definitions
│   │   ├── delta_strangle_tp_v1.toml
│   │   └── short_str_turb_dyn_v1.toml
│   │
│   └── ingest/
│       ├── check_data_completeness.py
│       └── bulkdownloadTardis/    # Tardis bulk download pipeline
│
├── tests/                         # UI and integration tests
│   └── ui/                        # Panel UI unit tests (180+ tests)
│
├── indicators/                    # Shared indicator compute functions
│   ├── hist_data.py               # On-disk Binance kline cache (used by indicators)
│   ├── supertrend.py              # SuperTrend computation
│   └── turbulence.py              # Turbulence composite score
│
├── market_hours.py                # US market hours / NYSE calendar (stdlib only)
└── pyproject.toml                 # Python project config and pytest settings
```

**Gitignored directories (local work, not code):**
- `backtester/data/` — parquet snapshots (~924 MB)
- `backtester/archive/` — archived parquets
- `backtester/planning/` — research notes, drafts, reference reports
- `backtester/reports/` — generated HTML reports and run bundles
- `backtester/ui/state/` — SQLite UI state DB
- `indicators/data/` — cached kline data

---

## Data

### Format
Option data is stored as per-day parquet files in `backtester/data/`:
- `options_YYYY-MM-DD.parquet` — 5-minute option chain snapshots
- `spot_YYYY-MM-DD.parquet` — 1-minute BTC spot OHLC bars

All option prices are **BTC-denominated** (e.g. `0.0068 BTC`). USD value = `price × spot`.
`mark_iv` is stored as a **percentage** (e.g. `39.8` = 39.8% annualised vol). Divide by 100 before passing to Black-Scholes.

### Ingestion sources

**Tardis bulk download** (historic data, up to ~2 weeks lag):
```bash
python -m backtester.ingest.bulkdownloadTardis.bulk_fetch
```
See `backtester/ingest/bulkdownloadTardis/TARDIS_DATA_NOTES.md` for format details.

**Sync from VPS live recorder** (done from the CryoTrader repo):
The live tick recorder runs as `ct-recorder` on the VPS and writes daily parquets.
Sync them using `backtester/ingest/tickrecorder/sync.py` in CryoTrader.

Both sources produce the same parquet schema — the engine sees no difference.

### Data paths

`backtester/config.toml` `[data]` section points to the data directory:
```toml
options_parquet = "data"
spot_parquet    = "data"
```
`MarketReplay` loads all `options_YYYY-MM-DD.parquet` and `spot_YYYY-MM-DD.parquet` files found in those directories.

---

## Market Replay

`backtester/market_replay.py` — converts parquet files into a time-stepped iterator that strategies consume.

**`MarketReplay`** loads all parquet files on construction. Strategies iterate over it:

```python
replay = MarketReplay("backtester/data", "backtester/data")
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
stop_loss_pct(pct)                  # close if unrealised loss > pct% of entry
profit_target_pct(pct)              # close if unrealised gain > pct% of entry
max_hold_hours(hours)               # close after N hours
max_hold_days(days)                 # close after N days
time_exit(hour)                     # close at specific UTC hour
index_move_trigger(pct)             # close if spot moved pct% since entry
                                    # (checks 1-min bars, not just 5-min close)
```

### Reprice caching

`_reprice_legs(pos, state)` marks all legs to market and caches the result in `pos._last_reprice_usd`. The engine reads this cache for NAV accounting instead of repricing twice per tick per position. The cache is cleared after each read.

---

## The Engine

`backtester/engine.py` — runs all parameter combos in a **single pass** over the data.

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

---

## Research UI

An interactive Panel-based web app for exploring backtest results without re-running the engine.

```bash
python -m backtester.ui.app              # open on http://localhost:5006
python -m backtester.ui.app --port 5007  # custom port
python -m backtester.ui.app --no-browser # suppress auto-open
python -m backtester.ui.app --dev        # autoreload on file changes
```

### What it reads

The UI scans `backtester/reports/` for run bundles — directories created by `run.py`
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

The engine calls `backtester/indicators.py` → `build_indicators()` once before the grid run, then attaches the computed series to every strategy instance. **All indicator computation uses historic cached data only** — no live API calls inside the backtest loop.

### Available indicators

| Module | Purpose |
|---|---|
| `indicators/supertrend.py` | SuperTrend trend-direction signal |
| `indicators/turbulence.py` | Composite turbulence score (Parkinson RV, trend, burst, decay) |
| `indicators/hist_data.py` | On-disk Binance kline cache — loads/saves to `indicators/data/`, no live fetch at backtest time |

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

Each run writes a self-contained HTML file to `backtester/reports/`. No server or external assets required — open directly in a browser.

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

`backtester/config.toml` — application-level settings. Strategy-specific logic stays in strategy files.

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

All strategies live in `backtester/strategies/`. Register them in `backtester/run.py`.

| CLI key | File | Description |
|---|---|---|
| `long_gamma_whitelist` | `long_gamma_whitelist.py` | Buy straddle/strangle on whitelisted bull/bear regime days |
| `short_str_turb_dyn` | `short_str_turb_dyn.py` | Short strangle; enter only in low-turbulence regime |
| `ss_turb_dyn_mk2` | `ss_turb_dyn_mk2.py` | Short strangle turbulence v2 |
| `ss_turb_dyn_sl` | `ss_turb_dyn_sl.py` | Short strangle turbulence with stop-loss variant |
| `short_generic` | `short_generic.py` | Generic configurable short strangle |
| `short_strangle_weekly_cap` | `short_strangle_weekly_cap.py` | Weekly strangle with premium cap |
| `daily_put_sell` | `daily_put_sell.py` | Sell 1DTE OTM put, delta-selected |
| `deltaswipswap` | `deltaswipswap.py` | Delta-selected swap entry |
| `l_straddle_index_move` | `l_straddle_index_move.py` | Long straddle, exit on BTC index move |
| `preopen_straddle` | `preopen_straddle.py` | Pre-open straddle entry |
| `batman_calendar` | `batman_calendar.py` | Batman calendar spread |
| `bt_supertrend_lc` | `bt_supertrend_lc.py` | Long call with SuperTrend regime filter |

---

## Adding a New Strategy

1. Create `backtester/strategies/my_strategy.py` implementing the `Strategy` protocol:

```python
class MyStrategy:
    name = "my_strategy"
    PARAM_GRID = {
        "delta":    [0.10, 0.15, 0.20],
        "dte":      [1, 2],
        "sl_pct":   [50, 100, 150],
    }

    def configure(self, params):
        self.delta  = params["delta"]
        self.dte    = params["dte"]
        self.sl_pct = params["sl_pct"]
        self.pos    = None

    def on_market_state(self, state):
        # entry logic → open self.pos
        # exit logic  → call close_trade(), return [trade]
        return []

    def on_end(self, state):
        if self.pos:
            return [close_trade(self.pos, state, exit_reason="end")]
        return []

    def reset(self):
        self.pos = None

    def describe_params(self):
        return {"delta": self.delta, "dte": self.dte, "sl_pct": self.sl_pct}
```

2. Register in `backtester/run.py`:
```python
from backtester.strategies.my_strategy import MyStrategy
STRATEGIES["my_strategy"] = MyStrategy
```

3. Run discovery:
```bash
python -m backtester.run --strategy my_strategy
```

4. Once you have a candidate, create `backtester/experiments/my_strategy_v1.toml` and run sensitivity + WFO.

**Key rule: keep `PARAM_GRID` wide and unbiased. Never narrow it after seeing results.**

---

## Testing

```bash
# Full test suite: UI tests + strategy tests (187 tests)
python -m pytest tests/ backtester/strategies/tests/ -v

# Strategy tests only
python -m pytest backtester/strategies/tests/ -v

# UI tests only
python -m pytest tests/ui/ -v

# Live/network tests (deselected by default, require network)
python -m pytest backtester/strategies/tests/ -m live -v
```

Tests live in two directories:
- `tests/ui/` — Panel UI unit tests (state, views, services, filter parser, etc.)
- `backtester/strategies/tests/` — per-strategy backtesting unit tests

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

At BTC ~$84k the index cap ≈ 0.00025 BTC/leg and typically binds for options above ~0.002 BTC. Implemented in `backtester/pricing.py`. Parameters configurable in `config.toml` `[fees]`.

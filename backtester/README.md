# Backtester
75
BTC options backtester using real historic Deribit prices. Replays 5-minute
option snapshots + 1-minute BTC spot OHLC bars, evaluates parameter grids
across strategies in a single data pass, and generates self-contained HTML
reports with equity curves, composite scoring, heatmaps, and trade logs.

Data sources: [Tardis](https://tardis.dev) historic tick data and the live
tick recorder (`ingest/tickrecorder/`) — both produce the same parquet schema.

---

## Directory layout

```
backtester/
├── engine.py              # Single-pass grid runner (run_grid_full)
├── market_replay.py       # Snapshot loader → MarketState iterator
├── strategy_base.py       # Trade/OpenPosition dataclasses, Strategy protocol,
│                          # composable entry/exit condition factories
├── results.py             # GridResult: per-combo stats, scoring, DSR, equity metrics
├── robustness.py          # deflated_sharpe_ratio(), _robustness_stats()
├── reporting_v2.py        # Self-contained HTML report generator (render only)
├── reporting_charts.py    # SVG chart helpers extracted from reporting_v2
├── walk_forward.py        # WFOWindow/WFOResult dataclasses + run_walk_forward()
├── experiment.py          # Load experiment TOMLs; build sensitivity grids
├── pricing.py             # Deribit fee model, Black-Scholes helpers
├── config.py / config.toml  # Runtime config: paths, scoring weights, simulation params
├── run.py                 # CLI entry point (--strategy, --experiment, --mode, --wfo)
├── check_parquet.py       # Dev utility: data quality checks on snapshot files
│
├── strategies/            # One file per strategy (implement Strategy protocol)
│   ├── daily_put_sell.py
│   ├── short_strangle_offset.py
│   ├── short_strangle_delta.py
│   ├── short_strangle_delta_tp.py
│   ├── short_strangle_weekly_tp.py
│   ├── short_strangle_weekly_cap.py
│   ├── deltaswipswap.py
│   └── l_straddle_index_move.py
│
├── experiments/           # Per-experiment TOML files (sensitivity params + WFO config)
│   └── delta_strangle_tp_v1.toml
│
├── ingest/                # Everything that produces input data
│   ├── snapshot_builder.py      # Converts raw tick parquets → backtester snapshots
│   ├── tardis/                  # Tardis.dev fetch/extract pipeline
│   ├── tickrecorder/            # Live Deribit WS recorder (also runs on VPS)
│   └── raw/                     # Raw daily parquets from tickrecorder (gitignored)
│
└── data/                  # Processed snapshots ready for the engine (gitignored)
```

---

## The Three-Step Research Pipeline

Running a parameter grid and picking the best result is statistically dangerous —
with enough combos you will find a "winner" by pure chance. The backtester is
structured around three distinct steps to combat this:

```
Step 1 — Discovery
  python -m backtester.run --strategy delta_strangle_tp
  Wide PARAM_GRID (hundreds of combos), full date range.
  Goal: find which region of parameter space is profitable at all.
  Output: discovery report with heatmaps, best-combo stats, DSR.

Step 2 — Sensitivity
  python -m backtester.run --experiment delta_strangle_tp_v1 --mode sensitivity
  Narrow grid centred on the candidate from Step 1 (±10%/±2h, 5 points per param).
  Goal: is the candidate on a smooth hill or a spike?
  Output: sensitivity report (--robustness auto-enabled), heatmaps around best params.

Step 3 — Walk-Forward Validation
  python -m backtester.run --experiment delta_strangle_tp_v1 --mode wfo
  IS uses the wide PARAM_GRID (honest search space); OOS is truly unseen.
  Goal: does the general region stay profitable on future data?
  Output: WFO report with per-window table, stitched OOS equity, IS/OOS scatter.
```

**Why this separation matters:**

- Strategy files contain one `PARAM_GRID` = a wide, unbiased discovery grid.
  It is never narrowed post-hoc.
- Experiment files capture "what we think is good and why" separately.
  They don't pollute the strategy definition.
- WFO uses the wide discovery grid for its IS runs, so the IS optimiser has a
  real search problem — not a trivially narrow space around a known-good point.

---

## Data pipeline

```
Tardis raw ticks          Live tick recorder (VPS)
(ingest/tardis/data/)     (ingest/raw/ after sync)
         └──────────────┬──────────────┘
               ingest/snapshot_builder.py
                         │
              ┌──────────▼──────────┐
              │  data/              │
              │  options_*.parquet  │  5-min option snapshots
              │  spot_track_*.parquet│  1-min BTC spot OHLC
              └──────────┬──────────┘
                 market_replay.py
              (MarketState iterator)
                         │
                    engine.py
              (single-pass grid run)
              run_grid_full() returns:
               df, keys, nav_daily_df, final_nav_df
                         │
                  results.py
              GridResult.__init__:
                Step 1: _all_combo_stats()  ← vectorised, all combos
                Step 2: _score_combos()     ← percentile-rank + weights
                Step 3: equity_metrics()    ← top-20 only
                Step 4: deflated_sharpe_ratio()  ← DSR for best combo
                         │
               reporting_v2.py
              generate_html(result, robustness=, wfo_result=)
                         │
               report.html  (self-contained)
```

---

## Scoring model

Combos are ranked by a composite score (0 → 1) computed as a weighted sum of
per-metric percentile ranks across all eligible combos:

| Metric | Weight | Direction | What it catches |
|---|---|---|---|
| R² (equity linearity) | 0.15 | ↑ higher | Non-linear curves: sleeping giants, lucky streaks |
| Sharpe (annualised) | 0.15 | ↑ higher | Risk-adjusted return |
| Total PnL | 0.15 | ↑ higher | Absolute profitability |
| Max DD % (intraday) | 0.15 | ↓ lower | Worst peak-to-trough loss |
| Omega ratio | 0.10 | ↑ higher | One catastrophic day hurts more than Sharpe captures |
| Ulcer Index | 0.10 | ↓ lower | Duration × severity of drawdown periods |
| Monthly consistency | 0.10 | ↑ higher | Fraction of months ending positive |
| Profit factor | 0.10 | ↑ higher | Total gains / total losses |

Weights are configured in `config.toml` `[scoring]` — changing them requires no code changes.

**Monthly consistency guard:** if the backtest spans fewer than 2 calendar months,
all consistency values are set to 0.5 (neutral) so this metric contributes no
differentiation on short backtests.

---

## Strategies

| CLI key | Class | Description |
|---|---|---|
| `put_sell` | `DailyPutSell` | Sell 1DTE OTM put, delta-selected; exit on SL or expiry |
| `short_straddle` | `ShortStrangleOffset` | Sell 1DTE ATM straddle / OTM strangle; SL + time/expiry exit |
| `delta_strangle` | `ShortStrangleDelta` | Sell N-DTE strangle, delta-selected; SL + time/expiry exit |
| `delta_strangle_tp` | `ShortStrangleDeltaTp` | Same + take-profit: close when combined ask drops to (1−tp_pct) × entry premium |
| `weekly_strangle_tp` | `ShortStrangleWeeklyTp` | Weekly-expiry strangle with take-profit |
| `weekly_strangle_cap` | `ShortStrangleWeeklyCap` | Weekly-expiry strangle with premium cap at entry |
| `deltaswipswap` | `DeltaSwipSwap` | Delta-selected swap entry; 5-min snapshot data |
| `straddle` | `ExtrusionStraddleStrangle` | Buy nearest-expiry straddle/strangle; exit on BTC index move |

---

## Quick start

### 1. Get data

**Option A — Tardis (historic, one-time):**
```bash
TARDIS_API_KEY=your_key python -m backtester.ingest.tardis.fetch --from 2026-03-09 --to 2026-03-23
```

**Option B — tick recorder (rolling live data):**
```bash
python -m backtester.ingest.tickrecorder.sync --days 14
```

### 2. Build snapshots (~2 min for 15 days)

```bash
python -m backtester.ingest.snapshot_builder
```

Output: `data/options_<from>_<to>.parquet` and `data/spot_track_<from>_<to>.parquet`.
Update the two path keys in `config.toml` to point to the new files.

### 3. Run a backtest

**Discovery (full grid, all combos):**
```bash
python -m backtester.run --strategy delta_strangle_tp
python -m backtester.run --strategy delta_strangle_tp --robustness   # + sensitivity charts
```

**Sensitivity (experiment TOML, ±N% around best):**
```bash
python -m backtester.run --experiment delta_strangle_tp_v1 --mode sensitivity
```

**Walk-forward (IS optimise on wide grid, honest OOS evaluation):**
```bash
python -m backtester.run --experiment delta_strangle_tp_v1 --mode wfo
```

**Legacy flags still work:**
```bash
python -m backtester.run --strategy delta_strangle_tp --wfo --is-days 45 --oos-days 15
```

### 4. View the report

Open the generated HTML in a browser. Each report contains:

- **Risk summary bar** — best combo's key metrics at a glance (Sharpe, R², Omega, Ulcer, max DD)
- **Best-combo box** — all parameters + all scoring metrics + Sortino, Calmar, DSR
- **Fan chart** — equity curves for the top-20 combos with intraday high/low shading
- **Leaderboard** — top-20 ranked by composite score
- **Heatmaps** — auto-generated for every 2D parameter pair
- **Robustness section** — (with `--robustness` or `--mode sensitivity`) distribution chart,
  marginal PnL charts, all-combos table
- **WFO section** — (with `--wfo` or `--mode wfo`) per-window table, OOS equity, IS/OOS scatter
- **Trade log** — every entry/exit for the best combo

---

## Experiment files

`backtester/experiments/<name>.toml` captures a research step against a specific
strategy candidate. It is the bridge between Step 1 (discovery) and Steps 2–3.

```toml
# backtester/experiments/delta_strangle_tp_v1.toml
strategy = "delta_strangle_tp"

[sensitivity]
steps = 5   # points per parameter

[sensitivity.best]
delta            = 0.15
entry_hour       = 18
stop_loss_pct    = 5.0
take_profit_pct  = 0.80
# Fixed params (held constant in sensitivity run):
dte              = 1
max_hold_hours   = 0
skip_weekends    = 1
min_otm_pct      = 0

# Per-param deviation rules:
[sensitivity.deviation.delta]
type   = "pct"   # ±10% of 0.15 → [0.135, 0.143, 0.15, 0.158, 0.165]
amount = 10

[sensitivity.deviation.entry_hour]
type   = "abs"   # ±2 hours → [16, 17, 18, 19, 20]
amount = 2

[sensitivity.deviation.stop_loss_pct]
type   = "pct"
amount = 10      # ±10% of 5.0 → [4.5, 4.75, 5.0, 5.25, 5.5]

[sensitivity.deviation.take_profit_pct]
type   = "pct"
amount = 10      # → [0.72, 0.76, 0.80, 0.84, 0.88]

[wfo]
is_days   = 45
oos_days  = 15
step_days = 15
```

Deviation types: `"pct"` (±N% of best), `"abs"` (±N in natural units), `"fixed"` (constant).

---

## Key design notes

### Prices and units
- All snapshot prices are **BTC-denominated** (e.g. `0.0068 BTC`). USD value = `price × spot`.
- `mark_iv` is stored as a **percentage** (e.g. `39.8` = 39.8%). Divide by 100 before passing to `bs_call` / `bs_put`.
- All NAV and PnL values inside the engine are USD.

### Drawdown: one measure, intraday
- `max_dd_pct` is the **intraday** peak-to-trough measure: daily low vs running NAV high watermark.
- This is strictly ≥ EOD-close-based drawdown and is more conservative and realistic.
- Ulcer Index captures the *duration* dimension: it squares every underwater day, so a
  prolonged recovery costs far more than a brief spike to the same depth.

### Expiry selection
- Delta-based strategies support `dte` ∈ {1, 2, 3}: select the expiry whose settlement
  date is exactly `dte` calendar days ahead.
- `_select_expiry()` matches by date only — it never selects an expiry whose 08:00 UTC
  settlement deadline has already passed at the current tick.

### Reprice caching (performance)
- `_reprice_legs()` writes its result to `pos._last_reprice_usd` after each call.
- The engine's `_open_unrealized_pnl()` reads this cached value instead of calling
  `_reprice_legs` again, eliminating one full reprice per open position per tick.
  The cache is cleared after each read to prevent stale reuse.

### Intra-bar trigger detection
- `index_move_trigger()` checks both the 5-min close and every 1-min high/low within
  the window. Price spikes that reverse before the next 5-min snapshot are not missed.

### Fees
- Deribit model: `min(0.03% × index, 12.5% × option_price)` per leg per side.
- At BTC ~$84k the index cap ≈ 0.00025 BTC/leg and typically binds for options
  above ~0.002 BTC.

---

## Performance

On M1 Mac, 15 days of data (4,027 × 5-min intervals):

| Strategy | Combos | Time |
|---|---|---|
| `delta_strangle_tp` (discovery grid, 600 combos) | 600 | ~1.5 min |
| `delta_strangle_tp` (sensitivity grid, 5-step) | 25 | ~5 s |
| `short_straddle` | 4,860 | ~2 min |
| `straddle` | 4,800 | ~33 s |
| `put_sell` | 770 | ~10 s |

Key engine optimisations:

- **Single data pass** — all combo instances receive the same `MarketState` simultaneously; market data is loaded exactly once.
- **NumPy columnar storage** — option data in contiguous typed arrays (`float32` prices, `uint8` expiry index, `bool` is_call). ~5× less RAM than Python dicts (384 MB → 61 MB for 1.9M rows).
- **Timestamp index** — `np.unique` with `return_index` / `return_counts` for O(1) per-tick array slicing.
- **Lazy `OptionQuote` construction** — `MarketState` holds NumPy slice references; `OptionQuote` objects built only when a strategy calls `get_option()`, with a per-tick dict cache.
- **Vectorised lookups** — `get_option()`, `get_chain()`, `get_atm_strike()` use `np.flatnonzero` masks (~0.5 µs per lookup on ~300 rows).
- **O(1) excursion queries** — `spot_high_since()` / `spot_low_since()` use pre-computed cummax/cummin arrays.
- **Reprice caching** — `_reprice_legs` result stored on `OpenPosition._last_reprice_usd`; engine NAV tracker reads it rather than repricing a second time (saves ~15% wall time on large grids with open positions every tick).
- **LRU-cached expiry parsing** — `_parse_expiry_date` / `_expiry_dt_utc` cached; without it, regex ran 1.5M times per grid run.
- **Inlined comparisons** — `max(a, b)` replaced with `(a if a > b else b)` in the hot-path reprice loop to avoid Python built-in dispatch overhead.

---

## Adding a strategy

1. Create `strategies/my_strategy.py` implementing the `Strategy` protocol
   (see `strategy_base.py` for the full protocol definition):
   - `name: str` — CLI-safe identifier
   - `PARAM_GRID: dict` — `{param: [values]}` for grid search. Keep this **wide and unbiased** (discovery grid). Do not narrow it post-hoc.
   - `configure(params)` — apply one combo's parameters, reset all state
   - `on_market_state(state) → List[Trade]` — called every 5-min tick
   - `on_end(state) → List[Trade]` — force-close any open position at data end
   - `reset()` — clear all state (including date counters and position list)
   - `describe_params() → dict` — return current parameter values for reporting

2. Register in `run.py`:
   ```python
   from backtester.strategies.my_strategy import MyStrategy
   STRATEGIES["my_strat"] = MyStrategy
   ```

3. Run discovery: `python -m backtester.run --strategy my_strat`

4. Once you have a candidate, create `experiments/my_strat_v1.toml` and run
   sensitivity + WFO.

---

## Dev utilities

```bash
# Check data quality of a snapshot parquet
python -m backtester.check_parquet

# Quick sanity-check a single tardis raw file
python backtester/ingest/tardis/_validate.py path/to/file.parquet

# Full quality sweep across all tardis raw parquets
python -m backtester.ingest.tardis.quality_check
```


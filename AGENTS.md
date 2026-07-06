# CryoBacktester — Agent Context / Working Memory

**Version:** 1.0.0 | **Created:** May 2026

Primary orientation guide for AI agents working on CryoBacktester.
Read fully before touching any code.

**This repo contains backtesting code only. There is no live trading, no exchange credentials, no production server.**

---

## ⚠️ Hard Rules for AI Agents

1. **Never `git commit` or `git push`** without explicit user approval.
2. **For any task bigger than a small edit: present a plan first.** Wait for the user to say "CODE" before writing code.
3. **Bug spotted? Describe it, do NOT fix it.** Report the problem and stop. Wait for "CODE".
4. **Run tests before and after any code change:** `python -m pytest backtester/strategies/tests/ -v`
5. **`PARAM_GRID` and `DATE_RANGE` in strategy files are NOT sacred.** Change them freely as part of any analysis or reproduction task — they are working state, not protected config.

---

## What this repo is

BTC options backtester using real Deribit historical tick data. Replays 5-minute option chain snapshots, evaluates parameter grids across strategies in a single data pass, and generates self-contained HTML reports with equity curves, heatmaps, composite scoring, and trade logs.

The companion live trading repo is **CryoTrader** (`https://github.com/Rick987-beep/CryoTrader`).
Strategies are occasionally ported from CryoBacktester → CryoTrader — that is the only coupling.

---

## Repo structure

```
CryoBacktester/
├── backtester/                 # Core backtesting engine (run from repo root)
│   ├── run.py                  # CLI: python -m backtester.run
│   ├── engine.py               # Single-pass grid runner (run_grid_full)
│   ├── market_replay.py        # 5-min snapshot iterator → MarketState
│   ├── results.py              # GridResult: vectorised scoring, equity metrics
│   ├── robustness.py           # Deflated Sharpe Ratio (Bailey & López de Prado)
│   ├── walk_forward.py         # Walk-forward optimisation windows
│   ├── reporting_v2.py         # Self-contained HTML report generator
│   ├── reporting_charts.py     # SVG chart primitives
│   ├── experiment.py           # Sensitivity/WFO from TOML experiment files
│   ├── indicators.py           # Indicator pre-computation (supertrend, turbulence)
│   ├── pricing.py              # Deribit fee model, Black-Scholes helpers
│   ├── bt_option_selection.py  # Option leg selection for backtester
│   ├── expiry_utils.py         # Expiry date utilities
│   ├── config.py               # Config loader
│   ├── config.toml             # Scoring weights, grid params, data paths
│   ├── strategy_base.py        # Strategy protocol, trade dataclasses
│   ├── strategies/             # One file per strategy
│   │   └── tests/              # Strategy unit tests
│   ├── experiments/            # TOML experiment definitions
│   ├── ingest/
│   │   ├── check_data_completeness.py
│   │   └── bulkdownloadTardis/ # Tardis bulk download pipeline
│   ├── data/                   # Parquet snapshots (gitignored, ~924 MB)
│   ├── data_archive/           # Archived parquets (gitignored, ~446 MB)
│   ├── data_spot_rebuilt/      # Rebuilt spot parquets (gitignored)
│   └── reports/                # Generated HTML reports (gitignored)
├── indicators/                 # Local copies of shared indicator compute functions
│   ├── hist_data.py            # On-disk Binance kline cache (for backtesting)
│   ├── supertrend.py           # SuperTrend computation
│   └── turbulence.py           # Turbulence composite score
└── market_hours.py             # US market hours / NYSE calendar (stdlib only)
```

---

## CLI

```bash
# Discovery — wide parameter grid, full date range
python -m backtester.run --strategy <name>

# With robustness stats (Deflated Sharpe Ratio)
python -m backtester.run --strategy short_str_turb_dyn --robustness

# Sensitivity analysis around a known-good candidate
python -m backtester.run --experiment short_str_turb_dyn_v1 --mode sensitivity

# Walk-forward validation
python -m backtester.run --experiment short_str_turb_dyn_v1 --mode wfo
```

Current strategy names: `short_str_turb_dyn`, `blueprint_howto`

All other strategies are in `backtester/archive/strategies_to_be_fixed/` — not in the CLI registry.

---

## Runtime model

1. Load snapshot parquets from `backtester/data/` via `MarketReplay`
2. `engine.run_grid_full()` runs **all parameter combos in one pass** over the data
3. `GridResult` computes vectorised metrics per combo: Sharpe, PnL, Omega, Ulcer Index, drawdown, DSR, composite score
4. `reporting_v2.generate_html()` renders a self-contained HTML file (no recomputation)

---

## Research pipeline (three steps)

```
Step 1 — Discovery
  Wide PARAM_GRID (hundreds of combos), full date range.
  Goal: find which region of parameter space is profitable at all.

Step 2 — Sensitivity
  --experiment <name> --mode sensitivity
  Narrow grid around the Step 1 candidate.
  Goal: is the candidate on a smooth hill or a spike?

Step 3 — Walk-Forward Validation
  --experiment <name> --mode wfo
  IS uses wide PARAM_GRID; OOS is truly unseen.
  Goal: does the region stay profitable on future data?
```

**PARAM_GRID in each strategy file is the wide, unbiased discovery grid — never narrow it post-hoc.**
Experiment TOMLs in `backtester/experiments/` capture candidates separately.

---

## Testing

```bash
# Run strategy tests (always do this)
python -m pytest backtester/strategies/tests/ -v

# Live/network tests only when explicitly asked
python -m pytest backtester/strategies/tests/ -m live -v
```

Tests live in `backtester/strategies/tests/`. `@pytest.mark.live` tests require network access and are deselected by default (`addopts = "-m 'not live'"`).

---

## Data

Parquet snapshots live in `backtester/data/` (~924 MB, gitignored). Two ingestion sources:

**Tardis bulk download** (historic, up to ~2 weeks lag):
```bash
python -m backtester.ingest.bulkdownloadTardis.bulk_fetch
```

**Sync live recorder data from VPS** (done from the CryoTrader repo):
The live tick recorder runs as `ct-recorder` on the VPS and writes daily parquets.
Sync them down using `backtester/ingest/tickrecorder/sync.py` in CryoTrader.

---

## Indicators (`indicators/`)

Pure-compute functions used by backtester strategies via `backtester/indicators.py`:

| File | Purpose |
|------|---------|
| `hist_data.py` | Persistent on-disk Binance kline cache — loads/saves to disk, no live fetch at backtest time |
| `supertrend.py` | SuperTrend computation |
| `turbulence.py` | Composite turbulence score (Parkinson RV, trend, burst, decay) |

**Design rule**: backtesting must be fully reproducible from cached/historic data.
Never add live-API fetches inside the backtest loop — only `hist_data.py`-style on-disk caches.
These indicator files are separate copies from CryoTrader's `indicators/` — they diverge independently.

---

## Coding conventions

- Python 3.12; venv at `.venv/`
- Strategies implement the `Strategy` protocol from `backtester/strategy_base.py`
- One strategy per file in `backtester/strategies/`
- `PARAM_GRID` in each strategy = wide, unbiased discovery grid (never narrowed post-hoc)
- Experiment TOMLs in `backtester/experiments/` capture "what we think is good and why"
- `logging.getLogger(__name__)` in every module

---

## Writing a new strategy — quick reference

The canonical pattern lives in `backtester/strategies/blueprint_howto.py` — read it first.
Full detail is in `docs/strategy_howto.md`.

### Required imports
```python
from backtester.bt_option_selection import select_by_delta
from backtester.expiry_utils import expiry_dt_utc, select_expiry
from backtester.pricing import deribit_fee_per_leg, EXPIRY_HOUR_UTC
from backtester.strategy_base import (
    OpenPosition, Trade, check_expiry, close_position,
    price_legs, profit_target_pct, stop_loss_pct, max_hold_hours,
)
```

### configure() — wire up exit conditions
```python
def configure(self, params):
    self._sl_pct  = float(params["stop_loss_pct"])
    self._tp_pct  = float(params.get("take_profit_pct", 0.0))
    self._pos     = None
    self._opened  = False
    # SL uses mark (stable, not manipulable by wide spreads)
    # TP uses executable (bid/ask — only fires when real market price available)
    self._exit_conds = [stop_loss_pct(self._sl_pct, price_mode="mark")]
    if self._tp_pct > 0:
        self._exit_conds.append(profit_target_pct(self._tp_pct, price_mode="executable"))
```

### Required leg fields (at open)
```python
leg = {
    "strike":          float,   # USD
    "is_call":         bool,
    "expiry":          str,     # e.g. "28MAY26"
    "side":            "sell",  # or "buy" — drives price_legs() per-leg pricing
    "qty":             float,
    "price_btc":       float,   # fill price (bid for short, ask for long)
    "entry_price":     float,   # same as price_btc (alias)
    "entry_price_usd": float,   # price_btc × spot × qty
    "entry_spot":      float,   # spot at entry (for close_position PnL math)
    "entry_bid":       float,   # for logs / reporting
    "entry_ask":       float,
    "entry_mark":      float,
    "entry_iv":        float,   # mark_iv from parquet (already %, e.g. 34.4 = 34.4%)
    "entry_delta":     float,
    "fee_usd_open":    float,   # deribit_fee_per_leg(spot, entry_price_usd)
}
```

### Required leg fields (added at close, before calling close_position)
```python
leg["exit_price_btc"] = float   # fill price at close
leg["exit_price_usd"] = float   # exit_price_btc × exit_spot × qty
```

### pos.metadata — mandatory keys
```python
metadata = {
    "direction": "sell",   # or "buy" — drives stop_loss_pct/profit_target_pct
    "expiry":    expiry,   # expiry code string
    "expiry_dt": exp_dt,   # tz-aware datetime — used by check_expiry()
    "pos_id":    pos_id,   # monotonic int — links open fills to close fills
}
```

### IV note
`mark_iv` in parquet is stored as a **percentage** (e.g. `34.4` = 34.4%).
Do NOT multiply by 100. Do NOT divide by 100 when storing in leg dict.

### price_legs modes
- `"mark"` — exchange model price; stable; use for SL
- `"executable"` — ask for sell legs, bid for buy legs; use for TP
- `"bid"` / `"ask"` — always that side regardless of leg direction

### Register in run.py
```python
from backtester.strategies.my_strategy import MyStrategy
STRATEGIES["my_strategy"] = MyStrategy
```

---

## Key documents

| File | Content |
|------|---------|
| `README.md` | Full backtester workflow, research pipeline, all sections |
| `docs/strategy_howto.md` | How to write a new strategy — authoritative reference |
| `backtester/strategies/blueprint_howto.py` | Canonical working strategy implementation |
| `backtester/config.toml` | Scoring weights, grid params, simulation config |
| `backtester/ingest/bulkdownloadTardis/TARDIS_DATA_NOTES.md` | Tardis data format notes |
| `backtester/ingest/bulkdownloadTardis/TARDIS_ARCHIVE_PLAN.md` | Raw options_chain archive to Storage Box (download-only, pre-expiry) |
| `backtester/ingest/bulkdownloadTardis/BULK_DOWNLOAD_PLAN.md` | Bulk extract pipeline (gz → parquets) |

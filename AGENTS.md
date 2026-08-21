# CryoBacktester — Agent Context / Working Memory

**Version:** 1.0.0 | **Created:** May 2026

Primary orientation guide for AI agents working on CryoBacktester.
Read fully before touching any code.

**This repo contains backtesting code only. There is no live trading, no exchange credentials, no production server.**

---

## ⚠️ Hard Rules for AI Agents

1. **Never `git push`** without explicit user approval. **Do commit** after each named research step (new strategy version, experiment A/B/C run, a selector/grid closed) once tests pass — current worktree / topic branch, do not wait for “please commit”. Prefer `wip/<topic>` if still on `main` at the start of a multi-step arc. Never commit secrets, `data/runs/`, or one-off analysis blobs. See `.cursor/rules/step-commits.mdc`.
2. **For any task bigger than a small edit: present a plan first.** Wait for the user to say "CODE" before writing code.
3. **Bug spotted? Describe it, do NOT fix it.** Report the problem and stop. Wait for "CODE".
4. **Run tests before and after any code change:** `python -m pytest workspace/tests/ -v`
5. **`PARAM_GRID` and `DATE_RANGE` in strategy files are NOT sacred.** Change them freely as part of any analysis or reproduction task — they are working state, not protected config.

---

## What this repo is

BTC options backtester using real Deribit historical tick data. Replays 5-minute option chain snapshots, evaluates parameter grids across strategies in a single data pass, and generates self-contained HTML reports with equity curves, heatmaps, composite scoring, and trade logs.

The companion live trading repo is **CryoTrader** (`https://github.com/Rick987-beep/CryoTrader`).
Strategies are occasionally ported from CryoBacktester → CryoTrader — that is the only coupling.

---

## Repo structure

Three planes: **product** (`backtester/`), **use** (`workspace/`), **data** (`data/`).

```
CryoBacktester/
├── backtester/          # PRODUCT — engine, UI, indicators code, ingest, compare
│   ├── core/            # includes paths.py data-plane resolver
│   ├── strategies/      # compatibility shims → workspace.strategies
│   ├── ui/  reporting/  research/  indicators/  ingest/  compare/
│   │         # ui: desktop.py (native) + app.py (browser)
│   └── run.py           # CLI; STRATEGIES façade from workspace.catalog
├── scripts/macos/CryoBacktester.app   # thin local Dock launcher → .venv desktop
├── workspace/           # USE — strategies by family, experiments, strategy tests
│   ├── catalog.py       # families + stable strategy IDs (never rename IDs)
│   ├── strategies/{tudysho,theta_engine,other}/
│   ├── experiments/
│   └── tests/
├── data/                # DATA PLANE — market, klines, runs (gitignored blobs)
├── tests/               # Product / UI / integration tests
├── analysis/
└── docs/
```

Path overrides: `CRYOBT_MARKET_DATA`, `CRYOBT_KLINE_DIR` / `CRYOTRADER_KLINE_DIR`, `CRYOBT_RUNS` (see `backtester.core.paths`).

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

# Research UI — native window (preferred)
python -m backtester.ui.desktop
# or: open scripts/macos/CryoBacktester.app

# Research UI — browser / Terminal (dev)
python -m backtester.ui.app --no-browser
```

Strategy IDs: see `workspace/catalog.py` (tudysho*, theta_engine_v*, blueprint_howto, …)

All other strategies are in `backtester/archive/strategies_to_be_fixed/` — not in the CLI registry.

---

## Runtime model

1. Load snapshot parquets from `backtester/data/` via `MarketReplay`
2. `engine.run_grid_full()` runs **all parameter combos in one pass** over the data
3. `GridResult` computes vectorised metrics per combo: Sharpe, PnL, Omega, Ulcer Index, drawdown, DSR, composite score
4. `backtester.reporting.generate_html()` renders a self-contained HTML file (no recomputation)

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
python -m pytest workspace/tests/ -v

# Live/network tests only when explicitly asked
python -m pytest workspace/tests/ -m live -v
```

Tests live in `workspace/tests/`. `@pytest.mark.live` tests require network access and are deselected by default (`addopts = "-m 'not live'"`).

---

## Data

Parquet snapshots live in `data/market/` (~924 MB, gitignored). Two ingestion sources:

**Tardis bulk download** (historic, up to ~2 weeks lag):
```bash
python -m backtester.ingest.tardis.bulk_fetch
```

**Sync live recorder data from VPS** (done from the CryoTrader repo):
The live tick recorder runs as `ct-recorder` on the VPS and writes daily parquets.
Sync them down using `backtester/ingest/tickrecorder/sync.py` in CryoTrader.

---

## Indicators (`backtester/indicators/`)

Pure-compute functions used by backtester strategies via `backtester.indicators.pipeline`:

| File | Purpose |
|------|---------|
| `pipeline.py` | `IndicatorDep` / `build_indicators` — wires compute into the engine |
| `hist_data.py` | Persistent on-disk Binance kline cache — loads/saves to disk, no live fetch at backtest time |
| `supertrend.py` | SuperTrend computation |
| `turbulence.py` | Composite turbulence score (Parkinson RV, trend, burst, decay) |
| `trend_regime.py` | 3-state BTC trend composite (+1 / 0 / −1) |
| `ingest_klines.py` | CLI to refresh the on-disk kline cache |

**Design rule**: backtesting must be fully reproducible from cached/historic data.
Never add live-API fetches inside the backtest loop — only `hist_data.py`-style on-disk caches.
These indicator files are separate copies from CryoTrader's `indicators/` — they diverge independently.

---

## Coding conventions

- Python 3.12; venv at `.venv/`
- Strategies implement the `Strategy` protocol from `backtester/core/strategy_base.py`
- **Canonical strategy code** lives under `workspace/strategies/{family}/` (tudysho, theta_engine, other)
- Register new strategies in `workspace/catalog.py` (stable ID + family + status) — never rename IDs
- `backtester/strategies/*.py` are **compatibility shims** only; do not put new logic there
- `PARAM_GRID` in each strategy = wide, unbiased discovery grid (never narrowed post-hoc)
- Experiment TOMLs in `workspace/experiments/`
- Market data / runs / kline cache: `data/` (see `backtester.core.paths` + env overrides)
- `logging.getLogger(__name__)` in every module

---

## Writing a new strategy — quick reference

The canonical pattern lives in `workspace/strategies/other/blueprint_howto.py` — read it first.
Full detail is in `docs/strategy_howto.md`.
Put new strategies under the right family dir and register them in `workspace/catalog.py`.

### Required imports
```python
from backtester.core.option_selection import select_by_delta
from backtester.core.expiry_utils import expiry_dt_utc, select_expiry
from backtester.core.pricing import deribit_fee_per_leg, EXPIRY_HOUR_UTC
from backtester.core.strategy_base import (
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

### Register in catalog (not run.py)
```python
# workspace/catalog.py — add a StrategySpec in _build_specs():
from workspace.strategies.other.my_strategy import MyStrategy
...
StrategySpec("my_strategy", "other", MyStrategy, status="active"),
```
Optional: add a thin shim at `backtester/strategies/my_strategy.py` that re-exports the class
so old `from backtester.strategies.my_strategy import …` imports keep working.

`backtester.run.STRATEGIES` is built automatically from the catalog.

---

## Research learnings (not locks)

These are results, not bans. New experiments are always allowed — including
revisiting the same region with a different idea.

**TuDySho 98% WR cells** sell very low delta, 1DTE, late NYC, often with
`min_otm` — they do not size (6 contracts per 1 BTC equity nightly).

**`tudysho_v4`** (bundle `tudysho_v4_20260819_123515`, 2025-04-11 → 2026-08-18):
honest 1DTE strangle at delta 0.15–0.25, morning–noon entry, 2–3× mark SL +
% proximity. Loss-rate floor worked (every cell ≥17% losers) but the book did
not pay: 11/324 profitable, median −$25k / −31% DD on $100k, median PF 0.79.
Best cell +16% / −13% DD sat at 0.15 / 12:00 NYC / turbulence 100. Stops
(~23% of trades) cost ~3× a typical win; open fees hit the Deribit 10.42%
cap of credit. Earlier entry was worse. This *grid* is closed; the version
is frozen as the artefact. Related ideas (defined-risk, longer DTE, other
tenors) are still open.

**`theta_engine_v14` `dte123`** (run 730, 2025-08-17 → 2026-08-18):
equal-weight 25Δ RR on exact 1/2/3 DTE never beat `front` (1 DTE) or
`skew6` (~14d) on the four baseline books. Front or skew6 is enough;
`dte123` stays in code as a closed selector.

**v14 #1 book:** RichForce2 15 skew6 (`eff2523b17b8`, run 727). Fan-out
only: Daily16, RichForce2 16 front, RichDay15.

**`theta_engine_v16`:** Mode C shorts + premium-match paired wing
(`wing_price_div` / `wing_budget_pct`).

**`theta_engine_v17`:** v14 plus investor D/G sidecar, locked to RichForce2
16 front (`c8573c839903`). Meter first; overlays later. (`theta_engine_v15`
was never a shipped ID — do not reuse.)

---

## Key documents

| File | Content |
|------|---------|
| `README.md` | Full backtester workflow, research pipeline, all sections |
| `CHANGELOG.md` | Checkpoint history of notable product changes |
| `scripts/macos/brand/DESIGN.md` | Cryo product-family visual language (icons / palette) |
| `docs/strategy_howto.md` | How to write a new strategy — authoritative reference |
| `workspace/strategies/other/blueprint_howto.py` | Canonical working strategy implementation |
| `workspace/catalog.py` | Family registry + stable strategy IDs |
| `workspace/README.md` / `data/README.md` | Use-plane and data-plane conventions |
| `backtester/core/config.toml` | Scoring weights, grid params, simulation config |
| `backtester/core/paths.py` | Data-plane path resolver + env overrides |
| `backtester/ingest/tardis/TARDIS_DATA_NOTES.md` | Tardis data format notes |
| `backtester/ingest/tardis/TARDIS_ARCHIVE_PLAN.md` | Raw options_chain archive to Storage Box (download-only, pre-expiry) |
| `backtester/ingest/tardis/BULK_DOWNLOAD_PLAN.md` | Bulk extract pipeline (gz → parquets) |

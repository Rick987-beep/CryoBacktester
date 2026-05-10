# CryoBacktester

BTC options backtester using real Deribit historical tick data.

Replays 5-minute option chain snapshots, evaluates parameter grids across strategies in a single data pass, and generates self-contained HTML reports with equity curves, composite scoring, heatmaps, and trade logs.

## Quickstart

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run a strategy discovery grid
python -m backtester.run --strategy short_str_turb_dyn

# Sensitivity analysis
python -m backtester.run --experiment short_str_turb_dyn_v1 --mode sensitivity

# Walk-forward validation
python -m backtester.run --experiment short_str_turb_dyn_v1 --mode wfo
```

## Documentation

See [`backtester/README.md`](backtester/README.md) for the full research workflow.

## Tests

```bash
python -m pytest backtester/strategies/tests/ -v
```

## Data

Parquet snapshots live in `backtester/data/` (gitignored, ~924 MB).
Ingest via Tardis bulk download (`backtester/ingest/bulkdownloadTardis/`) or sync from the VPS recorder in CryoTrader.

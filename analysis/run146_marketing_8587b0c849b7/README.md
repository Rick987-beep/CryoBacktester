# Run 146 Marketing Package — tudysho combo `8587b0c849b7`

Self-contained export for marketing / design work. Copy this folder to the misc repo as-is.

## Source backtest

| Field | Value |
|-------|-------|
| UI link | `http://localhost:5007/?tab=Combo+Detail&run=146&combo=8587b0c849b7` |
| Strategy | `tudysho` |
| Run bundle | `backtester/reports/tudysho_20260702_095428.bundle` |
| Full grid report | `backtest_report.html` (copy of `tudysho_20260702_095428.html`) |
| Date range | 2025-06-26 → 2026-06-27 |
| Starting capital | $100,000 |
| Combo index | 445 / 1024 |

## Combo parameters

See `params.json`. Summary:

- 1-DTE strangle, delta 0.10, min OTM 2.6%
- Entry 16:00 UTC, turbulence threshold 60
- Proximity stop: 8h buffer, $1,000 USD
- NAV premium stop 80%, max 12 contracts per 1 BTC equity
- No Friday trades

## Headline metrics (`stats.json`)

| Metric | Value |
|--------|-------|
| Total return | +92.9% |
| Sharpe | 7.65 |
| Sortino | 20.39 |
| Calmar | 8.66 |
| Max drawdown | 10.6% |
| Profit factor | 4.44 |
| Trades | 144 |
| Win rate | 99.3% |
| Positive months | 100% |

## File guide

### Raw data (for charts & copy)

| File | Description |
|------|-------------|
| `equity_daily.csv` | Daily NAV open/high/low/close, cumulative return %, drawdown % — primary input for custom equity charts |
| `combo_nav_daily.csv` | Raw per-combo NAV parquet export (intraday hi/lo/close from backtester) |
| `combo_trades.csv` | Round-trip trade log with PnL, entry/exit dates, legs metadata |
| `combo_fills.csv` | Individual open/close fills (if present in bundle) |
| `stats.json` | Pre-computed performance ratios for marketing bullets |
| `params.json` | Parameter tuple for this combo |
| `metadata.json` | Provenance: run id, bundle path, git sha, param grid, UI URL |

### Generated visuals

| File | Use |
|------|-----|
| `equity_chart_16x9.svg` | Web / deck — cumulative return + drawdown panel |
| `equity_chart_square.svg` | Square social crop |
| `equity_chart_16x9_linkedin.svg` / `.png` | LinkedIn landscape (1600×900 PNG) |
| `equity_chart_square_linkedin.svg` / `.png` | LinkedIn square post |
| `marketing_report.html` | Standalone one-pager with chart + bullet points |

### Source artefacts

| File | Description |
|------|-------------|
| `source/bundle_meta.json` | Full `meta.json` from the run bundle |
| `backtest_report.html` | Complete grid HTML report (all 1024 combos) |

## Regenerate

From repo root:

```bash
PYTHONPATH=. python analysis/run146_marketing_8587b0c849b7/build_marketing.py
```

Requires `.venv` with project dependencies. PNG export uses headless Chrome on macOS.

## Disclaimer

Past simulated performance is not indicative of future results. Figures are from a backtest on historical Deribit tick data with fees and realistic fills — not live client accounts.

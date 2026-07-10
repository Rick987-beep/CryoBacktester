# Tudysho combined slots — marketing package

Marketing export for **tudysho slots A + B + C** — full union, no dropped trades.

## Merge

**All 224 trades** from A (143) + B (30) + C (51). Slots are scheduled so positions do not overlap (Mon B expires 08:00 UTC before Mon A at 16:00 NYC). See `METHODOLOGY.md`.

## Headline metrics (`stats.json`)

| Metric | Value |
|--------|-------|
| Trades | 224 |
| Total return | ~+133% |
| CAGR | ~+133% |
| Sharpe | ~9.6 (EOD MTM returns) |
| Sortino | ~26.2 |
| Calmar | ~12.4 |
| Max drawdown | ~10.7% (intraday-scaled) |
| Profit factor | ~25.9 (trade-level) |
| By slot | A: 143, B: 30, C: 51 |

Regenerate: `python analysis/tudysho_slots_marketing/build_marketing.py`

## Files

Same layout as `run146_marketing`: `combo_trades.csv`, `equity_daily.csv`, `stats.json`, charts, `marketing_report.html`.

## Disclaimer

Isolated backtests merged with validated non-overlap. See `METHODOLOGY.md`.

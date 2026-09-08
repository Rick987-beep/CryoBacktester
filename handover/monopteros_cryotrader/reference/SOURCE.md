# Reference code — source of truth

Files in this folder are **verbatim copies** from the CryoBacktester repo,
refreshed by `build_handover.py`.

| File | Canonical path |
|------|----------------|
| `monopteros.py` | `workspace/strategies/tudysho/monopteros.py` |
| `test_tudysho_monopteros.py` | `workspace/tests/test_tudysho_monopteros.py` |
| `market_hours.py` | `backtester/core/market_hours.py` |
| `expiry_utils.py` | `backtester/core/expiry_utils.py` |

Catalog ID: **`tudysho_monopteros`**. Product / marketing name: **Monopteros**.

## Live needs

- Schedule routing (`resolve_schedule`), NYC `entry_time`, watch windows
- Global sizing (`nav_premium_pct` / `max_qty_per_1btc_equity`)
- Exits: premium SL (per schedule), strike proximity, equity drawdown stop
- `to_nyc` / `to_utc` from `market_hours.py`

## Dependencies (not copied — read in repo if porting)

- `backtester/core/strategy_base.py` — `OpenPosition`, `Trade`,
  `stop_loss_pct`, `strike_proximity_stop`, `equity_drawdown_stop`, …
- `backtester/core/option_selection.py` — `select_by_delta`
- `backtester/core/pricing.py` — `deribit_fee_per_leg`
- `backtester/indicators/` — turbulence series for the entry gate

## Single-instance backtester vs live

The copied code is one **backtester strategy instance** for the master combo:

- `_last_trade_nyc_date[schedule_id]` — at most one open per schedule per NYC day
- Max one concurrent position account-wide
- Monday can fire `mon_early` then `mon_thu` sequentially

**Live CryoTrader:** one account, one strategy (`monopteros`) with three
schedule windows. See `POSITION_RULES.md`.

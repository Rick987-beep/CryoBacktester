# Reference code — source of truth

Files in this folder are **verbatim copies** from the CryoBacktester repo, refreshed by `build_handover.py`.

| File | Canonical path |
|------|----------------|
| `tudysho.py` | `backtester/strategies/tudysho.py` |
| `test_tudysho.py` | `backtester/strategies/tests/test_tudysho.py` |
| `backtester/core/market_hours.py` | `backtester/core/market_hours.py` |
| `expiry_utils.py` | `backtester/core/expiry_utils.py` |

## Dependencies (not copied — read in repo if porting)

`tudysho.py` imports from:

- `backtester/core/strategy_base.py` — exit factories, `OpenPosition`, `Trade`, `close_position`
- `backtester/core/option_selection.py` — `select_by_delta`
- `backtester/core/pricing.py` — `deribit_fee_per_leg`
- `backtester/indicators/pipeline.py` / `backtester/indicators/turbulence.py` — turbulence composite

## Single-instance vs live multi-slot

The copied `tudysho.py` is one **backtester strategy instance** per grid combo. It uses:

- `_last_trade_date` — at most **one new entry per UTC calendar day** on that instance
- `max_concurrent = dte + 1` — allows up to two open legs when `dte=1` (rolling 1-DTE window)

**Live CryoTrader** runs **three slots** (A, B, C) on one account. See `POSITION_RULES.md` for the account-wide and per-slot limits. Do not map `_last_trade_date` 1:1 to a single global flag in live.

# Reference code — source of truth

Files in this folder are **verbatim copies** from the CryoBacktester repo,
refreshed by `build_handover.py`.

| File | Canonical path |
|------|----------------|
| `v18.py` | `workspace/strategies/theta_engine/v18.py` |
| `v17.py` | `workspace/strategies/theta_engine/v17.py` |
| `v14.py` | `workspace/strategies/theta_engine/v14.py` |
| `_common.py` | `workspace/strategies/theta_engine/_common.py` |
| `test_theta_engine_v18.py` | `workspace/tests/test_theta_engine_v18.py` |
| `expiry_utils.py` | `backtester/core/expiry_utils.py` |

Catalog ID: **`theta_engine_v18`**. Product / marketing name: **Starnberg**.

## Inheritance

```
ThetaEngineV18 (stops + wing)
  └─ ThetaEngineV17 (investor D/G sidecar; rich_mode closed)
       └─ ThetaEngineV14 (RichForce2 / front / short-DTE naked short)
            └─ helpers in _common.py
```

Live needs the **v18 open/exit/wing path**. Investor Greeks sidecar
(`track_investor_greeks`) is optional for live; keep if CryoTrader wants the
meter, otherwise stub/no-op.

## Dependencies (not copied — read in repo if porting)

- `backtester/core/strategy_base.py` — `OpenPosition`, `Trade`,
  `strike_proximity_stop_pct`, `close_position`, …
- `backtester/core/option_selection.py` — `select_by_delta`
- `backtester/core/pricing.py` — `deribit_fee_per_leg`
- `backtester/indicators/vol_context.py` — daily VRP for RichForce2
- `backtester/indicators/front_25d_iv_rank.py` — only if rich_mode were on
  (it is **off** for this lock)

## Single-instance backtester vs live

The copied code is one **backtester strategy instance** per grid combo:

- `_last_trade_date` — at most one new entry per UTC calendar day
- `max_concurrent=1` on the locked combo
- Wing attach is mandatory when `wing_pct>0`; missing outer → trade rolled back
  (never naked fill)

**Live CryoTrader:** one account, one slot (`starnberg`). See
`POSITION_RULES.md`.

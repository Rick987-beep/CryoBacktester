# Monopteros → CryoTrader handover package

Self-contained package for implementing **Monopteros**
(`tudysho_monopteros`) in CryoTrader as **one strategy with three NYC
schedules**.

**Product name:** Monopteros  
**Backtester ID:** `tudysho_monopteros`  
**Locked combo:** `7e21b64f1d94` (run **770**, bundle
`tudysho_monopteros_20260904_105017.bundle`)  
**CryoTrader status:** TuDySho-family port — replace obsolete multi-combo slot
A/B/C packs; do not edit CryoTrader from this repo automatically.

**Position limits:** `POSITION_RULES.md`  
**Clocks:** `TIMEZONE.md` — NYC `entry_time` (DST-aware)  
**Marketing report (master):**
`workspace/marketing/ship/monopteros/monopteros_strategyreport_082026.html`
(catalog: [`workspace/marketing/catalog.json`](../../workspace/marketing/catalog.json))

---

## Live lock — one strategy, three schedules

| Schedule | When (NYC) | Entry | `dte` | Notes |
|----------|------------|-------|------|-------|
| `mon_early` | Mon | **00:05** | 0 | Same-day expiry |
| `mon_thu` | Mon–Thu | **14:00** | 1 | Next daily expiry |
| `fri` | Fri | **12:30** | 1 | Saturday expiry |

All schedules share master combo **`7e21b64f1d94`**. Authoritative routing:
`LIVE_PARAM_SCHEDULE.json` · deploy params: `params/monopteros.json`.

### Shared lock (do not change without a new backtest)

| Knob | Value |
|------|-------|
| `nav_premium_pct` | **0.5** |
| `max_qty_per_1btc_equity` | **6** |
| `equity_drawdown_stop_pct` | **5** |
| Structure | Naked short strangle (no wings) |
| Max concurrent | 1 |
| Take-profit | off |

Schedule trade params (delta, turb, SL, proximity) live under
`params/monopteros.json` → `schedules`.

### Position limits (read `POSITION_RULES.md`)

1. **One concurrent open position** on the account.
2. **Each schedule** may open at most **once per NYC calendar day**.
3. **Monday = two trades:** `mon_early` then `mon_thu` — sequential, not overlapping.

---

## Backtest stats (locked combo)

| Metric | Value |
|--------|-------|
| Window | 2025-04-11 → 2026-08-31 |
| Capital | $100,000 |
| Trades | 382 |
| Total P&L | +$113,294 (+113.3%) |
| Sharpe | 4.10 |
| Sortino | 4.72 |
| Calmar | 6.37 |
| Max DD (intraday) | 11.35% |
| Win rate | 97.9% |
| Profit factor | 3.74 |
| Exits | 376 expiry · 4 proximity · 2 premium SL |
| Fees | ~$19,770 |

Full metrics: `backtests/stats_monopteros.json`.  
Favourite note: Aug-2026 master from schedule locks
`16dcb4f42c9a` / `b012cb51c397` / `fd39a836394d`.

**Expiry is not a clean win.** Most losers also expired. Equity DD stop did not
fire in this sample but remains on every ticket.

---

## Quick start (CryoTrader agent)

1. **`TIMEZONE.md`** — NYC `entry_time` vs UTC system time (read first).
2. **`POSITION_RULES.md`** — concurrent + per-schedule daily limits.
3. **`CRYOTRADER_IMPLEMENTATION.md`** — port checklist.
4. **`STRATEGY_LOGIC.md`** — schedules, sizing, exits.
5. **`DIFF_vs_short_str_turb_dyn.md`** — vs CryoTrader base / old slots.
6. **`params/monopteros.json`** — deploy param set.
7. **`reference/monopteros.py`** + **`reference/test_tudysho_monopteros.py`** —
   port behavior & tests (`reference/SOURCE.md`).

---

## Package layout

```
handover/monopteros_cryotrader/
├── README.md
├── POSITION_RULES.md
├── TIMEZONE.md
├── LIVE_PARAM_SCHEDULE.json
├── STRATEGY_LOGIC.md
├── CRYOTRADER_IMPLEMENTATION.md
├── DIFF_vs_short_str_turb_dyn.md
├── build_handover.py
├── params/
│   └── monopteros.json
├── reference/
│   ├── SOURCE.md
│   ├── monopteros.py
│   ├── test_tudysho_monopteros.py
│   ├── market_hours.py
│   └── expiry_utils.py
└── backtests/
    ├── summary.json
    ├── stats_monopteros.json
    ├── trades_monopteros.csv
    ├── nav_daily_monopteros.csv
    └── fills_monopteros.csv
```

Regenerate: `python handover/monopteros_cryotrader/build_handover.py`

---

## Version note

| Artifact | Role |
|----------|------|
| `reference/monopteros.py` | Verbatim copy of `workspace/strategies/tudysho/monopteros.py` |
| CryoTrader `short_str_turb_dyn` / prior tudysho | Execution / turbulence patterns to reuse |
| Obsolete `handover/tudysho_cryotrader/` | **Removed** — do not revive old slot hashes |

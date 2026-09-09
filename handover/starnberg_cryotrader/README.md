# Starnberg → CryoTrader handover package

Self-contained package for implementing **Starnberg**
(`theta_engine_v18`) in CryoTrader as **one strategy / one slot**.

**Product name:** Starnberg  
**Backtester ID:** `theta_engine_v18`  
**Locked combo:** `a1d621a81904` (run **746**, bundle
`theta_engine_v18_20260824_143142.bundle`)  
**CryoTrader status:** greenfield — no existing theta live strategy

**Position limits:** `POSITION_RULES.md`  
**Clocks:** `TIMEZONE.md` — entry is 16:00 UTC (same as system time)  
**Marketing report (master):**
`workspace/marketing/ship/starnberg/starnberg_strategyreport_082026.html` (catalog:
[`workspace/marketing/catalog.json`](../../workspace/marketing/catalog.json))

---

## Live lock — one slot

| Slot | When (UTC) | Combo | Entry | Book | Stops | Wing |
|------|------------|-------|-------|------|-------|------|
| **`starnberg`** | Mon–Fri | `a1d621a81904` | **16:00 UTC** | RichForce2 (`rf2_1600`) | Full2Eq8 | 7% width, min $2500 |

Authoritative routing: `LIVE_PARAM_SCHEDULE.json` · deploy params:
`params/starnberg.json`.

### Shared lock (do not change without a new backtest)

| Knob | Value |
|------|-------|
| `dte` | 1 |
| `delta` | 0.25 |
| `skew_source` | `front` |
| `skew_mode` | `cheaper` |
| `qty_per_1btc_equity` | 2.0 |
| `max_concurrent` | 1 |
| `stop_book` | `full2_eq8` → prox **2% @ 16h** + equity stop **8%** |
| `wing_pct` | 0.07 (always on) |
| `min_width_usd` | 2500 |
| `weekend_cover` | `skip` |
| `rich_mode` | `none` (closed, run 741) |
| `take_profit_pct` / credit `stop_loss_pct` | 0 |

---

## Backtest stats (locked combo)

| Metric | Value |
|--------|-------|
| Window | 2025-08-18 → 2026-08-22 |
| Capital | $100,000 |
| Trades | 182 |
| Total P&L | +$50,156 (+50.2%) |
| Sharpe | 4.02 |
| Sortino | 5.06 |
| Calmar | 7.51 |
| Max DD (intraday) | 6.57% |
| Win rate | 90.1% |
| Profit factor | 3.19 |
| Exits | 180 expiry · 2 strike proximity |
| Fees | ~$9,825 |

Full metrics: `backtests/stats_starnberg.json`.  
Favourite note: *“1DTE perfection: with long wings, high sharpe, 90% win rate
but not higher.”*

**Expiry is not a clean win.** Most of the 18 losers also expired.

---

## Quick start (CryoTrader agent)

1. **`POSITION_RULES.md`** — one concurrent position; one entry per UTC day.
2. **`CRYOTRADER_IMPLEMENTATION.md`** — greenfield checklist.
3. **`STRATEGY_LOGIC.md`** — RichForce2 entry, cheaper front, Full2Eq8, wing.
4. **`DIFF_vs_short_str_turb_dyn.md`** — what to reuse vs rewrite.
5. **`params/starnberg.json`** — deploy param set.
6. **`reference/v18.py`** (+ `v17.py` / `v14.py` / `_common.py`) and
   **`reference/test_theta_engine_v18.py`** — port behavior & tests
   (`reference/SOURCE.md`).
7. **`TIMEZONE.md`** — brief: entry is 16:00 UTC (no NYC conversion).

---

## Package layout

```
handover/starnberg_cryotrader/
├── README.md
├── POSITION_RULES.md
├── TIMEZONE.md
├── LIVE_PARAM_SCHEDULE.json
├── STRATEGY_LOGIC.md
├── CRYOTRADER_IMPLEMENTATION.md
├── DIFF_vs_short_str_turb_dyn.md
├── build_handover.py
├── params/
│   └── starnberg.json
├── reference/
│   ├── SOURCE.md
│   ├── v18.py · v17.py · v14.py · _common.py
│   ├── test_theta_engine_v18.py
│   └── expiry_utils.py
└── backtests/
    ├── summary.json
    ├── stats_starnberg.json
    ├── trades_starnberg.csv
    ├── nav_daily_starnberg.csv
    └── fills_starnberg.csv
```

Regenerate: `python handover/starnberg_cryotrader/build_handover.py`

---

## Version note

| Artifact | Role |
|----------|------|
| `reference/v18.py` | Verbatim `workspace/strategies/theta_engine/v18.py` — authoritative |
| CryoTrader | **New** `strategies/starnberg.py` (suggested); no CT theta fork base |
| Marketing HTML | Investor report; do not use as a coding spec |
| `rich_mode` / alpha grids | **Ignore** — closed; live is `rich_mode=none` |

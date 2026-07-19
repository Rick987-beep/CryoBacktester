# TuDySho → CryoTrader handover package

Self-contained package for implementing **tudysho** in CryoTrader as **one strategy with slot-based parameters** — different param sets for distinct times in the week.

**CryoTrader fork base:** `CryoTrader/strategies/short_str_turb_dyn.py`  
**Backtester strategy (verbatim copy):** `reference/tudysho.py` ← `backtester/strategies/tudysho.py`  
**Position limits (live):** `POSITION_RULES.md`  
**Timezone (mandatory):** `TIMEZONE.md`

---

## Live schedule — three slots

| Slot | When (NYC calendar) | Combo | `entry_time` NYC | `dte` | Expiry |
|------|---------------------|-------|------------------|-------|--------|
| **A** `slot_a_mon_thu` | Mon–Thu | `5cd986cf48cd` | **16:00** | 1 | Next morning 08:00 UTC |
| **B** `slot_b_mon_early` | Mon early | `e2f4ac2b3e69` | **01:00** | 0 | Same-day Mon 08:00 UTC |
| **C** `slot_c_fri_sat` | Fri | `829e7226cc48` | **12:00** | 1 | Saturday 08:00 UTC |
| **D** `slot_d_saturday` | *(reserved)* | TBD | — | — | — |

All three active slots use **distinct** combo hashes and param sets.  
Authoritative routing: `LIVE_PARAM_SCHEDULE.json`.

### Position limits (read `POSITION_RULES.md`)

1. **One concurrent open position** on the account (never two overlapping legs).
2. **Each slot** may open at most **once per NYC calendar day** when its schedule applies.
3. **Monday = two trades:** slot B (01:00 NYC, expires 08:00 UTC) then slot A (16:00 NYC) — sequential, not overlapping.

---

## Param diff across slots

| Parameter | A Mon–Thu | B Mon early | C Fri→Sat |
|-----------|-----------|-------------|-----------|
| Combo | `5cd986cf48cd` | `e2f4ac2b3e69` | `829e7226cc48` |
| `entry_time` NYC | 16:00 | 01:00 | 12:00 |
| `dte` | 1 | 0 | 1 |
| `min_otm_pct` | 2.6 | 1.0 | 2.4 |
| `stop_loss_pct` | 6.0 | 4.0 | 4.0 |
| `turbulence_threshold` | 60 | 99 | 99 |
| `proximity_stop_hours` | 8 | 4 | 4 |
| `proximity_buffer_usd` | 1000 | 500 | 0 |
| `premium_sl_except_final_hours` | 8 | 4 | 4 |
| `nav_premium_pct` | 0.8 | 0.8 | 0.8 |
| `max_qty_per_1btc_equity` | 12 | 12 | 12 |

`turbulence_threshold: 99` (B, C) → gate effectively always open when data exists.  
Slot A uses turbulence gate at 60.

---

## Backtest stats (isolated per combo)

| Slot | Combo | Trades | Return | Sharpe | Max DD | Win% |
|------|-------|--------|--------|--------|--------|------|
| A | `5cd986cf48cd` | 143 | +91.5% | 9.02 | 10.7% | 99.3% |
| B | `e2f4ac2b3e69` | 30 | +13.6% | 5.22 | 3.1% | 100% |
| C | `829e7226cc48` | 51 | +28.6% | 4.76 | 4.4% | 98.0% |

Date range: 2025-06-27 → 2026-06-27, $100k capital. Full metrics in `backtests/stats_slot_*.json`.

**Bundles:** run 296 (`tudysho_20260706_084941.bundle`) for A; run 306 (`tudysho_20260706_140834.bundle`) for B; run 295 (`tudysho_20260706_081343.bundle`) for C.

---

## Quick start (CryoTrader agent)

1. **`TIMEZONE.md`** — NYC `entry_time` vs UTC system time (read first).
2. **`POSITION_RULES.md`** — concurrent position cap, per-slot daily limit, Monday two-trade rule.
3. **`CRYOTRADER_IMPLEMENTATION.md`** — fork checklist.
4. **`STRATEGY_LOGIC.md`** — sizing, exits, slot routing.
5. **`DIFF_vs_short_str_turb_dyn.md`** — vs CryoTrader base.
6. **`params/slot_*.json`** — deploy param sets.
7. **`reference/tudysho.py`** + **`reference/test_tudysho.py`** — port behavior & tests (`reference/SOURCE.md` lists canonical paths).

---

## Package layout

```
handover/tudysho_cryotrader/
├── README.md
├── POSITION_RULES.md              ← concurrent + per-slot daily limits
├── TIMEZONE.md                    ← NYC entry_time vs UTC (critical)
├── LIVE_PARAM_SCHEDULE.json
├── STRATEGY_LOGIC.md
├── CRYOTRADER_IMPLEMENTATION.md
├── DIFF_vs_short_str_turb_dyn.md
├── build_handover.py
├── params/
│   ├── slot_a_mon_thu.json
│   ├── slot_b_mon_early.json
│   └── slot_c_fri_sat.json
├── reference/
│   ├── SOURCE.md                  ← canonical paths for copied files
│   ├── tudysho.py                 ← copy of backtester/strategies/tudysho.py
│   ├── test_tudysho.py
│   ├── backtester/core/market_hours.py
│   └── expiry_utils.py
└── backtests/
    ├── summary.json
    ├── stats_slot_*.json
    ├── trades_slot_*.csv
    ├── nav_daily_slot_*.csv
    └── fills_slot_*.csv
```

Regenerate: `python handover/tudysho_cryotrader/build_handover.py`

---

## Version note

| Artifact | Role |
|----------|------|
| `reference/tudysho.py` | Verbatim copy of `backtester/strategies/tudysho.py` — authoritative logic |
| `CryoTrader/strategies/short_str_turb_dyn.py` | Live fork base (execution, turbulence) |
| `trade_*` grid flags | **Ignore** — use slot routing instead |

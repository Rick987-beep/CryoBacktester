# CryoTrader implementation guide

For an AI agent or developer porting **tudysho** into CryoTrader.  
**Read `README.md`, `POSITION_RULES.md`, and `STRATEGY_LOGIC.md` first.**

---

## Goal

Implement **one live strategy** on one account that routes **three slots** (A, B, C):

| Slot | When (NYC) | Combo | `entry_time` | `dte` |
|------|------------|-------|--------------|-------|
| A | Mon–Thu | `5cd986cf48cd` | 16:00 | 1 |
| B | Mon | `e2f4ac2b3e69` | 01:00 | 0 |
| C | Fri | `829e7226cc48` | 12:00 | 1 |

Saturday slot D reserved. No weekend entries.

Matches `reference/tudysho.py` behavior for entry, sizing, and exits.

Start from **`CryoTrader/strategies/short_str_turb_dyn.py`** — do not use `CryoBacktester/backtester/strategies/short_str_turb_dyn.py` (different entry timezone, no execution layer).

---

## Implementation checklist

### Phase 1 — Shared infrastructure

- [ ] Fork `CryoTrader/strategies/short_str_turb_dyn.py` as the starting file.
- [ ] Confirm turbulence composite matches backtester (reuse existing `_turbulence_ok` + T−1h bucket logic).
- [ ] **Entry time:** NYC `entry_time` per slot; port `to_nyc` / `to_utc` (`reference/market_hours.py`).
- [ ] Expose **`nav_usd`** and **`equity_usd`** on the account object used in sizing.

### Phase 2 — Param loading

- [ ] Load `params/slot_a_mon_thu.json`, `params/slot_b_mon_early.json`, `params/slot_c_fri_sat.json`.
- [ ] Implement `resolve_slot(nyc_weekday, nyc_time) -> dict | None` per `LIVE_PARAM_SCHEDULE.json`.
- [ ] Strip/ignore `trade_*` keys from any imported config.
- [ ] On position open: snapshot `slot_id`, `combo_hash`, and exit params onto position metadata.

### Phase 3 — Entry

- [ ] **`POSITION_RULES.md`:** max **1 concurrent** open position; **per-slot** `_last_trade_date[slot_id]` (NYC date).
- [ ] Monday: both B and A may trade same NYC day (B flat before A opens).
- [ ] Replace UTC `ENTRY_HOUR` with per-slot NYC `entry_time`.
- [ ] Replace `_compute_quantity` with tudysho NAV / BTC-equity cap (`reference/tudysho.py`).

### Phase 4 — Exits

- [ ] Keep `_strike_proximity_stop()` from CryoTrader base.
- [ ] Adapt premium SL for tudysho `stop_loss_pct`; honour `premium_sl_except_final_hours`.
- [ ] Remove TP and max-hold for live param sets.

### Phase 5 — Validation

- [ ] Port or adapt `reference/test_tudysho.py`
- [ ] Compare fills to `backtests/trades_slot_*.csv` per slot
- [ ] Log `slot_id`, sizing metadata on every open

---

## CryoTrader file hints

| Path | Purpose |
|------|---------|
| `CryoTrader/strategies/short_str_turb_dyn.py` | **Fork this** |
| `CryoTrader/strategies/strategy.py` | Shared helpers |
| `CryoTrader/option_selection.py` | `strangle`, `resolve_legs` |
| `CryoTrader/indicators/turbulence.py` | Composite definition |
| `CryoTrader/execution/profiles.py` | `strangle_turb_best_effort` |

---

## Account state contract

```python
state.nav_usd     # float — for nav_premium_pct sizing
state.equity_usd  # float — for max_qty_per_1btc_equity cap
state.spot        # float — BTC index
state.dt          # tz-aware datetime (UTC)
```

---

## `stop_loss_pct` units (common pitfall)

Backtester tudysho: `(mark_cost - entry_premium) / entry_premium >= stop_loss_pct`.  
Live values: A `6.0`, B/C `4.0`. Reconcile with CryoTrader `_combined_sl()` — do not assume env vars drop in unchanged.

---

## Backtest caveats for QA

1. Each slot backtest is **isolated** — combined equity is a post-hoc merge (`analysis/tudysho_slots_marketing/`).
2. `trade_*` grid flags in bundles are for discovery only — live uses slot routing.
3. `end_of_data` closes at simulation tail can overlap in timestamps — not live behaviour.

---

## Related CryoBacktester paths

| Path | Purpose |
|------|---------|
| `backtester/strategies/tudysho.py` | Canonical strategy (copied to `reference/`) |
| `backtester/strategy_base.py` | Exit condition factories |
| `backtester/bt_option_selection.py` | Delta selection |
| `market_hours.py` | NYC/UTC |
| `docs/strategy_howto.md` | Leg dict required fields |

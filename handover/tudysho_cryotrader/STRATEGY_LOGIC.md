# TuDySho — full strategy logic

Authoritative reference implementation: `reference/tudysho.py`.  
This document describes **behavior** for CryoTrader porting.

---

## Overview

Short 1-DTE OTM premium (default: delta-selected strangle) on Deribit BTC options.

- **Entry:** once per slot per NYC calendar day (live), after NYC `entry_time`, when turbulence is calm.
- **Concurrency (live):** at most **one open position** on the account; see `POSITION_RULES.md`.
- **Sizing:** target premium as % of NAV; hard cap from BTC-equity.
- **Exit:** expiry settlement, or first match among independent stop rules.
- **No take-profit, no max-hold** in the live param sets.

---

## Entry

### Timing

1. `entry_time` is **NYC wall-clock** (e.g. `"12:00"`).
2. Each tick: convert today's NYC entry instant to UTC (`to_nyc` / `to_utc`, DST-aware).
3. No entry before that UTC minute-of-day.
4. After entry time: watch turbulence every tick until open or end of day.
5. **Backtester (single instance):** at most one new entry per UTC calendar day (`_last_trade_date`).
6. **Live (multi-slot):** at most one entry **per slot** per NYC calendar day; account holds **one concurrent position** max. Monday may produce **two** trades (B then A). See `POSITION_RULES.md`.

### Weekday gating (live)

Resolve param set from **entry-day weekday** (Mon=0 … Sun=6) and **active slot**:

| Slot | Weekdays (NYC) | Combo |
|------|----------------|-------|
| A `slot_a_mon_thu` | Mon–Thu | `5cd986cf48cd` |
| B `slot_b_mon_early` | Mon | `e2f4ac2b3e69` |
| C `slot_c_fri_sat` | Fri | `829e7226cc48` |
| Sat/Sun | — | no entry |

Do **not** use grid `trade_*` flags at runtime.

### Turbulence gate

- Indicator: hourly `composite` from `turbulence(BTCUSDT, 15m)`.
- Entry allowed when `composite < turbulence_threshold`.
- **Fail-open:** missing series, missing hour, or NaN → treat as calm (allow entry).
- Friday set uses `turbulence_threshold: 99` → gate almost never blocks.

### Leg selection

1. `select_expiry(state, dte)` — calendar DTE.
2. `select_by_delta` on call (+delta) and put (−delta).
3. If `min_otm_pct > 0`: push each leg further OTM until strike is at least `min_otm_pct`% from spot (`_apply_min_otm`).
4. `leg_min_price`: if > 0, require bid ≥ floor (BTC); if 0, require bid > 0.
5. `leg_type`: `strangle` (both legs), `call`, or `put` — live sets use strangle only.

### Concurrency

**Backtester (`reference/tudysho.py`, one instance):**

- `max_concurrent = dte + 1` (2 positions when `dte=1`) — rolling 1-DTE window within one combo.
- `_last_trade_date` — one **new** entry per UTC day on that instance.

**Live CryoTrader (slots A+B+C on one account):**

- **Max 1 concurrent open position** account-wide.
- **Per slot:** one entry per NYC calendar day when that slot’s window applies.
- Do **not** use a single global `_last_trade_date` for all slots.

See `POSITION_RULES.md`.

---

## Sizing (`_compute_quantity`)

Two inputs from account state (injected by engine / must exist in CryoTrader):

| Field | Use |
|-------|-----|
| `state.nav_usd` | NAV target numerator (includes open PnL) |
| `state.equity_usd` | Realized equity for BTC-equity cap |
| `state.spot` | BTC spot for cap math |

### NAV premium target

When `nav_premium_pct > 0`:

```
target_premium_usd = nav_usd × (nav_premium_pct / 100)
qty_from_premium   = target_premium_usd / premium_usd_per_contract
```

`premium_usd_per_contract` = sum of bid USD for strangle legs (per 1 contract) at entry.

### BTC-equity cap

When `max_qty_per_1btc_equity > 0`:

```
equity_btc     = equity_usd / spot
max_contracts  = equity_btc × max_qty_per_1btc_equity
quantity       = round(min(qty_from_premium, max_contracts), 1)
quantity       = max(quantity, 0.1)   # Deribit min
```

Skip entry if `max_contracts < 0.1`.

When `max_qty_per_1btc_equity == 0`: no cap (premium target only).

When `nav_premium_pct == 0`: fixed `quantity = 1.0`.

Skip entry if `nav_usd ≤ 0`, `spot ≤ 0`, or `premium_usd_per_contract ≤ 0`.

### Metadata on open

Store sizing audit fields on open trade metadata: `nav_usd_at_entry`, `equity_usd_at_entry`, `equity_btc_at_entry`, `target_premium_usd`, `qty_from_premium`, `max_contracts_applied`, `premium_capped`, etc.

Also store `equity_at_entry_usd` on position metadata for optional equity drawdown SL (disabled in live sets).

---

## Exit layer (`_build_exit_conds` / `_check_exit`)

Evaluated **in order**; first firing reason wins after expiry check.

### 1. Expiry

`check_expiry` — settlement at 08:00 UTC on `expiry_dt`. Intrinsic value at spot; zero close fee.

### 2. Strike proximity (optional)

`strike_proximity_stop(proximity_stop_hours, proximity_buffer_usd)`

- Active only in final `proximity_stop_hours` before `expiry_dt`.
- Short premium only.
- Strangle: spot beyond call strike + buffer OR below put strike − buffer.
- **Quote-free** — only needs spot; fires even if option rows missing.

Live sets: 4h window, $0 buffer.

### 3. Equity drawdown (optional, disabled live)

`equity_drawdown_stop(pct)` — loss vs `equity_at_entry_usd`.  
Optional time gating via `exit_expiry_window` + `equity_sl_only_final_hours` / `equity_sl_except_final_hours`.  
All zero in live sets.

### 4. Premium stop-loss (optional)

`stop_loss_pct(stop_loss_pct, price_mode="mark")` wrapped with `exit_expiry_window(..., except_final_hours=premium_sl_except_final_hours)`.

- **Mark** pricing (not bid/ask) for stability.
- `stop_loss_pct` is a **multiplier of entry premium**: `6.0` = exit when mark-to-close cost exceeds entry premium by 600% (see `strategy_base.stop_loss_pct` docstring).
- Suppressed in final `premium_sl_except_final_hours` before expiry (live: 4h).

Live sets: Mon–Thu `stop_loss_pct=6.0`; Fri `4.0`.

### Quote guard

After a reason is chosen, if reason ∉ `{strike_proximity_stop}` and `position_quotes_available` is false → **suppress** exit (retry next tick). Prevents closing on stale/missing chain data.

### Early close pricing

Non-expiry closes: buy back at ask; if ask missing use `0.0001` BTC min tick. Fees via `deribit_fee_per_leg`.

---

## Params NOT used in live sets

| Param | Live value | Note |
|-------|------------|------|
| `take_profit_pct` | — | Not in tudysho; always 0 |
| `max_hold_hours` | — | Not in tudysho |
| `equity_drawdown_stop_pct` | 0 | Disabled |
| `leg_type` | strangle | Could support call/put in code |

---

## Pseudocode (single tick)

```
for each open position:
    reason = check_expiry OR proximity_stop OR equity_dd_stop OR premium_sl
    if reason needs quotes and quotes missing: reason = None
    if reason: close(position, reason)

if len(open_positions) == 0:
    slot = resolve_active_slot(nyc_weekday, nyc_time)
    if slot and per_slot_last_trade[slot] != nyc_date:
        if weekday_allowed and after_entry_time and turbulence_ok:
            try_open(slot)
```

---

## Live implementation: slot param resolution

```python
def resolve_slot(nyc_weekday: int, nyc_time: time) -> str | None:
    if nyc_weekday == 0 and at_or_after(nyc_time, "01:00"):
        return "slot_b_mon_early"   # Mon early
    if nyc_weekday in (0, 1, 2, 3) and at_or_after(nyc_time, "16:00"):
        return "slot_a_mon_thu"     # Mon–Thu daytime
    if nyc_weekday == 4 and at_or_after(nyc_time, "12:00"):
        return "slot_c_fri_sat"     # Friday
    return None
```

Load params from `params/{slot_id}.json`. Re-resolve on each entry attempt. Open positions keep exit rules from **entry params** (store on position metadata at open).

---

## Tests to port or re-run

`reference/test_tudysho.py` covers:

- Weekday gating helper
- Exit condition wiring (proximity + premium + equity)
- Premium SL suppressed inside `premium_sl_except_final_hours`
- Proximity fires without quotes; premium SL suppressed without quotes
- NAV sizing and BTC-equity cap edge cases

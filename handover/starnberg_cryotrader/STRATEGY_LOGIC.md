# Starnberg — full strategy logic

Authoritative reference: `reference/v18.py` (with `v17.py` / `v14.py` /
`_common.py`). This document describes **behavior** for CryoTrader porting.

Marketing name: **Starnberg**. Catalog ID: `theta_engine_v18`.

---

## Overview

Short-dated listed Bitcoin option income on Deribit:

- **Entry:** Mon–Fri at/after **16:00 UTC**, RichForce2 gate (VRP ≥ 4 else
  force after 2 idle sessions), cheaper side of front 25Δ RR.
- **Structure:** sell 1 DTE ~25Δ option; immediately buy equal-qty further-OTM
  long on the same expiry (`wing_pct=0.07`, floor `min_width_usd=2500`).
- **Sizing:** `qty = qty_per_1btc_equity × (equity_usd / spot)` → **2** contracts
  per 1 BTC of equity.
- **Exit:** next daily expiry, or Full2Eq8 stops (strike proximity 2%@16h,
  open-PnL equity stop 8%). No take-profit; credit SL off.
- **Always hedged** on the marketed book.

---

## Entry

### Timing

1. `entry_time_utc = "16:00"` (system UTC; see `TIMEZONE.md`).
2. Each tick: allow only when minute-of-day ≥ 16:00.
3. Weekdays: Mon–Fri only (`entry_days = 0..4`).
4. At most one new entry per UTC calendar day.
5. Skip if an open position already exists (`max_concurrent=1`).

### RichForce2 gate (`rich_force_2d_1600` / book `rf2_1600`)

Policy ID: `rich_force_2d_1600` (stable — never rename).

| Condition | Behavior |
|-----------|----------|
| Daily VRP ≥ **4** at the entry window | Open (`entry_reason=rich`) |
| Else, and ≥ **2** Mon–Fri sessions without an open | Force open (`entry_reason=forced`) |
| Else | Skip the day |

VRP comes from the backtester `vol_context` daily indicator
(`lookup_vol_context`). Live must feed an equivalent daily VRP series
(same definition as CryoBacktester / CryoTrader indicators if already shared).

Idle / skipped days are expected. Do not force every session.

### Skew / side selection

- `skew_source=front`: 25Δ call IV − 25Δ put IV on the **trade expiry** (front).
- `skew_mode=cheaper`: sell the **cheaper** wing (lower IV side).
- `delta=0.25`: select short by absolute delta nearest 0.25.
- Single short (call **or** put), not a two-sided strangle.

Exact RR helper: `rr_25d_on_expiry` / side pick in `reference/v14.py` +
`_common.py`.

### Expiry selection

- `select_expiry(state, dte=1)` — next calendar DTE.
- `weekend_cover=skip` — reject expiries that settle Sun/Mon 08:00 UTC.

### Concurrency

- One open ticket max.
- One new entry per UTC day.

---

## Structure at open

1. Open short leg at executable bid.
2. Attach long wing same expiry, equal qty, further OTM:
   - Target width = `wing_pct × short_strike` (0.07 × K).
   - Listed width must be ≥ `min_width_usd` (2500).
   - Pick nearest ask>0 outer to target (`pick_wing_quote_by_pct`).
3. If no outer available → **rollback** short (no naked fill).
4. Fees: Deribit fee per leg on short and wing.

In the locked sample every ticket carried the wing (182/182 opens = 364 open
fills).

---

## Sizing

```
qty = qty_per_1btc_equity * (equity_usd / spot)
```

Locked: `qty_per_1btc_equity = 2.0`. Floor at `MIN_QTY` (see `_common.py`).

Requires `state.equity_usd` and `state.spot` on the live account object.

---

## Exits (Full2Eq8)

Packed `stop_book=full2_eq8` expands to:

| Control | Spec | Meaning |
|---------|------|---------|
| Strike proximity | `2@16` | 2% of strike buffer; active in last **16 hours** before expiry |
| Open-PnL equity stop | **8%** | If mark open loss across open positions ≥ 8% of equity → flatten |
| Credit stop loss | 0 | Off |
| Take profit | 0 | Off |

Proximity uses `strike_proximity_stop_pct` from strategy_base (short-leg
strike). Equity stop is portfolio-level on open mark PnL
(`_open_pnl_equity_stop_hit` in `v18.py`).

Default close: **expiry** (~08:00 UTC next day). In sample: 180 expiry,
2 proximity. Equity stop did not fire in this sample.

**Expiry includes losers.** Do not treat expiry count as win count.

---

## What not to port as “edge”

- Exact VRP formula internals beyond matching the indicator series.
- Investor Greeks sidecar (optional meter).
- `rich_mode` sizing (closed — leave `none`).
- Discovery grids (`V18_STOP_DISCOVERY_GRID`, etc.).

---

## Live param snapshot

See `params/starnberg.json`. Key fields for the slot router:

```json
{
  "combo_hash": "a1d621a81904",
  "book": "rf2_1600",
  "entry_time_utc": "16:00",
  "dte": 1,
  "delta": 0.25,
  "skew_source": "front",
  "skew_mode": "cheaper",
  "qty_per_1btc_equity": 2.0,
  "stop_book": "full2_eq8",
  "wing_pct": 0.07,
  "min_width_usd": 2500.0,
  "weekend_cover": "skip",
  "max_concurrent": 1
}
```

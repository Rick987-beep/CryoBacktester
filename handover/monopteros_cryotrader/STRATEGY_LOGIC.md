# Monopteros — full strategy logic

Authoritative reference: `reference/monopteros.py`. This document describes
**behavior** for CryoTrader porting.

Marketing name: **Monopteros**. Catalog ID: `tudysho_monopteros`.

---

## Overview

Short-dated listed Bitcoin option income on Deribit:

- **Entry:** three NYC schedules (early Monday, Mon–Thu afternoon, Friday late
  morning). Skip when turbulence looks too high for that schedule.
- **Structure:** short listed **strangle** (call + put) into the next daily
  expiry selected by schedule `dte`. **Naked** — no wing / cover.
- **Sizing:** target premium = `nav_premium_pct` % of NAV (0.5%); cap contracts
  at `max_qty_per_1btc_equity` × equity/spot (6).
- **Exit:** expiry by default, else premium stop, strike-proximity stop, or
  equity drawdown stop (5% of equity, mark). No take-profit.

---

## Schedule routing

| ID | NYC days | `entry_time` | `dte` | δ | turb | premium SL | proximity |
|----|----------|--------------|------|---|------|------------|-----------|
| `mon_early` | Mon | 00:05 | 0 | 0.05 | 99 | 6.0 | 4h / $800 |
| `mon_thu` | Mon–Thu | 14:00 | 1 | 0.05 | 70 | 0 (off) | 8h / $500 |
| `fri` | Fri | 12:30 | 1 | 0.06 | 99 | 3.5 | 8h / $1000 |

Monday: `mon_early` from 00:05 until `mon_thu` entry; `mon_thu` from 14:00
onward. See `resolve_schedule` in `reference/monopteros.py`.

Each schedule has a watch window after its NYC entry time
(`watch_until_utc_midnight` or `watch_until_utc_hour`).

Research sources for these cells (not live combo ids):

| Schedule | Discovery combo | Run |
|----------|-----------------|-----|
| `mon_thu` | `16dcb4f42c9a` | 768 |
| `mon_early` | `b012cb51c397` | 764 |
| `fri` | `fd39a836394d` | 766 |

Live master combo for the product: **`7e21b64f1d94`** (run 770).

---

## Entry

1. Convert tick to NYC; resolve schedule (or skip).
2. Enforce `POSITION_RULES.md` (concurrency + per-schedule daily cap).
3. Turbulence gate: skip if score ≥ schedule `turbulence_threshold`
   (99 ≈ always open when data exists).
4. Select expiry by `dte`; pick call+put near schedule `delta` with
   `min_otm_pct` floor.
5. Size to target premium; open short strangle at executable bids.

Idle / skipped sessions are expected.

---

## Exits (every ticket)

| Stop | Behavior |
|------|----------|
| Premium `stop_loss_pct` | Mark SL as multiple of credit; 0 = off (`mon_thu`) |
| Strike proximity | Within `proximity_buffer_usd` of a short strike in the last `proximity_stop_hours` |
| Equity drawdown | Aggregate open mark loss ≥ **5%** of equity |
| Expiry | Default close at Deribit daily settlement (~08:00 UTC) |

In the locked sample (run 770): 376 expiry / 4 proximity / 2 premium SL;
equity DD did not fire.

---

## Sizing (global, not per schedule)

| Knob | Lock |
|------|------|
| `nav_premium_pct` | **0.5** |
| `max_qty_per_1btc_equity` | **6** |
| `equity_drawdown_stop_pct` | **5** |
| `leg_min_price` | 0 (off) |

---

## Concurrency

- One open ticket max.
- One new entry per schedule per NYC day.
- Monday may produce two sequential tickets (`mon_early` then `mon_thu`).

# Timezones: `entry_time` (NYC) vs system UTC

**Read this before porting entry logic to CryoTrader.**

TuDySho params use **`entry_time` as NYC wall-clock** (`America/New_York`, DST-aware).  
CryoTrader's loop, Deribit API, logs, and expiry settlement all run in **UTC**.

The existing CryoTrader base strategy (`short_str_turb_dyn.py`) uses **`ENTRY_HOUR` in UTC** — do **not** map `entry_time` directly to that field.

---

## The rule

> Config says **when NYC traders would look at the clock**; code compares against **UTC now**.

From `reference/tudysho.py` (`_maybe_open`):

```python
entry_utc = to_utc(
    to_nyc(dt).replace(
        hour=self._entry_hour,
        minute=self._entry_minute,
        second=0,
        microsecond=0,
    )
)
if dt.hour * 60 + dt.minute < entry_utc.hour * 60 + entry_utc.minute:
    return None  # too early today
```

Steps each tick:

1. Take current UTC time `dt`.
2. Convert to NYC: `to_nyc(dt)` → today's NYC **calendar date**.
3. Build NYC datetime today at `entry_time` (e.g. 12:00).
4. Convert that instant back to UTC: `entry_utc`.
5. Compare **UTC** minute-of-day: open only when `dt` ≥ `entry_utc`.

The NYC **date** matters: Friday 12:00 NYC is a different UTC instant than Thursday 12:00 NYC near DST boundaries.

Reference implementation: `reference/market_hours.py` (`to_nyc`, `to_utc`).

---

## DST conversion table (live param sets)

| `entry_time` NYC | Winter (EST, UTC−5) | Summer (EDT, UTC−4) |
|------------------|---------------------|---------------------|
| **01:00** (slot B) | 06:00 UTC same day | 05:00 UTC same day |
| **12:00** (slot C) | 17:00 UTC same day | 16:00 UTC same day |
| **16:00** (slot A) | 21:00 UTC same day | 20:00 UTC same day |

Examples on a **Monday**:

| NYC local | EST → UTC | EDT → UTC |
|-----------|-----------|-----------|
| Mon 01:00 | Mon 06:00 UTC | Mon 05:00 UTC |
| Mon 16:00 | Mon 21:00 UTC | Mon 20:00 UTC |

Examples on **Friday** (slot C):

| NYC local | EST → UTC | EDT → UTC |
|-----------|-----------|-----------|
| Fri 12:00 | Fri 17:00 UTC | Fri 16:00 UTC |

→ Saturday 08:00 UTC expiry (because `dte=1` on Friday selects next calendar day's expiry).

---

## What is NOT `entry_time`

| Field / pattern | Timezone | Notes |
|-----------------|----------|-------|
| `entry_time` param | **NYC** | TuDySho handover |
| CryoTrader `ENTRY_HOUR` | **UTC** | Legacy `short_str_turb_dyn` — replace for tudysho |
| Deribit `expiry_dt` | **UTC** | Always 08:00 UTC on expiry date |
| Turbulence hourly bucket | **UTC** | Backtester indexes by UTC hour; CryoTrader uses T−1h fix |
| `weekdays` in schedule | **NYC calendar** | Mon=0 … Sun=6 on **America/New_York** date of tick |

**Weekday routing:** use the NYC calendar date of the tick, not UTC date, when deciding slot A/B/C. At Mon 02:00 UTC it may still be **Sunday evening** in NYC — slot B would not apply (it's Monday-only in NYC).

```python
weekday = to_nyc(dt).weekday()  # 0=Monday … 6=Sunday
```

---

## Three slots and expiry (`dte`)

| Slot | NYC days | `entry_time` | `dte` | Expiry |
|------|----------|--------------|-------|--------|
| **A** Mon–Thu | Mon–Thu | **16:00** | 1 | Next calendar day 08:00 UTC |
| **B** Mon early | Mon | 01:00 | 0 | **Same** calendar day 08:00 UTC |
| **C** Fri→Sat | Fri | 12:00 | 1 | Saturday 08:00 UTC |

`dte` = calendar days from **NYC entry date** to expiry date (`select_expiry` in `reference/expiry_utils.py`).

### Slot B vs "Sunday night"

Combo `e2f4ac2b3e69` was backtested with **`trade_monday=1`**, **`entry_time=01:00` NYC**, **`dte=0`**.

That is **Monday 1:00 AM NYC** → Monday 08:00 UTC expiry.  
It does **not** include **Sunday evening** NYC entries (Sunday NYC date + `dte=0` would target Sunday expiry, not Monday morning).

To add Sunday-night opens later, you need a separate combo / slot (reserved: `slot_d_saturday` or new Sunday slot).

### Monday: two slots, two trades (sequential)

Monday runs **both** slot B and slot A on the **same NYC calendar day**:

| Slot | Entry (NYC) | Expiry | Overlap? |
|------|-------------|--------|----------|
| **B** | Mon 01:00 | Mon 08:00 UTC | — |
| **A** | Mon 16:00 | Tue 08:00 UTC | **No** — B is flat before A opens |

This is **not** “one trade per day globally.” Rules:

1. **One concurrent open position** on the account.
2. **One entry per slot per NYC day** (B once, A once on Monday).
3. Monday therefore yields **two round-trips** when both slots fire.

Full detail: `POSITION_RULES.md`.

---

## CryoTrader porting checklist (timezone)

- [ ] Port or import `to_nyc` / `to_utc` (CryoTrader has `market_hours.py`).
- [ ] Replace `ENTRY_HOUR` UTC gate with NYC `entry_time` from active slot.
- [ ] Resolve slot using `to_nyc(dt).weekday()` and schedule in `LIVE_PARAM_SCHEDULE.json`.
- [ ] Store `entry_time_nyc`, `slot_id`, and `combo_hash` on trade metadata at open.
- [ ] Log both UTC and NYC in open notifications for ops debugging.
- [ ] Unit-test entry gate across DST boundary (March/November).

---

## Turbulence lookup (UTC, separate from entry_time)

Entry **time** uses NYC; turbulence **data** is UTC-hour indexed.

CryoTrader `short_str_turb_dyn._turbulence_ok` uses the **previous** UTC hour bucket (T−1h) — keep that when forking. Do not switch turbulence to NYC hours.

---

## Quick reference: slot param files

| Slot | File | Combo | `entry_time` | `dte` |
|------|------|-------|--------------|-------|
| A | `params/slot_a_mon_thu.json` | `5cd986cf48cd` | 16:00 | 1 |
| B | `params/slot_b_mon_early.json` | `e2f4ac2b3e69` | 1:00 | 0 |
| C | `params/slot_c_fri_sat.json` | `829e7226cc48` | 12:00 | 1 |

Each slot has a **distinct** combo hash and param set.

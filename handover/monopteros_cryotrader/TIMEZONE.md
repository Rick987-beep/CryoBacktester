# Timezones: `entry_time` (NYC) vs system UTC

**Read this before porting entry logic to CryoTrader.**

Monopteros params use **`entry_time` as NYC wall-clock** (`America/New_York`,
DST-aware). CryoTrader’s loop, Deribit API, logs, and expiry settlement all run
in **UTC**.

Do **not** map `entry_time` directly onto a UTC `ENTRY_HOUR` field from
`short_str_turb_dyn`.

---

## The rule

> Config says **when NYC traders would look at the clock**; code compares against
> **UTC now**.

From `reference/monopteros.py` (watch / entry path):

1. Take current UTC time `dt`.
2. Convert to NYC: `to_nyc(dt)` → today’s NYC **calendar date** and local time.
3. Resolve schedule with `resolve_schedule(nyc_weekday, nyc_time, schedules)`.
4. Build today’s NYC entry instant at that schedule’s `entry_time`, convert to UTC.
5. Open only inside the schedule’s watch window (`in_watch_window`).

Reference: `reference/market_hours.py` (`to_nyc`, `to_utc`).

---

## DST conversion table (this lock)

| `entry_time` NYC | Winter (EST, UTC−5) | Summer (EDT, UTC−4) |
|------------------|---------------------|---------------------|
| **00:05** (`mon_early`) | 05:05 UTC same day | 04:05 UTC same day |
| **14:00** (`mon_thu`) | 19:00 UTC same day | 18:00 UTC same day |
| **12:30** (`fri`) | 17:30 UTC same day | 16:30 UTC same day |

---

## What is NOT `entry_time`

| Field / pattern | Timezone | Notes |
|-----------------|----------|-------|
| `entry_time` param | **NYC** | Monopteros / TuDySho family |
| CryoTrader `ENTRY_HOUR` | **UTC** | Legacy `short_str_turb_dyn` — replace |
| Deribit `expiry_dt` | **UTC** | Always ~08:00 UTC on expiry date |
| Turbulence hourly bucket | **UTC** | Do not switch to NYC hours |
| Schedule weekdays | **NYC calendar** | Mon=0 … Sun=6 on `America/New_York` date |

**Weekday routing:** use the NYC calendar date of the tick, not UTC date.

```python
weekday = to_nyc(dt).weekday()  # 0=Monday … 6=Sunday
```

At Mon 02:00 UTC it may still be **Sunday evening** in NYC — `mon_early` must
not fire.

---

## Schedules and expiry (`dte`)

| Schedule | NYC days | `entry_time` | `dte` | Expiry |
|----------|----------|--------------|------|--------|
| `mon_early` | Mon | **00:05** | 0 | **Same** calendar day 08:00 UTC |
| `mon_thu` | Mon–Thu | **14:00** | 1 | Next calendar day 08:00 UTC |
| `fri` | Fri | **12:30** | 1 | Saturday 08:00 UTC |

`dte` = calendar days from **NYC entry date** to expiry date
(`select_expiry` in `reference/expiry_utils.py`).

### Monday: two schedules, two trades (sequential)

| Schedule | Entry (NYC) | Expiry | Overlap? |
|----------|-------------|--------|----------|
| `mon_early` | Mon 00:05 | Mon 08:00 UTC | — |
| `mon_thu` | Mon 14:00 | Tue 08:00 UTC | **No** — early is flat before afternoon |

Rules: one concurrent position; one entry per schedule per NYC day. Full detail:
`POSITION_RULES.md`.

---

## CryoTrader porting checklist (timezone)

- [ ] Port or import `to_nyc` / `to_utc` (`reference/market_hours.py`).
- [ ] Replace UTC `ENTRY_HOUR` with NYC `entry_time` from the active schedule.
- [ ] Resolve schedule with `to_nyc(dt).weekday()` + `resolve_schedule`.
- [ ] Store `entry_time_nyc`, `schedule_id`, and `combo_hash` on open metadata.
- [ ] Unit-test entry gate across a DST boundary (March/November).

---

## Turbulence lookup (UTC, separate from entry_time)

Entry **time** uses NYC; turbulence **data** is UTC-hour indexed. Keep the
CryoTrader / backtester UTC bucket convention; do not reindex turbulence to NYC.

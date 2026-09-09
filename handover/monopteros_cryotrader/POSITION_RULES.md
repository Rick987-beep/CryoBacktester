# Position and entry limits (live)

Authoritative rules for **one CryoTrader account** running Monopteros
(single strategy `monopteros` with three NYC schedules).

---

## Three rules (read in order)

### 1. One concurrent open position (account-wide)

At any instant the account holds **at most one** open short-premium position —
regardless of which schedule opened it.

Before any new entry: `len(open_positions) == 0`.

### 2. One entry per schedule per NYC calendar day

Each schedule may open **at most once** on a given **NYC calendar date** when
that schedule’s weekday and `entry_time` apply:

| Schedule | NYC weekdays | `entry_time` | Max entries on an active day |
|----------|--------------|--------------|------------------------------|
| `mon_early` | Mon | 00:05 | 1 |
| `mon_thu` | Mon–Thu | 14:00 | 1 |
| `fri` | Fri | 12:30 | 1 |

Track **per schedule**, e.g. `_last_trade_nyc_date[schedule_id]`, not one global
“already traded today” flag.

### 3. Mondays can have two trades

Monday is the only NYC calendar day where **two schedules** both fire:

| Order | Schedule | Entry (NYC) | Typical expiry | Overlap? |
|-------|----------|-------------|----------------|----------|
| 1 | `mon_early` | Mon **00:05** | Mon **08:00 UTC** (`dte=0`) | — |
| 2 | `mon_thu` | Mon **14:00** | Tue 08:00 UTC (`dte=1`) | **No** — early ticket is flat before afternoon |

Monday therefore contributes **two round-trips** when both schedules open.

---

## What this is NOT

| Wrong mental model | Correct model |
|--------------------|---------------|
| “One trade per calendar day globally” | Up to **one per schedule per day**; Monday = 2 schedules → 2 trades |
| “Monday: only one of early or afternoon may trade” | **Both** may trade the same Monday (sequential) |
| Three separate live strategies | **One** strategy with schedule routing |

---

## CryoTrader implementation sketch

```python
def may_open(schedule_id: str, nyc_date: date, open_positions: list) -> bool:
    if open_positions:
        return False  # rule 1
    if last_trade_date.get(schedule_id) == nyc_date:
        return False  # rule 2
    return True
```

On successful open: `last_trade_date[schedule_id] = nyc_date`.

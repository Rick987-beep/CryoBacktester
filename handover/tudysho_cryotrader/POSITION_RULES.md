# Position and entry limits (live)

Authoritative rules for **one CryoTrader account** running slots A + B + C.

---

## Three rules (read in order)

### 1. One concurrent open position (account-wide)

At any instant the account holds **at most one** open short-premium position — regardless of which slot opened it.

Before any new entry: `len(open_positions) == 0`.

This is the hard constraint. Combined marketing validates it: **0 position overlaps** across the union of all slot backtests (excluding `end_of_data` tail artifacts).

### 2. One entry per slot per NYC calendar day

Each slot may open **at most once** on a given **NYC calendar date** when that slot’s weekday schedule and `entry_time` apply:

| Slot | NYC weekdays | `entry_time` | Max entries on an active day |
|------|--------------|--------------|------------------------------|
| A | Mon–Thu | 16:00 | 1 |
| B | Mon | 01:00 | 1 |
| C | Fri | 12:00 | 1 |

Track **per slot**, e.g. `_last_trade_date[slot_id]`, not one global “already traded today” flag.

### 3. Mondays can have two trades

Monday is the only NYC calendar day where **two slots** both fire:

| Order | Slot | Entry (NYC) | Expiry | Overlap with other slot? |
|-------|------|-------------|--------|--------------------------|
| 1 | B | Mon **01:00** | Mon **08:00 UTC** | — |
| 2 | A | Mon **16:00** | Tue 08:00 UTC | **No** — B is flat hours before A |

So Monday contributes **two round-trips** (B then A), not one. This is correct and expected.

---

## What this is NOT

| Wrong mental model | Correct model |
|--------------------|---------------|
| “One trade per calendar day globally” | Up to **one per slot per day**; Monday = 2 slots → 2 trades |
| “Monday: evaluate B first, only one of B or A may trade” | **Both** B and A may trade the same Monday (sequential) |
| `max_trades_per_day=1` on the whole account | **One concurrent position** + **per-slot daily cap** |

The old `monday_priority` field in early handover drafts described a shared daily budget — **deprecated**. Slots are independent except for the account-wide concurrency cap.

---

## Backtester mapping

Each slot was backtested as a **separate** `tudysho` strategy instance (isolated combo). That instance’s `_last_trade_date` enforces rule **2** for that slot only. Rule **1** across slots is guaranteed by the live schedule (B expires before A opens on Monday; Thu A expires before Fri C; etc.).

`max_concurrent = dte + 1` inside `reference/tudysho.py` is a **per-instance** rolling-window limit for 1-DTE grids — not the live account-wide cap. Live should use **max 1 concurrent** across slots.

---

## CryoTrader implementation sketch

```python
def may_open(slot_id: str, nyc_date: date, open_positions: list) -> bool:
    if open_positions:
        return False  # rule 1
    if last_trade_date.get(slot_id) == nyc_date:
        return False  # rule 2
    return True
```

On successful open: `last_trade_date[slot_id] = nyc_date`.

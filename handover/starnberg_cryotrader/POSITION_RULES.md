# Position and entry limits (live)

Authoritative rules for **one CryoTrader account** running Starnberg
(single slot `starnberg`).

---

## Three rules

### 1. One concurrent open position

At any instant the account holds **at most one** open Starnberg ticket
(short + wing counted as one position).

Before any new entry: `len(open_positions) == 0`.

Locked backtest uses `max_concurrent=1`.

### 2. One entry per UTC calendar day

At most **one new open** per **UTC** calendar date when the Mon–Fri schedule
and 16:00 UTC clock apply.

Track `_last_trade_date` in **UTC** (matches `reference/v14.py`).

### 3. Always hedged — never naked

Every open attaches an equal-quantity further-OTM long on the **same expiry**
(`wing_pct=0.07`, `min_width_usd=2500`).

If no outer strike is available: **do not** open a naked short. Roll back the
ticket (backtester behavior in `v18._open_single`).

---

## Weekend cover

`weekend_cover=skip`: do not open into expiries that settle Sunday or Monday
08:00 UTC (weekend session cover). See `expiry_covers_weekend` in
`reference/v14.py`.

---

## CryoTrader implementation sketch

```python
# Pseudocode
if open_positions:
    return  # rule 1
if utc_date == last_trade_date:
    return  # rule 2
if weekday not in mon_fri:
    return
if utc_minutes < 16 * 60:
    return
if not richforce2_gate(...):
    return
if weekend_cover_skip(selected_expiry):
    return

short = open_short(...)
wing = attach_wing(short, wing_pct=0.07, min_width_usd=2500)
if wing is None:
    rollback(short)  # rule 3 — never naked
```

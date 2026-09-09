# Timezones

CryoTrader’s loop, Deribit API, logs, and expiry settlement run in **UTC**.

Starnberg’s entry clock is also **UTC** (`entry_time_utc = 16:00`). No
NYC conversion is required.

---

## Entry

1. Tick `dt` is UTC.
2. Entry allowed Mon–Fri when minute-of-day ≥ **16:00 UTC** and the RichForce2
   gate passes (see `STRATEGY_LOGIC.md`).
3. Expiry is the next daily Deribit settlement (~08:00 UTC).

---

## Related clocks

| Event | Timebase |
|-------|----------|
| Entry screen / RichForce2 | 16:00 UTC Mon–Fri |
| Proximity stop window | hours before expiry (Full2Eq8: 16h @ 2%) |
| Deribit daily expiry | ~08:00 UTC |
| Weekend cover | `skip` — no tickets whose expiry covers Sun/Mon settlement |

---

## Note

TuDySho uses NYC `entry_time` and needs a DST-aware conversion. That does
**not** apply here. Do not import TuDySho’s `to_nyc` entry path for Defined
Theta.

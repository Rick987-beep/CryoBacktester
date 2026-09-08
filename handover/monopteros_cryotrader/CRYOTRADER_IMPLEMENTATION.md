# CryoTrader implementation guide

For an AI agent or developer porting **Monopteros** into CryoTrader.  
**Read `README.md`, `POSITION_RULES.md`, `TIMEZONE.md`, and `STRATEGY_LOGIC.md` first.**

**Do not modify CryoTrader from this package automatically** — this folder is
the brief. Implementation happens in the CryoTrader repo under human/agent
control there.

---

## Goal

Implement **one live strategy** on one account:

| Strategy | Schedules (NYC) | Combo | Structure |
|----------|-----------------|-------|-----------|
| `monopteros` | `mon_early` 00:05 · `mon_thu` 14:00 · `fri` 12:30 | `7e21b64f1d94` | Naked short strangle |

Matches `reference/monopteros.py` for routing, sizing, and exits.

Suggested module: `CryoTrader/strategies/monopteros.py` (or update an existing
TuDySho / multi-slot module to this lock — do not leave the obsolete three-combo
A/B/C hashes live).

---

## Implementation checklist

### Phase 1 — Shared infrastructure

- [ ] Create / update strategy module and register it.
- [ ] Confirm turbulence feed matches backtester hourly UTC convention.
- [ ] **Entry time:** NYC `entry_time` via `to_nyc` / `to_utc` (`TIMEZONE.md`).
- [ ] Expose `equity_usd`, `spot`, `nav_usd` on account state.
- [ ] Execution profile: open short call+put; close both on exit.

### Phase 2 — Param loading

- [ ] Load `params/monopteros.json`.
- [ ] Apply global sizing + equity DD; load nested `schedules`.
- [ ] Ignore inert grid keys listed in `LIVE_PARAM_SCHEDULE.json`.
- [ ] On open: snapshot `combo_hash`, `schedule_id`, stop params onto metadata.

### Phase 3 — Entry

- [ ] `POSITION_RULES.md`: max 1 concurrent; one entry per schedule per NYC day.
- [ ] `resolve_schedule` + watch window.
- [ ] Turbulence gate per schedule.
- [ ] `select_expiry(dte)` + delta strangle with `min_otm_pct`.

### Phase 4 — Exits

- [ ] Premium SL when schedule `stop_loss_pct` > 0.
- [ ] Strike proximity (hours + USD buffer).
- [ ] Equity drawdown stop at 5%.
- [ ] Expiry settlement as default close.
- [ ] No take-profit.

### Phase 5 — Validation

- [ ] Port / adapt `reference/test_tudysho_monopteros.py`.
- [ ] Compare live fills to `backtests/trades_monopteros.csv` /
      `fills_monopteros.csv` (exit mix, Monday double-print).
- [ ] Log `combo_hash=7e21b64f1d94` and `schedule_id` on every open.

---

## CryoTrader file hints

| Path | Purpose |
|------|---------|
| `CryoTrader/strategies/` | **Add/update** `monopteros.py` |
| `CryoTrader/strategies/strategy.py` | Shared helpers |
| `CryoTrader/strategies/short_str_turb_dyn.py` | Execution / turbulence patterns to reuse |
| `CryoTrader/option_selection.py` | Delta selection |
| Slot TOML | Point at fields from `params/monopteros.json` |

---

## Account state contract

```python
state.equity_usd  # float — sizing + equity DD
state.spot        # float — BTC index
state.dt          # tz-aware datetime (UTC)
state.nav_usd     # float — premium target base
# plus option chain / quotes with bid, ask, mark, mark_iv, delta
```

---

## Locked identifiers (do not rename casually)

| Field | Value |
|-------|-------|
| Product | Monopteros |
| Backtester ID | `tudysho_monopteros` |
| Combo hash | `7e21b64f1d94` |
| UI run | 770 |
| Bundle | `tudysho_monopteros_20260904_105017.bundle` |

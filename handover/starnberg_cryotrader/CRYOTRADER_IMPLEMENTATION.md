# CryoTrader implementation guide

For an AI agent or developer porting **Starnberg** into CryoTrader.  
**Read `README.md`, `POSITION_RULES.md`, and `STRATEGY_LOGIC.md` first.**

---

## Goal

Implement **one live strategy** on one account:

| Slot | When (UTC) | Combo | Entry | Structure |
|------|------------|-------|-------|-----------|
| `starnberg` | Mon–Fri | `a1d621a81904` | 16:00 UTC | Short 1DTE 25Δ cheaper + 1:1 further-OTM wing |

Matches `reference/v18.py` for entry, sizing, wing attach, and Full2Eq8 exits.

**Greenfield:** CryoTrader has no theta strategy today. Suggested module:
`CryoTrader/strategies/starnberg.py`. Reuse shared execution / option
helpers; do **not** pretend this is a thin fork of `tudysho.py`.

---

## Implementation checklist

### Phase 1 — Shared infrastructure

- [ ] Create `strategies/starnberg.py` (or agreed name) and register it.
- [ ] Confirm daily **VRP / vol_context** feed matches backtester definition
      (RichForce2 gate).
- [ ] Confirm front 25Δ RR (call IV − put IV on trade expiry) for cheaper-side
      pick.
- [ ] **Entry time:** 16:00 UTC (system time; see `TIMEZONE.md`).
- [ ] Expose `equity_usd`, `spot`, `nav_usd` (optional) on account state.
- [ ] Execution profile: open short at bid + buy wing at ask in one ticket;
      close both legs on exit.

### Phase 2 — Param loading

- [ ] Load `params/starnberg.json`.
- [ ] Expand `stop_book=full2_eq8` → prox `2@16` + equity 8% (or read the
      expanded fields already in the JSON).
- [ ] Ignore inert rich-sizing keys (`rich_mode` stays `none`; see
      `LIVE_PARAM_SCHEDULE.json` → `ignore_grid_params`).
- [ ] On open: snapshot `combo_hash`, `slot_id`, stop + wing params onto
      position metadata.

### Phase 3 — Entry

- [ ] `POSITION_RULES.md`: max 1 concurrent; one entry per UTC day; always
      hedge or roll back.
- [ ] RichForce2: VRP≥4 else force after 2 idle Mon–Fri sessions.
- [ ] `select_expiry(dte=1)` + weekend cover skip.
- [ ] Cheaper front 25Δ short; qty = `2 × equity/spot`.

### Phase 4 — Wing

- [ ] Port `pick_wing_quote_by_pct` / `wing_target_width_usd` from `v18.py`.
- [ ] Equal qty, same expiry, further OTM; min width $2500.
- [ ] Missing outer → abort open (never naked).

### Phase 5 — Exits

- [ ] Strike proximity 2% in last 16h before expiry (short strike).
- [ ] Open-PnL equity stop at 8% of equity (flatten when quotes exist).
- [ ] Expiry settlement as default close.
- [ ] No TP / no credit SL on this lock.

### Phase 6 — Validation

- [ ] Port or adapt `reference/test_theta_engine_v18.py` for wing + stop parse.
- [ ] Compare live fills to `backtests/trades_starnberg.csv` /
      `fills_starnberg.csv` (exit mix, wing on every open).
- [ ] Log `combo_hash=a1d621a81904`, `stop_book`, `wing_pct` on every open.

---

## CryoTrader file hints

| Path | Purpose |
|------|---------|
| `CryoTrader/strategies/` | **Add** `starnberg.py` |
| `CryoTrader/strategies/strategy.py` | Shared helpers |
| `CryoTrader/option_selection.py` | Delta selection / resolve legs |
| `CryoTrader/execution/profiles.py` | New or extended profile for short+wing |
| Indicators (vol / IV) | Must match backtester VRP + 25Δ IV |

TuDySho / `short_str_turb_dyn` are useful for **execution plumbing**, not for
entry/exit semantics.

---

## Account state contract

```python
state.equity_usd  # float — sizing + equity stop
state.spot        # float — BTC index
state.dt          # tz-aware datetime (UTC)
state.nav_usd     # float — optional; equity stop uses equity in BT
# plus option chain / quotes with bid, ask, mark, mark_iv, delta
```

---

## Common pitfalls

1. **Naked opens:** opening the short when the wing cannot fill.
2. **Strangle assumption:** this book is a **single** short + wing, not call+put
   short.
3. **Expiry = win:** 180 expiries include most losers.
4. **Wings as PnL edge:** wings are a defined-risk principle; do not claim they
   “save” blow-ups in live marketing or logs.
5. **Re-opening rich_mode:** closed on run 741 — leave off.

---

## Backtest caveats for QA

1. Single combo / single slot — no multi-slot merge.
2. Header risk ratios from `inspect combo --full` (engine lock).
3. Proximity fired twice in sample; equity stop did not.
4. `end_of_data` closes are not present on this favourite (all closed).

---

## Related CryoBacktester paths

| Path | Purpose |
|------|---------|
| `workspace/strategies/theta_engine/v18.py` | Canonical strategy |
| `workspace/strategies/theta_engine/v14.py` | RichForce2 / front / open |
| `workspace/strategies/theta_engine/_common.py` | Entry helpers, skew |
| `workspace/tests/test_theta_engine_v18.py` | Unit tests |
| `workspace/marketing/ship/starnberg/` | Master investor HTML |
| `docs/strategy_howto.md` | Leg dict required fields |

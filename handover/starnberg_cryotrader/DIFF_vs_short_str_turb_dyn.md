# Starnberg vs CryoTrader `short_str_turb_dyn` / `tudysho`

CryoTrader has **no** theta live strategy today. This note explains what to
**reuse** from existing CT modules and what must be **new**.

---

## Reuse from CryoTrader (keep / adapt)

| Area | Likely CT location | Notes |
|------|--------------------|-------|
| Strategy base / tick loop | `strategies/strategy.py` | Account state, logging |
| Option selection primitives | `option_selection.py` | Delta pick; adapt for single leg |
| Fees | shared pricing helpers | Match Deribit fee per leg |
| Execution plumbing | `execution/profiles.py` | Need short+wing open/close, not strangle-only |
| Indicators cache | CT indicators package | Wire **vol_context VRP** + front 25Δ IV |

---

## Do not reuse as-is

| TuDySho / `short_str_turb_dyn` | Starnberg |
|-------------------------------|---------------|
| Turbulence gate | **RichForce2** VRP≥4 / force-2d |
| Short **strangle** (call+put) | Short **one** side + further-OTM **long wing** |
| Premium % NAV sizing | `qty_per_1btc_equity` (2×) |
| Premium SL + proximity hours | Full2Eq8: prox **2%@16h** + **8%** equity stop; no credit SL |
| Multi-slot schedule | **One** Mon–Fri slot @ 16:00 UTC |
| Turbulence fail-open | Different gate entirely |

TuDySho / Monopteros handover (`handover/monopteros_cryotrader/`) is a
**sibling protocol**, not a fork base for this product. (TuDySho’s NYC
entry-time conversion is TuDySho-specific and does not apply here.)

---

## Suggested CryoTrader approach

1. **New file** `strategies/starnberg.py` (do not overwrite `tudysho.py`).
2. Port behavior from `reference/v18.py` (+ v14 entry / wing helpers).
3. Add an execution profile that opens short+wing atomically and closes both.
4. Register strategy; deploy slot TOML pointing at `params/starnberg.json`
   fields (or embed the JSON).
5. Validate against `backtests/fills_starnberg.csv` (2 open fills per
   trade_idx).

---

## Param map (backtester → live)

| Backtester | Live |
|------------|------|
| `book=rf2_1600` | RichForce2 @ 16:00 UTC |
| `stop_book=full2_eq8` | prox 2@16 + equity 8% |
| `wing_pct=0.07` | always-on wing |
| `min_width_usd=2500` | listed width floor |
| `skew_source=front` + `skew_mode=cheaper` | sell cheaper 25Δ front side |
| `combo_hash` | `a1d621a81904` (log / metadata) |

---

## Related packages

| Package | Role |
|---------|------|
| `handover/starnberg_cryotrader/` | **This** package |
| `handover/monopteros_cryotrader/` | Separate product; NYC multi-schedule |
| `workspace/marketing/ship/starnberg/` | Investor HTML (not a coding spec) |

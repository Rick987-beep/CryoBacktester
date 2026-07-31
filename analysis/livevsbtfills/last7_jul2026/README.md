# Last 7 live fills vs backtester (Jul 20–29, 2026)

Forensic comparison of the most recent slot-02 prod trades against a
**single-combo** `tudysho_eisbach` run configured to match current
[CryoTrader slot-02.toml](https://github.com/Rick987-beep/CryoTrader/blob/main/slots/slot-02.toml).

## Quick start

```bash
# Refresh prod blotter
bash analysis/livevsbtfills/scripts/pull_live_blotter.sh

# Run BT (slot-02 params, Jul 19–29)
python analysis/livevsbtfills/last7_jul2026/scripts/run_slot02_bt.py

# Build comparison CSVs
python analysis/livevsbtfills/last7_jul2026/scripts/build_last7_comparison.py
```

Outputs: `data/live_last7.csv`, `data/bt_window.csv`, `data/comparison.csv`

## BT configuration

| Source | Value |
|--------|-------|
| Strategy | `tudysho_eisbach` (1 combo) |
| mon_thu | 13:00 NYC, δ=0.05, min_otm=2.6, SL=4%, prox 8h/$500 |
| fri | 12:00 NYC, δ=0.1, min_otm=2.4, SL=4% |
| mon_early | disabled (turb=999) |
| Sizing | BT convention: 0.8% NAV / 12 per BTC-equity on $100k |
| Bundle | see `metadata.json` → `backtest.bundle` |

Reference combo: UI run **387** favourite **`97dabfdeaade`** (mon_thu leg).

## Last 7 live fills — summary

| Date | Sched | Live exit | Live $/lot | BT $/lot | Match? | Notes |
|------|-------|-----------|----------:|---------:|--------|-------|
| Jul 20 | mon_thu | expiry | +11.85 | — | ✗ | Live still on **old config** (16:00, δ=0.1); BT uses new 13:00 params |
| Jul 21 | mon_thu | expiry | +5.90 | +11.86 | ~ | Live **zero call fill** (put only); different strikes |
| Jul 22 | mon_thu | expiry | +11.72 | +11.86 | ✓ | Same strikes 68000/64000 |
| Jul 23 | mon_thu | expiry | +29.18 | +28.99 | ✓ | Same strikes 66500/63000 |
| Jul 24 | fri | expiry | +11.46 | +11.48 | ✓ | Same strikes 66000/62000 |
| **Jul 27** | mon_thu | **combined_mark_sl** | **−59.35** | **−100.90** | ~ | **Both SL at 23:00 UTC**, same strikes; BT loss deeper (close pricing / qty) |
| Jul 28 | mon_thu | expiry | +0.94 | −2.66 | ✗ | Live **partial put** (3.3/20.2 @ 61500); BT delayed to 20:40, put **62000**, `end_of_data` |

## Peculiar fills — what happened

### Jul 27 loss (combined_mark_sl @ 23:00 UTC)

The trade you're interested in. Live and BT both:

- Opened **Jul 27 18:00 UTC** (14:00 NYC) — 67000C / 63000P, 1-DTE
- Entered at ~**0.0001 BTC** per leg (minimum tick)
- Held through a spot drop from ~65k → **63.7k** by 23:00
- Stop triggered when **put mark ~0.0012** pushed combined mark above SL threshold (5× entry premium with SL=4.0)

Live closed at 23:00: call 0.0001, **put 0.001** → **−$59/lot**.  
BT closed same tick as `stop_loss` → **−$101/lot** (mark-based close, ~19 lots vs live 20.3).

**Takeaway:** Timing and trigger **match**. PnL gap is close cost / sizing, not a missed stop.

### Jul 28 partial fill

- Live opened 18:33 NYC: **call fill None (0 qty)**, put **3.3/20.2** @ 0.0001 on 61500P
- Snapshots 17:00–18:30: **zero openable BT ticks** (call bid=0 after min_otm)
- BT finally opened **20:40 UTC** when both bids > 0 — but selected **62000P** not 61500P
- BT run ended **`end_of_data`** Jul 28 23:55 (Jul 29 data incomplete to 07:55 only)

### Jul 21 zero-call

- Live filled **put only** at 64500; call 69000 got 0 qty
- BT opened 68000/64000 at 17:55 with both legs — standard full strangle
- Live $/lot lower because PnL spread over 20.2 target qty but only one leg earned premium

### Jul 20 (config transition)

- Live metadata shows **entry_time=16:00, delta=0.1** (pre slot-02 update)
- BT run uses **13:00, delta=0.05** — intentionally not comparable

## Caveats

1. Compare **$/lot**, not raw $ (live ~20 lots @ 0.28% on ~$285k vs BT ~19–22 on $100k @ 0.8%)
2. BT requires **bid > 0**; live uses **mark fallback** → live enters earlier / with partial legs
3. Live **`combined_mark_sl`** ≈ BT **`stop_loss`** on combined mark (same threshold math)
4. Jul 29 parquet is **partial** (through ~07:55 UTC) — Jul 28 BT trade cannot reach Saturday expiry cleanly

## Files

| File | Description |
|------|-------------|
| `data/live_last7.csv` | Last 7 prod fills with leg-level JSON |
| `data/bt_window.csv` | BT trades in window |
| `data/comparison.csv` | Side-by-side match table |
| `metadata.json` | Bundle path, caveats, provenance |
| `scripts/` | `run_slot02_bt.py`, `build_last7_comparison.py` |

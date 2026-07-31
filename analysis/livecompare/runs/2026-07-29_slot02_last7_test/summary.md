# Livecompare — slot 02 — 2026-07-29

**Window:** last 7 live fills
**BT:** `tudysho_eisbach` | bundle `backtester/reports/tudysho_eisbach_slot02_slot_20260729_083041.bundle`

## Comparability
- OK: 4 | WARN: 1 | EXCLUDE: 2

## Trades

| Date | Sched | Comp | Live $/lot | BT $/lot | Δ | Notes |
|------|-------|------|----------:|---------:|--:|-------|
| 2026-07-20 | mon_thu | EXCLUDE | 11.85 | — | — | Live trade metadata params differ from current slot TOML |
| 2026-07-21 | mon_thu | WARN | 5.90 | 11.86 | -5.97 | BTC-22JUL26-69000-C: filled 0/20.2 @ None |
| 2026-07-22 | mon_thu | OK | 11.72 | 11.86 | -0.15 |  |
| 2026-07-23 | mon_thu | OK | 29.18 | 28.99 | +0.19 |  |
| 2026-07-24 | fri | OK | 11.46 | 11.48 | -0.03 |  |
| 2026-07-27 | mon_thu | OK | -59.35 | -100.90 | +41.55 | Stopped out via combined_mark_sl(metadata) at 2026-07-27 23: |
| 2026-07-28 | mon_thu | EXCLUDE | 0.94 | -2.79 | +3.74 | BTC-29JUL26-65500-C: filled 0/20.2 @ None; BTC-29JUL26-61500 |

## Warnings

- **SIZING_DIFF** (warn): BT uses default sizing (0.8% NAV / 12 per BTC-equity); compare $/lot only
- **FILL_MODEL** (warn): BT requires bid>0; live min_qty_price_floor=0 allows mark fallback
- **MON_EARLY_DISABLED** (info): mon_early disabled on live; BT uses turbulence_threshold=999
- **DATA_GAP** (error): Missing options parquet for 2026-07-30

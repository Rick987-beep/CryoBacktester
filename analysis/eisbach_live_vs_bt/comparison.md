# Eisbach forensic: Live slot-02 vs BT run 375

**Window:** entry dates 2026-07-01 → 2026-07-17  
**BT:** run 375, combo `42dbac5b4976`, bundle `tudysho_eisbach_20260720_090214.bundle`  
**Live:** prod `root@46.225.137.92` blotter `/opt/ct/trade_history/slot-02.jsonl` (pulled 2026-07-20)

## Caveats (read first)

1. **Strategy change mid-window:** Live slot-02 ran `short_str_turb_dyn` through **Jul 7**, then **tudysho Eisbach** from **Jul 8**. Only Jul 8–17 is an apples-to-apples strategy comparison.
2. **Sizing differs:** Live uses `nav_premium_pct=0.28`, `max_qty_per_1btc_equity=4.36`, `max_quantity=20` on ~$285k equity → typically **20 lots**. BT uses `0.8%` / `12` on **$100k** → ~21–22 lots. Compare **$/lot** and trade presence, not raw $ PnL.
3. **Bid vs mark:** Live pricing falls back to **mark** when bid is missing (`min_qty_price_floor=0`). BT requires **bid > 0** after `min_otm_pct`. This alone explains several BT skips/delays.
4. **Jul 17 BT** closes `end_of_data` because the run date range ends `2026-07-18` before Saturday 08:00 expiry; live booked full expiry PnL.

## Totals

| Scope | Live n | BT n | Live $ | BT $ | Live $/lot | BT $/lot |
|-------|-------:|-----:|-------:|-----:|-----------:|---------:|
| Full Jul 1–17 | 14 | 12 | 4,279.55 | 4,060.43 | 213.98 | 186.72 |
| Tudysho-only Jul 8–17 | 9 | 8 | 3,123.62 | 2,490.42 | 156.18 | 115.04 |

## Day-by-day

| Date | WD | Live# | BT# | Live $ | BT $ | Live $/lot | BT $/lot | Notes |
|------|----|------:|----:|-------:|-----:|-----------:|---------:|-------|
| 2026-07-01 | Wed | 1 | 1 | 543.37 | 600.62 | 27.17 | 26.81 | live still short_str_turb_dyn; strikes L 62000/58000 vs BT 61500/58000 |
| 2026-07-02 | Thu | 1 | 1 | 110.53 | 362.93 | 5.53 | 16.50 | live still short_str_turb_dyn; strikes L 64000/59500 vs BT 63000/59500 |
| 2026-07-03 | Fri | 1 | 1 | 125.04 | 242.29 | 6.25 | 11.11 | live still short_str_turb_dyn; strikes L 64000/60000 vs BT 64000/60500 |
| 2026-07-04 | Sat | 0 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | flat |
| 2026-07-05 | Sun | 0 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | flat |
| 2026-07-06 | Mon | 1 | 1 | 125.64 | 364.16 | 6.28 | 17.26 | live still short_str_turb_dyn; strikes L 66000/61500 vs BT 66000/62500; BT no mon_early (0-DTE call bid=0 after min_otm); live was legacy strat |
| 2026-07-07 | Tue | 1 | 0 | 251.35 | 0.00 | 12.57 | 0.00 | live still short_str_turb_dyn; count L1/B0; BT skip: min_otm call bid=0 in snapshots |
| 2026-07-08 | Wed | 1 | 1 | 688.10 | 607.93 | 34.41 | 27.89 |  |
| 2026-07-09 | Thu | 1 | 0 | 255.78 | 0.00 | 12.79 | 0.00 | count L1/B0; BT skip: min_otm call bid=0 in snapshots |
| 2026-07-10 | Fri | 1 | 1 | 229.75 | 244.48 | 11.49 | 11.48 | strikes L 66000/62000 vs BT 66000/62500 |
| 2026-07-11 | Sat | 0 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | flat |
| 2026-07-12 | Sun | 0 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | flat |
| 2026-07-13 | Mon | 2 | 2 | 461.70 | 737.65 | 23.08 | 33.61 |  |
| 2026-07-14 | Tue | 1 | 1 | 462.45 | 494.95 | 23.12 | 23.13 |  |
| 2026-07-15 | Wed | 1 | 1 | 229.99 | 247.76 | 11.50 | 11.63 |  |
| 2026-07-16 | Thu | 1 | 1 | 337.42 | 248.19 | 16.87 | 11.44 | strikes L 66000/62500 vs BT 65500/62000; BT delayed to 23:20 waiting for positive bids after min_otm |
| 2026-07-17 | Fri | 1 | 1 | 458.43 | -90.54 | 22.92 | -4.13 | BT end_of_data (run ends 2026-07-18); live expired Sat 08:00 |

## Pairwise matched trades (Jul 8+, same schedule)

| Date | Schedule | Live C/P | BT C/P | Live $/lot | BT $/lot | Δ $/lot | Entry (L / BT UTC) |
|------|----------|----------|--------|-----------:|---------:|--------:|--------------------|
| 2026-07-08 | mon_thu | 64000/60500 | 64000/60500 | 34.41 | 27.89 | +6.52 | 20:03 / 20:05 |
| 2026-07-09 | mon_thu | 65000/61500 | — | 12.79 | — | — | 20:00 / — |
| 2026-07-10 | fri | 66000/62000 | 66000/62500 | 11.49 | 11.48 | +0.01 | 16:00 / 16:15 |
| 2026-07-13 | mon_early | 63500/61500 | 63500/62000 | 6.29 | 16.89 | -10.59 | 05:33 / 05:10 |
| 2026-07-13 | mon_thu | 64000/60000 | 64000/60000 | 16.79 | 16.72 | +0.07 | 20:03 / 20:00 |
| 2026-07-14 | mon_thu | 66500/62500 | 66500/62500 | 23.12 | 23.13 | -0.01 | 20:03 / 20:00 |
| 2026-07-15 | mon_thu | 67000/63000 | 67000/63000 | 11.50 | 11.63 | -0.13 | 20:00 / 20:05 |
| 2026-07-16 | mon_thu | 66000/62500 | 65500/62000 | 16.87 | 11.44 | +5.43 | 20:00 / 23:20 |
| 2026-07-17 | fri | 65000/61500 | 65000/61500 | 22.92 | -4.13 | +27.06 | 16:00 / 16:00 |

## Key findings

1. **Exit quality matches when both trade:** Almost all comparable round-trips expired OTM with near-identical $/lot (Jul 10 fri, Jul 13 mon_thu, Jul 14–15 mon_thu within pennies).
2. **BT skips Jul 7 & Jul 9:** Not turbulence (composite < 60). After `min_otm_pct=2.6`, the selected **call bid is 0** in the 5-min snapshot → BT refuses entry. Live still filled (mark/RFQ path).
3. **Jul 16 timing gap:** Live filled 20:00 UTC; BT waited until **23:20** for positive bids post-min_otm → different strikes (66000/62500 vs 65500/62000) and different $/lot.
4. **Jul 13 mon_early:** Both traded; put strike differs (live 61500 vs BT 62000) — live $/lot lower ($6.29 vs $16.89), worth a separate fill-quality look.
5. **Jul 17 fri:** Same strikes 65000/61500; live +$22.92/lot at expiry, BT **−$4.13/lot** only because of `end_of_data` MTM at midnight, not a real loss vs live.
6. **Pre-Eisbach (Jul 1–7):** Live legacy `short_str_turb_dyn` vs BT Eisbach schedules — different entry times (esp. Friday) and strike selection; not a fair logic match.

## Artifacts

- `analysis/eisbach_live_vs_bt/slot-02.jsonl` — raw prod blotter
- `analysis/eisbach_live_vs_bt/live_jul1_17.csv`
- `analysis/eisbach_live_vs_bt/bt_jul1_17.csv`
- `analysis/eisbach_live_vs_bt/day_by_day.csv`

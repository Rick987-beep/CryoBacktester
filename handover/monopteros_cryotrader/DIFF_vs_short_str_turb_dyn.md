# Monopteros vs CryoTrader `short_str_turb_dyn` / legacy tudysho slots

Monopteros is in the **TuDySho family** (short strangle + turbulence). This note
explains what to **reuse** from CryoTrader and what changed vs the obsolete
three-combo slot pack.

---

## Reuse from CryoTrader (keep / adapt)

| Area | Likely CT location | Notes |
|------|--------------------|-------|
| Strategy base / tick loop | `strategies/strategy.py` | Account state, logging |
| Short strangle execution | `short_str_turb_dyn` / tudysho | Open/close two short legs |
| Option selection | `option_selection.py` | Delta pick + min OTM |
| Fees | shared pricing helpers | Match Deribit fee per leg |
| Turbulence | CT indicators | UTC hour bucket |
| NYC clocks | `market_hours` / DST helpers | Mandatory — see `TIMEZONE.md` |

---

## Do not reuse as-is

| Legacy / `short_str_turb_dyn` | Monopteros lock |
|-------------------------------|-----------------|
| Single UTC `ENTRY_HOUR` | Three **NYC** `entry_time` schedules |
| One param set | Nested `schedules` + global sizing |
| Old slot hashes (`5cd986…`, `e2f4ac…`, `829e72…`) | Master combo **`7e21b64f1d94`** |
| Slot A 16:00 / B 01:00 / C 12:00 | `mon_thu` **14:00** / `mon_early` **00:05** / `fri` **12:30** |
| nav 0.8% / max qty 12 | **0.5%** NAV / max qty **6** |
| No equity DD (or off) | Equity drawdown stop **5%** |
| Separate strategy instances per slot | **One** strategy, schedule routing |

The deleted `handover/tudysho_cryotrader/` pack is obsolete. Do not deploy its
param files.

---

## Suggested CryoTrader approach

1. **New or refreshed** `strategies/monopteros.py` from `reference/monopteros.py`.
2. Keep strangle execution plumbing from `short_str_turb_dyn` / prior tudysho.
3. Replace multi-slot TOMLs with one schedule-aware config from
   `params/monopteros.json`.
4. Validate against `backtests/fills_monopteros.csv` and Monday double entries.

---

## Param map (backtester → live)

| Backtester | Live |
|------------|------|
| `nav_premium_pct=0.5` | Target premium fraction of NAV |
| `max_qty_per_1btc_equity=6` | Contract cap |
| `equity_drawdown_stop_pct=5` | Account mark DD stop |
| `schedules.*.entry_time` | NYC clocks |
| `schedules.*.stop_loss_pct` | Per-schedule premium SL (0 = off) |
| `combo_hash` | `7e21b64f1d94` (log / metadata) |

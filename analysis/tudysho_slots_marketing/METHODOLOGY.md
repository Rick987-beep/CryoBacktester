# Combined tudysho slots — marketing methodology

## Merge rule: full union (no drops)

All trades from slots A, B, and C are included. **No priority deduplication.**

Slots are timed so **open positions do not overlap**:

| Sequence | Entry | Expiry | Next entry |
|----------|-------|--------|------------|
| **Mon B** | ~01:00 NYC | Mon **08:00 UTC** | — |
| **Mon A** | 16:00 NYC (~20–21 UTC) | Tue 08:00 UTC | 12–14h after B closes |
| **Thu A** | 16:00 NYC | Fri 08:00 UTC | — |
| **Fri C** | 12:00 NYC (~16–17 UTC) | Sat 08:00 UTC | After Thu leg expires |

`tudysho` live limits: **one concurrent open position** and **one entry per slot per NYC day** — not one entry globally per calendar day. Mondays can have **two sequential trades** (B then A).

## Validation

We check every pair of trades (excluding `end_of_data` closes) for interval overlap  
`entry₁ < exit₂ AND entry₂ < exit₁`. **Result: 0 overlaps** on the current trade lists.

(`end_of_data` trades at the simulation tail can appear to overlap each other — not live behaviour.  
They are excluded from EOD position detection and intraday band scaling; PnL still books on exit date.)

## Equity curve & risk ratios

1. Union → `combo_trades.csv` (224 trades).
2. Single $100,000 account; realized PnL booked on **exit date**.
3. **EOD MTM `nav_close`**: for each day, combined realized equity plus scaled open-leg mark from the slot still open at end of day (from per-slot `nav_daily`).
4. **Sharpe, Sortino, Calmar** from that MTM daily return series via `equity_metrics` (same engine path as single-combo reports).
5. **Profit factor** from **trade-level** gross wins ÷ gross losses (not daily buckets).
6. **Consecutive win/loss streaks** from trade PnL order.

`stats.json` also keeps `sharpe_close_only` / `sortino_close_only` / `max_dd_pct_close_only` for the old stair-step curve.

## Max drawdown (marketing headline)

Close-only max DD from trade exit dates **understates** open-position stress (slot A alone: ~1% close-only vs **10.7%** with `nav_daily`).

We compute an **intraday-scaled** combined DD:

1. Build combined realized equity from the union trade list (PnL booked on exit date).
2. For each calendar day with an open position in slot *s*, load that slot’s isolated `nav_daily` (`nav_high`, `nav_low`, `nav_close`).
3. Scale intraday excursion to the combined account that day:
   - `scale = combined_realized / slot_nav_close`
   - `est_high = combined_realized + (nav_high − nav_close) × scale`
   - `est_low  = combined_realized − (nav_close − nav_low) × scale`
4. If multiple slots touch the same calendar date (e.g. Mon B AM + A PM — sequential, not overlapping), take the **widest** band: max `est_high`, min `est_low`.
5. Running peak uses intraday highs; max DD = worst `(peak − est_low) / peak`.

Calmar uses the same intraday max DD as Sharpe/Sortino use the MTM return series — one consistent risk basis.

This is sound when slots have **no overlapping open positions**: only one slot marks to market at a time, so scaling each slot’s isolated intraday % move onto the current combined equity is consistent. It is still approximate because each slot was sized in isolation, not on a single replayed combined NAV.

`stats.json` reports:
- `max_dd_pct` — intraday-scaled (marketing)
- `max_dd_pct_close_only` — trade-date stair-step
- `isolated_slot_max_dd_pct` — per-slot isolated backtests for reference

## Remaining approximation

Each slot was still backtested in **isolation** with full NAV for sizing. A single combined replay could shift `nav_premium_pct` contract sizes slightly. PnL union is correct for **timing**; sizing compounding is approximate (same caveat as before, but **no trades dropped**).

## Disclaimer

Past simulated performance is not indicative of future results. Synthetic merge of isolated backtests — see limitations above.

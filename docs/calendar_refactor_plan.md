# Refactor: engine owns fills, kept legs are not fills

**Status:** Phases A–D complete; SL removal + archiving done (20 May 2026)
**Driver:** `cal_premium_collect.py` rolls produced virtual fill rows for kept long
legs. These were accounting fictions — no exchange transaction occurred — yet they
appeared in the fills table with `fee_usd=0.00`. Worse, the strategy authored
`exit_price_btc` on the kept leg, which was a layering violation.

---

## Status update — 20 May 2026

### Done

- **Phase A** — `close_position`, `partial_close`, `add_legs` shipped in
  `strategy_base.py`; engine handles `partial_close=True` and `skip_open_fill=True`
  in `_append_fills`.
- **Phase B** — `cal_premium_collect` migrated. Kept long legs no longer emit virtual
  fills. `_roll_pair` Path B uses `partial_close` for the short only.
- **Phase C** — `preopen_straddle` and `ss_turb_dyn_mk2` migrated to engine-owned fills
  (explicit `side='open'` Trades; per-leg pricing; no post-multiply hacks). Smoke
  tests reconcile to sub-cent.
- **Phase D** — Reconciliation test tolerance tightened from `$0.5` → `$0.01`.
- **Leg-aware unrealized PnL** — `_reprice_legs` now also stores a per-leg USD list
  on `pos._last_reprice_legs`. `engine._open_unrealized_pnl` uses a leg-aware path
  when all legs carry `side` + `price_btc`. Fixes the calendar-spread equity-curve
  bug where the old `direction="sell"` formula compared a NET entry (`short − long`)
  against a GROSS reprice sum (`long_bid + short_ask`).
- **`_roll_pair` reason label fix** — Path B now emits `"expiry"` instead of `"roll"`
  when the short already settled before the Friday roll tick.
- **Stop-loss removed from `cal_premium_collect`** — the per-pair SL check was based
  on the same flawed direction-scalar math that produced the equity-curve bug, and
  added complexity we don't need while we are still verifying the roll logic.
  Removed: `stop_loss_mult` param, `_sl_mult` attr, `_check_stop_loss` method,
  the SL check loop at top of `on_market_state`, the `metadata["stop_loss_mult"]`
  writes in all close/roll paths, and `TestStopLoss` from the test file. The pair
  now closes only via the weekly roll (Path A drift, Path B expiry/short roll),
  long-leg drift, or `on_end`.
- **Running USD account balance in fills** — engine tracks per-combo running cash
  balance and appends it to every fill row as `balance_usd`. Surfaced as a `Balance`
  column in the HTML trade log, the UI fills view, and the persisted fills parquet.
- **Daily realized close in nav_daily** — engine appends `realized_close` next to
  `nav_low/high/close` so the Daily Equity report can split equity into `balance`
  (`account_size + realized_close`) and `open position pnl` (`equity − balance`).
  Both columns now appear in the "Daily Equity — Best Combo" HTML table before
  the existing equity column.
- **Archived legacy strategies** — strategies that were never migrated to the
  leg-aware engine-owned-fills API (`close_position` / `partial_close` /
  `add_legs`) moved to `backtester/archive/strategies_to_be_fixed/`. We are
  deliberately pushing back on fixing them — they would all need the same
  leg-tagging audit and exit-price-annotation cleanup that the calendar refactor
  exposed, and we have no immediate research need for them. The runnable
  registry in `backtester/run.py` now contains only the three migrated
  strategies: `cal_premium_collect`, `preopen_straddle`, `ss_turb_dyn_mk2`.

### Archived strategies (in `backtester/archive/strategies_to_be_fixed/`)

`batman_calendar`, `bt_supertrend_lc`, `daily_put_sell`, `deltaswipswap`,
`hedged_put_sell`, `l_momentum`, `l_straddle_index_move`, `long_gamma_MOVE`,
`long_gamma_whitelist`, `short_generic`, `short_str_turb_dyn`,
`short_strangle_weekly_cap`, `ss_turb_dyn_sl`, `str_volburst_pullback`. Tests
that targeted them (`test_l_momentum`, `test_long_gamma_whitelist`,
`test_short_generic`, `test_str_volburst_pullback`) moved alongside under
`archive/.../tests/` and are not collected by the default test suite.

The corresponding experiment TOMLs (`short_str_turb_dyn_v1.toml`,
`delta_strangle_tp_v1.toml`) are left in place but will fail at run time until
their strategies are migrated and re-registered.

### Verified per-leg consistency (cal_premium_collect, March 2026 backtest)

| Path | Source of per-leg PnL | Reconciles? |
|---|---|---|
| Fills cashflow | `engine._append_fills` (sign × qty × price_btc × spot) | ✓ |
| Realized PnL on close | `close_trade` / `close_position` leg-aware path | ✓ |
| Unrealized PnL mark | `engine._open_unrealized_pnl` leg-aware path | ✓ |
| Running cash balance | `engine._append_fills` `running_balance[i]` | ✓ |
| End-of-day equity = balance + open_pos_pnl | `nav_daily_df.realized_close` + `nav_close` | ✓ |

### What we learned

- **Direction is a per-leg property, not a per-position property.** Every
  `OpenPosition`-level scalar (`entry_price_usd`, `metadata["direction"]`,
  `_last_reprice_usd`) is ambiguous the moment a strategy mixes a long and a
  short leg. The only sound representation is "tag every leg with `side` and
  `price_btc`, derive everything else." Everywhere we still rely on
  position-level scalars is a latent calendar-style bug.
- **A fill must correspond to an exchange order.** Synthesising "virtual" close
  fills for kept legs (the old roll path) made the trade log look complete but
  poisoned every downstream PnL attribution. The engine-owned fills model
  (`partial_close=True`, `skip_open_fill=True`) is what lets the strategy say
  "this is a roll, not two trades" without lying about fills.
- **Reconciliation tolerance is a signal, not a knob.** The old `$0.5` tolerance
  in `test_engine_fills_recon` was hiding the NET-vs-GROSS sign mismatch in
  `_open_unrealized_pnl`. Tightening it to `$0.01` immediately exposed the
  calendar bug and forced the leg-aware path.
- **NET-vs-GROSS comparisons are the recurring hazard.** Strategy code thinks in
  NET premium (what the user paid / received); pricing code returns GROSS marks
  (bid + ask for each leg). Any formula that mixes the two without tagging
  per-leg side will silently double-count on a calendar.
- **Stop-loss math built on top of position-level scalars inherits the same
  bug.** That's why we removed SL from `cal_premium_collect` rather than try
  to patch it. We will re-add SL only once the leg-aware reprice cache is
  the single source of truth for every comparison.

### Still to clean up (non-urgent, in priority order)

1. **Consolidate leg-pricing onto one helper.** Today there are multiple paths
   to "what is this leg worth right now":
   - `_reprice_legs(state, pos)` in `strategy_base.py` (canonical, leg-aware,
     caches on `pos._last_reprice_legs`)
   - `engine._open_unrealized_pnl` (uses the cache when present, falls back
     to direction-scalar arithmetic otherwise)
   - Per-strategy `_close_pair` / `_close` methods that hand-roll
     `leg["exit_price_btc"]` / `leg["fee_btc_close"]` annotations
     (intrinsic-if-expired, bid-for-long, ask-for-short, deribit-fee)
   - Strategy-side TP math that reads `metadata["long_entry_premium_usd"]`
     and recomputes a mark instead of consulting the cache.
   All four need to collapse to a single helper, say
   `auto_annotate_exit_prices(state, pos)` for the close-side and
   `_reprice_legs(state, pos)` for the open-side, with `pos._last_reprice_legs`
   as the only place anyone reads "current per-leg USD value." This is the
   single biggest piece of remaining work and the one most likely to surface
   the next subtle bug if left alone.
2. **Position-level `entry_price_usd` + `metadata["direction"]`** — remove
   from `OpenPosition` once (1) is done and nothing depends on them anymore.
3. **Merge `close_position` and `close_trade`.** `close_position` is currently
   a thin wrapper; once no strategy imports `close_trade` directly we can
   collapse them. The remaining `close_trade` callers all live in the archived
   strategies — i.e. this depends on Phase E.
4. **Phase E — migrate (or formally retire) archived strategies.** Each one
   needs the same audit: leg-tag every open, replace `close_trade` with
   `close_position`, kill any per-strategy exit-price annotation that the
   shared helper from (1) can handle. Do this only when we actually want to
   research the strategy again.
5. **Re-introduce stop-loss on the consolidated reprice cache.** Once (1) is
   in place we can add a strategy-agnostic SL helper that consults
   `pos._last_reprice_legs` directly — no per-strategy direction math.

---

## Principles

1. **A fill is an order Deribit executed.** Nothing else.
2. **A trade is the lifecycle of an exchange position from open to close.** If a leg is
   carried forward, it stays in the same trade; it does not close.
3. **Strategies do not author fills.** They mutate `OpenPosition` via engine-owned helpers
   that return `Trade` objects; the engine derives fills from those trades.

## Conceptual change

Today, a "trade" in `cal_premium_collect` actually represents a *week* (a roll cycle),
not the lifetime of an exchange position. Two distinct concepts are conflated:

- **Strategy cycle** — "this week's spread" — opens Friday, closes/rolls next Friday
- **Exchange position** — the actual long put, which lives across multiple weekly cycles
  until it is eventually unwound

The refactor separates them.

## Proposed model

```
Trade = one exchange position (open → close), regardless of strategy cycles
Fill  = one real exchange transaction
Strategy cycle log = optional, strategy-owned (NOT part of engine output)
```

A roll where the long is kept becomes:

- Short leg: trade #N closes (it was opened, now bought back) → 2 fills total
  (the open from week 1, the close at roll)
- Long leg: stays in trade #N-or-prior, NO close event, NO new fill
- New short leg for next week: trade #M opens → 1 open fill

When the long is *eventually* sold (drift threshold hit or end of strategy), THEN
trade #N-long closes with a close fill.

## Concrete changes

### 1. New API in `strategy_base.py`

- `close_position(state, pos, reason, fees_close=0.0) -> Trade`
  - Drop-in replacement for current `close_trade()`. Stamps `pos_id` and `skip_open_fill`
    semantics. Engine derives close fills from `trade.metadata["legs"]`.
- `partial_close(state, pos, leg_indices, reason, fees_close=0.0) -> Trade`
  - Closes only the specified legs. Mutates `pos` to drop those legs and reduce
    `entry_price_usd` / `fees_open`. Returns a `Trade` representing the closed legs
    only. Strategy keeps the mutated `pos` in its position list — the surviving legs
    continue marked-to-market under the original trade.
- `add_legs(pos, new_legs, new_entry_price_usd, new_fees_open) -> None`
  - Extends an open position with additional legs (e.g. a new weekly short for a
    calendar). Does NOT create a Trade; the strategy yields a separate `side='open'`
    Trade for the new legs.

### 2. Engine `_append_fills` (`engine.py`)

- Recognise `trade.metadata["partial_close"] = True` → look up the open trade_idx
  via `.get(pos_id)` instead of `.pop(pos_id)`, so the surviving legs retain their
  open-fill linkage for a future close.
- No new fields. Existing schema (`price_btc`, `amount_btc`, `fee_btc`, `spot`,
  `amount_usd`, `fee_usd`, `open_idx`) is sufficient.

### 3. Strategy migration

**`cal_premium_collect._roll_pair`** (the actual fix this refactor enables):

- If long drifted (full close):
  - `close_position(state, pair, "drift")` → 4 fills (2 open from entry, 2 close now)
  - Open a new pair with a new `side='open'` Trade.
- If long kept (partial close + extend):
  - Strategy annotates `exit_price_btc` on the short leg only.
  - `partial_close(state, pair, [short_idx], "roll")` → 2 fills (1 open from entry,
    1 close now for short leg only). Long leg untouched.
  - `add_legs(pair, [new_short_leg], new_entry_price_usd_for_new_short,
    new_fees_open_for_new_short)` extends the pair with the new short.
  - Strategy yields a `side='open'` Trade for the new short leg → 1 open fill.

**`cal_premium_collect._close_pair`** (end of run): one `close_position()` call.

**Other strategies** (`short_str_turb_dyn`, `long_gamma_whitelist`, etc.): all currently
do full closes only. Rename `close_trade` → `close_position` for consistency. No
behavior change.

### 4. Trade schema

No struct change. `Trade.entry_price_usd`/`exit_price_usd` now represent only the legs
included in this trade (which for full closes is all legs, identical to today).
`Trade.metadata["legs"]` carries the closed-legs subset.

### 5. Downstream impact

| Consumer | Impact | Action |
|---|---|---|
| `results.py` scoring | More trade rows for `cal_premium_collect` (each leg lifecycle is its own trade). Per-trade metrics shift; aggregate PnL unchanged. | Recompute composite scores; no code change. |
| `reporting/html_report.py` | Trade log table shows more rows; equity curve unchanged (NAV-driven). Fills table cleaner — no `$0.00` virtual rows. | None. |
| `detail_view.py` | Same. | None. |
| `tests/test_engine_fills_recon.py` | Per-trade reconciliation now strictly holds (no partial-close fudge). | Tighten tolerance from `$0.5` → `$0.01`. |
| `walk_forward.py`, `experiment.py` | Reads aggregate `GridResult`. | None. |

## Risk areas

1. **Trade count change for `cal_premium_collect`** — roughly 2× (one per leg lifecycle
   vs one per week). Composite score recomputation needed.
2. **PnL attribution timing** — long's mark-to-market gain crystallises only when the
   long is sold, not at each weekly roll. Cumulative PnL identical; per-trade
   attribution changes.
3. **NAV/equity curve** — unchanged, driven by `realized + unrealized`. The
   `_reprice_legs` fix already corrected the open-PnL marks.

## Phased execution

- **Phase A — API surface (this phase):**
  Add `close_position` / `partial_close` / `add_legs` in `strategy_base.py`. Support
  `partial_close=True` in `engine._append_fills`. Add unit tests covering full close,
  partial close, add_legs, and full-cycle reconciliation. No strategy changes yet.
  All existing tests must continue to pass.
- **Phase B — Migrate `cal_premium_collect`** to the new API. Remove `exit_price_btc`
  writes on kept legs. Add tests asserting kept long produces no fill / no trade row.
  Run backtest, compare cumulative PnL with pre-refactor.
- **Phase C — Migrate other BTC-native strategies** (`short_str_turb_dyn`,
  `long_gamma_whitelist`) to `close_position` for consistency.
- **Phase D — Remove dead code** in `engine._append_fills` (`skip_open_fill`,
  `fee_btc_close=0.0` overrides where no longer needed, `exit_price_btc` leg attribute
  for kept legs). Tighten reconciliation test.
- **Phase E — Audit remaining strategies** (`delta_strangle_tp`, `daily_put_sell`,
  etc.) not yet on BTC-native fills. Decide: migrate now or defer.

## Open questions

1. Long leg of a calendar = **one trade across all weeks** (entered Friday W1, closed
   Friday W5) vs **fresh trade each week** with explicit "rollover" semantics?
   **Decision (current plan):** one trade across all weeks — cleanest representation.
2. Strategy still tracks cost basis on the long for SL/TP logic. That metadata lives
   on `OpenPosition` already and is unaffected.
3. `partial_close` produces a new Trade for the closed legs; the surviving legs
   continue under the same `OpenPosition`. The original open trade (with pos_id)
   remains the anchor for fills linkage until ALL legs close.

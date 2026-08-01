# Long-Gamma Bull/Bear Discretionary-Whitelist Strategy — Spec

**Source package:** `backtester/newstrategy/coincall_signal_schedule_bull/`
(Executive Summary.docx, signal schedule CSVs, combined trades CSV, PDF report)

**Audience:** an AI agent (or human dev) building either:
- a CryoTrader **backtester strategy** under `backtester/strategies/`, or
- a CryoTrader **live strategy** under `strategies/`.

This document is the canonical extraction of the strategy design. It is
self-contained — no need to re-read the docx or PDF.

---

## 1. One-paragraph summary

A discretionary-gated, rules-triggered **long-gamma** options strategy on BTC.
A **human operator** must classify the broader market regime as **BULL**,
**BEAR**, or **SIDEWAYS** at all times. Only the whitelist matching the human
view is armed; SIDEWAYS arms neither sleeve (flat).

Two independent sleeves run on **4-hour UTC BTC candles** evaluated at bar
close. Each sleeve has its own regime filter (SMA stack), trigger filter
(EMA-cross or RSI zone-cross), instrument profile (delta / DTE), and pair-chain
exit logic. Entries fire only when **(human whitelist) ∧ (regime stack) ∧
(trigger event)** all align. Exits fire on the matching reverse trigger event
of the same chain.

Both sleeves buy options outright (long calls / long puts) — long premium, long
gamma, long vega. There is **no short-option leg, no hedging, no stop-loss in
price space** — the exit is purely the trigger-chain reversal (or option
expiry).

---

## 2. Inputs & timeframe

| Input | Value |
|---|---|
| Underlying | BTC spot (USD) |
| Bar size | **4h, UTC** |
| Evaluation point | **bar close** (no intrabar / no lookahead) |
| Indicators allowed at bar `t` | data through `t` inclusive |
| Human input | regime ∈ `{BULL, BEAR, SIDEWAYS}` — set externally, can change anytime |
| Whitelist (bull) | `#whitelist_bull_calendar_union_sweep.csv` — 190 UTC calendar dates (2024-10-24 → 2026-04-24) |
| Whitelist (bear) | `#whitelist_bear_calendar_union_sweep.csv` — 240 UTC calendar dates (2024-12-17 → 2026-04-01) |

The **whitelist is a human discretionary input** — a list of UTC dates the
operator decides are valid for that sleeve. SIDEWAYS days are simply not on
either whitelist. The two `#whitelist_*_calendar_union_sweep.csv` files in the
source package are the **authoritative historical whitelists** — they were
delivered by the strategy author and validated against all 46 reference trades
(100% match). In practice the human can either (a) maintain two date-sets and
let the engine gate on them, or (b) flip a single mode switch that the engine
maps to "today is bull-whitelisted / bear-whitelisted / both empty".

---

## 3. Sleeve A — BULLISH (long calls)

### 3.1 Universe gate
- `utc_date(bar_close) ∈ bull_whitelist`. If not, skip.

### 3.2 Regime filter — `dual_8_21`
- Compute SMA(8) and SMA(21) on 4h closes.
- Bull regime active at bar close iff `SMA8 > SMA21`.

### 3.3 Trigger filter — EMA(8) / EMA(21) crossover chain
- Compute EMA(8), EMA(21) on 4h closes (standard `ewm(span=N, adjust=False)`).
- **Entry event:** EMA8 crosses **above** EMA21 at close
  (i.e. `prev: EMA8 ≤ EMA21` → `curr: EMA8 > EMA21`).
- **Exit event:** EMA8 crosses **below** EMA21 at close.
- Pairing semantics (`pair_first_entry_then_next_exit`): scan bars in time
  order. Each entry event is independently paired with the **chronologically
  next exit event after it**. The exit index is **not advanced** after each
  pairing — multiple entry events that occur before the same exit bar all
  receive that exit bar as their close timestamp. This means multiple open
  positions per sleeve are possible simultaneously. There is **no hard
  concurrency cap** in the signal construction rules; a live risk cap is a
  separate deployment policy.

### 3.4 Per-pair filter
For each `(entry_ts, exit_ts)` pair from the EMA chain:
1. If `utc_date(entry_ts) ∉ bull_whitelist` → discard.
2. If `regime_dual_8_21(entry_ts) != bull` → discard.
3. Otherwise emit signal.

### 3.5 Instrument profile
| Field | Value |
|---|---|
| Side | **Long** |
| Type | **Call** |
| Target delta | **+0.70** |
| Target DTE | **≈ 11 calendar days** |
| Min DTE at entry | **≥ 2 days** (hard floor) |

Pick the listed option whose `(delta, dte)` is closest to target:
- Among all options with `dte ≥ min_dte`, pick the strike with the smallest
  `|delta − 0.70|`. If the chain has no entries (missing quotes), skip the pair.

### 3.6 Exit
- Close at bar close of `exit_ts` (the EMA8↓EMA21 cross), **or** at expiry,
  whichever comes first.
- In the supplied trades CSV, exit kinds are `bid` (closed at exit_ts via
  bid-side fill) or `expiry` (held to expiration because the EMA exit
  hadn't fired yet).

---

## 4. Sleeve B — BEARISH (long puts)

### 4.1 Universe gate
- `utc_date(bar_close) ∈ bear_whitelist`. If not, skip.

### 4.2 Regime filter — `dual_20_50`
- Compute SMA(20) and SMA(50) on 4h closes.
- Bear regime active at bar close iff `SMA20 < SMA50` (fast below slow).

### 4.3 Trigger filter — Wilder RSI(14) zone-cross chain
- Compute Wilder RSI with period 14 on 4h closes.
  (Wilder smoothing = `ewm(alpha=1/14, adjust=False)` of up/down moves.)
- **Entry event:** RSI crosses **upward through 55**
  (`prev: RSI ≤ 55` → `curr: RSI > 55`).
- **Exit event:** RSI crosses **downward through 45**
  (`prev: RSI ≥ 45` → `curr: RSI < 45`).
- Pairing: same `pair_first_entry_then_next_exit` semantics as the bull
  sleeve — each RSI entry event is independently paired with the next RSI
  exit event after it; multiple entries before the same exit bar each produce
  their own open position, all closing at that shared exit bar.

> Note the asymmetry: a bullish-momentum trigger (RSI crossing up) opens a
> **bearish** put position. The design rationale (per the executive summary)
> is that the bearish sleeve is gated by a heavy regime stack (`SMA20 < SMA50`
> + bear whitelist) and uses the RSI bounce into 55 as a **fade entry**;
> the put is held until RSI confirms momentum failure by losing 45.

### 4.4 Per-pair filter
For each `(entry_ts, exit_ts)` pair from the RSI chain:
1. If `utc_date(entry_ts) ∉ bear_whitelist` → discard.
2. If `regime_dual_20_50(entry_ts) != bear` → discard.
3. Otherwise emit signal.

### 4.5 Instrument profile
| Field | Value |
|---|---|
| Side | **Long** |
| Type | **Put** |
| Target delta | **-0.60** (i.e. `|Δ| ≈ 0.60`) |
| Target DTE | **≈ 21 calendar days** |
| Min DTE at entry | **≥ 2 days** |

Pick the listed option whose `(delta, dte)` is closest to target:
- Among all options with `dte ≥ min_dte`, pick the strike with the smallest
  `||delta| − 0.60|`. If the chain has no entries (missing quotes), skip the pair.

### 4.6 Exit
- Close at bar close of `exit_ts` (RSI↓45), or at expiry — same logic as bull.

---

## 5. Sideways / undecided

- Human sets `mode = SIDEWAYS`.
- Equivalent to loading **empty whitelists** for both sleeves.
- No signals emitted. The engine still ticks (compute regime/trigger state for
  audit logging) but issues nothing.

---

## 6. Capital & risk framing (from the report)

- One account, both sleeves merged chronologically.
- $100 000 starting capital baseline → max sequential drawdown ≈ **1.2 %**,
  CAGR mid-50 % range over the report window (Nov 2024 – Apr 2026).
- Bull sleeve standalone: ≈ 97 % CAGR, ≈ 1.1 % max DD on $100k bookkeeping.
- Quoted illustrative scaling: at $50k deployed capital, > 100 % CAGR with
  < 4 % drawdown.
- Position sizing in the supplied backtest is implicit (one contract / fixed
  qty per signal; fees of ≈ $26–$46 per trade visible in
  `trades_combined_bullbear_chrono_mai26.csv`). **Sizing rule must be
  specified by the implementer** — not in the spec.

---

## 7. Reference signals & trades (ground truth)

These files are the reference output from the original backtest. A correct implementation must
reproduce them (within fill-model tolerance).

### `coincall_signal_schedule_bull.csv`
```
pair_index, schedule_entry_bar_utc, schedule_exit_bar_utc
```
7 pairs total, Nov 2024 → Apr 2026.

### `coincall_signal_schedule_bear.csv`
```
pair_index, schedule_entry_bar_utc, schedule_exit_bar_utc
```
39 pairs total, same window.

### `trades_combined_bullbear_chrono_mai26.csv`
46 rows (= 7 + 39), with realised PnL, fees, fill timestamps, and
`exit_kind ∈ {bid, expiry}`. Net PnL on $100k starting equity is positive in
44 / 46 trades; two losers (one bull, one bear).

### `#whitelist_bull_calendar_union_sweep.csv`
190 UTC dates (2024-10-24 → 2026-04-24) on which the bull sleeve is armed.
Format: single `date` column, one ISO date per row.
**Authoritative — delivered by strategy author; validated against all 7 bull
trades.**

### `#whitelist_bear_calendar_union_sweep.csv`
240 UTC dates (2024-12-17 → 2026-04-01) on which the bear sleeve is armed.
Format: single `date` column, one ISO date per row.
**Authoritative — delivered by strategy author; validated against all 39 bear
trades.**

No dates appear on both lists simultaneously (zero overlap).

### `whitelist_gaps_neither_bull_nor_bear_oct2024.csv`
141 dates in Oct 2024 – early 2025 that are on neither whitelist
(SIDEWAYS days). Superseded by the two authoritative files above;
retained for reference.

### `whitelist_coverage_timeline_oct2024.png`
Visual of which days are bull-armed / bear-armed / sideways across the report
window. The pixel-extracted `daily_regime.csv` derived from this image agrees
with the authoritative whitelists on 567/571 days (99.3%) and is superseded.

---

## 8. Pseudocode (canonical)

```
INPUT:
  bars_4h_utc          # OHLC dataframe, UTC index, sorted ascending
  human_mode           # BULL | BEAR | SIDEWAYS
  bull_whitelist_dates # set[date]   (UTC calendar dates)
  bear_whitelist_dates # set[date]

OUTPUT:
  signals              # list of (leg, entry_ts, exit_ts, opt_profile)

if human_mode == SIDEWAYS:
    return []

precompute on closes:
  sma8, sma21, sma20, sma50
  ema8, ema21
  rsi14_wilder

if human_mode == BULL:
    pairs = pair_first_entry_then_next_exit(
        entries = timestamps_where(ema8 crosses above ema21),
        exits   = timestamps_where(ema8 crosses below ema21),
    )
    for (entry_ts, exit_ts) in pairs:
        if entry_ts.date() not in bull_whitelist_dates: continue
        if not (sma8[entry_ts] > sma21[entry_ts]):       continue
        emit(leg="bull_call", entry_ts, exit_ts,
             profile=OptionProfile(type="call", delta=+0.70,
                                   dte=11, min_dte=2))

elif human_mode == BEAR:
    pairs = pair_first_entry_then_next_exit(
        entries = timestamps_where(rsi14 crosses up   through 55),
        exits   = timestamps_where(rsi14 crosses down through 45),
    )
    for (entry_ts, exit_ts) in pairs:
        if entry_ts.date() not in bear_whitelist_dates: continue
        if not (sma20[entry_ts] < sma50[entry_ts]):      continue
        emit(leg="bear_put", entry_ts, exit_ts,
             profile=OptionProfile(type="put", delta=-0.60,
                                   dte=21, min_dte=2))

return signals
```

`pair_first_entry_then_next_exit(entry_events, exit_events)` semantics:
- Both lists are sorted ascending by timestamp.
- For each `entry_ts` in `entry_events`:
  - Find the first `exit_ts` in `exit_events` where `exit_ts > entry_ts`.
  - If found: emit `(entry_ts, exit_ts)`. **Do not remove** `exit_ts` from
    the pool — the same exit can match multiple entries.
  - If not found: discard (trailing unpaired entry).
- Result: multiple entry events that precede the same exit bar all produce
  separate pairs sharing that exit timestamp (fan-out pattern).

Reference implementation:
```python
def pair_first_entry_then_next_exit(entries, exits):
    pairs = []
    for ent in entries:
        for ext in exits:
            if ext > ent:
                pairs.append((ent, ext))
                break
    return pairs
```

---

## 9. Implementation contract for an AI agent

When asked to wire this into CryoTrader, follow these rules:

### 9.1 Where it lives
- **Backtester variant:** new file `backtester/strategies/long_gamma_whitelist.py`
  (or split into `..._bull.py` / `..._bear.py` if the existing strategies use
  one-leg-per-file). Register an alias in `backtester/run.py`.
- **Live variant:** new file `strategies/long_gamma_whitelist.py`. Use the
  `_p("NAME", default, cast)` env-var pattern (see `strategies/put_sell_80dte.py`
  as a reference). Register in `strategies/__init__.py`. Use
  `option_selection.LegSpec` / `find_option()` for chain selection.
- The backtester runs Deribit historical data; the live runtime can target
  Coincall (the package name suggests Coincall as the intended venue)
  or Deribit. Strike/DTE selection logic is venue-agnostic.

### 9.2 Required parameters (env-driven for the live strategy)
| Param | Bull default | Bear default |
|---|---|---|
| `BAR_TIMEFRAME` | `4h` | `4h` |
| `REGIME_FAST` | 8 | 20 |
| `REGIME_SLOW` | 21 | 50 |
| `TRIGGER_KIND` | `ema_cross` | `rsi_zone` |
| `EMA_FAST` | 8 | — |
| `EMA_SLOW` | 21 | — |
| `RSI_PERIOD` | — | 14 |
| `RSI_ENTRY_LEVEL` | — | 55 |
| `RSI_EXIT_LEVEL` | — | 45 |
| `OPTION_TYPE` | `call` | `put` |
| `TARGET_DELTA` | +0.70 | -0.60 |
| `TARGET_DTE_DAYS` | 11 | 21 |
| `MIN_DTE_DAYS` | 2 | 2 |
| `WHITELIST_PATH` | `backtester/newstrategy/coincall_signal_schedule_bull/#whitelist_bull_calendar_union_sweep.csv` | `backtester/newstrategy/coincall_signal_schedule_bull/#whitelist_bear_calendar_union_sweep.csv` |
| `HUMAN_MODE` | `BULL` \| `BEAR` \| `SIDEWAYS` | (single global switch) |

The human mode should be a **runtime-mutable switch** (e.g. a file or
dashboard toggle) — the executive summary explicitly states the human can
change view at any time. Practical wiring:
- A toggle in the hub/dashboard (`hub/`) writes a small JSON file
  (e.g. `data/human_mode.json`); the strategy reads it on each tick.
- Default to `SIDEWAYS` (safe).

### 9.3 Pair-chain state (live)
Each sleeve maintains a per-position state list across ticks:
- On every closed 4h bar, compute the indicators using bars **strictly through
  that close**, determine the event for that bar (entry / exit / none).
- **Entry event fires:** if whitelist + regime gates pass, open a new position.
  Do not block on existing open positions — fan-out is allowed.
- **Exit event fires:** close **all** open positions in this sleeve that have
  no earlier expiry override. Each position's actual close is
  `min(option_expiry_ts, signal_exit_ts)`.
- **Expiry:** when an option expires before the trigger exit fires, close that
  position at expiry. **Do not wait** for the trigger exit bar — expiry
  is a fully independent close path. The chain itself continues untouched;
  the next entry event after expiry will open a new position normally.
- There is no global "pending slot" — each open position is tracked
  independently with its own entry timestamp and option contract details.

### 9.4 Things NOT to implement (intentionally absent in the spec)
- No price-based stop-loss, no take-profit, no delta hedging, no scaling in/out.
- No multi-leg structures — both sleeves are single long options.
- No intrabar evaluation — bar-close only.

### 9.5 Tests to add (mirror existing pattern under `backtester/strategies/tests/`)
1. **Indicator parity:** SMA/EMA/RSI Wilder reproduce hand-computed values on a
   small fixture.
2. **Chain pairing / fan-out:** synthetic event sequence
   `[entry@t1, entry@t2, exit@t3, entry@t4, exit@t5]` produces exactly 3
   pairs: `(t1,t3)`, `(t2,t3)`, `(t4,t5)`. Verify that `t3` is reused for
   both `t1` and `t2`.
3. **Whitelist gate:** an EMA pair whose entry date is not on the whitelist is
   discarded.
4. **Regime gate:** an EMA pair where SMA8 ≤ SMA21 at entry is discarded.
5. **SIDEWAYS:** mode forces zero signals regardless of input.
6. **Reference replay:** feed a 4h BTC close series spanning one of the
   pairs in `coincall_signal_schedule_bull.csv` and assert the strategy emits
   that exact `(entry_ts, exit_ts)` pair.

---

## 10. Open questions for the operator

These are **not in the source package** and must be confirmed before live
deployment:

1. **Position sizing rule.** Fixed qty? % of equity? Fixed premium budget?
   The supplied trades imply ~0.1–1 BTC notional but it is not stated.
2. **Concurrency across sleeves.** Can bull and bear sleeves both be open at
   the same time on the same account? The combined ledger suggests yes
   (and they almost never overlap in practice because the human regime
   typically routes to one whitelist at a time), but the rule should be made
   explicit.
3. ~~**Whitelist source-of-truth in production.**~~ **Resolved.** The
   authoritative historical whitelists are
   `#whitelist_bull_calendar_union_sweep.csv` and
   `#whitelist_bear_calendar_union_sweep.csv`, delivered by the strategy
   author (May 2026). For live deployment the operator will need to extend
   or replace these files as new whitelist dates are decided.
4. **Venue.** The folder names "coincall_signal_schedule_*" suggest Coincall;
   live deployment must confirm whether the available option chain on the
   target venue (Coincall vs Deribit) supports the requested
   delta-0.70 / DTE-11 and delta-0.60 / DTE-21 instruments at all times.

# Long-Gamma Whitelist — Strategy Overview

**Status:** Implemented & backtested  
**Last updated:** 10 May 2026  
**Backtester strategy file:** [`long_gamma_whitelist.py`](long_gamma_whitelist.py)  
**Latest backtest report:** [`long_gamma_whitelist_20260510_153710.html`](long_gamma_whitelist_20260510_153710.html)

---

## Quick-start for humans and AI agents

This directory is the **single source of truth** for the Long-Gamma Whitelist strategy.
Everything needed to understand, reproduce, extend, or deploy the strategy is here.

| File | What it is |
|---|---|
| `STRATEGY_OVERVIEW.md` | **This document — read first** |
| `long_gamma_whitelist.py` | CryoTrader backtester strategy (copy of `backtester/strategies/`) |
| `long_gamma_whitelist_20260510_153710.html` | Latest backtester HTML report (432-combo grid, 10 May 2026) |
| `#whitelist_bull_calendar_union_sweep.csv` | **Authoritative** bull-sleeve whitelist (190 UTC dates) |
| `#whitelist_bear_calendar_union_sweep.csv` | **Authoritative** bear-sleeve whitelist (240 UTC dates) |
| `coincall_signal_schedule_bull.csv` | Quant reference: 7 bull signal pairs (ground-truth) |
| `coincall_signal_schedule_bear.csv` | Quant reference: 39 bear signal pairs (ground-truth) |
| `trades_combined_bullbear_chrono_mai26.csv` | Quant reference: 46 trades with PnL, fees, exit kinds |
| `Backtest_Report_Combined_BullBear_Chronological_...pdf` | Quant's original backtest PDF |
| `Executive Summary.docx` | Quant's written strategy brief |
| `_STRATEGY_SPEC_archived.md` | Original detailed spec (superseded by this document) |

---

## 1. What this strategy does — one paragraph

A **long-gamma, trend-following options strategy** on BTC. A human operator classifies
each calendar day as **BULL**, **BEAR**, or **SIDEWAYS** (encoded in the whitelist CSV
files). Two independent sleeves run in parallel on **4-hour UTC BTC candles**:

- **BULL sleeve** — buys calls when an EMA8/EMA21 golden-cross fires on a bull-whitelisted
  day where SMA8 > SMA21. Closes on the matching death-cross, or at option expiry.
- **BEAR sleeve** — buys puts when RSI(14) crosses up through 55 on a bear-whitelisted
  day where SMA20 < SMA50. Closes when RSI crosses back down through 45, or at expiry.

Both sleeves are long premium only — no short legs, no delta hedging, no price-space stops.
The exit is purely signal-driven (indicator reversal) or expiry-forced.

---

## 2. Inputs

| Input | Detail |
|---|---|
| Underlying | BTC spot (USD) |
| Bar timeframe | **4h, UTC** (Binance BTCUSDT candles) |
| Evaluation point | Bar **close** — no intra-bar lookahead |
| Exchange / data | Deribit options (Coincall for live deployment) |
| Human whitelist (bull) | `#whitelist_bull_calendar_union_sweep.csv` — 190 UTC dates, Oct 2024 – Apr 2026 |
| Human whitelist (bear) | `#whitelist_bear_calendar_union_sweep.csv` — 240 UTC dates, Dec 2024 – Apr 2026 |

The whitelists are **discretionary human input** — the operator decides which calendar
dates are valid for each sleeve. Zero dates overlap between the two lists. SIDEWAYS days
are on neither list; both sleeves are silent.

---

## 3. Indicator dependencies

Computed once per backtest run via the `long_gamma_regime` indicator
(`backtester/indicators/long_gamma_regime.py`), evaluated on BTCUSDT 4h bars:

| Column produced | Formula | Used by |
|---|---|---|
| `bull_armed` | `(SMA8 > SMA21) AND (bar_date ∈ bull_whitelist)` | BULL entry gate |
| `bear_armed` | `(SMA20 < SMA50) AND (bar_date ∈ bear_whitelist)` | BEAR entry gate |
| `ema_cross_up` | EMA8 crosses above EMA21 | BULL entry trigger |
| `ema_cross_down` | EMA8 crosses below EMA21 | BULL exit trigger |
| `rsi_cross_up55` | Wilder RSI(14) crosses upward through 55 | BEAR entry trigger |
| `rsi_cross_dn45` | Wilder RSI(14) crosses downward through 45 | BEAR exit trigger |

**Warmup:** 60 days of candles required before the first usable bar.

---

## 4. Signal construction — fan-out pairing

Signals are pre-computed once per parameter combo via `pair_signals(regime_df, mode)`.

**Pairing rule (`pair_first_entry_then_next_exit`):**
```
for each entry_ts in entry_events (ascending):
    exit_ts = first exit_event where exit_ts > entry_ts
    if found: emit (entry_ts, exit_ts)
    # exit_ts is NOT consumed — multiple entries can share the same exit
```

This produces a **fan-out** pattern: several entry events that occur before the same
exit bar each generate their own independent position, all closing at that shared exit
bar. There is no concurrency cap in the signal rules; live risk caps are a separate
deployment policy.

Both sleeves apply the same pairing logic. Entries without a subsequent exit are silently
discarded (trailing signals at end of data are not forced closed by the signal logic —
`on_end()` handles any residual positions at backtest termination).

---

## 5. BULL sleeve detail

| Parameter | Value |
|---|---|
| Entry event | EMA8 crosses above EMA21 |
| Entry gate | `bar_date ∈ bull_whitelist` AND `SMA8 > SMA21` |
| Exit event | EMA8 crosses below EMA21 (or option expiry, whichever is first) |
| Option type | **Long call** |
| Target delta | `+0.70` (quant reference; grid-tested 0.50–0.80) |
| Target DTE | `≈ 11 calendar days` (quant reference; grid-tested 7–15) |
| Min DTE at entry | `≥ 2 days` |
| Fill model | **Buy at ASK** (worst-case long entry); skip if ask = 0 |
| Exit fill | **Sell at BID**; fall back to mark if bid = 0; intrinsic at expiry |

---

## 6. BEAR sleeve detail

| Parameter | Value |
|---|---|
| Entry event | Wilder RSI(14) crosses **up** through 55 |
| Entry gate | `bar_date ∈ bear_whitelist` AND `SMA20 < SMA50` |
| Exit event | RSI crosses **down** through 45 (or option expiry, whichever is first) |
| Option type | **Long put** |
| Target delta | `-0.60` (absolute; quant reference; grid-tested 0.50–0.80) |
| Target DTE | `≈ 21 calendar days` (quant reference; grid-tested 14–27) |
| Min DTE at entry | `≥ 2 days` |
| Fill model | **Buy at ASK**; skip if ask = 0 |
| Exit fill | **Sell at BID**; fall back to mark if bid = 0; intrinsic at expiry |

> Design note: the BEAR sleeve uses a bullish-momentum trigger (RSI crossing **up**)
> to open a **bearish** put. The rationale (per the executive summary) is that the
> heavy regime stack (bear whitelist + SMA20 < SMA50) pre-qualifies the environment,
> and the RSI bounce into 55 is a **fade entry** — the put is held until momentum
> confirms failure by losing 45.

---

## 7. Execution model (backtester)

**Bar timing:** The regime DataFrame is indexed by 4h bar-**open** timestamps (Binance
convention). The strategy evaluates at the bar that just closed:

```
prev_bar_ts = state.dt − 4h
```

A signal on bar T is acted on when `state.dt = T + 4h` (i.e. at the next bar open,
which equals the close of bar T).

**Instrument selection:**
1. Find the listed expiry closest to `target_dte` within `[dte_min, dte_max]`.
2. In that expiry's chain, pick the strike with the smallest `|delta − target_delta|`.
3. Skip the signal if no valid chain or ask = 0.

---

## 8. Backtester integration

```bash
# Run with full param grid (432 combos)
python -m backtester.run --strategy long_gamma_whitelist

# Register alias: backtester/run.py
#   "long_gamma_whitelist": LongGammaWhitelist
```

**PARAM_GRID** sweeps:

| Parameter | Values swept |
|---|---|
| `mode` | `BOTH` (both sleeves) |
| `bull_target_delta` | 0.50, 0.60, 0.70, 0.80 |
| `bull_target_dte` | 7, 11, 15 |
| `bear_target_delta` | 0.50, 0.60, 0.70, 0.80 |
| `bear_target_dte` | 14, 21, 27 |
| `dte_min` | 2 |
| `dte_max` | 40, 60, 90 |

**Total:** 432 combos, 12,312 trades, 108,837 market states — completes in ~45s.

---

## 9. Backtest results summary (10 May 2026 run)

**Full report:** [`long_gamma_whitelist_20260510_153710.html`](long_gamma_whitelist_20260510_153710.html)

Key findings from the 432-combo grid:

- **Signal quality dominates.** Differences between combos are minor — the
  whitelist gating and indicator crossover timing are the primary profit drivers.
  Delta and DTE are second-order parameters.
- **Quant reference params** (bull δ=0.70 / DTE=11, bear δ=0.60 / DTE=21) sit
  comfortably in the middle of the performance distribution — not outliers.
- **Data window:** 2024-10-24 → 2026-04-24 (≈18 months).
- **Quant reference trades:** 46 total (7 bull + 39 bear); 44/46 profitable
  (2 losers: one bull, one bear).
- **Quant reference capital metrics (PDF):** ~$100k base, max sequential
  drawdown ≈ 1.2%, CAGR mid-50% range over the report window.

---

## 10. Ground-truth reference files

These files were delivered by the strategy author and are the authoritative
validation targets for any reimplementation:

| File | Content |
|---|---|
| `coincall_signal_schedule_bull.csv` | 7 bull signal pairs (entry/exit timestamps) |
| `coincall_signal_schedule_bear.csv` | 39 bear signal pairs (entry/exit timestamps) |
| `trades_combined_bullbear_chrono_mai26.csv` | 46 trades with PnL, fees, fill times, exit kinds |
| `#whitelist_bull_calendar_union_sweep.csv` | 190 bull-whitelist dates (authoritative) |
| `#whitelist_bear_calendar_union_sweep.csv` | 240 bear-whitelist dates (authoritative) |

A correct implementation must reproduce the 46 reference trades within fill-model
tolerance (ask/bid vs. theoretical fills). The fan-out pairing semantics were confirmed
by the strategy author — see `answers.md` for the full clarification thread.

---

## 11. Live deployment notes

- **Exchange:** Coincall (live); Deribit data used for backtesting.
- **Position sizing:** not specified in the reference design. Must be set by the
  implementer (e.g. fixed contract qty, % of equity).
- **Concurrency cap:** no cap in the signal rules. Apply a live risk policy separately.
- **Whitelist maintenance:** the operator must extend the two whitelist CSVs forward
  in time as the strategy runs live.
- **Live strategy file:** not yet created under `strategies/`. The backtester strategy
  (`long_gamma_whitelist.py`) is the reference implementation to port.

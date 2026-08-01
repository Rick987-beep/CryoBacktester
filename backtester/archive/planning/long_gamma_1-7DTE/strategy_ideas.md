# Long Gamma — 1–7 DTE Strategy Ideas

**Status:** Work in progress / idea dump  
**Last updated:** May 2026  
**Research source:** `IndicatorBench/research/intraday_options/` (90-day BTC weekday dataset, Apr 2025 – May 2026)

---

## Background

The previous `long_gamma_whitelist` attempt failed mainly because entry timing was naive — options were bought whenever IV conditions were met, ignoring the strong intraday IV seasonality that makes 1-DTE options 12 pp more expensive at 07:00 UTC than at 09:00 UTC. This strategy tries to exploit that seasonality and the NY-open volatility cluster.

Core idea: buy 1-DTE (possibly 2-3 DTE) OTM options in the London–NY overlap window when IV has already pulled back from its daily peak and realized moves are disproportionately large relative to cost.

---

## Key research numbers (see CSVs)

| Fact | Value |
|------|-------|
| Best single cell (combined score) | 12:00 UTC entry, 2–3 h hold, score 1.68–1.77 |
| Best call entry (directional score) | 09:00 UTC, 6 h hold, score 0.78 |
| Best put entry (directional score) | 12:00 UTC, 3 h hold, score 0.94 |
| 1-DTE call IV range intraday | 39.3% (23:00) → 51.3% (07:00) — 12 pp |
| 1-DTE put IV premium vs calls | +4–5 pp structural skew |
| Median time to ±2% move (all entries) | 15 h |
| Median time to ±2% (London 08–11 entry) | 10 h — p25 = 5 h |
| Median time to ±2% (Overlap 12–16 entry) | 17 h — p25 = 6 h |
| Probability of hitting ±2% within 96 h | 97.5% |

---

## 1. Option Selection

### DTE
- **Primary:** 1-DTE. IV is maximally time-sensitive — 12 pp intraday range means timing the entry to the post-peak window is high-value. Theta risk is real but the move window (London/NY) is well-defined.
- **Secondary:** 2–3 DTE as a lower-theta alternative when 1-DTE premium looks elevated relative to the session's IV level. 2- and 3-DTE IV is nearly flat intraday (< 4 pp range) so entry timing matters much less — they are the "safe" fallback.
- **Avoid:** 4–7 DTE for the core trade. Gamma is too low; you're paying for vega you don't need. Could be useful as a hedge leg only.

### Strike / Delta selection
- Target **30-delta OTM** (|delta| ∈ 0.25–0.35) — this is what the research was based on. Good balance of leverage vs cost.
- Avoid deep OTM (< 0.15 delta): too cheap in absolute terms but win rate collapses below 0.9% median move threshold.
- Avoid near ATM (> 0.40 delta): too expensive, IV-implied move already covers the expected move.

### Direction
- **Calls preferred** as the base instrument. Structurally cheaper (no put skew), upside bias in recent data (67.6% of entries go up first). Combined score is higher for calls (1.77 vs best directional call 0.78).
- **Puts as directional add-on only** — when there is a bearish signal (see entry conditions). Despite higher IV, puts achieve slightly better directional scores (0.94 vs 0.78) because BTC down moves in the 09–12 UTC window are faster.
- **Straddle / strangle?** The combined score of 1.68–1.77 at 12:00 UTC was computed as max(up, down) — i.e. it implicitly assumed a straddle. This is the most compelling pure-IV-edge trade but doubles the premium outlay. Worth exploring as a parameter variant.

### IV entry filter
- **Require 1-DTE call IV ≤ 48%** at entry time. This excludes the 07:00 peak (51.3%) and Asian noise hours. The ideal range is 45–48%.
- Reject if IV > 52% (IV spike / event risk — you'll overpay).
- Optional: require IV < rolling 5-day median for that hour (buys only when relatively cheap for the session).

### IV relative filter (idea, unvalidated)
- IV vs HV ratio: if current 1-DTE IV < some multiple of realized HV (e.g. IV/HV < 1.2), the option is cheap in absolute terms. May help avoid IV-elevated sessions.

---

## 2. Entry Conditions

### Time window (hard gate — non-negotiable from research)
- **Primary window: 09:00–13:00 UTC** (London morning through NY open)
- Best single hour for bang-for-buck is 12:00 UTC (score 1.77 at 2-3 h hold)
- 09:00 UTC is the best for longer holds (6–7 h, score 1.50–1.54)
- **Never enter 17:00–07:00 UTC** — either IV is at its peak (07:00) or moves are too slow (late NY / Asia)
- Weekdays only (Mon–Fri)

### Possible directional filters (ideas — need backtesting)
- **No directional filter (pure vol play):** Enter whenever time/IV conditions are met. Relies on the combined score > 1 observation. Simplest. Suitable for a straddle variant.
- **Supertrend filter:** Enter calls only when BTC supertrend is bullish (green). Enter puts only when bearish. Reduces to one-sided position but improves win rate for that leg.
- **Turbulence score:** Require turbulence to be elevated (above threshold) — entering long gamma when the market is already showing signs of instability. This is the inverse of how `short_str_turb_dyn` uses it.
- **Index move filter:** Require SPX/NDX pre-market move > X% (London session US pre-market) as a proxy for NY open volatility. May predict fast 2% hits.
- **BTC overnight range:** If BTC 00:00–08:00 range > 1.5%, the market is already in motion — enter on the call side only.

### IV entry signal
- Enter when IV has dropped ≥ 2 pp from the last 2-hour high (IV pullback signal). Avoids entering right at an IV spike.

---

## 3. Exit Conditions

### Primary: profit target (move-based)
- **Close when BTC has moved X% from entry** — this is the clean exit tied to the research.
  - 1.5–2% move target looks appropriate: p50 of time-to-2% in London/Overlap sessions is 10–17 h; at a 1.5% target, you exit faster.
  - Alternative: close when option mark price doubles (100% profit). Simpler and accounts for skew/vega.
  - Alternative: delta-based exit — when option delta exceeds 0.55 (now in-the-money, take profit).

### Secondary: time stop (theta protection)
- **Hard time stop at N hours before expiry.** For 1-DTE, theta accelerates rapidly in the last 4–6 hours. Consider exiting at:
  - 6 h before expiry if no significant move
  - Or end of NY session (22:00 UTC) — after that, moves slow and theta bites hard
- For 2-3 DTE, time stop is less urgent but still useful as a backstop.

### Stop loss
- **Close at 50% of premium paid.** Research shows score < 1 on a directional basis, meaning losing trades are common — cutting losses preserves capital for the next entry.
- Alternative: no hard stop (pure theta decay defines max loss). Valid only if position sizing is small enough that full premium loss is acceptable.
- **Never hold a 1-DTE through expiry hoping for a late move** — the 10-h median time-to-2% means holding past the close of the entry session is usually a loser.

### Compound / scaling
- If BTC moves 1% in the first 2 h and position is up, consider closing half and running the rest to the 2% target.
- Do not add to losing positions.

---

## 4. Open Questions / Ideas to Explore

- [ ] What is the right option: call, put, or straddle? Need to compare net P&L after premium for all three in the backtester.
- [ ] Is the 30-delta the right strike or should we look at 20-delta (cheaper) or ATM (higher win rate)?
- [ ] What is the actual fill cost on Deribit 1-DTE options? The research shows ~$295–368 median mark price; need to account for bid-ask spread (typically 5–15% for short-dated OTM).
- [ ] Should entry be time-triggered (e.g. enter at 09:00 if conditions met) or event-triggered (first 1-min candle that breaks a range)?
- [ ] Turbulence or supertrend as entry filter — does filtering improve expected value or just reduce trade count?
- [ ] Walk-forward: the 90-day research window is entirely in one regime (BTC ~80–100k). Need to test on a longer history when data is available.
- [ ] Interaction with `short_str_turb_dyn`: this strategy is the inverse. Can they run simultaneously (offsetting gamma) or should they be mutually exclusive?

---

## 5. Param Grid Ideas (for backtester)

```python
PARAM_GRID = {
    "entry_hour_start":    [8, 9, 10, 11, 12],      # earliest UTC entry hour
    "entry_hour_end":      [12, 13, 14],             # latest UTC entry hour (inclusive)
    "dte":                 [1, 2, 3],                # target DTE at entry
    "delta_target":        [0.25, 0.30, 0.35],       # target delta OTM
    "direction":           ["call", "put", "straddle"],
    "iv_max_entry":        [48.0, 50.0, 52.0],       # reject if 1DTE call IV above
    "profit_target_pct":   [75, 100, 150],           # % gain on option mark price
    "stop_loss_pct":       [40, 50, 60],             # % loss on option mark price
    "time_stop_h_before":  [4, 6, 8],               # exit N hours before expiry
}
```

---

## 6. Reference Files (this folder)

| File | Contents |
|------|----------|
| `entry_score_top10.csv` | Top 10 combined bang-for-buck entry/hold combos |
| `directional_score_top.csv` | Top calls and puts scored per direction |
| `iv_intraday_summary.csv` | Median 1-DTE call/put IV by hour (1-DTE, 2-DTE, 3-DTE) |
| `iv_vs_move.csv` | IV vs realized move by entry hour |
| `time_to_2pct_summary.csv` | Time to ±2% by session window (London fastest) |
| `best_entry_summary.csv` | Broad best-entry by hold period |

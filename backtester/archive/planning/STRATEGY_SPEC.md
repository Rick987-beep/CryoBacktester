# BTC Long Strangle Strategy — Specification v1.0

*Empirically derived 17 May 2026 from V2 analysis (Jan–May 2026 Deribit options data).*  
*See `V2_PLAN.md` for the full research trail. This document is the actionable handover.*

---

## 1. Overview

Two independent entry signals (`pullback` and `vol_burst`) each trigger a strangle
purchase on BTC Deribit options. The signals are regime-gated, have different expiry
targets, and use different exit rules. They are traded independently — a fire on one
does not affect the other.

| | pullback | vol_burst |
|---|---|---|
| Frequency | ~3.0/wk in-regime | ~5.8/wk in-regime |
| Expiry | DTE 2 | DTE 1 |
| Delta target | δ ≈ 0.30 | δ ≈ 0.35 |
| Median cost | ~$1,112/strangle | ~$829/strangle |
| Take-profit | 2.0× | none |
| Time-stop | 20h | 4h |
| Stop-loss | none | none |
| E[$] per fire | +$160 | +$211 |
| Win rate | 45% | 48% |

---

## 2. Data requirements

| Data | Resolution | Source |
|---|---|---|
| BTCUSDT price + volume | 1h OHLCV | Binance Spot (or equivalent) |
| BTC options chain | 5-min bars | Deribit (bid, ask, mark, delta, IV) |

The indicator computation requires a minimum of **30 days of 1h OHLCV history** before
any signal is valid (rv_rank warmup = 720 bars).

---

## 3. Indicator definitions

All indicators are computed on 1h BTCUSDT OHLCV bars. All backward-looking — no
lookahead. "Bar t" means the bar that just closed, triggering the signal check.

### 3.1 `ret_1h` — 1-hour close-to-close return

```
ret_1h[t] = (close[t] / close[t-1] - 1) × 100   (%)
```

### 3.2 `ret_4h` — 4-hour return (closed-bar HTF, no lookahead)

Resample 1h bars to 4h (session-aligned, e.g. midnight UTC). At each 1h bar, use the
**most recently completed** 4h candle's close-to-close return. A 4h candle that has not
yet closed at bar t is never used.

```
df4h      = resample(df1h, "4h")          # 4h OHLCV, closed bars only
ret_4h_htf = df4h["close"].pct_change() × 100
ret_4h[t]  = ret_4h_htf value at the last 4h close ≤ bar t's timestamp
```

Implementation note: use a forward-fill / left-join from the 4h series onto the 1h index
(standard `htf_align` / `merge_asof` pattern). Never use a partially-formed bar.

### 3.3 `rv_24h` — 24-hour annualised realised volatility

```
log_ret[t] = ln(close[t] / close[t-1])
rv_24h[t]  = std(log_ret[t-23 … t], ddof=1) × sqrt(8760) × 100   (%, annualised)
```

Requires 24 consecutive bars. Use `min_periods=24` (produce NaN until warm).

### 3.4 `rv_rank` — 30-day percentile rank of realised vol

```
rv_rank[t] = percentile_rank(rv_24h[t]  within  rv_24h[t-719 … t])
```

Window: 720 bars = 30 days × 24h. Range: 0.0 (lowest vol in 30d) → 1.0 (highest).  
Use `min_periods=360` (allow signal after 15 days of warmup; full 720 required for stable rank).

### 3.5 `vol_z` — volume z-score (24-bar rolling)

```
vol_mean[t] = rolling_mean(volume[t-23 … t])
vol_std[t]  = rolling_std(volume[t-23 … t], ddof=0)
vol_z[t]    = (volume[t] - vol_mean[t]) / vol_std[t]
```

If `vol_std == 0`, treat as NaN (no signal).

---

## 4. Signal conditions

### 4.1 `pullback`

A strong 4h trend interrupted by a 1h counter-move, in a high-vol regime.

```
bull_pullback = (ret_4h >= +1.0%)  AND  (ret_1h <= -0.5%)
bear_pullback = (ret_4h <= -1.0%)  AND  (ret_1h >= +0.5%)

pullback = (bull_pullback OR bear_pullback)
           AND  rv_rank >= 0.60
           AND  day_of_week != Saturday
```

- `bear_pullback` historically stronger (76% vs 68% mag@2.5%), but both are traded.
- Saturday exclusion: removes a structural noise regime (~12% mag@2.5% on Saturdays).

### 4.2 `vol_burst`

A volume spike coinciding with a high-vol regime.

```
vol_burst = (vol_z >= 1.5)
            AND  rv_rank >= 0.60
            AND  day_of_week != Saturday
```

---

## 5. Regime filter and stand-aside rule

Do not enter any trade when:

```
rv_rank has been continuously below 0.35 for ≥ 12 consecutive 1h bars
```

This identifies calm/squeeze regimes where the magnitude hit rate collapses (~21% at 2.5%
vs 56% historical). Re-enable once rv_rank rises back above 0.35.

---

## 6. Cooldown

After any **pullback** signal fires (and a trade is taken), suppress the next `pullback`
signal for **4 hours**.

After any **vol_burst** signal fires (and a trade is taken), suppress the next `vol_burst`
signal for **4 hours**.

The two signals are independent — a `vol_burst` cooldown does not affect `pullback` and
vice versa.

---

## 7. Position management

- **One open position per signal tier** at a time. If a new signal fires while a same-tier
  strangle is still open, skip it.
- `pullback` and `vol_burst` positions can be held simultaneously (different strangles,
  potentially different expirations).

---

## 8. Option selection

### 8.1 Target expiry

At signal fire time (bar close), compute the target expiry date:

```
target_expiry_date = floor(fire_date) + DTE_target  (calendar days)
target_expiry_dt   = target_expiry_date at 08:00 UTC
```

Where `DTE_target = 2` for pullback, `DTE_target = 1` for vol_burst.

Find the nearest Deribit expiry at or after `target_expiry_dt` that satisfies:
```
hours_to_expiry = (expiry_dt - fire_ts) / 3600  ≥  4.0 hours
```

If no such expiry exists, skip the trade on this fire.

**Fallback**: if the target DTE is unavailable (hours_to_expiry < 4h), try the next
calendar-day expiry. Do not trade a DTE ≥ 4 as fallback — skip instead.

### 8.2 Strike selection (independent OTM strikes per leg)

Select each leg's strike independently by minimising delta distance from the target:
- pullback: `delta_target = 0.30`
- vol_burst: `delta_target = 0.35`

This is a **strangle**, not a straddle — the call and put are OTM on opposite sides of spot.

```
call_strike = argmin |call_delta - (+delta_target)|  across call options for this expiry
put_strike  = argmin |put_delta  - (-delta_target)|  across put  options for this expiry
```

Delta is taken from the real-time options chain at signal fire time. `call_strike > spot > put_strike` in the typical case.

### 8.3 Availability filter

Accept the trade only if both legs pass:
```
ask_price_usd  ≥  $75.00   (per leg)
spread_pct     ≤  30%       (= (ask - bid) / ask per leg)
hours_to_expiry ≥ 4.0 hours
```

If either leg fails, skip the trade on this fire.

### 8.4 Entry fill

```
entry_cost_usd = call_ask_usd + put_ask_usd   (conservative — pay the ask)
```

---

## 9. Exit rules

### 9.1 pullback — DTE 2, δ ≈ 0.30

| Rule | Condition | Action |
|---|---|---|
| Take-profit | `strangle_bid_usd ≥ 2.0 × entry_cost_usd` | Exit immediately at `strangle_bid` |
| Time-stop | `time_since_entry ≥ 20h` | Exit at market (`strangle_bid`) |
| Expiry stop | `hours_to_expiry ≤ 1h` | Exit at market — mandatory safety exit |
| Stop-loss | — | Not used (SL reduces EV; positions that dip recover) |

Priority: take-profit is checked first at each 5-min bar. If both TP and time-stop would
fire on the same bar, TP takes priority.

### 9.2 vol_burst — DTE 1, δ ≈ 0.35

| Rule | Condition | Action |
|---|---|---|
| Take-profit | — | Not used (holding to time-stop captures higher EV) |
| Time-stop | `time_since_entry ≥ 4h` | Exit at market (`strangle_bid`) |
| Expiry stop | `hours_to_expiry ≤ 1h` | Exit at market — mandatory safety exit |
| Stop-loss | — | Not used |

With DTE=1 and a 4h time-stop, the expiry stop is a backup safety net only (should
rarely fire if the availability filter (≥4h) is applied at entry).

### 9.3 Exit fill

```
exit_proceeds_usd = call_bid_usd + put_bid_usd   (conservative — hit the bid per leg)
strangle_bid_usd  = call_bid_usd + put_bid_usd   (alias used in exit conditions above)
```

---

## 10. Fees

Use the Deribit taker fee schedule per leg:

```
fee_per_leg_usd = min(
    0.0003 × btc_index_price_usd,      # 0.03% of underlying notional
    0.125  × mark_price_usd            # 12.5% of option premium cap
)
round_trip_fee_usd = (fee_per_leg_usd_call + fee_per_leg_usd_put) × 2  (entry + exit)
```

Both legs × entry and exit = 4 fee events per strangle.

---

## 11. P&L calculation

```
net_pnl_usd = exit_proceeds_usd - entry_cost_usd - round_trip_fee_usd
net_multiple = exit_proceeds_usd / entry_cost_usd    (ignoring fees, for comparison)
```

---

## 12. Expected performance

From empirical backtesting on 54 pullback and 116 vol_burst fires (Jan–May 2026):

### 12.1 pullback (DTE=2, δ=0.30, TP=2.0×, TS=20h, no SL)

| Metric | Value |
|---|---|
| N fires | 53 |
| Mean exit multiple | 1.09× |
| E[$] per fire (gross, pre-fee) | +$160 |
| Win rate (exit ≥ 1.0×) | 45% |
| TP hit rate | 26% |
| Time-stop exit rate | 74% |
| Median strangle cost | ~$1,112 |

At 3.0 fires/week: ~$480/week gross. Fees ≈ $8–15 per strangle round-trip.

### 12.2 vol_burst (DTE=1, δ=0.35, no TP, TS=4h, no SL)

| Metric | Value |
|---|---|
| N fires | 116 |
| Mean exit multiple | 1.35× |
| E[$] per fire (gross, pre-fee) | +$211 |
| Win rate (exit ≥ 1.0×) | 48% |
| TP hit rate | 0% (no TP) |
| Time-stop exit rate | 98% |
| Median strangle cost | ~$829 |

At 5.8 fires/week: ~$1,225/week gross.

### 12.3 Combined weekly gross EV (in-regime, both signals active)

~$1,700/week gross, from ~8.8 strangles/week at average ~$950 each.

---

## 13. Regime context and limitations

- All metrics are measured during **high-vol regimes (rv_rank ≥ 0.60)**.
- In low-vol calm periods (rv_rank < 0.40), the stand-aside rule suppresses signals. Do
  not expect any trades during extended flat/squeeze periods.
- Sample sizes are moderate: ±14pp 95% CI on win rate for pullback (N=53), ±9pp for
  vol_burst (N=116). The primary purpose of Phase 4 was to **eliminate bad configurations**
  and identify candidate parameters — not to fit precise numbers.
- Final parameter validation requires live paper trading (Phase 5).

---

## 14. Key parameters at a glance (tunable)

| Parameter | pullback | vol_burst | Rationale |
|---|---|---|---|
| rv_rank threshold | 0.60 | 0.60 | Top 40% vol regime gate |
| vol_z threshold | — | 1.5 | Volume spike (1.5 std above 24h mean) |
| ret_4h threshold | ±1.0% | — | Strong 4h trend |
| ret_1h threshold | ∓0.5% | — | Counter-move |
| DTE target | 2 | 1 | Empirically optimal |
| Delta target | 0.30 | 0.35 | Empirically optimal |
| Min ask per leg | $75 | $75 | Liquidity filter |
| Max spread | 30% | 30% | Liquidity filter |
| Min hours to expiry | 4h | 4h | Execution safety |
| TP multiple | 2.0× | none | From 11d grid search |
| Time-stop | 20h | 4h | From 11d grid search |
| Cooldown | 4h | 4h | Prevents clustering |
| Stand-aside threshold | rv_rank < 0.35 for 12h | same | Regime filter |

---

## 15. Open questions for the backtester

1. **Position sizing**: spec assumes 1 strangle per fire. Size to a fixed $ amount per
   trade or a fixed % of capital?
2. **Simultaneous positions**: currently spec'd as max 1 open per signal tier (skip if
   open). Test also: allow stacking (up to e.g. 3 per tier) to measure capacity.
3. **TP on vol_burst**: no TP was empirically better. Consider testing TP=2.5× as a
   defensive cap against the rare scenario where bid collapses before 4h.
4. **Delta drift**: the delta drifts after entry (move in-/out-of-the-money). Current
   spec does not rebalance. No rebalancing is consistent with the analysis.
5. **Both signal simultaneous fires**: 0.5/wk on average (very rare). Current spec opens
   both strangles (pullback DTE-2 + vol_burst DTE-1). Reasonable to keep both.

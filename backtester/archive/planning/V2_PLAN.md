# V2 Option Signal Discovery — Agent Brief

## Purpose of this document
Handoff brief for a new AI agent session continuing the V2 intraday options analysis.
**Do not start coding without the user explicitly asking. Present a plan first.**

---

## Context: what V1 did and why V2 is different

### V1 (archived in `v1_mtf_4h_results/`)
- Philosophy: "find a 4h momentum signal → trade an option on it"
- Entry granularity: every 4h UTC
- Result: 33% win rate, −24% EV — signal was weak and assumed upfront

### V2 philosophy: winners-first, signal-second
- "Find all moments where options became 2× winners → discover what entry conditions predicted them"
- Signal is discovered from data, not assumed
- Entry granularity: every 1h UTC (24 windows/day)

---

## Current workspace state

### Scripts (all in `research/long_tradable_options/`)
| File | Status | Purpose |
|---|---|---|
| `01_v2_scan_1h.py` | ✅ Complete | Universe scan — generates the two parquets below |
| `02_v2_enrich.py` | ❌ Not written | **Next step**: enrich candidates with entry features |
| `03_v2_signal_discovery.py` | ❌ Not written | After enrichment: lift analysis per feature |

### Data files (all in `research/long_tradable_options/`)
| File | Rows | Notes |
|---|---|---|
| `candidates_1h.parquet` | 123,715 | All evaluated options; base scan output |
| `winners_2x_1h.parquet` | 27,405 | Subset: peak bid ≥ 2× entry ask within 24h |
| `winner_peaks.parquet` | 15,347 | V1 lifetime peak data — keep for DTE/delta reference |
| `vol_regime_candidates.parquet` | — | V1 vol regime data — keep for sanity checks |

### Option data (in `research/intraday_options/option_data/`)
- `options_YYYY-MM-DD.parquet`: 188K rows/day, 288 × 5-min bars, ~653 contracts/day
  - Columns: `timestamp` (int64 µs), `expiry`, `strike`, `is_call`, `underlying_price`, `bid_price`, `ask_price`, `mark_price`, `mark_iv`, `delta` (float32/bool)
- `spot_YYYY-MM-DD.parquet`: commercial provider ≤ 2026-04-26, 1441 rows, 1-min OHLC
- `spot_track_YYYY-MM-DD.parquet`: self-recorder ≥ 2026-04-27, 497 rows, ~3-4 min intervals, same schema
- `option_utils.py` has `load_spot_day()` with automatic fallback from `spot_*` to `spot_track_*`

### Scan config (from `01_v2_scan_1h.py`)
```python
DATE_START    = date(2026, 1, 1)
DATE_END      = date(2026, 5, 12)   # 132 days
ENTRY_HOURS   = list(range(24))     # every UTC hour
FORWARD_HOURS = 24
WIN_FACTOR    = 2.0
DTE_MIN       = 1
DTE_MAX       = 7
DELTA_TARGETS = [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40]
DELTA_TOL     = 0.04
MIN_ENTRY_USD = 75.0
FEE_RATE_BTC  = 0.0003
FEE_CAP_RATIO = 0.125   # fee = min(0.0003, 0.125 × mark_btc) per leg
```

### `candidates_1h.parquet` columns (complete list)
```
entry_ts          — "YYYY-MM-DD HH:MM UTC"
entry_ts_us       — int64 µs epoch
contract          — option contract code string
expiry            — expiry code string
strike            — float
is_call           — bool
dte_at_entry      — int
delta_at_entry    — float
entry_ask_usd     — float
entry_ask_btc     — float
entry_bid_usd     — float
entry_iv          — float (mark_iv at entry)
entry_spot_usd    — float
spread_pct        — (ask − bid) / ask
tradeable         — bool (True if peak_multiple_24h >= 2.0)
skip_reason       — str or None
peak_multiple_24h — float (peak bid / entry ask over 24h window)
peak_hold_hours   — float (hours from entry to peak bid)
peak_spot_usd     — float (spot at time of peak bid)
spot_move_pct     — float (spot % change entry → peak)
abs_spot_move_pct — float
```

### `winners_2x_1h.parquet` adds
```
peak_bid_usd      — float
entry_fee_usd     — float
exit_fee_usd      — float
rt_fee_usd        — float (round-trip fee)
net_pnl_usd       — float
```

### Scan summary stats
- Base rate (2× within 24h): **22.2%** over full 132-day period
- By DTE: DTE 2 has most winners (8,088), DTE 1 next (6,172), drops sharply at DTE 5+
- By delta: <0.20 dominates (11,794), steep drop above 0.25

---

## Next task: `02_v2_enrich.py`

### Purpose
Enrich `candidates_1h.parquet` with entry-context features needed for signal discovery.
Write output to `candidates_1h_enriched.parquet`.

### Feature set to compute

| Feature | Definition | Source |
|---|---|---|
| `entry_hour_utc` | Hour of entry (0–23) | from `entry_ts` |
| `entry_day_of_week` | 0=Mon … 6=Sun | from `entry_ts` |
| `spot_1h_chg_pct` | (spot_entry / spot_1h_ago − 1) × 100 | `load_spot_day()` |
| `spot_4h_chg_pct` | (spot_entry / spot_4h_ago − 1) × 100 | `load_spot_day()` |
| `spot_1h_accel` | spot_1h_chg_pct − prior_1h_chg_pct | requires two lookups |
| `spot_vs_24h_ema` | (spot_entry / 24h_ema − 1) × 100 | rolling EMA over spot day |
| `atm_iv_at_entry` | mark_iv of nearest-to-spot call, DTE 1→2→3 | `load_day()` |
| `iv_30d_pct_rank` | percentile rank of atm_iv vs prior 30 calendar days | requires iv history |
| `iv_hv_ratio` | atm_iv / hv_1d | derived |
| `hv_1d` | 1-day realized vol from hourly spot returns | from spot data |

### Notes on implementation
- `entry_ts_us` is the int64 µs timestamp — use it for all lookups
- `load_spot_day(date_str)` handles both `spot_*` and `spot_track_*` automatically
- `load_day(date_str)` returns option chain for one day
- `ou.parse_expiry(code)` → datetime at 08:00 UTC
- For `iv_30d_pct_rank`: collect ATM IV for each day in the prior 30 days; `iv_30d_pct_rank` = fraction of those days where IV was lower than current IV
- For `hv_1d`: use the last 24 hourly log-returns of spot, annualise with `× sqrt(8760)`
- For `spot_1h_accel`: requires spot at T−1h and T−2h (i.e., two prior-1h deltas)
- For `spot_vs_24h_ema`: compute EMA of the last 24h of spot closes, then (spot / ema − 1) × 100
- The existing `02_enrich_context.py` (v1) has reference implementations for spot lookups and ATM IV that can be adapted. **Do not modify that file** — it is archived.

### Performance
- 123,715 rows × several lookups each
- Group by `entry_ts_us // (86400 × 1e6)` (i.e. date) and load spot/option day once per date, then vectorise within-day lookups
- Expect ~3-5 minutes total with day-level caching (LRU or dict)

### Output
`candidates_1h_enriched.parquet` — same rows as `candidates_1h.parquet`, with feature columns appended.

---

## Task after enrichment: `03_v2_signal_discovery.py`

**Do not proceed to this step without the user explicitly reviewing the enriched data.**

### Purpose
For each feature in the feature set, compute the 2× hit rate at each decile (or bucket) and compare against the unconditional base rate (22.2%). Report lift = hit_rate / base_rate. Identify features where any bucket shows lift ≥ 1.5×.

### Baseline to beat
| DTE | Unconditional 2× rate |
|---|---|
| 1 | 27.5% |
| 2 | 34.6% (primary target) |
| 3 | 37.4% |
| 4 | 39.6% |
| overall | 22.2% |

---

## Phase 3: Spot-Signal Discovery (BTCUSD chart-based)

**Decision (17 May 2026):** Drop options-level analysis for entry signal discovery.
Option selection parameters (DTE 1-2, delta 0.15-0.40, ask > $100) are already settled from Phase 2.
The entry *timing* signal is a pure BTCUSD price/vol question and should be treated as such.
This makes the result directly Pine Script-portable and avoids options microstructure noise.

### Script
`research/long_tradable_options/06_v2_spot_signals.py`

### Data
- Source: `pineforge/data/BTCUSDT_1h.parquet` via `pineforge.data.load()` — no new downloads needed.
- Resample to 4h / 1d on the fly with `pineforge.data.resample()` + `pineforge.data.htf_align()` (closed-bar safe).
- **Analysis window: 2025-01-01 → 2026-05-15 only.** BTC changes regime too frequently; pre-2025 data adds noise, not signal.

### Train / test split
| Set | Window | ~Hours |
|---|---|---|
| Train | 2025-01-01 → 2025-09-30 | ~6,500 |
| Test | 2025-10-01 → 2026-05-15 | ~5,400 |

Features are **selected only on the train set**. AUC on the test set confirms generalization.
The test window overlaps with the options analysis period (Jan–May 2026).

### Outcome variables (three targets, threshold tunable at 2.0 / 2.5 / 3.0%)

| Target | Definition |
|---|---|
| `mag_win` | `max(fwd_24h_high − close, close − fwd_24h_low) / close ≥ thresh` — big move either direction |
| `call_win` | `fwd_24h_high ≥ close × (1 + thresh)` — upside breakout |
| `put_win`  | `fwd_24h_low ≤ close × (1 − thresh)` — downside breakout |

### Feature set

Features are computed from 1h OHLCV only, using `pineforge.ta.*` primitives. No lookahead.

| Feature | Formula | Rationale |
|---|---|---|
| `ret_1h` | `close.pct_change()` | immediate 1h momentum |
| `ret_4h` | `htf_align` 4h close return | medium-term direction |
| `ret_1d` | `htf_align` 1d close return | daily trend context |
| `accel_1h` | `ret_1h − ret_1h.shift(1)` | momentum acceleration / reversal |
| `close_vs_ema24` | `(close / ema(24) − 1) × 100` | short mean-reversion distance |
| `close_vs_ema168` | `(close / ema(168) − 1) × 100` | weekly mean-reversion distance |
| `rv_24h` | rolling 24-bar annualised log-return std | realized vol |
| `rv_rank` | rolling 30d percentile of `rv_24h` | vol regime rank (≡ IV rank proxy; #1 predictor from options analysis) |
| `rv_trend` | `rv_24h − rv_24h.shift(24)` | is vol expanding into this entry? |
| `bb_width` | `(bb_upper − bb_lower) / bb_mid × 100` (20-bar, 2σ) | squeeze-then-explosion precursor; directly predicts magnitude moves |
| `vol_z` | `(volume − vol_mean24) / vol_std24` | relative volume spike; elevated vol co-signals large BTC moves |
| `range_ratio` | `(high − low) / close ÷ rolling24_mean((high−low)/close)` | is this bar expanding range vs recent average? |
| `hour_utc` | `index.hour` | session effect (US open, Asia) |
| `day_of_week` | `index.dayofweek` | weekly seasonality |

**Dropped from original proposal (with reason):**
- `adx_4h` — measures trend *strength*, not move *magnitude probability*. No direct link to ≥2.5% outcome.
- `donch_pos` — channel breakout logic; burst research showed this doesn't discriminate at 15m, even less so at 24h horizon.
- `atr_norm` — redundant; `rv_rank` captures the same vol-regime signal more cleanly.

### Statistical sections

**A — Feature AUC table**
Mann-Whitney U → AUC per feature × 3 targets. Bonferroni-corrected p-values. Threshold: AUC > 0.55, p < 0.005.

**B — Bucket win rates**
Each continuous feature binned into 10 quantiles. Win rate per bucket (non-linear sweet spots like the 66% MTF pullback pattern can only be found this way).

**C — Train/test validation**
Re-run AUC on test set for any feature that passes in train. Flag if direction reverses (overfit).

**D — Combination table**
Top 3-4 features combined into human-readable conditions. Format matches `04_kernel_combos.csv`.

### Outputs
- `06_feature_auc.csv` — AUC / p-value per feature × 3 targets
- `06_bucket_winrates.csv` — bucket-level win rates
- `06_train_test_validation.csv` — train vs test AUC
- `06_conditions.csv` — top combination conditions

### Pine Script path
Every feature in the table above is a native Pine primitive (EMA, Bollinger Bands, volume, pct_change).
The final validated conditions translate directly into indicator inputs/alerts.

---

---

## Phase 3 Results — Entry Signal Findings  *(17 May 2026)*

### Analysis window
- Full history: **2025-01-01 → 2026-05-15** (11,977 bars; train/test split at 2025-09-30)
- Recent check: **2026-04-26 → 2026-05-15** (~465 bars, 2.7 weeks) — regime snapshot

### Base rates (full window)

| Threshold | mag_win | call_win | put_win |
|---|---|---|---|
| 1.5% | 73.9% | 41.1% | 42.4% |
| 2.0% | 58.0% | 30.2% | 32.0% |
| **2.5%** | **44.0%** | **21.6%** | **24.2%** |
| 3.0% | 32.8% | 15.5% | 18.1% |
| 3.5% | 24.2% | 11.5% | 13.1% |

Saturday is a strong noise source (mag_win@2.5% = 12.4%). All conditions below exclude Saturday.
The **primary threshold** is 2.5% (maps to ~2.5% OTM options, the most liquid near-money strikes).

---

### Named entry conditions

Each condition has a short name used for charts and Pine Script.

#### `vol_regime`
> rv_rank ≥ 0.60  +  no Saturday

Rolling 30-day realized-vol percentile in the top 40%. The single most predictive feature
(AUC 0.60 on test set). Filters to hours where BTC is already in an elevated vol environment.

| fw/wk | 1.5% | 2.0% | **2.5%** | 3.0% | 3.5% | call | put | type |
|---|---|---|---|---|---|---|---|---|
| 58.0 | 83% | 69% | **56%** | 44% | 34% | 26% | 34% | straddle |

---

#### `vol_burst`
> vol_z ≥ 1.5  +  rv_rank ≥ 0.60

Volume spike (z-score > 1.5) coinciding with a high-vol regime. The key sweet-spot condition.
Trade frequency sits in the 3-7/wk target band. Low false-signal rate.

| fw/wk | 1.5% | 2.0% | **2.5%** | 3.0% | 3.5% | call | put | type |
|---|---|---|---|---|---|---|---|---|
| 6.3 | 87% | 73% | **61%** | 47% | 36% | 35% | 33% | straddle |

Tighter version: `vol_surge` (vol_z ≥ 2.0 + rv_rank ≥ 0.60) fires 4.2/wk with 63% at 2.5%.

---

#### `pullback`
> (4h ≥ +1% + 1h ≤ -0.5%)  OR  (4h ≤ -1% + 1h ≥ +0.5%)  +  rv_rank ≥ 0.60

Multi-timeframe momentum pullback: a strong 4h trend interrupted by a 1h counter-move.
**Best single signal in the sweet-spot band.** High win rate at every threshold.

| fw/wk | 1.5% | 2.0% | **2.5%** | 3.0% | 3.5% | call | put | type |
|---|---|---|---|---|---|---|---|---|
| 3.4 | 94% | 82% | **73%** | 63% | 52% | 44% | 39% | straddle |

The threshold gradient (94% → 73% → 52%) is directly actionable for strike selection:
buy straddle at 1.5-2% OTM for near-certain hit, accept lower certainty at 3.5% OTM.

Split by direction:
- `bull_pullback`: 4h ≥ +1% + 1h ≤ -0.5% + rv ≥ 0.60 → **68% @ 2.5%**, 1.5 fw/wk
- `bear_pullback`: 4h ≤ -1% + 1h ≥ +0.5% + rv ≥ 0.60 → **76% @ 2.5%**, 1.9 fw/wk

The bear direction is stronger (76% vs 68%) — BTC downside moves tend to be faster.

---

#### `bear_burst`
> ret_4h < -0.5%  +  vol_z ≥ 1.5  +  rv_rank ≥ 0.60

Downward 4h momentum with a simultaneous volume spike in a high-vol regime. Good
straddle signal; fires 2.2/wk — within the sweet-spot band.

| fw/wk | 1.5% | 2.0% | **2.5%** | 3.0% | 3.5% | call | put | type |
|---|---|---|---|---|---|---|---|---|
| 2.2 | 90% | 82% | **73%** | 62% | 50% | 40% | 44% | straddle |

---

#### `bb_extreme`
> bb_width ≥ 90th percentile  (Bollinger Band very wide)

Counter-intuitive result: high bb_width (already expanded bands) is more predictive than
low bb_width (squeeze). BB width is effectively a second vol-regime indicator.
Fires 16.8/wk — high frequency, useful as a filter overlay rather than standalone signal.

| fw/wk | 1.5% | 2.0% | **2.5%** | 3.0% | 3.5% | call | put | type |
|---|---|---|---|---|---|---|---|---|
| 16.8 | 91% | 81% | **71%** | 58% | 49% | 37% | 41% | straddle |

BB squeeze (≤25th pct) is the **worst** regime: 30% @ 2.5% — well below baseline. Do not trade in squeeze.

---

### Direction finding (key conclusion)

No price-only condition reliably produces call_win >> put_win (or vice versa) by enough
margin to justify buying single options over straddles. The clearest finding:

- **Everything is a straddle signal from price data alone.** The few conditions that classify
  as "puts" (e.g. rv_rank ≥ 0.60 pure) are put-skewed by only ~8pp, not enough to justify
  the extra directionality risk.
- The "directional calls" conditions (4h uptrend + rv) still show *higher put win rates* than
  call win rates at the primary threshold: price rising 4h does not predict the next move direction.
- **If single-options directionality is desired**, the signal must come from a source not tested
  here: options IV skew (calls vs puts IV), order flow, or macro/catalyst timing.

---

### Regime dependency (recent check — 2026-04-26 → 2026-05-15)

The last ~3 weeks showed a radically different picture:

| Condition | Historical mag@2.5% | Recent mag@2.5% |
|---|---|---|
| Baseline | 44% | **20%** |
| vol_regime | 56% | 21% |
| vol_burst | 61% | 21% |
| pullback | 73% | 50% (only 2 fires) |
| bear_burst | 73% | 43% |

**Interpretation:** BTC has been in a calm uptrend with low vol rank (~0.25–0.60). The strategy
is intentionally regime-gated — `rv_rank < 0.60` conditions are not recommended. When rv_rank
drops below 0.40 for multiple days, stand aside. The `vol_burst` condition's low fire rate (5/wk
vs 6.3 historical) confirms the market correctly triggered fewer entries.

---

### Recommended Pine Signal Design

Priority order for Pine Script implementation:

1. **`pullback`** (primary) — 3.4/wk, 73% at 2.5%.
   Clearest signal with widest threshold range. Implement first.
   *Gate: rv_rank ≥ 0.60 (computed as rolling 30-day rank of 24h realized vol)*

2. **`vol_burst`** (secondary) — 6.3/wk, 61% at 2.5%.
   Higher frequency, slightly lower accuracy. Good complement to pullback in quiet stretches.
   *Gate: vol_z ≥ 1.5 (volume z-score vs 24h rolling mean/std)*

3. **Saturday exclusion** — always apply. Drops base rate from 44% to 48% at zero cost.

4. **Strike selection via threshold gradient** — use 2.0% OTM as primary target (82%/74% hit rate
   for pullback/vol_burst). 3.5% OTM is 50/50 on the best signals — not worth the premium.

5. **Stand-aside rule** — suppress signals when rv_rank < 0.35 for 12+ consecutive hours.

### Charts
- [09_named_conditions.svg](09_named_conditions.svg) — mag_win comparison across named conditions
- [09_threshold_curves.svg](09_threshold_curves.svg) — win-rate gradient 1.5%→3.5% for top conditions
- [09_call_put_map.svg](09_call_put_map.svg) — call vs put win rates; straddle vs directional
- [09_regime_context.svg](09_regime_context.svg) — historical vs recent-3wk win rates

---

### Rolling Win-Rate Stability  *(script: `10_rolling_winrate.py`)*

All conditions evaluated over rolling 12-week windows, 1-week step, Wilson 95% CI.
Script: `10_rolling_winrate.py` → charts `10_rolling_winrate.svg`, `10_rolling_current.svg`.

**Key finding: the slope is a regime artefact, not signal decay.**

Every condition shows a negative OLS slope in the trailing 8 windows. But inspection of the
window table reveals the decline is entirely concentrated in windows ending May 6 and May 13 —
coinciding with BTC entering a low-vol calm uptrend in late April. Windows through April 22
were *above* full-period baseline for all conditions:

| Condition | Full-period WR | Apr 22 window WR | May 13 window WR | May 13 N |
|---|---|---|---|---|
| pullback | 72.8% | **84.8%** | 57.9% | **19** |
| bear_pullback | 76.3% | **91.7%** | 75.0% | **8** |
| vol_burst | 60.9% | **81.6%** | 57.4% | 61 |
| bear_burst | 73.4% | **86.7%** | 40.0% | **10** |
| vol_regime | 55.9% | 71.4% | 46.8% | 476 |

The trailing-12wk estimates for low-frequency conditions (`pullback`, `bear_burst`) have
±30-40pp Wilson CIs at N=10-19 — statistically uninformative. `bear_pullback` at N=8 is
statistically consistent with its full-period 76% (trailing CI [41%–93%]).

**Conclusion:** The `rv_rank ≥ 0.60` gate is already the recency filter — it suppresses
signals in quiet regimes. No additional recency weighting is warranted or statistically
justifiable for low-frequency conditions. The signals are stable within high-vol regimes.

---

## Phase 4: Strategy Round-Trip Optimisation  ✅ *Complete — 17 May 2026*

### Objective
Find the empirically optimal (entry signal combination, straddle structure, exit rule) by
backsimulating on **actual options data** (Jan–May 2026, 5-min resolution).
No Black-Scholes approximations — all P&L numbers come from real bid/ask prices.

### Scripts
| Script | Status | Purpose |
|---|---|---|
| `11a_signal_combinations.py` | ✅ | Signal combination characterisation |
| `11b_option_universe.py` | ✅ | Option chain availability + costs at signal fires |
| `11c_pnl_lookup.py` | ✅ | Raw straddle forward P&L lookup (no exits) |
| `11d_optimise.py` | ✅ | Exit rule grid search (75 combos × 4,442 fire×param rows) |

### Output files
`11a_signal_combinations.csv`, `11b_option_universe.csv`, `11c_pnl_summary.csv`,
`11c_pnl_curves.parquet` (1.1M rows), `11d_grid_results.csv`, `11d_best_per_group.csv`

### Actual fires in options window (2026-01-01 → 2026-05-12, 18.7 weeks)

| Signal | Actual fires (all DTE) | After 4h cooldown |
|---|---|---|
| `pullback` | 54 | ~41 |
| `vol_burst` | 116 | ~68 |

---

### Phase 4A Results — Signal combination characterisation  (`11a`)

| Combo | fw/wk | mag@2.5% | Signal type |
|---|---|---|---|
| `pullback_only` | 3.0 | 72% | straddle |
| `vol_burst_only` | 5.8 | 61% | straddle |
| `both` (same bar) | 0.5 | **79%** | calls (58% call vs 30% put) |
| `either` (union) | 9.2 | 64% | straddle |

**Co-firing lift**: when pullback and vol_burst co-fire within ±8h, mag@2.5% jumps
to 77.3% vs 55.1% for non-co-firing fires (1.40× lift). Not enough occurrences to
warrant a separate signal tier but worth monitoring.

**Cooldown**: 4h cooldown cuts frequency (pullback 2.4/wk, vol_burst 3.7/wk) while
preserving quality. Used in 11b/11c/11d.

---

### Phase 4B Results — Option universe at signal fires  (`11b`)

**Availability** (ask ≥ $75, spread ≤ 30%, hours_to_expiry ≥ 4h):
- DTE-2 and DTE-3: 98–100% available across all delta targets
- DTE-1 at δ≥0.15: 89–100% available; δ0.10: 59–61% (ask too low, not actionable)
- DTE-4: ~80–85% (some weekly expirations absent)

**Straddle costs at pullback fires:**
| DTE | δ | Median straddle ask |
|---|---|---|
| 1 | 0.20 | ~$194 |
| 2 | 0.20 | ~$664 |
| 2 | 0.30 | ~$1,112 |
| 2 | 0.35 | ~$1,389 |

**Key IV finding**: pullback entries see ~55% IV vs ~47–50% for vol_burst. The vol-regime
gate selects by *realised* vol, not implied — premium does not inflate dramatically at
pullback entries relative to vol_burst.

---

### Phase 4C Results — Raw straddle P&L (no exits)  (`11c`)

**Peak multiple distribution** (what the market gave without any exit):

| Signal | DTE | δ | ≥1.3× | ≥1.5× | ≥2.0× | ≥2.5× | Median peak | Median T-to-peak |
|---|---|---|---|---|---|---|---|---|
| pullback | 1 | 0.25 | 62% | 43% | 19% | 13% | 1.46× | 3.8h |
| pullback | 2 | 0.30 | 64% | 43% | 36% | 17% | 1.43× | 13.8h |
| vol_burst | 1 | 0.35 | 70% | 59% | 39% | 29% | 1.68× | 2.9h |
| vol_burst | 1 | 0.20 | 71% | 63% | 50% | 38% | 2.07× | 2.2h |
| vol_burst | 2 | 0.20 | 70% | 53% | 37% | 27% | 1.56× | 3.8h |

**Time-to-peak**: vol_burst DTE-1 peaks fast (median 2–3h, 85% within 8h). Pullback
DTE-2 peaks slowly (median 12–14h). This directly determines the optimal time-stop.

**Hold-to-end**: median final multiple at T+24h / expiry is 0.21–0.50× across all
combos — never profitable without exits. Theta decay makes passive holding a loser.

**SL finding**: positions that hit a 0.3× or 0.5× SL level typically recover before
the time-stop. Adding any SL reduces EV across both signals.

---

### Phase 4D Results — Exit rule grid search  (`11d`)

Grid: TP ∈ {None, 1.3×, 1.5×, 2.0×, 2.5×} × TS ∈ {4, 8, 12, 16, 20h} × SL ∈ {None, 0.3×, 0.5×}
= 75 combinations × 4,442 (signal_fire × DTE × delta_target) rows = 333,150 exit simulations.

**Optimal configuration per signal tier:**

| Signal | DTE | δ | TP | TS | SL | N | E[exit×] | E[$]/fire | Win rate |
|---|---|---|---|---|---|---|---|---|---|
| pullback | 2 | 0.30 | 2.0× | 20h | none | 53 | 1.09× | **+$160** | 45% |
| vol_burst | 1 | 0.35 | none | 4h | none | 116 | 1.35× | **+$211** | 48% |

**Runner-up configs** (for robustness testing):
- pullback: DTE=2, δ=0.25, TP=2.0×, TS=20h, no SL → E[$]=+$109
- vol_burst: DTE=1, δ=0.30, no TP, TS=4h, no SL → E[$]=+$207

**Key observations:**
1. **Pullback needs time** — TS=20h dominates; cutting at 4–8h turns +$160 to −$130
2. **Vol_burst is fast** — TS=4h is the primary exit; no TP needed because the 4h window
   captures the full move without early truncation
3. **SL hurts both** — across all combos, SL=None ≥ SL=0.3× ≥ SL=0.5×. Losers recover.
4. **DTE-1 dominates for vol_burst** — E[$] $150–211 for DTE-1 vs $80–160 for DTE-2
5. **Pullback DTE-1 barely works** — best $49 vs $160 for DTE-2; DTE-2 is clearly right

**Combined weekly gross EV (in-regime):**  ~$480/wk (pullback) + ~$1,225/wk (vol_burst)
= ~$1,700/wk gross at ~8.8 straddles/week.

---

### ⚠ Uncertainty note

Sample sizes are moderate: ±14pp 95% CI on win rate for pullback (N=53), ±9pp for
vol_burst (N=116). The grid search rules out bad configurations decisively but cannot
finely tune parameters. Final validation requires live paper trading (Phase 5).

---

### Original Step 4A–4D plan (kept for reference)

#### Step 4A — Signal combination characterisation
**Script: `11a_signal_combinations.py`**

Before touching options data, understand how the signals overlap and whether simultaneous
firing is a stronger predictor.

For every 1h bar in 2025-01-01 → 2026-05-15, compute four masks:
- `pullback_only` — pullback fires, vol_burst does not
- `vol_burst_only` — vol_burst fires, pullback does not
- `both` — both fire simultaneously
- `either` — union

For each mask, report: N, fw/wk, mag_win at all five thresholds (1.5–3.5%), call/put split,
signal_type. This answers:
- Is `both` meaningfully better than `pullback_only`? (If yes, it earns a priority tier.)
- Is `vol_burst_only` good enough to trade on its own, or only when paired with pullback?
- What is the natural cooldown period? (How often do signals cluster within 4h?)

**Output:** `11a_signal_combinations.csv` + console table (no new SVGs needed).

---

### Step 4B — Option universe characterisation at signal fires
**Script: `11b_option_universe.py`**

For every signal fire in the Jan–May 2026 options window, load the actual options chain
and record what was available across the full (DTE, delta_target) grid.

**Grid:**
- DTE: 1, 2, 3, 4
- delta_target: 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40

For each (fire_timestamp, DTE, delta_target) cell, find the closest matching contract
(call side and put side separately) and record:

| Field | Definition |
|---|---|
| `actual_delta` | delta at entry time |
| `actual_iv` | mark_iv at entry |
| `ask_usd` | ask price in USD |
| `bid_usd` | bid price in USD |
| `spread_pct` | (ask − bid) / ask |
| `straddle_ask` | ask_call + ask_put (same strike, same expiry) |
| `straddle_spread_pct` | combined spread as % of straddle ask |
| `hours_to_expiry` | hours until 08:00 UTC expiry |
| `available` | bool — passes min-ask ($75) and max-spread (30%) filters |

**Output:** `11b_option_universe.csv` — one row per (fire × DTE × delta_target).

This directly answers: *"What does a 0.20-delta DTE-2 straddle actually cost at a pullback entry, on average?"*
No assumptions. The data tells us.

**Also compute summary statistics:**
- Median straddle_ask by (DTE, delta_target) across all signal fires
- Availability rate (% of fires where a valid contract exists) by (DTE, delta_target)
- IV at entry vs rv_rank — does our vol-regime gate predict high or low IV? (affects premium cost)

---

### Step 4C — Straddle P&L grid simulation
**Script: `11c_straddle_grid.py`**

For each signal fire × each available (DTE, delta_target) combination, simulate the full
24-hour straddle P&L using actual 5-min bid prices. No assumptions.

**P&L tracking:**
- Entry value: `ask_call + ask_put` (actual ask at entry bar)
- Forward value at each 5-min step: `bid_call + bid_put` (conservative — use bid for exit)
- Net P&L at time t: `(bid_call(t) + bid_put(t)) − (ask_call(0) + ask_put(0)) − fees`
- Fees: from scan fee model (`FEE_RATE_BTC`, `FEE_CAP_RATIO`)

**Exit rule grid** (applied independently to each (signal, DTE, delta) combo):

| Axis | Values |
|---|---|
| Take-profit multiple | 1.3×, 1.5×, 2.0×, 2.5× entry cost |
| Stop-loss floor | 0.35×, 0.45×, 0.55× entry cost (i.e. −65%, −55%, −45% max loss) |
| Time-stop horizon | 8h, 12h, 18h, 24h |
| Rule type | TP-only, stop-only, TP+stop+time (combined) |

For each combination, record: exit_time, exit_value, net_pnl, exit_reason (tp / stop / time).

**Output CSVs:**
- `11c_pnl_grid.csv` — one row per (signal_fire × DTE × delta_target × exit_rule)
- `11c_pnl_curves.parquet` — 5-min P&L time series per (signal_fire × DTE × delta_target)
  (parquet because this is ~120 fires × 28 combos × 288 bars = ~1M rows)

**Key question this answers:** For the `pullback` low-freq signal, does DTE-3 with 0.20-delta
outperform DTE-1 ATM? Does the cheaper OTM structure recover more premium in losing trades?

---

### Step 4D — Optimisation and recommendation
**Script: `11d_optimise.py`**

Run across the full grid output from 4C. For each (signal_tier, DTE, delta_target, exit_rule)
combination compute:

| Metric | Definition |
|---|---|
| `mean_pnl` | Average net P&L per trade |
| `median_pnl` | Median net P&L (robust to outliers) |
| `pnl_p10` | 10th percentile net P&L (worst-case typical loss) |
| `win_rate` | Fraction of trades with net_pnl > 0 |
| `ev_per_week` | mean_pnl × fw/wk (capital efficiency) |
| `max_drawdown` | Worst consecutive losing streak in USD |

**Two separate optimisations:**
- **Low-freq (`pullback`, ~3.4/wk):** Optimise for `mean_pnl` and `pnl_p10` — fewer trades
  means each loser matters more; want tight stop, higher TP, maybe more DTE for cushion.
- **High-freq (`vol_burst`, ~4.2/wk):** Optimise for `ev_per_week` — higher frequency allows
  more losers; want cheaper structure (lower delta), tighter time-stop.

Report top-5 configurations for each tier. Select final recommended configuration.

**Output SVGs:**
- `11d_pnl_distribution.svg` — P&L histogram for recommended configs (low-freq vs high-freq)
- `11d_pnl_curve.svg` — average straddle mark vs hours post-entry for each tier (shows *when* move happens)
- `11d_grid_heatmap.svg` — heatmap: mean_pnl by (DTE × delta_target) for top exit rule

---

### Execution order
```
11a → 11b → 11c → 11d
```
Each step produces outputs that inform the next. Do not skip steps or assume parameters.

### Data constraint note
~64 `pullback` fires and ~79 `vol_burst` fires in the options window. P&L estimates carry
±15–25pp uncertainty at 95% confidence. The goal of Phase 4 is to **rule out bad
configurations** and narrow to 2–3 candidates, not to fit a precise number. Final
parameter selection will be validated on live paper trading (Phase 5).

---

## Phase 5: Real Backtester Handover  *(next phase)*

### Status
Phase 4 is complete. The empirical analysis is done and parameters are selected.
The next step is to validate the strategy in the production backtester (CryoTrader)
against a wider historical window and with realistic order/position management.

### Handover document
**`research/long_tradable_options/STRATEGY_SPEC.md`** — complete strategy specification:
- Exact indicator formulas (rv_rank, vol_z, ret_4h, ret_1h)
- Signal boolean logic with thresholds
- Option selection rules (expiry, strike, availability filter)
- Entry / exit / fee rules
- Expected performance table
- Open questions for the backtester implementer

### Phase 5 objectives
1. **Wider window** — run the strategy on 2025-01-01 → 2025-12-31 Deribit data (not used
   in Phase 4 optimisation) to check out-of-sample P&L.
2. **Position sizing** — test fixed $ per trade vs fixed % of capital; measure drawdown.
3. **Regime sensitivity** — verify that the stand-aside rule (rv_rank < 0.35 for 12h)
   correctly suppresses trades in unprofitable periods.
4. **Simultaneous positions** — test stacking (max 2–3 per tier) vs single-position rule.
5. **Paper trading** — run live for 4–6 weeks with small size before committing capital.

### Key open questions (from STRATEGY_SPEC.md §15)
- Position sizing method (fixed $ vs % of capital)
- Whether to allow position stacking per signal tier
- TP=2.5× as a defensive cap on vol_burst (currently no TP)

---

## Key conventions
- Python venv: `/Users/ulrikdeichsel/IndicatorBench/pineforge/.venv`
- Run: `source pineforge/.venv/bin/activate && python3 -u research/long_tradable_options/02_v2_enrich.py`
- All scripts live in `research/long_tradable_options/`
- `option_utils` is imported from `research/intraday_options/option_utils.py`
- Pine Script work is unrelated — do not touch `pine/` directory
- **NEVER commit or deploy without explicit user approval**
- **Do not proceed to signal discovery without the user reviewing the enriched data**

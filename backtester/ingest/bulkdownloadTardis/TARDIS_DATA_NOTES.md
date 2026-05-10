# Tardis Deribit Options Data — Cleaning Notes

**Source:** Tardis `OPTIONS.csv.gz` bulk dataset (Deribit BTC options, snapshot format)  
**Units:** All prices BTC-denominated. IV in percent (e.g. 60.0 = 60%). Delta is a signed float.

---

## 1. Known Data Bugs

### 1.1 Inverted bid/ask

Occasionally `ask < bid`. Cause is unclear — likely a race condition in Tardis snapshot assembly. Frequency: low but consistent across dates.

**Fix:** swap the two values when `ask < bid` and both are positive.

### 1.2 mark outside spread

`mark_price` sometimes falls outside `[bid, ask]` even after the above swap. Expected: `bid ≤ mark ≤ ask`.

**Fix:** clamp
```
mark = max(mark, bid)   if mark < bid
mark = min(mark, ask)   if mark > ask
```
Applied only when both `bid > 0` and `ask > 0`.

### 1.3 IV format inconsistency

IV should be in percent (e.g. 60.0). On some days the field is in decimal (e.g. 0.60).

**Detection:** compute median of all positive non-NaN values for the day. If median < 2.0, rescale the entire day: `iv *= 100`.

This is a day-level rescale, never a per-record one. The threshold 2.0 is safe because a legitimate IV below 200% annual would still be > 2.0% when expressed correctly.

### 1.4 Corrupt spot price rows

`underlying_price` occasionally contains garbage values (0, `NaN`, or implausible spikes). These contaminate any ratio-based filter that uses spot as a denominator.

**Fix (two-pass):**
1. Drop records where spot is `NaN` or `0`.
2. Compute day median $S_{med}$. Drop records where $|S - S_{med}| / S_{med} > 0.20$.

---

## 2. NaN and Zero Price Policy

After spot filtering, the remaining price fields (bid, ask, mark, IV, delta) may still be `NaN`. These are **preserved as `NaN`** in the output parquet — they are NOT replaced with `0.0`.

**Convention (updated April 2026):**
- **`NaN`** = "data absent from exchange" — the instrument was never ticked in this 5-min window, or the exchange did not provide this field. Must not be passed to any formula expecting a valid price.
- **`0.0`** = "exchange reported this value as zero" — e.g. a real zero bid on a far-OTM option with no market maker interest. This is genuine market data.

Downstream consumers should check `isnan(value)` to detect absent data, and treat `0.0` as a real exchange-reported value.

Parquet stores `NaN` natively in float32 columns. Pandas reads these as `NaN` automatically.

---

## 3. Extreme Price Filter (ask > 10 BTC)

Raw data occasionally contains records where ask is on the order of hundreds or thousands of BTC. These are hard data errors — no BTC option can be worth more than the underlying itself.

**Theoretical bounds:**

- **Call:** payout ≤ $S$ (underlying). Since prices are BTC-denominated: `ask_call ≤ 1.0 BTC` in theory. In practice long-dated deep ITM calls carry significant time value above intrinsic, so we use a generous cap of `6.0 BTC`.
- **Put:** intrinsic value ≤ $(K - S)/S$ BTC = $K/S$ — 1 BTC. We allow 5% above intrinsic for residual time value:

$$\text{ask\_put}^{\max} = \frac{K}{S} \times 1.05$$

- **Universal hard cap:** `ask > 10.0 BTC` dropped unconditionally, regardless of option type. This catches records where the spot field itself is corrupted (making the per-record put bound unreliable).

**Drop condition (pseudocode):**
```
drop if ask > 10.0                          # absolute cap
drop if is_call  and ask > 6.0             # call cap
drop if is_put   and ask > (K / S) * 1.05  # put intrinsic cap
```
Records matching any condition are removed entirely (they corrupt downstream ratio metrics and BSM calibration).

---

## 4. Approximating Fills for Missing bid or ask

In normal market hours most instruments have both sides quoted. Near expiry or for deep OTM strikes it is common for one side to be absent (`== 0`). The backtester needs a fill estimate even in those cases.

### 4.1 Entry (selling — need ask equivalent)

We need the price at which a short seller could reasonably be filled.

| Available | Formula | Rationale |
|---|---|---|
| `bid > 0`, `ask > 0` | `effective_ask = max(ask, mark)` | Floor at mark prevents a stale thin ask from pricing below exchange fair value |
| `ask == 0`, `mark > 0`, `mark_usd > threshold` | `effective_ask = mark × (1 + slip)` | Mark is exchange model price; add a conservative slippage factor |
| `ask == 0`, `mark_usd ≤ threshold` | skip tick (return `None`) | Too illiquid; any estimate is noise |

`slip` (e.g. 5%) reflects the expected adverse spread when there is no ask quote. `threshold` (e.g. $2 USD) prevents this approximation from running on near-zero OTM options where the relative error is enormous.

### 4.2 Exit (buying back — need bid equivalent)

| Available | Formula | Rationale |
|---|---|---|
| `bid > 0` | `bid_usd = bid × spot` | Use directly |
| `bid == 0`, `mark > 0`, `mark_usd > threshold` | `effective_bid = mark × (1 − slip)` | Mirror of the entry case |
| `bid == 0`, `mark_usd ≤ threshold` | skip tick | Same illiquidity logic |

### 4.3 Example

```
spot  = 85 000 USD
mark  = 0.002 BTC  → mark_usd  = 170 USD
ask   = 0.000 BTC  → ask       = 0   (absent)
slip  = 0.05
threshold = 2 USD

mark_usd (170) > threshold (2) → proceed
effective_ask = 0.002 × 1.05 = 0.0021 BTC  → 178.50 USD fill estimate
```

This is a conservative estimate (slightly worse than mark). It is not a realistic fill — it is the minimum pessimistic price we assign when the book is one-sided.

---

## 5. Midnight Snapshot Gap

When processing each calendar day in isolation, at the first 5-min boundary (00:00 UTC) virtually no instruments have ticked yet. The 00:00 snapshot is sparse — typically < 10% of the expected instrument count.

**Fix:** for each day $D$, take all instruments present in day $D{-1}$'s 23:55 snapshot that are absent from $D$'s 00:00 snapshot, and inject them with their $D{-1}$ state and timestamp set to $D$'s 00:00. This mirrors what a live system would see: stale-but-valid quotes from the previous session persist until the exchange re-quotes.

The fixup is idempotent and only modifies the 00:00 boundary of each day.

---

## 6. Record-Count Plausibility Check

A correctly processed day has ≈ 86 000 records (288 boundaries × ~300 instruments). After all cleaning steps, if the total record count falls below ~10 000, the day should be flagged as suspect and the raw file retained. This prevents silent data loss from a failed extraction being treated as a clean result.

---

## 7. Spot Track — Authoritative Source

### 7.1 Why `underlying_price` from options ticks is not a reliable spot source

Each Deribit options tick carries an `underlying_price` field. For short-dated
options (DTE ≤ 3) this is very close to the spot index. For long-dated options
it is the settlement price of the corresponding quarterly/yearly futures contract,
which carries a significant basis premium above spot.

When `stream_extract.py` processes all DTE up to 700, long-DTE options ticks
dominate the spot OHLC accumulator, inflating the spot track by thousands of
dollars around expiry windows. See `backtester/ingest/SPOT_DATA_ISSUE.md` for full
details and measured examples.

### 7.2 Correct source: `derivative_ticker/BTC-PERPETUAL index_price`

The authoritative spot source for historical data is the Tardis
`derivative_ticker` dataset for the `BTC-PERPETUAL` instrument:

```
GET https://datasets.tardis.dev/v1/deribit/derivative_ticker/{YYYY}/{MM}/{DD}/BTC-PERPETUAL.csv.gz
Authorization: Bearer {TARDIS_API_KEY}
```

The `index_price` column in this dataset is the Deribit composite settlement
index — a capped median of prices from 5+ constituent exchanges (Coinbase, Kraken,
Bitstamp, Bitfinex, Gemini). It has no DTE, no futures basis, and records at
~245 k ticks/day, giving clean and dense coverage for all 1440 minutes of each day.

This is what all Deribit options actually settle against, and it is immune to the
long-DTE futures basis problem.

### 7.3 Tool: `rebuild_spot_parquets.py`

`backtester/ingest/bulkdownloadTardis/rebuild_spot_parquets.py` builds
`spot_YYYY-MM-DD.parquet` files from this source. It was used to rebuild all
378 corrupted spot parquets in the dataset. Use it as the reference implementation
whenever spot parquets need to be created or regenerated.

### 7.4 Guard in `stream_extract.py`

`stream_extract.py` now has `SPOT_MAX_DTE = 2`. The spot OHLC accumulator only
accepts ticks from options with DTE ≤ 2, limiting contamination from long-DTE
instruments. Future `bulk_fetch.py` runs will produce clean spot tracks without
needing `rebuild_spot_parquets.py`, but the derivative_ticker approach remains
cleaner for any spot-only rebuild.

---

## Summary Table

| Issue | Frequency | Action |
|---|---|---|
| IV in decimal instead of percent | ~5% of days | Multiply all IV values × 100 if day median < 2.0 |
| NaN in price fields | Common for illiquid instruments | Replace with 0.0 (sentinel) |
| Spot == 0 or NaN | Rare, <0.01% of records | Drop record |
| Spot spike (>20% from day median) | Rare | Drop record |
| ask > 10 BTC (absolute) | Very rare, but present | Drop record |
| ask > put intrinsic × 1.05 | Rare, deep ITM puts | Drop record |
| ask < bid | Occasional | Swap bid and ask |
| mark outside [bid, ask] | Occasional | Clamp mark to spread |
| 00:00 sparse snapshot | Every day | Seed from D−1 23:55 |

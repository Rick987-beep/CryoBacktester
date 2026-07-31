# Deribit BTC Options — Historic Snapshot Package

**Coverage:** 2026-07-01 → 2026-07-29 (UTC)  
**Packaged:** 2026-07-29  
**Venue / underlying:** Deribit Bitcoin (BTC) options + BTC index/spot track  
**Format:** Apache Parquet (one file per calendar day, UTC)

---

## Important limitation

**These are only 5-minute snapshots of BTC options on Deribit.**

They are **not** tick-by-tick order-book history, **not** trade prints, and **not** continuous quotes.
At each 5-minute boundary the recorder captures one cross-section of the option chain
(bid / ask / mark / IV / delta) plus the BTC spot used as the underlying price.

Do **not** treat this package as full-resolution market data. Intra-interval moves
between snapshots are not represented in the options files.

---

## What’s in this package

```
deribit_btc_options_jul2026/
├── README.md                 ← this file
├── extract_example.py        ← runnable example: how to load / filter / convert timestamps
└── data/
    ├── options_YYYY-MM-DD.parquet          ← complete daily option-chain snapshots
    ├── spot_track_YYYY-MM-DD.parquet       ← BTC 1-minute OHLC bars for that day
    ├── options_2026-07-29_INCOMPLETE.parquet      ← in-progress day (see below)
    └── spot_track_2026-07-29_INCOMPLETE.parquet
```

| Series | Days | Notes |
|--------|------|--------|
| Complete | 2026-07-01 … 2026-07-28 | Finalized daily files from the production tick recorder |
| Incomplete | 2026-07-29 | Still being written at package time — use with caution |

Source: CryoTrader `ct-recorder` on production (same schema used by CryoBacktester).

---

## File naming

| Pattern | Contents |
|---------|----------|
| `options_YYYY-MM-DD.parquet` | Full option chain at every **5-minute** snapshot for that UTC day |
| `spot_track_YYYY-MM-DD.parquet` | BTC **1-minute** OHLC bars for that UTC day |
| `*_INCOMPLETE.parquet` | Same schemas, but the UTC day was not finished when packaged |

---

## Options schema (`options_*.parquet`)

One row = one instrument at one 5-minute snapshot.

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | `int64` | Snapshot time in **microseconds since Unix epoch (UTC)**. Aligned to 5-minute boundaries. |
| `expiry` | string | Deribit expiry code, e.g. `28JUL26` |
| `strike` | float | Strike in USD |
| `is_call` | bool | `True` = call, `False` = put |
| `underlying_price` | float | BTC spot (USD) at this snapshot |
| `bid_price` | float | Best bid in **BTC** (not USD). `0` / `NaN` = no bid / absent |
| `ask_price` | float | Best ask in **BTC**. `0` / `NaN` = no ask / absent |
| `mark_price` | float | Exchange mark price in **BTC** |
| `mark_iv` | float | Mark implied volatility as a **percentage** (e.g. `58.2` = 58.2%). Do **not** multiply or divide by 100. |
| `delta` | float | Option delta (signed) |

Typical day size: ~2–3 MB compressed; on the order of ~200k–300k rows
(all listed strikes × calls/puts × ~288 five-minute snapshots).

### Units reminder

- Option prices (`bid_price`, `ask_price`, `mark_price`) are in **BTC**, not USD.
- USD premium ≈ `price_btc × underlying_price`.
- `mark_iv` is already in percent.

### Missing quotes

- **`NaN`** means the field was absent in the snapshot (instrument not quoted / not updated).
- **`0.0`** can be a real exchange-reported zero (common for far OTM bids).
- Check with `pandas.isna(...)` before treating a value as a valid price.

---

## Spot schema (`spot_track_*.parquet`)

One row = one 1-minute OHLC bar for BTC.

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | `int64` | Bar start time in **microseconds since Unix epoch (UTC)** |
| `open` | float | Open (USD) |
| `high` | float | High (USD) |
| `low` | float | Low (USD) |
| `close` | float | Close (USD) |

A full day has up to 1440 bars; some days may have fewer if the recorder missed minutes.

Options snapshots are on a **5-minute** grid; spot track is **1-minute**. Join by flooring
the options timestamp to the minute, or by nearest-previous bar.

---

## Timestamps

```python
import pandas as pd

df["dt"] = pd.to_datetime(df["timestamp"], unit="us", utc=True)
```

Example: `1783900800000000` → `2026-07-13 00:00:00+00:00`.

---

## How to extract data

See `extract_example.py` in this folder. Minimal dependency set:

```bash
pip install pandas pyarrow
python extract_example.py
```

The script shows how to:

1. List available days
2. Load one options day and convert timestamps
3. Filter to a strike / expiry / calls-only
4. Load spot bars and align to a 5-minute snapshot
5. Convert BTC option prices to USD

---

## What this data is *not*

- Not tick data / not L2 order book depth
- Not trade (fill) history
- Not continuous quotes between 5-minute marks
- Not multi-asset — **BTC options on Deribit only**

---

## Provenance

| Field | Value |
|-------|--------|
| Recorder | CryoTrader `ct-recorder` |
| Exchange | Deribit |
| Instrument class | BTC options (+ BTC spot track) |
| Snapshot interval (options) | **5 minutes** |
| Spot bar interval | 1 minute |
| Timezone of day files | UTC calendar days |

# Bulk Tardis Download — Agent Manual

This document is a complete, accurate manual for downloading and processing a full year of historic Deribit BTC options data from tardis.dev and producing the compact snapshot parquets consumed by the backtester. It reflects what the code actually does, verified in production (April 2026).

---

## What You Are Building

371 days of 5-minute option chain snapshots and 1-minute spot OHLC bars, covering the Tardis subscription window. All DTE options are included (monthlies, quarterlies — no DTE cap). Output is two small parquet files per day:

```
data/options_YYYY-MM-DD.parquet   (~3-5 MB)   -- 5-min snapshots, ~200-350k rows/day
data/spot_YYYY-MM-DD.parquet      (~20-35 KB) -- 1-min OHLC bars, ~1440 rows/day
```

Total output for 371 days: ~1.5 GB. Total inbound data processed: ~2-3 TB (raw files deleted after extraction).

---

## Scripts in This Folder

All scripts are self-contained. No dependency on the wider backtester codebase.

| Script | Role |
|---|---|
| `stream_extract.py` | Core: streams one day's OPTIONS.csv.gz -> two parquets |
| `bulk_fetch.py` | Orchestrator: date-range loop, download -> extract -> clean -> delete raw |
| `clean.py` | Data cleaning and validation, called inline by bulk_fetch.py |
| `fixup_midnight.py` | Post-pass: seeds sparse 00:00 snapshots from prior day's 23:55 state |
| `run_bulk.sh` | tmux launcher: 4 parallel workers + auto-fixup monitor window |

---

## Step 1 -- Verify Your Tardis API Key

```bash
curl -s "https://api.tardis.dev/v1/api-key-info" \
  -H "Authorization: Bearer $TARDIS_API_KEY" | python3 -m json.tool
```

Check that the response includes `"exchange": "deribit"` and that the `from`/`to` dates cover the range you want. The current academic key covers **2025-04-11 to 2026-07-12** (deribit options plan). The `from` date is the earliest date you can download -- this sets your `--from` boundary for the workers.

Free tier: only the 1st of each month is available without a key. Any other date requires a paid/academic key.

---

## Step 2 -- Provision the Server

Run this on a Hetzner dedicated server, not a home machine. Home ISP caps make this impractically slow (~30 min/day vs ~8-12 min/day on Hetzner).

**Recommended: Hetzner CCX33, Nuremberg (NBG)**

| Spec | Value |
|---|---|
| CPU | 8 vCPU AMD EPYC dedicated |
| RAM | 32 GB |
| SSD | 240 GB |
| Network | 1 Gbit/s inbound -- **inbound traffic is free at Hetzner** |
| Price | 0.077 EUR/hr -- hourly billing, delete when done |

Create at https://console.hetzner.com/ -> New Project -> Add Server -> Nuremberg -> Ubuntu 24.04 -> Dedicated -> CCX33. Add your SSH public key.

**Current server (April 2026):** `46.225.129.121` (hostname: TardisDownload, CCX33, Ubuntu 24.04)
Pipeline deployed at `/root/tardis/` with `.venv/`, `data/`, `logs/`, `.env`.

Verified download speed Tardis -> Hetzner Nuremberg: **~20-30 MB/s per worker** (4 parallel workers share the 1 Gbit/s pipe).

---

## Step 3 -- Bootstrap the Server

Run from your local machine:

```bash
# Install system packages
ssh root@<SERVER_IP> 'apt-get update -qq && apt-get install -y tmux rsync python3-venv python3-pip'

# Copy scripts to server
rsync -av /path/to/backtester/ingest/bulkdownloadTardis/{stream_extract.py,bulk_fetch.py,clean.py,fixup_midnight.py,run_bulk.sh,__init__.py} \
  root@<SERVER_IP>:/bulk/

# Create venv, install deps
ssh root@<SERVER_IP> 'cd /bulk && python3 -m venv .venv && .venv/bin/pip install --quiet requests pyarrow zstandard numpy pandas && .venv/bin/python -c "import requests,pyarrow,zstandard,numpy,pandas; print(\"OK\")"'

# Set API key persistently
ssh root@<SERVER_IP> 'echo "export TARDIS_API_KEY=\"your_key_here\"" >> ~/.bashrc'
```

---

## Step 4 -- Smoke Test (2 Consecutive Days)

Two consecutive days is the minimum meaningful test -- it exercises every pipeline stage including fixup_midnight.py's cross-day seeding.

```bash
ssh root@<SERVER_IP> 'cd /bulk && source .venv/bin/activate && \
  export TARDIS_API_KEY="your_key_here" && \
  python bulk_fetch.py --from 2025-05-01 --to 2025-05-02 --worker TEST 2>&1 | tee /bulk/test_log.txt'
```

Expected output per day:
```
--- Worker TEST  [1/2]  2025-05-02 ---
[download] 2025-05-02
  Size: 4.55 GB
  Downloaded: 4.55 GB  in 216s  (21.5 MB/s avg)
[stream_extract] 2025-05-02  max_dte=700  source: 4.55 GB
  Scan done: 19,469,292 matched / 105,133,575 total  ->  124,358 snapshot rows  (193s)
  Options: /bulk/data/options_2025-05-02.parquet  (1.2 MB, 124,358 rows)
  Spot:    /bulk/data/spot_2025-05-02.parquet  (33 KB, 1,441 1-min bars)
[clean] 2025-05-02  final_rows=245,000  nan_remaining=85,000  mark_clamped_low=227  mark_clamped_high=472
  Deleted raw: /bulk/data/options_chain_2025-05-02.csv.gz
[done] 2025-05-02  416s
```

Then run fixup:
```bash
ssh root@<SERVER_IP> 'cd /bulk && source .venv/bin/activate && python fixup_midnight.py --data-dir /bulk/data'
```

Expected fixup output:
```
[fixup] 2025-05-01  SKIP -- no previous day parquet   <- correct: first day has no D-1
[fixup] 2025-05-02  prev_last=23:55  midnight_had=17  seeding=437  already_present=17
Done.  days_fixed=1  days_skipped=1  total_rows_added=437
```

If both look correct, proceed. If anything fails, do not continue to the bulk run.

---

## Step 5 -- Configure Worker Date Splits

Edit the block boundaries at the top of `run_bulk.sh` to match your subscription window. Split evenly into 4 blocks, each processed newest-first within the block.

Example for window 2025-04-11 to 2026-04-16 (371 days):

```bash
WORKER_A_FROM="2026-01-15"
WORKER_A_TO="2026-04-16"

WORKER_B_FROM="2025-10-14"
WORKER_B_TO="2026-01-14"

WORKER_C_FROM="2025-07-13"
WORKER_C_TO="2025-10-13"

WORKER_D_FROM="2025-04-11"
WORKER_D_TO="2025-07-12"
```

Days that already have both output parquets are automatically skipped -- the run is safely resumable.

Dry-run to verify before launching:
```bash
ssh root@<SERVER_IP> 'cd /bulk && export TARDIS_API_KEY=x && bash run_bulk.sh --dry-run'
```

---

## Step 6 -- Launch the Bulk Run

```bash
ssh root@<SERVER_IP> "cd /bulk && export TARDIS_API_KEY='your_key_here' && bash run_bulk.sh"
```

This opens a tmux session named `bulk` with **5 windows**:
- Windows 0-3: Workers A, B, C, D -- each running bulk_fetch.py over its block, newest-first
- Window 4 (Monitor): polls every 60s; fires fixup_midnight.py automatically when all 4 workers report completion

Detach immediately (session survives SSH disconnects):
```
Ctrl-B D
```

Reattach any time:
```bash
ssh root@<SERVER_IP> 'tmux attach -t bulk'
# Ctrl-B 0/1/2/3 = worker windows | Ctrl-B 4 = monitor
```

---

## Monitoring Progress

```bash
ssh root@<SERVER_IP> 'bash /bulk/status.sh'
```

Shows: days completed, per-worker progress, failed/suspect counts, disk usage, fixup status.

Other useful commands:
```bash
ssh root@<SERVER_IP> 'tail -20 /bulk/logs/worker_A.log'   # tail a worker log
ssh root@<SERVER_IP> 'ls /bulk/data/options_*.parquet | wc -l'  # total days done
ssh root@<SERVER_IP> 'df -h /'                             # disk usage
```

---

## Timing Reference (Observed in Production, April 2026)

Per-day timing scales with raw file size. Deribit listed many more instruments in late 2025, so recent data takes longer.

| Period | Raw file size | Time per day per worker |
|---|---|---|
| Apr-Jul 2025 | 3-5 GB | 4-7 min |
| Jul-Oct 2025 | 5-7 GB | 8-10 min |
| Oct 2025-Jan 2026 | 5-7 GB | ~10 min |
| Jan-Apr 2026 | 5-10 GB | 12-20 min |

Wall-clock total for 371 days, 4 parallel workers: **~24-30 hours** (more rows per day with all DTE included).
Worker A (most recent data) is always the bottleneck.

Server cost: ~24 hrs x 0.077 EUR/hr = **~2 EUR total**.

---

## How the Pipeline Works

### Per-day flow (bulk_fetch.py)

```
1. Skip if both output parquets already exist
2. Download OPTIONS.csv.gz
     - No HTTP Range support on tardis.dev: dropped connection = restart from 0
     - Retries: up to 20 attempts, exponential backoff 10s to 5 min
     - Partial file detection: HEAD request validates size before reusing cached gz
3. stream_extract: single-pass gzip streaming -> two parquets (never loads full file)
4. clean: validate + fix parquets in-place
5. Delete raw gz  (unless day flagged suspect -- kept for inspection)
```

### stream_extract.py internals

- Reads gzip line by line -- never loads more than one line at a time
- Maintains last_quote: dict[(expiry, strike, is_call) -> latest row values], updated on every matching row
- At each 5-min UTC boundary: flushes last_quote -> appends ~300 snapshot rows
- Spot OHLC: accumulates underlying_price per 1-min bucket inline
- Filters to BTC options (all expiries, no DTE cap — max_dte=700)
- Writes both parquets at end using zstd compression

### fixup_midnight.py -- why it exists and when to run it

stream_extract.py processes each day in isolation. At 00:00 UTC the last_quote dict is nearly empty -- only instruments that ticked in the first few seconds of the file appear in the 00:00 snapshot. By 00:05 the snapshot is fully populated.

fixup_midnight.py does a single forward (chronological) pass over all finished parquets:
- For each day D: reads 23:55 rows from options_{D-1}.parquet, identifies instruments absent from D's 00:00, appends them with D-1 closing state
- Idempotent -- safe to re-run
- Skips D if D-1 parquet doesn't exist (first day in dataset, or a gap)
- Fast: reads/writes only the small snapshot parquets

**Critical: run fixup only after ALL workers complete.** Workers process newest-first within their blocks, so earlier dates in a block complete last. Running fixup mid-run produces incorrect 00:00 seeds. The run_bulk.sh monitor window handles this correctly.

---

## Data Cleaning (clean.py)

Called inline by bulk_fetch.py after extraction, before the raw gz is deleted. Each step logged in the [clean] line.

**Step 1 -- IV format normalisation**
If median(mark_iv) on non-zero rows < 2.0, the column is in decimal format. Multiply entire column by 100. Logged as iv_rescaled=True.

**Step 2 -- NaN handling**
- Rows where underlying_price is NaN or 0: drop entirely
- bid_price, ask_price, mark_price, mark_iv, delta: NaN is **preserved** (not filled)
- Convention: NaN = "data absent from exchange", 0.0 = "exchange reported zero"

**Step 3 -- Spot price outlier removal**
Drop rows where underlying_price deviates >20% from the day's median. Guards against corrupt one-off values. In practice this has never fired across 370+ days -- Tardis spot data is clean.

**Step 4 -- Option price outlier removal**
Two-tier check (deep ITM options can legitimately be worth several BTC):
- Universal hard cap: ask > 10.0 BTC -> drop
- Calls: ask > 6.0 BTC -> drop
- Puts: ask > (strike / spot) x 1.05 -> drop (intrinsic value + 5% time-value slack)

Fired on ~22/371 days, always small counts (typical: 2-20 rows, max 576 rows on a high-volatility day).

**Step 5 -- Bid/ask/mark ordering**
Applied only where bid > 0 and ask > 0:
- ask < bid -> swap them
- mark < bid -> clamp mark up to bid
- mark > ask -> clamp mark down to ask

**Step 6 -- Row-count plausibility**
If final_rows < 10,000: flag as suspect. Write options_YYYY-MM-DD.warn, keep raw gz for inspection.

What cleaning does NOT do: no interpolation, no imputation of Greeks, no forward/backward fill.

---

## Output Schema

### data/options_YYYY-MM-DD.parquet

| Column | Type | Notes |
|---|---|---|
| timestamp | int64 | Microseconds since epoch, 5-min boundaries |
| expiry | category | e.g. "9MAR26" |
| strike | float32 | e.g. 85000.0 |
| is_call | bool | |
| underlying_price | float32 | BTC spot at snapshot time |
| bid_price | float32 | BTC-denominated |
| ask_price | float32 | |
| mark_price | float32 | |
| mark_iv | float32 | Percent (60.0 = 60% IV) |
| delta | float32 | |

### data/spot_YYYY-MM-DD.parquet

| Column | Type | Notes |
|---|---|---|
| timestamp | int64 | Microseconds, 1-min boundaries |
| open | float32 | |
| high | float32 | |
| low | float32 | |
| close | float32 | |

---

## Getting Data Back

```bash
# Run from your local machine after all workers and fixup complete
rsync -av --progress \
  root@<SERVER_IP>:/bulk/data/ \
  /path/to/backtester/data/
# No -z flag: parquets are already zstd-compressed
```

~800 MB rsync completes in under 3 minutes.

---

## Final Verification Checklist

```bash
# 1. Day count matches expected
ssh root@<SERVER_IP> 'ls /bulk/data/options_*.parquet | wc -l'

# 2. Fixup completed, 0 gaps remaining
ssh root@<SERVER_IP> 'cd /bulk && source .venv/bin/activate && python fixup_midnight.py --data-dir /bulk/data --dry-run'

# 3. No suspect days
ssh root@<SERVER_IP> 'ls /bulk/data/*.warn 2>/dev/null && echo "SUSPECT DAYS FOUND" || echo "clean"'

# 4. Sample row counts are sane
ssh root@<SERVER_IP> '/bulk/.venv/bin/python3 -c "
import pyarrow.parquet as pq, glob, os
for f in sorted(glob.glob(\"/bulk/data/options_*.parquet\"))[::30]:
    print(os.path.basename(f), pq.read_metadata(f).num_rows)
"'

# 5. After rsync to Mac confirmed -- delete server
# https://console.hetzner.com/ -> Server -> Delete
```

---

## Backtester Memory Design (365-day scale)

Loading 371 days with Python object boxing (~24 bytes per float) requires ~14 GB RAM and 90-120s load time. Use columnar NumPy arrays instead.

Target layout (~3 GB RAM for 371 days):

```
Option data (~40M rows):
  timestamp:      int64[40M]       320 MB
  expiry_idx:     uint8[40M]        40 MB  <- index into string table of ~30 expiries
  strike:        float32[40M]      160 MB
  is_call:          bool[40M]       40 MB
  bid/ask/mark/iv/delta: float32   ~800 MB
  underlying_px: float32[40M]      160 MB

Timestamp index (~130k unique 5-min boundaries):
  ts_sorted, ts_starts, ts_lens:     ~2 MB

Spot (~530k 1-min rows):             ~25 MB
```

Loading from per-day parquets:

```python
import pyarrow.dataset as ds
import numpy as np

table = ds.dataset("/path/to/data/", format="parquet").to_table(
    columns=["timestamp","expiry","strike","is_call",
             "bid_price","ask_price","mark_price","mark_iv","delta","underlying_price"]
)
opt_df = table.to_pandas()

expiry_cat   = opt_df["expiry"].astype("category")
expiry_table = list(expiry_cat.cat.categories)
expiry_idx   = expiry_cat.cat.codes.values.astype(np.uint8)
timestamps   = opt_df["timestamp"].values

ts_sorted, ts_starts, ts_counts = np.unique(timestamps, return_index=True, return_counts=True)
ts_lens   = ts_counts.astype(np.int32)
ts_starts = ts_starts.astype(np.int32)
```

Load time: ~10-15 s. Per-tick lookup: vectorised scan over ~300 rows = ~0.5 us.

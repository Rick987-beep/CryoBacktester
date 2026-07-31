# Live vs Backtester Fill Comparison — Eisbach (Jul 2026)

Forensic package comparing CryoTrader **slot-02** (prod) against backtester **run 375** /
combo `42dbac5b4976` for the Jul 1–17, 2026 entry window.

## Quick start

```bash
# From repo root, with venv active:

# 1. Refresh prod blotter (optional — data already included)
bash analysis/livevsbtfills/scripts/pull_live_blotter.sh

# 2. Rebuild trade CSVs from blotter + BT bundle
python analysis/livevsbtfills/scripts/build_comparison.py

# 3. Scan snapshot fill gap (requires backtester/data parquets)
python analysis/livevsbtfills/scripts/analyze_fill_gap.py

# 4. Generate HTML report
python analysis/livevsbtfills/scripts/build_report.py
```

Open **`report.html`** in a browser for the full write-up.

## Contents

| Path | Description |
|------|-------------|
| `report.html` | Branded human-readable report (main deliverable) |
| `metadata.json` | Run IDs, sizing notes, provenance |
| `data/slot-02.jsonl` | Raw prod trade blotter (pulled 2026-07-20) |
| `data/live_jul1_17.csv` | Normalised live trades |
| `data/bt_jul1_17.csv` | Normalised backtester trades |
| `data/day_by_day.csv` | Daily presence + PnL summary |
| `data/fill_gap.csv` | Snapshot bid vs mark scan (generated) |
| `scripts/` | Reproducible extraction and analysis |

## Sources

- **Live:** `root@46.225.137.92:/opt/ct/trade_history/slot-02.jsonl`
- **Live config:** CryoTrader `slots/slot-02.toml`, strategy `tudysho` v1.0.0 Eisbach
- **BT strategy:** `backtester/strategies/tudysho_eisbach.py`
- **BT bundle:** `backtester/reports/tudysho_eisbach_20260720_090214.bundle`

## Headline findings

1. **Jul 8–17 only** is apples-to-apples (live switched from `short_str_turb_dyn` to `tudysho` on Jul 8).
2. When both trade, **$/lot matches closely** — exit logic is sound.
3. **Live trades more often** when OTM wing bids are zero: live uses mark fallback; BT requires `bid > 0`.
4. Jul 7, Jul 9: BT skipped (0 openable snapshot ticks); live filled.
5. Jul 17 BT PnL is an **`end_of_data`** artifact, not a real loss vs live.

See `report.html` for full detail, tables, and follow-up options.

## Related work

Prior draft of this analysis lived in `analysis/eisbach_live_vs_bt/` — this folder is the consolidated,
documented package for future reference.

---
name: livecompare
description: Compare CryoTrader live slot fills to CryoBacktester with parity checks, forensics, and structured reports. Use when the user asks live vs backtest, livecompare, slot-N blotter comparison, forensic trade reconciliation, or fill parity analysis.
---

# Livecompare

## When to use

User wants to compare **live CryoTrader** trades to **CryoBacktester** for a deployment slot.

## Run (do not improvise comparisons)

```bash
python -m backtester.compare run --slot 02 --last-n 7
```

Offline / cached blotter:

```bash
python -m backtester.compare run --slot 02 --last-n 7 --skip-pull
```

Read outputs **in this order**:
1. `analysis/livecompare/LATEST` → run directory
2. `manifest.json` — provenance
3. `warnings.json` — parity gaps **before** interpreting PnL
4. `data/comparison.csv` — use `comparability` column
5. `summary.md` / `report.html` — human narrative

## Interpretation rules

| Rule | Why |
|------|-----|
| Compare **$/lot**, never raw $ | `SIZING_DIFF` always present with bt_default |
| `comparability=EXCLUDE` → do not count in totals | CONFIG_DRIFT, DATA_GAP, STRATEGY_MISMATCH |
| `NO_BT_TRADE` → check FILL_MODEL | BT bid>0 vs live mark fallback |
| `PARTIAL_FILL` → read `forensics.jsonl` | Zero-call / partial put not modelled in BT |
| `end_of_data` BT exit ≠ live loss | DATA_GAP — run date range too short |
| Live `combined_mark_sl` ≈ BT `stop_loss` | Same trigger, different label |

## Comparability codes

- **OK** — same schedule, strikes, exit type; trust $/lot delta
- **WARN** — partial fill, strike mismatch, exit mismatch, config drift
- **EXCLUDE** — legacy strategy, end_of_data, not apples-to-apples

## Prerequisites

- CryoTrader at `CT_ROOT` (default `../CryoTrader`) with `slots/slot-NN.toml`
- Parquet data in `backtester/data/` for the window
- SSH access for blotter pull unless `--skip-pull`

## Extend mapping

Edit `analysis/livecompare/config/strategy_map.yaml` for new live strategies.

See [reference.md](reference.md) for architecture.

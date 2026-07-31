# Livecompare

Compare CryoTrader live slot fills to CryoBacktester with validated parity checks.

## CLI

```bash
# Full pipeline (pull blotter, resolve config, run BT, compare, report)
python -m backtester.compare run --slot 02 --last-n 7

# Offline test (reuse cached blotter, no SSH)
python -m backtester.compare run --slot 02 --last-n 7 --skip-pull

# Custom output dir
python -m backtester.compare run --slot 02 --last-n 7 --out analysis/livecompare/runs/my_run
```

Environment:
- `CT_ROOT` — path to CryoTrader repo (default: `../CryoTrader`)
- `CT_HOST` — SSH host for blotter pull (default: `root@46.225.137.92`)

## Outputs (per run under `runs/YYYY-MM-DD_slotNN_lastN/`)

| File | Purpose |
|------|---------|
| `manifest.json` | Provenance, bundle path, comparability counts |
| `warnings.json` | Parity gaps (sizing, fill model, data) |
| `resolved_config.json` | Live TOML → BT param grid |
| `summary.md` | Short human summary |
| `report.html` | Branded report |
| `data/comparison.csv` | Matched trades + comparability codes |
| `data/forensics.jsonl` | Peculiar fill narratives |

Latest run path: `analysis/livecompare/LATEST`

## Config

- `config/strategy_map.yaml` — live strategy → BT strategy mapping

## Skill

Agent instructions: `.cursor/skills/livecompare/SKILL.md`

# Run audit

Grid-quality autopsy for an **existing** backtester run: parameter influence (η²),
dangerous settings, curve-fitting / multiplicity, and diverse live combo suggestions.

## CLI

```bash
python -m backtester.research.run_audit 748 --html
# alias
python -m backtester.inspect audit 748 --html

# Custom output dir / full JSON on stdout
python -m backtester.research.run_audit 748 --out-dir /tmp/audit748 --html
python -m backtester.research.run_audit 748 --stdout
```

Resolve run identity first if needed:

```bash
python -m backtester.inspect show 748
python -m backtester.inspect runs --strategy tudysho
```

## Outputs (default: `analysis/run_audit/<bundle_stem>/`)

| File | Purpose |
|------|---------|
| `audit.json` | Full pack (`schema_version` 1) — source of truth |
| `report.html` | Optional section-kit document (`--html`) |

Do not commit one-off audit blobs unless they are intentional artefacts.

## Agent skill

`.cursor/skills/run-audit/SKILL.md` — workflow, interpretation rules, flexible HTML
sections. Related: **run-lookup** (identity / trades), **livecompare** (after deploy).

## Module

`backtester/research/run_audit/` — reuses `backtester.inspect` resolve + the same
`_all_combo_stats` metrics path as `inspect combo`.

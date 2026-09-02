---
name: marketing
description: >-
  Build and ship investor strategy reports and due-diligence packs from locked
  backtest combos. Use when generating monthly marketing artefacts, refreshing
  ship HTML/CSVs, adding a promoted product, or zipping allocator-facing files
  under workspace/marketing/ship/.
---

# Strategy marketing — ship reports & diligence

## When to use

- Monthly (or ad-hoc) refresh of **investor-facing** strategy reports and diligence CSVs/HTML
- Adding a **new promoted product** after research funnel closes
- Locating the **canonical ship path** for a product (never guess from old `analysis/` trees)

## Layout (canonical)

```
workspace/marketing/
  ship/{product_id}/          DISTRIBUTABLE — zip for allocators
  _build/{product_id}/        scripts, data/, _internal/, quality.json
  tools/                      export_diligence, build_investor_report, naming, investor_notes, social_square
  catalog.json                internal inventory (run/combo/rebuild commands)
  archive/                    superseded ship vintages
```

**Rule:** only `ship/` is investor-safe. Never put scripts, run ids, combo hashes, or rebuild CSVs in `ship/`.

## Filenames (locked convention)

| Artefact | Pattern |
|----------|---------|
| Strategy report | `{product_id}_strategyreport_{MMYYYY}.html` |
| NAV daily | `{product_id}_nav_daily_{MMYYYY}.csv` |
| NAV extended | `{product_id}_nav_daily_extended_{MMYYYY}.csv` |
| Trades | `{product_id}_trades_{MMYYYY}.csv` / `.json` / `.html` |
| Social square | `{product_id}_social_{MMYYYY}.png` (1080×1080 LinkedIn / X) |

`MMYYYY` = report vintage (usually backtest window **end month**), e.g. `082026`.  
Helpers: `workspace/marketing/tools/naming.py`. Paths in `catalog.json`.

## Promoted products (current)

Read [`workspace/marketing/catalog.json`](../../workspace/marketing/catalog.json) — do not hard-code run/combo in ship copy.

| Product id | Display name |
|------------|--------------|
| `defined_theta` | Defined Theta |
| `monopteros` | Monopteros |
| `lenbach` | Lenbach |

## Monthly refresh workflow

1. **Check lock** — `catalog.json` run/combo still current? If the funnel moved, use **run-lookup** first.
2. **Refresh rebuild inputs** (only if lock or window changed):
   ```bash
   python -m backtester.inspect combo RUN HASH --full   # params
   python -m backtester.inspect trades RUN HASH --out workspace/marketing/_build/{id}/data/trades.csv
   # NAV: export combo equity_daily to _build/{id}/data/equity_daily.csv (nav_close column required)
   ```
3. **Rebuild strategy report** (requires agent-commons on `PYTHONPATH`):
   ```bash
   PYTHONPATH="$HOME/agent-commons" python workspace/marketing/_build/{id}/render.py      # defined_theta
   PYTHONPATH="$HOME/agent-commons" python workspace/marketing/_build/{id}/build_report.py  # monopteros, lenbach
   ```
4. **Export diligence + social square** (pandas + headless Chrome for PNG):
   ```bash
   python workspace/marketing/export_diligence.py           # all products
   python workspace/marketing/export_diligence.py lenbach # one product
   # social-only: python workspace/marketing/tools/social_square.py
   ```
5. **New vintage?** Move prior `ship/{id}/*_{OLDMMYYYY}.*` → `archive/{id}/{YYYY-MM-DD}_{reason}/`. Update `catalog.json` (`report_period`, diligence paths, `report_html`).
6. **Verify** — `python -m pytest workspace/tests/test_marketing.py -v`

## Adding a new promoted product

1. Close research (run-audit / WFO as needed); lock run + combo in **run-lookup**.
2. Create `_build/{product_id}/`:
   - `data/equity_daily.csv`, `data/trades.csv`
   - `_internal/` for params/combo_stats (optional; do not distribute)
   - `build_report.py` or `render.py` with `StrategyReportCopy` + `build_investor_report()`
3. Set `REPORT_PERIOD` and product copy in the build script; wire `ship_report_path(_MARKETING, product_id, period)`.
4. Add entry to `catalog.json` (`products[]` with `report_html`, `diligence`, `rebuild` commands).
5. Run build + `export_diligence.py {product_id}` (writes diligence + social PNG).
6. Confirm `ship/{product_id}/` has 7 files and passes investor tests.

## Social square

Locked 1080×1080 eye-catcher (`tools/social_square.py`): report-aligned dark header, daily equity + drawdown (Chart.js), three KPIs, Aureas GmbH footer. Pitch scraped from the ship report `header-subtitle`. Preview: `_build/social_square_preview.html`. Requires Google Chrome / Chromium for PNG.

## Investor copy rules

- Build scripts set `performance_lead_html` and `notes_html` via `tools/investor_notes.py` — **no post-hoc sanitization**.
- Ship HTML/JSON must not contain: run ids, combo hashes, strategy module names, "engine lock", "replayed", "chain replay".
- Internal metadata stays in `catalog.json` and `_build/_internal/` only.

## Do not use

- `analysis/defined_theta_strategy_report*` — historical v1–v3 iterations; not canonical ship paths.
- `analysis/marketing/` — redirect stub only.
- Old per-product folders at `workspace/marketing/{id}/` (removed; use `ship/` + `_build/`).

## Distribute

Zip `workspace/marketing/ship/{product_id}/` (7 files) or entire `ship/` for all products.

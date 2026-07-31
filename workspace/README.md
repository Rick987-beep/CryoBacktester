# Workspace — research use artifacts

Strategies, experiments, and strategy unit tests live here (not in the
shippable `backtester/` product package).

## Layout

```
workspace/
  catalog.py              # families + stable strategy IDs
  strategies/
    tudysho/
    theta_engine/
    other/
  experiments/            # TOML experiment definitions
  tests/                  # strategy unit tests
```

## Rules

- **Never rename strategy IDs** (`theta_engine_v6`, `tudysho_eisbach`, …).
  Bundles, favourites, livecompare, and experiments key off these strings.
- Register new strategies in [`catalog.py`](catalog.py) (family + status).
- Compatibility shims under `backtester/strategies/` re-export these modules.
- **Family** is a lightweight taxonomy for UI grouping and folder layout — not a
  separate persistence layer. Unknown IDs map to family `other`.

## Families

| Family | Examples |
|---|---|
| `tudysho` | tudysho, eisbach, starnberg, stradysho, v1/v2 |
| `theta_engine` | v1–v6 |
| `other` | blueprint_howto, cadysho, one-offs (dump family) |

## Add a new family

1. Create `workspace/strategies/<family_id>/` with an `__init__.py`.
2. Add a `Family(...)` entry to `FAMILIES` in [`catalog.py`](catalog.py).
3. Put strategy modules in that directory and register `StrategySpec(..., family="<family_id>", ...)`.
4. Optionally add shims under `backtester/strategies/` for old import paths.
5. UI Family filters (New Run, Runs, Favourites) pick up `FAMILIES` automatically.

## Add a strategy to an existing family

1. Add the module under the family directory.
2. Append a `StrategySpec` in `catalog.py` with the right `family` and a **new stable id**.
3. Optional shim under `backtester/strategies/<id>.py`.

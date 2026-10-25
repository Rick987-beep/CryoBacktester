# How to Write a New Strategy (demo)

This is a short guide for the **CryoBacktester demo** package. The annotated
reference implementation is `backtester/strategies/blueprint_howto.py`.

## Responsibilities

A strategy is a plain Python class that implements:

- `configure(params)` — wire exits / state from the param grid
- `on_market_state(state)` — return `Trade` objects for opens/closes
- `on_end()` / `reset()` as needed

Keep open positions in `self._positions` (a `List[OpenPosition]`). The engine
marks them to market each tick for NAV.

## Registration (demo)

In this demo package, register strategies in `backtester/public_catalog.py`
and place the module under `backtester/strategies/`. Stable IDs must not be renamed.

```python
# backtester/public_catalog.py
from backtester.strategies.my_strategy import MyStrategy
...
StrategySpec("my_strategy", "other", MyStrategy),
```

## Core helpers

```python
from backtester.core.option_selection import select_by_delta
from backtester.core.expiry_utils import expiry_dt_utc, select_expiry
from backtester.core.pricing import deribit_fee_per_leg
from backtester.core.strategy_base import (
    OpenPosition, Trade, check_expiry, close_position,
    price_legs, profit_target_pct, stop_loss_pct,
)
```

- SL: prefer `price_mode="mark"`
- TP: prefer `price_mode="executable"`
- `mark_iv` in parquet is already a **percentage** (e.g. `34.4` = 34.4%)

## PARAM_GRID and DATE_RANGE

`PARAM_GRID` expands into every combo; `DATE_RANGE` is the replay window.
In the demo, `blueprint_howto` uses a short window inside the bundled sample data.

## Checklist

- [ ] Legs carry the required open/close fields (see blueprint)
- [ ] `metadata` has `direction`, `expiry`, `expiry_dt`, `pos_id`
- [ ] Registered in `backtester/public_catalog.py`
- [ ] `python -m pytest tests/ -v` passes
- [ ] `python -m backtester.run --strategy <id>` produces a report under `data/runs/`

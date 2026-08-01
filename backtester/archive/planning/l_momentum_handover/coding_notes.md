# l_momentum — Coding Notes for AI Agents

This document is addressed to AI coding assistants working on the `l_momentum` strategy or
the indicator system it depends on. It records implementation decisions, pitfalls, and
patterns that are not obvious from reading the code alone.

---

## 1. Indicator system — how `spot_mom_4h` and `spot_mom_1h` were added

### Registration
`backtester/indicators.py` has a dict called `_BUILDERS`:

```python
_BUILDERS: dict[str, Callable] = {
    ...
    "spot_mom_4h": _build_spot_momentum,
    "spot_mom_1h": _build_spot_momentum,
}
```

A builder function is looked up by the `IndicatorDep.name` field at backtest start.
Both names share **the same builder function** — the builder just returns `pct_change(1) * 100`
on the close column of whatever OHLCV data `hist_data.py` provides for the given symbol/interval.

### Builder function (lines ~325–328 in `backtester/indicators.py`)

```python
def _build_spot_momentum(dep: IndicatorDep) -> pd.Series:
    df_raw = _load_spot_klines(dep.symbol, dep.interval, warmup_days=dep.warmup_days)
    return df_raw["close"].pct_change(1) * 100.0
```

`_load_spot_klines` calls `hist_data.get_klines(symbol, interval)` from `indicators/hist_data.py`
which returns a DataFrame with a UTC DatetimeIndex and columns `open, high, low, close, volume`.

### Adding a new indicator builder
1. Write a function `_build_<name>(dep: IndicatorDep) -> pd.Series`
2. Register it in `_BUILDERS` under the string key that will be used in `IndicatorDep.name`
3. Declare it in the strategy class: `indicator_deps = [IndicatorDep(name="...", ...)]`

---

## 2. Timezone requirement for indicator series

**Critical rule:** The Series index **must be timezone-aware (UTC)**. The backtest engine
calls `strategy.set_indicators(ind)` and the strategy uses `series.loc[some_dt]` where
`some_dt` is derived from `state.dt`, which is always `tz_aware (UTC)`.

Pandas `.loc` on a tz-naive Series using a tz-aware key raises `KeyError` (or `TypeError`
in some versions). Symptoms: test passes but prod backtest silently looks up wrong bar, or
tests fail with confusing key errors.

In `_build_spot_momentum`, `hist_data.get_klines` returns a UTC-indexed DataFrame.
If you write a new builder that constructs a Series manually (e.g. in tests), always:

```python
import pandas as pd
idx = pd.date_range("2026-01-01", periods=10, freq="4h", tz="UTC")
series = pd.Series(data, index=idx)
```

In test fixtures, verify with `assert series.index.tzinfo is not None`.

---

## 3. Bar timing convention for `_lookup_mom`

The module-level helper function in `l_momentum.py`:

```python
def _lookup_mom(series: pd.Series, dt: datetime, interval_h: int) -> float:
    bar_open = dt.replace(minute=0, second=0, microsecond=0) - timedelta(hours=interval_h)
    try:
        return float(series.loc[bar_open])
    except KeyError:
        return 0.0
```

**Convention:** at a 4h boundary tick at time T, the *just-closed* bar has `open_time = T − interval`.

Examples:
- Tick at 16:00 UTC, interval=4h → bar to use has open_time 12:00 UTC
- Tick at 16:00 UTC, interval=1h → bar to use has open_time 15:00 UTC

This is correct for Deribit snapshot data where snapshots arrive after the bar closes.
If `series.loc[bar_open]` raises `KeyError` (data gap), the function returns `0.0` — this
silently suppresses the signal for that tick, which is safe for backtesting.

---

## 4. Leg dict — `entry_price_usd` is mandatory for fills

The engine (`backtester/engine.py`) populates the fills table via:

```python
amount_usd = leg.get("entry_price_usd", 0.0)
```

If `"entry_price_usd"` is missing from the dict returned by `_try_entry`, all fill rows
will have `amount_usd = 0.0`. The trade PnL in `trade_log.parquet` is still correct
(it uses the strategy's own accounting), but the fills table will be wrong.

In `l_momentum.py` the leg dict is:
```python
leg = {
    "instrument": best.instrument_name,
    "direction": "buy",
    "qty": 1,
    "entry_price": best.ask,          # BTC-denominated (for Deribit display)
    "entry_price_usd": entry_ask_usd, # USD-denominated (for fills table)
    "entry_spot": state.spot,
    "option_type": option_type,
    "mark_price": best.mark,
}
```

This bug was present in the initial implementation and was fixed before the final backtest.
When porting to a new strategy, always include `"entry_price_usd"` in the leg dict.

---

## 5. Strategy registration

To register a new strategy, add two things in `backtester/run.py`:

```python
# Import
from backtester.strategies.l_momentum import LMomentum

# In STRATEGIES dict
STRATEGIES: dict[str, type] = {
    ...
    "l_momentum": LMomentum,
}
```

Then run with: `python -m backtester.run --strategy l_momentum`

---

## 6. Test patterns

### tz-aware mock series
All mock indicator Series in tests must use UTC-aware indices:

```python
_ENTRY_DT = datetime(2026, 5, 15, 16, 0, tzinfo=timezone.utc)

def _mom_series_4h(values: list[float]) -> pd.Series:
    """Build a UTC-aware 4h momentum Series covering the test entry window."""
    idx = pd.date_range(
        end=_ENTRY_DT - timedelta(hours=4),
        periods=len(values), freq="4h", tz="UTC"
    )
    return pd.Series(values, index=idx)
```

If you use naive datetimes, `series.loc[state.dt - timedelta(hours=4)]` will raise `KeyError`.

### Unified call/put in get_chain / get_option mocks

The mock `get_chain` returns both calls and puts in a single list. `get_option` likewise
accepts both. This is required because the strategy may request either direction depending
on the signal. Tests set up one call and one put object with appropriate `option_type`,
`delta`, `ask`, `bid`, `mark` fields.

```python
def _make_option(option_type="call", ask=0.010, bid=0.009, ...):
    return SimpleNamespace(
        instrument_name=f"BTC-15MAY26-93000-{'C' if option_type=='call' else 'P'}",
        option_type=option_type,
        strike=93_000,
        expiry=...,
        delta=0.35 if option_type=="call" else -0.35,
        ask=ask,
        bid=bid,
        mark=ask,
        dte=5,
    )
```

---

## 7. Recency gate — understand before touching scoring

The backtester config (`backtester/config.toml`) has:
```toml
recency_gate_enabled = true
recency_gate_sharpe  = 0.3
recency_pct          = 0.20
```

This vetos any combo whose Sharpe over the **last `recency_pct * total_days` of the backtest**
is below `recency_gate_sharpe`. For a 131-day run that is the last 26 days.

All 243 `l_momentum` combos have `recent_sharpe` in the range `−8.4 → 0.0` — the strategy
genuinely underperformed in April–May 2026. The composite score for every combo is `0.0`.

**This is not a code bug.** The full-period metrics (Sharpe ~1.8, positive PnL) are real.
Options for research: disable the gate in config, lower `recency_gate_sharpe`, or investigate
what made the last 26 days unfavourable.

---

## 8. Backtest data range

Strategy constant: `DATE_RANGE = ("2026-01-01", "2026-05-12")`

This is set in the strategy class and overrides the config default. The data ends at
2026-05-12 because that was the latest available parquet snapshot at the time the strategy
was written. When newer data is ingested, update `DATE_RANGE` or remove the override to
let the config default apply.

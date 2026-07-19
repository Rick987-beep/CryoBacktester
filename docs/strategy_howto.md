# How to Write a New Strategy

*Last updated: June 2026 — reflects price_legs / stop_loss_pct(price_mode) / profit_target_pct(price_mode) refactor.*

---

## Table of Contents

1. [Overview: what a strategy is responsible for](#1-overview)
2. [What the engine does for you](#2-what-the-engine-does)
3. [The Strategy protocol](#3-the-strategy-protocol)
4. [OpenPosition and the leg model](#4-openposition-and-the-leg-model)
5. [Opening a position — the open Trade pattern](#5-opening-a-position)
6. [Closing a position — `close_position`](#6-closing-a-position)
7. [Partial closes and multi-leg mutations — `partial_close` / `add_legs`](#7-partial-closes-and-add_legs)
8. [Exit condition helpers](#8-exit-condition-helpers)
9. [Expiry utilities](#9-expiry-utilities)
10. [Option selection utilities](#10-option-selection-utilities)
11. [Fees](#11-fees)
12. [Indicators](#12-indicators)
13. [PARAM_GRID conventions](#13-param_grid-conventions)
14. [Registering the strategy](#14-registering-the-strategy)
15. [What NOT to use](#15-what-not-to-use)
16. [Checklist](#16-checklist)

---

## 1. Overview

A strategy is a **pure Python class** — no base class, no framework inheritance.
It implements a structural protocol (see §3) and signals positions by returning
`Trade` objects from `on_market_state`.

**Strategy responsibilities:**
- Decide when to open a position (entry logic, gates, filters).
- Select the right option legs from the snapshot chain.
- Track open positions in `self._positions` — a plain `List[OpenPosition]`.
  **This is mandatory.** The engine reads `strategy._positions` on every tick to
  mark open positions to market for NAV and intraday drawdown tracking.  Strategies
  that store positions under any other name (e.g. `_pos`, `_call_pos`) will have a
  flat equity curve between trades.
  Append on open, remove on close.  For strategies with named slots (e.g. a call
  slot and a put slot), expose a `@property _positions` that returns the live slots
  as a list — no manual bookkeeping needed.
- Decide when to close (exit conditions, expiry, TP, SL).
- Annotate each leg with exit prices before calling `close_position`.
- Return `Trade` objects for every open and every close event.

**Strategy does NOT:**
- Compute P&L or fees for the trade log — the engine does that from leg annotations.
- Track NAV, equity, or account balance.
- Load market data or indicators (the engine pre-computes them).
- Know anything about the grid runner or result aggregation.

---

## 2. What the engine does

The engine drives the replay loop and handles all accounting:

| Engine responsibility | How |
|---|---|
| Iterate 5-min snapshots | `MarketReplay` → `on_market_state(state)` each tick |
| Mark open positions to market | Calls `_reprice_legs()` on every open `OpenPosition` each tick |
| Track per-combo NAV and realized PnL | Accumulates from closed `Trade` objects |
| Emit per-leg fills to `df_fills` | Reads `trade.metadata['legs']` from open/close Trades |
| Link open fills to close fills | Via `pos.metadata['pos_id']` |
| Compute metrics (Sharpe, DSR, etc.) | `GridResult` after the replay |
| Generate the HTML report | `reporting_v2.generate_html()` |

The engine reads `trade.side` (`"open"` or `"close"`) to decide what to emit.
For `side="close"` Trades that set `trade.metadata["skip_open_fill"] = True`, the
engine assumes open fills were already emitted by the earlier `side="open"` Trade —
it only emits close fills. This is the standard pattern for new strategies.

---

## 3. The Strategy protocol

Implement these five methods and two class attributes:

```python
class MyStrategy:
    name = "my_strategy"          # string key used in run.py registry and reports
    DATE_RANGE = ("2025-01-01", "2026-01-01")   # default backtest window (YYYY-MM-DD)
    DESCRIPTION = "One-sentence description."
    PARAM_GRID = { ... }          # see §13

    # Optional — declare indicator dependencies (see §12)
    indicator_deps = []           # type: List[IndicatorDep]

    def configure(self, params):
        # type: (Dict[str, Any]) -> None
        """Called once per grid combo before the replay starts.
        Parse all parameters from `params`. Also reset all instance state here —
        configure() doubles as reset-for-next-combo."""
        ...

    def on_market_state(self, state):
        # type: (Any) -> List[Trade]
        """Called on every 5-min tick. Return a list of Trade objects:
        - one side='open' Trade when a position is opened
        - one side='close' Trade when a position is closed
        - empty list when nothing happened"""
        ...

    def on_end(self, state):
        # type: (Any) -> List[Trade]
        """Called once at end of data. Force-close all open positions."""
        ...

    def reset(self):
        # type: () -> None
        """Called between grid combos. Clear positions and all per-run state."""
        ...

    def describe_params(self):
        # type: () -> Dict[str, Any]
        """Return current parameter values. Used for result labeling."""
        ...

    # Optional — called once by the engine after indicators are built
    def set_indicators(self, ind):
        # type: (Dict[str, Any]) -> None
        ...
```

### `configure` is the canonical reset point

`configure` is called fresh for every parameter combo. Put all instance state
initialisation there — `self._positions = []`, counters, watched state, etc.
`reset()` should mirror it (the engine calls `reset()` instead of `configure()`
between runs without re-parsing params, so keep them in sync).

> **NAV tracking contract:** the engine locates open positions via
> `strategy._positions` (a `List[OpenPosition]`).  Initialise it as `[]` in
> both `configure` and `reset`.  For multi-slot strategies where each slot has
> its own named pointer (e.g. `self._call_pos`, `self._put_pos`), add a property:
>
> ```python
> @property
> def _positions(self):
>     return [p for p in (self._call_pos, self._put_pos) if p is not None]
> ```
>
> **Never** name the position `self._pos`, `self._position`, or anything else —
> the engine only looks for `_positions` and will silently skip NAV marking if
> the attribute is absent or not a list.

---

## 4. OpenPosition and the leg model

```python
from backtester.strategy_base import OpenPosition

pos = OpenPosition(
    entry_time=state.dt,
    entry_spot=state.spot,
    legs=[...],               # list of leg dicts — see below
    entry_price_usd=total,    # sum of |leg premium| × quantity (USD)
    fees_open=total_fees,     # opening fees (USD, all legs combined)
    metadata={
        "direction":  "sell",  # "sell" (short premium) or "buy" (long premium)
        "expiry":     expiry,  # expiry code string e.g. "21MAY26"
        "expiry_dt":  exp_dt,  # datetime with tzinfo — used by check_expiry()
        "pos_id":     pos_id,  # monotonic int from _next_pos_id() — links fills
        # ... strategy-specific fields
    },
)
```

### Leg dict — mandatory fields

Every leg dict must carry these fields **at open time**:

```python
{
    "strike":          float,      # strike price (USD)
    "is_call":         bool,       # True = call, False = put
    "expiry":          str,        # Deribit expiry code
    "side":            "sell",     # "sell" (short) or "buy" (long) — drives price_legs() per-leg pricing
    "qty":             float,      # number of contracts (Deribit min 0.1)
    "price_btc":       float,      # fill price at open (BTC per contract)
    "entry_price":     float,      # same as price_btc (alias used by close_position)
    "entry_price_usd": float,      # price_btc × spot (per contract, not scaled by qty)
    "entry_spot":      float,      # BTC spot at open — used for USD PnL math at close
    "entry_bid":       float,      # bid at open (for logs and reporting)
    "entry_ask":       float,      # ask at open
    "entry_mark":      float,      # mark at open
    "entry_iv":        float,      # mark_iv from parquet — already % (e.g. 34.4 = 34.4%)
    "entry_delta":     float,      # delta at open
    "fee_usd_open":    float,      # opening fee for this leg (USD, unscaled)
}
```

> **IV note:** `mark_iv` in the parquet is stored as a percentage (e.g. `34.4` = 34.4%).
> Do NOT multiply by 100. Do NOT divide by 100 when storing in leg dict.

These fields are added **at close time** (before calling `close_position`):

```python
leg["exit_price_btc"] = float   # fill price at close (BTC per contract)
leg["exit_price_usd"] = float   # exit_price_btc × exit_spot (per contract)
```

> `fee_btc_close` is optional. If omitted, `close_position` uses `fees_close` passed directly.

### pos_id tracking

Every position must have a `pos_id` integer in its metadata. This links the
open fill rows to the close fill rows in `df_fills`:

```python
def __init__(self):
    self._pos_counter = 0

def _next_pos_id(self):
    self._pos_counter += 1
    return self._pos_counter
```

Reset `self._pos_counter = 0` in both `configure` and `reset`.

---

## 5. Opening a position

When you open a position, you **must** return an explicit `Trade(side="open")`.
The engine reads `trade.metadata["legs"]` to emit open fills.

```python
from backtester.strategy_base import OpenPosition, Trade
from backtester.pricing import deribit_fee_per_leg

def _open_strangle(self, state, expiry, exp_dt, calls, puts):
    # ... select legs, validate quotes ...
    fee_call = deribit_fee_per_leg(state.spot, call_usd)
    fee_put  = deribit_fee_per_leg(state.spot, put_usd)

    legs = [
        {"strike": call.strike, "is_call": True,  "expiry": expiry, "side": "sell",
         "qty": qty, "price_btc": call.bid, "entry_price": call.bid,
         "entry_price_usd": call_usd, "entry_spot": state.spot,
         "entry_bid": call.bid, "entry_ask": call.ask, "entry_mark": call.mark,
         "entry_iv": call.mark_iv, "entry_delta": call.delta, "fee_usd_open": fee_call},
        {"strike": put.strike,  "is_call": False, "expiry": expiry, "side": "sell",
         "qty": qty, "price_btc": put.bid,  "entry_price": put.bid,
         "entry_price_usd": put_usd, "entry_spot": state.spot,
         "entry_bid": put.bid, "entry_ask": put.ask, "entry_mark": put.mark,
         "entry_iv": put.mark_iv, "entry_delta": put.delta, "fee_usd_open": fee_put},
    ]
    pos_id = self._next_pos_id()
    pos = OpenPosition(
        entry_time=state.dt, entry_spot=state.spot,
        legs=legs,
        entry_price_usd=(call_usd + put_usd) * qty,
        fees_open=(fee_call + fee_put) * qty,
        metadata={"direction": "sell", "expiry": expiry, "expiry_dt": exp_dt,
                  "call_strike": call.strike, "put_strike": put.strike,
                  "quantity": qty, "pos_id": pos_id},
    )
    self._positions.append(pos)
    self._last_trade_date = state.dt.date()

    # Return an open Trade — engine emits open fills from this.
    return Trade(
        entry_time=state.dt, exit_time=state.dt,
        entry_spot=state.spot, exit_spot=state.spot,
        entry_price_usd=(call_usd + put_usd) * qty,
        exit_price_usd=0.0, fees=(fee_call + fee_put) * qty,
        pnl=0.0, triggered=False, exit_reason="", exit_hour=0,
        entry_date=state.dt.strftime("%Y-%m-%d"),
        side="open",
        metadata={"direction": "sell", "pos_id": pos_id, "legs": legs},
    )
```

Return this Trade from `on_market_state` alongside any close Trades:

```python
def on_market_state(self, state):
    trades = []
    # ... exit logic ...
    open_trade = self._maybe_open(state)
    if open_trade is not None:
        trades.append(open_trade)
    return trades
```

---

## 6. Closing a position

Use `close_position` (the canonical close path). Before calling it, annotate
each leg with exit prices and fees:

```python
from backtester.strategy_base import close_position
from backtester.pricing import deribit_fee_per_leg

def _close(self, state, pos, reason):
    expiry      = pos.metadata["expiry"]
    call_strike = pos.metadata["call_strike"]
    put_strike  = pos.metadata["put_strike"]
    quantity    = float(pos.metadata.get("quantity", 1.0))

    if reason == "expiry":
        # Intrinsic settlement — no fee on Deribit at expiry
        call_exit_usd = max(0.0, state.spot - call_strike)
        put_exit_usd  = max(0.0, put_strike  - state.spot)
        call_exit_btc = call_exit_usd / state.spot if state.spot else 0.0
        put_exit_btc  = put_exit_usd  / state.spot if state.spot else 0.0
        fee_call = fee_put = 0.0
    else:
        _min_tick_btc = 0.0001
        _min_tick_usd = _min_tick_btc * state.spot
        call_q = state.get_option(expiry, call_strike, True)
        put_q  = state.get_option(expiry, put_strike,  False)
        call_exit_usd = call_q.ask_usd if call_q and call_q.ask > 0 else _min_tick_usd
        put_exit_usd  = put_q.ask_usd  if put_q  and put_q.ask  > 0 else _min_tick_usd
        call_exit_btc = call_q.ask if call_q and call_q.ask > 0 else _min_tick_btc
        put_exit_btc  = put_q.ask  if put_q  and put_q.ask  > 0 else _min_tick_btc
        fee_call = deribit_fee_per_leg(state.spot, call_exit_usd)
        fee_put  = deribit_fee_per_leg(state.spot, put_exit_usd)

    for leg in pos.legs:
        if leg["is_call"]:
            leg["exit_price_btc"] = call_exit_btc
            leg["exit_price_usd"] = call_exit_usd
            leg["fee_btc_close"]  = fee_call / state.spot if state.spot else 0.0
        else:
            leg["exit_price_btc"] = put_exit_btc
            leg["exit_price_usd"] = put_exit_usd
            leg["fee_btc_close"]  = fee_put / state.spot if state.spot else 0.0

    trade = close_position(
        state, pos, reason,
        current_usd=(call_exit_usd + put_exit_usd) * quantity,
        fees_close=(fee_call + fee_put) * quantity,
    )
    # Signal to engine: open fills already emitted by the open Trade
    trade.metadata["skip_open_fill"] = True
    # Add strategy-specific metadata for reporting
    trade.metadata["leg_type"] = "strangle"
    return trade
```

### Data gap guard

Before closing on a non-expiry reason, confirm quotes are available — otherwise
skip the tick (the exit will fire on the next tick when data returns):

```python
if reason and reason != "expiry":
    if state.get_option(expiry, call_strike, True) is None:
        reason = None   # data gap — retry next tick
```

---

## 7. Partial closes and `add_legs`

For calendar spreads or any strategy that mutates a live position (rolling a leg,
closing one leg of a multi-leg spread):

```python
from backtester.strategy_base import partial_close, add_legs

# Close only the short leg (index 0), keep the long leg (index 1)
leg["exit_price_btc"] = exit_btc      # annotate before calling
partial_trade = partial_close(state, pos, leg_indices=[0], reason="roll",
                               fees_close=close_fee_usd)
partial_trade.metadata["skip_open_fill"] = True

# Later: add a new short leg to the surviving position
new_leg = {"strike": ..., "is_call": ..., "expiry": new_expiry, "side": "sell",
           "qty": qty, "price_btc": new_leg_q.bid, "entry_price": new_leg_q.bid,
           "entry_price_usd": new_leg_q.bid_usd, "entry_delta": ..., "fee_usd_open": fee}
open_trade = add_legs(state, pos, [new_leg], fees_open=fee)
```

`partial_close` mutates `pos.legs` in place (removes the closed legs) and
reduces `pos.entry_price_usd` and `pos.fees_open` proportionally.
`add_legs` appends the new legs to `pos.legs` and increases `pos.entry_price_usd`.

See `cal_premium_collect.py` for a full working example with weekly rolls.

---

## 8. Exit condition helpers

All helpers are in `backtester.strategy_base`. They are **callables** that return
`None` (hold) or a reason string (exit).

### Composable exit factories

```python
from backtester.strategy_base import (
    stop_loss_pct, profit_target_pct, max_hold_hours, max_hold_days,
    time_exit, index_move_trigger, price_legs,
)

# Compose in configure():
self._exit_conditions = [
    stop_loss_pct(self._sl_pct, price_mode="mark"),   # mark = stable, not manipulable
]
if self._tp_pct > 0:
    self._exit_conditions.append(
        profit_target_pct(self._tp_pct, price_mode="executable"),  # executable = real bid/ask
    )
if self._max_hold_hours > 0:
    self._exit_conditions.append(max_hold_hours(self._max_hold_hours))

# Apply in on_market_state():
for exit_cond in self._exit_conditions:
    reason = exit_cond(state, pos)
    if reason:
        break
```

### `price_mode` — the key design choice

`stop_loss_pct` and `profit_target_pct` both accept a `price_mode` kwarg that
controls which option price is used to evaluate the condition:

| mode | Price used | When to use |
|---|---|---|
| `"mark"` | Exchange model price (`mark_usd`) | **SL checks** — stable, not manipulable by wide bid/ask spreads in thin books |
| `"executable"` | Ask for sell legs, bid for buy legs | **TP checks** — only fires when a real market price exists at which you can exit |
| `"bid"` | Always bid regardless of side | Special analytics |
| `"ask"` | Always ask regardless of side | Special analytics |

**Rule of thumb: SL uses mark, TP uses executable.**
With a wide spread (bid=0.0005, ask=0.0050, mark=0.0012), executable SL fires
from the ask spike but mark SL stays calm. TP only fires when the bid/ask confirms
you can actually exit at that price.

### `price_legs(state, pos, mode)` — low-level pricing

```python
from backtester.strategy_base import price_legs

# Price all legs at their current mark
current_mark_usd = price_legs(state, pos, mode="mark")

# Price all legs at executable prices (ask for sell legs, bid for buy)
current_exec_usd = price_legs(state, pos, mode="executable")
```

Returns `None` only when a quote row is entirely absent from the snapshot (genuine
data gap). Never returns `None` for zero-mark options — those are priced at `$0`.
Also writes the result to `pos._last_reprice_usd` (engine NAV cache).

`_reprice_legs(state, pos)` is a backward-compat alias for
`price_legs(state, pos, mode="executable")`. The engine still imports it.

### `stop_loss_pct` semantics

```python
stop_loss_pct(pct, price_mode="mark")
# pct is a fraction: 1.5 = 150% of premium.
# SHORT: fires when mark cost to close = (1 + pct) × collected premium
# LONG:  fires when mark value has fallen pct below entry cost
# Reads pos.metadata["direction"] ("sell"/"buy") for direction.
```

### `profit_target_pct` semantics

```python
profit_target_pct(pct, price_mode="executable")
# pct is a fraction: 0.30 = 30% of premium.
# SHORT: fires when executable buyback cost <= (1 - pct) × collected premium
# LONG:  fires when executable bid value >= (1 + pct) × entry cost
# Reads pos.metadata["direction"] ("sell"/"buy") for direction.
```

### Expiry exit

```python
from backtester.strategy_base import check_expiry

reason = check_expiry(state, pos)
# Returns "expiry" when state.dt >= pos.metadata["expiry_dt"], else None.
# Requires "expiry_dt" (tz-aware datetime) in pos.metadata — set at open.
```

### Take-profit helper (strangle-specific, legacy)

```python
from backtester.strategy_base import check_take_profit_strangle

reason = check_take_profit_strangle(state, pos, self._tp_pct)
# Available but not preferred for new strategies.
# Uses raw ask prices; does not support price_mode.
# Prefer profit_target_pct(pct, price_mode="executable") instead.
```

---

## 9. Expiry utilities

Import from `backtester.expiry_utils`:

```python
from backtester.expiry_utils import (
    parse_expiry_date,      # "21MAY26" → datetime(2026, 5, 21)
    expiry_dt_utc,          # "21MAY26", tzinfo → datetime at 08:00 UTC
    select_expiry,          # find expiry exactly N days from now
    select_expiry_for_week, # find expiry in [N*7, N*7+6] DTE bucket
    nearest_valid_expiry,   # nearest expiry not yet past 08:00 UTC
)
```

**`select_expiry(state, dte)`** — returns the expiry code that is exactly `dte`
calendar days from today. Returns `None` if no matching expiry exists in the
snapshot; the calling strategy should silently skip entry when `None`.

**`select_expiry_for_week(state, weeks)`** — finds an expiry in the DTE bucket
`[weeks*7, weeks*7+6]`. Useful for weekly-roll calendars. `weeks=6` ≈ 45 DTE.

**`nearest_valid_expiry(state)`** — the closest expiry that hasn't settled yet
(handles the 08:00 UTC settlement cut-off). Use for 0DTE strategies.

**`parse_expiry_date` and `expiry_dt_utc`** are `lru_cache`-decorated — safe to
call in hot loops; they run the regex only on first call per code.

**`state.expiries()`** returns all expiry codes present in the current snapshot.

---

## 10. Option selection utilities

Import from `backtester.bt_option_selection` (NOT from `option_selection` — that
module is for the live trading system and has an incompatible interface):

```python
from backtester.bt_option_selection import select_by_delta, apply_min_otm
```

**`select_by_delta(chain, target_delta)`** — returns the `OptionQuote` in
`chain` whose delta is closest to `target_delta`. Pass `+delta` for calls,
`-delta` for puts (e.g. `select_by_delta(puts, -0.15)`).

**`apply_min_otm(chain, selected, spot, min_pct, is_call)`** — if the selected
option is within `min_pct`% of spot, pushes to the nearest qualifying OTM
strike. Returns `None` if no qualifying strike exists.

### Reading the chain

```python
chain = state.get_chain(expiry)          # all OptionQuote for this expiry
calls = [q for q in chain if q.is_call]
puts  = [q for q in chain if not q.is_call]

call = select_by_delta(calls, +self._delta)   # returns OptionQuote or None
put  = select_by_delta(puts,  -self._delta)
if call is None or put is None:
    return None  # chain empty or delta not found — skip entry
```

### OptionQuote fields

| Field | Type | Description |
|---|---|---|
| `strike` | `float` | Strike price (USD) |
| `is_call` | `bool` | True = call |
| `expiry` | `str` | Expiry code |
| `bid` | `float` | Best bid (BTC) |
| `ask` | `float` | Best ask (BTC) |
| `bid_usd` | `float` | `bid × spot` |
| `ask_usd` | `float` | `ask × spot` |
| `mark` | `float` | Mark price (BTC) |
| `mark_usd` | `float` | `mark × spot` |
| `delta` | `float` | Option delta |
| `spot` | `float` | BTC spot at this tick |

**Never trade on zero bid/ask**: always guard before opening:
```python
if call.bid <= 0 or put.bid <= 0:
    return None
```
Use `0.0001 BTC` as the minimum-tick fallback only on close (you can never get out cheaper than one tick on Deribit).

---

## 11. Fees

```python
from backtester.pricing import deribit_fee_per_leg

fee_usd = deribit_fee_per_leg(spot, leg_premium_usd)
# Deribit model: min(0.03% × spot, 12.5% × leg_premium_usd)
# Returns USD fee for ONE contract at the given premium.
# Scale by quantity yourself: fee_total = fee_per_leg * qty
```

At expiry there is no closing fee (`fee = 0.0`) — options settle automatically.

---

## 12. Indicators

Declare dependencies with `indicator_deps`, implement `set_indicators`.
The engine calls `build_indicators()` once before the replay, then calls
`strategy.set_indicators(ind)` on every strategy instance.

```python
from backtester.indicators import IndicatorDep

class MyStrategy:
    indicator_deps = [
        IndicatorDep(name="turbulence", symbol="BTCUSDT", interval="15m"),
        IndicatorDep(name="supertrend", symbol="BTCUSDT", interval="1h"),
    ]

    def set_indicators(self, ind):
        self._turbulence = ind.get("turbulence")  # DataFrame, hourly index, "composite" col
        self._supertrend = ind.get("supertrend")

    def on_market_state(self, state):
        # Read the pre-computed value for the current hour
        hour_ts = state.dt.replace(minute=0, second=0, microsecond=0)
        try:
            row = self._turbulence.loc[hour_ts]
            composite = float(row["composite"])
        except (KeyError, TypeError):
            composite = 0.0   # fail-open on missing data
```

**Rules:**
- Never fetch live data inside `on_market_state`. Indicators are pre-computed
  from the on-disk `indicators/data/` cache — do not add any network calls.
- Treat missing rows (KeyError) as fail-open (allow the action, don't crash).
- Treat NaN values as fail-open (weekend gaps, warmup period).
- `warmup_days` defaults to 30 in `IndicatorDep` — increase it if your indicator
  needs a longer rolling window.

**Registered indicators**: `"turbulence"` (composite score 0–100, hourly),
`"supertrend"` (trend direction, configurable interval). To add a new indicator
register a builder in `backtester/indicators.py`'s `_BUILDERS` dict.

---

## 13. PARAM_GRID conventions

```python
PARAM_GRID = {
    "dte":             [1, 2, 3],
    "delta":           [0.10, 0.15, 0.20],
    "stop_loss_pct":   [3.0, 4.0, 5.0],
    "take_profit_pct": [0.0],        # 0 = disabled
    "max_hold_hours":  [0],          # 0 = no time exit
    "skip_weekends":   [1],          # 0 or 1
}
```

**Rules:**
- `PARAM_GRID` is the wide, unbiased **discovery grid** — never narrow it after
  seeing results. That is overfitting. Experiment TOMLs in `backtester/experiments/`
  are where you record a "looks promising, let's study it" candidate.
- List every parameter in `PARAM_GRID` even if there is only one value. This makes
  the grid explicit and means `describe_params()` captures it in every result row.
- Use consistent naming: `stop_loss_pct` (fraction, e.g. 4.0 = 400%), 
  `take_profit_pct` (fraction, 0 = disabled), `max_hold_hours` (int, 0 = disabled),
  `skip_weekends` (int 0/1), `dte` (int), `delta` (float 0–1).
- Keep combos manageable for a discovery run (< ~500 combos is comfortable).
- Optional `PARAM_HELP = {"param_name": "short description", ...}` — shown in the
  Research UI New Run help column. Omit entirely or leave keys out; missing help
  displays as "—".

---

## 14. Registering the strategy

Add the import and registry entry in `backtester/run.py`:

```python
from backtester.strategies.my_strategy import MyStrategy

STRATEGIES = {
    ...
    "my_strategy": MyStrategy,
}
```

Only strategies that use the leg-aware API (`close_position` / `partial_close` /
`add_legs` + explicit open Trade) belong in this registry. Legacy strategies live in
`backtester/archive/strategies_to_be_fixed/` and must be migrated before
re-registering.

Currently registered: `short_str_turb_dyn`, `blueprint_howto`.

---

## 15. What NOT to use

### Legacy API — do not import these in new strategies

| Symbol | Why deprecated | Use instead |
|---|---|---|
| `close_trade` | Does not emit leg-aware fills; no open/close fill linkage | `close_position` |
| `close_short_strangle` | Same; also doesn't annotate legs | `close_position` with manual leg annotation |
| `check_take_profit_strangle` | Uses raw ask; no `price_mode` support | `profit_target_pct(pct, price_mode="executable")` |
| Opening without returning a Trade | Engine never sees open fills; balance column is wrong | Return `Trade(side="open")` from `_open_*` methods |

### Anti-patterns

- **Narrowing PARAM_GRID after seeing results** — this is look-ahead bias. Use an
  experiment TOML.
- **Live API calls inside `on_market_state`** — network calls inside the hot loop
  break reproducibility and ruin performance. Indicators must come from
  `set_indicators` or from the snapshot.
- **Using `option_selection.py` (root-level)** — that module targets live exchange
  objects. Use `bt_option_selection.py`.
- **Skipping `entry_price`, `entry_price_usd`, `fee_usd_open` on legs** — these
  fields are required by `close_position`'s leg-aware PnL path. Without them it
  falls back to a legacy formula that is less accurate for mixed-side positions.
- **Forgetting `pos_id`** — without `pos_id`, the engine cannot link open fills to
  close fills in `df_fills`.
- **Forgetting `skip_open_fill = True`** on close Trades — without this, the engine
  emits a duplicate set of open fills (once from the open Trade, once inferred
  from the close Trade).
- **Not guarding data gaps before non-expiry closes** — option quotes can be absent
  mid-day; always check for `None` before reading `.ask`.

---

## 16. Checklist

Before committing a new strategy, verify:

- [ ] Implements all five protocol methods (`configure`, `on_market_state`,
      `on_end`, `reset`, `describe_params`).
- [ ] `configure` resets ALL instance state (`_positions`, `_pos_counter`,
      `_last_trade_date`, etc.).
- [ ] `self._positions` is a `List[OpenPosition]` (not `_pos` or `_position`).
      Multi-slot strategies: expose `@property _positions` returning live slots.
- [ ] `reset` mirrors `configure` state resets.
- [ ] Every open returns a `Trade(side="open")` with `metadata["legs"]`.
- [ ] Legs carry `price_btc`, `entry_price`, `entry_price_usd`, `entry_spot`,
      `entry_bid`, `entry_ask`, `entry_mark`, `entry_iv`, `entry_delta`, `fee_usd_open`.
- [ ] `pos.metadata["pos_id"]` is set; `_pos_counter` resets in `configure`/`reset`.
- [ ] Before closing: each leg has `exit_price_btc`, `exit_price_usd`.
- [ ] Closes use `close_position` (not `close_trade` or `close_short_strangle`).
- [ ] Close Trades set `trade.metadata["skip_open_fill"] = True`.
- [ ] SL wired with `stop_loss_pct(pct, price_mode="mark")`.
- [ ] TP wired with `profit_target_pct(pct, price_mode="executable")` (when used).
- [ ] Data gap guard on non-expiry closes: check `state.get_option(...)` is not `None`.
- [ ] Uses `deribit_fee_per_leg` for fees; fee = 0 at expiry.
- [ ] Imports from `backtester.bt_option_selection`, not `option_selection`.
- [ ] Imports from `backtester.expiry_utils`, not local copies.
- [ ] `PARAM_GRID` is wide and unbiased; 0-disables optional parameters.
- [ ] `mark_iv` stored as-is from parquet (already %, e.g. 34.4 = 34.4%).
- [ ] Strategy is registered in `backtester/run.py`.
- [ ] `python -m pytest backtester/strategies/tests/ -v` passes.

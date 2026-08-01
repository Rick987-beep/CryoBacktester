# l_momentum — Strategy Code & Logic Explanation

## Overview

`l_momentum` buys a single Deribit BTC option (call or put) at each 4-hour UTC boundary
when BTC spot momentum on two timeframes is aligned in the same direction.  It is a
**directional long options** strategy — every trade pays limited premium upfront and
profits if BTC makes a sustained move in the expected direction.

---

## Entry Logic

### When entry is evaluated
- **Tick condition:** `state.dt.minute == 0` AND `state.dt.hour ∈ {0, 4, 8, 12, 16, 20}`
- One entry attempt per 4h window. If a position is opened, no second entry that window.
- Hard cap: no entry if `len(open_positions) >= max_concurrent` (default 8)

### Signal: multi-timeframe momentum
At the 4h boundary tick, two momentum values are looked up:

```
mom_4h = pct_change of the 4h BTCUSDT kline that just closed
mom_1h = pct_change of the 1h BTCUSDT kline that just closed
```

The "just closed" bar for a boundary at time T has Binance open_time = T − interval:
- At 16:00 UTC: the 4h bar with open_time 12:00 just closed → `series.loc[12:00]`
- At 16:00 UTC: the 1h bar with open_time 15:00 just closed → `series.loc[15:00]`

**Call signal:** `mom_4h >= +mom_4h_thr` AND `mom_1h >= +mom_1h_thr`  
**Put signal:**  `mom_4h <= −mom_4h_thr` AND `mom_1h <= −mom_1h_thr`  
**No trade:** mixed or weak signal (e.g. 4h up but 1h down)

### Option selection
1. Scan all available expiries; keep those with DTE = 4 or 5 (prefer DTE=5)
2. From the selected expiry's chain, take only calls or puts matching the signal direction
3. Use `select_by_delta(options, target=±0.35)` → picks the option closest to delta 0.35
4. Apply hard filters:
   - `|delta| ∈ [0.30, 0.40]` — reject if outside range
   - `(ask − bid) / mark × 100 ≤ spread_max_pct` (default 10%) — reject if spread too wide
5. Open the first qualifying option; stop searching once one is found.

### Entry fill
- **Fill price:** `ask_usd` (taker buy)
- **Fee:** `deribit_fee_per_leg(spot, mark_usd)` = `min(0.03% × spot, 12.5% × mark_usd)`

---

## Exit Logic

Exits are checked on **every 5-minute tick** for all open positions.

### Exit 1 — Take Profit
```
if current_ask_usd >= entry_ask_usd × tp_mult:
    exit reason = "take_profit"
```
- Triggered by the **ask** price (conservative — requires a firm bid to be available)
- Fill at **bid_usd**

### Exit 2 — Spot Stop (optional)
```
spot_chg_pct = (current_spot − entry_spot) / entry_spot × 100

For calls: if spot_chg_pct <= −spot_stop_pct → "spot_stop"
For puts:  if spot_chg_pct >= +spot_stop_pct → "spot_stop"
```
- Measures adverse BTC spot move from entry spot, not from option price
- Disabled when `spot_stop_pct = 0.0` (all three starred combos have it disabled)

### Exit 3 — Time Gate
```
held_hours = (current_time − entry_time).total_seconds() / 3600
if held_hours >= time_gate_h AND bid_usd < entry_ask_usd × time_gate_min_gain:
    exit reason = "time_gate"
```
- After `time_gate_h` hours (default 48), exit if the option has not reached 1.30× premium
- Rationale: 93% of eventual winners already show 1.2× by 36h (per research); a stale
  position that hasn't gained sufficiently is likely a loser.

### Exit fill
- **Fill price:** `bid_usd` (taker sell)
- **Fee:** `deribit_fee_per_leg(spot, mark_usd)`
- Fallback: if bid = 0, use mark_usd as exit price (defensive)

### End of data
- `on_end()` force-closes all open positions at the last available bid.

---

## PnL Calculation

```
pnl = exit_usd − entry_ask_usd − fee_open − fee_close
```

Both fees are computed on `mark_usd` (Deribit convention), not on fill price.

---

## Source Files

- Strategy: `backtester/strategies/l_momentum.py` (copy below and in `l_momentum.py`)
- Tests: `backtester/strategies/tests/test_l_momentum.py`
- Indicator builder: `backtester/indicators.py`, function `_build_spot_momentum` (lines ~325–328)

---

## PARAM_GRID

```python
PARAM_GRID = {
    "mom_4h_thr":    [1.0, 1.5, 2.0],      # 4h momentum threshold %
    "mom_1h_thr":    [0.3, 0.5, 1.0],      # 1h momentum threshold %
    "tp_mult":       [1.75, 2.0, 2.5],     # take-profit multiplier on entry ask
    "spot_stop_pct": [1.5, 2.0, 0.0],      # spot adverse % before stop (0=disabled)
    "time_gate_h":   [24, 36, 48],         # hours before time gate activates
}
```

Fixed parameters (not in grid):

| Param | Value | Rationale |
|-------|-------|-----------|
| `spread_max_pct` | 10.0 | Rejects illiquid options |
| `dte_range` | (4, 5) | Weekly Deribit expiries |
| `delta_range` | (0.30, 0.40) | Empirically validated in research |
| `time_gate_min_gain` | 1.30 | "1.2× by 36h" from research |
| `max_concurrent` | 8 | Portfolio cap |

---

## Strategy Source (copy as of 2026-05-16)

See `l_momentum.py` in this directory for the full source.

Key function signatures:

```python
class LMomentum:
    name = "l_momentum"
    indicator_deps = [
        IndicatorDep(name="spot_mom_4h", symbol="BTCUSDT", interval="4h", warmup_days=2),
        IndicatorDep(name="spot_mom_1h", symbol="BTCUSDT", interval="1h", warmup_days=2),
    ]

    def configure(self, params): ...        # called once per combo
    def set_indicators(self, ind): ...      # receives pre-computed Series
    def on_market_state(self, state): ...   # called every 5-min tick
    def on_end(self, state): ...            # called at end of replay
    def reset(self): ...                    # called between combos
    def _try_entry(self, state): ...        # entry logic
    def _check_exit(self, state, pos): ...  # exit condition check
    def _close(self, state, pos, reason): ... # exit fill + trade creation

def _lookup_mom(series, dt, interval_h): ...  # bar-timing helper, module-level
```

# TuDySho vs CryoTrader `short_str_turb_dyn`

**CryoTrader base file (build from this, not the CryoBacktester copy):**

```
CryoTrader/strategies/short_str_turb_dyn.py
```

**TuDySho reference implementation (backtester logic to port):**

```
handover/tudysho_cryotrader/reference/tudysho.py
```

---

## Reuse from CryoTrader `short_str_turb_dyn` (keep)

| Area | CryoTrader location | Notes |
|------|-------------------|-------|
| Turbulence gate | `_turbulence_ok()` | Live fetch + fail-open; includes T−1h bucket fix |
| Entry time window | `_turbulence_entry()` | Pattern: watch from entry hour until midnight |
| Leg resolution | `_legs_factory`, `strangle`, `resolve_legs` | Delta + DTE + `min_otm_pct` |
| Proximity stop | `_strike_proximity_stop()` | Final N hours, spot-only, strike ± buffer |
| Premium SL near expiry | `_combined_sl()` | Already **suppresses** mark SL inside proximity window |
| Execution | `strangle_turb_best_effort` profile, RFQ, limit phases | Keep live execution stack as-is |
| Trade callbacks | `_on_trade_opened`, `_on_trade_closed` | Extend metadata, not rewrite |
| Strategy factory | `short_str_turb_dyn()` → `StrategyConfig` | Fork into new module or extend factory |
| Config pattern | `PARAM_*` env vars from slot `.toml` | Map tudysho params to same pattern |
| Operational | `max_concurrent_trades=1` account-wide; per-slot daily entry cap | See `POSITION_RULES.md` |

---

## Change or add for TuDySho

### Entry time — **timezone**

| | CryoTrader `short_str_turb_dyn` | TuDySho |
|---|--------------------------------|---------|
| Param | `ENTRY_HOUR` — **UTC** hour | `entry_time` — **NYC wall-clock** `"HH:MM"` (DST-aware) |

Live sets use `"12:00"` NYC. Do **not** pass this as `ENTRY_HOUR=12` UTC. Port `to_nyc` / `to_utc` from backtester `market_hours.py` or equivalent in CryoTrader.

### Sizing — **largest code change**

| | CryoTrader | TuDySho |
|---|------------|---------|
| Target | `DYN_TARGET_PREMIUM` fixed USD | `nav_premium_pct` % of **`nav_usd`** |
| Cap | `MAX_QUANTITY` contracts | `max_qty_per_1btc_equity` × (`equity_usd` / spot) |
| Rounding | `floor` to 0.1 | `round` to 0.1 |
| Account | `account.equity` in notifications only | Needs **`nav_usd`** and **`equity_usd`** at sizing time |

Replace `_compute_quantity` / `_legs_factory` sizing block with tudysho `_compute_quantity(state, premium_usd_per_contract)` logic. Wire NAV from account (define whether NAV = equity + open uPnL).

### Stop-loss semantics — **align carefully**

CryoTrader `_combined_sl()`:

```python
sl_threshold = combined_premium * (1.0 + STOP_LOSS_PCT)   # STOP_LOSS_PCT=5.0 → 500% of premium
trigger when combined_mark >= sl_threshold
```

Backtester tudysho uses `stop_loss_pct(pct)` where `pct=6.0` means `(mark_cost - entry_premium) / entry_premium >= 6.0`.

Live param sets: Mon–Thu `stop_loss_pct=6.0`, Fri `4.0`. When porting, either:

1. Convert to CryoTrader's `(1 + pct)` threshold form (`STOP_LOSS_PCT = 6.0` → same numeric convention), or  
2. Keep backtester ratio form and adapt `_combined_sl()`.

Verify with one known trade from `backtests/run_295_friday_grid/trades_*.csv`.

### Premium SL timing

CryoTrader: premium SL off whenever inside `PROXIMITY_STOP_HOURS` window.

TuDySho: separate `premium_sl_except_final_hours` (live: 4h) — same effect when equal to `proximity_stop_hours`, but can diverge if params change. Prefer tudysho's explicit param for the new strategy.

### Weekday routing — **new**

CryoTrader: `WEEKEND_FILTER` + `weekday_filter(["mon"…"fri"])`.

TuDySho live: **two param sets** by weekday (`LIVE_PARAM_SCHEDULE.json`). Implement `resolve_params(weekday)` and snapshot params on trade open.

### Remove for live TuDySho sets

| CryoTrader feature | Live TuDySho |
|--------------------|--------------|
| `TAKE_PROFIT_PCT` | 0 — drop TP exit |
| `MAX_HOLD_HOURS` | 0 — drop max-hold exit |
| `DYN_TARGET_PREMIUM` / `MAX_QUANTITY` | Replaced by NAV sizing params |

### Optional (disabled in live sets)

- `equity_drawdown_stop` — not in CryoTrader base; add only if enabled later

---

## Param name mapping (slot `.toml` / `PARAM_*`)

| CryoTrader `short_str_turb_dyn` | TuDySho live |
|---------------------------------|--------------|
| `DTE` | `dte` |
| `DELTA` | `delta` |
| `ENTRY_HOUR` (UTC) | `entry_time` (NYC `"HH:MM"`) |
| `TURBULENCE_THRESHOLD` | `turbulence_threshold` |
| `STOP_LOSS_PCT` | `stop_loss_pct` |
| `MIN_OTM_PCT` | `min_otm_pct` |
| `PROXIMITY_STOP_HOURS` | `proximity_stop_hours` |
| `PROXIMITY_BUFFER_USD` | `proximity_buffer_usd` |
| `DYN_TARGET_PREMIUM` | → `nav_premium_pct` |
| `MAX_QUANTITY` | → `max_qty_per_1btc_equity` |
| `TAKE_PROFIT_PCT` | *(remove)* |
| `MAX_HOLD_HOURS` | *(remove)* |
| `WEEKEND_FILTER` | weekday schedule (see `LIVE_PARAM_SCHEDULE.json`) |
| — | `premium_sl_except_final_hours` |

---

## Suggested CryoTrader approach

1. **Copy** `CryoTrader/strategies/short_str_turb_dyn.py` → `strategies/tudysho.py` (or similar name).
2. **Keep:** turbulence gate, execution profiles, proximity stop, trade lifecycle hooks.
3. **Replace:** `_legs_factory` sizing, entry hour handling (NYC), exit list (no TP/max-hold), static `PARAM_*` block with weekday-aware param resolution.
4. **Register** new strategy in CryoTrader strategy registry; deploy via slot `.toml` with two param profiles or embedded schedule JSON.

Do **not** use `CryoBacktester/backtester/strategies/short_str_turb_dyn.py` — it diverges from live (NYC vs UTC entry, different SL factory, no execution layer).

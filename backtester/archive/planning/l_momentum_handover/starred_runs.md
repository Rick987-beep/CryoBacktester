# l_momentum — Starred Combos

Backtest: 2026-01-01 → 2026-05-12 | Account size: $10,000 | 243 combos tested  
Run bundle: `backtester/reports/l_momentum_20260516_100720.bundle`

---

## Combo A — Strictest filter

### Parameters
| Parameter | Value | Description |
|-----------|-------|-------------|
| `mom_4h_thr` | **1.5** | Min 4h spot % change to qualify |
| `mom_1h_thr` | **1.0** | Min 1h spot % change to qualify |
| `tp_mult` | **2.5** | Take profit at ask × 2.5× entry ask |
| `spot_stop_pct` | **0.0** | Spot stop **disabled** |
| `time_gate_h` | **48** | Exit if not +30% after 48h |
| *(fixed)* `spread_max_pct` | 10.0 | Max bid-ask spread as % of mark |
| *(fixed)* `dte_range` | (4, 5) | Prefer DTE=5, accept DTE=4 |
| *(fixed)* `delta_range` | (0.30, 0.40) | Call delta / abs(put delta) range |
| *(fixed)* `time_gate_min_gain` | 1.30 | Gain threshold for time gate |
| *(fixed)* `max_concurrent` | 8 | Max open positions |

### Performance metrics (full period)
| Metric | Value |
|--------|-------|
| Trades | 18 |
| Win rate | 50.0% |
| Total PnL | **+$7,031** |
| Sharpe | 1.791 |
| Max drawdown | 3.8% |
| Profit factor | 2.25 |
| R² (equity trend) | 0.834 |
| Omega ratio | 15.15 |
| Ulcer Index | 1.18 |
| Monthly consistency | 80% |

### Data files
- Trades: `combo_A_trades.csv`
- Daily equity: `combo_A_equity.csv`
- combo_idx in bundle: **215**

---

## Combo B — Relaxed 4h threshold

### Parameters
| Parameter | Value | Description |
|-----------|-------|-------------|
| `mom_4h_thr` | **1.0** | Min 4h spot % change to qualify |
| `mom_1h_thr` | **1.0** | Min 1h spot % change to qualify |
| `tp_mult` | **2.5** | Take profit at ask × 2.5× entry ask |
| `spot_stop_pct` | **0.0** | Spot stop **disabled** |
| `time_gate_h` | **48** | Exit if not +30% after 48h |
| *(fixed)* `spread_max_pct` | 10.0 | |
| *(fixed)* `dte_range` | (4, 5) | |
| *(fixed)* `delta_range` | (0.30, 0.40) | |
| *(fixed)* `time_gate_min_gain` | 1.30 | |
| *(fixed)* `max_concurrent` | 8 | |

### Performance metrics (full period)
| Metric | Value |
|--------|-------|
| Trades | 24 |
| Win rate | 45.8% |
| Total PnL | **+$7,436** |
| Sharpe | 1.798 |
| Max drawdown | 4.6% |
| Profit factor | 1.96 |
| R² (equity trend) | 0.866 |
| Omega ratio | 11.23 |
| Ulcer Index | 1.44 |
| Monthly consistency | 60% |

### Data files
- Trades: `combo_B_trades.csv`
- Daily equity: `combo_B_equity.csv`
- combo_idx in bundle: **188**

---

## Combo C — Best PnL / most trades

### Parameters
| Parameter | Value | Description |
|-----------|-------|-------------|
| `mom_4h_thr` | **1.5** | Min 4h spot % change to qualify |
| `mom_1h_thr` | **0.5** | Min 1h spot % change to qualify |
| `tp_mult` | **2.5** | Take profit at ask × 2.5× entry ask |
| `spot_stop_pct` | **0.0** | Spot stop **disabled** |
| `time_gate_h` | **48** | Exit if not +30% after 48h |
| *(fixed)* `spread_max_pct` | 10.0 | |
| *(fixed)* `dte_range` | (4, 5) | |
| *(fixed)* `delta_range` | (0.30, 0.40) | |
| *(fixed)* `time_gate_min_gain` | 1.30 | |
| *(fixed)* `max_concurrent` | 8 | |

### Performance metrics (full period)
| Metric | Value |
|--------|-------|
| Trades | 27 |
| Win rate | 51.9% |
| Total PnL | **+$8,828** |
| Sharpe | 1.822 |
| Max drawdown | 7.2% |
| Profit factor | 1.99 |
| R² (equity trend) | 0.836 |
| Omega ratio | 9.18 |
| Ulcer Index | 1.81 |
| Monthly consistency | 80% |

### Data files
- Trades: `combo_C_trades.csv`
- Daily equity: `combo_C_equity.csv`
- combo_idx in bundle: **134**

---

## Comparison

| Combo | mom_4h_thr | mom_1h_thr | Trades | Win% | Total PnL | Sharpe | Max DD% | PF | R² |
|-------|-----------|-----------|--------|------|-----------|--------|---------|-----|-----|
| A | 1.5 | 1.0 | 18 | 50.0 | +$7,031 | 1.791 | 3.8 | 2.25 | 0.834 |
| B | 1.0 | 1.0 | 24 | 45.8 | +$7,436 | 1.798 | 4.6 | 1.96 | 0.866 |
| C | 1.5 | 0.5 | 27 | 51.9 | +$8,828 | 1.822 | 7.2 | 1.99 | 0.836 |

**Key observations:**
- All three share `tp_mult=2.5` and disabled spot stop — the winning configuration ignores intra-trade spot noise
- Combo C has the most trades and best absolute PnL; Combo A has the lowest drawdown
- Sharpe is nearly identical across all three (~1.79–1.82) — suggesting the filter strictness matters less than the TP/gate settings
- `mom_1h_thr=1.0` (A+B) vs `0.5` (C): tighter 1h filter → fewer, cleaner entries; looser → more trades but comparable quality

---

## Notes — Recency gate (important)

The backtester's composite scoring vetoed **all 243 combos** (score = 0.0) because:
- Recency window = last 20% of 131 days ≈ last 26 days (≈ mid-April to May 12, 2026)
- All combo recent Sharpes were in range **−8.4 → 0.0**, well below the gate threshold of 0.3
- The strategy genuinely underperformed in that final window

This does not affect the full-period metrics above, but it is a signal worth investigating:
**What happened to BTC momentum in mid-April to mid-May 2026 that killed all entries?**
Possible causes: low volatility period, choppy 4h ranges, no sustained directional moves.

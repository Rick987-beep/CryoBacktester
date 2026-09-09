"""Run-777 discovery fixture: tudysho, 648 combos, 2026-05-01 → 2026-07-31.

Used by short-window parity and the full multicore acceptance run.
Do not treat this as a PARAM_GRID lock for other tudysho work.
"""
from __future__ import annotations

PARAM_GRID_777 = {
    "delta": [0.05, 0.075, 0.1],
    "dte": [1],
    "entry_time": ["12:00", "14:00"],
    "equity_drawdown_stop_pct": [0],
    "equity_sl_except_final_hours": [0],
    "equity_sl_only_final_hours": [0],
    "leg_min_price": [0],
    "leg_type": ["strangle"],
    "max_qty_per_1btc_equity": [6],
    "min_otm_pct": [2.2, 2.4, 2.6],
    "nav_premium_pct": [0.8],
    "premium_sl_except_final_hours": [8],
    "proximity_buffer_usd": [0, 500, 1000],
    "proximity_stop_hours": [8],
    "stop_loss_pct": [0.0, 3.0, 5.0],
    "trade_friday": [0],
    "trade_monday": [1],
    "trade_saturday": [0],
    "trade_sunday": [0],
    "trade_thursday": [1],
    "trade_tuesday": [1],
    "trade_wednesday": [1],
    "turbulence_threshold": [99, 70, 60, 50],
}

DATE_RANGE_777 = ("2026-05-01", "2026-07-31")
N_COMBOS_777 = 648
ACCOUNT_SIZE_777 = 100_000.0
STRATEGY_777 = "tudysho"

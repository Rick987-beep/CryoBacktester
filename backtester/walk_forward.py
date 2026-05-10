#!/usr/bin/env python3
"""
walk_forward.py — Walk-Forward Optimisation (WFO) for the backtester.

Process for each window:
    1. Slice the data into IS (in-sample) and OOS (out-of-sample) periods.
    2. Run the full PARAM_GRID on the IS period.
    3. Build a GridResult to rank combos by composite score.
    4. Take the IS-best combo → run it alone on the OOS period.
    5. Record IS and OOS stats.

Aggregate metrics:
    oos_win_rate    — fraction of windows where OOS PnL > 0
    oos_total_pnl   — sum of all OOS PnLs (the "deployed" equity)
    oos_equity      — stitched daily equity curve across all OOS periods

Usage:
    from backtester.walk_forward import run_walk_forward
    wfo = run_walk_forward(
        strategy_cls, options_path, spot_path,
        is_days=45, oos_days=15, step_days=15,
        account_size=10000,
    )
    # wfo.windows — list of WFOWindow
    # wfo.oos_win_rate, wfo.oos_total_pnl, wfo.oos_equity
"""
import statistics
import time as _time
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple, Type

from backtester.market_replay import MarketReplay
from backtester.engine import run_grid_full
from backtester.results import GridResult
from backtester.config import cfg as _cfg


# ── Data classes ─────────────────────────────────────────────────

@dataclass
class WFOWindow:
    """Stats for one IS + OOS window pair."""
    idx: int                    # 1-based window number
    is_start: str               # "YYYY-MM-DD"
    is_end: str
    oos_start: str
    oos_end: str
    best_params: dict           # IS winner's parameter dict
    is_pnl: float               # IS best-combo total PnL
    is_n_trades: int
    is_sharpe: float            # IS best-combo (daily-annualised)
    oos_pnl: float              # OOS PnL for the frozen IS-winner combo
    oos_n_trades: int
    oos_sharpe: float           # OOS (daily-annualised)
    oos_win: bool               # oos_pnl > 0


@dataclass
class WFOResult:
    """Aggregated walk-forward validation results."""
    windows: List[WFOWindow]
    is_days: int
    oos_days: int
    step_days: int
    oos_win_rate: float         # fraction of windows where OOS PnL > 0
    oos_total_pnl: float        # sum of per-window OOS PnLs
    oos_avg_sharpe: float       # mean OOS Sharpe across windows
    # Stitched OOS daily equity: list of (date_str, day_pnl, cum_pnl, hi, lo, close)
    oos_equity: List[Tuple]     # 6-tuple per day; capital = account_size start


# ── Internal helpers ─────────────────────────────────────────────

def _build_windows(start_date, end_date, is_days, oos_days, step_days):
    # type: (date, date, int, int, int) -> List[Tuple[date, date, date, date]]
    """Generate (is_start, is_end, oos_start, oos_end) date tuples.

    Windows shift by step_days.  A window is only emitted when the full OOS
    period fits within end_date.
    """
    windows = []
    is_start = start_date
    while True:
        is_end = is_start + timedelta(days=is_days - 1)
        oos_start = is_end + timedelta(days=1)
        oos_end = oos_start + timedelta(days=oos_days - 1)
        if oos_end > end_date:
            break
        windows.append((is_start, is_end, oos_start, oos_end))
        is_start += timedelta(days=step_days)
    return windows


def _simple_sharpe(daily_pnl, periods=365):
    # type: (List[float], int) -> float
    """Annualised Sharpe from a list of daily PnL values (zero risk-free rate)."""
    n = len(daily_pnl)
    if n < 2:
        return 0.0
    mean = sum(daily_pnl) / n
    std = statistics.stdev(daily_pnl)
    return (mean / std * periods ** 0.5) if std > 0.0 else 0.0


def _daily_pnl_from_df(df, date_from, date_to):
    # type: (Any, str, str) -> List[float]
    """Build a full daily PnL list (zero-filled gaps) from a trade log DataFrame."""
    if df is None or df.empty:
        return []
    date_pnl = df.groupby("entry_date")["pnl"].sum().to_dict()
    from datetime import datetime
    first = datetime.strptime(date_from, "%Y-%m-%d").date()
    last = datetime.strptime(date_to, "%Y-%m-%d").date()
    result = []
    d = first
    while d <= last:
        result.append(date_pnl.get(d.strftime("%Y-%m-%d"), 0.0))
        d += timedelta(days=1)
    return result


def _oos_equity_rows(df, date_from, date_to, capital_start):
    # type: (Any, str, str, float) -> List[Tuple]
    """Build equity curve rows (6-tuple) for one OOS window.

    Rows: (date_str, day_pnl, cum_pnl, high_nav, low_nav, close_nav)
    capital_start is the NAV level at the start of this window.
    """
    daily_pnl = _daily_pnl_from_df(df, date_from, date_to)
    from datetime import datetime
    first = datetime.strptime(date_from, "%Y-%m-%d").date()
    rows = []
    cum = 0.0
    for i, pnl in enumerate(daily_pnl):
        cum += pnl
        d_str = (first + timedelta(days=i)).strftime("%Y-%m-%d")
        nav = capital_start + cum
        rows.append((d_str, pnl, cum, nav, nav, nav))
    return rows


# ── Public API ───────────────────────────────────────────────────

def run_walk_forward(
    strategy_cls,           # type: Type[Any]
    options_path,           # type: str
    spot_path,              # type: str
    is_days=45,             # type: int
    oos_days=15,            # type: int
    step_days=15,           # type: int
    account_size=10000.0,   # type: float
):
    # type: (...) -> WFOResult
    """Run walk-forward validation.

    For each rolling window:
      - IS period: run full PARAM_GRID → pick best combo by composite score
      - OOS period: run that single combo frozen → record OOS PnL/Sharpe

    Args:
        strategy_cls:  Strategy class with PARAM_GRID and DATE_RANGE attributes.
        options_path:  Path to options parquet (file or directory).
        spot_path:     Path to spot parquet (file or directory).
        is_days:       In-sample window length in calendar days.
        oos_days:      Out-of-sample window length in calendar days.
        step_days:     Window shift per step.
        account_size:  Virtual account size (for equity curve baseline).

    Returns:
        WFOResult with per-window stats and aggregate metrics.
    """
    # Determine full date range
    date_range_filter = getattr(strategy_cls, "DATE_RANGE", (None, None))
    if date_range_filter[0] and date_range_filter[1]:
        from datetime import datetime
        full_start = datetime.strptime(date_range_filter[0], "%Y-%m-%d").date()
        full_end = datetime.strptime(date_range_filter[1], "%Y-%m-%d").date()
    else:
        # Peek at data to find range
        _probe = MarketReplay(options_path, spot_path)
        t_start, t_end = _probe.time_range
        full_start = t_start.date()
        full_end = t_end.date()

    windows_dates = _build_windows(full_start, full_end, is_days, oos_days, step_days)
    if not windows_dates:
        raise ValueError(
            f"No WFO windows fit in {full_start} – {full_end} "
            f"with IS={is_days}d / OOS={oos_days}d / step={step_days}d. "
            f"Need at least {is_days + oos_days} days of data."
        )

    print(f"\n  Walk-Forward Validation: {len(windows_dates)} windows "
          f"(IS={is_days}d / OOS={oos_days}d / step={step_days}d)")

    wfo_windows = []
    oos_equity_all = []     # stitched: nav continues from last window
    nav_cursor = account_size

    for win_idx, (is_start, is_end, oos_start, oos_end) in enumerate(windows_dates, 1):
        is_start_s = is_start.strftime("%Y-%m-%d")
        is_end_s = is_end.strftime("%Y-%m-%d")
        oos_start_s = oos_start.strftime("%Y-%m-%d")
        oos_end_s = oos_end.strftime("%Y-%m-%d")

        print(f"  Window {win_idx}/{len(windows_dates)}: "
              f"IS [{is_start_s} – {is_end_s}]  OOS [{oos_start_s} – {oos_end_s}]")

        # ── IS run ────────────────────────────────────────────────
        t0 = _time.time()
        is_replay = MarketReplay(options_path, spot_path,
                                 start=is_start_s, end=is_end_s)
        df_is, keys_is, nav_daily_is, final_nav_is = run_grid_full(
            strategy_cls, strategy_cls.PARAM_GRID, is_replay
        )
        is_result = GridResult(
            df_is, keys_is, nav_daily_is, final_nav_is,
            param_grid=strategy_cls.PARAM_GRID,
            account_size=account_size,
            date_range=(is_start_s, is_end_s),
        )
        print(f"    IS:  {len(keys_is)} combos in {_time.time()-t0:.1f}s  "
              f"best={is_result.best_stats['total_pnl']:+,.0f}  "
              f"Sharpe={is_result.best_stats['sharpe']:.2f}"
              if is_result.best_stats else f"    IS: no results")

        if not is_result.ranked or is_result.best_key is None:
            print(f"    IS produced no ranked combos — skipping window {win_idx}")
            continue

        best_key = is_result.ranked[0][0]
        best_params = dict(best_key)
        is_pnl = float(is_result.best_stats["total_pnl"])
        is_n = int(is_result.best_stats["n"])
        is_sharpe = float(is_result.best_stats["sharpe"])

        # ── OOS run — single frozen combo ─────────────────────────
        t1 = _time.time()
        oos_param_grid = {k: [v] for k, v in best_params.items()}
        oos_replay = MarketReplay(options_path, spot_path,
                                  start=oos_start_s, end=oos_end_s)
        df_oos, keys_oos, _, _ = run_grid_full(
            strategy_cls, oos_param_grid, oos_replay
        )

        oos_pnl = float(df_oos["pnl"].sum()) if not df_oos.empty else 0.0
        oos_n = int(len(df_oos)) if not df_oos.empty else 0
        oos_daily = _daily_pnl_from_df(df_oos, oos_start_s, oos_end_s)
        oos_sharpe = _simple_sharpe(oos_daily)
        oos_win = oos_pnl > 0

        print(f"    OOS: PnL={oos_pnl:+,.0f}  trades={oos_n}  "
              f"Sharpe={oos_sharpe:.2f}  {'✓ win' if oos_win else '✗ loss'}  "
              f"({_time.time()-t1:.1f}s)")

        # Build per-window OOS equity rows (absolute NAV continuing from cursor)
        oos_rows = _oos_equity_rows(df_oos, oos_start_s, oos_end_s, nav_cursor)
        oos_equity_all.extend(oos_rows)
        nav_cursor += oos_pnl

        # Apply a label separator between windows in the stitched curve
        # (dates stay sequential so the chart renders cleanly without gaps)

        wfo_windows.append(WFOWindow(
            idx=win_idx,
            is_start=is_start_s,
            is_end=is_end_s,
            oos_start=oos_start_s,
            oos_end=oos_end_s,
            best_params=best_params,
            is_pnl=is_pnl,
            is_n_trades=is_n,
            is_sharpe=is_sharpe,
            oos_pnl=oos_pnl,
            oos_n_trades=oos_n,
            oos_sharpe=oos_sharpe,
            oos_win=oos_win,
        ))

    # ── Aggregates ────────────────────────────────────────────────
    n_win = len(wfo_windows)
    n_oos_win = sum(1 for w in wfo_windows if w.oos_win)
    oos_win_rate = n_oos_win / n_win if n_win else 0.0
    oos_total_pnl = sum(w.oos_pnl for w in wfo_windows)
    sharpes = [w.oos_sharpe for w in wfo_windows if w.oos_n_trades > 0]
    oos_avg_sharpe = sum(sharpes) / len(sharpes) if sharpes else 0.0

    print(f"\n  WFO summary: {n_oos_win}/{n_win} windows profitable  "
          f"OOS PnL={oos_total_pnl:+,.0f}  avg Sharpe={oos_avg_sharpe:.2f}")

    return WFOResult(
        windows=wfo_windows,
        is_days=is_days,
        oos_days=oos_days,
        step_days=step_days,
        oos_win_rate=oos_win_rate,
        oos_total_pnl=oos_total_pnl,
        oos_avg_sharpe=oos_avg_sharpe,
        oos_equity=oos_equity_all,
    )

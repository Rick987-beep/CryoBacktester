"""Combined equity curve with scaled intraday drawdown from per-slot nav_daily."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

HANDOVER = Path(__file__).resolve().parents[2] / "handover/tudysho_cryotrader/backtests"

SLOT_NAV_FILES = {
    "A": HANDOVER / "nav_daily_slot_a_mon_thu.csv",
    "B": HANDOVER / "nav_daily_slot_b_mon_early.csv",
    "C": HANDOVER / "nav_daily_slot_c_fri_sat.csv",
}


def _date_range(date_from: str, date_to: str) -> List[str]:
    first = datetime.strptime(date_from, "%Y-%m-%d").date()
    last = datetime.strptime(date_to, "%Y-%m-%d").date()
    dates: List[str] = []
    d = first
    while d <= last:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates


def _combined_close_equity(
    trades: pd.DataFrame,
    capital: float,
    dates: List[str],
) -> Dict[str, float]:
    """Realized close equity; PnL booked on exit date."""
    t = trades.copy()
    if "exit_time" not in t.columns:
        raise ValueError("trades need exit_time")
    if not pd.api.types.is_datetime64_any_dtype(t["exit_time"]):
        t["exit_time"] = pd.to_datetime(t["exit_time"], utc=True)
    exit_pnl = t.groupby(t["exit_time"].dt.strftime("%Y-%m-%d"))["pnl"].sum().to_dict()

    equity: Dict[str, float] = {}
    eq = capital
    for ds in dates:
        eq += float(exit_pnl.get(ds, 0.0))
        equity[ds] = eq
    return equity


def _live_trades(trades: pd.DataFrame) -> pd.DataFrame:
    """Exclude simulation-tail end_of_data closes from position timing."""
    if "exit_reason" not in trades.columns:
        return trades
    return trades[~trades["exit_reason"].isin(["end_of_data"])]


def _active_slots_on_day(
    trades: pd.DataFrame,
    ds: str,
) -> List[str]:
    """Return slot labels with an open position at any point on calendar day ds (UTC)."""
    day_start = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)
    active: List[str] = []
    live = _live_trades(trades)
    for slot in ("A", "B", "C"):
        sub = live[live["slot"] == slot]
        for _, tr in sub.iterrows():
            entry = tr["entry_time"]
            exit_t = tr["exit_time"]
            if entry.tzinfo is None:
                entry = entry.replace(tzinfo=timezone.utc)
            if exit_t.tzinfo is None:
                exit_t = exit_t.replace(tzinfo=timezone.utc)
            if entry < day_end and exit_t > day_start:
                active.append(slot)
                break
    return active


def _slots_open_at_eod(trades: pd.DataFrame, ds: str) -> List[str]:
    """Return slots still open at end of calendar day ds (UTC)."""
    day_end = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    open_slots: List[str] = []
    live = _live_trades(trades)
    for slot in ("A", "B", "C"):
        sub = live[live["slot"] == slot]
        for _, tr in sub.iterrows():
            entry = tr["entry_time"]
            exit_t = tr["exit_time"]
            if entry.tzinfo is None:
                entry = entry.replace(tzinfo=timezone.utc)
            if exit_t.tzinfo is None:
                exit_t = exit_t.replace(tzinfo=timezone.utc)
            if entry < day_end and exit_t > day_end:
                open_slots.append(slot)
                break
    return open_slots


def _scale_nav(capital: float, nav: float, size_scale: float) -> float:
    """Linear size scaling from common starting capital."""
    return capital + size_scale * (nav - capital)


def _eod_mtm_close(
    eq_realized: float,
    capital: float,
    eod_slots: List[str],
    navs: Dict[str, pd.DataFrame],
    ds: str,
    size_scale: float = 1.0,
) -> float:
    """Scale active slot's EOD mark-to-market onto combined realized equity."""
    if not eod_slots:
        return eq_realized
    if len(eod_slots) > 1:
        raise ValueError(f"multiple EOD positions on {ds}: {eod_slots}")

    slot = eod_slots[0]
    if ds not in navs[slot].index:
        return eq_realized

    row = navs[slot].loc[ds]
    slot_realized = capital + float(row.get("realized_close", row["nav_close"] - capital))
    slot_mtm = float(row["nav_close"])
    open_pnl = slot_mtm - slot_realized
    return eq_realized + open_pnl * size_scale


def compute_combined_intraday_equity(
    trades: pd.DataFrame,
    capital: float = 100_000.0,
    date_from: str = "2025-06-27",
    date_to: str = "2026-06-27",
    size_scale: float = 1.0,
) -> Tuple[pd.DataFrame, dict]:
    """Merge slot nav_daily into one combined account curve.

    Intraday band (nav_high / nav_low):
      scale = combined_realized_close / slot_nav_close
      est_high = combined_realized + (nav_high - nav_close) * scale
      est_low  = combined_realized - (nav_close - nav_low) * scale

    When multiple slots touch the same calendar day (Mon B then A), take the
    widest intraday band. Positions do not overlap in time.

    EOD nav_close (MTM):
      combined_realized + scaled (slot_nav_close - slot_realized) for the slot
      open at end of day. Used for Sharpe / Sortino daily returns.
    """
    dates = _date_range(date_from, date_to)
    close_eq = _combined_close_equity(trades, capital, dates)

    navs: Dict[str, pd.DataFrame] = {}
    for slot, path in SLOT_NAV_FILES.items():
        nav = pd.read_csv(path)
        nav["date"] = nav["date"].astype(str)
        navs[slot] = nav.set_index("date")

    rows = []
    peak_high = capital
    max_dd_pct = 0.0
    max_dd_date = dates[0]
    isolated_max_dd = {
        slot: _isolated_max_dd(nav, capital, size_scale)
        for slot, nav in navs.items()
    }

    for ds in dates:
        eq_realized = close_eq[ds]
        est_high = eq_realized
        est_low = eq_realized
        active = _active_slots_on_day(trades, ds)

        for slot in active:
            if ds not in navs[slot].index:
                continue
            row = navs[slot].loc[ds]
            slot_close = float(row["nav_close"])
            if slot_close <= 0:
                continue
            up = (float(row["nav_high"]) - slot_close) * size_scale
            down = (slot_close - float(row["nav_low"])) * size_scale
            est_high = max(est_high, eq_realized + up)
            est_low = min(est_low, eq_realized - down)

        eod_slots = _slots_open_at_eod(trades, ds)
        eq_mtm = _eod_mtm_close(
            eq_realized, capital, eod_slots, navs, ds, size_scale=size_scale,
        )

        peak_high = max(peak_high, est_high)
        dd_pct = (peak_high - est_low) / peak_high * 100 if peak_high > 0 else 0.0
        if dd_pct > max_dd_pct:
            max_dd_pct = dd_pct
            max_dd_date = ds

        nav_open = rows[-1]["nav_close"] if rows else capital
        ret_pct = 100.0 * (eq_mtm / capital - 1.0)
        dd_from_peak = -100.0 * (peak_high - est_low) / peak_high if peak_high > 0 else 0.0
        rows.append({
            "date": ds,
            "nav_open": nav_open,
            "nav_high": est_high,
            "nav_low": est_low,
            "nav_close": eq_mtm,
            "nav_close_realized": eq_realized,
            "return_pct": round(ret_pct, 4),
            "drawdown_pct": round(dd_from_peak, 4),
            "active_slots": ",".join(active) if active else "",
            "eod_slot": eod_slots[0] if eod_slots else "",
        })

    daily = pd.DataFrame(rows)
    meta = {
        "max_dd_pct_intraday_scaled": round(max_dd_pct, 2),
        "max_dd_date": max_dd_date,
        "isolated_slot_max_dd_pct": {
            k: round(v, 2) for k, v in isolated_max_dd.items()
        },
        "worst_isolated_slot_max_dd_pct": round(max(isolated_max_dd.values()), 2),
    }
    return daily, meta


def nav_daily_for_equity_metrics(daily: pd.DataFrame, capital: float) -> pd.DataFrame:
    """nav_daily frame for backtester.core.results.equity_metrics."""
    out = daily[["date", "nav_close", "nav_high", "nav_low"]].copy()
    out["realized_close"] = daily["nav_close_realized"] - capital
    return out


def trade_level_metrics(trades: pd.DataFrame) -> dict:
    """Profit factor and consecutive win/loss streaks from round-trip PnL."""
    ordered = trades.sort_values("exit_time")
    pnl = ordered["pnl"]
    gross_win = float(pnl[pnl > 0].sum())
    gross_loss = abs(float(pnl[pnl < 0].sum()))
    profit_factor = (gross_win / gross_loss) if gross_loss > 0 else 99.9

    max_cw = max_cl = cw = cl = 0
    for p in pnl:
        if p > 0:
            cw += 1
            cl = 0
        elif p < 0:
            cl += 1
            cw = 0
        max_cw = max(max_cw, cw)
        max_cl = max(max_cl, cl)

    return {
        "profit_factor": profit_factor,
        "consec_wins": max_cw,
        "consec_losses": max_cl,
    }


def _isolated_max_dd(
    nav: pd.DataFrame,
    capital: float,
    size_scale: float = 1.0,
) -> float:
    peak = 0.0
    max_dd = 0.0
    for _, row in nav.iterrows():
        high = _scale_nav(capital, float(row["nav_high"]), size_scale)
        low = _scale_nav(capital, float(row["nav_low"]), size_scale)
        peak = max(peak, high)
        if peak > 0:
            max_dd = max(max_dd, (peak - low) / peak * 100)
    return max_dd

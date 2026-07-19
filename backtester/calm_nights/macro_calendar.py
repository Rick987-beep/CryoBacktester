"""Load synced macro calendar and expose tier-N entry-day flags."""

from __future__ import annotations

import logging
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from backtester.core.config import cfg

logger = logging.getLogger(__name__)


def _default_calendar_dir() -> Path:
    return Path(cfg.data.macro_calendar_dir)


@lru_cache(maxsize=1)
def load_events(calendar_dir: str | None = None) -> pd.DataFrame:
    """Load merged us_scheduled parquets from macro_store sync."""
    root = Path(calendar_dir) if calendar_dir else _default_calendar_dir()
    if not root.exists():
        logger.warning("macro calendar missing at %s — tier skips disabled", root)
        return pd.DataFrame(columns=["event_date", "tier", "event_type", "title"])

    frames: list[pd.DataFrame] = []
    for path in sorted(root.rglob("*.parquet")):
        try:
            frames.append(pd.read_parquet(path))
        except Exception as exc:
            logger.warning("failed reading macro parquet %s: %s", path, exc)

    if not frames:
        logger.warning("no macro parquet files under %s", root)
        return pd.DataFrame(columns=["event_date", "tier", "event_type", "title"])

    df = pd.concat(frames, ignore_index=True)
    if "event_date" not in df.columns:
        return pd.DataFrame(columns=["event_date", "tier", "event_type", "title"])
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date.astype(str)
    return df


def events_on_date(d: date, calendar_dir: str | None = None) -> pd.DataFrame:
    cal = load_events(calendar_dir)
    if cal.empty:
        return cal
    key = d.isoformat()
    return cal[cal["event_date"] == key].copy()


def tier_entry_day(d: date, max_tier: int = 1, calendar_dir: str | None = None) -> bool:
    """True when any scheduled event on *d* has tier <= max_tier."""
    if max_tier <= 0:
        return False
    sub = events_on_date(d, calendar_dir)
    if sub.empty:
        return False
    tiers = pd.to_numeric(sub["tier"], errors="coerce")
    return bool((tiers <= max_tier).any())


def tier_entry_day_series(
    entry_dates: list[date],
    max_tier: int = 1,
    calendar_dir: str | None = None,
) -> pd.Series:
    """Boolean tier flag per entry date string."""
    cal = load_events(calendar_dir)
    if cal.empty or max_tier <= 0:
        return pd.Series(False, index=[d.isoformat() for d in entry_dates])

    flagged: set[str] = set()
    tiers = pd.to_numeric(cal["tier"], errors="coerce")
    mask = tiers <= max_tier
    for ed in cal.loc[mask, "event_date"].astype(str):
        flagged.add(ed)

    return pd.Series(
        [d.isoformat() in flagged for d in entry_dates],
        index=[d.isoformat() for d in entry_dates],
        name="tier_entry_day",
    )


def events_metadata(d: date, calendar_dir: str | None = None) -> list[dict[str, Any]]:
    sub = events_on_date(d, calendar_dir)
    if sub.empty:
        return []
    cols = ["event_type", "tier", "title"]
    present = [c for c in cols if c in sub.columns]
    return sub[present].to_dict(orient="records")

"""Load synced macro calendar and expose tier-N entry-day flags.

Cadysho / calm_nights use parquet ``tier`` values from CryoQuant.
TuDySho Starnberg remaps by ``event_type`` via ``STARNBERG_EVENT_TIERS``
(local table — ignores parquet ``tier``).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from backtester.core.config import cfg

logger = logging.getLogger(__name__)

_NYC = ZoneInfo("America/New_York")

# Local Starnberg remapping by event_type (parquet tier column ignored).
STARNBERG_EVENT_TIERS: dict[str, int] = {
    "fomc": 1,
    "cpi": 1,
    "nfp": 2,
    "ppi": 2,
    "earnings": 2,
    "jolts": 3,
    "eci": 3,
    "import_export_prices": 3,
}


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


def starnberg_local_tier(event_type: str) -> int | None:
    """Return Starnberg remapped tier, or None if event_type is ignored."""
    return STARNBERG_EVENT_TIERS.get(str(event_type))


def _event_available_utc(row: pd.Series, event_date: date) -> datetime | None:
    """Prefer available_utc; fall back to event_time_et on event_date (NYC)."""
    raw = row.get("available_utc")
    if raw is not None and not (isinstance(raw, float) and pd.isna(raw)):
        try:
            ts = pd.Timestamp(raw)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            return ts.to_pydatetime()
        except (TypeError, ValueError, OverflowError):
            pass

    et = row.get("event_time_et")
    if et is None or (isinstance(et, float) and pd.isna(et)):
        return None
    try:
        parts = str(et).strip().split(":")
        hour, minute = int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
        local = datetime(
            event_date.year, event_date.month, event_date.day,
            hour, minute, tzinfo=_NYC,
        )
        return local.astimezone(timezone.utc)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None


def starnberg_qualifying_events(
    d: date,
    max_tier: int = 3,
    calendar_dir: str | None = None,
) -> pd.DataFrame:
    """Events on NYC date *d* with local remapped tier <= max_tier."""
    if max_tier <= 0:
        return pd.DataFrame()
    sub = events_on_date(d, calendar_dir)
    if sub.empty:
        return sub

    rows: list[dict[str, Any]] = []
    for _, row in sub.iterrows():
        local_tier = starnberg_local_tier(row.get("event_type", ""))
        if local_tier is None or local_tier > max_tier:
            continue
        available = _event_available_utc(row, d)
        if available is None:
            continue
        rows.append({
            "event_type": row.get("event_type"),
            "title": row.get("title"),
            "tier": local_tier,
            "available_utc": available,
            "event_date": d.isoformat(),
        })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def starnberg_latest_release_utc(
    d: date,
    max_tier: int = 3,
    calendar_dir: str | None = None,
) -> datetime | None:
    """Max available_utc among Starnberg-qualifying events on *d*."""
    sub = starnberg_qualifying_events(d, max_tier=max_tier, calendar_dir=calendar_dir)
    if sub.empty:
        return None
    return max(sub["available_utc"].tolist())


def starnberg_events_metadata(
    d: date,
    max_tier: int = 3,
    calendar_dir: str | None = None,
) -> list[dict[str, Any]]:
    """Metadata list with *local* remapped tiers for trade logs."""
    sub = starnberg_qualifying_events(d, max_tier=max_tier, calendar_dir=calendar_dir)
    if sub.empty:
        return []
    out: list[dict[str, Any]] = []
    for _, row in sub.iterrows():
        avail = row["available_utc"]
        out.append({
            "event_type": row["event_type"],
            "tier": int(row["tier"]),
            "title": row.get("title"),
            "available_utc": avail.isoformat() if hasattr(avail, "isoformat") else str(avail),
        })
    return out


def starnberg_macro_blocks_entry(
    nyc_date: date,
    base_entry_utc: datetime,
    delay_hours: float,
    max_tier: int = 3,
    calendar_dir: str | None = None,
) -> tuple[bool, datetime | None, list[dict[str, Any]]]:
    """Delay-or-skip gate for TuDySho Starnberg.

    Returns ``(skip_day, cleared_after_utc, events_metadata)``.

    ``cleared_after = latest_release_utc + delay_hours``.
    ``skip_day`` is True when ``cleared_after > base_entry_utc`` (no late entries).
    When there are no qualifying events, returns ``(False, None, [])``.
    """
    events = starnberg_events_metadata(
        nyc_date, max_tier=max_tier, calendar_dir=calendar_dir,
    )
    latest = starnberg_latest_release_utc(
        nyc_date, max_tier=max_tier, calendar_dir=calendar_dir,
    )
    if latest is None:
        return False, None, events

    cleared_after = latest + timedelta(hours=float(delay_hours))
    if base_entry_utc.tzinfo is None:
        base = base_entry_utc.replace(tzinfo=timezone.utc)
    else:
        base = base_entry_utc
    if cleared_after.tzinfo is None:
        cleared_after = cleared_after.replace(tzinfo=timezone.utc)

    skip_day = cleared_after > base
    return skip_day, cleared_after, events

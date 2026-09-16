"""FOMC policy-statement indicator (rate decision / hold), not minutes.

Each scheduled FOMC meeting publishes a funds-rate decision — cut, hike, or
hold — at the Fed embargo time: **14:00:00 America/New_York** on the last
meeting day (``For release at 2:00 p.m. ET``). That is the timestamp this
indicator emits, to the minute. It is the official digital-publication
embargo, not a measured Bloomberg/Dow-Jones first-print tick. Backtest
snapshots are 5 minutes, so sub-minute wire lag is not observable anyway.

What is in
    Regularly scheduled FOMC meetings whose result is a **potential rate
    change** (the policy statement). Holds are included: the announcement
    is still a rate decision.

What is out
    FOMC minutes (~3 weeks later, also 14:00 ET), framework / notation votes,
    and the local ``us_scheduled`` parquet ``event_type=fomc`` rows (those
    mis-title minutes as “FOMC rate decision” and currently stop 2026-07-08).

How a strategy uses it
    The indicator does **not** block. It is a table of ``announced_at``
    timestamps. Call ``fomc_entry_gate`` each tick:

    * ``mode="off"`` — never block.
    * ``mode="skip"`` — block the whole NYC date of a meeting.
    * ``mode="delay"`` — block from the start of that NYC date until
      ``announced_at + delay_minutes`` (30 / 60 / 120), then allow late
      entry. Before the print the day is blocked; after the delay it is not.

    ``delay_minutes=0`` with ``mode="delay"`` allows from the embargo minute
    (same clock as a 14:00 NYC entry).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

UTC = timezone.utc
_NYC = ZoneInfo("America/New_York")

TIME_SOURCE = "fed_embargo_14:00_et"
ANNOUNCE_HOUR_ET = 14
ANNOUNCE_MINUTE_ET = 0

# (announce_date, meeting_label, action, delta_bp|None, target_low, target_high)
# action: hold | cut | hike | pending
# Source: federalreserve.gov FOMC calendars + statements. pending = statement
# not yet published (or future scheduled meeting).
_MEETINGS: Tuple[Tuple[str, str, str, Optional[int], float, float], ...] = (
    ("2025-01-29", "28–29 Jan 2025", "hold", 0, 4.25, 4.50),
    ("2025-03-19", "18–19 Mar 2025", "hold", 0, 4.25, 4.50),
    ("2025-05-07", "6–7 May 2025", "hold", 0, 4.25, 4.50),
    ("2025-06-18", "17–18 Jun 2025", "hold", 0, 4.25, 4.50),
    ("2025-07-30", "29–30 Jul 2025", "hold", 0, 4.25, 4.50),
    ("2025-09-17", "16–17 Sep 2025", "cut", -25, 4.00, 4.25),
    ("2025-10-29", "28–29 Oct 2025", "cut", -25, 3.75, 4.00),
    ("2025-12-10", "9–10 Dec 2025", "cut", -25, 3.50, 3.75),
    ("2026-01-28", "27–28 Jan 2026", "hold", 0, 3.50, 3.75),
    ("2026-03-18", "17–18 Mar 2026", "hold", 0, 3.50, 3.75),
    ("2026-04-29", "28–29 Apr 2026", "hold", 0, 3.50, 3.75),
    ("2026-06-17", "16–17 Jun 2026", "hold", 0, 3.50, 3.75),
    ("2026-07-29", "28–29 Jul 2026", "hold", 0, 3.50, 3.75),
    ("2026-09-16", "15–16 Sep 2026", "pending", None, 3.50, 3.75),
    ("2026-10-28", "27–28 Oct 2026", "pending", None, 3.50, 3.75),
    ("2026-12-09", "8–9 Dec 2026", "pending", None, 3.50, 3.75),
)

# Dates the parquet (and earlier canvas) labelled fomc but which are not
# policy-statement meetings. Kept for tests / audits — not emitted.
KNOWN_NON_DECISION_DATES: Tuple[str, ...] = (
    "2025-01-08",  # minutes of Dec 2024
    "2025-02-19",
    "2025-04-09",
    "2025-05-28",
    "2025-07-09",
    "2025-08-20",
    "2025-08-22",  # framework notation vote
    "2025-10-08",
    "2025-11-19",
    "2025-12-30",
    "2026-02-18",
    "2026-04-08",
    "2026-05-20",
    "2026-07-08",
    "2026-08-19",
)


def announced_at_utc(d: date) -> datetime:
    """Fed embargo instant: 14:00:00 ET on *d*, as timezone-aware UTC."""
    local = datetime(
        d.year, d.month, d.day,
        ANNOUNCE_HOUR_ET, ANNOUNCE_MINUTE_ET, 0,
        tzinfo=_NYC,
    )
    return local.astimezone(UTC)


def _as_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


@dataclass(frozen=True)
class FomcEvent:
    """One FOMC policy-statement print."""

    announced_at: datetime
    nyc_date: date
    meeting: str
    action: str
    delta_bp: Optional[int]
    target_low: float
    target_high: float
    announced_et: str = "14:00:00"
    time_source: str = TIME_SOURCE

    def to_dict(self) -> Dict[str, Any]:
        return {
            "announced_at": self.announced_at.isoformat(),
            "nyc_date": self.nyc_date.isoformat(),
            "meeting": self.meeting,
            "action": self.action,
            "delta_bp": self.delta_bp,
            "target_low": self.target_low,
            "target_high": self.target_high,
            "announced_et": self.announced_et,
            "time_source": self.time_source,
        }


@dataclass(frozen=True)
class FomcGate:
    """Per-tick entry gate derived from the event table + strategy mode."""

    blocked: bool
    reason: str
    event: Optional[FomcEvent]
    cleared_after: Optional[datetime]


def _rows() -> List[FomcEvent]:
    out: List[FomcEvent] = []
    for iso, meeting, action, delta_bp, lo, hi in _MEETINGS:
        d = date.fromisoformat(iso)
        out.append(FomcEvent(
            announced_at=announced_at_utc(d),
            nyc_date=d,
            meeting=meeting,
            action=action,
            delta_bp=delta_bp,
            target_low=lo,
            target_high=hi,
        ))
    return out


def all_events() -> List[FomcEvent]:
    """Full locked meeting list (not filtered by backtest window)."""
    return list(_rows())


def events_frame(
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> pd.DataFrame:
    """DataFrame indexed by ``announced_at`` (UTC)."""
    rows = _rows()
    if start is not None:
        start_u = _as_utc(start)
        rows = [r for r in rows if r.announced_at >= start_u]
    if end is not None:
        end_u = _as_utc(end)
        rows = [r for r in rows if r.announced_at <= end_u]
    if not rows:
        return pd.DataFrame(
            columns=[
                "nyc_date", "meeting", "action", "delta_bp",
                "target_low", "target_high", "announced_et", "time_source",
            ],
        )
    idx = pd.DatetimeIndex([r.announced_at for r in rows], name="announced_at")
    return pd.DataFrame(
        {
            "nyc_date": [r.nyc_date.isoformat() for r in rows],
            "meeting": [r.meeting for r in rows],
            "action": [r.action for r in rows],
            "delta_bp": [r.delta_bp for r in rows],
            "target_low": [r.target_low for r in rows],
            "target_high": [r.target_high for r in rows],
            "announced_et": [r.announced_et for r in rows],
            "time_source": [r.time_source for r in rows],
        },
        index=idx,
    )


def build_fomc_decision(
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    **_params: Any,
) -> pd.DataFrame:
    """Pipeline builder. Klines are not used."""
    return events_frame(start=start, end=end)


def event_on_nyc_date(
    d: date,
    events: Optional[Sequence[FomcEvent]] = None,
) -> Optional[FomcEvent]:
    """Return the meeting whose statement prints on NYC date *d*, if any."""
    for ev in (events if events is not None else _rows()):
        if ev.nyc_date == d:
            return ev
    return None


def events_from_frame(df: Optional[pd.DataFrame]) -> List[FomcEvent]:
    """Rebuild ``FomcEvent`` list from the pipeline DataFrame.

    ``None`` (indicator not injected) falls back to the full lock so unit
    tests work without ``build_indicators``. An empty frame means the
    backtest window contained no meetings — do not revive the full lock.
    """
    if df is None:
        return _rows()
    if df.empty:
        return []
    out: List[FomcEvent] = []
    for ts, row in df.iterrows():
        announced = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
        announced = _as_utc(announced)
        nyc = row["nyc_date"]
        if not isinstance(nyc, date):
            nyc = date.fromisoformat(str(nyc))
        delta = row["delta_bp"]
        if delta is not None and not (isinstance(delta, float) and pd.isna(delta)):
            delta_bp: Optional[int] = int(delta)
        else:
            delta_bp = None
        out.append(FomcEvent(
            announced_at=announced,
            nyc_date=nyc,
            meeting=str(row["meeting"]),
            action=str(row["action"]),
            delta_bp=delta_bp,
            target_low=float(row["target_low"]),
            target_high=float(row["target_high"]),
            announced_et=str(row.get("announced_et", "14:00:00")),
            time_source=str(row.get("time_source", TIME_SOURCE)),
        ))
    return out


def fomc_entry_gate(
    now_utc: datetime,
    *,
    mode: str = "off",
    delay_minutes: float = 0.0,
    events: Optional[Sequence[FomcEvent]] = None,
) -> FomcGate:
    """Should a new entry be blocked at *now_utc*?

    ``mode``:
        off    — never block (baseline Monopteros).
        skip   — block every tick on the NYC date of a meeting.
        delay  — block while ``now < announced_at + delay_minutes`` on that
                 NYC date; allow from the clear instant (late entry).
    """
    now = _as_utc(now_utc)
    mode_n = str(mode or "off").strip().lower()
    if mode_n in ("", "off", "none", "0"):
        return FomcGate(False, "off", None, None)

    nyc_date = now.astimezone(_NYC).date()
    ev = event_on_nyc_date(nyc_date, events)
    if ev is None:
        return FomcGate(False, "no_event", None, None)

    if mode_n == "skip":
        return FomcGate(True, "skip_day", ev, None)

    if mode_n != "delay":
        raise ValueError(
            f"unknown fomc gate mode {mode!r}; expected off, skip, or delay"
        )

    cleared = ev.announced_at + timedelta(minutes=float(delay_minutes))
    if now < cleared:
        return FomcGate(True, "before_clear", ev, cleared)
    return FomcGate(False, "cleared", ev, cleared)

"""Explicit weekly UTC quoting schedule for classifying tick gaps.

A schedule is supplied by the caller, never inferred from ticker history.
Each interval is [opening, closing) and may cross midnight.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta

from mt5_datahub.timeutils import validate_range


@dataclass(frozen=True, slots=True)
class WeeklySession:
    weekday: int  # Monday=0 ... Sunday=6
    opens: time
    closes: time

    def __post_init__(self) -> None:
        if self.weekday not in range(7):
            raise ValueError("weekday must be 0..6")
        if self.opens.tzinfo is not None or self.closes.tzinfo is not None:
            raise ValueError("Session times must be naive UTC wall-clock times")
        if self.opens == self.closes:
            raise ValueError("Opening and closing cannot be equal")


@dataclass(frozen=True, slots=True)
class SessionSchedule:
    sessions: tuple[WeeklySession, ...]
    name: str = "user-supplied UTC schedule"

    def open_seconds_during(self, start: datetime, end: datetime) -> float:
        """Duration overlapping quoted sessions in [start,end)."""
        start, end = validate_range(start, end)
        total = 0.0
        day = (start - timedelta(days=1)).date()
        final = end.date()
        while day <= final:
            for session in self.sessions:
                if day.weekday() != session.weekday:
                    continue
                opened = datetime.combine(day, session.opens, UTC)
                closed = datetime.combine(day, session.closes, UTC)
                if closed <= opened:
                    closed += timedelta(days=1)
                if opened < end and closed > start:
                    total += (min(closed, end) - max(opened, start)).total_seconds()
            day += timedelta(days=1)
        return total


def classify_gap(
    start: datetime,
    end: datetime,
    schedule: SessionSchedule | None,
) -> str:
    """Return 'session_break', 'unexplained_gap' or 'unverified_history'."""
    if schedule is None:
        return "unverified_history"
    start, end = validate_range(start, end)
    seconds = (end - start).total_seconds()
    open_seconds = schedule.open_seconds_during(start, end)
    tolerated = min(120.0, seconds * 0.05)
    return "session_break" if open_seconds <= tolerated else "unexplained_gap"

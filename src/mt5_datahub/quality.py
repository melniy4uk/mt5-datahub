"""Historical tick-quality audit for an existing Parquet cache.

Large quote gaps are suspicious, not proof of missing market data.
"""
from __future__ import annotations

from dataclasses import dataclass
from heapq import heappush, heappushpop
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

from mt5_datahub.sessions import SessionSchedule, classify_gap
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import from_us, to_us, validate_range


@dataclass(frozen=True, slots=True)
class TickGap:
    before: datetime
    after: datetime
    seconds: float
    classification: str = "unverified_history"


@dataclass(frozen=True, slots=True)
class TickQualityReport:
    symbol: str
    source: str
    start: datetime
    end: datetime
    ticks: int
    segments: int
    uncovered: tuple[tuple[datetime, datetime], ...]
    large_gaps: tuple[TickGap, ...]
    repeated_timestamps: int
    out_of_order_timestamps: int
    session_schedule_name: str | None = None

    @property
    def cached_request_coverage(self) -> bool:
        """Coverage of saved *requests*, not verification of complete ticks."""
        return not self.uncovered

    def format(self) -> str:
        lines = [
            f"Instrument: {self.symbol}",
            f"Source: {self.source}",
            f"Range UTC: {self.start.isoformat()} to {self.end.isoformat()}",
            f"Stored ticks: {self.ticks:,}",
            f"Parquet segments: {self.segments}",
            f"Uncovered request intervals: {len(self.uncovered)}",
            f"Large inter-tick gaps: {len(self.large_gaps)}",
            f"Repeated millisecond timestamps: {self.repeated_timestamps}",
            f"Out-of-order timestamps: {self.out_of_order_timestamps}",
            f"Session schedule: {self.session_schedule_name or 'not provided'}",
        ]
        for left, right in self.uncovered:
            lines.append(f"UNCOVERED {left.isoformat()} .. {right.isoformat()}")
        for gap in self.large_gaps:
            lines.append(
                f"GAP {gap.before.isoformat()} .. {gap.after.isoformat()}"
                f" ({gap.seconds:.3f}s) [{gap.classification}]"
            )
        lines.append(
            "NOTE: Neither stored intervals nor inter-tick gaps establish "
            "completeness. Weekends, market breaks and broker sessions may "
            "explain large quote gaps."
        )
        return "\n".join(lines)


def audit_ticks(
    cache: ParquetCache | str | Path,
    source: str,
    symbol: str,
    start: datetime,
    end: datetime,
    *,
    gap_threshold: timedelta = timedelta(minutes=5),
    max_gaps: int = 20,
    schedule: SessionSchedule | None = None,
) -> TickQualityReport:
    """Inspect saved XAUUSD.s ticks without querying MT5.

    max_gaps limits only the *reported* gaps; it never changes tick totals.
    A supplied session schedule classifies gaps as expected session breaks\n    or unexplained open-session gaps; without one gaps remain unverified.\n    This is not a certification of broker-history completeness.
    """
    start, end = validate_range(start, end)
    if not symbol.strip() or not source.strip():
        raise ValueError("source and symbol must not be empty")
    if gap_threshold <= timedelta(0):
        raise ValueError("gap_threshold must be positive")
    if max_gaps < 0:
        raise ValueError("max_gaps must not be negative")

    storage = cache if isinstance(cache, ParquetCache) else ParquetCache(cache)
    first, last = to_us(start), to_us(end)
    raw_segments = storage._segments_for(source, symbol, "ticks", 0, first, last)
    uncovered_us = storage.missing(source, symbol, "ticks", 0, first, last)
    threshold_ms = gap_threshold / timedelta(milliseconds=1)

    # Read one time column in bounded batches. Maintain only the longest
    # max_gaps pauses, instead of assembling millions of full tick records.
    # Historical overlapping segments can be out of order: fall back to the
    # full sorted read only in that exceptional case.
    candidates: list[tuple[int, int, int, int]] = []
    count = repeated = disorder = 0
    previous: int | None = None
    def scan(times: np.ndarray) -> None:
        nonlocal count, repeated, disorder, previous
        if not len(times):
            return
        if previous is None:
            lefts = times[:-1]
            rights = times[1:]
            indices_start = count
        else:
            lefts = np.concatenate(([previous], times[:-1]))
            rights = times
            indices_start = count - 1
        deltas = rights.astype(np.int64) - lefts.astype(np.int64)
        repeated += int(np.count_nonzero(deltas == 0))
        disorder += int(np.count_nonzero(deltas < 0))
        if max_gaps:
            for idx in np.flatnonzero(deltas > threshold_ms):
                duration = int(deltas[idx])
                # Earlier pauses win ties; keep the largest durations.
                entry = (duration, -(indices_start + int(idx)),
                         int(lefts[idx]), int(rights[idx]))
                if len(candidates) < max_gaps:
                    heappush(candidates, entry)
                elif entry > candidates[0]:
                    heappushpop(candidates, entry)
        count += len(times)
        previous = int(times[-1])

    for batch in storage.iter_batches(
        source, symbol, "ticks", 0, first, last,
        columns=("time_msc",), batch_size=65_536,
    ):
        scan(batch["time_msc"])

    raw_disorder = disorder
    if raw_disorder:
        # Old overlapping files may not be globally ordered. Compute quote
        # gaps from a sorted view, but still report physical-order anomalies.
        sorted_times = storage.read(
            source, symbol, "ticks", 0, first, last,
            columns=("time_msc",),
        )["time_msc"]
        candidates.clear()
        count = repeated = disorder = 0
        previous = None
        scan(sorted_times)

    selected = sorted(candidates, key=lambda item: (-item[0], -item[1]))
    gaps = tuple(
        TickGap(
            before=from_us(left_ms * 1000),
            after=from_us(right_ms * 1000),
            seconds=duration / 1000,
            classification=classify_gap(
                from_us(left_ms * 1000), from_us(right_ms * 1000), schedule
            ),
        )
        for duration, _, left_ms, right_ms in selected
    )
    return TickQualityReport(
        symbol=symbol,
        source=source,
        start=start,
        end=end,
        ticks=count,
        segments=len(raw_segments),
        uncovered=tuple((from_us(left), from_us(right)) for left, right in uncovered_us),
        large_gaps=gaps,
        repeated_timestamps=repeated,
        out_of_order_timestamps=raw_disorder,
        session_schedule_name=schedule.name if schedule is not None else None,
    )

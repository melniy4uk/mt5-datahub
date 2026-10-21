"""Historical tick-quality audit for an existing Parquet cache.

Large quote gaps are suspicious, not proof of missing market data.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import from_us, to_us, validate_range


@dataclass(frozen=True, slots=True)
class TickGap:
    before: datetime
    after: datetime
    seconds: float


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
        ]
        for left, right in self.uncovered:
            lines.append(f"UNCOVERED {left.isoformat()} .. {right.isoformat()}")
        for gap in self.large_gaps:
            lines.append(
                f"GAP {gap.before.isoformat()} .. {gap.after.isoformat()}"
                f" ({gap.seconds:.3f}s)"
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
) -> TickQualityReport:
    """Inspect saved XAUUSD.s ticks without querying MT5.

    max_gaps limits only the *reported* gaps; it never changes tick totals.
    This is a diagnostic, not a certification of broker-history completeness.
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
    rows = storage.read(source, symbol, "ticks", 0, first, last)
    times_ms = rows["time_msc"].astype(np.int64)
    deltas_ms = np.diff(times_ms)
    min_gap_ms = gap_threshold / timedelta(milliseconds=1)
    candidate_indices = np.flatnonzero(deltas_ms > min_gap_ms)
    # Show longest gaps first, then chronological order for equal lengths.
    order = sorted(candidate_indices.tolist(), key=lambda i: (-int(deltas_ms[i]), i))
    selected = order[:max_gaps]

    gaps = tuple(
        TickGap(
            before=from_us(int(times_ms[i]) * 1000),
            after=from_us(int(times_ms[i + 1]) * 1000),
            seconds=float(deltas_ms[i]) / 1000,
        )
        for i in selected
    )
    return TickQualityReport(
        symbol=symbol,
        source=source,
        start=start,
        end=end,
        ticks=len(rows),
        segments=len(raw_segments),
        uncovered=tuple((from_us(left), from_us(right)) for left, right in uncovered_us),
        large_gaps=gaps,
        repeated_timestamps=int(np.count_nonzero(deltas_ms == 0)),
        out_of_order_timestamps=int(np.count_nonzero(deltas_ms < 0)),
    )

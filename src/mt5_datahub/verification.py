"""Compare an existing tick cache with a fresh MT5 read, without modifying it.

This verifies *repeatability of the same broker's reply*, not independent
historical completeness. A broker can return the same truncated data twice.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Protocol

import numpy as np
from numpy import ndarray

from mt5_datahub.arrays import TICK_DTYPE, empty
from mt5_datahub.exceptions import MT5RequestError, StorageError
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import from_us, to_us, validate_range


class TickReader(Protocol):
    def get_ticks(self, symbol: str, start: datetime, end: datetime) -> ndarray: ...


@dataclass(frozen=True, slots=True)
class TickDifference:
    start: datetime
    end: datetime
    reason: str
    cached_rows: int
    fresh_rows: int | None


@dataclass(frozen=True, slots=True)
class TickComparison:
    symbol: str
    source: str
    start: datetime
    end: datetime
    windows: int
    matched_windows: int
    compared_ticks: int
    source_binding: str
    issue_count: int
    differences: tuple[TickDifference, ...]

    @property
    def matches(self) -> bool:
        """All windows matched the repeat read; NOT a completeness guarantee."""
        return self.issue_count == 0 and self.matched_windows == self.windows

    def format(self) -> str:
        lines = [
            f"Symbol: {self.symbol}",
            f"Source: {self.source}",
            f"Range UTC: {self.start.isoformat()} .. {self.end.isoformat()}",
            f"Windows: {self.windows}; exact matches: {self.matched_windows}",
            f"Compared cached ticks: {self.compared_ticks:,}",
            f"Comparison issues: {self.issue_count}",
            f"Broker account binding: {self.source_binding}",
        ]
        for issue in self.differences:
            fresh = "N/A" if issue.fresh_rows is None else str(issue.fresh_rows)
            lines.append(
                f"DIFF {issue.start.isoformat()} .. {issue.end.isoformat()} "
                f"[{issue.reason}] cached={issue.cached_rows} fresh={fresh}"
            )
        if self.issue_count > len(self.differences):
            lines.append(
                f"... {self.issue_count - len(self.differences)} more issues not displayed"
            )
        lines.append(
            "NOTE: matching the same MT5 broker twice does not establish "
            "independent historical completeness."
        )
        return "\n".join(lines)


def compare_cached_ticks(
    cache: ParquetCache | str | Path,
    source: str,
    symbol: str,
    start: datetime,
    end: datetime,
    provider: TickReader,
    *,
    window: timedelta = timedelta(hours=1),
    max_rows_per_window: int = 250_000,
    max_reported_issues: int = 20,
) -> TickComparison:
    """Read cache once and re-fetch MT5 chunks without writing to the cache.

    The provider must already be connected. Cached ticks stream from Parquet
    in timestamp order. Each interval is checked with *all tick columns*,
    comparing sorted records as multisets to preserve same-ms multiplicity.

    Memory is bounded for the stored side by max_rows_per_window plus a single
    Parquet batch. The MetaTrader5 API itself materializes one whole native
    response and can still exceed this limit before returning.
    """
    start, end = validate_range(start, end)
    if not source.strip() or not symbol.strip():
        raise ValueError("source and symbol must not be empty")
    if window <= timedelta(0) or max_rows_per_window < 1:
        raise ValueError("window and max_rows_per_window must be positive")
    if max_reported_issues < 0:
        raise ValueError("max_reported_issues must not be negative")
    storage = cache if isinstance(cache, ParquetCache) else ParquetCache(cache)

    bound_identity = storage.get_source_identity(source)
    identity_fn = getattr(provider, "source_identity", None)
    live_identity = identity_fn() if callable(identity_fn) else None
    if bound_identity and live_identity and bound_identity != live_identity:
        raise StorageError(
            f"Cache source {source!r} is bound to another MT5 account/server"
        )
    binding = (
        "legacy-unbound" if bound_identity is None else
        "unknown-live-account" if live_identity is None else "matched"
    )

    first, last = to_us(start), to_us(end)
    step_us = int(window / timedelta(microseconds=1))
    batches = iter(storage.iter_batches(
        source, symbol, "ticks", 0, first, last, batch_size=65_536,
    ))
    pending = next(batches, None)
    offset = 0

    differences: list[TickDifference] = []
    issue_count = matched = windows = compared = 0
    cursor = first
    while cursor < last:
        right = min(cursor + step_us, last)
        count = 0
        parts: list[ndarray] = []
        oversized = False
        # Integer tick milliseconds must satisfy time_msc * 1000 < right.
        cutoff_ms = (right + 999) // 1000
        while pending is not None:
            split = int(np.searchsorted(
                pending["time_msc"], cutoff_ms, side="left",
            ))
            if split > offset:
                count += split - offset
                if count > max_rows_per_window:
                    oversized = True
                    parts.clear()
                if not oversized:
                    parts.append(pending[offset:split])
                offset = split
            if offset < len(pending):
                break
            pending = next(batches, None)
            offset = 0

        left_dt, right_dt = from_us(cursor), from_us(right)
        windows += 1
        issue: TickDifference | None = None
        if oversized:
            issue = TickDifference(
                left_dt, right_dt, "cached-window-too-large", count, None,
            )
        elif storage.missing(source, symbol, "ticks", 0, cursor, right):
            issue = TickDifference(
                left_dt, right_dt, "uncovered-cache-range", count, None,
            )
        else:
            cached = np.concatenate(parts) if parts else empty("ticks")
            fresh = provider.get_ticks(symbol, left_dt, right_dt)
            if fresh.dtype != TICK_DTYPE:
                raise MT5RequestError(
                    "Fresh tick response does not have the expected MT5 dtype"
                )
            if len(fresh) > max_rows_per_window:
                issue = TickDifference(
                    left_dt, right_dt, "fresh-window-too-large", count, len(fresh),
                )
            else:
                compared += count
                fields = list(TICK_DTYPE.names or ())
                equal = len(cached) == len(fresh) and np.array_equal(
                    np.sort(cached, order=fields, kind="stable"),
                    np.sort(fresh, order=fields, kind="stable"),
                )
                if equal:
                    matched += 1
                else:
                    issue = TickDifference(
                        left_dt, right_dt, "different-tick-records",
                        count, len(fresh),
                    )
        if issue is not None:
            issue_count += 1
            if len(differences) < max_reported_issues:
                differences.append(issue)
        cursor = right
    return TickComparison(
        symbol=symbol, source=source, start=start, end=end,
        windows=windows, matched_windows=matched, compared_ticks=compared,
        source_binding=binding, issue_count=issue_count,
        differences=tuple(differences),
    )

"""Public offline inspection of cached MT5 request ranges and Parquet files."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeframes import VALID
from mt5_datahub.timeutils import from_us, to_us, validate_range


@dataclass(frozen=True, slots=True)
class CacheInspection:
    """Cache status, NOT a certificate of complete broker history."""

    source: str
    symbol: str
    kind: str
    timeframe: int
    start: datetime
    end: datetime
    unrequested: tuple[tuple[datetime, datetime], ...]
    corrupt_segments: dict[str, str]

    @property
    def saved_request_coverage(self) -> bool:
        """Only confirms that the requested intervals were saved."""
        return not self.unrequested

    @property
    def files_readable(self) -> bool:
        return not self.corrupt_segments


def inspect_cache(
    cache: ParquetCache | str | Path,
    source: str,
    symbol: str,
    start: datetime,
    end: datetime,
    *,
    kind: str = "ticks",
    timeframe: int = 0,
) -> CacheInspection:
    """Inspect coverage and integrity without any broker or terminal connection.

    Unrequested intervals are not evidence that quotes exist during sessions.
    Conversely, saved requests are not proof MT5 supplied complete history.
    """
    start, end = validate_range(start, end)
    if not source.strip() or not symbol.strip():
        raise ValueError("source and symbol must not be empty")
    if kind not in ("ticks", "candles"):
        raise ValueError("kind must be 'ticks' or 'candles'")
    if kind == "ticks" and timeframe != 0:
        raise ValueError("Tick data requires timeframe=0")
    if kind == "candles" and timeframe not in VALID:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    storage = cache if isinstance(cache, ParquetCache) else ParquetCache(cache)
    first, last = to_us(start), to_us(end)
    uncovered = tuple(
        (from_us(left), from_us(right))
        for left, right in storage.missing(source, symbol, kind, timeframe, first, last)
    )
    broken = storage.verify_segments(source, symbol, kind, timeframe, first, last)
    return CacheInspection(
        source=source,
        symbol=symbol,
        kind=kind,
        timeframe=timeframe,
        start=start,
        end=end,
        unrequested=uncovered,
        corrupt_segments=broken,
    )

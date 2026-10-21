"""Public service combining MetaTrader 5 requests and persistent caching."""
from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Self

from numpy import ndarray

from mt5_datahub.providers.mt5 import MT5Provider
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeframes import VALID, closed_only
from mt5_datahub.timeutils import from_us, to_us, validate_range

logger = logging.getLogger(__name__)


class MarketData:
    """Historical MT5 arrays with best-effort request caching.

    The cache does not certify historical completeness. One process must own
    the cache writer. A source string identifies the broker/account dataset.
    """

    def __init__(
        self,
        source: str,
        cache_dir: str | Path = ".mt5-datahub",
        provider: MT5Provider | None = None,
        tick_chunk: timedelta = timedelta(hours=2),
        candle_chunk: timedelta = timedelta(days=7),
    ) -> None:
        if not source.strip():
            raise ValueError("source must identify broker/server/account")
        if tick_chunk <= timedelta(0) or candle_chunk <= timedelta(0):
            raise ValueError("Chunk durations must be positive")
        self.source = source
        self.cache = ParquetCache(cache_dir)
        self._provider = provider if provider is not None else MT5Provider()
        self._owns_provider = provider is None
        self._connected = False
        self._chunk = {
            "ticks": int(tick_chunk / timedelta(microseconds=1)),
            "candles": int(candle_chunk / timedelta(microseconds=1)),
        }

    def connect(self) -> None:
        if not self._connected:
            self._provider.connect()
            self._connected = True

    def disconnect(self) -> None:
        if self._connected and self._owns_provider:
            self._provider.disconnect()
        self._connected = False

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        self.disconnect()

    def get_ticks(self, symbol: str, start: datetime, end: datetime) -> ndarray:
        """Return ticks with timestamps in [start, end)."""
        return self._get(symbol, "ticks", 0, start, end)

    def get_candles(
        self, symbol: str, timeframe: int, start: datetime, end: datetime
    ) -> ndarray:
        """Return closed candles whose opening time is in [start, end)."""
        if timeframe not in VALID:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        return self._get(symbol, "candles", timeframe, start, end)

    def iter_ticks(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        columns: tuple[str, ...] | None = None,
        batch_size: int = 65_536,
    ) -> Iterator[ndarray]:
        """Download uncached ranges, then yield bounded NumPy tick batches.

        The iterator is lazy and does not assemble the full result in memory.
        Downloaded data remains cached if iteration stops or fails.
        Batches follow Parquet segment order, which might not be globally
        sorted when cached segments overlap.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        first, last = self._prepare_range(symbol, start, end)
        if first >= last:
            return
        self._fill_cache(symbol, "ticks", 0, first, last)
        yield from self.cache.iter_batches(
            self.source, symbol, "ticks", 0, first, last,
            columns=columns, batch_size=batch_size,
        )

    def stream_ticks(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        columns: tuple[str, ...] | None = None,
        batch_size: int = 65_536,
    ) -> Iterator[ndarray]:
        """Fetch, persist and yield one bounded time interval at a time.

        Stops downloading when the caller stops consuming. Cached intervals
        survive interruption. A single MT5 request can still be large.
        """
        from mt5_datahub.arrays import TICK_DTYPE

        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        names = tuple(TICK_DTYPE.names or ())
        if columns is not None and (
            not columns or len(set(columns)) != len(columns)
            or any(name not in names for name in columns)
        ):
            raise ValueError("Unknown or duplicated requested column")
        first, last = self._prepare_range(symbol, start, end)
        cursor = first
        while cursor < last:
            stop = min(cursor + self._chunk["ticks"], last)
            self._fill_cache(symbol, "ticks", 0, cursor, stop)
            yield from self.cache.iter_batches(
                self.source, symbol, "ticks", 0, cursor, stop,
                columns=columns, batch_size=batch_size,
            )
            cursor = stop

    def _prepare_range(
        self, symbol: str, start: datetime, end: datetime
    ) -> tuple[int, int]:
        start, end = validate_range(start, end)
        if not symbol.strip():
            raise ValueError("symbol cannot be empty")
        return to_us(start), min(to_us(end), to_us(datetime.now(UTC)))

    def _fill_cache(
        self, symbol: str, kind: str, timeframe: int, first: int, last: int
    ) -> None:
        if first >= last:
            return
        gaps = self.cache.missing(self.source, symbol, kind, timeframe, first, last)
        if gaps:
            self.connect()
        for left, right in gaps:
            cursor = left
            while cursor < right:
                stop = min(cursor + self._chunk[kind], right)
                if kind == "ticks":
                    rows = self._provider.get_ticks(symbol, from_us(cursor), from_us(stop))
                else:
                    rows = self._provider.get_candles(
                        symbol, timeframe, from_us(cursor), from_us(stop)
                    )
                    rows = closed_only(rows, timeframe, to_us(datetime.now(UTC)))
                if len(rows):
                    self.cache.save(self.source, symbol, kind, timeframe, rows, cursor, stop)
                else:
                    logger.debug("No %s for %s in [%s, %s)", kind, symbol, cursor, stop)
                cursor = stop

    def _get(
        self, symbol: str, kind: str, timeframe: int,
        start: datetime, end: datetime,
    ) -> ndarray:
        first, last = self._prepare_range(symbol, start, end)
        if first >= last:
            from mt5_datahub.arrays import empty
            return empty(kind)
        self._fill_cache(symbol, kind, timeframe, first, last)
        return self.cache.read(self.source, symbol, kind, timeframe, first, last)

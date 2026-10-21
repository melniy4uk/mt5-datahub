"""Public service combining MetaTrader 5 requests and persistent caching."""
from __future__ import annotations

import logging
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

    def _get(
        self, symbol: str, kind: str, timeframe: int,
        start: datetime, end: datetime,
    ) -> ndarray:
        start, end = validate_range(start, end)
        if not symbol.strip():
            raise ValueError("symbol cannot be empty")
        first, last = to_us(start), to_us(end)
        # Do not query a future period. The cache may contain old closed bars,
        # but current unfinished bars must never be stored.
        last = min(last, to_us(datetime.now(UTC)))
        if first >= last:
            from mt5_datahub.arrays import empty
            return empty(kind)
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
        return self.cache.read(self.source, symbol, kind, timeframe, first, last)

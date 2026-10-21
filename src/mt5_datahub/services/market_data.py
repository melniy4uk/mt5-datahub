"""Cached MT5 market data service."""
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Self

from numpy import ndarray

from mt5_datahub.providers.mt5 import MT5Provider
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeframes import VALID, closed_only
from mt5_datahub.timeutils import from_us, to_us, validate_range


class MarketData:
    def __init__(self, source: str, cache_dir: str | Path = ".mt5-datahub",
                 provider: MT5Provider | None = None,
                 tick_chunk: timedelta = timedelta(hours=2),
                 candle_chunk: timedelta = timedelta(days=7)) -> None:
        if not source.strip():
            raise ValueError("source must identify the broker/account dataset")
        if tick_chunk <= timedelta(0) or candle_chunk <= timedelta(0):
            raise ValueError("Chunk sizes must be positive")
        self.source = source
        self.cache = ParquetCache(cache_dir)
        self._provider = provider if provider is not None else MT5Provider()
        self._owns_provider = provider is None
        self._started = False
        self._tick_chunk_us = int(tick_chunk / timedelta(microseconds=1))
        self._candle_chunk_us = int(candle_chunk / timedelta(microseconds=1))

    def connect(self) -> None:
        if not self._started:
            self._provider.connect()
            self._started = True

    def disconnect(self) -> None:
        if self._started and self._owns_provider:
            self._provider.disconnect()
        self._started = False

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        self.disconnect()

    def get_ticks(self, symbol: str, start: datetime, end: datetime) -> ndarray:
        return self._get(symbol, "ticks", 0, start, end)

    def get_candles(self, symbol: str, timeframe: int,
                    start: datetime, end: datetime) -> ndarray:
        if timeframe not in VALID:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        return self._get(symbol, "candles", timeframe, start, end)

    def _get(self, symbol: str, kind: str, timeframe: int,
             start: datetime, end: datetime) -> ndarray:
        start, end = validate_range(start, end)
        if not symbol.strip():
            raise ValueError("symbol cannot be empty")
        first, last = to_us(start), to_us(end)
        gaps = self.cache.missing(self.source, symbol, kind, timeframe, first, last)
        if not gaps:
            return self.cache.read(self.source, symbol, kind, timeframe, first, last)
        self.connect()
        step = self._tick_chunk_us if kind == "ticks" else self._candle_chunk_us
        for left, right in gaps:
            current = left
            while current < right:
                stop = min(current + step, right)
                if kind == "ticks":
                    rows = self._provider.get_ticks(symbol, from_us(current), from_us(stop))
                else:
                    rows = self._provider.get_candles(
                        symbol, timeframe, from_us(current), from_us(stop))
                    rows = closed_only(rows, timeframe, to_us(datetime.now(UTC)))
                if len(rows):
                    self.cache.save(self.source, symbol, kind, timeframe, rows, current, stop)
                current = stop
        return self.cache.read(self.source, symbol, kind, timeframe, first, last)

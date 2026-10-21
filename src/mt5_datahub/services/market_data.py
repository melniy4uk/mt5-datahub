"""Historical data façade with conservative, reusable Parquet caching."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Self

import MetaTrader5 as mt5
import numpy as np
from numpy import ndarray

from mt5_datahub.providers.mt5 import MT5Provider
from mt5_datahub.storage.parquet import ParquetCache

logger = logging.getLogger(__name__)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

_SECONDS = {
    mt5.TIMEFRAME_M1: 60,
    mt5.TIMEFRAME_M2: 120,
    mt5.TIMEFRAME_M3: 180,
    mt5.TIMEFRAME_M4: 240,
    mt5.TIMEFRAME_M5: 300,
    mt5.TIMEFRAME_M6: 360,
    mt5.TIMEFRAME_M10: 600,
    mt5.TIMEFRAME_M12: 720,
    mt5.TIMEFRAME_M15: 900,
    mt5.TIMEFRAME_M20: 1200,
    mt5.TIMEFRAME_M30: 1800,
    mt5.TIMEFRAME_H1: 3600,
    mt5.TIMEFRAME_H2: 7200,
    mt5.TIMEFRAME_H3: 10800,
    mt5.TIMEFRAME_H4: 14400,
    mt5.TIMEFRAME_H6: 21600,
    mt5.TIMEFRAME_H8: 28800,
    mt5.TIMEFRAME_H12: 43200,
    mt5.TIMEFRAME_D1: 86400,
    mt5.TIMEFRAME_W1: 604800,
}


def _unix_us(value: datetime) -> int:
    offset = value - EPOCH
    return (offset.days * 86400 + offset.seconds) * 1_000_000 + offset.microseconds


def _datetime(us: int) -> datetime:
    return EPOCH + timedelta(microseconds=us)


def _bar_end_us(open_seconds: int, timeframe: int) -> int:
    """Next bar opening, including monthly periods of differing length."""
    opened = datetime.fromtimestamp(open_seconds, UTC)
    if timeframe == mt5.TIMEFRAME_MN1:
        year, month = opened.year, opened.month
        return _unix_us(datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=UTC))
    return open_seconds * 1_000_000 + _SECONDS[timeframe] * 1_000_000


class MarketData:
    """Fetch/caches historical MT5 arrays by source, symbol and timeframe.

    The index records observed (nonempty) ranges only. Missing edges and
    empty market hours may be re-requested. No completeness guarantee is made.
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
            raise ValueError("source must identify the MT5 broker/server/account dataset")
        if tick_chunk <= timedelta(0) or candle_chunk <= timedelta(0):
            raise ValueError("Chunk sizes must be positive")
        self.source = source
        self.cache = ParquetCache(cache_dir)
        self._provider = provider if provider is not None else MT5Provider()
        self._owns_provider = provider is None
        self._started = False
        self._tick_chunk_us = _unix_us(EPOCH + tick_chunk)
        self._candle_chunk_us = _unix_us(EPOCH + candle_chunk)

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
        """All available cached and newly fetched ticks in [start, end)."""
        return self._get(symbol, 0, "ticks", start, end)

    def get_candles(
        self, symbol: str, timeframe: int, start: datetime, end: datetime
    ) -> ndarray:
        """Closed historical bars only, selected by bar-open time [start, end)."""
        if timeframe not in (*_SECONDS, mt5.TIMEFRAME_MN1):
            raise ValueError(f"Unsupported MT5 timeframe: {timeframe}")
        return self._get(symbol, timeframe, "candles", start, end)

    def _get(
        self, symbol: str, timeframe: int, kind: str, start: datetime, end: datetime
    ) -> ndarray:
        start, end = MT5Provider._validate_dates(start, end)
        if not symbol.strip():
            raise ValueError("symbol cannot be empty")
        self.connect()
        key = self.source + "::" + symbol
        start_us, end_us = _unix_us(start), _unix_us(end)
        chunk_us = self._tick_chunk_us if kind == "ticks" else self._candle_chunk_us

        missing = self.cache.missing(key, kind, timeframe, start_us, end_us)
        logger.info(
            "%s %s: %d uncovered range(s) in [%s, %s)",
            kind, symbol, len(missing), start, end,
        )
        for gap_start, gap_end in missing:
            current = gap_start
            while current < gap_end:
                next_us = min(current + chunk_us, gap_end)
                from_time, to_time = _datetime(current), _datetime(next_us)
                if kind == "ticks":
                    rows = self._provider.get_ticks(symbol, from_time, to_time)
                    field, scale = "time_msc", 1_000
                else:
                    rows = self._provider.get_candles(
                        symbol, timeframe, from_time, to_time
                    )
                    field, scale = "time", 1_000_000
                    # Do not save or return bars still forming at query time.
                    now_us = _unix_us(datetime.now(UTC))
                    rows = rows[np.fromiter(
                        (_bar_end_us(int(t), timeframe) <= now_us for t in rows["time"]),
                        dtype=bool, count=len(rows),
                    )]
                if len(rows):
                    times = rows[field].astype(np.int64) * scale
                    observed_start = max(int(times.min()), current)
                    if kind == "ticks":
                        observed_end = int(times.max()) + scale
                    else:
                        observed_end = _bar_end_us(int(rows["time"].max()), timeframe)
                    observed_end = min(observed_end, next_us)
                    if observed_start < observed_end:
                        self.cache.save(
                            key, kind, timeframe, rows, observed_start, observed_end
                        )
                else:
                    logger.info("No %s received for %s in [%s, %s)", kind, symbol, from_time, to_time)
                current = next_us

        return self.cache.read(key, kind, timeframe, start_us, end_us)

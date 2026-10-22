"""MetaTrader 5 adapter. No caching or filesystem responsibilities."""
import logging
from datetime import datetime
from collections.abc import Iterator
from time import perf_counter
from typing import Self

import MetaTrader5 as mt5
import numpy as np
from numpy import ndarray

from mt5_datahub.arrays import TICK_DTYPE, select_range
from mt5_datahub.exceptions import MT5ConnectionError, MT5HistoryLimitError, MT5RequestError
from mt5_datahub.timeframes import VALID
from mt5_datahub.timeutils import from_us, to_us, validate_range

logger = logging.getLogger(__name__)


class MT5Provider:
    def __init__(self) -> None:
        self._connected = False

    def connect(self) -> None:
        if self._connected:
            self._check_connection()
            return
        if not mt5.initialize():
            code, message = mt5.last_error()
            raise MT5ConnectionError(f"MT5 initialize failed [{code}]: {message}")
        self._connected = True
        logger.info("MT5 terminal initialized")

    def disconnect(self) -> None:
        if self._connected:
            mt5.shutdown()
            self._connected = False
            logger.info("MT5 terminal disconnected")

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        self.disconnect()

    def _check_connection(self) -> None:
        if not self._connected:
            raise MT5ConnectionError("Call connect() before requesting data")
        terminal = mt5.terminal_info()
        if terminal is not None and terminal.connected:
            return
        logger.warning("MT5 connection lost; attempting one reinitialization")
        mt5.shutdown()
        self._connected = False
        if not mt5.initialize():
            code, message = mt5.last_error()
            raise MT5ConnectionError(f"MT5 reconnect failed [{code}]: {message}")
        terminal = mt5.terminal_info()
        if terminal is None or not terminal.connected:
            mt5.shutdown()
            code, message = mt5.last_error()
            raise MT5ConnectionError(f"MT5 broker still disconnected [{code}]: {message}")
        self._connected = True
        logger.info("MT5 connection restored")

    @staticmethod
    def source_identity() -> str | None:
        """Broker account/server fingerprint, never an access token."""
        account = mt5.account_info()
        if account is None:
            return None
        return f"{account.server}/{account.login}"

    @staticmethod
    def _ensure_symbol(symbol: str) -> None:
        if not symbol.strip():
            raise ValueError("symbol cannot be empty")
        info = mt5.symbol_info(symbol)
        if info is None:
            code, message = mt5.last_error()
            raise MT5RequestError(f"Unknown symbol {symbol!r} [{code}]: {message}")
        if not info.visible and not mt5.symbol_select(symbol, True):
            code, message = mt5.last_error()
            raise MT5RequestError(f"Cannot select {symbol!r} [{code}]: {message}")

    def _request(self, symbol: str, start: datetime, end: datetime, kind: str, timeframe: int = 0) -> ndarray:
        start, end = validate_range(start, end)
        started = perf_counter()
        for attempt in range(2):
            self._check_connection()
            self._ensure_symbol(symbol)
            if kind == "ticks":
                result = mt5.copy_ticks_range(symbol, start, end, mt5.COPY_TICKS_ALL)
            else:
                result = mt5.copy_rates_range(symbol, timeframe, start, end)
            if result is not None:
                filtered = select_range(result, kind, to_us(start), to_us(end))
                if kind == "candles":
                    terminal = mt5.terminal_info()
                    maxbars = getattr(terminal, "maxbars", None)
                    if isinstance(maxbars, int) and maxbars > 0 and len(result) >= maxbars:
                        raise MT5HistoryLimitError(
                            f"MT5 candle history for {symbol!r} reached "
                            f"Max. bars in chart={maxbars}; reduce request interval"
                        )
                logger.info(
                    "mt5_request kind=%s symbol=%s rows=%d elapsed_s=%.3f", kind, symbol,
                    len(filtered), perf_counter() - started,
                )
                return filtered
            # Do not retry bad symbols, invalid dates or unavailable history.
            # Only retry when the terminal actually lost its connection.
            terminal = mt5.terminal_info()
            if attempt == 0 and (terminal is None or not terminal.connected):
                logger.warning("mt5_retry kind=%s symbol=%s attempt=%d", kind, symbol, attempt + 1)
                continue
            code, message = mt5.last_error()
            logger.error(
                "mt5_request_failed kind=%s symbol=%s code=%d",
                kind, symbol, code,
            )
            raise MT5RequestError(
                f"MT5 {kind} request for {symbol} failed [{code}]: {message}"
            )
        raise MT5RequestError("MT5 request retry exhausted")

    def iter_tick_pages(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        page_size: int = 50_000,
    ) -> Iterator[tuple[ndarray, int, int]]:
        """Fetch count-bounded tick pages as (rows, left_us, right_us).

        Uses copy_ticks_from, whose *count* caps the number of records returned
        by any single native request. When the last millisecond is cut off by
        a full page, save only records strictly before that millisecond and
        re-request it inclusive on the next page. We never discard different
        ticks sharing a time_msc. If one millisecond itself exceeds page_size,
        fail rather than silently losing ticks.

        Coverage is best-effort broker-request coverage, not proof that MT5
        retains all historic broker quotes. An empty reply yields no coverage.
        """
        start, end = validate_range(start, end)
        if page_size < 2:
            raise ValueError("page_size must be >= 2")
        start_us, end_us = to_us(start), to_us(end)
        # Tick timestamps have 1 ms resolution. Round the inclusive start up:
        # any tick before that millisecond cannot belong to [start, end).
        cursor_us = ((start_us + 999) // 1000) * 1000
        if cursor_us >= end_us:
            return
        while cursor_us < end_us:
            started = perf_counter()
            result = None
            for attempt in range(2):
                self._check_connection()
                self._ensure_symbol(symbol)
                result = mt5.copy_ticks_from(
                    symbol, from_us(cursor_us), page_size, mt5.COPY_TICKS_ALL,
                )
                if result is not None:
                    break
                terminal = mt5.terminal_info()
                if attempt == 0 and (terminal is None or not terminal.connected):
                    logger.warning(
                        "mt5_page_retry symbol=%s attempt=%d", symbol, attempt + 1,
                    )
                    continue
                code, message = mt5.last_error()
                raise MT5RequestError(
                    f"MT5 tick page for {symbol!r} failed [{code}]: {message}"
                )
            if result is None:
                raise MT5RequestError("MT5 tick page retry exhausted")
            if result.dtype != TICK_DTYPE:
                raise MT5RequestError("MT5 returned an unexpected tick dtype")
            if len(result) > page_size:
                raise MT5RequestError("MT5 returned more ticks than requested")
            if len(result) == 0:
                return
            times = result["time_msc"]
            if np.any(np.diff(times) < 0):
                raise MT5RequestError("MT5 returned ticks out of chronological order")
            last_tick_us = int(times[-1]) * 1000
            if last_tick_us < cursor_us:
                raise MT5RequestError("MT5 tick pagination did not progress")

            if len(result) < page_size or last_tick_us >= end_us:
                # A non-full batch cannot be truncated by the count limit.
                # A batch reaching the request end has every preceding ms.
                selected = select_range(result, "ticks", cursor_us, end_us)
                if len(selected):
                    yield selected, cursor_us, end_us
                return

            if last_tick_us == cursor_us:
                raise MT5RequestError(
                    f"More than {page_size} ticks may share millisecond "
                    f"{cursor_us // 1000}; cannot paginate without loss. "
                    "Use a larger page_size or another broker API."
                )

            selected = select_range(result, "ticks", cursor_us, last_tick_us)
            if len(selected):
                yield selected, cursor_us, last_tick_us
            logger.debug(
                "mt5_page symbol=%s rows=%d window_us=%d elapsed_s=%.3f",
                symbol, len(selected), last_tick_us - cursor_us,
                perf_counter() - started,
            )
            cursor_us = last_tick_us

    def get_ticks(self, symbol: str, start: datetime, end: datetime) -> ndarray:
        return self._request(symbol, start, end, "ticks")

    def get_candles(self, symbol: str, timeframe: int, start: datetime, end: datetime) -> ndarray:
        if timeframe not in VALID:
            raise ValueError(f"Unsupported MT5 timeframe: {timeframe}")
        return self._request(symbol, start, end, "candles", timeframe)

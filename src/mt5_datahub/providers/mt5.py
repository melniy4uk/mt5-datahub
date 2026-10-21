"""MetaTrader 5 adapter. No caching or filesystem responsibilities."""
import logging
from datetime import datetime
from time import perf_counter
from typing import Self

import MetaTrader5 as mt5
from numpy import ndarray

from mt5_datahub.arrays import select_range
from mt5_datahub.exceptions import MT5ConnectionError, MT5RequestError
from mt5_datahub.timeframes import VALID
from mt5_datahub.timeutils import to_us, validate_range

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
                        logger.warning(
                            "MT5 candle response for %s reached Max. bars in chart=%d; "
                            "earlier requested history may be truncated",
                            symbol, maxbars,
                        )
                logger.info(
                    "MT5 %s %s: %d rows in %.3fs", kind, symbol,
                    len(filtered), perf_counter() - started,
                )
                return filtered
            # Do not retry bad symbols, invalid dates or unavailable history.
            # Only retry when the terminal actually lost its connection.
            terminal = mt5.terminal_info()
            if attempt == 0 and (terminal is None or not terminal.connected):
                logger.warning("MT5 %s request interrupted; retrying once", kind)
                continue
            code, message = mt5.last_error()
            raise MT5RequestError(
                f"MT5 {kind} request for {symbol} failed [{code}]: {message}"
            )
        raise MT5RequestError("MT5 request retry exhausted")

    def get_ticks(self, symbol: str, start: datetime, end: datetime) -> ndarray:
        return self._request(symbol, start, end, "ticks")

    def get_candles(self, symbol: str, timeframe: int, start: datetime, end: datetime) -> ndarray:
        if timeframe not in VALID:
            raise ValueError(f"Unsupported MT5 timeframe: {timeframe}")
        return self._request(symbol, start, end, "candles", timeframe)

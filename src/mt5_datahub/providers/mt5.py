import logging
from datetime import UTC, datetime
from typing import Self

import MetaTrader5 as mt5
from numpy import ndarray

from mt5_datahub.exceptions import MT5ConnectionError, MT5RequestError

logger = logging.getLogger(__name__)

VALID_TIMEFRAMES = frozenset(
    {
        mt5.TIMEFRAME_M1,
        mt5.TIMEFRAME_M2,
        mt5.TIMEFRAME_M3,
        mt5.TIMEFRAME_M4,
        mt5.TIMEFRAME_M5,
        mt5.TIMEFRAME_M6,
        mt5.TIMEFRAME_M10,
        mt5.TIMEFRAME_M12,
        mt5.TIMEFRAME_M15,
        mt5.TIMEFRAME_M20,
        mt5.TIMEFRAME_M30,
        mt5.TIMEFRAME_H1,
        mt5.TIMEFRAME_H2,
        mt5.TIMEFRAME_H3,
        mt5.TIMEFRAME_H4,
        mt5.TIMEFRAME_H6,
        mt5.TIMEFRAME_H8,
        mt5.TIMEFRAME_H12,
        mt5.TIMEFRAME_D1,
        mt5.TIMEFRAME_W1,
        mt5.TIMEFRAME_MN1,
    }
)


class MT5Provider:
    """Провайдер исторических тиков и свечей MetaTrader 5."""

    def __init__(self) -> None:
        self._connected: bool = False

    def connect(self) -> None:
        """Подключиться к терминалу MT5."""
        if self._connected:
            self._check_connection()
            return

        if not mt5.initialize():
            code, message = mt5.last_error()
            raise MT5ConnectionError(f"MT5: ошибка подключения [{code}]: {message}")

        self._connected = True
        logger.info("MT5: соединение установлено")

    def disconnect(self) -> None:
        """Отключиться от терминала MT5."""
        if not self._connected:
            return

        mt5.shutdown()
        self._connected = False
        logger.info("MT5: соединение закрыто")

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        self.disconnect()

    def _check_connection(self) -> None:
        """Проверить состояние терминала и соединения."""
        if not self._connected:
            raise MT5ConnectionError("MT5: сначала вызовите connect()")

        terminal = mt5.terminal_info()

        if terminal is None:
            self._connected = False
            code, message = mt5.last_error()
            raise MT5ConnectionError(
                f"MT5: связь с терминалом потеряна [{code}]: {message}"
            )

        if not terminal.connected:
            raise MT5ConnectionError("MT5: нет соединения с сервером брокера")

    @staticmethod
    def _validate_dates(start: datetime, end: datetime) -> tuple[datetime, datetime]:
        """Проверить диапазон и преобразовать даты в UTC."""
        if start.utcoffset() is None or end.utcoffset() is None:
            raise ValueError("MT5: даты должны содержать часовой пояс")

        if start >= end:
            raise ValueError("MT5: начало периода должно быть раньше окончания")

        return start.astimezone(UTC), end.astimezone(UTC)

    @staticmethod
    def _to_unix_us(value: datetime) -> int:
        """Преобразовать UTC datetime в микросекунды Unix."""
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        delta = value - epoch

        return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds

    @staticmethod
    def _ensure_symbol(symbol: str) -> None:
        """Проверить инструмент и его доступность."""
        if not symbol.strip():
            raise ValueError("MT5: название инструмента не может быть пустым")

        info = mt5.symbol_info(symbol)

        if info is None:
            code, message = mt5.last_error()
            raise ValueError(
                f"MT5: инструмент '{symbol}' недоступен [{code}]: {message}"
            )

        if not info.visible:
            if not mt5.symbol_select(symbol, True):
                code, message = mt5.last_error()
                raise MT5RequestError(
                    f"MT5: не удалось активировать '{symbol}' [{code}]: {message}"
                )

            logger.info("MT5: инструмент %s добавлен в Обзор рынка", symbol)

    def _prepare_request(
        self, symbol: str, start: datetime, end: datetime
    ) -> tuple[datetime, datetime]:
        """Проверить параметры и подготовить запрос."""
        start, end = self._validate_dates(start, end)
        self._check_connection()
        self._ensure_symbol(symbol)
        return start, end

    @staticmethod
    def _handle_result(data: ndarray | None, symbol: str, kind: str) -> ndarray:
        """Обработать результат запроса."""
        if data is None:
            code, message = mt5.last_error()
            logger.error(
                "MT5: ошибка получения %s %s [%s]: %s",
                kind,
                symbol,
                code,
                message,
            )
            raise MT5RequestError(
                f"MT5: ошибка получения {kind} {symbol} [{code}]: {message}"
            )

        if len(data) == 0:
            logger.warning("MT5: %s %s за указанный период не найдены", kind, symbol)
        else:
            logger.info("MT5: получено %d %s %s", len(data), kind, symbol)

        return data

    def get_ticks(self, symbol: str, start: datetime, end: datetime) -> ndarray:
        """Получить исторические тики за указанный период."""
        start, end = self._prepare_request(symbol, start, end)

        logger.info("MT5: запрос тиков %s: %s — %s", symbol, start, end)

        ticks = mt5.copy_ticks_range(symbol, start, end, mt5.COPY_TICKS_ALL)
        ticks = self._handle_result(ticks, symbol, "тиков")

        start_us = self._to_unix_us(start)
        end_us = self._to_unix_us(end)

        time_us = ticks["time_msc"] * 1000

        mask = (time_us >= start_us) & (time_us < end_us)
        return ticks[mask]

    def get_candles(
        self, symbol: str, timeframe: int, start: datetime, end: datetime
    ) -> ndarray:
        """Получить исторические свечи за указанный период."""
        if timeframe not in VALID_TIMEFRAMES:
            raise ValueError(f"MT5: неподдерживаемый таймфрейм {timeframe}")

        start, end = self._prepare_request(symbol, start, end)

        logger.info(
            "MT5: запрос свечей %s TF=%s: %s — %s",
            symbol,
            timeframe,
            start,
            end,
        )

        candles = mt5.copy_rates_range(symbol, timeframe, start, end)
        candles = self._handle_result(candles, symbol, "свечей")

        start_us = self._to_unix_us(start)
        end_us = self._to_unix_us(end)

        time_us = candles["time"] * 1_000_000

        mask = (time_us >= start_us) & (time_us < end_us)
        return candles[mask]

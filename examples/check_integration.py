"""Run integration checks against a real MT5 terminal and XAUUSD.s only.

Usage:
    uv run python examples/check_integration.py

The test uses a temporary cache and does not change the trading account.
"""
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import MetaTrader5 as mt5
import numpy as np

from mt5_datahub import MarketData
from mt5_datahub.providers.mt5 import MT5Provider

SYMBOL = "XAUUSD.s"
SOURCE = "integration-xauusd-s"
START = datetime(2026, 10, 20, 10, 0, tzinfo=UTC)
MIDDLE = START + timedelta(minutes=5)
END = START + timedelta(minutes=10)


class CountingMT5Provider(MT5Provider):
    def __init__(self) -> None:
        super().__init__()
        self.tick_calls = 0
        self.candle_calls = 0

    def iter_tick_pages(self, symbol, start, end, *, page_size=50_000):
        assert symbol == SYMBOL
        self.tick_calls += 1
        yield from super().iter_tick_pages(
            symbol, start, end, page_size=page_size,
        )

    def get_ticks(self, symbol, start, end):
        assert symbol == SYMBOL
        self.tick_calls += 1
        return super().get_ticks(symbol, start, end)

    def get_candles(self, symbol, timeframe, start, end):
        assert symbol == SYMBOL
        self.candle_calls += 1
        return super().get_candles(symbol, timeframe, start, end)


def main() -> None:
    with TemporaryDirectory(prefix="mt5-xauusd-test-") as directory:
        provider = CountingMT5Provider()
        with MarketData(
            source=SOURCE,
            cache_dir=Path(directory),
            provider=provider,
        ) as market:
            print("Проверка XAUUSD.s — реальные данные MT5")
            first = market.get_ticks(SYMBOL, START, MIDDLE)
            assert len(first), "Нет тиков в проверяемом интервале — проверь историю инструмента"
            count_after_first = provider.tick_calls
            again = market.get_ticks(SYMBOL, START, MIDDLE)
            assert provider.tick_calls == count_after_first, "Повторный запрос обращается к MT5"
            assert np.array_equal(first, again), "Повторный запрос изменил тики"

            full = market.get_ticks(SYMBOL, START, END)
            assert provider.tick_calls == count_after_first + 1, (
                "Ожидалась дозагрузка только соседнего пятиминутного интервала"
            )
            assert np.array_equal(first, full[: len(first)]), (
                "Дозагрузка изменила сохранённые тики"
            )
            count_after_full = provider.tick_calls
            full_cached = market.get_ticks(SYMBOL, START, END)
            assert provider.tick_calls == count_after_full, "Полный диапазон не взят из кэша"
            assert np.array_equal(full, full_cached)

            bars = market.get_candles(SYMBOL, mt5.TIMEFRAME_M1, START, END)
            assert len(bars), "Нет свечей M1 в проверяемом интервале"
            candle_calls = provider.candle_calls
            cached_bars = market.get_candles(
                SYMBOL, mt5.TIMEFRAME_M1, START, END
            )
            assert provider.candle_calls == candle_calls, "Свечи повторно скачиваются"
            assert np.array_equal(bars, cached_bars), "Свечи после кэша изменились"

            print(f"Первый интервал: {len(first)} тиков")
            print(f"Полный интервал: {len(full)} тиков")
            print(f"Свечей M1: {len(bars)}")
            print(f"Участков загрузки MT5: тики={provider.tick_calls}, свечи={provider.candle_calls}")
            print("PASS: кэш, повторное чтение, дозагрузка и свечи XAUUSD.s")


if __name__ == "__main__":
    main()

"""MT5 history limit: capped candle arrays must never mark gaps as complete."""
import tempfile
import unittest
from datetime import UTC, datetime, timedelta

import MetaTrader5 as mt5
import numpy as np

from mt5_datahub import MarketData
from mt5_datahub.arrays import CANDLE_DTYPE
from mt5_datahub.exceptions import MT5HistoryLimitError
from mt5_datahub.timeutils import to_us


class CappedProvider:
    def __init__(self, maxbars=3):
        self.maxbars = maxbars
        self.calls = []

    def connect(self):
        pass

    def get_candles(self, symbol, timeframe, start, end):
        self.calls.append((start, end))
        base = datetime(2026, 10, 20, 10, tzinfo=UTC)
        minutes = [base + timedelta(minutes=i) for i in range(8)]
        selected = [t for t in minutes if start <= t < end]
        if len(selected) >= self.maxbars:
            raise MT5HistoryLimitError("synthetic maxbars")
        bars = np.zeros(len(selected), dtype=CANDLE_DTYPE)
        bars["time"] = [int(t.timestamp()) for t in selected]
        return bars


class CandleHistoryTests(unittest.TestCase):
    def test_window_splits_and_preserves_all_bars(self):
        start = datetime(2026, 10, 20, 10, tzinfo=UTC)
        end = start + timedelta(minutes=8)
        with tempfile.TemporaryDirectory() as folder:
            provider = CappedProvider()
            api = MarketData(
                "mock", folder, provider=provider,
                candle_chunk=timedelta(minutes=8),
            )
            bars = api.get_candles("x", mt5.TIMEFRAME_M1, start, end)
            self.assertEqual(len(bars), 8)
            self.assertGreater(len(provider.calls), 4)
            self.assertEqual(api.cache.missing(
                "mock", "x", "candles", mt5.TIMEFRAME_M1,
                to_us(start), to_us(end),
            ), [])
            count = len(provider.calls)
            np.testing.assert_array_equal(
                bars, api.get_candles("x", mt5.TIMEFRAME_M1, start, end)
            )
            self.assertEqual(len(provider.calls), count)

    def test_irreducible_limit_does_not_cache_response(self):
        start = datetime(2026, 10, 20, 10, tzinfo=UTC)
        end = start + timedelta(minutes=1)
        with tempfile.TemporaryDirectory() as folder:
            provider = CappedProvider(maxbars=1)
            api = MarketData(
                "mock", folder, provider=provider,
                candle_chunk=timedelta(minutes=1),
            )
            with self.assertRaises(MT5HistoryLimitError):
                api.get_candles("x", mt5.TIMEFRAME_M1, start, end)
            self.assertEqual(
                api.cache.missing(
                    "mock", "x", "candles", mt5.TIMEFRAME_M1,
                    to_us(start), to_us(end),
                ),
                [(to_us(start), to_us(end))],
            )


if __name__ == "__main__":
    unittest.main()

"""Regression tests: open MT5 bars must never become cached coverage."""
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import MetaTrader5 as mt5
import numpy as np

from mt5_datahub import MarketData
from mt5_datahub.arrays import CANDLE_DTYPE
from mt5_datahub.timeutils import to_us


class CandleProvider:
    def __init__(self, opens):
        self.opens = opens
        self.requests = []

    def connect(self):
        pass

    def disconnect(self):
        pass

    def get_candles(self, symbol, timeframe, start, end):
        self.requests.append((start, end))
        selected = [x for x in self.opens if start <= x < end]
        rows = np.zeros(len(selected), dtype=CANDLE_DTYPE)
        rows["time"] = [int(x.timestamp()) for x in selected]
        rows["open"] = rows["high"] = rows["low"] = rows["close"] = 10
        return rows


def frozen_clock(moment):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return moment if tz is UTC else moment.astimezone(tz)
    return patch("mt5_datahub.services.market_data.datetime", Clock)


class CandleCoverageTests(unittest.TestCase):
    def check_rollover(self, timeframe, earlier, current, after):
        with tempfile.TemporaryDirectory() as tmp:
            provider = CandleProvider((earlier, current))
            data = MarketData("mock", tmp, provider=provider, candle_chunk=timedelta(days=62))
            with frozen_clock(current + timedelta(seconds=30)):
                first = data.get_candles("XAUUSD.s", timeframe, earlier, after)
                self.assertEqual(first["time"].tolist(), [int(earlier.timestamp())])
                self.assertEqual(len(provider.requests), 1)
                self.assertEqual(provider.requests[0][1], current)
            with frozen_clock(after + timedelta(seconds=30)):
                second = data.get_candles("XAUUSD.s", timeframe, earlier, after)
                self.assertEqual(second["time"].tolist(), [int(x.timestamp()) for x in (earlier, current)])
                self.assertEqual(len(provider.requests), 2)
                self.assertEqual(provider.requests[1], (current, after))
                cached = data.get_candles("XAUUSD.s", timeframe, earlier, after)
                np.testing.assert_array_equal(second, cached)
                self.assertEqual(len(provider.requests), 2)

    def test_m1(self):
        self.check_rollover(
            mt5.TIMEFRAME_M1,
            datetime(2026, 10, 21, 10, 0, tzinfo=UTC),
            datetime(2026, 10, 21, 10, 1, tzinfo=UTC),
            datetime(2026, 10, 21, 10, 2, tzinfo=UTC),
        )

    def test_week(self):
        self.check_rollover(
            mt5.TIMEFRAME_W1,
            datetime(2026, 10, 12, tzinfo=UTC),
            datetime(2026, 10, 19, tzinfo=UTC),
            datetime(2026, 10, 26, tzinfo=UTC),
        )

    def test_month(self):
        self.check_rollover(
            mt5.TIMEFRAME_MN1,
            datetime(2026, 9, 1, tzinfo=UTC),
            datetime(2026, 10, 1, tzinfo=UTC),
            datetime(2026, 11, 1, tzinfo=UTC),
        )

    def test_future_bars_do_not_query_provider(self):
        with tempfile.TemporaryDirectory() as tmp:
            now = datetime(2026, 10, 21, 10, 1, 30, tzinfo=UTC)
            provider = CandleProvider(())
            data = MarketData("mock", tmp, provider=provider)
            with frozen_clock(now):
                rows = data.get_candles(
                    "XAUUSD.s", mt5.TIMEFRAME_M1,
                    now.replace(second=0), now + timedelta(minutes=3)
                )
            self.assertEqual(len(rows), 0)
            self.assertEqual(provider.requests, [])


if __name__ == "__main__":
    unittest.main()

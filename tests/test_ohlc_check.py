"""Read-only M1 candle cross-check regression tests."""
import tempfile
import unittest
from datetime import UTC, datetime, timedelta

import MetaTrader5 as mt5
import numpy as np

from mt5_datahub.arrays import CANDLE_DTYPE, TICK_DTYPE
from mt5_datahub.ohlc_check import compare_tick_ohlc_to_m1
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import to_us


class CandleProvider:
    def __init__(self, bars):
        self.bars = bars
        self.requests = []

    def get_candles(self, symbol, timeframe, start, end):
        self.requests.append((start, end))
        if timeframe != mt5.TIMEFRAME_M1:
            raise AssertionError("Expected M1")
        left, right = int(start.timestamp()), int(end.timestamp())
        return self.bars[(self.bars["time"] >= left) & (self.bars["time"] < right)]


class TickToCandleTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 10, 20, 10, tzinfo=UTC)
        self.end = self.start + timedelta(minutes=2)

    def make_rows(self):
        ticks = np.zeros(4, dtype=TICK_DTYPE)
        stamps = [0, 15_000, 60_000, 75_000]
        ticks["time_msc"] = to_us(self.start) // 1000 + np.array(stamps)
        ticks["time"] = ticks["time_msc"] // 1000
        ticks["bid"] = [100, 102, 103, 101]

        bars = np.zeros(2, dtype=CANDLE_DTYPE)
        bars["time"] = [
            int(self.start.timestamp()),
            int((self.start + timedelta(minutes=1)).timestamp()),
        ]
        for column, values in (
            ("open", [100, 103]), ("high", [102, 103]),
            ("low", [100, 101]), ("close", [102, 101]),
        ):
            bars[column] = values
        return ticks, bars

    def test_exact_match_in_windowed_read(self):
        ticks, bars = self.make_rows()
        with tempfile.TemporaryDirectory() as directory:
            storage = ParquetCache(directory)
            storage.save("broker", "x", "ticks", 0, ticks,
                         to_us(self.start), to_us(self.end))
            provider = CandleProvider(bars)
            result = compare_tick_ohlc_to_m1(
                storage, "broker", "x", self.start, self.end,
                provider, window=timedelta(minutes=1),
            )
            self.assertTrue(result.consistent)
            self.assertEqual(result.checked_minutes, 2)
            self.assertEqual(result.matching_minutes, 2)
            self.assertEqual(len(provider.requests), 2)

    def test_changed_high_is_detected(self):
        ticks, bars = self.make_rows()
        bars["high"][0] = 104
        with tempfile.TemporaryDirectory() as directory:
            storage = ParquetCache(directory)
            storage.save("broker", "x", "ticks", 0, ticks,
                         to_us(self.start), to_us(self.end))
            result = compare_tick_ohlc_to_m1(
                storage, "broker", "x", self.start, self.end,
                CandleProvider(bars),
            )
            self.assertFalse(result.consistent)
            self.assertEqual(result.issue_count, 1)
            self.assertEqual(result.issues[0].reason, "ohlc-differs")

            tolerated = compare_tick_ohlc_to_m1(
                storage, "broker", "x", self.start, self.end,
                CandleProvider(bars), tolerance=2.0,
            )
            self.assertTrue(tolerated.consistent)

    def test_uncovered_cache_is_not_marked_consistent(self):
        ticks, bars = self.make_rows()
        with tempfile.TemporaryDirectory() as directory:
            storage = ParquetCache(directory)
            middle = self.start + timedelta(minutes=1)
            storage.save("broker", "x", "ticks", 0, ticks[:2],
                         to_us(self.start), to_us(middle))
            result = compare_tick_ohlc_to_m1(
                storage, "broker", "x", self.start, self.end,
                CandleProvider(bars), window=timedelta(minutes=1),
            )
            self.assertFalse(result.consistent)
            self.assertEqual(result.checked_minutes, 1)
            self.assertEqual(result.issue_count, 1)
            self.assertEqual(result.issues[0].reason, "uncovered-tick-cache-window")

    def test_tick_minute_with_no_bar_is_reported(self):
        ticks, bars = self.make_rows()
        with tempfile.TemporaryDirectory() as directory:
            storage = ParquetCache(directory)
            storage.save("broker", "x", "ticks", 0, ticks,
                         to_us(self.start), to_us(self.end))
            result = compare_tick_ohlc_to_m1(
                storage, "broker", "x", self.start, self.end,
                CandleProvider(bars[:1]),
            )
            self.assertFalse(result.consistent)
            self.assertEqual(result.issues[0].reason, "cached-ticks-without-broker-bar")

    def test_non_aligned_time_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            _, bars = self.make_rows()
            with self.assertRaises(ValueError):
                compare_tick_ohlc_to_m1(
                    directory, "b", "x", self.start + timedelta(seconds=1),
                    self.end, CandleProvider(bars),
                )


if __name__ == "__main__":
    unittest.main()

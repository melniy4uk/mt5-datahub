"""Regression: actual MT5 rates can have unsigned volume dtypes."""
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import MetaTrader5 as mt5
import numpy as np

from mt5_datahub.arrays import CANDLE_DTYPE, TICK_DTYPE, canonical_candles
from mt5_datahub.exceptions import MT5RequestError
from mt5_datahub.ohlc_check import compare_tick_ohlc_to_m1
from mt5_datahub.providers.mt5 import MT5Provider
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import to_us

NATIVE_MT5_CANDLES = np.dtype([
    ("time", "<i8"), ("open", "<f8"), ("high", "<f8"),
    ("low", "<f8"), ("close", "<f8"), ("tick_volume", "<u8"),
    ("spread", "<i4"), ("real_volume", "<u8"),
])


def native_bars(start):
    bars = np.zeros(2, dtype=NATIVE_MT5_CANDLES)
    bars["time"] = [
        int(start.timestamp()), int(start.timestamp()) + 60,
    ]
    bars["open"] = [100, 103]
    bars["high"] = [102, 103]
    bars["low"] = [100, 101]
    bars["close"] = [102, 101]
    bars["tick_volume"] = [2, 2]
    return bars


class UnsignedVolumeReader:
    def __init__(self, bars):
        self.bars = bars

    def get_candles(self, symbol, timeframe, start, end):
        if timeframe != mt5.TIMEFRAME_M1:
            raise AssertionError("Must request M1 bars")
        return self.bars


class CandleDTypeCompatibilityTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 10, 20, 10, tzinfo=UTC)
        self.end = self.start + timedelta(minutes=2)

    def test_bid_ohlc_check_accepts_native_unsigned_volume_fields(self):
        ticks = np.zeros(4, dtype=TICK_DTYPE)
        ticks["time_msc"] = to_us(self.start) // 1000 + np.array(
            [0, 15_000, 60_000, 75_000]
        )
        ticks["time"] = ticks["time_msc"] // 1000
        ticks["bid"] = [100, 102, 103, 101]
        with tempfile.TemporaryDirectory() as tmp:
            cache = ParquetCache(tmp)
            cache.save("broker", "XAUUSD.s", "ticks", 0, ticks,
                       to_us(self.start), to_us(self.end))
            report = compare_tick_ohlc_to_m1(
                cache, "broker", "XAUUSD.s", self.start, self.end,
                UnsignedVolumeReader(native_bars(self.start)),
            )
            self.assertEqual(report.checked_minutes, 2)
            self.assertEqual(report.issue_count, 0)
            self.assertTrue(report.consistent)

    def test_provider_normalizes_native_unsigned_candles(self):
        bars = native_bars(self.start)
        with patch("mt5_datahub.providers.mt5.mt5") as fake:
            fake.initialize.return_value = True
            fake.terminal_info.return_value = SimpleNamespace(
                connected=True, maxbars=10000,
            )
            fake.symbol_info.return_value = SimpleNamespace(visible=True)
            fake.copy_rates_range.return_value = bars
            with MT5Provider() as provider:
                result = provider.get_candles(
                    "XAUUSD.s", mt5.TIMEFRAME_M1, self.start, self.end,
                )
        self.assertEqual(result.dtype, CANDLE_DTYPE)
        np.testing.assert_array_equal(result["time"], bars["time"])
        np.testing.assert_array_equal(result["tick_volume"], bars["tick_volume"])
        with tempfile.TemporaryDirectory() as tmp:
            cache = ParquetCache(tmp)
            cache.save("broker", "XAUUSD.s", "candles", mt5.TIMEFRAME_M1,
                       result, to_us(self.start), to_us(self.end))
            self.assertEqual(cache.verify_segments(
                "broker", "XAUUSD.s", "candles", mt5.TIMEFRAME_M1,
                to_us(self.start), to_us(self.end),
            ), {})

    def test_overflow_cannot_silently_wrap_volume(self):
        bars = native_bars(self.start)
        bars["real_volume"][0] = 2**63
        with self.assertRaisesRegex(ValueError, "real_volume"):
            canonical_candles(bars)

    def test_missing_price_column_is_rejected(self):
        invalid = np.zeros(1, dtype=[
            ("time", "<i8"), ("open", "<f8"),
            ("high", "<f8"), ("close", "<f8"),
        ])
        with tempfile.TemporaryDirectory() as tmp:
            cache = ParquetCache(tmp)
            ticks = np.zeros(1, dtype=TICK_DTYPE)
            ticks["time_msc"][0] = to_us(self.start) // 1000
            cache.save(
                "broker", "XAUUSD.s", "ticks", 0, ticks,
                to_us(self.start), to_us(self.end),
            )
            with self.assertRaisesRegex(MT5RequestError, "required"):
                compare_tick_ohlc_to_m1(
                    cache, "broker", "XAUUSD.s", self.start, self.end,
                    UnsignedVolumeReader(invalid),
                )


if __name__ == "__main__":
    unittest.main()

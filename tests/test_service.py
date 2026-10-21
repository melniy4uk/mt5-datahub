"""Offline integration tests for MarketData cache/recovery behavior."""
import tempfile
import unittest
from datetime import UTC, datetime, timedelta

import numpy as np

from mt5_datahub import MarketData
from mt5_datahub.arrays import TICK_DTYPE
from mt5_datahub.timeutils import to_us


class FakeProvider:
    def __init__(self, fail_at=None):
        self.requests = []
        self.fail_at = fail_at
        self.connected = False

    def connect(self):
        self.connected = True

    def disconnect(self):
        self.connected = False

    def get_ticks(self, symbol, start, end):
        self.requests.append((symbol, start, end))
        if len(self.requests) == self.fail_at:
            raise RuntimeError("simulated interruption")
        result = np.zeros(2, dtype=TICK_DTYPE)
        result["time_msc"] = [to_us(start) // 1000, to_us(end) // 1000 - 1]
        result["time"] = result["time_msc"] // 1000
        result["bid"] = [100, 101]
        return result


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 10, 19, 12, tzinfo=UTC)
        self.mid = self.start + timedelta(hours=1)
        self.end = self.start + timedelta(hours=3)

    def test_cache_hit_and_extension(self):
        with tempfile.TemporaryDirectory() as temp:
            fake = FakeProvider()
            with MarketData("fake", temp, provider=fake,
                            tick_chunk=timedelta(hours=1)) as data:
                first = data.get_ticks("XAUUSD.s", self.start, self.mid)
                self.assertEqual(len(fake.requests), 1)
                again = data.get_ticks("XAUUSD.s", self.start, self.mid)
                self.assertEqual(len(fake.requests), 1)
                np.testing.assert_array_equal(first, again)

                full = data.get_ticks("XAUUSD.s", self.start, self.end)
                self.assertEqual(len(fake.requests), 3)
                self.assertEqual(len(full), 6)
                np.testing.assert_array_equal(first, full[:2])

    def test_interruption_and_resume(self):
        with tempfile.TemporaryDirectory() as temp:
            failing = FakeProvider(fail_at=2)
            with MarketData("fake", temp, provider=failing,
                            tick_chunk=timedelta(hours=1)) as data:
                with self.assertRaisesRegex(RuntimeError, "simulated interruption"):
                    data.get_ticks("XAUUSD.s", self.start, self.end)
            self.assertEqual(len(failing.requests), 2)
            recovered = FakeProvider()
            with MarketData("fake", temp, provider=recovered,
                            tick_chunk=timedelta(hours=1)) as data:
                result = data.get_ticks("XAUUSD.s", self.start, self.end)
                self.assertEqual(len(result), 6)
                self.assertEqual(len(recovered.requests), 2)
                again = data.get_ticks("XAUUSD.s", self.start, self.end)
                np.testing.assert_array_equal(again, result)
                self.assertEqual(len(recovered.requests), 2)

    def test_source_isolation(self):
        with tempfile.TemporaryDirectory() as temp:
            one = FakeProvider()
            two = FakeProvider()
            MarketData("broker-one", temp, provider=one).get_ticks(
                "XAUUSD.s", self.start, self.mid)
            MarketData("broker-two", temp, provider=two).get_ticks(
                "XAUUSD.s", self.start, self.mid)
            self.assertEqual(len(one.requests), 1)
            self.assertEqual(len(two.requests), 1)


if __name__ == "__main__":
    unittest.main()

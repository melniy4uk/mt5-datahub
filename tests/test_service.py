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

    def test_iter_ticks_matches_full_and_uses_cache(self):
        with tempfile.TemporaryDirectory() as temp:
            fake = FakeProvider()
            with MarketData("fake", temp, provider=fake,
                            tick_chunk=timedelta(hours=1)) as data:
                iterator = data.iter_ticks("XAUUSD.s", self.start, self.end,
                                           columns=("time_msc", "bid"), batch_size=1)
                self.assertEqual(len(fake.requests), 0)  # truly lazy
                batches = list(iterator)
                self.assertEqual(len(fake.requests), 3)
                self.assertEqual(sum(map(len, batches)), 6)
                self.assertTrue(all(len(batch) <= 1 for batch in batches))
                self.assertTrue(all(batch.dtype.names == ("time_msc", "bid")
                                    for batch in batches))
                expected = data.get_ticks("XAUUSD.s", self.start, self.end)
                np.testing.assert_array_equal(
                    np.concatenate(batches)["time_msc"], expected["time_msc"]
                )
                self.assertEqual(len(fake.requests), 3)  # no network on cache hit

    def test_iter_ticks_resume_and_validation(self):
        with tempfile.TemporaryDirectory() as temp:
            failing = FakeProvider(fail_at=2)
            with MarketData("fake", temp, provider=failing,
                            tick_chunk=timedelta(hours=1)) as data:
                with self.assertRaisesRegex(RuntimeError, "simulated interruption"):
                    list(data.iter_ticks("XAUUSD.s", self.start, self.end))
            recovered = FakeProvider()
            with MarketData("fake", temp, provider=recovered,
                            tick_chunk=timedelta(hours=1)) as data:
                result = list(data.iter_ticks("XAUUSD.s", self.start, self.end,
                                              batch_size=2))
                self.assertEqual(sum(len(part) for part in result), 6)
                self.assertEqual(len(recovered.requests), 2)
                with self.assertRaises(ValueError):
                    list(data.iter_ticks("XAUUSD.s", self.start, self.end,
                                         batch_size=0))
                with self.assertRaises(ValueError):
                    list(data.iter_ticks("XAUUSD.s", self.start, self.end,
                                         columns=("nonexistent",)))
                self.assertEqual(len(recovered.requests), 2)

    def test_iter_ticks_empty_future_range(self):
        future = datetime.now(UTC) + timedelta(days=1)
        with tempfile.TemporaryDirectory() as temp:
            fake = FakeProvider()
            data = MarketData("fake", temp, provider=fake)
            self.assertEqual(list(data.iter_ticks("XAUUSD.s", future,
                                                   future + timedelta(hours=1))), [])
            self.assertEqual(len(fake.requests), 0)


    def test_stream_ticks_returns_first_batch_before_next_download(self):
        with tempfile.TemporaryDirectory() as temp:
            fake = FakeProvider()
            data = MarketData("fake", temp, provider=fake,
                              tick_chunk=timedelta(hours=1))
            iterator = data.stream_ticks(
                "XAUUSD.s", self.start, self.end,
                columns=("time_msc", "bid"), batch_size=1,
            )
            self.assertEqual(len(fake.requests), 0)
            first = next(iterator)
            self.assertEqual(len(first), 1)
            self.assertEqual(first.dtype.names, ("time_msc", "bid"))
            self.assertEqual(len(fake.requests), 1)
            iterator.close()
            self.assertEqual(len(fake.requests), 1)

            # The first chunk was saved before the iterator stopped.
            resumed = list(data.stream_ticks(
                "XAUUSD.s", self.start, self.end,
                columns=("time_msc", "bid"), batch_size=1,
            ))
            self.assertEqual(len(fake.requests), 3)
            self.assertEqual(sum(len(batch) for batch in resumed), 6)
            expected = data.get_ticks("XAUUSD.s", self.start, self.end)
            np.testing.assert_array_equal(
                np.concatenate(resumed)["time_msc"], expected["time_msc"],
            )

    def test_stream_ticks_failure_preserves_previous_chunk(self):
        with tempfile.TemporaryDirectory() as temp:
            failing = FakeProvider(fail_at=2)
            data = MarketData("fake", temp, provider=failing,
                              tick_chunk=timedelta(hours=1))
            iterator = data.stream_ticks("XAUUSD.s", self.start, self.end)
            self.assertEqual(len(next(iterator)), 2)
            with self.assertRaisesRegex(RuntimeError, "simulated interruption"):
                list(iterator)
            self.assertEqual(len(failing.requests), 2)

            recovered = FakeProvider()
            data = MarketData("fake", temp, provider=recovered,
                              tick_chunk=timedelta(hours=1))
            parts = list(data.stream_ticks("XAUUSD.s", self.start, self.end))
            self.assertEqual(len(recovered.requests), 2)
            self.assertEqual(sum(len(part) for part in parts), 6)
            self.assertEqual(len(list(data.stream_ticks(
                "XAUUSD.s", self.start, self.end,
            ))), 3)
            self.assertEqual(len(recovered.requests), 2)

    def test_stream_ticks_validation_and_future(self):
        with tempfile.TemporaryDirectory() as temp:
            fake = FakeProvider()
            data = MarketData("fake", temp, provider=fake)
            with self.assertRaises(ValueError):
                list(data.stream_ticks("XAUUSD.s", self.start, self.end,
                                       batch_size=0))
            with self.assertRaises(ValueError):
                list(data.stream_ticks("XAUUSD.s", self.start, self.end,
                                       columns=("unknown",)))
            future = datetime.now(UTC) + timedelta(days=1)
            self.assertEqual(list(data.stream_ticks(
                "XAUUSD.s", future, future + timedelta(hours=1)
            )), [])
            self.assertEqual(len(fake.requests), 0)


if __name__ == "__main__":
    unittest.main()

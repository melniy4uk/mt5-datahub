"""Adaptive MT5 request splitting without losing equal-millisecond ticks."""
import tempfile
import unittest
from datetime import UTC, datetime, timedelta

import numpy as np

from mt5_datahub import MarketData
from mt5_datahub.arrays import TICK_DTYPE


class DenseProvider:
    def __init__(self):
        self.requests = []

    def connect(self):
        pass

    def get_ticks(self, symbol, start, end):
        self.requests.append((start, end))
        n = int((end - start).total_seconds())
        ticks = np.zeros(n, dtype=TICK_DTYPE)
        ticks["time_msc"] = int(start.timestamp() * 1000) + np.arange(n) * 1000
        ticks["time"] = ticks["time_msc"] // 1000
        ticks["bid"] = np.arange(n) + 100
        return ticks


class AdaptiveChunkTests(unittest.TestCase):
    def test_large_response_retried_as_smaller_segments(self):
        with tempfile.TemporaryDirectory() as temp:
            start = datetime(2026, 10, 20, 10, tzinfo=UTC)
            end = start + timedelta(seconds=16)
            provider = DenseProvider()
            data = MarketData(
                "fake", temp, provider=provider,
                tick_chunk=timedelta(seconds=16), max_tick_rows=3,
            )
            stream = list(data.stream_ticks("x", start, end, batch_size=3))
            self.assertEqual(len(np.concatenate(stream)), 16)
            self.assertTrue(all(len(batch) <= 3 for batch in stream))
            self.assertGreater(len(provider.requests), 8)
            previous_calls = len(provider.requests)
            full = data.get_ticks("x", start, end)
            self.assertEqual(len(full), 16)
            self.assertEqual(len(provider.requests), previous_calls)
            self.assertEqual(
                full["time_msc"].tolist(),
                [int(start.timestamp() * 1000) + i * 1000 for i in range(16)],
            )

    def test_invalid_limit(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(ValueError):
                MarketData("fake", temp, max_tick_rows=0)


if __name__ == "__main__":
    unittest.main()

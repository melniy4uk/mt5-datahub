"""Offline regression checks for operational logging without account secrets."""
import tempfile
import unittest
from datetime import UTC, datetime, timedelta

import numpy as np

from mt5_datahub import MarketData
from mt5_datahub.arrays import TICK_DTYPE


class StubProvider:
    def __init__(self):
        self.calls = 0

    def connect(self):
        pass

    def get_ticks(self, symbol, start, end):
        self.calls += 1
        rows = np.zeros(1, dtype=TICK_DTYPE)
        rows["time_msc"] = int(start.timestamp() * 1_000)
        return rows


class LogTests(unittest.TestCase):
    def test_miss_save_then_hit(self):
        with tempfile.TemporaryDirectory() as directory:
            provider = StubProvider()
            api = MarketData("test-broker", directory, provider=provider)
            start = datetime(2026, 10, 20, 10, tzinfo=UTC)
            end = start + timedelta(minutes=5)
            with self.assertLogs("mt5_datahub", level="DEBUG") as logs:
                self.assertEqual(len(api.get_ticks("XAUUSD", start, end)), 1)
                self.assertEqual(len(api.get_ticks("XAUUSD", start, end)), 1)
            events = "\n".join(logs.output)
            self.assertIn("cache_miss source=test-broker", events)
            self.assertIn("cache_saved source=test-broker", events)
            self.assertIn("cache_hit source=test-broker", events)
            self.assertEqual(provider.calls, 1)


if __name__ == "__main__":
    unittest.main()

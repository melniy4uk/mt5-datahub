"""Offline public diagnostics contract; no active MT5 terminal required."""
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np

from mt5_datahub.arrays import TICK_DTYPE
from mt5_datahub.diagnostics import inspect_cache
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import to_us


class CacheDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 10, 20, 10, tzinfo=UTC)
        self.end = self.start + timedelta(minutes=2)

    def test_unrequested_saved_and_broken(self):
        with tempfile.TemporaryDirectory() as temp:
            cache = ParquetCache(temp)
            before = inspect_cache(cache, "broker", "XAUUSD", self.start, self.end)
            self.assertFalse(before.saved_request_coverage)
            self.assertEqual(before.unrequested, ((self.start, self.end),))
            self.assertTrue(before.files_readable)
            rows = np.zeros(1, dtype=TICK_DTYPE)
            rows["time_msc"] = to_us(self.start) // 1000
            cache.save("broker", "XAUUSD", "ticks", 0, rows,
                       to_us(self.start), to_us(self.end))
            saved = inspect_cache(temp, "broker", "XAUUSD", self.start, self.end)
            self.assertTrue(saved.saved_request_coverage)
            self.assertTrue(saved.files_readable)
            filename = cache._segments_for(
                "broker", "XAUUSD", "ticks", 0,
                to_us(self.start), to_us(self.end),
            )[0][0]
            (Path(temp) / filename).write_bytes(b"invalid parquet")
            broken = inspect_cache(cache, "broker", "XAUUSD", self.start, self.end)
            self.assertTrue(broken.saved_request_coverage)
            self.assertFalse(broken.files_readable)
            self.assertEqual(len(broken.corrupt_segments), 1)

    def test_validation(self):
        with tempfile.TemporaryDirectory() as temp:
            for arguments in (
                {"kind": "unknown"}, {"kind": "ticks", "timeframe": 1},
                {"kind": "candles", "timeframe": -1},
            ):
                with self.assertRaises(ValueError):
                    inspect_cache(temp, "s", "x", self.start, self.end, **arguments)


if __name__ == "__main__":
    unittest.main()

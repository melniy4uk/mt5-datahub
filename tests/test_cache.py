import tempfile
import unittest
from datetime import UTC, datetime

import numpy as np

from mt5_datahub.arrays import TICK_DTYPE, select_range
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import from_us, to_us, validate_range


class CacheTests(unittest.TestCase):
    def test_roundtrip(self):
        value = datetime(2026, 10, 20, 10, 5, 0, 123456, tzinfo=UTC)
        self.assertEqual(from_us(to_us(value)), value)

    def test_naive(self):
        with self.assertRaises(ValueError):
            validate_range(datetime(2026, 1, 1), datetime(2026, 1, 2))

    def test_half_open(self):
        rows = np.zeros(3, dtype=TICK_DTYPE)
        rows["time_msc"] = [1000, 2000, 3000]
        self.assertEqual(select_range(rows, "ticks", 1_000_000, 3_000_000)["time_msc"].tolist(), [1000, 2000])

    def test_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = ParquetCache(directory)
            rows = np.zeros(3, dtype=TICK_DTYPE)
            rows["time_msc"] = [1000, 2000, 3000]
            cache.save("broker", "XAUUSD", "ticks", 0, rows, 1_000_000, 4_000_000)
            self.assertEqual(cache.missing("broker", "XAUUSD", "ticks", 0, 0, 5_000_000),
                             [(0, 1_000_000), (4_000_000, 5_000_000)])
            self.assertEqual(cache.read("broker", "XAUUSD", "ticks", 0, 1_000_000, 3_000_000)["time_msc"].tolist(), [1000, 2000])


if __name__ == "__main__":
    unittest.main()

"""Offline verification of damaged Parquet segments and safe invalidation."""
import tempfile
import unittest
from pathlib import Path

import numpy as np

from mt5_datahub.arrays import TICK_DTYPE
from mt5_datahub.exceptions import StorageError
from mt5_datahub.storage.parquet import ParquetCache


class CorruptCacheTests(unittest.TestCase):
    def test_verify_clean_and_invalidate_damaged(self):
        with tempfile.TemporaryDirectory() as temp:
            cache = ParquetCache(temp)
            rows = np.zeros(2, dtype=TICK_DTYPE)
            rows["time_msc"] = [1000, 1500]
            cache.save("a", "XAUUSD.s", "ticks", 0, rows, 1_000_000, 2_000_000)
            self.assertEqual(cache.verify_segments("a", "XAUUSD.s", "ticks", 0,
                                                   1_000_000, 2_000_000), {})
            filename = cache._segments_for("a", "XAUUSD.s", "ticks", 0,
                                           1_000_000, 2_000_000)[0][0]
            file = Path(temp) / filename
            file.write_bytes(b"corrupted parquet file")
            issues = cache.verify_segments("a", "XAUUSD.s", "ticks", 0,
                                           1_000_000, 2_000_000)
            self.assertEqual(list(issues), [filename])
            with self.assertRaises(StorageError):
                cache.invalidate_corrupt("a", "XAUUSD.s", "ticks", 0,
                                         1_000_000, 2_000_000)
            self.assertEqual(cache.missing("a", "XAUUSD.s", "ticks", 0,
                                           1_000_000, 2_000_000), [])
            cache.invalidate_corrupt("a", "XAUUSD.s", "ticks", 0,
                                     1_000_000, 2_000_000, confirm=True)
            self.assertTrue(file.exists())
            self.assertEqual(cache.missing("a", "XAUUSD.s", "ticks", 0,
                                           1_000_000, 2_000_000),
                             [(1_000_000, 2_000_000)])
            cache.save("a", "XAUUSD.s", "ticks", 0, rows, 1_000_000, 2_000_000)
            self.assertEqual(len(cache.read("a", "XAUUSD.s", "ticks", 0,
                                            1_000_000, 2_000_000)), 2)


if __name__ == "__main__":
    unittest.main()

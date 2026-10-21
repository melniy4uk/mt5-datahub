"""Offline verification of damaged Parquet segments and safe invalidation."""
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

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

    def test_verify_detects_unsorted_outside_range_and_wrong_type(self):
        with tempfile.TemporaryDirectory() as temp:
            cache = ParquetCache(temp)
            rows = np.zeros(3, dtype=TICK_DTYPE)
            rows["time_msc"] = [1000, 1500, 1800]
            cache.save("a", "x", "ticks", 0, rows, 1_000_000, 2_000_000)
            path = Path(temp) / cache._segments_for("a", "x", "ticks", 0,
                                                       1_000_000, 2_000_000)[0][0]
            original = pq.read_table(path)
            changed = original.set_column(
                original.schema.get_field_index("time_msc"),
                "time_msc", pa.array([1800, 1000, 1500], type=pa.int64()),
            )
            pq.write_table(changed, path)
            issues = cache.verify_segments("a", "x", "ticks", 0, 1_000_000, 2_000_000)
            self.assertEqual(len(issues), 1)
            self.assertIn("chronological", next(iter(issues.values())))

            changed = original.set_column(
                original.schema.get_field_index("time_msc"),
                "time_msc", pa.array([1000, 1500, 2200], type=pa.int64()),
            )
            pq.write_table(changed, path)
            issues = cache.verify_segments("a", "x", "ticks", 0, 1_000_000, 2_000_000)
            self.assertIn("interval", next(iter(issues.values())))

            changed = original.set_column(
                original.schema.get_field_index("time_msc"),
                "time_msc", pa.array([1000.0, 1500.0, 1800.0]),
            )
            pq.write_table(changed, path)
            issues = cache.verify_segments("a", "x", "ticks", 0, 1_000_000, 2_000_000)
            self.assertIn("dtype", next(iter(issues.values())))

    def test_save_sorts_time_and_preserves_distinct_same_millisecond(self):
        with tempfile.TemporaryDirectory() as temp:
            cache = ParquetCache(temp)
            rows = np.zeros(3, dtype=TICK_DTYPE)
            rows["time_msc"] = [1500, 1000, 1000]
            rows["bid"] = [3, 1, 2]
            cache.save("a", "x", "ticks", 0, rows, 1_000_000, 2_000_000)
            self.assertEqual(
                cache.read("a", "x", "ticks", 0, 1_000_000, 2_000_000)["bid"].tolist(),
                [1, 2, 3],
            )
            self.assertEqual(
                cache.verify_segments("a", "x", "ticks", 0, 1_000_000, 2_000_000), {},
            )


if __name__ == "__main__":
    unittest.main()

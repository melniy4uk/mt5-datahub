"""Tests for streamed/projection reads and interval-safe compaction."""
import tempfile
import unittest
from pathlib import Path

import numpy as np

from mt5_datahub.arrays import TICK_DTYPE
from mt5_datahub.exceptions import StorageError
from mt5_datahub.storage.parquet import ParquetCache


class OptimizedCacheTests(unittest.TestCase):
    def rows(self, stamps, bids):
        rows = np.zeros(len(stamps), dtype=TICK_DTYPE)
        rows["time_msc"] = stamps
        rows["bid"] = bids
        return rows

    def test_projection_batches_and_compaction(self):
        with tempfile.TemporaryDirectory() as path:
            cache = ParquetCache(path)
            cache.save("broker", "XAUUSD.s", "ticks", 0,
                       self.rows([1000, 1000, 2000], [1, 2, 3]), 1_000_000, 3_000_000)
            cache.save("broker", "XAUUSD.s", "ticks", 0,
                       self.rows([3000, 4000], [4, 5]), 3_000_000, 5_000_000)
            before = cache.read("broker", "XAUUSD.s", "ticks", 0, 1_000_000, 5_000_000)
            batches = list(cache.iter_batches(
                "broker", "XAUUSD.s", "ticks", 0, 1_000_000, 5_000_000,
                columns=("time_msc", "bid"), batch_size=2,
            ))
            self.assertGreaterEqual(len(batches), 3)
            self.assertEqual(sum(len(batch) for batch in batches), 5)
            self.assertEqual(batches[0].dtype.names, ("time_msc", "bid"))
            self.assertEqual(
                cache.read("broker", "XAUUSD.s", "ticks", 0, 1_000_000, 5_000_000,
                           columns=("time_msc", "bid"))["bid"].tolist(),
                [1, 2, 3, 4, 5],
            )
            self.assertEqual(cache.compact("broker", "XAUUSD.s", "ticks", 0,
                                           1_000_000, 5_000_000), 1)
            after = cache.read("broker", "XAUUSD.s", "ticks", 0, 1_000_000, 5_000_000)
            np.testing.assert_array_equal(before, after)
            self.assertEqual(len(list((Path(path) / "segments").glob("*.parquet"))), 1)
            self.assertEqual(cache.missing("broker", "XAUUSD.s", "ticks", 0,
                                           1_000_000, 5_000_000), [])

    def test_compaction_rejects_missing_interval(self):
        with tempfile.TemporaryDirectory() as path:
            cache = ParquetCache(path)
            cache.save("broker", "XAUUSD.s", "ticks", 0,
                       self.rows([1000], [1]), 1_000_000, 2_000_000)
            cache.save("broker", "XAUUSD.s", "ticks", 0,
                       self.rows([4000], [2]), 4_000_000, 5_000_000)
            with self.assertRaises(StorageError):
                cache.compact("broker", "XAUUSD.s", "ticks", 0, 1_000_000, 5_000_000)
            self.assertEqual(cache.missing("broker", "XAUUSD.s", "ticks", 0,
                                           1_000_000, 5_000_000),
                             [(2_000_000, 4_000_000)])

    def test_bad_columns_and_batch_size(self):
        with tempfile.TemporaryDirectory() as path:
            cache = ParquetCache(path)
            with self.assertRaises(ValueError):
                list(cache.iter_batches("b", "XAUUSD.s", "ticks", 0,
                                        0, 10, columns=("unknown",)))
            with self.assertRaises(ValueError):
                list(cache.iter_batches("b", "XAUUSD.s", "ticks", 0,
                                        0, 10, batch_size=0))


if __name__ == "__main__":
    unittest.main()

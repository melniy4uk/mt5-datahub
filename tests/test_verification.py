"""Offline cache-vs-provider comparison: coverage, changes and duplicate ticks."""
import tempfile
import unittest
from datetime import UTC, datetime, timedelta

import numpy as np

from mt5_datahub.arrays import TICK_DTYPE
from mt5_datahub.exceptions import StorageError
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import to_us
from mt5_datahub.verification import compare_cached_ticks


class StubProvider:
    def __init__(self, rows, identity=None):
        self.rows = rows
        self.identity = identity
        self.calls = []

    def get_ticks(self, symbol, start, end):
        self.calls.append((start, end))
        begin, finish = to_us(start) // 1000, to_us(end) // 1000
        data = self.rows
        return data[(data["time_msc"] >= begin) & (data["time_msc"] < finish)]

    def source_identity(self):
        return self.identity


class HistoryVerificationTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 10, 20, 10, tzinfo=UTC)
        self.mid = self.start + timedelta(minutes=1)
        self.end = self.start + timedelta(minutes=2)

    def rows(self):
        data = np.zeros(4, dtype=TICK_DTYPE)
        data["time_msc"] = [
            to_us(self.start) // 1000,
            to_us(self.start) // 1000,
            to_us(self.mid) // 1000,
            (to_us(self.mid) // 1000) + 1000,
        ]
        data["time"] = data["time_msc"] // 1000
        data["bid"] = [1, 2, 3, 4]
        return data

    def test_exact_match_with_same_ms_in_different_order(self):
        with tempfile.TemporaryDirectory() as folder:
            cache = ParquetCache(folder)
            data = self.rows()
            cache.save("history", "x", "ticks", 0, data,
                       to_us(self.start), to_us(self.end))
            fresh = data[[1, 0, 3, 2]].copy()
            provider = StubProvider(fresh)
            result = compare_cached_ticks(
                cache, "history", "x", self.start, self.end,
                provider, window=timedelta(minutes=1),
            )
            self.assertTrue(result.matches)
            self.assertEqual(result.matched_windows, 2)
            self.assertEqual(result.compared_ticks, 4)
            self.assertEqual(result.source_binding, "legacy-unbound")
            self.assertEqual(len(provider.calls), 2)

    def test_changed_content_same_count_is_detected(self):
        with tempfile.TemporaryDirectory() as folder:
            cache = ParquetCache(folder)
            data = self.rows()
            cache.save("history", "x", "ticks", 0, data,
                       to_us(self.start), to_us(self.end))
            fresh = data.copy()
            fresh["bid"][0] = 99
            result = compare_cached_ticks(
                cache, "history", "x", self.start, self.end,
                StubProvider(fresh), window=timedelta(minutes=1),
            )
            self.assertFalse(result.matches)
            self.assertEqual(result.issue_count, 1)
            self.assertEqual(result.differences[0].reason, "different-tick-records")

    def test_uncovered_window_is_not_called_a_match(self):
        with tempfile.TemporaryDirectory() as folder:
            cache = ParquetCache(folder)
            data = self.rows()[:2]
            cache.save("history", "x", "ticks", 0, data,
                       to_us(self.start), to_us(self.mid))
            provider = StubProvider(data)
            result = compare_cached_ticks(
                cache, "history", "x", self.start, self.end,
                provider, window=timedelta(minutes=1),
            )
            self.assertFalse(result.matches)
            self.assertEqual(result.matched_windows, 1)
            self.assertEqual(result.differences[0].reason, "uncovered-cache-range")
            self.assertEqual(len(provider.calls), 1)

    def test_account_binding_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            cache = ParquetCache(folder)
            cache.bind_source("history", "server-a/1")
            with self.assertRaises(StorageError):
                compare_cached_ticks(
                    cache, "history", "x", self.start, self.end,
                    StubProvider(self.rows(), "server-b/2"),
                )

    def test_oversized_cached_chunk_is_reported(self):
        with tempfile.TemporaryDirectory() as folder:
            cache = ParquetCache(folder)
            data = self.rows()
            cache.save("history", "x", "ticks", 0, data,
                       to_us(self.start), to_us(self.end))
            provider = StubProvider(data)
            result = compare_cached_ticks(
                cache, "history", "x", self.start, self.end,
                provider, window=timedelta(minutes=1),
                max_rows_per_window=1,
            )
            self.assertFalse(result.matches)
            self.assertEqual(result.issue_count, 2)
            self.assertEqual(len(provider.calls), 0)

    def test_validation(self):
        with tempfile.TemporaryDirectory() as folder:
            reader = StubProvider(self.rows())
            with self.assertRaises(ValueError):
                compare_cached_ticks(
                    folder, "s", "x", self.start, self.end, reader,
                    window=timedelta(0),
                )
            with self.assertRaises(ValueError):
                compare_cached_ticks(
                    folder, "s", "x", self.start, self.end, reader,
                    max_rows_per_window=0,
                )


if __name__ == "__main__":
    unittest.main()

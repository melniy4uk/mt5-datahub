import tempfile
import unittest
from datetime import UTC, datetime, timedelta

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from mt5_datahub.arrays import TICK_DTYPE
from mt5_datahub.quality import audit_ticks
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import to_us


class QualityTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 10, 20, 10, 0, tzinfo=UTC)
        self.end = self.start + timedelta(minutes=10)

    def _rows(self, seconds):
        rows = np.zeros(len(seconds), dtype=TICK_DTYPE)
        rows["time_msc"] = [
            to_us(self.start + timedelta(seconds=s)) // 1000
            for s in seconds
        ]
        return rows

    def test_missing_interval_and_gap_across_files(self):
        with tempfile.TemporaryDirectory() as folder:
            cache = ParquetCache(folder)
            middle = self.start + timedelta(minutes=5)
            first = self._rows([0, 2, 2])
            second = self._rows([480, 490])
            cache.save("broker", "XAUUSD.s", "ticks", 0, first,
                       to_us(self.start), to_us(middle))
            cache.save("broker", "XAUUSD.s", "ticks", 0, second,
                       to_us(middle), to_us(self.end))
            report = audit_ticks(
                cache, "broker", "XAUUSD.s", self.start, self.end,
                gap_threshold=timedelta(minutes=1),
            )
            self.assertTrue(report.cached_request_coverage)
            self.assertEqual(report.ticks, 5)
            self.assertEqual(report.segments, 2)
            self.assertEqual(report.repeated_timestamps, 1)
            self.assertEqual(len(report.large_gaps), 1)
            self.assertAlmostEqual(report.large_gaps[0].seconds, 478)
            self.assertEqual(report.out_of_order_timestamps, 0)

    def test_uncovered_interval_is_reported(self):
        with tempfile.TemporaryDirectory() as folder:
            cache = ParquetCache(folder)
            first = self._rows([0, 1])
            midpoint = self.start + timedelta(minutes=5)
            cache.save("broker", "XAUUSD.s", "ticks", 0, first,
                       to_us(self.start), to_us(midpoint))
            report = audit_ticks(
                cache, "broker", "XAUUSD.s", self.start, self.end
            )
            self.assertFalse(report.cached_request_coverage)
            self.assertEqual(report.uncovered, ((midpoint, self.end),))

    def test_empty_cache_and_gap_limit(self):
        with tempfile.TemporaryDirectory() as folder:
            report = audit_ticks(
                folder, "broker", "XAUUSD.s", self.start, self.end,
                max_gaps=0,
            )
            self.assertEqual(report.ticks, 0)
            self.assertEqual(report.large_gaps, ())
            self.assertEqual(report.uncovered, ((self.start, self.end),))

    def test_bad_threshold(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(ValueError):
                audit_ticks(
                    folder, "broker", "XAUUSD.s", self.start, self.end,
                    gap_threshold=timedelta(0),
                )

    def test_raw_out_of_order_is_reported_before_sorting(self):
        with tempfile.TemporaryDirectory() as folder:
            cache = ParquetCache(folder)
            first = self._rows([3, 1, 2])
            # Deliberately inject an old unsorted file (normal save sorts).
            cache.save("broker", "XAUUSD.s", "ticks", 0, first,
                       to_us(self.start), to_us(self.end))
            path = cache.directory / cache._segments_for(
                "broker", "XAUUSD.s", "ticks", 0,
                to_us(self.start), to_us(self.end),
            )[0][0]
            original = pq.read_table(path)
            # Stored order becomes 3s, 1s, 2s.
            stamp = original["time_msc"].to_pylist()
            changed = original.set_column(
                original.schema.get_field_index("time_msc"),
                "time_msc", pa.array([stamp[2], stamp[0], stamp[1]]),
            )
            pq.write_table(changed, path)
            report = audit_ticks(folder, "broker", "XAUUSD.s", self.start, self.end)
            self.assertEqual(report.out_of_order_timestamps, 1)
            self.assertEqual(report.ticks, 3)


if __name__ == "__main__":
    unittest.main()

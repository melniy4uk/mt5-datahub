"""Test explicit broker-session classification of quote gaps."""
import tempfile
import unittest
from datetime import UTC, datetime, time, timedelta

import numpy as np

from mt5_datahub.arrays import TICK_DTYPE
from mt5_datahub.quality import audit_ticks
from mt5_datahub.sessions import SessionSchedule, WeeklySession, classify_gap
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import to_us


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.schedule = SessionSchedule((
            WeeklySession(0, time(1), time(23, 58)),
            WeeklySession(1, time(1), time(23, 58)),
        ), name="test schedule")

    def test_break_near_session_boundaries(self):
        before = datetime(2026, 10, 19, 23, 57, 59, tzinfo=UTC)
        after = datetime(2026, 10, 20, 1, 0, 1, tzinfo=UTC)
        self.assertEqual(classify_gap(before, after, self.schedule), "session_break")

    def test_unexplained_gap_inside_session(self):
        before = datetime(2026, 10, 20, 12, tzinfo=UTC)
        after = before + timedelta(minutes=10)
        self.assertEqual(classify_gap(before, after, self.schedule), "unexplained_gap")

    def test_unverified_without_schedule(self):
        before = datetime(2026, 10, 20, 12, tzinfo=UTC)
        self.assertEqual(
            classify_gap(before, before + timedelta(minutes=10), None),
            "unverified_history",
        )

    def test_overnight_sessions(self):
        schedule = SessionSchedule((WeeklySession(0, time(22), time(2)),))
        start = datetime(2026, 10, 20, 0, 30, tzinfo=UTC)
        end = start + timedelta(minutes=10)
        self.assertEqual(classify_gap(start, end, schedule), "unexplained_gap")

    def test_audit_classification(self):
        with tempfile.TemporaryDirectory() as directory:
            start = datetime(2026, 10, 19, 23, 55, tzinfo=UTC)
            end = datetime(2026, 10, 20, 1, 5, tzinfo=UTC)
            rows = np.zeros(2, dtype=TICK_DTYPE)
            rows["time_msc"] = [
                to_us(datetime(2026, 10, 19, 23, 57, 59, tzinfo=UTC)) // 1000,
                to_us(datetime(2026, 10, 20, 1, 0, 1, tzinfo=UTC)) // 1000,
            ]
            cache = ParquetCache(directory)
            cache.save("broker", "XAUUSD.s", "ticks", 0, rows, to_us(start), to_us(end))
            report = audit_ticks(
                cache, "broker", "XAUUSD.s", start, end,
                schedule=self.schedule, gap_threshold=timedelta(minutes=5),
            )
            self.assertEqual(report.large_gaps[0].classification, "session_break")
            self.assertEqual(report.session_schedule_name, "test schedule")
            self.assertIn("[session_break]", report.format())

    def test_invalid_sessions(self):
        with self.assertRaises(ValueError):
            WeeklySession(7, time(1), time(2))
        with self.assertRaises(ValueError):
            WeeklySession(0, time(1), time(1))


if __name__ == "__main__":
    unittest.main()

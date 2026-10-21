"""Atomic history refresh and orphan handling without a live MT5 terminal."""
import tempfile
import unittest
from datetime import UTC, datetime, timedelta

import numpy as np

from mt5_datahub import MarketData
from mt5_datahub.arrays import TICK_DTYPE
from mt5_datahub.exceptions import StorageError


class MutableProvider:
    def __init__(self):
        self.data = [1, 2]
        self.fail = False
        self.calls = 0

    def connect(self):
        pass

    def get_ticks(self, symbol, start, end):
        self.calls += 1
        if self.fail:
            raise RuntimeError("download failed")
        data = np.zeros(len(self.data), dtype=TICK_DTYPE)
        data["time_msc"] = [
            int(start.timestamp() * 1000) + i * 1000 for i in self.data
        ]
        data["time"] = data["time_msc"] // 1000
        data["bid"] = self.data
        return data


class RefreshTests(unittest.TestCase):
    def test_refresh_keeps_original_on_download_failure_and_truncation(self):
        with tempfile.TemporaryDirectory() as tmp:
            start = datetime(2026, 10, 20, 12, tzinfo=UTC)
            end = start + timedelta(minutes=5)
            provider = MutableProvider()
            data = MarketData("test", tmp, provider=provider)
            original = data.get_ticks("x", start, end)
            with self.assertRaises(StorageError):
                data.refresh_ticks("x", start, end)
            provider.fail = True
            with self.assertRaisesRegex(RuntimeError, "download failed"):
                data.refresh_ticks("x", start, end, confirm=True)
            np.testing.assert_array_equal(original, data.get_ticks("x", start, end))
            provider.fail = False
            provider.data = [2]
            with self.assertRaises(StorageError):
                data.refresh_ticks("x", start, end, confirm=True)
            np.testing.assert_array_equal(original, data.get_ticks("x", start, end))

    def test_refresh_adds_new_ticks_and_preserves_old_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            start = datetime(2026, 10, 20, 12, tzinfo=UTC)
            end = start + timedelta(minutes=5)
            provider = MutableProvider()
            data = MarketData("test", tmp, provider=provider)
            original = data.get_ticks("x", start, end)
            provider.data = [1, 2, 3]
            refreshed = data.refresh_ticks("x", start, end, confirm=True)
            self.assertEqual(len(refreshed), 3)
            np.testing.assert_array_equal(original, refreshed[:2])
            self.assertEqual(len(data.cache.orphan_files()), 1)
            with self.assertRaises(StorageError):
                data.cache.prune_orphans()
            self.assertEqual(data.cache.prune_orphans(confirm=True), 1)
            np.testing.assert_array_equal(
                data.get_ticks("x", start, end), refreshed,
            )

    def test_refuse_modified_quotes_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            start = datetime(2026, 10, 20, 12, tzinfo=UTC)
            end = start + timedelta(minutes=5)
            provider = MutableProvider()
            data = MarketData("test", tmp, provider=provider)
            data.get_ticks("x", start, end)
            provider.data = [2, 3]
            with self.assertRaises(StorageError):
                data.refresh_ticks("x", start, end, confirm=True)
            data.refresh_ticks(
                "x", start, end, confirm=True, allow_changes=True,
            )
            self.assertEqual(data.get_ticks("x", start, end)["bid"].tolist(), [2, 3])


if __name__ == "__main__":
    unittest.main()

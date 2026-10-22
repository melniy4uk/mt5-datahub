"""Count-limited native MT5 pagination and same-ms loss prevention."""
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

from mt5_datahub import MarketData
from mt5_datahub.arrays import TICK_DTYPE
from mt5_datahub.exceptions import MT5RequestError
from mt5_datahub.timeutils import to_us


class TickPaginationTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 10, 20, 10, tzinfo=UTC)
        self.end = self.start + timedelta(seconds=1)
        self.base_ms = to_us(self.start) // 1000

    def rows(self, offsets):
        rows = np.zeros(len(offsets), dtype=TICK_DTYPE)
        rows["time_msc"] = [self.base_ms + i for i in offsets]
        rows["time"] = rows["time_msc"] // 1000
        rows["bid"] = np.arange(len(offsets)) + 100.0
        return rows

    def setup_mt5(self, mt, rows):
        mt.initialize.return_value = True
        mt.terminal_info.return_value = SimpleNamespace(connected=True)
        mt.symbol_info.return_value = SimpleNamespace(visible=True)
        mt.account_info.return_value = None

        def fetch(symbol, start, count, flags):
            first = to_us(start) // 1000
            return rows[rows["time_msc"] >= first][:count].copy()

        mt.copy_ticks_from.side_effect = fetch

    def test_full_history_with_same_ms_at_page_edges(self):
        data = self.rows([0, 0, 1, 2, 2, 3, 5])
        with patch("mt5_datahub.providers.mt5.mt5") as mt:
            self.setup_mt5(mt, data)
            with tempfile.TemporaryDirectory() as folder:
                with MarketData("broker", folder, max_tick_rows=3) as service:
                    result = service.get_ticks("XAUUSD.s", self.start, self.end)
                    np.testing.assert_array_equal(result, data)
                    count = mt.copy_ticks_from.call_count
                    self.assertGreater(count, 2)
                    self.assertEqual(service.get_ticks("XAUUSD.s", self.start, self.end).shape, (7,))
                    self.assertEqual(mt.copy_ticks_from.call_count, count)
                    self.assertEqual(service.cache.missing(
                        "broker", "XAUUSD.s", "ticks", 0,
                        to_us(self.start), to_us(self.end),
                    ), [])
                    self.assertTrue(all(
                        call.args[2] == 3 for call in mt.copy_ticks_from.call_args_list
                    ))
                    self.assertEqual(service.cache.verify_segments(
                        "broker", "XAUUSD.s", "ticks", 0,
                        to_us(self.start), to_us(self.end),
                    ), {})

    def test_single_millisecond_overflows_page_without_false_coverage(self):
        data = self.rows([0, 0, 0, 1])
        with patch("mt5_datahub.providers.mt5.mt5") as mt:
            self.setup_mt5(mt, data)
            with tempfile.TemporaryDirectory() as folder:
                with MarketData("broker", folder, max_tick_rows=2) as service:
                    with self.assertRaisesRegex(MT5RequestError, "same|millisecond"):
                        service.get_ticks("XAUUSD.s", self.start, self.end)
                    self.assertEqual(service.cache.missing(
                        "broker", "XAUUSD.s", "ticks", 0,
                        to_us(self.start), to_us(self.end),
                    ), [(to_us(self.start), to_us(self.end))])

    def test_excludes_tick_exactly_at_request_end(self):
        data = self.rows([0, 500, 1000, 1001])
        with patch("mt5_datahub.providers.mt5.mt5") as mt:
            self.setup_mt5(mt, data)
            with tempfile.TemporaryDirectory() as folder:
                with MarketData("broker", folder, max_tick_rows=2) as service:
                    rows = service.get_ticks("XAUUSD.s", self.start, self.end)
                    self.assertEqual(rows["time_msc"].tolist(), [
                        self.base_ms, self.base_ms + 500,
                    ])

    def test_non_monotonic_native_page_rejected(self):
        rows = self.rows([10, 0])
        with patch("mt5_datahub.providers.mt5.mt5") as mt:
            self.setup_mt5(mt, rows)
            with tempfile.TemporaryDirectory() as folder:
                with MarketData("broker", folder, max_tick_rows=2) as service:
                    with self.assertRaisesRegex(MT5RequestError, "chronological"):
                        service.get_ticks("XAUUSD.s", self.start, self.end)


if __name__ == "__main__":
    unittest.main()

"""Offline tests for bounded MT5 reconnect and source identity binding."""
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

from mt5_datahub.arrays import TICK_DTYPE
from mt5_datahub.exceptions import MT5ConnectionError, MT5RequestError, StorageError
from mt5_datahub.providers.mt5 import MT5Provider
from mt5_datahub.storage.parquet import ParquetCache


class ProviderRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 10, 20, 10, tzinfo=UTC)
        self.end = self.start + timedelta(seconds=5)

    def test_reconnects_once_after_broker_disconnect(self):
        with patch("mt5_datahub.providers.mt5.mt5") as mt:
            mt.initialize.return_value = True
            mt.last_error.return_value = (-1, "broken")
            mt.terminal_info.side_effect = [
                SimpleNamespace(connected=False),
                SimpleNamespace(connected=True),
            ]
            mt.symbol_info.return_value = SimpleNamespace(visible=True)
            ticks = np.zeros(1, dtype=TICK_DTYPE)
            ticks["time_msc"] = int(self.start.timestamp() * 1000)
            mt.copy_ticks_range.return_value = ticks
            with MT5Provider() as provider:
                got = provider.get_ticks("XAUUSD.s", self.start, self.end)
                self.assertEqual(len(got), 1)
            self.assertEqual(mt.initialize.call_count, 2)
            self.assertEqual(mt.copy_ticks_range.call_count, 1)

    def test_request_retries_only_if_connection_lost(self):
        with patch("mt5_datahub.providers.mt5.mt5") as mt:
            mt.initialize.return_value = True
            mt.terminal_info.side_effect = [
                SimpleNamespace(connected=True),
                SimpleNamespace(connected=False),
                SimpleNamespace(connected=False),
                SimpleNamespace(connected=True),
            ]
            mt.symbol_info.return_value = SimpleNamespace(visible=True)
            mt.copy_ticks_range.side_effect = [None, np.zeros(0, dtype=TICK_DTYPE)]
            with MT5Provider() as provider:
                self.assertEqual(len(provider.get_ticks("x", self.start, self.end)), 0)
            self.assertEqual(mt.copy_ticks_range.call_count, 2)
            self.assertEqual(mt.initialize.call_count, 2)

    def test_no_retry_for_provider_error(self):
        with patch("mt5_datahub.providers.mt5.mt5") as mt:
            mt.initialize.return_value = True
            mt.terminal_info.return_value = SimpleNamespace(connected=True)
            mt.symbol_info.return_value = SimpleNamespace(visible=True)
            mt.copy_ticks_range.return_value = None
            mt.last_error.return_value = (-7, "failed")
            with MT5Provider() as provider:
                with self.assertRaises(MT5RequestError):
                    provider.get_ticks("x", self.start, self.end)
            self.assertEqual(mt.copy_ticks_range.call_count, 1)

    def test_reconnection_fails_cleanly(self):
        with patch("mt5_datahub.providers.mt5.mt5") as mt:
            mt.initialize.side_effect = [True, False]
            mt.terminal_info.return_value = SimpleNamespace(connected=False)
            mt.last_error.return_value = (-2, "MT5 not available")
            with MT5Provider() as provider:
                with self.assertRaises(MT5ConnectionError):
                    provider.get_ticks("x", self.start, self.end)


class SourceIdentityTests(unittest.TestCase):
    def test_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = ParquetCache(directory)
            self.assertTrue(storage.bind_source("broker", "server-A/100"))
            self.assertTrue(storage.bind_source("broker", "server-A/100"))
            with self.assertRaises(StorageError):
                storage.bind_source("broker", "server-B/200")

    def test_legacy_cache_requires_new_source_to_bind(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = ParquetCache(directory)
            rows = np.zeros(1, dtype=TICK_DTYPE)
            rows["time_msc"] = 1000
            storage.save("legacy", "x", "ticks", 0, rows, 1_000_000, 2_000_000)
            with self.assertLogs("mt5_datahub.storage.parquet", level="WARNING"):
                self.assertFalse(storage.bind_source("legacy", "server-A/100"))
            self.assertTrue(storage.bind_source("new", "server-A/100"))


if __name__ == "__main__":
    unittest.main()

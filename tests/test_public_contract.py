"""Stable 0.2.0 package-level import and non-network API contract."""
import inspect
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from importlib.metadata import version

from mt5_datahub import CacheInspection, MarketData, inspect_cache
from mt5_datahub.providers.mt5 import MT5Provider
from mt5_datahub.storage.parquet import ParquetCache


class PublicContractTests(unittest.TestCase):
    def test_distribution_version(self):
        self.assertEqual(version("mt5-datahub"), "0.2.0")

    def test_primary_api_signatures(self):
        params = inspect.signature(MarketData.__init__).parameters
        for name in (
            "source", "cache_dir", "provider", "tick_chunk",
            "candle_chunk", "max_tick_rows",
        ):
            self.assertIn(name, params)
        for name in (
            "get_ticks", "get_candles", "iter_ticks", "stream_ticks",
            "refresh_ticks", "refresh_candles", "connect", "disconnect",
        ):
            self.assertTrue(callable(getattr(MarketData, name)))
        self.assertTrue(callable(MT5Provider.get_ticks))
        self.assertTrue(callable(MT5Provider.get_candles))

    def test_offline_inspection_from_package_root(self):
        start = datetime(2026, 10, 20, 10, tzinfo=UTC)
        end = start + timedelta(minutes=1)
        with tempfile.TemporaryDirectory() as path:
            market = MarketData("release-smoke", path)
            self.assertIsInstance(market.cache, ParquetCache)
            report = inspect_cache(
                path, "release-smoke", "XAUUSD.s", start, end,
            )
            self.assertIsInstance(report, CacheInspection)
            self.assertFalse(report.saved_request_coverage)
            self.assertEqual(report.unrequested, ((start, end),))


if __name__ == "__main__":
    unittest.main()

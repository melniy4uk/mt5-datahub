"""Automatic MT5 source naming and cache-safety tests."""
import tempfile
import unittest
from datetime import UTC, datetime, timedelta

import numpy as np

from mt5_datahub import MarketData, inspect_cache
from mt5_datahub.arrays import TICK_DTYPE
from mt5_datahub.exceptions import MT5ConnectionError, StorageError
from mt5_datahub.timeutils import to_us


class FakeAccountProvider:
    def __init__(self, identity):
        self.identity = identity
        self.requests = 0
        self.connected = False

    def connect(self):
        self.connected = True

    def disconnect(self):
        self.connected = False

    def source_identity(self):
        return self.identity

    def get_ticks(self, symbol, start, end):
        self.requests += 1
        rows = np.zeros(2, dtype=TICK_DTYPE)
        rows["time_msc"] = [to_us(start) // 1000, to_us(end) // 1000 - 1]
        rows["time"] = rows["time_msc"] // 1000
        rows["bid"] = [100, 101]
        return rows


class AutoSourceTests(unittest.TestCase):
    def setUp(self):
        self.a = datetime(2026, 10, 20, 10, tzinfo=UTC)
        self.b = self.a + timedelta(minutes=2)

    def test_auto_connect_resolves_source(self):
        with tempfile.TemporaryDirectory() as path:
            provider = FakeAccountProvider("Bybit-Live-6/12345")
            hub = MarketData(cache_dir=path, provider=provider)
            with self.assertRaises(MT5ConnectionError):
                _ = hub.source
            with hub:
                self.assertEqual(hub.source, "mt5:Bybit-Live-6/12345")
                self.assertEqual(hub.cache.get_source_identity(hub.source),
                                 "Bybit-Live-6/12345")
            self.assertTrue(provider.connected)  # external provider ownership

    def test_reuse_and_isolation(self):
        with tempfile.TemporaryDirectory() as path:
            p1 = FakeAccountProvider("server/1")
            first = MarketData(cache_dir=path, provider=p1)
            ticks = first.get_ticks("XAUUSD.s", self.a, self.b)
            np.testing.assert_array_equal(first.get_ticks("XAUUSD.s", self.a, self.b), ticks)
            self.assertEqual(p1.requests, 1)
            p2 = FakeAccountProvider("server/1")
            second = MarketData(cache_dir=path, provider=p2)
            np.testing.assert_array_equal(second.get_ticks("XAUUSD.s", self.a, self.b), ticks)
            self.assertEqual(p2.requests, 0)
            self.assertTrue(inspect_cache(path, second.source, "XAUUSD.s",
                                          self.a, self.b).saved_request_coverage)
            p3 = FakeAccountProvider("server/2")
            third = MarketData(cache_dir=path, provider=p3)
            third.get_ticks("XAUUSD.s", self.a, self.b)
            self.assertEqual(p3.requests, 1)
            self.assertNotEqual(first.source, third.source)

    def test_missing_identity_fails_closed(self):
        with tempfile.TemporaryDirectory() as path:
            for identity in (None, ""):
                with self.subTest(identity=identity):
                    p = FakeAccountProvider(identity)
                    hub = MarketData(cache_dir=path, provider=p)
                    with self.assertRaisesRegex(MT5ConnectionError, "Cannot determine"):
                        hub.get_ticks("XAUUSD.s", self.a, self.b)
                    self.assertEqual(p.requests, 0)

    def test_switch_account_on_cache_hit_rejected(self):
        with tempfile.TemporaryDirectory() as path:
            p = FakeAccountProvider("server/1")
            hub = MarketData(cache_dir=path, provider=p)
            hub.get_ticks("XAUUSD.s", self.a, self.b)
            p.identity = "server/2"
            with self.assertRaisesRegex(StorageError, "account/server changed"):
                hub.get_ticks("XAUUSD.s", self.a, self.b)
            self.assertEqual(p.requests, 1)
            self.assertEqual(hub.source, "mt5:server/1")

    def test_explicit_source_and_refresh_remain_supported(self):
        with tempfile.TemporaryDirectory() as path:
            p = FakeAccountProvider("server/1")
            explicit = MarketData("my-existing-source", path, provider=p)
            explicit.get_ticks("XAUUSD.s", self.a, self.b)
            self.assertEqual(explicit.source, "my-existing-source")
            self.assertEqual(explicit.cache.get_source_identity("my-existing-source"),
                             "server/1")
            auto = MarketData(cache_dir=path, provider=p)
            rows = auto.refresh_ticks("XAUUSD.s", self.a, self.b, confirm=True)
            self.assertEqual(len(rows), 2)
            self.assertEqual(auto.source, "mt5:server/1")
            self.assertEqual(sum(len(b) for b in auto.stream_ticks(
                "XAUUSD.s", self.a, self.b, batch_size=1)), 2)


if __name__ == "__main__":
    unittest.main()

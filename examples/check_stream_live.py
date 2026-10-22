"""Check progressive tick loading against a real MT5 terminal (Bybit-Live-6).

Run with terminal connected to Bybit-Live-6:
    uv run python examples/check_stream_live.py

Uses an isolated temporary cache, never places orders, and checks that the
first batch is yielded before downloading the second time interval.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from tempfile import TemporaryDirectory

import numpy as np

from mt5_datahub import MarketData
from mt5_datahub.providers.mt5 import MT5Provider


SYMBOL = "XAUUSD.s"
SOURCE = "bybit-live-6-stream-integration"
START = datetime(2026, 10, 20, 10, 0, tzinfo=UTC)
MIDDLE = START + timedelta(minutes=5)
END = START + timedelta(minutes=10)


class CountingProvider(MT5Provider):
    def __init__(self) -> None:
        super().__init__()
        self.tick_calls = 0

    def iter_tick_pages(self, symbol, start, end, *, page_size=50_000):
        assert symbol == SYMBOL
        self.tick_calls += 1
        yield from super().iter_tick_pages(
            symbol, start, end, page_size=page_size,
        )

    def get_ticks(self, symbol, start, end):
        assert symbol == SYMBOL
        self.tick_calls += 1
        return super().get_ticks(symbol, start, end)


def main() -> None:
    provider = CountingProvider()
    with TemporaryDirectory(prefix="mt5-stream-live-") as cache_dir:
        data = MarketData(
            SOURCE, cache_dir, provider=provider, tick_chunk=timedelta(minutes=5)
        )
        try:
            iterator = data.stream_ticks(
                SYMBOL, START, END, batch_size=1024
            )
            assert provider.tick_calls == 0, "Iterator should be lazy"
            first = next(iterator)
            assert len(first) > 0, "No ticks in first 5 minutes"
            assert provider.tick_calls == 1, (
                "Second range was downloaded before first batch was emitted"
            )
            iterator.close()
            assert provider.tick_calls == 1

            # Resume: previously cached interval must not be fetched again.
            batches = list(data.stream_ticks(SYMBOL, START, END, batch_size=1024))
            streamed = np.concatenate(batches)
            assert provider.tick_calls == 2, (
                "Expected exactly one extra MT5 request for remaining 5 minutes"
            )
            assert len(streamed) >= len(first)
            assert np.array_equal(streamed[:len(first)], first)

            # Full read on a completely cached range must avoid MT5.
            full = data.get_ticks(SYMBOL, START, END)
            assert provider.tick_calls == 2, "Unexpected download on cache hit"
            np.testing.assert_array_equal(full, streamed)

            again = list(data.stream_ticks(SYMBOL, START, END, batch_size=1024))
            assert provider.tick_calls == 2
            np.testing.assert_array_equal(np.concatenate(again), full)
            print(f"First batch: {len(first):,} ticks")
            print(f"Complete range: {len(full):,} ticks")
            print(f"Batches: {len(batches)}")
            print(f"MT5 tick windows: {provider.tick_calls}")
            print("PASS: early yield, resume, cached replay and exact tick equality")
        finally:
            # An injected provider is intentionally not owned by MarketData.
            provider.disconnect()


if __name__ == "__main__":
    main()

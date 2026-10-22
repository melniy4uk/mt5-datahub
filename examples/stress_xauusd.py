"""On-demand stress test for historical XAUUSD.s tick caching.

Run with a connected MT5 terminal:
    uv run python examples/stress_xauusd.py

Uses a temporary cache. Does not submit trading orders.
"""
from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from mt5_datahub import MarketData
from mt5_datahub.providers.mt5 import MT5Provider
from mt5_datahub.timeutils import to_us

SYMBOL = "XAUUSD.s"
SOURCE = "xauusd-s-stress-test"
START = datetime(2026, 10, 19, tzinfo=UTC)
END = datetime(2026, 10, 21, tzinfo=UTC)


class CountingProvider(MT5Provider):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0
        self.fail_on_call: int | None = None

    def iter_tick_pages(self, symbol, start, end, *, page_size=50_000):
        if symbol != SYMBOL:
            raise AssertionError(f"Unexpected instrument {symbol}")
        self.calls += 1
        if self.fail_on_call == self.calls:
            raise RuntimeError("Deliberate interruption for recovery test")
        yield from super().iter_tick_pages(
            symbol, start, end, page_size=page_size,
        )

    def get_ticks(self, symbol: str, start: datetime, end: datetime) -> np.ndarray:
        if symbol != SYMBOL:
            raise AssertionError(f"Unexpected instrument {symbol}")
        self.calls += 1
        if self.fail_on_call == self.calls:
            raise RuntimeError("Deliberate interruption for recovery test")
        return super().get_ticks(symbol, start, end)


def assert_valid(rows: np.ndarray, start: datetime, end: datetime) -> None:
    stamps = rows["time_msc"]
    if len(stamps):
        assert np.all(np.diff(stamps) >= 0), "Ticks are not sorted"
        assert np.all(stamps >= to_us(start) // 1000), "Tick before start"
        assert np.all(stamps * 1000 < to_us(end)), "Tick after end"


def run(cache_dir: Path, start: datetime, end: datetime) -> None:
    half = start + (end - start) / 2
    extended = end + timedelta(hours=1)
    # We intentionally simulate a failure before the second request. The
    # first completed chunk must survive and be reused after reopening.
    first_provider = CountingProvider()
    first_provider.fail_on_call = 2
    try:
        with MarketData(SOURCE, cache_dir, provider=first_provider,
                        tick_chunk=timedelta(hours=2)) as market:
            market.get_ticks(SYMBOL, start, end)
    except RuntimeError as exc:
        assert str(exc) == "Deliberate interruption for recovery test", str(exc)
    else:
        raise AssertionError("Expected interruption did not occur")
    assert first_provider.calls == 2
    first_provider.disconnect()

    second_provider = CountingProvider()
    try:
        with MarketData(SOURCE, cache_dir, provider=second_provider,
                        tick_chunk=timedelta(hours=2)) as market:
            all_ticks = market.get_ticks(SYMBOL, start, end)
            assert len(all_ticks), "No historical data returned"
            assert_valid(all_ticks, start, end)
            calls_after_resume = second_provider.calls

            # A fully cached query MUST NOT contact the provider.
            cached = market.get_ticks(SYMBOL, start, end)
            assert second_provider.calls == calls_after_resume
            assert np.array_equal(cached, all_ticks), "Cache roundtrip differs"

            overlapping = market.get_ticks(SYMBOL, half, end)
            assert second_provider.calls == calls_after_resume
            expected = all_ticks[all_ticks["time_msc"] * 1000 >= to_us(half)]
            assert np.array_equal(overlapping, expected), "Overlapping range differs"

            # Only one new request should be made for a one-hour extension.
            extended_ticks = market.get_ticks(SYMBOL, start, extended)
            assert second_provider.calls == calls_after_resume + 1
            assert np.array_equal(extended_ticks[:len(all_ticks)], all_ticks)
            assert_valid(extended_ticks, start, extended)

            # Different ticks can have the same millisecond; do not deduplicate by time.
            print(f"Recovered rows: {len(all_ticks):,}")
            print(f"Extended rows: {len(extended_ticks):,}")
            print(f"Tick windows before simulated failure: {first_provider.calls}")
            print(f"Tick windows after restart: {second_provider.calls}")
            print(f"Duplicate timestamps (not dropped): {len(all_ticks) - len(np.unique(all_ticks['time_msc'])):,}")
            print("PASS: XAUUSD.s multi-day load, recovery, overlap, cache reuse, extension")
    finally:
        second_provider.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keep-cache", type=Path, default=None,
                        help="Keep stress-test cache in this directory")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    if args.keep_cache:
        args.keep_cache.mkdir(parents=True, exist_ok=True)
        run(args.keep_cache, START, END)
    else:
        with TemporaryDirectory(prefix="mt5-stress-xauusd-") as tmp:
            run(Path(tmp), START, END)

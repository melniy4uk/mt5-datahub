"""Real-world check of public MarketData.iter_ticks against compacted XAUUSD.s cache.

Requires preloaded cache from examples/audit_xauusd.py. Reads cache only;
a blocking fake provider prevents silent MT5 downloads.
"""
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter

import numpy as np

from mt5_datahub import MarketData
from mt5_datahub.timeutils import to_us


class NoDownload:
    def connect(self):
        raise AssertionError("Unexpected MT5 connection: cached range is incomplete")

    def get_ticks(self, *args):
        raise AssertionError("Unexpected MT5 tick request")


def main():
    source, symbol = "xauusd-s-history", "XAUUSD.s"
    cache_dir = Path(".mt5-datahub/audit")
    start = datetime(2026, 10, 19, tzinfo=UTC)
    end = datetime(2026, 10, 21, tzinfo=UTC)
    data = MarketData(source=source, cache_dir=cache_dir, provider=NoDownload())
    missing = data.cache.missing(source, symbol, "ticks", 0, to_us(start), to_us(end))
    if missing:
        raise SystemExit(f"Cache is incomplete ({len(missing)} intervals); run audit first")
    before = perf_counter()
    expected = data.get_ticks(symbol, start, end)
    full_sec = perf_counter() - before
    before = perf_counter()
    chunks = []
    rows = 0
    for batch in data.iter_ticks(symbol, start, end, batch_size=65_536):
        if len(batch) > 65_536:
            raise AssertionError("Batch size exceeded")
        chunks.append(batch)
        rows += len(batch)
    streamed_sec = perf_counter() - before
    actual = np.concatenate(chunks) if chunks else expected[:0]
    np.testing.assert_array_equal(expected, actual)
    print(f"Verified ticks: {rows:,}")
    print(f"Full read: {full_sec:.3f}s")
    print(f"Public iterator: {streamed_sec:.3f}s")
    print(f"Batches: {len(chunks)}")
    print("PASS: Public API preserves every tick and uses cached data only")


if __name__ == "__main__":
    main()

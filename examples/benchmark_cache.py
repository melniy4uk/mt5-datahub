"""Benchmark cached XAUUSD.s data; no requests to MT5.

Run:
    uv run python examples/benchmark_cache.py
    uv run python examples/benchmark_cache.py --compact
"""
import argparse
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter

from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import to_us

SOURCE = "xauusd-s-history"
SYMBOL = "XAUUSD.s"
START = datetime(2026, 10, 19, tzinfo=UTC)
END = datetime(2026, 10, 21, tzinfo=UTC)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache-dir", type=Path, default=Path(".mt5-datahub/audit"))
    parser.add_argument("--compact", action="store_true",
                        help="Merge adjacent segments; modifies the cache")
    args = parser.parse_args()

    cache = ParquetCache(args.cache_dir)
    first, last = to_us(START), to_us(END)
    if args.compact:
        reduced = cache.compact(SOURCE, SYMBOL, "ticks", 0, first, last)
        print(f"Compaction removed {reduced} Parquet segments")

    begin = perf_counter()
    full = cache.read(SOURCE, SYMBOL, "ticks", 0, first, last)
    full_seconds = perf_counter() - begin

    begin = perf_counter()
    selected = cache.read(SOURCE, SYMBOL, "ticks", 0, first, last,
                          columns=("time_msc", "bid", "ask"))
    projected_seconds = perf_counter() - begin

    begin = perf_counter()
    count = sum(len(batch) for batch in cache.iter_batches(
        SOURCE, SYMBOL, "ticks", 0, first, last,
        columns=("time_msc", "bid", "ask"), batch_size=65536
    ))
    batch_seconds = perf_counter() - begin

    assert count == len(full) == len(selected)
    print(f"XAUUSD.s cached ticks: {len(full):,}")
    print(f"Full array:       {full_seconds:.3f}s")
    print(f"Projected array:  {projected_seconds:.3f}s")
    print(f"Streamed batches: {batch_seconds:.3f}s")
    print(f"Full bytes: {full.nbytes:,}; projected bytes: {selected.nbytes:,}")


if __name__ == "__main__":
    main()

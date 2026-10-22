"""Minimal real-MT5 workflow: download, reuse cache, inspect and audit.

Example (PowerShell):
    uv run python examples/workflow.py --source "my-broker/my-account" --symbol XAUUSD.s

Requires a connected MT5 terminal and broker history. No trading operations.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta

import MetaTrader5 as mt5

from mt5_datahub import MarketData, inspect_cache
from mt5_datahub.quality import audit_ticks


def main() -> None:
    parser = argparse.ArgumentParser(description="MT5 history -> cache -> inspection")
    parser.add_argument("--source", required=True, help="Unique broker/account dataset name")
    parser.add_argument("--symbol", default="XAUUSD.s", help="MT5 symbol, broker-specific")
    parser.add_argument("--cache-dir", default=".mt5-datahub", help="Persistent cache directory")
    parser.add_argument(
        "--start",
        default="2026-10-20T10:00:00+00:00",
        help="Start datetime in ISO 8601 with timezone; default is a historical UTC window",
    )
    parser.add_argument("--minutes", type=int, default=10, help="Requested duration in minutes")
    args = parser.parse_args()

    start = datetime.fromisoformat(args.start)
    if start.utcoffset() is None:
        parser.error("--start must include a timezone (for example +00:00)")
    if args.minutes <= 0:
        parser.error("--minutes must be positive")
    end = start + timedelta(minutes=args.minutes)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    with MarketData(source=args.source, cache_dir=args.cache_dir) as data:
        ticks = data.get_ticks(args.symbol, start, end)
        candles = data.get_candles(args.symbol, mt5.TIMEFRAME_M1, start, end)
        cached = data.get_ticks(args.symbol, start, end)
        count = sum(
            len(batch)
            for batch in data.iter_ticks(
                args.symbol, start, end,
                columns=("time_msc", "bid", "ask"), batch_size=10_000,
            )
        )
        print(f"First request: {len(ticks)} ticks; second request: {len(cached)} ticks")
        print(f"Batch read: {count} ticks; closed M1 candles: {len(candles)}")

    inspection = inspect_cache(args.cache_dir, args.source, args.symbol, start, end)
    print(f"Saved request coverage: {inspection.saved_request_coverage}")
    print(f"Parquet readable: {inspection.files_readable}")
    print(f"Unrequested intervals: {inspection.unrequested}")
    print(f"Corrupt segments: {inspection.corrupt_segments}")

    report = audit_ticks(args.cache_dir, args.source, args.symbol, start, end)
    print(report.format())
    print("Note: cache coverage is not proof of complete broker history.")


if __name__ == "__main__":
    main()

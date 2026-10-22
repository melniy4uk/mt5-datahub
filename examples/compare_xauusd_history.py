"""Re-fetch an existing XAUUSD.s cache read-only and compare tick records.

Usage (requires connected MetaTrader 5; DOES NOT place trades):
    uv run python examples/compare_xauusd_history.py
    uv run python examples/compare_xauusd_history.py --minutes 15

Only reports differences. Never replaces/deletes files or updates coverage.
"""
from argparse import ArgumentParser
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mt5_datahub.providers.mt5 import MT5Provider
from mt5_datahub.verification import compare_cached_ticks

SOURCE = "xauusd-s-history"
SYMBOL = "XAUUSD.s"
CACHE = Path(".mt5-datahub") / "audit"
START = datetime(2026, 10, 19, tzinfo=UTC)
END = datetime(2026, 10, 21, tzinfo=UTC)


def main() -> None:
    parser = ArgumentParser(description="Compare saved MT5 ticks against a fresh read")
    parser.add_argument("--minutes", type=int, default=60, help="MT5 request window in minutes")
    parser.add_argument("--max-rows", type=int, default=250_000)
    args = parser.parse_args()

    with MT5Provider() as provider:
        report = compare_cached_ticks(
            CACHE, SOURCE, SYMBOL, START, END, provider,
            window=timedelta(minutes=args.minutes),
            max_rows_per_window=args.max_rows,
        )
    print(report.format())
    if not report.matches:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

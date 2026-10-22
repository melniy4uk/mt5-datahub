"""Read-only cached tick OHLC vs fresh M1 MT5 candle cross-check.

Run:
    uv run python examples/check_tick_ohlc_live.py --tolerance 0.01

Requires historical XAUUSD.s ticks in .mt5-datahub/audit under
source xauusd-s-history and a connected MT5 terminal. This check neither
writes cache data nor makes trading requests. Different broker candle
construction rules can produce false mismatches.
"""
import argparse
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mt5_datahub.ohlc_check import compare_tick_ohlc_to_m1
from mt5_datahub.providers.mt5 import MT5Provider

SOURCE = "xauusd-s-history"
SYMBOL = "XAUUSD.s"
CACHE = Path(".mt5-datahub") / "audit"
START = datetime(2026, 10, 19, tzinfo=UTC)
END = datetime(2026, 10, 21, tzinfo=UTC)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tolerance", type=float, default=0.01)
    parser.add_argument("--minutes", type=int, default=60)
    args = parser.parse_args()
    with MT5Provider() as provider:
        report = compare_tick_ohlc_to_m1(
            CACHE, SOURCE, SYMBOL, START, END, provider,
            tolerance=args.tolerance,
            window=timedelta(minutes=args.minutes),
        )
    print(report.format())
    if not report.consistent:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

"""Download XAUUSD.s historical ticks and audit the local cache.

Run:
    uv run python examples/audit_xauusd.py

Uses a persistent cache in .mt5-datahub/audit; subsequent runs reuse it.
"""
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mt5_datahub import MarketData
from mt5_datahub.quality import audit_ticks

SYMBOL = "XAUUSD.s"
SOURCE = "xauusd-s-history"
CACHE = Path(".mt5-datahub") / "audit"
START = datetime(2026, 10, 19, tzinfo=UTC)
END = datetime(2026, 10, 21, tzinfo=UTC)


def main() -> None:
    with MarketData(source=SOURCE, cache_dir=CACHE) as market:
        rows = market.get_ticks(SYMBOL, START, END)
        print(f"Downloaded/read {len(rows):,} ticks")
    report = audit_ticks(
        CACHE, SOURCE, SYMBOL, START, END,
        gap_threshold=timedelta(minutes=5),
        max_gaps=20,
    )
    print(report.format())


if __name__ == "__main__":
    main()

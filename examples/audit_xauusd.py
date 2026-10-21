"""Download XAUUSD.s historical ticks and audit the local cache.

Run:
    uv run python examples/audit_xauusd.py

Uses a persistent cache in .mt5-datahub/audit; subsequent runs reuse it.
"""
import argparse
import json
from datetime import UTC, datetime, time, timedelta
from pathlib import Path

from mt5_datahub import MarketData
from mt5_datahub.quality import audit_ticks
from mt5_datahub.sessions import SessionSchedule, WeeklySession

SYMBOL = "XAUUSD.s"
SOURCE = "xauusd-s-history"
CACHE = Path(".mt5-datahub") / "audit"
START = datetime(2026, 10, 19, tzinfo=UTC)
END = datetime(2026, 10, 21, tzinfo=UTC)


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit historical XAUUSD.s ticks")
    parser.add_argument(
        "--sessions",
        type=Path,
        default=None,
        help="Path to verified broker quoting sessions JSON (UTC)",
    )
    args = parser.parse_args()
    schedule = None
    if args.sessions is not None:
        with args.sessions.open(encoding="utf-8") as handle:
            config = json.load(handle)
        schedule = SessionSchedule(
            sessions=tuple(
                WeeklySession(
                    weekday=int(row["weekday"]),
                    opens=time.fromisoformat(row["opens"]),
                    closes=time.fromisoformat(row["closes"]),
                )
                for row in config["sessions"]
            ),
            name=config.get("name", "User-supplied UTC sessions"),
        )

    with MarketData(source=SOURCE, cache_dir=CACHE) as market:
        rows = market.get_ticks(SYMBOL, START, END)
        print(f"Downloaded/read {len(rows):,} ticks")
    report = audit_ticks(
        CACHE, SOURCE, SYMBOL, START, END,
        gap_threshold=timedelta(minutes=5),
        max_gaps=20,
        schedule=schedule,
    )
    print(report.format())


if __name__ == "__main__":
    main()

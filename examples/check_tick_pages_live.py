"""Validate actual MT5 copy_ticks_from pagination against copy_ticks_range.

Run on a connected Bybit-Live-6 MT5 terminal (read-only):
    uv run python examples/check_tick_pages_live.py

Uses a small historical window. Does not write or modify any cache and never
places orders. Specifically checks the Python terminal's cursor semantics.
"""
from datetime import UTC, datetime, timedelta

import numpy as np

from mt5_datahub.providers.mt5 import MT5Provider

SYMBOL = "XAUUSD.s"
START = datetime(2026, 10, 20, 10, tzinfo=UTC)
END = START + timedelta(minutes=10)
PAGE_SIZE = 512


def main() -> None:
    with MT5Provider() as mt5:
        baseline = mt5.get_ticks(SYMBOL, START, END)
        pages = list(
            mt5.iter_tick_pages(SYMBOL, START, END, page_size=PAGE_SIZE)
        )
    if not len(baseline):
        raise AssertionError("No historical ticks returned by MT5")
    if not pages:
        raise AssertionError("Count-paged MT5 returned no historical ticks")
    actual = np.concatenate([rows for rows, _, _ in pages])
    if not np.array_equal(actual, baseline):
        raise AssertionError(
            "MT5 count-pagination differed from range history: "
            f"reference={len(baseline):,}, count_pages={len(actual):,}. "
            "Do not use count-based mode until this is investigated."
        )
    if not all(0 < len(rows) <= PAGE_SIZE for rows, _, _ in pages):
        raise AssertionError("A native tick page exceeded its row bound")
    if not all(a < b for _, a, b in pages):
        raise AssertionError("Invalid page coverage interval")
    print(f"Reference ticks: {len(baseline):,}")
    print(f"Count-paged ticks: {len(actual):,}")
    print(f"Saved ranges: {len(pages)}")
    print(f"Per-call MT5 maximum ticks: {PAGE_SIZE:,}")
    print("PASS: count pages exactly match MT5 range ticks, including all fields")


if __name__ == "__main__":
    main()

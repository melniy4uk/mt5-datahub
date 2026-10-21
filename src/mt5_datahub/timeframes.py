"""MT5 timeframe lengths and closed-bar checks."""
from datetime import UTC, datetime

import MetaTrader5 as mt5
import numpy as np
from numpy import ndarray

from mt5_datahub.timeutils import to_us

SECONDS = {
    mt5.TIMEFRAME_M1: 60, mt5.TIMEFRAME_M2: 120,
    mt5.TIMEFRAME_M3: 180, mt5.TIMEFRAME_M4: 240,
    mt5.TIMEFRAME_M5: 300, mt5.TIMEFRAME_M6: 360,
    mt5.TIMEFRAME_M10: 600, mt5.TIMEFRAME_M12: 720,
    mt5.TIMEFRAME_M15: 900, mt5.TIMEFRAME_M20: 1200,
    mt5.TIMEFRAME_M30: 1800, mt5.TIMEFRAME_H1: 3600,
    mt5.TIMEFRAME_H2: 7200, mt5.TIMEFRAME_H3: 10800,
    mt5.TIMEFRAME_H4: 14400, mt5.TIMEFRAME_H6: 21600,
    mt5.TIMEFRAME_H8: 28800, mt5.TIMEFRAME_H12: 43200,
    mt5.TIMEFRAME_D1: 86400, mt5.TIMEFRAME_W1: 604800,
}
VALID = frozenset((*SECONDS, mt5.TIMEFRAME_MN1))


def bar_end_us(open_seconds: int, timeframe: int) -> int:
    if timeframe == mt5.TIMEFRAME_MN1:
        opened = datetime.fromtimestamp(open_seconds, UTC)
        year, month = opened.year, opened.month
        return to_us(datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=UTC))
    return (open_seconds + SECONDS[timeframe]) * 1_000_000


def closed_only(rows: ndarray, timeframe: int, now_us: int) -> ndarray:
    mask = np.fromiter(
        (bar_end_us(int(t), timeframe) <= now_us for t in rows["time"]),
        dtype=bool, count=len(rows),
    )
    return rows[mask]

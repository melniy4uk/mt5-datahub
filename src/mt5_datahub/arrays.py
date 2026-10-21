import numpy as np
from numpy import ndarray

TICK_DTYPE = np.dtype([
    ("time", "<i8"), ("bid", "<f8"), ("ask", "<f8"),
    ("last", "<f8"), ("volume", "<u8"), ("time_msc", "<i8"),
    ("flags", "<u4"), ("volume_real", "<f8"),
])
CANDLE_DTYPE = np.dtype([
    ("time", "<i8"), ("open", "<f8"), ("high", "<f8"),
    ("low", "<f8"), ("close", "<f8"), ("tick_volume", "<i8"),
    ("spread", "<i4"), ("real_volume", "<i8"),
])


def empty(kind: str) -> ndarray:
    return np.empty(0, dtype=TICK_DTYPE if kind == "ticks" else CANDLE_DTYPE)


def timestamps_us(data: ndarray, kind: str) -> ndarray:
    field, scale = ("time_msc", 1000) if kind == "ticks" else ("time", 1000000)
    return data[field].astype(np.int64) * scale


def select_range(data: ndarray, kind: str, start_us: int, end_us: int) -> ndarray:
    timestamps = timestamps_us(data, kind)
    return data[(timestamps >= start_us) & (timestamps < end_us)]


def combine(chunks: list[ndarray], kind: str) -> ndarray:
    if not chunks:
        return empty(kind)
    merged = np.concatenate(chunks)
    return merged[np.argsort(timestamps_us(merged, kind), kind="stable")]

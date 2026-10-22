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



def canonical_candles(rows: ndarray) -> ndarray:
    """Normalize MetaTrader5 rates into the library's stable candle dtype.

    MT5 may supply tick_volume and real_volume as unsigned int64, while
    CANDLE_DTYPE historically stores these fields as signed int64. Validate
    named fields before converting; reject integer overflow instead of
    silently wrapping large unsigned values. Existing Parquet schemas stay
    compatible.
    """
    names = CANDLE_DTYPE.names or ()
    actual_names = rows.dtype.names or ()
    if rows.ndim != 1 or any(name not in actual_names for name in names):
        raise ValueError("MT5 candles must have the canonical named columns")
    if rows.dtype == CANDLE_DTYPE:
        return rows
    result = np.empty(len(rows), dtype=CANDLE_DTYPE)
    for name in names:
        values = rows[name]
        if name in ("open", "high", "low", "close"):
            if values.dtype.kind != "f":
                raise ValueError(f"MT5 candle {name!r} is not floating point")
        elif values.dtype.kind not in "iu":
            raise ValueError(f"MT5 candle {name!r} is not an integer")
        else:
            if name == "spread":
                minimum, maximum = -(2**31), 2**31 - 1
            else:
                minimum, maximum = -(2**63), 2**63 - 1
            if values.size and (np.any(values < minimum) or np.any(values > maximum)):
                raise ValueError(f"MT5 candle {name!r} exceeds internal integer range")
        result[name] = values
    return result

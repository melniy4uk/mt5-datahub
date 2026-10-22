"""Read-only consistency check between stored quote ticks and MT5 M1 OHLC.

This checks two representations served by the same broker, not an independent
market feed and not a mathematical proof of tick-history completeness.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol

import MetaTrader5 as mt5
import numpy as np
from numpy import ndarray

from mt5_datahub.exceptions import MT5RequestError, StorageError
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import from_us, to_us, validate_range

_MINUTE_US = 60_000_000


class CandleReader(Protocol):
    def get_candles(
        self, symbol: str, timeframe: int, start: datetime, end: datetime
    ) -> ndarray: ...


@dataclass(frozen=True, slots=True)
class CandleDifference:
    minute: datetime
    reason: str
    ticks_ohlc: tuple[float, float, float, float] | None
    broker_ohlc: tuple[float, float, float, float] | None


@dataclass(frozen=True, slots=True)
class CandleConsistency:
    symbol: str
    source: str
    checked_minutes: int
    matching_minutes: int
    issue_count: int
    issues: tuple[CandleDifference, ...]
    account_binding: str

    @property
    def consistent(self) -> bool:
        return self.checked_minutes > 0 and self.issue_count == 0

    def format(self) -> str:
        lines = [
            f"Instrument: {self.symbol}",
            f"Source: {self.source}",
            f"Minutes with quote/bar pairs: {self.checked_minutes}",
            f"Matching minute OHLC: {self.matching_minutes}",
            f"Anomalies: {self.issue_count}",
            f"Account binding: {self.account_binding}",
        ]
        for issue in self.issues:
            lines.append(
                f"MISMATCH {issue.minute.isoformat()} [{issue.reason}] "
                f"ticks={issue.ticks_ohlc} bars={issue.broker_ohlc}"
            )
        if len(self.issues) < self.issue_count:
            lines.append(f"... {self.issue_count - len(self.issues)} additional anomalies")
        lines.append(
            "NOTE: broker M1 candles are a cross-check, NOT independent proof "
            "that all historical ticks were ever supplied."
        )
        return "\n".join(lines)


def compare_tick_ohlc_to_m1(
    cache: ParquetCache | str | Path,
    source: str,
    symbol: str,
    start: datetime,
    end: datetime,
    provider: CandleReader,
    *,
    tolerance: float = 0.0,
    window: timedelta = timedelta(hours=1),
    max_reported_issues: int = 20,
) -> CandleConsistency:
    """Compare BID-derived OHLC from cached ticks with MT5 historical M1 bars.

    Uses one hour of M1 history and bounded Parquet tick batches at a time.
    Any interval that was not fetched into the cache is marked as unverified.
    Quote records with identical timestamps are preserved in their stored
    order, allowing stable open/close reconstruction.
    """
    start, end = validate_range(start, end)
    a, z = to_us(start), to_us(end)
    window_us = int(window / timedelta(microseconds=1))
    if not source.strip() or not symbol.strip():
        raise ValueError("source and symbol must not be empty")
    if a % _MINUTE_US or z % _MINUTE_US:
        raise ValueError("start and end must be aligned to UTC M1 boundaries")
    if window_us < _MINUTE_US or window_us % _MINUTE_US:
        raise ValueError("window must be a positive multiple of 1 minute")
    if tolerance < 0 or not np.isfinite(tolerance):
        raise ValueError("tolerance must be finite and nonnegative")
    if max_reported_issues < 0:
        raise ValueError("max_reported_issues must be nonnegative")
    current_open = (to_us(datetime.now(UTC)) // _MINUTE_US) * _MINUTE_US
    if z > current_open:
        raise ValueError("Current open M1 candle cannot be compared")

    storage = cache if isinstance(cache, ParquetCache) else ParquetCache(cache)
    bound = storage.get_source_identity(source)
    identity_fn = getattr(provider, "source_identity", None)
    actual = identity_fn() if callable(identity_fn) else None
    if bound and actual and bound != actual:
        raise StorageError(f"Cache source {source!r} belongs to another MT5 account")
    binding = (
        "legacy-unbound" if bound is None else
        "unknown-live-account" if actual is None else "matched"
    )

    anomalies: list[CandleDifference] = []
    issue_count = checked = matched = 0

    def record(
        minute_us: int, reason: str,
        ticks: tuple[float, float, float, float] | None,
        candle: tuple[float, float, float, float] | None,
    ) -> None:
        nonlocal issue_count
        issue_count += 1
        if len(anomalies) < max_reported_issues:
            anomalies.append(
                CandleDifference(from_us(minute_us), reason, ticks, candle)
            )

    cursor = a
    while cursor < z:
        right = min(cursor + window_us, z)
        if storage.missing(source, symbol, "ticks", 0, cursor, right):
            record(cursor, "uncovered-tick-cache-window", None, None)
            cursor = right
            continue

        # Each minute has [open, high, low, close] reconstructed from BID.
        minute_ohlc: dict[int, list[float]] = {}
        for batch in storage.iter_batches(
            source, symbol, "ticks", 0, cursor, right,
            columns=("time_msc", "bid"), batch_size=65_536,
        ):
            if not len(batch):
                continue
            stamps = batch["time_msc"] // 60_000
            boundaries = np.concatenate((
                np.array([0]),
                np.flatnonzero(np.diff(stamps)) + 1,
                np.array([len(batch)]),
            ))
            for left_index, right_index in zip(boundaries[:-1], boundaries[1:]):
                minute_us = int(stamps[left_index]) * _MINUTE_US
                bids = batch["bid"][left_index:right_index]
                opening, high, low, closing = (
                    float(bids[0]), float(np.max(bids)),
                    float(np.min(bids)), float(bids[-1]),
                )
                current = minute_ohlc.get(minute_us)
                if current is None:
                    minute_ohlc[minute_us] = [opening, high, low, closing]
                else:
                    current[1] = max(current[1], high)
                    current[2] = min(current[2], low)
                    current[3] = closing

        candles = provider.get_candles(
            symbol, mt5.TIMEFRAME_M1, from_us(cursor), from_us(right),
        )
        # MT5 often returns unsigned tick_volume/real_volume. Those fields
        # are irrelevant to an OHLC comparison, so validating the *entire*
        # structured dtype incorrectly rejects otherwise valid live bars.
        required = ("time", "open", "high", "low", "close")
        names = candles.dtype.names or ()
        if any(name not in names for name in required):
            raise MT5RequestError(
                "MT5 M1 candles are missing required time/OHLC columns"
            )
        for name in required:
            kind = candles[name].dtype.kind
            valid = kind in "iu" if name == "time" else kind == "f"
            if not valid:
                raise MT5RequestError(
                    f"MT5 M1 candle field {name!r} has invalid type "
                    f"{candles[name].dtype}"
                )
        candle_map: dict[int, tuple[float, float, float, float]] = {}
        for bar in candles:
            stamp = int(bar["time"]) * 1_000_000
            if cursor <= stamp < right:
                candle_map[stamp] = (
                    float(bar["open"]), float(bar["high"]),
                    float(bar["low"]), float(bar["close"]),
                )
        for minute_us in sorted(set(minute_ohlc) | set(candle_map)):
            tick_prices = minute_ohlc.get(minute_us)
            ohlc = (
                (tick_prices[0], tick_prices[1], tick_prices[2], tick_prices[3])
                if tick_prices is not None else None
            )
            broker = candle_map.get(minute_us)
            if ohlc is None:
                record(minute_us, "broker-bar-without-cached-ticks", None, broker)
                continue
            if broker is None:
                record(minute_us, "cached-ticks-without-broker-bar", ohlc, None)
                continue
            checked += 1
            if np.allclose(ohlc, broker, rtol=0.0, atol=tolerance):
                matched += 1
            else:
                record(minute_us, "ohlc-differs", ohlc, broker)
        cursor = right

    return CandleConsistency(
        symbol=symbol, source=source, checked_minutes=checked,
        matching_minutes=matched, issue_count=issue_count,
        issues=tuple(anomalies), account_binding=binding,
    )

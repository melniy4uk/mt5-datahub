"""Parquet files with a small SQLite manifest of observed data ranges."""

from __future__ import annotations

import logging
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from numpy import ndarray

from mt5_datahub.exceptions import StorageError

logger = logging.getLogger(__name__)

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


@dataclass(frozen=True, slots=True)
class Segment:
    path: str
    start_us: int
    end_us: int


class ParquetCache:
    """Persistent, single-writer cache. SQLite indexes immutable Parquet chunks.

    Intervals mean *observed data*, not guaranteed broker-history completeness.
    Empty responses are deliberately not indexed.
    """

    def __init__(self, directory: str | Path = ".mt5-datahub") -> None:
        self.directory = Path(directory).expanduser().resolve()
        self._data_dir = self.directory / "segments"
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._database = self.directory / "index.sqlite3"
        with self._connect() as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS segments (
                    path TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    timeframe INTEGER NOT NULL,
                    start_us INTEGER NOT NULL,
                    end_us INTEGER NOT NULL,
                    CHECK (start_us < end_us)
                )
            """)
            db.execute("""
                CREATE INDEX IF NOT EXISTS idx_segments_lookup
                ON segments(symbol, kind, timeframe, start_us, end_us)
            """)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._database, timeout=30)

    def segments(
        self, symbol: str, kind: str, timeframe: int, start_us: int, end_us: int
    ) -> list[Segment]:
        with self._connect() as db:
            rows = db.execute(
                """SELECT path, start_us, end_us FROM segments
                   WHERE symbol=? AND kind=? AND timeframe=?
                     AND end_us>? AND start_us<?
                   ORDER BY start_us""",
                (symbol, kind, timeframe, start_us, end_us),
            ).fetchall()
        return [Segment(*row) for row in rows]

    def missing(
        self, symbol: str, kind: str, timeframe: int, start_us: int, end_us: int
    ) -> list[tuple[int, int]]:
        """Subtract observed intervals from [start_us, end_us)."""
        gaps: list[tuple[int, int]] = []
        cursor = start_us
        for segment in self.segments(symbol, kind, timeframe, start_us, end_us):
            if segment.start_us > cursor:
                gaps.append((cursor, min(segment.start_us, end_us)))
            cursor = max(cursor, segment.end_us)
            if cursor >= end_us:
                break
        if cursor < end_us:
            gaps.append((cursor, end_us))
        return gaps

    def save(
        self,
        symbol: str,
        kind: str,
        timeframe: int,
        data: ndarray,
        start_us: int,
        end_us: int,
    ) -> None:
        """Atomically register a new immutable file after a successful write."""
        if len(data) == 0:
            return
        if data.dtype.names is None or start_us >= end_us:
            raise ValueError("Expected a structured NumPy array and a nonempty interval")

        name = uuid4().hex + ".parquet"
        relative = "segments/" + name
        final = self.directory / relative
        temporary = self._data_dir / (name + ".tmp")
        try:
            table = pa.table({field: pa.array(data[field]) for field in data.dtype.names})
            pq.write_table(table, temporary, compression="zstd")
            os.replace(temporary, final)
            with self._connect() as db:
                db.execute(
                    """INSERT INTO segments
                       (path, symbol, kind, timeframe, start_us, end_us)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (relative, symbol, kind, timeframe, start_us, end_us),
                )
            logger.info("Cached %d %s for %s in %s", len(data), kind, symbol, final)
        except Exception as exc:
            temporary.unlink(missing_ok=True)
            final.unlink(missing_ok=True)
            raise StorageError(f"Failed writing cache segment {final}: {exc}") from exc

    def read(
        self, symbol: str, kind: str, timeframe: int, start_us: int, end_us: int
    ) -> ndarray:
        """Read stored rows in time order. Identical-time ticks are preserved."""
        field = "time_msc" if kind == "ticks" else "time"
        scale = 1_000 if kind == "ticks" else 1_000_000
        dtype = TICK_DTYPE if kind == "ticks" else CANDLE_DTYPE
        chunks: list[ndarray] = []

        for segment in self.segments(symbol, kind, timeframe, start_us, end_us):
            file = self.directory / segment.path
            try:
                table = pq.read_table(file)
                columns = {
                    name: table[name].to_numpy(zero_copy_only=False)
                    for name in table.column_names
                }
                current = np.empty(table.num_rows, dtype=[
                    (name, values.dtype) for name, values in columns.items()
                ])
                for name, values in columns.items():
                    current[name] = values
                timestamps = current[field].astype(np.int64) * scale
                selected = current[(timestamps >= start_us) & (timestamps < end_us)]
                if len(selected):
                    chunks.append(selected)
            except Exception as exc:
                raise StorageError(f"Cannot read cache segment {file}: {exc}") from exc

        if not chunks:
            return np.empty(0, dtype=dtype)

        result = np.concatenate(chunks)
        return result[np.argsort(result[field], kind="stable")]

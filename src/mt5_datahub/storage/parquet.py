"""Parquet-backed request cache with SQLite metadata.

A cache hit means a successful request was previously saved, not that the
broker's historical feed was independently verified as complete.
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from numpy import ndarray

from mt5_datahub.arrays import combine, empty, select_range
from mt5_datahub.exceptions import StorageError


class ParquetCache:
    """Cache of successful half-open requests. Use one writer per directory."""

    def __init__(self, directory: str | Path = ".mt5-datahub") -> None:
        self.directory = Path(directory).expanduser().resolve()
        self._chunks = self.directory / "segments"
        self._chunks.mkdir(parents=True, exist_ok=True)
        self._index = self.directory / "index.sqlite3"
        with self._db() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS segments (
                path TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                symbol TEXT NOT NULL,
                kind TEXT NOT NULL,
                timeframe INTEGER NOT NULL,
                start_us INTEGER NOT NULL,
                end_us INTEGER NOT NULL,
                CHECK(start_us < end_us)
            )""")
            db.execute("""CREATE INDEX IF NOT EXISTS segments_query
                ON segments(source,symbol,kind,timeframe,start_us,end_us)""")

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        """Commit or roll back the transaction and always close the connection.

        sqlite3.Connection.__exit__ handles transactions but does NOT close
        the database. On Windows an unclosed connection locks the index file.
        """
        db = sqlite3.connect(self._index, timeout=30)
        try:
            with db:
                yield db
        finally:
            db.close()

    def _segments_for(self, source: str, symbol: str, kind: str, timeframe: int,
                      start: int, end: int) -> list[tuple[str, int, int]]:
        with self._db() as db:
            return db.execute("""SELECT path,start_us,end_us FROM segments
                WHERE source=? AND symbol=? AND kind=? AND timeframe=?
                  AND end_us>? AND start_us<? ORDER BY start_us,path""",
                (source, symbol, kind, timeframe, start, end)).fetchall()

    def missing(self, source: str, symbol: str, kind: str, timeframe: int,
                start: int, end: int) -> list[tuple[int, int]]:
        if start >= end:
            raise ValueError("Invalid interval")
        gaps: list[tuple[int, int]] = []
        cursor = start
        for _, left, right in self._segments_for(source, symbol, kind, timeframe, start, end):
            if left > cursor:
                gaps.append((cursor, min(left, end)))
            cursor = max(cursor, right)
            if cursor >= end:
                break
        if cursor < end:
            gaps.append((cursor, end))
        return gaps

    def save(self, source: str, symbol: str, kind: str, timeframe: int,
             rows: ndarray, start: int, end: int) -> None:
        """Commit an immutable chunk and its index row.

        A failed index transaction can leave an unreferenced file, never a
        reference to a partially written file.
        """
        if start >= end:
            raise ValueError("Invalid interval")
        if rows.dtype.names is None:
            raise ValueError("Expected structured NumPy array")
        selected = select_range(rows, kind, start, end)
        # Empty responses are not cached: the broker may populate them later.
        if len(selected) == 0:
            return
        filename = uuid4().hex + ".parquet"
        relative = "segments/" + filename
        destination = self.directory / relative
        temporary = self._chunks / (filename + ".tmp")
        try:
            table = pa.table({name: pa.array(selected[name]) for name in selected.dtype.names or ()})
            pq.write_table(table, temporary, compression="zstd")
            os.replace(temporary, destination)
            with self._db() as db:
                db.execute("""INSERT INTO segments
                    (path,source,symbol,kind,timeframe,start_us,end_us)
                    VALUES (?,?,?,?,?,?,?)""",
                    (relative, source, symbol, kind, timeframe, start, end))
        except Exception as exc:
            temporary.unlink(missing_ok=True)
            # Do not remove destination: transaction outcome might be uncertain.
            raise StorageError(f"Cannot save Parquet chunk {relative}: {exc}") from exc

    def read(self, source: str, symbol: str, kind: str, timeframe: int,
             start: int, end: int) -> ndarray:
        chunks: list[ndarray] = []
        for filename, _, _ in self._segments_for(source, symbol, kind, timeframe, start, end):
            try:
                table = pq.read_table(self.directory / filename)
                fields = {name: table[name].to_numpy(zero_copy_only=False)
                          for name in table.column_names}
                array = np.empty(table.num_rows, dtype=[
                    (name, values.dtype) for name, values in fields.items()])
                for name, values in fields.items():
                    array[name] = values
                part = select_range(array, kind, start, end)
                if len(part):
                    chunks.append(part)
            except Exception as exc:
                raise StorageError(f"Cannot read Parquet chunk {filename}: {exc}") from exc
        return combine(chunks, kind) if chunks else empty(kind)

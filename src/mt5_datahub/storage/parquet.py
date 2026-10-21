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

from mt5_datahub.arrays import combine, empty, select_range, TICK_DTYPE, CANDLE_DTYPE
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

    @staticmethod
    def _array(table: pa.Table | pa.RecordBatch) -> ndarray:
        fields = {
            name: table.column(i).to_numpy(zero_copy_only=False)
            for i, name in enumerate(table.schema.names)
        }
        result = np.empty(table.num_rows, dtype=[
            (name, values.dtype) for name, values in fields.items()
        ])
        for name, values in fields.items():
            result[name] = values
        return result

    def iter_batches(
        self, source: str, symbol: str, kind: str, timeframe: int,
        start: int, end: int, *,
        columns: tuple[str, ...] | None = None,
        batch_size: int = 65536,
    ) -> Iterator[ndarray]:
        """Read bounded NumPy batches; iteration follows stored file order.

        Files are traversed by interval, then path. The caller may sort
        across batches if overlapping files were written separately.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        dtype = TICK_DTYPE if kind == "ticks" else CANDLE_DTYPE
        names = tuple(dtype.names or ())
        fields = names if columns is None else tuple(columns)
        if not fields or len(set(fields)) != len(fields) or any(x not in names for x in fields):
            raise ValueError("Unknown or duplicated requested column")
        timestamp = "time_msc" if kind == "ticks" else "time"
        scale = 1000 if kind == "ticks" else 1000000
        projection = tuple(dict.fromkeys((*fields, timestamp)))
        for filename, _, _ in self._segments_for(source, symbol, kind, timeframe, start, end):
            try:
                with pq.ParquetFile(self.directory / filename) as file:
                    for batch in file.iter_batches(batch_size=batch_size, columns=list(projection)):
                        rows = self._array(batch)
                        selected = select_range(rows, kind, start, end)
                        if len(selected):
                            yield selected[list(fields)]
            except Exception as exc:
                raise StorageError(f"Cannot stream Parquet chunk {filename}: {exc}") from exc

    def read(
        self, source: str, symbol: str, kind: str, timeframe: int,
        start: int, end: int, *,
        columns: tuple[str, ...] | None = None,
    ) -> ndarray:
        """Read selected rows and columns; materializes the full result."""
        chunks = list(self.iter_batches(
            source, symbol, kind, timeframe, start, end, columns=columns
        ))
        if chunks:
            # Sorting is stable; two different ticks at the same time survive.
            field = "time_msc" if kind == "ticks" else "time"
            if field in (chunks[0].dtype.names or ()):
                return combine(chunks, kind)
            return np.concatenate(chunks)
        schema = TICK_DTYPE if kind == "ticks" else CANDLE_DTYPE
        fields = schema.names if columns is None else columns
        return np.empty(0, dtype=np.dtype([(name, schema.fields[name][0]) for name in fields]))

    def compact(
        self, source: str, symbol: str, kind: str, timeframe: int,
        start: int, end: int, *, max_rows: int = 2_000_000,
    ) -> int:
        """Merge nonoverlapping stored segments into one, transactionally.

        Skips overlapping segments to avoid accidentally duplicating ticks.
        Old files are unlinked only after SQLite commits the replacement.
        """
        segments = self._segments_for(source, symbol, kind, timeframe, start, end)
        if len(segments) < 2:
            return 0
        if any(a[2] != b[1] for a, b in zip(segments, segments[1:])):
            raise StorageError("Only adjacent, nonoverlapping segments can be compacted")
        # Only compact fully enclosed segments; never absorb outside the scope.
        if any(left < start or right > end for _, left, right in segments):
            raise StorageError("Compaction range must contain complete segments")
        rowcounts = [
            pq.read_metadata(self.directory / filename).num_rows
            for filename, _, _ in segments
        ]
        if sum(rowcounts) > max_rows:
            raise StorageError("Compaction exceeds max_rows; select a shorter range")
        rows = self.read(source, symbol, kind, timeframe, segments[0][1], segments[-1][2])
        filename = "segments/" + uuid4().hex + ".parquet"
        destination = self.directory / filename
        temporary = destination.with_suffix(".tmp")
        try:
            pq.write_table(
                pa.table({name: pa.array(rows[name]) for name in rows.dtype.names or ()}),
                temporary, compression="zstd"
            )
            os.replace(temporary, destination)
            with self._db() as db:
                for path, left, right in segments:
                    deleted = db.execute(
                        "DELETE FROM segments WHERE path=? AND source=? AND symbol=?",
                        (path, source, symbol)
                    ).rowcount
                    if deleted != 1:
                        raise StorageError("Index changed during compaction")
                db.execute("""INSERT INTO segments
                    (path,source,symbol,kind,timeframe,start_us,end_us)
                    VALUES (?,?,?,?,?,?,?)""",
                    (filename, source, symbol, kind, timeframe, segments[0][1], segments[-1][2])
                )
        except Exception as exc:
            temporary.unlink(missing_ok=True)
            # Do not remove final file if index commit state is unknown.
            raise StorageError(f"Compaction failed: {exc}") from exc
        for path, _, _ in segments:
            (self.directory / path).unlink(missing_ok=True)
        return len(segments) - 1

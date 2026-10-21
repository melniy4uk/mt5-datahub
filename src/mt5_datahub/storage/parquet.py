"""Single-writer immutable Parquet cache with SQLite interval index."""
import os
import sqlite3
from pathlib import Path
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from numpy import ndarray

from mt5_datahub.arrays import combine, empty, select_range
from mt5_datahub.exceptions import StorageError


class ParquetCache:
    def __init__(self, directory: str | Path = ".mt5-datahub") -> None:
        self.directory = Path(directory).expanduser().resolve()
        (self.directory / "segments").mkdir(parents=True, exist_ok=True)
        self.database = self.directory / "index.sqlite3"
        with self._db() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS segments (
                path TEXT PRIMARY KEY, source TEXT NOT NULL, symbol TEXT NOT NULL,
                kind TEXT NOT NULL, timeframe INTEGER NOT NULL,
                start_us INTEGER NOT NULL, end_us INTEGER NOT NULL)""")
            db.execute("""CREATE INDEX IF NOT EXISTS idx_segments ON segments
                (source, symbol, kind, timeframe, start_us, end_us)""")

    def _db(self) -> sqlite3.Connection:
        return sqlite3.connect(self.database, timeout=30)

    def _files(self, source: str, symbol: str, kind: str, timeframe: int,
               start_us: int, end_us: int) -> list[tuple[str, int, int]]:
        with self._db() as db:
            return db.execute("""SELECT path,start_us,end_us FROM segments
                WHERE source=? AND symbol=? AND kind=? AND timeframe=?
                AND end_us>? AND start_us<? ORDER BY start_us""",
                (source, symbol, kind, timeframe, start_us, end_us)).fetchall()

    def missing(self, source: str, symbol: str, kind: str, timeframe: int,
                start_us: int, end_us: int) -> list[tuple[int, int]]:
        cursor = start_us
        gaps = []
        for _, left, right in self._files(source, symbol, kind, timeframe, start_us, end_us):
            if left > cursor:
                gaps.append((cursor, min(left, end_us)))
            cursor = max(cursor, right)
        if cursor < end_us:
            gaps.append((cursor, end_us))
        return gaps

    def save(self, source: str, symbol: str, kind: str, timeframe: int,
             data: ndarray, start_us: int, end_us: int) -> None:
        data = select_range(data, kind, start_us, end_us)
        if len(data) == 0:
            return
        filename = "segments/" + uuid4().hex + ".parquet"
        path = self.directory / filename
        temp = path.with_suffix(".tmp")
        try:
            pq.write_table(pa.table({name: pa.array(data[name])
                for name in data.dtype.names or ()}), temp, compression="zstd")
            os.replace(temp, path)
            with self._db() as db:
                db.execute("""INSERT INTO segments VALUES (?,?,?,?,?,?,?)""",
                    (filename, source, symbol, kind, timeframe, start_us, end_us))
        except Exception as exc:
            temp.unlink(missing_ok=True)
            raise StorageError(f"Cache write failed: {exc}") from exc

    def read(self, source: str, symbol: str, kind: str, timeframe: int,
             start_us: int, end_us: int) -> ndarray:
        chunks = []
        for filename, _, _ in self._files(source, symbol, kind, timeframe, start_us, end_us):
            try:
                table = pq.read_table(self.directory / filename)
                columns = {name: table[name].to_numpy(zero_copy_only=False)
                           for name in table.column_names}
                rows = np.empty(table.num_rows, dtype=[
                    (name, values.dtype) for name, values in columns.items()])
                for name, values in columns.items():
                    rows[name] = values
                selected = select_range(rows, kind, start_us, end_us)
                if len(selected):
                    chunks.append(selected)
            except Exception as exc:
                raise StorageError(f"Cache read failed for {filename}: {exc}") from exc
        return combine(chunks, kind) if chunks else empty(kind)

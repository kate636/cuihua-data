from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from dataclasses import dataclass
from typing import Iterator, Sequence
import uuid

import duckdb
import pandas as pd


@dataclass(frozen=True)
class PartitionWrite:
    table: str
    frame: pd.DataFrame
    partition_columns: Sequence[str]
    partition_values: Sequence[object]


class DuckDBStore:
    def __init__(self, path: Path | str):
        self.path = str(path)
        self.connection = duckdb.connect(self.path)

    def close(self) -> None:
        self.connection.close()

    @contextmanager
    def transaction(self) -> Iterator[duckdb.DuckDBPyConnection]:
        self.connection.execute("BEGIN")
        try:
            yield self.connection
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    def table_exists(self, table: str) -> bool:
        row = self.connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table],
        ).fetchone()
        return bool(row and row[0])

    def replace_partition(
        self,
        table: str,
        frame: pd.DataFrame,
        partition_columns: Sequence[str],
        partition_values: Sequence[object],
    ) -> None:
        write = PartitionWrite(table, frame, partition_columns, partition_values)
        self.replace_partitions_atomic([write])

    @staticmethod
    def _validate_partition(write: PartitionWrite) -> None:
        if len(write.partition_columns) != len(write.partition_values):
            raise ValueError("partition columns and values differ in length")
        missing = sorted(set(write.partition_columns) - set(write.frame.columns))
        if missing:
            raise ValueError(f"{write.table}: frame missing partition columns {missing}")
        if write.frame.empty:
            return
        for column, expected in zip(write.partition_columns, write.partition_values):
            values = write.frame[column]
            if values.isna().any():
                raise ValueError(f"{write.table}: partition column {column} contains NULL")
            unexpected = values.loc[values.ne(expected)].unique().tolist()
            if unexpected:
                raise ValueError(
                    f"{write.table}: frame contains values outside {column}={expected!r}: {unexpected[:10]}"
                )

    def _replace_in_transaction(
        self,
        conn: duckdb.DuckDBPyConnection,
        write: PartitionWrite,
        registered: str,
    ) -> None:
        table = write.table
        stage = f"_stage_{table}_{uuid.uuid4().hex[:10]}"
        conn.execute(f'CREATE TEMP TABLE "{stage}" AS SELECT * FROM "{registered}"')
        if not self.table_exists(table):
            if write.frame.empty:
                raise ValueError(f"{table}: cannot infer a new table schema from a legal-empty partition")
            conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM "{stage}"')
        else:
            target_columns = [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]
            if list(write.frame.columns) != target_columns:
                raise ValueError(
                    f"schema mismatch for {table}: target={target_columns}, frame={list(write.frame.columns)}"
                )
            predicate = " AND ".join(f'"{column}" = ?' for column in write.partition_columns)
            conn.execute(f'DELETE FROM "{table}" WHERE {predicate}', list(write.partition_values))
            if len(write.frame):
                conn.execute(f'INSERT INTO "{table}" BY NAME SELECT * FROM "{stage}"')
        conn.execute(f'DROP TABLE "{stage}"')

    def replace_partitions_atomic(self, writes: Sequence[PartitionWrite]) -> None:
        """Publish multiple validated partitions in one DuckDB transaction."""
        if not writes:
            raise ValueError("at least one partition write is required")
        for write in writes:
            self._validate_partition(write)
        registered: list[str] = []
        try:
            for write in writes:
                name = f"_df_{uuid.uuid4().hex[:10]}"
                self.connection.register(name, write.frame)
                registered.append(name)
            with self.transaction() as conn:
                for write, name in zip(writes, registered):
                    self._replace_in_transaction(conn, write, name)
        finally:
            for name in registered:
                self.connection.unregister(name)

"""
DuckDB 存储层

本地单文件数据库：
  - 本地开发：默认 <repo>/data/fm.duckdb
  - 云端生产：/opt/fm/data/fm.duckdb（由 FM_DUCKDB_PATH 指定）

设计要点：
  - 同一进程内共享同一 connection（DuckDB 进程内不支持多连接同时写）
  - 提供 load_df / query / execute / table_exists 等便捷方法
  - 分区追加写入：写入前先删除相同日期分区的旧数据
"""

from __future__ import annotations

import duckdb
import pandas as pd
from pathlib import Path
from typing import Optional

from ..config import get_settings
from ..utils import get_logger

_log = get_logger("duckdb")


class DuckDBStore:
    """进程内单例 DuckDB 连接封装（ETL 写入侧）。"""

    def __init__(self, conn_str: Optional[str] = None):
        cfg = get_settings()
        self._conn_str = conn_str or cfg.duckdb_conn_str
        Path(self._conn_str).parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(self._conn_str)
        self._conn.execute("SET threads TO 8")
        self._conn.execute("SET memory_limit = '12GB'")
        _log.info(f"DuckDB connected: {self._conn_str}")

    def close(self) -> None:
        self._conn.close()

    # ── 写入 ──────────────────────────────────────────────────────────────────
    def load_df(
        self,
        df: pd.DataFrame,
        table: str,
        date_col: str = "business_date",
        start: Optional[str] = None,
        end: Optional[str] = None,
        mode: str = "replace_partition",  # "replace_partition" | "replace" | "append"
    ) -> None:
        """
        将 DataFrame 写入 DuckDB 表。

        mode:
          - replace_partition: 先删除 [start, end] 的分区行，再 INSERT
          - replace: DROP 整表再建
          - append: 直接 INSERT
        """
        if df.empty:
            _log.debug(f"load_df: empty df, skip {table}")
            return

        if mode == "replace":
            self._conn.execute(f"DROP TABLE IF EXISTS {table}")
            self._conn.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
        elif mode == "replace_partition":
            if not self.table_exists(table):
                self._conn.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
            else:
                if start and end and date_col:
                    self._conn.execute(
                        f"DELETE FROM {table} WHERE {date_col} BETWEEN '{start}' AND '{end}'"
                    )
                self._conn.execute(f"INSERT INTO {table} SELECT * FROM df")
        else:  # append
            if not self.table_exists(table):
                self._conn.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
            else:
                self._conn.execute(f"INSERT INTO {table} SELECT * FROM df")

        _log.debug(f"load_df: {len(df)} rows → {table} (mode={mode})")

    # ── 查询 ──────────────────────────────────────────────────────────────────
    def query(self, sql: str) -> pd.DataFrame:
        """执行 SQL，返回 DataFrame。"""
        return self._conn.execute(sql).df()

    def execute(self, sql: str) -> None:
        """执行非查询 SQL（CREATE / DROP / INSERT 等）。"""
        self._conn.execute(sql)

    def table_exists(self, table: str) -> bool:
        schema, _, tbl = table.rpartition(".")
        schema = schema or "main"
        r = self._conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, tbl],
        ).fetchone()
        return bool(r and r[0] > 0)

    def row_count(self, table: str) -> int:
        if not self.table_exists(table):
            return 0
        return self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    # ── 导出 ─────────────────────────────────────────────────────────────────
    def to_df(self, table: str) -> pd.DataFrame:
        return self.query(f"SELECT * FROM {table}")

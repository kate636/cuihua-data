"""原子层提取器基类。"""

from __future__ import annotations

from abc import ABC, abstractmethod

from ..connectors import ApiConnector, DuckDBStore
from ..utils import get_logger, split_date_range


class BaseExtractor(ABC):
    """
    所有原子域提取器的基类。

    子类需实现：
      - TARGET_TABLE: DuckDB 落地表名
      - _fetch_sql(start, end, yesterday): 返回 StarRocks 查询 SQL
    """

    TARGET_TABLE: str = ""

    def __init__(self, sr: ApiConnector, duck: DuckDBStore):
        self._sr = sr
        self._duck = duck
        self._log = get_logger(self.__class__.__name__)

    @abstractmethod
    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        """返回从 StarRocks 提取原子数据的 SQL。"""

    def extract(
        self,
        start: str,
        end: str,
        yesterday: str,
        chunk: int = 7,
    ) -> None:
        """
        按 chunk 天分批从 StarRocks 提取，写入 DuckDB TARGET_TABLE。
        已有数据的日期分区会被先删后插（replace_partition 模式）。
        """
        segments = split_date_range(start, end, chunk)
        self._log.info(
            f"extracting {self.TARGET_TABLE}: {start}~{end} in {len(segments)} segments"
        )
        for seg_start, seg_end in segments:
            sql = self._fetch_sql(seg_start, seg_end, yesterday)
            df = self._sr.query(sql)
            self._log.info(f"  [{seg_start}~{seg_end}] fetched {len(df)} rows")
            if df.empty:
                continue
            self._duck.load_df(
                df,
                self.TARGET_TABLE,
                date_col="business_date",
                start=seg_start,
                end=seg_end,
                mode="replace_partition",
            )
        self._log.info(
            f"extract done: {self.TARGET_TABLE} total rows = "
            f"{self._duck.row_count(self.TARGET_TABLE)}"
        )

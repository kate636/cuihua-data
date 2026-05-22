"""
域③附 差异调整域取数器

源表: strategy_fm_scm_adjust_di  (测试期 0 行，未来上游写入后自动生效)
目标: DuckDB atomic_scm_adjust

说明:
- `strategy_fm_scm_di.adjustment_amt` 已经是合并好的总调整金额，下游 AtomicMerger 直接使用它。
- 本表仅作为独立审计/明细保留，不参与 AtomicMerger 合并。
- v4: 源表缺失或空时自动降级为空表骨架，保证下游 JOIN 不报错。
"""

from ._base import BaseExtractor


class ScmAdjustExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_scm_adjust"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        return f"""
        SELECT
            store_id,
            business_date,
            article_id,
            dc_id,
            matnr,
            tax,
            new_sp_store_id,
            SUM(COALESCE(adjustment_amt, 0))        AS adjustment_amt,
            SUM(COALESCE(adjustment_amt_notax, 0))  AS adjustment_amt_notax
        FROM strategy_fm_scm_adjust_di
        WHERE inc_day BETWEEN '{start}' AND '{end}'
        GROUP BY
            store_id,
            business_date,
            article_id,
            dc_id,
            matnr,
            tax,
            new_sp_store_id
        """

    def extract(self, start: str, end: str, yesterday: str, chunk: int = 7) -> None:
        """容错版 extract：源表缺失/空时落空表骨架。"""
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
        try:
            super().extract(start=start, end=end, yesterday=yesterday, chunk=chunk)
        except RuntimeError as e:
            msg = str(e).lower()
            if "unknown table" in msg and "scm_adjust" in msg:
                self._log.warning(
                    "⚠ strategy_fm_scm_adjust_di 未落库，降级为空表。"
                )
            else:
                raise

        if not self._duck.table_exists(self.TARGET_TABLE):
            self._duck.execute(f"""
                CREATE TABLE {self.TARGET_TABLE} (
                    store_id             VARCHAR,
                    business_date        VARCHAR,
                    article_id           VARCHAR,
                    dc_id                VARCHAR,
                    matnr                VARCHAR,
                    tax                  DOUBLE,
                    new_sp_store_id      VARCHAR,
                    adjustment_amt       DOUBLE,
                    adjustment_amt_notax DOUBLE
                )
            """)
            self._log.info(f"{self.TARGET_TABLE}: fallback empty (0 rows)")

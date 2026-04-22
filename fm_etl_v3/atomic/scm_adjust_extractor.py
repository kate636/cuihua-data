"""
域③附 差异调整域取数器 (新增)

源表: strategy_fm_scm_adjust_di  (当前 0 行，未来上游写入后自动生效)
目标: DuckDB atomic_scm_adjust

说明:
- `strategy_fm_scm_di.adjustment_amt` 已经是合并好的总调整金额，下游 AtomicMerger 直接使用它。
- 本表仅作为独立审计/明细保留，不参与 AtomicMerger 合并。
- 空 DataFrame 会被 BaseExtractor.load_df 静默跳过。
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

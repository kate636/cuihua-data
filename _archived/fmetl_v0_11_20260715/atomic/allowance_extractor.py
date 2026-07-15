"""
域⑥ 补贴域取数器

源表: strategy_fm_allowance_di  (理论上只含翠花门店，已聚合但可能每 SKU 多条规则/活动)
目标: DuckDB atomic_allowance
原子字段: allowance_amt (系统拆分后直接记录)

说明:
- 老 extractor INNER JOIN `dim_chdj_store_list_di` 做门店过滤；新表已上游过滤，下游
  `AtomicMerger` 也会 `INNER JOIN chdj_stores` 再过滤一次，故此处不再 JOIN。
"""

from ._base import BaseExtractor


class AllowanceExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_allowance"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        return f"""
        SELECT
            t1.store_id,
            t1.business_date,
            t1.sale_article_id          AS article_id,
            SUM(COALESCE(t1.split_allowance_amt, 0)) AS allowance_amt
        FROM strategy_fm_allowance_di t1
        WHERE t1.inc_day BETWEEN '{start}' AND '{end}'
        GROUP BY
            t1.store_id,
            t1.business_date,
            t1.sale_article_id
        """

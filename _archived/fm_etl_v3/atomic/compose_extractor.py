"""
域⑤ 加工转换域取数器

源表: strategy_fm_compose_di  (已聚合，字段与 atomic_compose 完全对齐)
目标: DuckDB atomic_compose
原子字段: compose_in_qty, compose_out_qty (只有数量)
"""

from ._base import BaseExtractor


class ComposeExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_compose"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        return f"""
        SELECT
            store_id,
            business_date,
            article_id,
            SUM(COALESCE(compose_in_qty, 0))    AS compose_in_qty,
            SUM(COALESCE(compose_out_qty, 0))   AS compose_out_qty
        FROM strategy_fm_compose_di
        WHERE inc_day BETWEEN '{start}' AND '{end}'
        GROUP BY
            store_id,
            business_date,
            article_id
        """

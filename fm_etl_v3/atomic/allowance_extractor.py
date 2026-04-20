"""
域⑥ 补贴域取数器

源表: hive.dal.dal_activity_article_order_sale_info_di
目标: DuckDB atomic_allowance
原子字段: allowance_amt (系统拆分后直接记录)
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
            SUM(t1.split_allowance_amt) AS allowance_amt
        FROM hive.dal.dal_activity_article_order_sale_info_di t1
        INNER JOIN (
            SELECT store_id, inc_day
            FROM hive.dim.dim_chdj_store_list_di
            WHERE inc_day BETWEEN '{start}' AND '{end}'
        ) stores ON t1.store_id = stores.store_id AND t1.inc_day = stores.inc_day
        WHERE t1.inc_day BETWEEN '{start}' AND '{end}'
        GROUP BY
            t1.store_id,
            t1.business_date,
            t1.sale_article_id
        """

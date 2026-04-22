"""
域② 进货库存域取数器

源表: strategy_fm_purchase_di  (已聚合到 store × article × business_date × day_clear)
辅表: strategy_fm_dim_goods    (category_level1_id 排除物料类)
目标: DuckDB atomic_inventory
原子字段: receive_qty, init_stock_qty, end_stock_qty (数量)
"""

from ._base import BaseExtractor


class InventoryExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_inventory"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        mat_excl = "('70','71','72','73','74','75','76','77')"
        return f"""
        SELECT
            m1.store_id,
            m1.business_date,
            m1.sale_article_id          AS article_id,
            SUM(m1.sale_article_qty)    AS receive_qty,
            SUM(m1.init_stock_qty)      AS init_stock_qty,
            SUM(m1.end_stock_qty)       AS end_stock_qty,
            m1.day_clear
        FROM (
            SELECT
                business_date,
                store_id,
                sale_article_id,
                sale_article_qty,
                init_stock_qty,
                end_stock_qty,
                day_clear
            FROM strategy_fm_purchase_di
            WHERE inc_day BETWEEN '{start}' AND '{end}'
        ) m1
        LEFT JOIN (
            SELECT article_id, category_level1_id
            FROM strategy_fm_dim_goods
        ) m2 ON m1.sale_article_id = m2.article_id
        WHERE m2.category_level1_id NOT IN {mat_excl}
        GROUP BY
            m1.store_id,
            m1.business_date,
            m1.sale_article_id,
            m1.day_clear
        """

"""
域⑧ 成本价域取数器

源表: hive.ods_sc_db.t_shop_inventory_sku_pool
目标: DuckDB atomic_cost_price
原子字段: cost_price (进货成本价)

注意: 此表以 inventory_date 为业务日期，需按日期范围筛选。
"""

from ._base import BaseExtractor


class CostPriceExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_cost_price"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        return f"""
        SELECT
            shop_id                 AS store_id,
            inventory_date          AS business_date,
            sku_code                AS article_id,
            MAX(cost_price)         AS cost_price
        FROM hive.ods_sc_db.t_shop_inventory_sku_pool
        WHERE inc_day = '{yesterday}'
          AND inventory_date BETWEEN '{start}' AND '{end}'
        GROUP BY
            shop_id,
            inventory_date,
            sku_code
        """

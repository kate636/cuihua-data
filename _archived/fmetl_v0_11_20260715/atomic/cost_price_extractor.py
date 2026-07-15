"""
域⑧ 成本价域取数器

源表: strategy_fm_inventory_pool_di
目标: DuckDB atomic_cost_price
原子字段: cost_price (进货成本价)

说明:
- 新表 `inc_day` 只保留一天（= 最新快照日），故不再按 inc_day 过滤；
  `inventory_date` 作为业务日期继续做 `BETWEEN start AND end`。
- 字段映射：shop_id → store_id，sku_code → article_id，inventory_date → business_date。
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
        FROM strategy_fm_inventory_pool_di
        WHERE inventory_date BETWEEN '{start}' AND '{end}'
        GROUP BY
            shop_id,
            inventory_date,
            sku_code
        """

"""
域⑨ 价格域取数器

源表: strategy_fm_price_da
目标: DuckDB atomic_price
原子字段: current_price, yesterday_price, dc_original_price, original_price

说明:
- 新表没有 business_date 字段，用 `inc_day AS business_date`。
- 当前上游只保留最新一天快照；跑历史日期时若 start/end 不含 `inc_day`，这张表会空。
  TODO: 等 `strategy_fm_price_da` 保留多天历史后去掉这里的妥协。
- 字段映射：shop_id → store_id，sku_code → article_id。
"""

from ._base import BaseExtractor


class PriceExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_price"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        return f"""
        SELECT
            shop_id                         AS store_id,
            inc_day                         AS business_date,
            sku_code                        AS article_id,
            COALESCE(current_price, 0)      AS current_price,
            COALESCE(yesterday_price, 0)    AS yesterday_price,
            COALESCE(dc_original_price, 0)  AS dc_original_price,
            COALESCE(original_price, 0)     AS original_price
        FROM strategy_fm_price_da
        WHERE inc_day BETWEEN '{start}' AND '{end}'
        """

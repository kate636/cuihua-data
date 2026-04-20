"""
域⑨ 价格域取数器

源表: hive.dim.dim_store_article_price_info_da
目标: DuckDB atomic_price
原子字段: current_price, yesterday_price, dc_original_price, original_price
"""

from ._base import BaseExtractor


class PriceExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_price"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        return f"""
        SELECT
            store_id,
            business_date,
            article_id,
            COALESCE(current_price, 0)      AS current_price,
            COALESCE(yesterday_price, 0)    AS yesterday_price,
            COALESCE(dc_original_price, 0)  AS dc_original_price,
            COALESCE(original_price, 0)     AS original_price
        FROM hive.dim.dim_store_article_price_info_da
        WHERE business_date BETWEEN '{start}' AND '{end}'
        """

"""
域⑨ 价格域取数器

从价格表提取价格相关的原子数据
- 源表: dim.dim_store_article_price_info_da
- 原子字段: current_price, yesterday_price, dc_original_price, original_price
"""

from typing import List
import pandas as pd

from .base_extractor import BaseExtractor


class PriceExtractor(BaseExtractor):
    """
    价格域取数器

    提取销售价格相关字段
    """

    @property
    def domain_name(self) -> str:
        return "price"

    @property
    def atomic_fields(self) -> List[str]:
        """价格域原子字段列表"""
        return [
            "current_price",       # 今日销售价格
            "yesterday_price",     # 昨日销售价格
            "dc_original_price",   # 出库原价
            "original_price",      # 销售原价
        ]

    def get_sql(self, start_date: str, end_date: str) -> str:
        """生成价格域取数 SQL"""
        return f"""
        SELECT
            store_id,
            business_date,
            article_id,
            COALESCE(current_price, 0) AS current_price,
            COALESCE(yesterday_price, 0) AS yesterday_price,
            COALESCE(dc_original_price, 0) AS dc_original_price,
            COALESCE(original_price, 0) AS original_price
        FROM hive.dim.dim_store_article_price_info_da
        WHERE business_date BETWEEN '{start_date}' AND '{end_date}'
        """

    def _post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """后处理：数据类型转换"""
        numeric_cols = self.atomic_fields
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df

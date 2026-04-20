"""
域② 进货库存域取数器

从进货库存表提取进货和库存相关的原子数据
- 源表: dsl.dsl_transaction_non_daily_store_article_purchase_di
- 原子字段: receive_qty, init_stock_qty, end_stock_qty (只有数量)
"""

from typing import List
import pandas as pd

from .base_extractor import BaseExtractor


class PurchaseExtractor(BaseExtractor):
    """
    进货库存域取数器

    只提取数量字段，金额字段移入计算层
    """

    @property
    def domain_name(self) -> str:
        return "purchase"

    @property
    def atomic_fields(self) -> List[str]:
        """进货库存域原子字段列表"""
        return [
            # 数量类 (只有数量是原子)
            "receive_qty",       # 进货数量 (BOM拆分后)
            "init_stock_qty",    # 期初库存数量
            "end_stock_qty",     # 期末库存数量 (有盘点时为原子，无盘点时为计算)
        ]

    def get_sql(self, start_date: str, end_date: str) -> str:
        """生成进货库存域取数 SQL"""
        return f"""
        SELECT
            store_id,
            business_date,
            article_id,
            -- 进货数量 (BOM 拆分后)
            COALESCE(sale_article_qty, 0) AS receive_qty,
            -- 期初库存数量
            COALESCE(init_stock_qty, 0) AS init_stock_qty,
            -- 期末库存数量
            COALESCE(end_stock_qty, 0) AS end_stock_qty
        FROM hive.dsl.dsl_transaction_non_daily_store_article_purchase_di
        WHERE business_date BETWEEN '{start_date}' AND '{end_date}'
        """

    def _post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """后处理：数据类型转换"""
        numeric_cols = self.atomic_fields
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df

"""
域④ 损耗域取数器

从损耗表提取损耗相关的原子数据
- 源表: dal.dal_transaction_store_article_lost_di
- 原子字段: know_lost_qty (只有已知损耗数量是原子)
"""

from typing import List
import pandas as pd

from .base_extractor import BaseExtractor


class LossExtractor(BaseExtractor):
    """
    损耗域取数器

    只提取已知损耗数量，未知损耗由库存方程反推
    """

    @property
    def domain_name(self) -> str:
        return "loss"

    @property
    def atomic_fields(self) -> List[str]:
        """损耗域原子字段列表"""
        return [
            "know_lost_qty",  # 已知损耗数量 (门店报损)
        ]

    def get_sql(self, start_date: str, end_date: str) -> str:
        """生成损耗域取数 SQL"""
        return f"""
        SELECT
            store_id,
            business_date,
            article_id,
            COALESCE(know_lost_qty, 0) AS know_lost_qty
        FROM hive.dal.dal_transaction_store_article_lost_di
        WHERE business_date BETWEEN '{start_date}' AND '{end_date}'
        """

    def _post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """后处理：数据类型转换"""
        numeric_cols = self.atomic_fields
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df

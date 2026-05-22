"""
域⑥ 补贴域取数器

从补贴表提取补贴相关的原子数据
- 源表: dal.dal_activity_article_order_sale_info_di
- 原子字段: allowance_amt (补贴金额)
"""

from typing import List
import pandas as pd

from .base_extractor import BaseExtractor


class SubsidyExtractor(BaseExtractor):
    """
    补贴域取数器

    提取补贴金额，系统拆分后直接记录
    """

    @property
    def domain_name(self) -> str:
        return "subsidy"

    @property
    def atomic_fields(self) -> List[str]:
        """补贴域原子字段列表"""
        return [
            "allowance_amt",  # 补贴金额 (系统拆分后直接记录)
        ]

    def get_sql(self, start_date: str, end_date: str) -> str:
        """生成补贴域取数 SQL"""
        return f"""
        SELECT
            store_id,
            business_date,
            article_id,
            COALESCE(allowance_amt, 0) AS allowance_amt
        FROM hive.dal.dal_activity_article_order_sale_info_di
        WHERE business_date BETWEEN '{start_date}' AND '{end_date}'
        """

    def _post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """后处理：数据类型转换"""
        numeric_cols = self.atomic_fields
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df

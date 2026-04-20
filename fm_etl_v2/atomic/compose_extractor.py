"""
域⑤ 加工转换域取数器

从加工转换表提取加工转入/转出的原子数据
- 源表: dsl.dsl_transaction_sotre_article_compose_info_di
- 原子字段: compose_in_qty, compose_out_qty (只有数量)
"""

from typing import List
import pandas as pd

from .base_extractor import BaseExtractor


class ComposeExtractor(BaseExtractor):
    """
    加工转换域取数器

    只提取数量字段，金额字段移入计算层
    compose_in_qty: 加工转入数量 (成品被生产出来)
    compose_out_qty: 加工转出数量 (原材料被消耗)
    """

    @property
    def domain_name(self) -> str:
        return "compose"

    @property
    def atomic_fields(self) -> List[str]:
        """加工转换域原子字段列表"""
        return [
            "compose_in_qty",   # 加工转入数量 (成品被生产出来)
            "compose_out_qty",  # 加工转出数量 (原材料被消耗)
        ]

    def get_sql(self, start_date: str, end_date: str) -> str:
        """生成加工转换域取数 SQL"""
        return f"""
        SELECT
            store_id,
            business_date,
            article_id,
            COALESCE(compose_in_qty, 0) AS compose_in_qty,
            COALESCE(compose_out_qty, 0) AS compose_out_qty
        FROM hive.dsl.dsl_transaction_sotre_article_compose_info_di
        WHERE business_date BETWEEN '{start_date}' AND '{end_date}'
        """

    def _post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """后处理：数据类型转换"""
        numeric_cols = self.atomic_fields
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df

"""
域⑩ 盘点域取数器

从盘点表提取盘点相关的原子数据
- 源表: 待确认
- 原子字段: physical_count_qty (物理盘点/系统实盘数量)

当前实现：返回空数据，待确认源表后补充
"""

from typing import List
import pandas as pd

from .base_extractor import BaseExtractor


class InventoryExtractor(BaseExtractor):
    """
    盘点域取数器

    提取物理盘点数量，用于打破库存-损耗循环

    盘点优先策略：
    - 有盘点数据 → end_stock_qty = physical_count_qty (原子①)
    - 无盘点数据 → end_stock_qty 由库存方程推算 (计算②)
    """

    @property
    def domain_name(self) -> str:
        return "inventory"

    @property
    def atomic_fields(self) -> List[str]:
        """盘点域原子字段列表"""
        return [
            "physical_count_qty",  # 物理盘点/系统实盘数量
        ]

    def get_sql(self, start_date: str, end_date: str) -> str:
        """生成盘点域取数 SQL

        当前返回空数据，待确认源表后补充
        """
        # TODO: 待确认盘点数据源表
        # 返回空结果
        return f"""
        SELECT
            '' AS store_id,
            '{start_date}' AS business_date,
            '' AS article_id,
            0 AS physical_count_qty
        WHERE 1 = 0
        """

    def _post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """后处理：数据类型转换"""
        numeric_cols = self.atomic_fields
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df

    def extract(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        执行取数并返回 DataFrame

        重写此方法以处理空数据情况
        """
        try:
            return super().extract(start_date, end_date)
        except Exception:
            # 如果取数失败，返回空 DataFrame
            self.logger.warning("Inventory data source not configured, returning empty DataFrame")
            return pd.DataFrame(columns=self.key_fields + self.atomic_fields)

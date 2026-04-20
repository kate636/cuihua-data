"""
域⑧ 成本价域取数器

从成本价表提取成本价相关的原子数据
- 源表: ods_sc_db.t_shop_inventory_sku_pool
- 原子字段: cost_price (进货成本价)
"""

from typing import List
import pandas as pd

from .base_extractor import BaseExtractor


class CostPriceExtractor(BaseExtractor):
    """
    成本价域取数器

    提取进货成本价
    """

    @property
    def domain_name(self) -> str:
        return "cost_price"

    @property
    def atomic_fields(self) -> List[str]:
        """成本价域原子字段列表"""
        return [
            "cost_price",  # 进货成本价
        ]

    @property
    def key_fields(self) -> List[str]:
        """主键字段：门店+商品"""
        return ["store_id", "article_id"]

    def get_sql(self, start_date: str, end_date: str) -> str:
        """生成成本价域取数 SQL"""
        return f"""
        SELECT
            shop_id AS store_id,
            sku_code AS article_id,
            COALESCE(cost_price, 0) AS cost_price
        FROM hive.ods_sc_db.t_shop_inventory_sku_pool
        WHERE update_time >= '{start_date}'
        """

    def _post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """后处理：数据类型转换"""
        numeric_cols = self.atomic_fields
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df

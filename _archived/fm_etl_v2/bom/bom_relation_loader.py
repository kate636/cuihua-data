"""
BOM 关系加载器

加载 BOM 配比关系表
"""

import pandas as pd
import numpy as np
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class BOMRelationLoader:
    """
    BOM 关系加载器

    加载成品与原料的配比关系

    假设 BOM 关系表结构:
    - ddl.ddl_compose_bom_relation_di

    字段:
    - product_article_id: 成品ID (销售码)
    - raw_article_id: 原料ID (进货码)
    - bom_ratio: 配比系数 (0-1)
    - bom_qty: 配比数量
    - effective_date: 生效日期
    - expiry_date: 失效日期
    - is_active: 是否有效
    """

    def __init__(self, conn=None):
        """
        初始化 BOM 关系加载器

        Args:
            conn: 数据库连接 (可选)
        """
        self.conn = conn
        self.logger = logger

    def load(
        self,
        business_date: str,
        conn=None,
    ) -> pd.DataFrame:
        """
        加载 BOM 关系

        Args:
            business_date: 业务日期
            conn: 数据库连接 (可选)

        Returns:
            BOM 关系 DataFrame
        """
        if conn is None:
            conn = self.conn

        if conn is None:
            self.logger.warning("No database connection, returning empty BOM relation")
            return self._get_empty_df()

        sql = f"""
        SELECT
            product_article_id,    -- 成品ID (销售码)
            raw_article_id,        -- 原料ID (进货码)
            bom_ratio,             -- 配比系数
            bom_qty,               -- 配比数量
            effective_date,        -- 生效日期
            expiry_date            -- 失效日期
        FROM hive.ddl.ddl_compose_bom_relation_di
        WHERE effective_date <= '{business_date}'
          AND (expiry_date IS NULL OR expiry_date > '{business_date}')
          AND is_active = 'Y'
        """

        try:
            df = pd.read_sql(sql, conn)
            self.logger.info(f"Loaded {len(df)} BOM relations")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load BOM relations: {e}")
            return self._get_empty_df()

    def load_from_parquet(self, file_path: str) -> pd.DataFrame:
        """
        从 Parquet 文件加载 BOM 关系

        Args:
            file_path: Parquet 文件路径

        Returns:
            BOM 关系 DataFrame
        """
        try:
            df = pd.read_parquet(file_path)
            self.logger.info(f"Loaded {len(df)} BOM relations from {file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load BOM relations from Parquet: {e}")
            return self._get_empty_df()

    def load_from_dict(self, bom_data: list) -> pd.DataFrame:
        """
        从字典列表加载 BOM 关系

        Args:
            bom_data: BOM 关系字典列表

        Returns:
            BOM 关系 DataFrame
        """
        df = pd.DataFrame(bom_data)
        self.logger.info(f"Loaded {len(df)} BOM relations from dict")
        return df

    def _get_empty_df(self) -> pd.DataFrame:
        """返回空的 BOM 关系 DataFrame"""
        return pd.DataFrame(columns=[
            "product_article_id",
            "raw_article_id",
            "bom_ratio",
            "bom_qty",
            "effective_date",
            "expiry_date",
        ])

    def validate(self, df: pd.DataFrame) -> dict:
        """
        验证 BOM 关系数据

        Args:
            df: BOM 关系 DataFrame

        Returns:
            验证结果字典
        """
        result = {
            "total_relations": len(df),
            "unique_products": df["product_article_id"].nunique() if "product_article_id" in df.columns else 0,
            "unique_raws": df["raw_article_id"].nunique() if "raw_article_id" in df.columns else 0,
            "invalid_ratio_count": 0,
            "duplicate_count": 0,
        }

        if "bom_ratio" in df.columns:
            # 检查配比系数是否在 0-1 范围内
            result["invalid_ratio_count"] = ((df["bom_ratio"] < 0) | (df["bom_ratio"] > 1)).sum()

        if "product_article_id" in df.columns and "raw_article_id" in df.columns:
            # 检查重复关系
            result["duplicate_count"] = df.duplicated(subset=["product_article_id", "raw_article_id"]).sum()

        return result

    def normalize_ratios(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        归一化配比系数

        确保同一原料的所有成品配比之和为 1

        Args:
            df: BOM 关系 DataFrame

        Returns:
            归一化后的 DataFrame
        """
        if "raw_article_id" not in df.columns or "bom_ratio" not in df.columns:
            return df

        df = df.copy()

        # 按原料分组，计算总配比
        total_ratios = df.groupby("raw_article_id")["bom_ratio"].transform("sum")

        # 归一化
        df["bom_ratio_normalized"] = df["bom_ratio"] / total_ratios

        return df

    def get_products_for_raw(self, df: pd.DataFrame, raw_article_id: str) -> pd.DataFrame:
        """
        获取原料对应的所有成品

        Args:
            df: BOM 关系 DataFrame
            raw_article_id: 原料ID

        Returns:
            成品列表 DataFrame
        """
        if "raw_article_id" not in df.columns:
            return pd.DataFrame()

        return df[df["raw_article_id"] == raw_article_id]

    def get_raws_for_product(self, df: pd.DataFrame, product_article_id: str) -> pd.DataFrame:
        """
        获取成品对应的所有原料

        Args:
            df: BOM 关系 DataFrame
            product_article_id: 成品ID

        Returns:
            原料列表 DataFrame
        """
        if "product_article_id" not in df.columns:
            return pd.DataFrame()

        return df[df["product_article_id"] == product_article_id]

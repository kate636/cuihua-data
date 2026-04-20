"""
BOM 毛利调整器

在 BOM 成本分摊后调整毛利计算
"""

import pandas as pd
import numpy as np
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class BOMProfitAdjuster:
    """
    BOM 毛利调整器

    在 BOM 成本分摊后重新计算毛利
    """

    def __init__(self):
        """初始化 BOM 毛利调整器"""
        self.logger = logger

    def adjust(
        self,
        df: pd.DataFrame,
        bom_split_cost_col: str = "bom_split_cost",
        sale_amt_col: str = "sale_amt",
        profit_amt_col: str = "profit_amt",
    ) -> pd.DataFrame:
        """
        调整毛利计算

        Args:
            df: 包含 BOM 分摊成本的 DataFrame
            bom_split_cost_col: BOM 分摊成本列名
            sale_amt_col: 销售金额列名
            profit_amt_col: 毛利额列名

        Returns:
            调整后的 DataFrame
        """
        df = df.copy()

        # 检查是否有 BOM 分摊成本
        if bom_split_cost_col not in df.columns:
            self.logger.warning("BOM split cost column not found")
            return df

        # 保存原始毛利
        df["profit_amt_original"] = df[profit_amt_col]

        # 调整毛利
        has_bom_cost = df[bom_split_cost_col] > 0

        if has_bom_cost.any():
            # 成品毛利 = 销售额 - 分摊成本
            df.loc[has_bom_cost, profit_amt_col] = (
                df.loc[has_bom_cost, sale_amt_col] - df.loc[has_bom_cost, bom_split_cost_col]
            )

        self.logger.info(f"Profit adjusted for {has_bom_cost.sum()} records")

        return df

    def calculate_bom_profit_rate(
        self,
        df: pd.DataFrame,
        profit_amt_col: str = "profit_amt",
        sale_amt_col: str = "sale_amt",
        bom_profit_rate_col: str = "bom_profit_rate",
    ) -> pd.DataFrame:
        """
        计算 BOM 调整后的毛利率

        Args:
            df: DataFrame
            profit_amt_col: 毛利额列名
            sale_amt_col: 销售金额列名
            bom_profit_rate_col: BOM 毛利率列名

        Returns:
            计算后的 DataFrame
        """
        df = df.copy()

        df[bom_profit_rate_col] = np.where(
            df[sale_amt_col] > 0,
            df[profit_amt_col] / df[sale_amt_col],
            0
        )

        return df

    def validate(
        self,
        df: pd.DataFrame,
        profit_amt_col: str = "profit_amt",
        sale_amt_col: str = "sale_amt",
    ) -> dict:
        """
        验证毛利调整结果

        Args:
            df: 调整后的 DataFrame
            profit_amt_col: 毛利额列名
            sale_amt_col: 销售金额列名

        Returns:
            验证结果字典
        """
        result = {
            "total_records": len(df),
            "negative_profit_count": 0,
            "over_100_profit_rate_count": 0,
            "zero_profit_count": 0,
        }

        # 计算毛利率
        profit_rate = np.where(
            df[sale_amt_col] > 0,
            df[profit_amt_col] / df[sale_amt_col],
            np.nan
        )

        result["negative_profit_count"] = (df[profit_amt_col] < 0).sum()
        result["over_100_profit_rate_count"] = (profit_rate > 1).sum()
        result["zero_profit_count"] = (df[profit_amt_col] == 0).sum()

        return result

    def get_adjustment_summary(
        self,
        df: pd.DataFrame,
        article_id_col: str = "article_id",
        profit_amt_col: str = "profit_amt",
        profit_original_col: str = "profit_amt_original",
    ) -> pd.DataFrame:
        """
        获取调整汇总

        Args:
            df: 调整后的 DataFrame
            article_id_col: 商品ID列名
            profit_amt_col: 调整后毛利额列名
            profit_original_col: 原始毛利额列名

        Returns:
            汇总 DataFrame
        """
        if profit_original_col not in df.columns:
            return pd.DataFrame()

        summary = df.groupby(article_id_col).agg({
            profit_amt_col: "sum",
            profit_original_col: "sum",
        }).reset_index()

        summary["profit_diff"] = summary[profit_amt_col] - summary[profit_original_col]

        return summary

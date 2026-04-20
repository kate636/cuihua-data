"""
BOM 成本分摊器

按 BOM 配比分摊进货成本到销售码
"""

import pandas as pd
import numpy as np
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class BOMCostSplitter:
    """
    BOM 成本分摊器

    问题背景:
    - 进货码（原料）和销售码（成品）不同
    - 直接关联会导致：销售码 0 成本（毛利率 100%）或进货码负毛利

    解决方案:
    - 方案一：按 BOM 配比分摊（推荐）
    - 方案二：按销售金额分摊
    - 方案三：按标准成本分摊

    默认使用方案一：按 BOM 配比分摊
    """

    def __init__(self, bom_relation: pd.DataFrame, method: str = "ratio"):
        """
        初始化 BOM 成本分摊器

        Args:
            bom_relation: BOM 关系 DataFrame
            method: 分摊方法 ("ratio", "sales", "standard")
        """
        self.bom_relation = bom_relation
        self.method = method
        self.logger = logger

    def split_cost(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        分摊进货成本到销售码

        Args:
            df: 包含进货、销售数据的 DataFrame

        Returns:
            分摊后的 DataFrame
        """
        if self.bom_relation.empty:
            self.logger.warning("BOM relation is empty, returning original data")
            return df

        df = df.copy()

        # 初始化分摊成本列
        df["bom_split_cost"] = 0.0
        df["bom_adjusted_cost"] = df.get("cost_price", 0)

        if self.method == "ratio":
            df = self._split_by_ratio(df)
        elif self.method == "sales":
            df = self._split_by_sales(df)
        else:
            self.logger.warning(f"Unknown method: {self.method}, using ratio")
            df = self._split_by_ratio(df)

        self.logger.info(f"BOM cost split completed: {len(df)} rows")

        return df

    def _split_by_ratio(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        按 BOM 配比分摊成本

        成品分摊成本 = 原料进货成本 × 成品BOM配比
        """
        # 按原料分组处理
        for raw_id in self.bom_relation["raw_article_id"].unique():
            # 获取该原料的所有成品
            bom_rows = self.bom_relation[self.bom_relation["raw_article_id"] == raw_id]

            # 找到原料的进货记录
            raw_mask = df["article_id"] == raw_id
            raw_records = df[raw_mask]

            if raw_records.empty:
                continue

            # 计算原料总进货成本
            raw_total_cost = (
                raw_records["receive_qty"] * raw_records.get("cost_price", 0)
            ).sum()

            if raw_total_cost == 0:
                continue

            # 按配比分摊到各成品
            for _, bom_row in bom_rows.iterrows():
                product_id = bom_row["product_article_id"]
                ratio = bom_row.get("bom_ratio", 1.0)

                # 成品分摊成本
                split_cost = raw_total_cost * ratio

                # 写入成品记录
                product_mask = df["article_id"] == product_id
                if product_mask.any():
                    df.loc[product_mask, "bom_split_cost"] += split_cost

                    # 更新调整后成本
                    receive_qty = df.loc[product_mask, "receive_qty"].sum()
                    if receive_qty > 0:
                        df.loc[product_mask, "bom_adjusted_cost"] = (
                            df.loc[product_mask, "cost_price"] +
                            df.loc[product_mask, "bom_split_cost"] / receive_qty
                        )

        return df

    def _split_by_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        按销售金额分摊成本

        成品分摊成本 = 原料总成本 × (成品销售额 / 成品总销售额)
        """
        # 按原料分组处理
        for raw_id in self.bom_relation["raw_article_id"].unique():
            bom_rows = self.bom_relation[self.bom_relation["raw_article_id"] == raw_id]

            raw_mask = df["article_id"] == raw_id
            raw_records = df[raw_mask]

            if raw_records.empty:
                continue

            raw_total_cost = (
                raw_records["receive_qty"] * raw_records.get("cost_price", 0)
            ).sum()

            if raw_total_cost == 0:
                continue

            # 获取所有成品的销售额
            product_ids = bom_rows["product_article_id"].tolist()
            product_mask = df["article_id"].isin(product_ids)
            product_sales = df.loc[product_mask, "sale_amt"].sum()

            if product_sales == 0:
                continue

            # 按销售额比例分摊
            for product_id in product_ids:
                product_single_mask = df["article_id"] == product_id
                product_sale_amt = df.loc[product_single_mask, "sale_amt"].sum()

                split_cost = raw_total_cost * (product_sale_amt / product_sales)

                if product_single_mask.any():
                    df.loc[product_single_mask, "bom_split_cost"] += split_cost

        return df

    def adjust_profit(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        调整毛利计算

        成品毛利 = 销售额 - 分摊成本
        原料毛利 = 0 (成本已分摊到成品)

        Args:
            df: 包含毛利数据的 DataFrame

        Returns:
            调整后的 DataFrame
        """
        df = df.copy()

        # 成品毛利调整
        has_bom_cost = df["bom_split_cost"] > 0
        if has_bom_cost.any():
            df.loc[has_bom_cost, "profit_amt_bom"] = (
                df.loc[has_bom_cost, "sale_amt"] - df.loc[has_bom_cost, "bom_split_cost"]
            )

        # 原料毛利调整 (标记为已分摊)
        for raw_id in self.bom_relation["raw_article_id"].unique():
            raw_mask = df["article_id"] == raw_id
            if raw_mask.any():
                df.loc[raw_mask, "bom_cost_distributed"] = True

        return df

    def validate_split(self, df: pd.DataFrame) -> dict:
        """
        验证成本分摊结果

        检查:
        - 分摊成本是否合理
        - 是否有 0 成本的成品
        - 是否有异常毛利

        Args:
            df: 分摊后的 DataFrame

        Returns:
            验证结果字典
        """
        result = {
            "total_split_records": (df["bom_split_cost"] > 0).sum(),
            "zero_cost_products": 0,
            "negative_profit_count": 0,
            "over_100_profit_count": 0,
        }

        # 检查 0 成本成品
        if "bom_adjusted_cost" in df.columns:
            result["zero_cost_products"] = (df["bom_adjusted_cost"] == 0).sum()

        # 检查异常毛利率
        if "profit_amt_bom" in df.columns and "sale_amt" in df.columns:
            profit_rate = df["profit_amt_bom"] / df["sale_amt"].replace(0, np.nan)
            result["negative_profit_count"] = (profit_rate < 0).sum()
            result["over_100_profit_count"] = (profit_rate > 1).sum()

        return result

    def get_split_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        获取分摊汇总

        Args:
            df: 分摊后的 DataFrame

        Returns:
            汇总 DataFrame
        """
        summary_cols = ["article_id", "sale_amt", "cost_price", "bom_split_cost", "bom_adjusted_cost"]
        available_cols = [col for col in summary_cols if col in df.columns]

        if "bom_split_cost" not in df.columns:
            return pd.DataFrame()

        # 只返回有分摊成本的记录
        result = df[df["bom_split_cost"] > 0][available_cols].copy()

        return result

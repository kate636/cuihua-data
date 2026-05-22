"""
均价计算器

计算平均进货价和库存均价
"""

import pandas as pd
import numpy as np
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class AvgPriceCalculator:
    """
    均价计算器

    计算平均进货价 (avg_purchase_price) 和库存均价 (avg_price)

    非日清商品 (day_clear='1'):
    - avg_purchase_price = cost_price

    日清商品 (day_clear='0'):
    - avg_purchase_price = 加权平均
    - 公式: (init_stock_amt + receive_amt + compose_in_amt - compose_out_amt)
           / (init_stock_qty + receive_qty + compose_in_qty - compose_out_qty)
    """

    def __init__(self):
        """初始化均价计算器"""
        self.logger = logger

    def calculate(
        self,
        df: pd.DataFrame,
        day_clear_col: str = "day_clear",
        cost_price_col: str = "cost_price",
        init_stock_qty_col: str = "init_stock_qty",
        receive_qty_col: str = "receive_qty",
        compose_in_qty_col: str = "compose_in_qty",
        compose_out_qty_col: str = "compose_out_qty",
        init_stock_amt_col: str = "init_stock_amt",
        receive_amt_col: str = "receive_amt",
        compose_in_amt_col: str = "compose_in_amt",
        compose_out_amt_col: str = "compose_out_amt",
        avg_purchase_price_col: str = "avg_purchase_price",
        avg_price_col: str = "avg_price",
    ) -> pd.DataFrame:
        """
        计算均价

        Args:
            df: 包含所有输入字段的 DataFrame
            day_clear_col: 日清标识列名
            cost_price_col: 成本价列名
            ...: 其他列名
            avg_purchase_price_col: 平均进货价列名 (输出)
            avg_price_col: 库存均价列名 (输出)

        Returns:
            计算后的 DataFrame
        """
        df = df.copy()

        # 确保数值列存在
        for col in [cost_price_col, init_stock_qty_col, receive_qty_col,
                    compose_in_qty_col, compose_out_qty_col]:
            if col not in df.columns:
                df[col] = 0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # 检查是否有金额列 (如果没有，需要先计算)
        has_amount_cols = all(
            col in df.columns for col in
            [init_stock_amt_col, receive_amt_col, compose_in_amt_col, compose_out_amt_col]
        )

        if not has_amount_cols:
            # 使用成本价计算金额
            self.logger.info("Amount columns not found, calculating from cost_price")
            df[init_stock_amt_col] = df[init_stock_qty_col] * df[cost_price_col]
            df[receive_amt_col] = df[receive_qty_col] * df[cost_price_col]
            df[compose_in_amt_col] = df[compose_in_qty_col] * df[cost_price_col]
            df[compose_out_amt_col] = df[compose_out_qty_col] * df[cost_price_col]

        # 判断日清标识
        is_non_daily = df[day_clear_col] == "1"

        # 非日清商品：直接使用成本价
        df.loc[is_non_daily, avg_purchase_price_col] = df.loc[is_non_daily, cost_price_col]
        df.loc[is_non_daily, avg_price_col] = df.loc[is_non_daily, cost_price_col]

        # 日清商品：加权平均
        # 分母：总可用数量
        total_qty = (
            df[init_stock_qty_col]
            + df[receive_qty_col]
            + df[compose_in_qty_col]
            - df[compose_out_qty_col]
        )

        # 分子：总金额
        total_amt = (
            df[init_stock_amt_col]
            + df[receive_amt_col]
            + df[compose_in_amt_col]
            - df[compose_out_amt_col]
        )

        # 计算加权平均价
        df.loc[~is_non_daily, avg_purchase_price_col] = np.where(
            total_qty[~is_non_daily] > 0,
            total_amt[~is_non_daily] / total_qty[~is_non_daily],
            df.loc[~is_non_daily, cost_price_col]
        )

        # 库存均价 = 平均进货价
        df.loc[~is_non_daily, avg_price_col] = df.loc[~is_non_daily, avg_purchase_price_col]

        # 填充 NaN
        df[avg_purchase_price_col] = df[avg_purchase_price_col].fillna(df[cost_price_col])
        df[avg_price_col] = df[avg_price_col].fillna(df[cost_price_col])

        self.logger.info(f"Average prices calculated: {len(df)} rows")

        return df

    def calculate_simple(
        self,
        df: pd.DataFrame,
        cost_price_col: str = "cost_price",
        avg_purchase_price_col: str = "avg_purchase_price",
        avg_price_col: str = "avg_price",
    ) -> pd.DataFrame:
        """
        简化计算：直接使用成本价作为均价

        适用于不需要区分日清/非日清的场景

        Args:
            df: DataFrame
            cost_price_col: 成本价列名
            avg_purchase_price_col: 平均进货价列名 (输出)
            avg_price_col: 库存均价列名 (输出)

        Returns:
            计算后的 DataFrame
        """
        df = df.copy()

        if cost_price_col not in df.columns:
            df[cost_price_col] = 0

        df[avg_purchase_price_col] = df[cost_price_col]
        df[avg_price_col] = df[cost_price_col]

        return df

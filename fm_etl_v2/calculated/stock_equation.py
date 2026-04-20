"""
库存方程求解器

求解库存方程，推算期末库存和未知损耗
"""

import pandas as pd
import numpy as np
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class StockEquationSolver:
    """
    库存方程求解器

    标准形式:
    end_stock_qty = init_stock_qty + receive_qty + compose_in_qty
                  - compose_out_qty - sale_qty - know_lost_qty - unknow_lost_qty

    有盘点数据时:
    - end_stock_qty = physical_count_qty (原子①)
    - unknow_lost_qty = 库存方程反推 (计算②)

    无盘点数据时:
    - end_stock_qty = 库存方程推算 (计算②，假设 unknow_lost_qty = 0)
    - unknow_lost_qty = 0
    """

    def __init__(self):
        """初始化库存方程求解器"""
        self.logger = logger

    def solve(
        self,
        df: pd.DataFrame,
        init_stock_col: str = "init_stock_qty",
        receive_col: str = "receive_qty",
        compose_in_col: str = "compose_in_qty",
        compose_out_col: str = "compose_out_qty",
        sale_col: str = "sale_qty",
        know_lost_col: str = "know_lost_qty",
        physical_count_col: str = "physical_count_qty",
        end_stock_col: str = "end_stock_qty",
        unknow_lost_col: str = "unknow_lost_qty",
    ) -> pd.DataFrame:
        """
        求解库存方程

        Args:
            df: 包含所有输入字段的 DataFrame
            init_stock_col: 期初库存列名
            receive_col: 进货数量列名
            compose_in_col: 加工转入列名
            compose_out_col: 加工转出列名
            sale_col: 销售数量列名
            know_lost_col: 已知损耗列名
            physical_count_col: 盘点数量列名
            end_stock_col: 期末库存列名 (输出)
            unknow_lost_col: 未知损耗列名 (输出)

        Returns:
            求解后的 DataFrame
        """
        df = df.copy()

        # 确保所有数值列存在且为数值类型
        required_cols = [
            init_stock_col, receive_col, compose_in_col, compose_out_col,
            sale_col, know_lost_col
        ]

        for col in required_cols:
            if col not in df.columns:
                df[col] = 0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # 检查是否有盘点数据
        has_physical_count = physical_count_col in df.columns and df[physical_count_col].notna().any()

        if has_physical_count:
            self.logger.info("Solving with physical count data")
            df = self._solve_with_physical_count(
                df, init_stock_col, receive_col, compose_in_col, compose_out_col,
                sale_col, know_lost_col, physical_count_col, end_stock_col, unknow_lost_col
            )
        else:
            self.logger.info("Solving without physical count data (assuming unknow_lost = 0)")
            df = self._solve_without_physical_count(
                df, init_stock_col, receive_col, compose_in_col, compose_out_col,
                sale_col, know_lost_col, end_stock_col, unknow_lost_col
            )

        # 计算总损耗
        df["lost_qty"] = df[know_lost_col] + df[unknow_lost_col]

        self.logger.info(f"Stock equation solved: {len(df)} rows")

        return df

    def _solve_with_physical_count(
        self,
        df: pd.DataFrame,
        init_stock_col: str,
        receive_col: str,
        compose_in_col: str,
        compose_out_col: str,
        sale_col: str,
        know_lost_col: str,
        physical_count_col: str,
        end_stock_col: str,
        unknow_lost_col: str,
    ) -> pd.DataFrame:
        """
        有盘点数据时求解

        - end_stock_qty = physical_count_qty
        - unknow_lost_qty = 库存方程反推
        """
        df = df.copy()

        # 填充盘点数据
        df[physical_count_col] = df[physical_count_col].fillna(0)

        # 判断是否有盘点数据
        has_count = df[physical_count_col] > 0

        # 有盘点数据时
        df.loc[has_count, end_stock_col] = df.loc[has_count, physical_count_col]
        df.loc[has_count, unknow_lost_col] = (
            df.loc[has_count, init_stock_col]
            + df.loc[has_count, receive_col]
            + df.loc[has_count, compose_in_col]
            - df.loc[has_count, compose_out_col]
            - df.loc[has_count, sale_col]
            - df.loc[has_count, know_lost_col]
            - df.loc[has_count, physical_count_col]
        )

        # 无盘点数据时 (假设 unknow_lost = 0)
        df.loc[~has_count, end_stock_col] = (
            df.loc[~has_count, init_stock_col]
            + df.loc[~has_count, receive_col]
            + df.loc[~has_count, compose_in_col]
            - df.loc[~has_count, compose_out_col]
            - df.loc[~has_count, sale_col]
            - df.loc[~has_count, know_lost_col]
        )
        df.loc[~has_count, unknow_lost_col] = 0

        return df

    def _solve_without_physical_count(
        self,
        df: pd.DataFrame,
        init_stock_col: str,
        receive_col: str,
        compose_in_col: str,
        compose_out_col: str,
        sale_col: str,
        know_lost_col: str,
        end_stock_col: str,
        unknow_lost_col: str,
    ) -> pd.DataFrame:
        """
        无盘点数据时求解

        - 假设 unknow_lost_qty = 0
        - end_stock_qty = 库存方程推算
        """
        df = df.copy()

        # 期末库存 = 库存方程推算
        df[end_stock_col] = (
            df[init_stock_col]
            + df[receive_col]
            + df[compose_in_col]
            - df[compose_out_col]
            - df[sale_col]
            - df[know_lost_col]
        )

        # 未知损耗 = 0
        df[unknow_lost_col] = 0

        return df

    def validate(
        self,
        df: pd.DataFrame,
        init_stock_col: str = "init_stock_qty",
        receive_col: str = "receive_qty",
        compose_in_col: str = "compose_in_qty",
        compose_out_col: str = "compose_out_qty",
        sale_col: str = "sale_qty",
        know_lost_col: str = "know_lost_qty",
        unknow_lost_col: str = "unknow_lost_qty",
        end_stock_col: str = "end_stock_qty",
    ) -> dict:
        """
        验证库存方程平衡

        检查: init + receive + compose_in - compose_out - sale - know_lost - unknow_lost = end

        Args:
            df: 求解后的 DataFrame
            ...: 各列名

        Returns:
            验证结果字典
        """
        df = df.copy()

        # 计算方程左边
        lhs = (
            df[init_stock_col]
            + df[receive_col]
            + df[compose_in_col]
            - df[compose_out_col]
            - df[sale_col]
            - df[know_lost_col]
            - df[unknow_lost_col]
        )

        # 计算差异
        diff = lhs - df[end_stock_col]

        result = {
            "total_records": len(df),
            "balanced_count": (diff.abs() < 0.01).sum(),
            "unbalanced_count": (diff.abs() >= 0.01).sum(),
            "max_diff": diff.abs().max(),
            "mean_diff": diff.abs().mean(),
        }

        return result

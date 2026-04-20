"""
毛利计算器

计算所有毛利类指标
"""

import pandas as pd
import numpy as np
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class ProfitCalculator:
    """
    毛利计算器

    核心公式:
    profit_amt = sale_amt - (receive_amt + compose_in_amt - compose_out_amt)
               + (end_stock_amt - init_stock_amt)

    销售成本 (区分日清/非日清):
    - 非日清: sale_cost_amt = sale_qty × avg_purchase_price
    - 日清: sale_cost_amt = receive_amt + compose_in_amt - compose_out_amt - lost_amt
    """

    def __init__(self):
        """初始化毛利计算器"""
        self.logger = logger

    def calculate(
        self,
        df: pd.DataFrame,
        day_clear_col: str = "day_clear",
    ) -> pd.DataFrame:
        """
        计算所有毛利指标

        Args:
            df: 包含所有输入字段的 DataFrame
            day_clear_col: 日清标识列名

        Returns:
            计算后的 DataFrame
        """
        df = df.copy()

        # 确保必要列存在
        required_cols = [
            "sale_amt", "receive_amt", "compose_in_amt", "compose_out_amt",
            "init_stock_amt", "end_stock_amt", "lost_amt", "sale_qty",
            "avg_purchase_price", "original_price_sale_amt", "allowance_amt",
            "out_stock_pay_amt_notax", "return_stock_pay_amt_notax",
            "out_stock_amt_cb_notax", "return_stock_amt_cb_notax",
        ]

        for col in required_cols:
            if col not in df.columns:
                df[col] = 0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # 1. 运营毛利额
        df = self._calculate_profit_amt(df)

        # 2. 销售成本
        df = self._calculate_sale_cost_amt(df, day_clear_col)

        # 3. 预期毛利额
        df = self._calculate_pre_profit_amt(df)

        # 4. 补贴后毛利额
        df = self._calculate_allowance_profit(df)

        # 5. 供应链毛利
        df = self._calculate_scm_profit(df)

        # 6. 全链路毛利
        df = self._calculate_full_link_profit(df)

        self.logger.info(f"Profit calculations completed: {len(df)} rows")

        return df

    def _calculate_profit_amt(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算运营毛利额

        profit_amt = sale_amt - (receive_amt + compose_in_amt - compose_out_amt)
                   + (end_stock_amt - init_stock_amt)
        """
        df["profit_amt"] = (
            df["sale_amt"]
            - (df["receive_amt"] + df["compose_in_amt"] - df["compose_out_amt"])
            + (df["end_stock_amt"] - df["init_stock_amt"])
        )

        return df

    def _calculate_sale_cost_amt(
        self,
        df: pd.DataFrame,
        day_clear_col: str,
    ) -> pd.DataFrame:
        """
        计算销售成本

        非日清: sale_cost_amt = sale_qty × avg_purchase_price
        日清: sale_cost_amt = receive_amt + compose_in_amt - compose_out_amt - lost_amt
        """
        # 判断日清标识
        is_non_daily = df[day_clear_col] == "1"

        # 非日清商品
        df.loc[is_non_daily, "sale_cost_amt"] = (
            df.loc[is_non_daily, "sale_qty"] * df.loc[is_non_daily, "avg_purchase_price"]
        )

        # 日清商品
        df.loc[~is_non_daily, "sale_cost_amt"] = (
            df.loc[~is_non_daily, "receive_amt"]
            + df.loc[~is_non_daily, "compose_in_amt"]
            - df.loc[~is_non_daily, "compose_out_amt"]
            - df.loc[~is_non_daily, "lost_amt"]
        )

        return df

    def _calculate_pre_profit_amt(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算预期毛利额"""
        df["pre_profit_amt"] = df["original_price_sale_amt"] - df["sale_cost_amt"]
        return df

    def _calculate_allowance_profit(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算补贴后毛利额

        allowance_amt_profit = sale_amt - receive_amt + allowance_amt
                            + (end_stock_amt - init_stock_amt)
        """
        df["allowance_amt_profit"] = (
            df["sale_amt"] - df["receive_amt"] + df["allowance_amt"]
            + (df["end_stock_amt"] - df["init_stock_amt"])
        )

        return df

    def _calculate_scm_profit(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算供应链毛利

        scm_fin_article_income = out_stock_pay_amt_notax - |return_stock_pay_amt_notax|
        scm_fin_article_cost = out_stock_amt_cb_notax - |return_stock_amt_cb_notax|
        scm_fin_article_profit = income - cost
        """
        # 供应链收入
        df["scm_fin_article_income"] = (
            df["out_stock_pay_amt_notax"] - df["return_stock_pay_amt_notax"].abs()
        )

        # 供应链成本
        df["scm_fin_article_cost"] = (
            df["out_stock_amt_cb_notax"] - df["return_stock_amt_cb_notax"].abs()
        )

        # 供应链毛利
        df["scm_fin_article_profit"] = df["scm_fin_article_income"] - df["scm_fin_article_cost"]

        return df

    def _calculate_full_link_profit(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算全链路毛利

        full_link_article_profit = profit_amt + scm_fin_article_profit
        """
        df["full_link_article_profit"] = df["profit_amt"] + df["scm_fin_article_profit"]

        return df

    def get_profit_columns(self) -> list:
        """获取所有毛利列名"""
        return [
            "profit_amt",
            "sale_cost_amt",
            "pre_profit_amt",
            "allowance_amt_profit",
            "scm_fin_article_income",
            "scm_fin_article_cost",
            "scm_fin_article_profit",
            "full_link_article_profit",
        ]

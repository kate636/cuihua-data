"""
金额计算器

计算所有金额类指标 (数量 × 单价)
"""

import pandas as pd
import numpy as np
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


class AmountCalculator:
    """
    金额计算器

    所有金额 = 数量 × 单价

    计算内容:
    - 进货金额: receive_amt = receive_qty × avg_purchase_price
    - 库存金额: init_stock_amt, end_stock_amt
    - 损耗金额: know_lost_amt, unknow_lost_amt, lost_amt
    - 加工转换金额: compose_in_amt, compose_out_amt
    - 供应链金额: 出库/退仓 × 4种单价
    - SAP 让利汇总: scm_promotion_amt
    """

    def __init__(self):
        """初始化金额计算器"""
        self.logger = logger

    def calculate(
        self,
        df: pd.DataFrame,
        avg_purchase_price_col: str = "avg_purchase_price",
        avg_price_col: str = "avg_price",
        cost_price_col: str = "cost_price",
    ) -> pd.DataFrame:
        """
        计算所有金额指标

        Args:
            df: 包含数量和单价字段的 DataFrame
            avg_purchase_price_col: 平均进货价列名
            avg_price_col: 库存均价列名
            cost_price_col: 成本价列名

        Returns:
            计算后的 DataFrame
        """
        df = df.copy()

        # 确保单价列存在
        for col in [avg_purchase_price_col, avg_price_col, cost_price_col]:
            if col not in df.columns:
                df[col] = 0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # 1. 进货金额
        df = self._calculate_receive_amt(df, avg_purchase_price_col)

        # 2. 库存金额
        df = self._calculate_stock_amt(df, avg_price_col)

        # 3. 损耗金额
        df = self._calculate_loss_amt(df, cost_price_col)

        # 4. 加工转换金额
        df = self._calculate_compose_amt(df, cost_price_col)

        # 5. 供应链金额
        df = self._calculate_scm_amt(df)

        # 6. SAP 让利汇总
        df = self._calculate_scm_promotion(df)

        self.logger.info(f"Amount calculations completed: {len(df)} rows")

        return df

    def _calculate_receive_amt(
        self,
        df: pd.DataFrame,
        avg_purchase_price_col: str,
    ) -> pd.DataFrame:
        """计算进货金额"""
        if "receive_qty" in df.columns:
            df["receive_amt"] = df["receive_qty"] * df[avg_purchase_price_col]
        else:
            df["receive_amt"] = 0

        return df

    def _calculate_stock_amt(
        self,
        df: pd.DataFrame,
        avg_price_col: str,
    ) -> pd.DataFrame:
        """计算库存金额"""
        # 期初库存金额
        if "init_stock_qty" in df.columns:
            df["init_stock_amt"] = df["init_stock_qty"] * df[avg_price_col]
        else:
            df["init_stock_amt"] = 0

        # 期末库存金额
        if "end_stock_qty" in df.columns:
            df["end_stock_amt"] = df["end_stock_qty"] * df[avg_price_col]
        else:
            df["end_stock_amt"] = 0

        return df

    def _calculate_loss_amt(
        self,
        df: pd.DataFrame,
        cost_price_col: str,
    ) -> pd.DataFrame:
        """计算损耗金额"""
        # 已知损耗金额
        if "know_lost_qty" in df.columns:
            df["know_lost_amt"] = df["know_lost_qty"] * df[cost_price_col]
        else:
            df["know_lost_amt"] = 0

        # 未知损耗金额
        if "unknow_lost_qty" in df.columns:
            df["unknow_lost_amt"] = df["unknow_lost_qty"] * df[cost_price_col]
        else:
            df["unknow_lost_amt"] = 0

        # 总损耗金额
        df["lost_amt"] = df["know_lost_amt"] + df["unknow_lost_amt"]

        return df

    def _calculate_compose_amt(
        self,
        df: pd.DataFrame,
        cost_price_col: str,
    ) -> pd.DataFrame:
        """计算加工转换金额"""
        # 加工转入金额
        if "compose_in_qty" in df.columns:
            df["compose_in_amt"] = df["compose_in_qty"] * df[cost_price_col]
        else:
            df["compose_in_amt"] = 0

        # 加工转出金额
        if "compose_out_qty" in df.columns:
            df["compose_out_amt"] = df["compose_out_qty"] * df[cost_price_col]
        else:
            df["compose_out_amt"] = 0

        return df

    def _calculate_scm_amt(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算供应链金额 (4种单价 × 出库/退仓数量)"""
        # 出库数量 (使用 original_outstock_qty 或计算总出库)
        if "original_outstock_qty" in df.columns:
            # 计算总出库数量
            df["outstock_qty"] = df.get("original_outstock_qty", 0)
            if "promotion_outstock_qty" in df.columns:
                df["outstock_qty"] += df["promotion_outstock_qty"]
            if "gift_outstock_qty" in df.columns:
                df["outstock_qty"] += df["gift_outstock_qty"]
        else:
            df["outstock_qty"] = 0

        # 出库金额 (含税)
        if "outstock_unit_price" in df.columns:
            df["out_stock_pay_amt"] = df["outstock_qty"] * df["outstock_unit_price"]
        else:
            df["out_stock_pay_amt"] = 0

        # 出库金额 (不含税)
        if "outstock_unit_price_notax" in df.columns:
            df["out_stock_pay_amt_notax"] = df["outstock_qty"] * df["outstock_unit_price_notax"]
        else:
            df["out_stock_pay_amt_notax"] = 0

        # 出库成本 (含税)
        if "outstock_cost_price" in df.columns:
            df["out_stock_amt_cb"] = df["outstock_qty"] * df["outstock_cost_price"]
        else:
            df["out_stock_amt_cb"] = 0

        # 出库成本 (不含税)
        if "outstock_cost_price_notax" in df.columns:
            df["out_stock_amt_cb_notax"] = df["outstock_qty"] * df["outstock_cost_price_notax"]
        else:
            df["out_stock_amt_cb_notax"] = 0

        # 退仓数量
        if "return_stock_qty" in df.columns:
            df["return_qty"] = df["return_stock_qty"]
        else:
            df["return_qty"] = 0

        # 退仓金额 (含税)
        if "return_unit_price" in df.columns:
            df["return_stock_pay_amt"] = df["return_qty"] * df["return_unit_price"]
        else:
            df["return_stock_pay_amt"] = 0

        # 退仓金额 (不含税)
        if "return_unit_price_notax" in df.columns:
            df["return_stock_pay_amt_notax"] = df["return_qty"] * df["return_unit_price_notax"]
        else:
            df["return_stock_pay_amt_notax"] = 0

        # 退仓成本 (含税)
        if "return_cost_price" in df.columns:
            df["return_stock_amt_cb"] = df["return_qty"] * df["return_cost_price"]
        else:
            df["return_stock_amt_cb"] = 0

        # 退仓成本 (不含税)
        if "return_cost_price_notax" in df.columns:
            df["return_stock_amt_cb_notax"] = df["return_qty"] * df["return_cost_price_notax"]
        else:
            df["return_stock_amt_cb_notax"] = 0

        return df

    def _calculate_scm_promotion(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算 SAP 让利汇总"""
        # 非赠品出库让利
        if "scm_promotion_amt_total" in df.columns and "scm_promotion_amt_gift" in df.columns:
            df["scm_promotion_amt"] = df["scm_promotion_amt_total"] - df["scm_promotion_amt_gift"]
        else:
            df["scm_promotion_amt"] = 0

        return df

    def get_amount_columns(self) -> List[str]:
        """获取所有金额列名"""
        return [
            "receive_amt",
            "init_stock_amt",
            "end_stock_amt",
            "know_lost_amt",
            "unknow_lost_amt",
            "lost_amt",
            "compose_in_amt",
            "compose_out_amt",
            "out_stock_pay_amt",
            "out_stock_pay_amt_notax",
            "out_stock_amt_cb",
            "out_stock_amt_cb_notax",
            "return_stock_pay_amt",
            "return_stock_pay_amt_notax",
            "return_stock_amt_cb",
            "return_stock_amt_cb_notax",
            "scm_promotion_amt",
        ]

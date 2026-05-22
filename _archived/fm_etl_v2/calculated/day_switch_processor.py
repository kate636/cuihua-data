"""
日切处理器

处理期初库存 = 前日期末库存的逻辑
"""

import pandas as pd
import numpy as np
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class DaySwitchProcessor:
    """
    日切处理器

    处理期初库存 = 前日期末库存的逻辑

    核心原理：
    - init_stock_qty(今日) = end_stock_qty(昨日)
    - 首日数据需要特殊处理
    """

    def __init__(self):
        """初始化日切处理器"""
        self.logger = logger

    def process(
        self,
        df: pd.DataFrame,
        date_col: str = "business_date",
        group_cols: Optional[list] = None,
        init_stock_col: str = "init_stock_qty",
        end_stock_col: str = "end_stock_qty",
    ) -> pd.DataFrame:
        """
        处理日切依赖

        Args:
            df: 原子层数据
            date_col: 日期列名
            group_cols: 分组列 (默认: ["store_id", "article_id"])
            init_stock_col: 期初库存列名
            end_stock_col: 期末库存列名

        Returns:
            处理后的 DataFrame (init_stock_qty 已更新)
        """
        df = df.copy()

        if group_cols is None:
            group_cols = ["store_id", "article_id"]

        # 确保日期列是 datetime 类型
        df[date_col] = pd.to_datetime(df[date_col])

        # 按分组列和日期排序
        df = df.sort_values(group_cols + [date_col])

        # 保存原始期初库存 (用于首日数据)
        df["_original_init_stock"] = df[init_stock_col]

        # 期初库存 = 前日期末库存
        df[init_stock_col] = df.groupby(group_cols)[end_stock_col].shift(1)

        # 首日数据：使用原始期初库存
        first_day_mask = df[init_stock_col].isna()
        df.loc[first_day_mask, init_stock_col] = df.loc[first_day_mask, "_original_init_stock"]

        # 如果首日也没有期初库存，使用期末库存
        still_na = df[init_stock_col].isna()
        df.loc[still_na, init_stock_col] = df.loc[still_na, end_stock_col]

        # 清理临时列
        df = df.drop(columns=["_original_init_stock"])

        # 填充剩余 NaN
        df[init_stock_col] = df[init_stock_col].fillna(0)

        self.logger.info(f"Day switch processed: {len(df)} rows, {first_day_mask.sum()} first-day records")

        return df

    def process_multi_day(
        self,
        df: pd.DataFrame,
        date_col: str = "business_date",
        group_cols: Optional[list] = None,
        init_stock_col: str = "init_stock_qty",
        end_stock_col: str = "end_stock_qty",
    ) -> pd.DataFrame:
        """
        处理多日数据的日切依赖

        此方法会确保日期连续性，对于缺失日期会进行插值处理

        Args:
            df: 原子层数据
            date_col: 日期列名
            group_cols: 分组列
            init_stock_col: 期初库存列名
            end_stock_col: 期末库存列名

        Returns:
            处理后的 DataFrame
        """
        df = df.copy()

        if group_cols is None:
            group_cols = ["store_id", "article_id"]

        # 确保日期列是 datetime 类型
        df[date_col] = pd.to_datetime(df[date_col])

        # 获取日期范围
        min_date = df[date_col].min()
        max_date = df[date_col].max()
        all_dates = pd.date_range(min_date, max_date, freq="D")

        # 创建完整的日期索引
        multi_index = pd.MultiIndex.from_product(
            [df[g].unique() for g in group_cols] + [all_dates],
            names=group_cols + [date_col]
        )

        # 重新索引以填充缺失日期
        df = df.set_index(group_cols + [date_col]).reindex(multi_index).reset_index()

        # 对于缺失日期，填充 0
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].fillna(0)

        # 处理日切
        return self.process(df, date_col, group_cols, init_stock_col, end_stock_col)

    def validate(
        self,
        df: pd.DataFrame,
        date_col: str = "business_date",
        group_cols: Optional[list] = None,
        init_stock_col: str = "init_stock_qty",
        end_stock_col: str = "end_stock_qty",
    ) -> dict:
        """
        验证日切处理结果

        检查今日期初是否等于昨日期末

        Args:
            df: 处理后的 DataFrame
            date_col: 日期列名
            group_cols: 分组列
            init_stock_col: 期初库存列名
            end_stock_col: 期末库存列名

        Returns:
            验证结果字典
        """
        if group_cols is None:
            group_cols = ["store_id", "article_id"]

        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values(group_cols + [date_col])

        # 计算昨日期末
        df["_prev_end_stock"] = df.groupby(group_cols)[end_stock_col].shift(1)

        # 比较今日期初和昨日期末
        non_first = df["_prev_end_stock"].notna()
        diff = df.loc[non_first, init_stock_col] - df.loc[non_first, "_prev_end_stock"]

        result = {
            "total_records": len(df),
            "first_day_records": (~non_first).sum(),
            "mismatch_count": (diff.abs() > 0.01).sum(),
            "max_diff": diff.abs().max() if len(diff) > 0 else 0,
            "mean_diff": diff.abs().mean() if len(diff) > 0 else 0,
        }

        return result

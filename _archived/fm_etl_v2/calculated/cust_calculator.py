"""
客数计算器

从 POS 订单明细聚合客数指标
"""

import pandas as pd
import numpy as np
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


class CustCalculator:
    """
    客数计算器

    从 POS 订单明细聚合客数指标
    粒度: 门店 × 日期 × day_clear × level_description × level_id
    """

    def __init__(self):
        """初始化客数计算器"""
        self.logger = logger

    def calculate(
        self,
        order_df: pd.DataFrame,
        store_flag_col: str = "store_flag",
        pay_at_col: str = "pay_at",
        order_id_col: str = "order_id",
        article_id_col: str = "article_id",
        order_status_col: str = "order_status",
        day_clear_col: str = "day_clear",
    ) -> pd.DataFrame:
        """
        计算各维度客数

        Args:
            order_df: POS 订单明细 (线上 + 线下)
            store_flag_col: 门店标识列名
            pay_at_col: 支付时间列名
            order_id_col: 订单ID列名
            article_id_col: 商品ID列名
            order_status_col: 订单状态列名
            day_clear_col: 日清标识列名

        Returns:
            客数底表
        """
        df = order_df.copy()

        # 过滤有效订单
        if order_status_col in df.columns:
            df = df[df[order_status_col] == "os.completed"]

        # 确保 pay_at 是时间类型
        if pay_at_col in df.columns:
            df[pay_at_col] = pd.to_datetime(df[pay_at_col], errors="coerce")
            df["_pay_time"] = df[pay_at_col].dt.strftime("%H:%M:%S")
        else:
            df["_pay_time"] = "00:00:00"

        # 19点前判断 (翠花店 20:00，非翠花店 19:00)
        if store_flag_col in df.columns:
            df["_is_bf19"] = (
                (df[store_flag_col] == "food_mart") & (df["_pay_time"] < "20:00:00") |
                (df[store_flag_col] != "food_mart") & (df["_pay_time"] < "19:00:00")
            )
        else:
            df["_is_bf19"] = df["_pay_time"] < "19:00:00"

        # 线上订单判断 (order_id 以 '*' 结尾)
        if order_id_col in df.columns:
            df["_is_online"] = df[order_id_col].astype(str).str.endswith("*")
        else:
            df["_is_online"] = False

        # 按多维度聚合
        results = []
        levels = ["store", "category1", "category2", "category3", "spu", "blackwhite_pig"]

        for level in levels:
            agg_df = self._aggregate_by_level(df, level, day_clear_col, order_id_col, article_id_col)
            results.append(agg_df)

        result_df = pd.concat(results, ignore_index=True)

        # 清理临时列
        result_df = result_df.drop(columns=["_pay_time", "_is_bf19", "_is_online"], errors="ignore")

        self.logger.info(f"Customer count calculated: {len(result_df)} rows")

        return result_df

    def _aggregate_by_level(
        self,
        df: pd.DataFrame,
        level: str,
        day_clear_col: str,
        order_id_col: str,
        article_id_col: str,
    ) -> pd.DataFrame:
        """按层级聚合"""
        # 分组字段映射
        group_cols_map = {
            "store": ["store_id", "business_date", day_clear_col],
            "category1": ["store_id", "business_date", day_clear_col, "category_level1_id"],
            "category2": ["store_id", "business_date", day_clear_col, "category_level2_id"],
            "category3": ["store_id", "business_date", day_clear_col, "category_level3_id"],
            "spu": ["store_id", "business_date", day_clear_col, "spu_id"],
            "blackwhite_pig": ["store_id", "business_date", day_clear_col, "blackwhite_pig_id"],
        }

        group_cols = group_cols_map.get(level, group_cols_map["store"])

        # 过滤存在的列
        available_cols = [col for col in group_cols if col in df.columns]
        if not available_cols:
            return pd.DataFrame()

        # 聚合计算
        agg_dict = {}

        if order_id_col in df.columns:
            agg_dict["cust_num_cate"] = (order_id_col, "nunique")
            agg_dict["bf19_cust_num_cate"] = (order_id_col, lambda x: x[df.loc[x.index, "_is_bf19"]].nunique() if df.loc[x.index, "_is_bf19"].any() else 0)
            agg_dict["online_order_num_cate"] = (order_id_col, lambda x: x[df.loc[x.index, "_is_online"]].nunique() if df.loc[x.index, "_is_online"].any() else 0)

        if article_id_col in df.columns:
            agg_dict["sale_article_num_cate"] = (article_id_col, "nunique")

        # 执行聚合
        grouped = df.groupby(available_cols, dropna=False).agg(
            **{k: v for k, v in agg_dict.items()}
        ).reset_index()

        # 添加层级标识
        grouped["level_description"] = level

        # 添加 level_id 列
        level_id_map = {
            "store": "",
            "category1": "category_level1_id",
            "category2": "category_level2_id",
            "category3": "category_level3_id",
            "spu": "spu_id",
            "blackwhite_pig": "blackwhite_pig_id",
        }

        level_id_col = level_id_map.get(level, "")
        if level_id_col and level_id_col in grouped.columns:
            grouped["level_id"] = grouped[level_id_col]
        else:
            grouped["level_id"] = ""

        return grouped

    def calculate_simple(
        self,
        df: pd.DataFrame,
        group_cols: List[str],
        order_id_col: str = "order_id",
        article_id_col: str = "article_id",
    ) -> pd.DataFrame:
        """
        简化计算：按指定分组列聚合客数

        Args:
            df: 订单明细 DataFrame
            group_cols: 分组列
            order_id_col: 订单ID列名
            article_id_col: 商品ID列名

        Returns:
            聚合后的 DataFrame
        """
        agg_dict = {}

        if order_id_col in df.columns:
            agg_dict["cust_num_cate"] = (order_id_col, "nunique")

        if article_id_col in df.columns:
            agg_dict["sale_article_num_cate"] = (article_id_col, "nunique")

        if not agg_dict:
            return pd.DataFrame()

        result = df.groupby(group_cols, dropna=False).agg(**agg_dict).reset_index()

        return result

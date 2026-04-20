"""
指标汇总器

汇总所有计算层指标，生成最终输出
"""

import pandas as pd
import numpy as np
from typing import Optional, List
import logging

from .day_switch_processor import DaySwitchProcessor
from .stock_equation import StockEquationSolver
from .avg_price_calculator import AvgPriceCalculator
from .amount_calculator import AmountCalculator
from .profit_calculator import ProfitCalculator

logger = logging.getLogger(__name__)


class MetricsAggregator:
    """
    指标汇总器

    协调所有计算器，按正确顺序执行计算
    """

    def __init__(self):
        """初始化指标汇总器"""
        self.logger = logger
        self.day_switch_processor = DaySwitchProcessor()
        self.stock_equation_solver = StockEquationSolver()
        self.avg_price_calculator = AvgPriceCalculator()
        self.amount_calculator = AmountCalculator()
        self.profit_calculator = ProfitCalculator()

    def run_calculations(
        self,
        df: pd.DataFrame,
        process_day_switch: bool = True,
        day_clear_col: str = "day_clear",
    ) -> pd.DataFrame:
        """
        执行所有计算

        计算顺序:
        1. 日切处理 (可选)
        2. 库存方程求解
        3. 均价计算
        4. 金额计算
        5. 毛利计算

        Args:
            df: 合并后的原子层数据
            process_day_switch: 是否处理日切
            day_clear_col: 日清标识列名

        Returns:
            计算后的 DataFrame
        """
        df = df.copy()

        self.logger.info(f"Starting calculations with {len(df)} rows")

        # Step 1: 日切处理
        if process_day_switch:
            self.logger.info("Step 1: Processing day switch")
            df = self.day_switch_processor.process(df)

        # Step 2: 库存方程求解
        self.logger.info("Step 2: Solving stock equation")
        df = self.stock_equation_solver.solve(df)

        # Step 3: 均价计算
        self.logger.info("Step 3: Calculating average prices")
        df = self.avg_price_calculator.calculate(df, day_clear_col=day_clear_col)

        # Step 4: 金额计算
        self.logger.info("Step 4: Calculating amounts")
        df = self.amount_calculator.calculate(df)

        # Step 5: 毛利计算
        self.logger.info("Step 5: Calculating profits")
        df = self.profit_calculator.calculate(df, day_clear_col=day_clear_col)

        self.logger.info(f"Calculations completed: {len(df)} rows, {len(df.columns)} columns")

        return df

    def merge_atomic_data(
        self,
        atomic_dfs: dict,
        merge_on: List[str] = None,
        how: str = "outer",
    ) -> pd.DataFrame:
        """
        合并各域原子数据

        Args:
            atomic_dfs: 各域 DataFrame 字典 {domain: df}
            merge_on: 合并键
            how: 合并方式

        Returns:
            合并后的 DataFrame
        """
        if merge_on is None:
            merge_on = ["store_id", "business_date", "article_id"]

        # 获取第一个域作为基础
        domains = list(atomic_dfs.keys())
        if not domains:
            return pd.DataFrame()

        result = atomic_dfs[domains[0]].copy()

        # 逐个合并其他域
        for domain in domains[1:]:
            df = atomic_dfs[domain]
            if df.empty:
                continue

            result = result.merge(df, on=merge_on, how=how, suffixes=("", f"_{domain}"))

        self.logger.info(f"Merged {len(domains)} domains: {len(result)} rows, {len(result.columns)} columns")

        return result

    def add_day_clear_flag(
        self,
        df: pd.DataFrame,
        store_df: pd.DataFrame = None,
        article_df: pd.DataFrame = None,
        day_clear_col: str = "day_clear",
    ) -> pd.DataFrame:
        """
        添加日清标识

        日清判断规则:
        - 翠花店: 使用门店商品维度的 day_clear
        - 非翠花店: 按品类规则判断

        Args:
            df: 数据 DataFrame
            store_df: 门店信息 DataFrame (包含 store_flag)
            article_df: 商品信息 DataFrame (包含 day_clear)
            day_clear_col: 日清标识列名

        Returns:
            添加日清标识后的 DataFrame
        """
        df = df.copy()

        # 默认设为非日清
        df[day_clear_col] = "1"

        # 如果有商品信息，使用商品的日清标识
        if article_df is not None and day_clear_col in article_df.columns:
            merge_cols = ["article_id"]
            if "business_date" in article_df.columns:
                merge_cols.append("business_date")
            if "store_id" in article_df.columns:
                merge_cols.append("store_id")

            available_cols = [col for col in merge_cols if col in article_df.columns]
            if available_cols:
                df = df.merge(
                    article_df[available_cols + [day_clear_col]],
                    on=available_cols,
                    how="left",
                    suffixes=("", "_article")
                )
                df[day_clear_col] = df.get(f"{day_clear_col}_article", df[day_clear_col])

        return df

    def get_output_columns(self) -> List[str]:
        """获取输出列列表"""
        # 维度列
        dimension_cols = ["store_id", "business_date", "article_id", "day_clear"]

        # 数量列
        qty_cols = [
            "sale_qty", "receive_qty", "init_stock_qty", "end_stock_qty",
            "know_lost_qty", "unknow_lost_qty", "lost_qty",
            "compose_in_qty", "compose_out_qty",
        ]

        # 单价列
        price_cols = [
            "cost_price", "avg_purchase_price", "avg_price",
            "current_price", "original_price",
        ]

        # 金额列
        amount_cols = [
            "sale_amt", "receive_amt", "init_stock_amt", "end_stock_amt",
            "lost_amt", "compose_in_amt", "compose_out_amt",
        ]

        # 毛利列
        profit_cols = [
            "profit_amt", "sale_cost_amt", "pre_profit_amt",
            "scm_fin_article_profit", "full_link_article_profit",
        ]

        return dimension_cols + qty_cols + price_cols + amount_cols + profit_cols


def run_calculations(
    settings,
    start_date: str,
    end_date: str,
    use_bom: bool = False,
) -> pd.DataFrame:
    """
    执行计算层的入口函数

    Args:
        settings: 配置对象
        start_date: 开始日期
        end_date: 结束日期
        use_bom: 是否使用 BOM 成本分摊

    Returns:
        计算后的 DataFrame
    """
    from ..utils.parquet_handler import ParquetHandler

    aggregator = MetricsAggregator()
    parquet_handler = ParquetHandler(settings.paths.atomic_dir)

    # 加载各域原子数据
    atomic_dfs = {}
    for domain in settings.atomic_domains:
        try:
            df = parquet_handler.load(domain)
            atomic_dfs[domain] = df
        except FileNotFoundError:
            logger.warning(f"Atomic data not found for domain: {domain}")
            continue

    # 合并原子数据
    merged_df = aggregator.merge_atomic_data(atomic_dfs)

    # 执行计算
    result_df = aggregator.run_calculations(merged_df)

    # 保存计算结果
    output_handler = ParquetHandler(settings.paths.calculated_dir)
    output_handler.save(result_df, "calculated_metrics")

    return result_df

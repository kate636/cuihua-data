"""BOM 拆分管道模块"""
from .bom_relation_loader import BOMRelationLoader
from .cost_splitter import BOMCostSplitter
from .bom_profit_adjuster import BOMProfitAdjuster

__all__ = ["BOMRelationLoader", "BOMCostSplitter", "BOMProfitAdjuster"]


def run_bom_pipeline(
    df,
    bom_relation: "pd.DataFrame" = None,
    bom_loader: "BOMRelationLoader" = None,
    conn=None,
    business_date: str = None,
    method: str = "ratio",
) -> "pd.DataFrame":
    """
    执行 BOM 管道

    Args:
        df: 包含进货、销售数据的 DataFrame
        bom_relation: BOM 关系 DataFrame (可选)
        bom_loader: BOM 关系加载器 (可选)
        conn: 数据库连接 (可选)
        business_date: 业务日期 (可选)
        method: 分摊方法

    Returns:
        分摊后的 DataFrame
    """
    import pandas as pd
    import logging

    logger = logging.getLogger(__name__)

    # 加载 BOM 关系
    if bom_relation is None:
        if bom_loader is None:
            bom_loader = BOMRelationLoader(conn)

        if business_date is None:
            logger.warning("No business_date provided, using empty BOM relation")
            bom_relation = pd.DataFrame()
        else:
            bom_relation = bom_loader.load(business_date)

    if bom_relation.empty:
        logger.warning("BOM relation is empty, returning original data")
        return df

    # 执行成本分摊
    splitter = BOMCostSplitter(bom_relation, method=method)
    df = splitter.split_cost(df)

    # 调整毛利
    adjuster = BOMProfitAdjuster()
    df = adjuster.adjust(df)

    logger.info(f"BOM pipeline completed: {len(df)} rows")

    return df

"""
FM ETL v9.0 主执行器

用法:
    python -m fm_etl_v3.executor 2026-04-20 2026-04-20
    python -m fm_etl_v3.executor 2026-04-20 2026-04-20 --atomic-only
    python -m fm_etl_v3.executor 2026-04-20 2026-04-20 --calc-only
    python -m fm_etl_v3.executor 2026-04-20 2026-04-20 --fm-only

凭证（.env 或环境变量）:
    QDM_ACCESS_KEY      QDM BI API access key
    QDM_SECRET_KEY      QDM BI API secret key
    QDM_API_ID          API ID（默认 i_fjl10g687-790）
    FM_DUCKDB_PATH      DuckDB 文件路径（云端: /opt/fm/data/fm.duckdb）

Pipeline（v9.0，所有写入都落到同一个 DuckDB 文件）:
  Step 1   维度表一次性快照
  Step 2   13 个原子域抽数（receive_sale 为 BOM 核心源；order_receive / article_convert 仅落空骨架）
  Step 3   原子宽表合并 (t_atomic_wide)
  Step 4   库存方程 legacy (t_calc_inventory，观测表)
  Step 5   均价观测 (t_calc_avg_price，v9 profit 不再使用)
  Step 6   BOM 分摊事实 (t_calc_bom_alloc)            ← v9 Σ总权重 + 共享组识别
  Step 7   SKU 有效单位成本 (t_calc_sku_cost)          ← v9 加权平均含期初库存（读昨天 t_calc_stock）
  Step 8   库存与未知损耗 (t_calc_stock)              ← v9 跨日滚动（供明天 Step 7）
  Step 9   金额计算 (t_calc_amounts)                   ← v9 receive_amt 全计算
  Step 10  三层毛利 (t_calc_profit)                    ← v9 切 effective_unit_cost，双口径对齐
  Step 11  FM 商品维度底表 (t_fm_sku_dim)
  Step 12  FM 客数 (t_fm_cust)
  Step 13  FM 分类汇总 (t_fm_levels_sum)
  Step 14  FM 结果层 (t_fm_levels_result)
  Step 15  BOM 分摊溯源 (t_fm_bom_breakdown)
  Step 16  库存滚动展开 (t_fm_stock_roll)
"""

from __future__ import annotations

import sys
import time
from datetime import date, timedelta

from .config import get_settings
from .connectors import ApiConnector, DuckDBStore
from .atomic import (
    SalesExtractor, InventoryExtractor, ScmExtractor, ScmAdjustExtractor,
    LossExtractor, ComposeExtractor, AllowanceExtractor,
    PromoExtractor, CostPriceExtractor, PriceExtractor,
    BomRelationExtractor, ReceiveSaleExtractor,
    OrderReceiveExtractor, ArticleConvertExtractor,
    DimsExtractor,
)
from .calculated import (
    AtomicMerger, InventoryCalculator, AvgPriceCalculator,
    BomAllocCalculator, SkuCostCalculator, StockCalculator,
    AmountsCalculator, ProfitCalculator,
)
from .fm_tables import (
    SkuDimBuilder, CustBuilder, LevelsSumBuilder, LevelsResultBuilder,
    BomBreakdownBuilder, StockRollBuilder,
)
from .utils import get_logger

_log = get_logger("executor")


def run(start: str, end: str, stages: str = "all") -> None:
    """执行 ETL pipeline。

    stages:
        "all"    —— 完整流程
        "atomic" —— 仅抽数 + 合并宽表
        "calc"   —— 仅 calculated 层（需 atomic 已存在）
        "fm"     —— 仅 fm_tables 层（需 calculated 已存在）
    """
    cfg = get_settings()
    yesterday = (date.fromisoformat(end) - timedelta(days=1)).isoformat()

    _log.info(f"═══ FM ETL v9.0 START  {start} ~ {end}  (stage={stages}) ═══")
    t0 = time.time()

    api  = ApiConnector(cfg)
    duck = DuckDBStore()

    try:
        if stages in ("all", "atomic"):
            _run_atomic(api, duck, start, end, yesterday)
            _run_merge(duck, start, end)

        if stages in ("all", "calc"):
            _run_calc(duck)

        if stages in ("all", "fm"):
            _run_fm(duck, api, start, end, yesterday)

    finally:
        duck.close()

    elapsed = time.time() - t0
    _log.info(f"═══ FM ETL v9.0 DONE  elapsed={elapsed:.1f}s ═══")


def _run_atomic(api, duck, start, end, yesterday):
    _step("Step 1: 维度表提取")
    DimsExtractor(api, duck).extract_all(yesterday=yesterday, start=start, end=end)

    _step("Step 2: 原子域提取（13 个）")
    extractors = [
        SalesExtractor(api, duck),
        InventoryExtractor(api, duck),
        ScmExtractor(api, duck),
        ScmAdjustExtractor(api, duck),
        LossExtractor(api, duck),
        ComposeExtractor(api, duck),
        AllowanceExtractor(api, duck),
        PromoExtractor(api, duck),
        CostPriceExtractor(api, duck),
        PriceExtractor(api, duck),
        BomRelationExtractor(api, duck),
        ReceiveSaleExtractor(api, duck),
    ]
    for extractor in extractors:
        extractor.extract(start=start, end=end, yesterday=yesterday)

    # v4 预留：order_receive / article_convert 只落空骨架
    OrderReceiveExtractor(api, duck).ensure_empty_skeleton()
    ArticleConvertExtractor(api, duck).ensure_empty_skeleton()


def _run_merge(duck, start, end):
    _step("Step 3: 原子宽表合并 → t_atomic_wide")
    AtomicMerger(duck).run(start=start, end=end)


def _run_calc(duck):
    _step("Step 4: 库存方程 legacy → t_calc_inventory")
    InventoryCalculator(duck).run()

    _step("Step 5: 均价观测 → t_calc_avg_price（v9 观测，profit 不再使用）")
    AvgPriceCalculator(duck).run()

    _step("Step 6: BOM 分摊事实 → t_calc_bom_alloc [v9 Σ总权重 + 共享组]")
    BomAllocCalculator(duck).run()

    _step("Step 7: SKU 有效单位成本 → t_calc_sku_cost [v9 加权平均含期初]")
    SkuCostCalculator(duck).run()

    _step("Step 8: 库存 & 未知损耗 → t_calc_stock [v9 跨日滚动]")
    StockCalculator(duck).run()

    _step("Step 9: 金额计算 → t_calc_amounts [v9 receive_amt 全计算]")
    AmountsCalculator(duck).run()

    _step("Step 10: 三层毛利 → t_calc_profit [v9 双口径对齐]")
    ProfitCalculator(duck).run()


def _run_fm(duck, api, start, end, yesterday):
    _step("Step 11: FM 商品维度底表 → t_fm_sku_dim")
    SkuDimBuilder(duck).build(start=start, end=end)

    _step("Step 12: FM 客数底表 → t_fm_cust")
    CustBuilder(duck, api).build(start=start, end=end, yesterday=yesterday)

    _step("Step 13: FM 分类汇总 → t_fm_levels_sum")
    LevelsSumBuilder(duck).build(start=start, end=end)

    _step("Step 14: FM 结果层 → t_fm_levels_result")
    LevelsResultBuilder(duck).build(start=start, end=end)

    _step("Step 15: BOM 分摊溯源 → t_fm_bom_breakdown [v9]")
    BomBreakdownBuilder(duck).build(start=start, end=end)

    _step("Step 16: 库存滚动展开 → t_fm_stock_roll [v9]")
    StockRollBuilder(duck).build(start=start, end=end)


def _step(name: str) -> None:
    _log.info(f"───── {name} ─────")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python -m fm_etl_v3.executor <start_date> <end_date> [--atomic-only|--calc-only|--fm-only]")
        print("  e.g.: python -m fm_etl_v3.executor 2026-04-20 2026-04-20")
        sys.exit(1)

    stages = "all"
    if len(sys.argv) >= 4:
        flag = sys.argv[3]
        if flag == "--atomic-only":
            stages = "atomic"
        elif flag == "--calc-only":
            stages = "calc"
        elif flag == "--fm-only":
            stages = "fm"

    run(sys.argv[1], sys.argv[2], stages=stages)

"""
FM ETL v10.0 主执行器

用法:
    python -m fmetl.executor 2026-04-23 2026-04-23
    python -m fmetl.executor 2026-04-23 2026-04-23 --atomic-only
    python -m fmetl.executor 2026-04-23 2026-04-23 --calc-only
    python -m fmetl.executor 2026-04-23 2026-04-23 --fm-only

凭证: QDM_ACCESS_KEY / QDM_SECRET_KEY (.env 或环境变量)

Pipeline (v10.0, 13 步):
  Step 1   维度表快照
  Step 2   13 个原子域抽数
  Step 3   原子宽表合并 (t_atomic_wide)
  Step 4   BOM 分摊 (t_calc_bom_alloc)          ← v10 修复共享组 + 负值保护
  Step 5   SKU 有效单位成本 (t_calc_sku_cost)    ← v10 Python 重写
  Step 6   库存与金额 (t_calc_stock)             ← v10 四流合一，Python 重写
  Step 7   门店毛利 (t_calc_profit)              ← v10 Python 重写，新公式
  Step 8   FM 商品维度底表 (t_fm_sku_dim)
  Step 9   FM 客数 (t_fm_cust)
  Step 10  FM 分类汇总 (t_fm_levels_sum)
  Step 11  FM 结果层 (t_fm_levels_result)
  Step 12  BOM 分摊溯源 (t_fm_bom_breakdown)
  Step 13  库存滚动展开 (t_fm_stock_roll)
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
    InventoryDetailExtractor,
    DimsExtractor,
)
from .calculated import (
    AtomicMerger, BomAllocCalculator, SkuCostCalculator,
    StockCalculator, ProfitCalculator,
)
from .fm_tables import (
    SkuDimBuilder, CustBuilder, LevelsSumBuilder, LevelsResultBuilder,
    BomBreakdownBuilder, StockRollBuilder,
)
from .utils import get_logger

_log = get_logger("executor")


def run(start: str, end: str, stages: str = "all") -> None:
    cfg = get_settings()
    yesterday = (date.fromisoformat(end) - timedelta(days=1)).isoformat()

    _log.info(f"═══ FM ETL v10.0 START  {start} ~ {end}  (stage={stages}) ═══")
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
    _log.info(f"═══ FM ETL v10.0 DONE  elapsed={elapsed:.1f}s ═══")


def _run_atomic(api, duck, start, end, yesterday):
    _step("Step 1: 维度表提取")
    DimsExtractor(api, duck).extract_all(yesterday=yesterday, start=start, end=end)

    _step("Step 2: 原子域提取（14 个）")
    extractors = [
        SalesExtractor(api, duck),
        InventoryExtractor(api, duck),
        InventoryDetailExtractor(api, duck),
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

    OrderReceiveExtractor(api, duck).ensure_empty_skeleton()
    ArticleConvertExtractor(api, duck).ensure_empty_skeleton()


def _run_merge(duck, start, end):
    _step("Step 3: 原子宽表合并 → t_atomic_wide")
    AtomicMerger(duck).run(start=start, end=end)


def _run_calc(duck):
    _step("Step 4: BOM 分摊 → t_calc_bom_alloc [v10 修复]")
    BomAllocCalculator(duck).run()

    _step("Step 5: SKU 有效单位成本 → t_calc_sku_cost [v10 Python]")
    SkuCostCalculator(duck).run()

    _step("Step 6: 库存与金额 → t_calc_stock [v10 四流合一 Python]")
    StockCalculator(duck).run()

    _step("Step 7: 门店毛利 → t_calc_profit [v10 Python 新公式]")
    ProfitCalculator(duck).run()  # debug_categories 仅在排查单品类时传入


def _run_fm(duck, api, start, end, yesterday):
    _step("Step 8: FM 商品维度底表 → t_fm_sku_dim")
    SkuDimBuilder(duck).build(start=start, end=end)

    _step("Step 9: FM 客数底表 → t_fm_cust")
    CustBuilder(duck, api).build(start=start, end=end, yesterday=yesterday)

    _step("Step 10: FM 分类汇总 → t_fm_levels_sum")
    LevelsSumBuilder(duck).build(start=start, end=end)

    _step("Step 11: FM 结果层 → t_fm_levels_result")
    LevelsResultBuilder(duck).build(start=start, end=end)

    _step("Step 12: BOM 分摊溯源 → t_fm_bom_breakdown")
    BomBreakdownBuilder(duck).build(start=start, end=end)

    _step("Step 13: 库存滚动展开 → t_fm_stock_roll")
    StockRollBuilder(duck).build(start=start, end=end)


def _step(name: str) -> None:
    _log.info(f"───── {name} ─────")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python -m fmetl.executor <start_date> <end_date> [--atomic-only|--calc-only|--fm-only]")
        print("  e.g.: python -m fmetl.executor 2026-04-23 2026-04-23")
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

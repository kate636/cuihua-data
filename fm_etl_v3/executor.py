"""
FM ETL v3.0 主执行器

用法:
    python -m fm_etl_v3.executor 2025-01-01 2025-01-31

凭证（.env 或环境变量）:
    QDM_ACCESS_KEY      QDM BI API access key
    QDM_SECRET_KEY      QDM BI API secret key
    QDM_API_ID          API ID（默认 i_fjl10g687-790）
    MOTHERDUCK_TOKEN    MotherDuck token（空则使用本地 DuckDB）
    MOTHERDUCK_DB       MotherDuck 数据库名（默认 fm_etl_v3）
    FM_DUCKDB_PATH      本地 DuckDB 路径（MOTHERDUCK_TOKEN 为空时生效）

Pipeline 顺序：
  Step 1  提取维度表 (一次性快照)
  Step 2  提取 9 个原子域 (分批)
  Step 3  合并原子宽表
  Step 4  库存方程计算
  Step 5  均价计算
  Step 6  金额计算
  Step 7  毛利计算
  Step 8  构建 FM 商品维度底表 → DuckDB/MotherDuck
  Step 9  构建 FM 客数底表     → DuckDB/MotherDuck
  Step 10 构建 FM 分类汇总     → DuckDB/MotherDuck
  Step 11 构建 FM 结果层       → DuckDB/MotherDuck
"""

from __future__ import annotations

import sys
import time
from datetime import date, timedelta

from .config import get_settings
from .connectors import ApiConnector, DuckDBStore
from .atomic import (
    SalesExtractor, InventoryExtractor, ScmExtractor,
    LossExtractor, ComposeExtractor, AllowanceExtractor,
    PromoExtractor, CostPriceExtractor, PriceExtractor,
    DimsExtractor,
)
from .calculated import (
    AtomicMerger, InventoryCalculator, AvgPriceCalculator,
    AmountsCalculator, ProfitCalculator,
)
from .fm_tables import SkuDimBuilder, CustBuilder, LevelsSumBuilder, LevelsResultBuilder
from .utils import get_logger

_log = get_logger("executor")


def run(start: str, end: str) -> None:
    """执行完整 ETL pipeline。"""
    cfg = get_settings()
    yesterday = (date.fromisoformat(end) - timedelta(days=1)).isoformat()

    _log.info(f"═══ FM ETL v3.0 START  {start} ~ {end}  (yesterday={yesterday}) ═══")
    t0 = time.time()

    api  = ApiConnector(cfg)
    duck = DuckDBStore()

    try:
        # ── Step 1: 维度表 ────────────────────────────────────────────────────
        _step("Step 1: 维度表提取")
        DimsExtractor(api, duck).extract_all(yesterday=yesterday, start=start, end=end)

        # ── Step 2: 9 个原子域提取 ───────────────────────────────────────────
        _step("Step 2: 原子域提取")
        extractors = [
            SalesExtractor(api, duck),
            InventoryExtractor(api, duck),
            ScmExtractor(api, duck),
            LossExtractor(api, duck),
            ComposeExtractor(api, duck),
            AllowanceExtractor(api, duck),
            PromoExtractor(api, duck),
            CostPriceExtractor(api, duck),
            PriceExtractor(api, duck),
        ]
        for extractor in extractors:
            extractor.extract(start=start, end=end, yesterday=yesterday)

        # ── Step 3: 原子宽表合并 ─────────────────────────────────────────────
        _step("Step 3: 原子宽表合并")
        AtomicMerger(duck).run(start=start, end=end)

        # ── Step 4: 库存方程 ──────────────────────────────────────────────────
        _step("Step 4: 库存方程计算")
        InventoryCalculator(duck).run()

        # ── Step 5: 均价计算 ──────────────────────────────────────────────────
        _step("Step 5: 均价计算")
        AvgPriceCalculator(duck).run()

        # ── Step 6: 金额计算 ──────────────────────────────────────────────────
        _step("Step 6: 金额计算")
        AmountsCalculator(duck).run()

        # ── Step 7: 毛利计算 ──────────────────────────────────────────────────
        _step("Step 7: 毛利计算")
        ProfitCalculator(duck).run()

        # ── Step 8: FM 商品维度底表 ───────────────────────────────────────────
        _step("Step 8: FM 商品维度底表")
        SkuDimBuilder(duck).build(start=start, end=end)

        # ── Step 9: FM 客数底表 ───────────────────────────────────────────────
        _step("Step 9: FM 客数底表")
        CustBuilder(duck, api).build(start=start, end=end, yesterday=yesterday)

        # ── Step 10: FM 分类汇总 ──────────────────────────────────────────────
        _step("Step 10: FM 分类汇总")
        LevelsSumBuilder(duck).build(start=start, end=end)

        # ── Step 11: FM 结果层 ────────────────────────────────────────────────
        _step("Step 11: FM 结果层")
        LevelsResultBuilder(duck).build(start=start, end=end)

    finally:
        duck.close()

    elapsed = time.time() - t0
    _log.info(f"═══ FM ETL v3.0 DONE  elapsed={elapsed:.1f}s ═══")


def _step(name: str) -> None:
    _log.info(f"───── {name} ─────")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python -m fm_etl_v3.executor <start_date> <end_date>")
        print("  e.g.: python -m fm_etl_v3.executor 2025-01-01 2025-01-31")
        sys.exit(1)
    run(sys.argv[1], sys.argv[2])

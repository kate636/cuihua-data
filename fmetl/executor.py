"""
FM ETL v0.10 主执行器

用法:
    python -m fmetl.executor 2026-04-23 2026-04-23
    python -m fmetl.executor 2026-04-23 2026-04-23 --atomic-only
    python -m fmetl.executor 2026-04-23 2026-04-23 --calc-only
    python -m fmetl.executor 2026-04-23 2026-04-23 --fm-only

凭证: QDM_ACCESS_KEY / QDM_SECRET_KEY (.env 或环境变量)

Pipeline (v0.10, 13 步):
  Step 1   维度表快照
  Step 2   13 个原子域抽数
  Step 3   原子宽表合并 (t_atomic_wide)
  Step 4   BOM 分摊 (t_calc_bom_alloc)          ← v0.10 修复共享组 + 负值保护
  Step 5   SKU 有效单位成本 (t_calc_sku_cost)    ← v0.10 Python 重写
  Step 6   库存与金额 (t_calc_stock)             ← v0.10 四流合一，Python 重写
  Step 7   门店毛利 (t_calc_profit)              ← v0.10 Python 重写，新公式
  Step 8   FM 商品维度底表 (t_fm_sku_dim)
  Step 9   FM 客数 (t_fm_cust)
  Step 10  FM 分类汇总 (t_fm_levels_sum)
  Step 11  FM 结果层 (t_fm_levels_result)
  Step 12  BOM 分摊溯源 (t_fm_bom_breakdown)
  Step 13  库存滚动展开 (t_fm_stock_roll)
  Step 14  同步加工关系候选数据到云端
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

    _log.info(f"═══ FM ETL v0.10 START  {start} ~ {end}  (stage={stages}) ═══")
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
    _log.info(f"═══ FM ETL v0.10 DONE  elapsed={elapsed:.1f}s ═══")


def _run_atomic(api, duck, start, end, yesterday):
    _step("Step 1: 维度表提取")
    DimsExtractor(api, duck).extract_all(yesterday=yesterday, start=start, end=end)

    _step("Step 2: 原子域提取（14 个，多核并行）")
    extractor_classes = [
        SalesExtractor, InventoryExtractor, InventoryDetailExtractor,
        ScmExtractor, ScmAdjustExtractor, LossExtractor, ComposeExtractor,
        AllowanceExtractor, PromoExtractor, CostPriceExtractor, PriceExtractor,
        BomRelationExtractor, ReceiveSaleExtractor,
    ]

    def _extract_one(cls):
        """每个线程创建独立的 ApiConnector + DuckDB 连接，避免锁竞争。"""
        t_api = ApiConnector(get_settings())
        t_duck = DuckDBStore()
        try:
            cls(t_api, t_duck).extract(start=start, end=end, yesterday=yesterday, chunk=30)
        finally:
            t_duck.close()

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_extract_one, cls): cls for cls in extractor_classes}
        for f in as_completed(futures):
            cls = futures[f]
            try:
                f.result()
            except Exception as ex:
                _log.error(f"{cls.__name__} FAILED: {ex}")

    OrderReceiveExtractor(api, duck).ensure_empty_skeleton()
    ArticleConvertExtractor(api, duck).ensure_empty_skeleton()


def _run_merge(duck, start, end):
    _step("Step 3: 原子宽表合并 → t_atomic_wide")
    AtomicMerger(duck).run(start=start, end=end)


def _run_calc(duck):
    _step("Step 4: BOM 分摊 → t_calc_bom_alloc [v0.10 修复]")
    BomAllocCalculator(duck).run()

    _step("Step 5: SKU 有效单位成本 → t_calc_sku_cost [v0.10 Python]")
    SkuCostCalculator(duck).run()

    _step("Step 6: 库存与金额 → t_calc_stock [v0.10 四流合一 Python]")
    StockCalculator(duck).run()

    _step("Step 7: 门店毛利 → t_calc_profit [v0.10 Python 新公式]")
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

    _step("Step 14: 同步加工关系候选数据 → 云端")
    _sync_processing_candidates(duck)


def _sync_processing_candidates(duck):
    """从 DuckDB 提取烘焙+熟食类候选 SKU，同步到云端加工关系管理系统。"""
    import sqlite3
    import tempfile

    try:
        conn = duck._conn
    except Exception:
        return

    # 查询有销售的加工类 SKU（品类按 master-data v2.3 映射为报告品类）
    try:
        df = conn.execute("""
            WITH sku_sales AS (
                SELECT p.article_id,
                       SUM(p.pre_sale_amt) as total_sales
                FROM t_calc_profit p
                JOIN dim_goods g ON p.article_id = g.article_id
                WHERE (g.category_level2_description = '烘焙类'
                       OR g.category_level3_description LIKE '%熟食'
                       OR g.category_level2_description = '方便速食类'
                       OR g.category_level2_description IN ('即食类','即热类','即烹类')
                       OR (g.category_level1_description = '预制菜' AND g.sale_unit = '千克'))
                  AND p.pre_sale_amt > 0
                GROUP BY p.article_id
            )
            SELECT g.article_id, g.article_name,
                   CASE
                        WHEN g.category_level2_description = '烘焙类' THEN '烘焙类'
                        WHEN g.category_level3_description LIKE '%熟食' THEN '熟食类'
                        WHEN g.category_level2_description IN ('即烹类','即热类') THEN '熟食类'
                        WHEN (g.category_level1_description = '预制菜' AND g.sale_unit = '千克') THEN '熟食类'
                        WHEN g.category_level2_description = '方便速食类' THEN '方便速食类'
                        ELSE g.category_level2_description
                   END as category_type,
                   ROUND(COALESCE(s.total_sales, 0), 1) as total_sales,
                   0 as relation_count
            FROM dim_goods g
            INNER JOIN sku_sales s ON g.article_id = s.article_id
            ORDER BY s.total_sales DESC
        """).df()
    except Exception:
        return

    if df.empty:
        return

    _log.info(f"syncing {len(df)} processing candidates to cloud ...")

    # 写入临时 SQLite → SCP 到云端
    try:
        tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        tmp.close()
        sdb = sqlite3.connect(tmp.name)
        df.to_sql("proc_candidates", sdb, if_exists="replace", index=False)
        sdb.execute("CREATE INDEX IF NOT EXISTS idx_pc_id ON proc_candidates(article_id)")
        sdb.commit()
        sdb.close()

        import subprocess
        ssh_key = os.path.expanduser("~/.ssh/id_rsa")
        subprocess.run([
            "scp", "-i", ssh_key, "-o", "StrictHostKeyChecking=no",
            tmp.name, "root@47.115.213.115:/opt/fm/proc-rel/proc_candidates.db",
        ], check=True, capture_output=True, timeout=30)

        subprocess.run([
            "ssh", "-i", ssh_key, "-o", "StrictHostKeyChecking=no",
            "root@47.115.213.115",
            "python3 -c \"import sqlite3; db=sqlite3.connect('/opt/fm/proc-rel/processing_relation.db'); "
            "db.execute('DROP TABLE IF EXISTS proc_candidates'); "
            "db.execute('ATTACH DATABASE \\\"/opt/fm/proc-rel/proc_candidates.db\\\" AS cand'); "
            "db.execute('CREATE TABLE proc_candidates AS SELECT * FROM cand.proc_candidates'); "
            "db.execute('UPDATE proc_candidates SET relation_count = (SELECT COUNT(*) FROM processing_relation pr WHERE pr.finished_sku = proc_candidates.article_id AND pr.is_active = 1)'); "
            "db.commit(); db.close()\"",
        ], check=True, capture_output=True, timeout=30)

        os.unlink(tmp.name)
        _log.info(f"processing candidates synced: {len(df)} SKUs")
    except Exception as e:
        _log.warning(f"processing candidate sync failed (non-fatal): {e}")


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

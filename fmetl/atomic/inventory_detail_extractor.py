"""
域②附 库存明细取数器 (v10 新增)

源表: strategy_fm_store_article_inventory_detail_di
目标: DuckDB atomic_inventory_detail

取 actual_stock_qty + created_by，判断是否真正盘点：
- created_by != '系统' → 人工盘点，信任实盘值覆盖 end_stock
- created_by = '系统' → 系统快照，继续用方程自算
"""

from ._base import BaseExtractor


class InventoryDetailExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_inventory_detail"

    def extract(self, start: str, end: str, yesterday: str, chunk: int = 7) -> None:
        """分区写入模式，保留历史数据。"""
        super().extract(start=start, end=end, yesterday=yesterday, chunk=chunk)

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        return f"""
        SELECT
            shop_id                 AS store_id,
            inventory_date          AS business_date,
            sku_code                AS article_id,
            MAX(actual_stock_qty)   AS actual_stock_qty,
            MAX(created_by)         AS created_by
        FROM strategy_fm_store_article_inventory_detail_di
        WHERE inventory_date BETWEEN '{start}' AND '{end}'
        GROUP BY shop_id, inventory_date, sku_code
        """

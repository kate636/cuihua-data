"""
库存方程计算器

从 t_atomic_wide 推算 unknow_lost_qty、end_stock_qty，写入 t_calc_inventory。

库存方程（标准形式）:
    end_stock_qty = init_stock_qty + receive_qty + compose_in_qty
                  - compose_out_qty - sale_qty - know_lost_qty - unknow_lost_qty

有盘点数据时 (end_stock_qty_raw > 0):
    end_stock_qty  = end_stock_qty_raw (来自 BOM 拆分表的期末库存快照)
    unknow_lost_qty = init_stock_qty + receive_qty + compose_in_qty
                    - compose_out_qty - sale_qty - know_lost_qty - end_stock_qty

无盘点数据时 (end_stock_qty_raw = 0 且 init_stock_qty = 0):
    unknow_lost_qty = 0
    end_stock_qty   = init_stock_qty + receive_qty + compose_in_qty
                    - compose_out_qty - sale_qty - know_lost_qty
"""

from __future__ import annotations

from ..connectors import DuckDBStore
from ..utils import get_logger


class InventoryCalculator:
    TARGET_TABLE = "t_calc_inventory"

    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("InventoryCalculator")

    def run(self) -> None:
        """基于 t_atomic_wide 计算库存方程，写入 t_calc_inventory。"""
        self._log.info("calculating inventory equation ...")
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
        self._duck.execute(f"""
        CREATE TABLE {self.TARGET_TABLE} AS
        SELECT
            store_id,
            business_date,
            article_id,
            day_clear,
            init_stock_qty,
            -- 期末库存：优先使用 BOM 表的期末快照，其次由方程推算
            CASE
                WHEN end_stock_qty_raw <> 0 OR init_stock_qty <> 0
                THEN end_stock_qty_raw
                ELSE GREATEST(0,
                    init_stock_qty + receive_qty + compose_in_qty
                    - compose_out_qty - sale_qty - know_lost_qty
                )
            END AS end_stock_qty,
            -- 未知损耗：BOM 快照期末有值时反推，否则为 0
            CASE
                WHEN end_stock_qty_raw <> 0 OR init_stock_qty <> 0
                THEN init_stock_qty + receive_qty + compose_in_qty
                     - compose_out_qty - sale_qty - know_lost_qty
                     - end_stock_qty_raw
                ELSE 0
            END AS unknow_lost_qty,
            receive_qty,
            compose_in_qty,
            compose_out_qty,
            sale_qty,
            know_lost_qty
        FROM t_atomic_wide
        """)
        rows = self._duck.row_count(self.TARGET_TABLE)
        self._log.info(f"t_calc_inventory: {rows} rows")

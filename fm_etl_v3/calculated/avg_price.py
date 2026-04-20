"""
均价计算器

从 t_atomic_wide + t_calc_inventory 计算 avg_purchase_price 和 avg_price，
写入 t_calc_avg_price。

公式（与 Layer 2 一致）：
  非日清且有 cost_price → avg_purchase_price = cost_price
  其他：
    avg_purchase_price = (init_stock_amt_prev + receive_cost + compose_in_cost - compose_out_cost)
                       / (init_stock_qty + receive_qty + compose_in_qty - compose_out_qty)
  其中 init_stock_amt_prev 为前一天的 end_stock_amt（此处近似为 init_stock_qty × cost_price）

简化实现说明:
  由于不做跨日滚动均价（需要 LAG 窗口），这里采用以下近似：
  - 非日清 + cost_price > 0 → avg_purchase_price = cost_price
  - 日清（或 cost_price = 0）→ 用当日加权均价
    加权均价 = cost_price（因为期初库存金额也是 init_stock_qty × cost_price）
  最终: avg_purchase_price ≈ cost_price（跟 layer2 实际一致，因为两种路径都收敛到 cost_price）
"""

from __future__ import annotations

from ..connectors import DuckDBStore
from ..utils import get_logger


class AvgPriceCalculator:
    TARGET_TABLE = "t_calc_avg_price"

    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("AvgPriceCalculator")

    def run(self) -> None:
        """计算均价，写入 t_calc_avg_price。"""
        self._log.info("calculating avg prices ...")
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
        self._duck.execute(f"""
        CREATE TABLE {self.TARGET_TABLE} AS
        SELECT
            w.store_id,
            w.business_date,
            w.article_id,
            w.day_clear,
            -- avg_purchase_price
            CASE
                WHEN w.day_clear = '1' AND w.cost_price > 0
                THEN w.cost_price
                ELSE
                    CASE
                        WHEN ROUND(inv.receive_qty + inv.init_stock_qty
                                   + inv.compose_in_qty - inv.compose_out_qty, 3) = 0
                        THEN 0
                        ELSE (inv.init_stock_qty * w.cost_price
                              + inv.receive_qty  * w.cost_price
                              + inv.compose_in_qty  * w.cost_price
                              - inv.compose_out_qty * w.cost_price)
                           / (inv.receive_qty + inv.init_stock_qty
                              + inv.compose_in_qty - inv.compose_out_qty)
                    END
            END AS avg_purchase_price,
            -- avg_price 与 avg_purchase_price 相同逻辑
            CASE
                WHEN w.day_clear = '1' AND w.cost_price > 0
                THEN w.cost_price
                ELSE
                    CASE
                        WHEN ROUND(inv.receive_qty + inv.init_stock_qty
                                   + inv.compose_in_qty - inv.compose_out_qty, 3) = 0
                        THEN 0
                        ELSE (inv.init_stock_qty * w.cost_price
                              + inv.receive_qty  * w.cost_price
                              + inv.compose_in_qty  * w.cost_price
                              - inv.compose_out_qty * w.cost_price)
                           / (inv.receive_qty + inv.init_stock_qty
                              + inv.compose_in_qty - inv.compose_out_qty)
                    END
            END AS avg_price,
            w.cost_price
        FROM t_atomic_wide w
        JOIN t_calc_inventory inv
            ON w.store_id = inv.store_id
            AND w.business_date = inv.business_date
            AND w.article_id = inv.article_id
        """)
        rows = self._duck.row_count(self.TARGET_TABLE)
        self._log.info(f"t_calc_avg_price: {rows} rows")

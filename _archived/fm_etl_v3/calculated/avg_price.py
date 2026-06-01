"""
均价计算器（v4.0 · 观测表）

v4 架构下 "有效单位成本" 的权威源是 t_calc_sku_cost，本表仅保留
 cost_price / avg_inbound_price 的直接观测值，供下游 DQ 对账。

输出 t_calc_avg_price（和 v3.1 同名，字段兼容 downstream）:
    avg_purchase_price  — COALESCE(cost_price, avg_inbound_price, 0)
    avg_price           — 同上（历史字段保留）
    cost_price          — atomic_cost_price.cost_price 原始值
    avg_inbound_price   — strategy_fm_purchase_di 官方入库均价
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
        self._log.info("calculating avg prices (v4.0 观测表) ...")
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
        self._duck.execute(f"""
        CREATE TABLE {self.TARGET_TABLE} AS
        SELECT
            w.store_id,
            w.business_date,
            w.article_id,
            w.day_clear,
            COALESCE(NULLIF(w.cost_price, 0),
                     NULLIF(w.avg_inbound_price, 0), 0)    AS avg_purchase_price,
            COALESCE(NULLIF(w.cost_price, 0),
                     NULLIF(w.avg_inbound_price, 0), 0)    AS avg_price,
            w.cost_price,
            w.avg_inbound_price
        FROM t_atomic_wide w
        """)
        rows = self._duck.row_count(self.TARGET_TABLE)
        self._log.info(f"t_calc_avg_price: {rows} rows")

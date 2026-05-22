"""
t_fm_stock_roll — 库存滚动展开表 (v10)

四流分离展示:
  init + receive + bom_in - bom_out + compose_in - compose_out
  - sale - know_lost - unknow_lost = end

输入: t_calc_stock + dim_goods + dim_chdj_store_info + dim_store_profile
输出: t_fm_stock_roll
"""

from __future__ import annotations

from ..connectors import DuckDBStore
from ..utils import get_logger

TARGET_DUCK_TABLE = "t_fm_stock_roll"


class StockRollBuilder:
    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("StockRollBuilder")

    def build(self, start: str, end: str) -> None:
        self._log.info(f"building {TARGET_DUCK_TABLE}: {start} ~ {end}")
        self._duck.execute(f"DROP TABLE IF EXISTS {TARGET_DUCK_TABLE}")
        self._duck.execute(f"""
        CREATE TABLE {TARGET_DUCK_TABLE} AS
        SELECT
            stk.store_id,
            stk.business_date,
            COALESCE(ch.store_no, '')                  AS store_no,
            COALESCE(sp.store_name, '')                AS store_name,
            stk.article_id,
            COALESCE(g.article_name, '')               AS article_name,
            COALESCE(g.category_level1_description, '') AS category_level1_description,
            COALESCE(g.category_level2_description, '') AS category_level2_description,
            COALESCE(g.category_level3_description, '') AS category_level3_description,
            stk.day_clear,

            -- 期初
            stk.init_stock_qty,
            stk.init_stock_amt,

            -- 四流入
            stk.receive_qty,
            stk.receive_amt,
            stk.bom_in_qty,
            stk.bom_in_amt,
            stk.compose_in_qty,
            stk.compose_in_amt,

            -- 三流出
            stk.bom_out_qty,
            stk.bom_out_amt,
            stk.compose_out_qty,
            stk.compose_out_amt,

            -- 销售
            stk.sale_qty,
            stk.sale_amt,

            -- 损耗
            stk.know_lost_qty,
            stk.know_lost_amt,
            stk.unknow_lost_qty,
            stk.unknow_lost_amt,

            -- 期末
            stk.end_stock_qty,
            stk.end_stock_amt,

            -- 成本
            stk.effective_unit_cost,
            stk.cost_source,

            -- 校验: 方程残差
            (stk.init_stock_qty
             + stk.receive_qty
             + stk.bom_in_qty - stk.bom_out_qty
             + stk.compose_in_qty - stk.compose_out_qty
             - stk.sale_qty
             - stk.know_lost_qty - stk.unknow_lost_qty
             - stk.end_stock_qty)                       AS balance_qty

        FROM t_calc_stock stk
        LEFT JOIN (SELECT DISTINCT article_id, article_name,
                          category_level1_description,
                          category_level2_description,
                          category_level3_description,
                          sale_unit, unit_weight
                   FROM dim_goods) g
            ON stk.article_id = g.article_id
        LEFT JOIN dim_chdj_store_info ch ON stk.store_id = ch.store_id
        LEFT JOIN dim_store_profile sp   ON stk.store_id = sp.store_id
        WHERE stk.business_date BETWEEN '{start}' AND '{end}'
        """)
        rows = self._duck.row_count(TARGET_DUCK_TABLE)
        self._log.info(f"{TARGET_DUCK_TABLE}: {rows} rows")

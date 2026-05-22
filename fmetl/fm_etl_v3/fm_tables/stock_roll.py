"""
t_fm_stock_roll — 库存滚动展开表（v4.0 新增）

面向 AI 工作流：把每个 SKU 当天的库存八要素显式展平到一行：

    init + receive + compose_in - compose_out - sale - know_lost - unknow_lost = end

字段:
    store_id / business_date / store_no / store_name
    article_id / article_name
    category_level1_description / category_level2_description / category_level3_description
    day_clear
    -- 数量
    init_stock_qty, receive_qty, compose_in_qty, compose_out_qty,
    sale_qty, know_lost_qty, unknow_lost_qty, end_stock_qty
    -- 金额
    init_stock_amt, receive_amt, compose_in_amt, compose_out_amt,
    sale_amt, know_lost_amt, unknow_lost_amt, end_stock_amt,
    -- 成本锚
    effective_unit_cost, cost_source,
    -- 校验
    end_stock_amt_official, end_stock_amt_diff

典型查询:
    -- AI: "为什么这个 sku 今天期末库存是 X？"
    --     → 返回八要素展开，马上可以看出哪项异常
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

            -- 数量滚动
            stk.init_stock_qty,
            stk.receive_qty,
            stk.compose_in_qty,
            stk.compose_out_qty,
            stk.sale_qty,
            stk.know_lost_qty,
            stk.unknow_lost_qty,
            stk.end_stock_qty,

            -- 金额滚动
            amt.init_stock_amt,
            amt.receive_amt,
            amt.compose_in_amt,
            amt.compose_out_amt,
            w.sale_amt,
            amt.know_lost_amt,
            amt.unknow_lost_amt,
            amt.end_stock_amt,

            -- 成本锚
            stk.effective_unit_cost,
            stk.cost_source,

            -- D1-b 校验字段
            stk.end_stock_amt_official,
            stk.end_stock_amt_diff

        FROM t_calc_stock stk
        LEFT JOIN t_calc_amounts amt
            ON amt.store_id      = stk.store_id
           AND amt.business_date = stk.business_date
           AND amt.article_id    = stk.article_id
           AND amt.day_clear     = stk.day_clear
        LEFT JOIN t_atomic_wide w
            ON w.store_id      = stk.store_id
           AND w.business_date = stk.business_date
           AND w.article_id    = stk.article_id
           AND w.day_clear     = stk.day_clear
        LEFT JOIN (SELECT DISTINCT article_id, article_name, category_level1_description, category_level2_description, category_level3_description, sale_unit, unit_weight FROM dim_goods) g ON stk.article_id = g.article_id
        LEFT JOIN dim_chdj_store_info ch ON stk.store_id = ch.store_id
        LEFT JOIN dim_store_profile sp   ON stk.store_id = sp.store_id
        WHERE stk.business_date BETWEEN '{start}' AND '{end}'
        """)
        rows = self._duck.row_count(TARGET_DUCK_TABLE)
        self._log.info(f"{TARGET_DUCK_TABLE}: {rows} rows")

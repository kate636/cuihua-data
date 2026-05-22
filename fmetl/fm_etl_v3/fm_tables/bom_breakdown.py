"""
t_fm_bom_breakdown — BOM 分摊溯源表（v4.0 新增）

面向 AI 工作流：提供"某 sub sku 今天的进货/成本是由哪些 parent 分摊来的"
的可读答案，粒度精到 (store, date, parent, sub)。

字段:
    store_id / business_date / store_no / store_name
    parent_article_id / parent_article_name
    sub_article_id / sub_article_name
    category_level1_description / category_level2_description
    parent_inbound_qty / parent_inbound_amount / parent_unit_price
    dressing_rate / cost_rate_effective / cost_rate_source
    sub_qty_actual / sub_qty_source
    sub_alloc_amt / sub_unit_cost

典型查询:
    SELECT * FROM t_fm_bom_breakdown
    WHERE sub_article_id = '12345'
      AND business_date = '2026-04-20';

    -- AI: "为什么优鲜黑猪梅头肉今天毛利为负？"
    --     → 返回 parent=优鲜黑猪A级, inbound=500 元/kg, dressing=0.12, ...
"""

from __future__ import annotations

from ..connectors import DuckDBStore
from ..utils import get_logger

TARGET_DUCK_TABLE = "t_fm_bom_breakdown"


class BomBreakdownBuilder:
    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("BomBreakdownBuilder")

    def build(self, start: str, end: str) -> None:
        self._log.info(f"building {TARGET_DUCK_TABLE}: {start} ~ {end}")
        self._duck.execute(f"DROP TABLE IF EXISTS {TARGET_DUCK_TABLE}")
        self._duck.execute(f"""
        CREATE TABLE {TARGET_DUCK_TABLE} AS
        SELECT
            ba.store_id,
            ba.business_date,
            COALESCE(ch.store_no, '')                  AS store_no,
            COALESCE(sp.store_name, '')                AS store_name,
            ba.parent_article_id,
            COALESCE(gp.article_name, '')              AS parent_article_name,
            ba.sub_article_id,
            COALESCE(gs.article_name, '')              AS sub_article_name,
            COALESCE(gs.category_level1_description, '') AS category_level1_description,
            COALESCE(gs.category_level2_description, '') AS category_level2_description,
            ba.parent_inbound_qty,
            ba.parent_inbound_amount,
            ba.parent_unit_price,
            ba.dressing_rate,
            ba.cost_rate_effective,
            ba.cost_rate_source,
            ba.sub_qty_actual,
            ba.sub_qty_source,
            ba.sub_alloc_amt,
            ba.sub_unit_cost
        FROM t_calc_bom_alloc ba
        LEFT JOIN (SELECT DISTINCT article_id, article_name FROM dim_goods) gp ON ba.parent_article_id = gp.article_id
        LEFT JOIN (SELECT DISTINCT article_id, article_name, category_level1_description, category_level2_description FROM dim_goods) gs ON ba.sub_article_id    = gs.article_id
        LEFT JOIN dim_chdj_store_info ch ON ba.store_id = ch.store_id
        LEFT JOIN dim_store_profile sp   ON ba.store_id = sp.store_id
        WHERE ba.business_date BETWEEN '{start}' AND '{end}'
        """)
        rows = self._duck.row_count(TARGET_DUCK_TABLE)
        self._log.info(f"{TARGET_DUCK_TABLE}: {rows} rows")

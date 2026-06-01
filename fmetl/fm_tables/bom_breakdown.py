"""
t_fm_bom_breakdown — BOM 分摊溯源表 (v10)

粒度: (store, date, parent, sub)
输入: t_calc_bom_alloc + dim_goods + dim_chdj_store_info + dim_store_profile
输出: t_fm_bom_breakdown
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
        sel_sql = f"""
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
        LEFT JOIN (SELECT DISTINCT article_id, article_name FROM dim_goods) gp
            ON ba.parent_article_id = gp.article_id
        LEFT JOIN (SELECT DISTINCT article_id, article_name,
                          category_level1_description, category_level2_description
                   FROM dim_goods) gs
            ON ba.sub_article_id = gs.article_id
        LEFT JOIN dim_chdj_store_info ch ON ba.store_id = ch.store_id
        LEFT JOIN dim_store_profile sp   ON ba.store_id = sp.store_id
        WHERE ba.business_date BETWEEN '{start}' AND '{end}'
        """
        # 首次建表（空结构）
        self._duck.execute(f"CREATE TABLE IF NOT EXISTS {TARGET_DUCK_TABLE} AS {sel_sql} LIMIT 0")
        # 按日期分区覆盖
        self._duck.execute(f"DELETE FROM {TARGET_DUCK_TABLE} WHERE business_date BETWEEN '{start}' AND '{end}'")
        self._duck.execute(f"INSERT INTO {TARGET_DUCK_TABLE} {sel_sql}")
        rows = self._duck.row_count(TARGET_DUCK_TABLE)
        self._log.info(f"{TARGET_DUCK_TABLE}: {rows} rows")

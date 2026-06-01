"""
域② 进货库存域取数器

源表: strategy_fm_purchase_di  (已聚合到 store × article × business_date × day_clear)
辅表: strategy_fm_dim_goods    (category_level1_id 排除物料类)
目标: DuckDB atomic_inventory

⭐ v3.1.2 (2026-04-23):
  strategy_fm_purchase_di 本身就是"官方 BOM 分摊表"，除了 sale_article_qty 外，
  还有 sale_article_purchase_amt（当天销售 sub 的分摊进货额，已按根 parent 的
  avg_inbound_price 预分摊），我们之前完全没用，导致散称生鲜成本全靠自造 BOM 推。
  现在把 purchase_amt 也提出来，作为 receive_amt 的主口径（对应门店级成本）。

原子字段: receive_qty, receive_amt, avg_inbound_price,
          init_stock_qty, init_stock_amt, end_stock_qty, end_stock_amt
"""

from ._base import BaseExtractor


class InventoryExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_inventory"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        mat_excl = "('70','71','72','73','74','75','76','77')"
        return f"""
        SELECT
            m1.store_id,
            m1.business_date,
            m1.sale_article_id                      AS article_id,
            SUM(m1.sale_article_qty)                AS receive_qty,
            SUM(m1.sale_article_purchase_amt)       AS receive_amt,
            -- 根 parent 均价（取 qty 加权平均；若无 qty 则简单均值）
            CASE
                WHEN SUM(m1.sale_article_qty) > 0
                THEN SUM(m1.sale_article_qty * m1.avg_inbound_price)
                   / SUM(m1.sale_article_qty)
                ELSE AVG(m1.avg_inbound_price)
            END                                     AS avg_inbound_price,
            SUM(m1.init_stock_qty)                  AS init_stock_qty,
            SUM(m1.init_stock_amt)                  AS init_stock_amt,
            SUM(m1.end_stock_qty)                   AS end_stock_qty,
            SUM(m1.end_stock_amt)                   AS end_stock_amt,
            m1.day_clear
        FROM (
            SELECT
                business_date,
                store_id,
                sale_article_id,
                sale_article_qty,
                sale_article_purchase_amt,
                avg_inbound_price,
                init_stock_qty,
                init_stock_amt,
                end_stock_qty,
                end_stock_amt,
                day_clear
            FROM strategy_fm_purchase_di
            WHERE inc_day BETWEEN '{start}' AND '{end}'
        ) m1
        LEFT JOIN (
            SELECT DISTINCT article_id, category_level1_id
            FROM strategy_fm_dim_goods
            WHERE inc_day = '{end}'
        ) m2 ON m1.sale_article_id = m2.article_id
        WHERE m2.category_level1_id NOT IN {mat_excl}
        GROUP BY
            m1.store_id,
            m1.business_date,
            m1.sale_article_id,
            m1.day_clear
        """

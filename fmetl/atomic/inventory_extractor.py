"""
域② 库存域取数器 (v10)

源表: strategy_fm_purchase_di
目标: DuckDB atomic_inventory

v10 变更: 只取 init_stock + avg_inbound_price + purchase_receive。
receive_qty/amt 主源从 receive_sale_di 取，purchase_di 作回退；
end_stock_qty/amt 由 stock.py 自算。
"""

from ._base import BaseExtractor


class InventoryExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_inventory"

    def extract(self, start: str, end: str, yesterday: str, chunk: int = 7) -> None:
        """v10: 先 DROP 旧表（schema 变了），再走标准分区写入。"""
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
        super().extract(start=start, end=end, yesterday=yesterday, chunk=chunk)
        # 防止 API 返回 0 行时下游 merge 崩溃
        self._ensure_table_exists(f"""
            CREATE TABLE {self.TARGET_TABLE} (
                store_id VARCHAR, business_date VARCHAR, article_id VARCHAR,
                avg_inbound_price DOUBLE, init_stock_qty DOUBLE, init_stock_amt DOUBLE,
                purchase_receive_qty DOUBLE, purchase_receive_amt DOUBLE, day_clear VARCHAR
            )
        """)

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        mat_excl = "('70','71','72','73','74','75','76','77')"
        return f"""
        SELECT
            m1.store_id,
            m1.business_date,
            m1.sale_article_id                      AS article_id,
            CASE
                WHEN SUM(m1.sale_article_qty) > 0
                THEN SUM(m1.sale_article_qty * m1.avg_inbound_price)
                   / SUM(m1.sale_article_qty)
                ELSE AVG(m1.avg_inbound_price)
            END                                     AS avg_inbound_price,
            SUM(m1.init_stock_qty)                  AS init_stock_qty,
            SUM(m1.init_stock_amt)                  AS init_stock_amt,
            SUM(m1.sale_article_qty)                AS purchase_receive_qty,
            SUM(m1.sale_article_purchase_amt)       AS purchase_receive_amt,
            m1.day_clear
        FROM (
            SELECT
                business_date,
                store_id,
                sale_article_id,
                sale_article_qty,
                avg_inbound_price,
                init_stock_qty,
                init_stock_amt,
                sale_article_purchase_amt,
                day_clear
            FROM strategy_fm_purchase_di
            WHERE inc_day BETWEEN '{start}' AND '{end}'
        ) m1
        LEFT JOIN (
            SELECT DISTINCT article_id, category_level1_id
            FROM strategy_fm_dim_goods
            WHERE inc_day = (SELECT MAX(inc_day) FROM strategy_fm_dim_goods)
        ) m2 ON m1.sale_article_id = m2.article_id
        WHERE m2.category_level1_id NOT IN {mat_excl}
        GROUP BY
            m1.store_id,
            m1.business_date,
            m1.sale_article_id,
            m1.day_clear
        """

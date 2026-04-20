"""
域④ 损耗域取数器

源表: hive.dal.dal_transaction_store_article_lost_di
目标: DuckDB atomic_loss
原子字段: know_lost_qty (只有已知损耗数量)

注意: unknow_lost_qty 由计算层的库存方程反推，不在此处提取。
"""

from ._base import BaseExtractor


class LossExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_loss"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        mat_excl = "('70','71','72','73','74','75','76','77','98')"
        return f"""
        SELECT
            t1.store_id,
            t1.inc_day                  AS business_date,
            t1.article_id,
            SUM(t1.know_lost_qty)       AS know_lost_qty
        FROM hive.dal.dal_transaction_store_article_lost_di t1
        WHERE t1.inc_day BETWEEN '{start}' AND '{end}'
          AND t1.category_level1_id NOT IN {mat_excl}
        GROUP BY
            t1.store_id,
            t1.inc_day,
            t1.article_id
        """

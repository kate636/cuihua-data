"""
域④ 损耗域取数器

源表: strategy_fm_loss_di  (已聚合到 store × article × business_date 粒度，上游已做品类过滤)
目标: DuckDB atomic_loss
原子字段:
  - know_lost_qty / unknow_lost_qty             : 已知 / 未知损耗数量
  - know_lost_amt_src / unknow_lost_amt_src     : FM 上游已计算好的损耗金额（生鲜直配无 cost_price 时唯一可信来源）

注意:
- 老 ETL 只抽 know_lost_qty，未知损耗靠库存方程反推，金额 = qty × cost_price。
  但 strategy_fm_inventory_pool_di 对猪肉类等生鲜 SKU 没有 cost_price（LEFT JOIN 为 NULL → 金额 = 0）。
- strategy_fm_loss_di 源表直接提供了 know_lost_amt / unknow_lost_amt，优先用它，
  qty × cost_price 作兜底。字段名加 `_src` 后缀，避免和下游计算字段冲突。
"""

from ._base import BaseExtractor


class LossExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_loss"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        return f"""
        SELECT
            t1.store_id,
            t1.inc_day                     AS business_date,
            t1.article_id,
            SUM(t1.know_lost_qty)          AS know_lost_qty,
            SUM(t1.unknow_lost_qty)        AS unknow_lost_qty_src,
            SUM(t1.know_lost_amt)          AS know_lost_amt_src,
            SUM(t1.unknow_lost_amt)        AS unknow_lost_amt_src
        FROM strategy_fm_loss_di t1
        WHERE t1.inc_day BETWEEN '{start}' AND '{end}'
        GROUP BY
            t1.store_id,
            t1.inc_day,
            t1.article_id
        """

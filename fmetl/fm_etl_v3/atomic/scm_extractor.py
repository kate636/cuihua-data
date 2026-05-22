"""
域③ 供应链域取数器

源表: strategy_fm_scm_di  (已聚合到 store × article × business_date 粒度，含 adjustment_amt)
目标: DuckDB atomic_scm
原子字段: 出库/退仓数量、单价、SAP让利金额、订购数量、差异调整

说明:
- 新表 `strategy_fm_scm_di` 已预先合并 adjustment_amt，不再需要 JOIN `strategy_fm_scm_adjust_di`。
- 单价字段改为基于 `total_outstock_qty` + `out_stock_*` 金额口径推导；
  退仓单价改为基于 `return_stock_pay_amt*` / `return_stock_amt_cb*`。

WAF 注意: 所有 CASE WHEN 已替换为 IF()。
"""

from ._base import BaseExtractor


class ScmExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_scm"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        return f"""
        SELECT
            s.store_id,
            s.business_date,
            s.article_id,
            SUM(COALESCE(s.original_outstock_qty, 0))         AS original_outstock_qty,
            SUM(COALESCE(s.promotion_outstock_qty, 0))        AS promotion_outstock_qty,
            SUM(COALESCE(s.gift_outstock_qty, 0))             AS gift_outstock_qty,
            SUM(COALESCE(s.return_stock_qty, 0))              AS return_stock_qty,
            SUM(COALESCE(s.store_return_qty_shop, 0))         AS store_return_qty_shop,
            SUM(COALESCE(s.store_order_qty, 0))               AS store_order_qty,
            SUM(COALESCE(s.order_qty_payean, 0))              AS order_qty_payean,
            IF(SUM(COALESCE(s.total_outstock_qty, 0)) = 0, 0,
               SUM(COALESCE(s.out_stock_pay_amt, 0))
               / SUM(COALESCE(s.total_outstock_qty, 0))
            )                                                 AS outstock_unit_price,
            IF(SUM(COALESCE(s.total_outstock_qty, 0)) = 0, 0,
               SUM(COALESCE(s.out_stock_pay_amt_notax, 0))
               / SUM(COALESCE(s.total_outstock_qty, 0))
            )                                                 AS outstock_unit_price_notax,
            IF(SUM(COALESCE(s.total_outstock_qty, 0)) = 0, 0,
               SUM(COALESCE(s.out_stock_amt_cb, 0))
               / SUM(COALESCE(s.total_outstock_qty, 0))
            )                                                 AS outstock_cost_price,
            IF(SUM(COALESCE(s.total_outstock_qty, 0)) = 0, 0,
               SUM(COALESCE(s.out_stock_amt_cb_notax, 0))
               / SUM(COALESCE(s.total_outstock_qty, 0))
            )                                                 AS outstock_cost_price_notax,
            IF(SUM(COALESCE(s.return_stock_qty, 0)) = 0, 0,
               SUM(COALESCE(s.return_stock_pay_amt, 0))
               / SUM(COALESCE(s.return_stock_qty, 0))
            )                                                 AS return_unit_price,
            IF(SUM(COALESCE(s.return_stock_qty, 0)) = 0, 0,
               SUM(COALESCE(s.return_stock_pay_amt_notax, 0))
               / SUM(COALESCE(s.return_stock_qty, 0))
            )                                                 AS return_unit_price_notax,
            IF(SUM(COALESCE(s.return_stock_qty, 0)) = 0, 0,
               SUM(COALESCE(s.return_stock_amt_cb, 0))
               / SUM(COALESCE(s.return_stock_qty, 0))
            )                                                 AS return_cost_price,
            IF(SUM(COALESCE(s.return_stock_qty, 0)) = 0, 0,
               SUM(COALESCE(s.return_stock_amt_cb_notax, 0))
               / SUM(COALESCE(s.return_stock_qty, 0))
            )                                                 AS return_cost_price_notax,
            IF(SUM(COALESCE(s.store_order_qty, 0)) = 0, 0,
               SUM(COALESCE(s.order_amt, 0))
               / SUM(COALESCE(s.store_order_qty, 0))
            )                                                 AS order_unit_price,
            SUM(COALESCE(s.scm_promotion_amt_total, 0))       AS scm_promotion_amt_total,
            SUM(COALESCE(s.scm_promotion_amt_gift, 0))        AS scm_promotion_amt_gift,
            SUM(COALESCE(s.scm_bear_amt, 0))                  AS scm_bear_amt,
            SUM(COALESCE(s.vendor_bear_amt, 0))               AS vendor_bear_amt,
            SUM(COALESCE(s.business_bear_amt, 0))             AS business_bear_amt,
            SUM(COALESCE(s.market_bear_amt, 0))               AS market_bear_amt,
            SUM(COALESCE(s.vender_bear_gift_amt, 0))          AS vender_bear_gift_amt,
            SUM(COALESCE(s.scm_bear_gift_amt, 0))             AS scm_bear_gift_amt,
            SUM(COALESCE(s.adjustment_amt, 0))                AS adjustment_amt
        FROM strategy_fm_scm_di s
        WHERE s.inc_day BETWEEN '{start}' AND '{end}'
        GROUP BY
            s.store_id,
            s.business_date,
            s.article_id
        """

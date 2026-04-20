"""
域③ 供应链域取数器

源表: hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di
      hive.dal_bi_rpt.dal_debit_store_dc_difference_adjustment_di (差异调整)
目标: DuckDB atomic_scm
原子字段: 出库/退仓数量、单价、SAP让利金额、订购数量

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
            -- 出库数量
            SUM(COALESCE(s.original_outstock_qty, 0))         AS original_outstock_qty,
            SUM(COALESCE(s.promotion_outstock_qty, 0))        AS promotion_outstock_qty,
            SUM(COALESCE(s.gift_outstock_qty, 0))             AS gift_outstock_qty,
            -- 退仓数量
            SUM(COALESCE(s.store_return_scm_qty, 0))          AS return_stock_qty,
            SUM(COALESCE(s.store_return_qty_shop, 0))         AS store_return_qty_shop,
            -- 订购数量
            SUM(COALESCE(s.order_qty_order_unit, 0))          AS store_order_qty,
            SUM(COALESCE(s.order_qty_payean, 0))              AS order_qty_payean,
            -- 出库单价 (金额/数量 推导)
            IF(SUM(COALESCE(s.original_outstock_qty,0)+COALESCE(s.promotion_outstock_qty,0)+COALESCE(s.gift_outstock_qty,0)) = 0,
               0,
               SUM(COALESCE(s.outstock_amt, 0))
               / SUM(COALESCE(s.original_outstock_qty,0)+COALESCE(s.promotion_outstock_qty,0)+COALESCE(s.gift_outstock_qty,0))
            )                                                  AS outstock_unit_price,
            IF(SUM(COALESCE(s.original_outstock_qty,0)+COALESCE(s.promotion_outstock_qty,0)+COALESCE(s.gift_outstock_qty,0)) = 0,
               0,
               SUM(COALESCE(s.outstock_amt_notax, 0))
               / SUM(COALESCE(s.original_outstock_qty,0)+COALESCE(s.promotion_outstock_qty,0)+COALESCE(s.gift_outstock_qty,0))
            )                                                  AS outstock_unit_price_notax,
            IF(SUM(COALESCE(s.original_outstock_qty,0)+COALESCE(s.promotion_outstock_qty,0)+COALESCE(s.gift_outstock_qty,0)) = 0,
               0,
               SUM(COALESCE(s.outstock_cost, 0))
               / SUM(COALESCE(s.original_outstock_qty,0)+COALESCE(s.promotion_outstock_qty,0)+COALESCE(s.gift_outstock_qty,0))
            )                                                  AS outstock_cost_price,
            IF(SUM(COALESCE(s.original_outstock_qty,0)+COALESCE(s.promotion_outstock_qty,0)+COALESCE(s.gift_outstock_qty,0)) = 0,
               0,
               SUM(COALESCE(s.outstock_cost_notax, 0))
               / SUM(COALESCE(s.original_outstock_qty,0)+COALESCE(s.promotion_outstock_qty,0)+COALESCE(s.gift_outstock_qty,0))
            )                                                  AS outstock_cost_price_notax,
            -- 退仓单价
            IF(SUM(COALESCE(s.store_return_scm_qty,0)) = 0, 0,
               SUM(COALESCE(s.store_return_scm_amt,0)) / SUM(COALESCE(s.store_return_scm_qty,0))
            )                                                  AS return_unit_price,
            IF(SUM(COALESCE(s.store_return_scm_qty,0)) = 0, 0,
               SUM(COALESCE(s.store_return_scm_amt_notax,0)) / SUM(COALESCE(s.store_return_scm_qty,0))
            )                                                  AS return_unit_price_notax,
            IF(SUM(COALESCE(s.store_return_scm_qty,0)) = 0, 0,
               SUM(COALESCE(s.return_stock_amt_cb,0)) / SUM(COALESCE(s.store_return_scm_qty,0))
            )                                                  AS return_cost_price,
            IF(SUM(COALESCE(s.store_return_scm_qty,0)) = 0, 0,
               SUM(COALESCE(s.store_return_scm_cost_notax,0)) / SUM(COALESCE(s.store_return_scm_qty,0))
            )                                                  AS return_cost_price_notax,
            -- 订购单价
            IF(SUM(COALESCE(s.order_qty_order_unit,0)) = 0, 0,
               SUM(COALESCE(s.order_amt,0)) / SUM(COALESCE(s.order_qty_order_unit,0))
            )                                                  AS order_unit_price,
            -- SAP让利金额
            SUM(COALESCE(s.total_benefit_amt, 0))             AS scm_promotion_amt_total,
            SUM(COALESCE(s.total_gift_benefit_amt, 0))        AS scm_promotion_amt_gift,
            SUM(COALESCE(s.scm_bear_nogift_benefit_amt, 0))   AS scm_bear_amt,
            SUM(COALESCE(s.vendor_bear_nogift_benefit_amt, 0)) AS vendor_bear_amt,
            SUM(COALESCE(s.business_bear_nogift_benefit_amt,0)) AS business_bear_amt,
            SUM(COALESCE(s.market_bear_nogift_benefit_amt, 0)) AS market_bear_amt,
            SUM(COALESCE(s.vender_bear_gift_amt, 0))          AS vender_bear_gift_amt,
            SUM(COALESCE(s.scm_bear_gift_amt, 0))             AS scm_bear_gift_amt,
            -- 差异调整
            COALESCE(SUM(adj.adjustment_amt), 0)              AS adjustment_amt
        FROM (
            SELECT *
            FROM hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di
            WHERE business_date BETWEEN '{start}' AND '{end}'
        ) s
        LEFT JOIN (
            SELECT store_id, article_id, business_date,
                   SUM(adjustment_amt) AS adjustment_amt
            FROM hive.dal_bi_rpt.dal_debit_store_dc_difference_adjustment_di
            WHERE business_date BETWEEN '{start}' AND '{end}'
            GROUP BY store_id, article_id, business_date
        ) adj ON s.store_id = adj.store_id
              AND s.article_id = adj.article_id
              AND s.business_date = adj.business_date
        GROUP BY
            s.store_id,
            s.business_date,
            s.article_id
        """

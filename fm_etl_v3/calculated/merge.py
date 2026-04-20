"""
原子层合并器

将 10 个原子域在 DuckDB 中 FULL OUTER JOIN 合并成一张宽表 `t_atomic_wide`。
粒度: store_id × business_date × article_id × day_clear
"""

from __future__ import annotations

from ..connectors import DuckDBStore
from ..utils import get_logger


class AtomicMerger:
    """将所有原子表 FULL OUTER JOIN 合并为宽表 t_atomic_wide。"""

    TARGET_TABLE = "t_atomic_wide"

    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("AtomicMerger")

    def run(self, start: str, end: str) -> None:
        """
        合并 [start, end] 范围内的原子数据。
        先删除该日期范围的旧数据，再重建。
        """
        self._log.info(f"merging atomic tables: {start} ~ {end}")
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
        sql = self._build_merge_sql(start, end)
        self._duck.execute(f"CREATE TABLE {self.TARGET_TABLE} AS\n{sql}")
        rows = self._duck.row_count(self.TARGET_TABLE)
        self._log.info(f"t_atomic_wide: {rows} rows")

    def _build_merge_sql(self, start: str, end: str) -> str:
        """
        以 atomic_sales 为基底，依次 LEFT JOIN 其他原子表。
        缺少 sales 记录但有进货/损耗记录的商品通过 FULL OUTER JOIN 补全。
        """
        return f"""
        WITH
        -- ── 翠花门店过滤 ──────────────────────────────────────────────────
        chdj_stores AS (
            SELECT DISTINCT store_id FROM dim_store_list
        ),

        -- ── 日清标签 ──────────────────────────────────────────────────────
        day_clear_labels AS (
            SELECT store_id, business_date, article_id, day_clear
            FROM dim_day_clear
            WHERE business_date BETWEEN '{start}' AND '{end}'
        ),

        -- ── 合并销售 + 进货库存 ──────────────────────────────────────────
        base AS (
            SELECT
                COALESCE(s.store_id,   p.store_id)   AS store_id,
                COALESCE(s.business_date, p.business_date) AS business_date,
                COALESCE(s.article_id, p.article_id) AS article_id,
                -- 日清标签: 优先使用原子层带的 day_clear，后备用维度表
                COALESCE(s.day_clear, p.day_clear,
                    CASE WHEN dc.day_clear = 1 THEN '1' ELSE '0' END, '0')
                                                     AS day_clear,
                -- 域① 销售域
                COALESCE(s.sale_qty, 0)              AS sale_qty,
                COALESCE(s.sale_piece_qty, 0)        AS sale_piece_qty,
                COALESCE(s.return_sale_qty, 0)       AS return_sale_qty,
                COALESCE(s.gift_qty, 0)              AS gift_qty,
                COALESCE(s.online_sale_qty, 0)       AS online_sale_qty,
                COALESCE(s.offline_sale_qty, 0)      AS offline_sale_qty,
                COALESCE(s.bf19_sale_qty, 0)         AS bf19_sale_qty,
                COALESCE(s.af19_sale_qty, 0)         AS af19_sale_qty,
                COALESCE(s.bf12_sale_qty, 0)         AS bf12_sale_qty,
                COALESCE(s.sales_weight, 0)          AS sales_weight,
                COALESCE(s.sale_amt, 0)              AS sale_amt,
                COALESCE(s.original_price_sale_amt, 0) AS original_price_sale_amt,
                COALESCE(s.vip_discount_amt, 0)      AS vip_discount_amt,
                COALESCE(s.hour_discount_amt, 0)     AS hour_discount_amt,
                COALESCE(s.actual_amount, 0)         AS actual_amount,
                COALESCE(s.return_sale_amt, 0)       AS return_sale_amt,
                COALESCE(s.member_discount_amt, 0)   AS member_discount_amt,
                COALESCE(s.discount_amt, 0)          AS discount_amt,
                COALESCE(s.member_sale_amt, 0)       AS member_sale_amt,
                COALESCE(s.bf19_member_sale_amt, 0)  AS bf19_member_sale_amt,
                COALESCE(s.offline_original_amt, 0)  AS offline_original_amt,
                COALESCE(s.store_paylevel_discount, 0) AS store_paylevel_discount,
                COALESCE(s.company_paylevel_discount, 0) AS company_paylevel_discount,
                COALESCE(s.af19_sale_amt, 0)         AS af19_sale_amt,
                COALESCE(s.bf19_sale_amt, 0)         AS bf19_sale_amt,
                COALESCE(s.bf19_offline_sale_amt, 0) AS bf19_offline_sale_amt,
                COALESCE(s.bf12_sale_amt, 0)         AS bf12_sale_amt,
                COALESCE(s.bf19_sale_piece_qty, 0)   AS bf19_sale_piece_qty,
                s.last_sysdate,
                -- 域② 进货库存域
                COALESCE(p.receive_qty, 0)           AS receive_qty,
                COALESCE(p.init_stock_qty, 0)        AS init_stock_qty,
                COALESCE(p.end_stock_qty, 0)         AS end_stock_qty_raw
            FROM (
                SELECT * FROM atomic_sales
                WHERE business_date BETWEEN '{start}' AND '{end}'
            ) s
            FULL OUTER JOIN (
                SELECT * FROM atomic_inventory
                WHERE business_date BETWEEN '{start}' AND '{end}'
            ) p ON s.store_id = p.store_id
                AND s.business_date = p.business_date
                AND s.article_id = p.article_id
            LEFT JOIN day_clear_labels dc
                ON COALESCE(s.store_id, p.store_id) = dc.store_id
                AND COALESCE(s.business_date, p.business_date) = dc.business_date
                AND COALESCE(s.article_id, p.article_id) = dc.article_id
            -- 只保留翠花门店
            INNER JOIN chdj_stores cs
                ON COALESCE(s.store_id, p.store_id) = cs.store_id
        )

        SELECT
            base.*,
            -- 域③ 供应链域
            COALESCE(scm.original_outstock_qty, 0)     AS original_outstock_qty,
            COALESCE(scm.promotion_outstock_qty, 0)    AS promotion_outstock_qty,
            COALESCE(scm.gift_outstock_qty, 0)         AS gift_outstock_qty,
            COALESCE(scm.return_stock_qty, 0)          AS return_stock_qty,
            COALESCE(scm.store_return_qty_shop, 0)     AS store_return_qty_shop,
            COALESCE(scm.store_order_qty, 0)           AS store_order_qty,
            COALESCE(scm.order_qty_payean, 0)          AS order_qty_payean,
            COALESCE(scm.outstock_unit_price, 0)       AS outstock_unit_price,
            COALESCE(scm.outstock_unit_price_notax, 0) AS outstock_unit_price_notax,
            COALESCE(scm.outstock_cost_price, 0)       AS outstock_cost_price,
            COALESCE(scm.outstock_cost_price_notax, 0) AS outstock_cost_price_notax,
            COALESCE(scm.return_unit_price, 0)         AS return_unit_price,
            COALESCE(scm.return_unit_price_notax, 0)   AS return_unit_price_notax,
            COALESCE(scm.return_cost_price, 0)         AS return_cost_price,
            COALESCE(scm.return_cost_price_notax, 0)   AS return_cost_price_notax,
            COALESCE(scm.order_unit_price, 0)          AS order_unit_price,
            COALESCE(scm.scm_promotion_amt_total, 0)   AS scm_promotion_amt_total,
            COALESCE(scm.scm_promotion_amt_gift, 0)    AS scm_promotion_amt_gift,
            COALESCE(scm.scm_bear_amt, 0)              AS scm_bear_amt,
            COALESCE(scm.vendor_bear_amt, 0)           AS vendor_bear_amt,
            COALESCE(scm.business_bear_amt, 0)         AS business_bear_amt,
            COALESCE(scm.market_bear_amt, 0)           AS market_bear_amt,
            COALESCE(scm.vender_bear_gift_amt, 0)      AS vender_bear_gift_amt,
            COALESCE(scm.scm_bear_gift_amt, 0)         AS scm_bear_gift_amt,
            COALESCE(scm.adjustment_amt, 0)            AS adjustment_amt,
            -- 域④ 损耗域
            COALESCE(loss.know_lost_qty, 0)            AS know_lost_qty,
            -- 域⑤ 加工转换域
            COALESCE(cmp.compose_in_qty, 0)            AS compose_in_qty,
            COALESCE(cmp.compose_out_qty, 0)           AS compose_out_qty,
            -- 域⑥ 补贴域
            COALESCE(allow.allowance_amt, 0)           AS allowance_amt,
            -- 域⑦ 促销优惠域
            COALESCE(promo.member_coupon_shop_amt, 0)  AS member_coupon_shop_amt,
            COALESCE(promo.member_promo_amt, 0)        AS member_promo_amt,
            COALESCE(promo.member_coupon_company_amt,0) AS member_coupon_company_amt,
            COALESCE(promo.shop_promo_amt, 0)          AS shop_promo_amt,
            COALESCE(promo.no_ordercoupon_company_promotion_amt, 0) AS no_ordercoupon_company_promotion_amt,
            COALESCE(promo.ordercoupon_shop_promotion_amt, 0) AS ordercoupon_shop_promotion_amt,
            COALESCE(promo.ordercoupon_company_promotion_amt, 0) AS ordercoupon_company_promotion_amt,
            -- 域⑧ 成本价域
            COALESCE(cp.cost_price, 0)                 AS cost_price,
            -- 域⑨ 价格域
            COALESCE(pr.current_price, 0)              AS current_price,
            COALESCE(pr.yesterday_price, 0)            AS yesterday_price,
            COALESCE(pr.dc_original_price, 0)          AS dc_original_price,
            COALESCE(pr.original_price, 0)             AS original_price
        FROM base
        LEFT JOIN (
            SELECT * FROM atomic_scm
            WHERE business_date BETWEEN '{start}' AND '{end}'
        ) scm ON base.store_id = scm.store_id
              AND base.business_date = scm.business_date
              AND base.article_id = scm.article_id
        LEFT JOIN (
            SELECT * FROM atomic_loss
            WHERE business_date BETWEEN '{start}' AND '{end}'
        ) loss ON base.store_id = loss.store_id
               AND base.business_date = loss.business_date
               AND base.article_id = loss.article_id
        LEFT JOIN (
            SELECT * FROM atomic_compose
            WHERE business_date BETWEEN '{start}' AND '{end}'
        ) cmp ON base.store_id = cmp.store_id
              AND base.business_date = cmp.business_date
              AND base.article_id = cmp.article_id
        LEFT JOIN (
            SELECT * FROM atomic_allowance
            WHERE business_date BETWEEN '{start}' AND '{end}'
        ) allow ON base.store_id = allow.store_id
                AND base.business_date = allow.business_date
                AND base.article_id = allow.article_id
        LEFT JOIN (
            SELECT * FROM atomic_promo
            WHERE business_date BETWEEN '{start}' AND '{end}'
        ) promo ON base.store_id = promo.store_id
               AND base.business_date = promo.business_date
               AND base.article_id = promo.article_id
        LEFT JOIN (
            SELECT * FROM atomic_cost_price
            WHERE business_date BETWEEN '{start}' AND '{end}'
        ) cp ON base.store_id = cp.store_id
             AND base.business_date = cp.business_date
             AND base.article_id = cp.article_id
        LEFT JOIN (
            SELECT * FROM atomic_price
            WHERE business_date BETWEEN '{start}' AND '{end}'
        ) pr ON base.store_id = pr.store_id
             AND base.business_date = pr.business_date
             AND base.article_id = pr.article_id
        """

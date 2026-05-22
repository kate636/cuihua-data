"""
原子层合并器 (v10)

将 10+ 个原子域合并成 t_atomic_wide。
粒度: store_id × business_date × article_id × day_clear

v10 变更:
  - 新增 self_receive_qty / self_receive_amt (从 receive_sale_di 自购行)
  - 保留 init_stock_qty / init_stock_amt / avg_inbound_price (从 atomic_inventory)
  - 移除旧 receive_qty / receive_amt (不再从 purchase_di 取)
"""

from __future__ import annotations

from ..connectors import DuckDBStore
from ..utils import get_logger


class AtomicMerger:
    TARGET_TABLE = "t_atomic_wide"

    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("AtomicMerger")

    def run(self, start: str, end: str) -> None:
        self._log.info(f"merging atomic tables (v10): {start} ~ {end}")

        # Step A: 提取进货数据（含自购行 + BOM父品进货行）
        #   - 自购行: article_id = sale_article_id, 聚合 SUM
        #   - BOM父品: article_id != sale_article_id, 同一父品多行(每行对应一个子品)去重取 MAX
        self._log.info("  extracting self_receive from atomic_receive_sale (self + BOM parents) ...")
        self._duck.execute("DROP TABLE IF EXISTS _tmp_self_receive")
        # 构建 BOM 子品集合（不能被 purchase_di 回退覆盖）
        self._duck.execute(f"""
            DROP TABLE IF EXISTS _tmp_bom_subs
        """)
        self._duck.execute(f"""
            CREATE TABLE _tmp_bom_subs AS
            SELECT DISTINCT store_id, business_date, sale_article_id AS article_id
            FROM atomic_receive_sale
            WHERE article_id != sale_article_id
              AND business_date BETWEEN '{start}' AND '{end}'
        """)

        self._duck.execute(f"""
            CREATE TABLE _tmp_self_receive AS
            SELECT
                r.store_id, r.business_date, r.article_id,
                r.self_receive_qty, r.self_receive_amt,
                CASE WHEN bs.article_id IS NOT NULL THEN 1 ELSE 0 END AS is_bom_sub
            FROM (
                SELECT
                    store_id, business_date, article_id,
                    SUM(self_receive_qty)  AS self_receive_qty,
                    SUM(self_receive_amt)  AS self_receive_amt
                FROM (
                    SELECT
                        store_id, business_date, article_id,
                        SUM(inbound_qty)    AS self_receive_qty,
                        SUM(inbound_amount) AS self_receive_amt
                    FROM atomic_receive_sale
                    WHERE article_id = sale_article_id
                      AND business_date BETWEEN '{start}' AND '{end}'
                    GROUP BY store_id, business_date, article_id
                    UNION ALL
                    SELECT
                        store_id, business_date, article_id,
                        MAX(inbound_qty)    AS self_receive_qty,
                        MAX(inbound_amount) AS self_receive_amt
                    FROM atomic_receive_sale
                    WHERE article_id != sale_article_id
                      AND business_date BETWEEN '{start}' AND '{end}'
                    GROUP BY store_id, business_date, article_id
                ) t
                GROUP BY store_id, business_date, article_id
            ) r
            LEFT JOIN _tmp_bom_subs bs
                ON r.store_id = bs.store_id
               AND r.business_date = bs.business_date
               AND r.article_id = bs.article_id
        """)

        # Step B: 合并宽表
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
        self._duck.execute(f"""
            CREATE TABLE {self.TARGET_TABLE} AS
            WITH
            chdj_stores AS (
                SELECT DISTINCT store_id FROM dim_store_list
            ),
            day_clear_labels AS (
                SELECT store_id, business_date, article_id, day_clear
                FROM dim_day_clear
                WHERE business_date BETWEEN '{start}' AND '{end}'
            ),
            base AS (
                SELECT
                    COALESCE(s.store_id, p.store_id)        AS store_id,
                    COALESCE(s.business_date, p.business_date) AS business_date,
                    COALESCE(s.article_id, p.article_id)    AS article_id,
                    COALESCE(s.day_clear, p.day_clear,
                        CASE WHEN dc.day_clear = 1 THEN '1' ELSE '0' END, '0')
                                                            AS day_clear,
                    -- 域① 销售域
                    COALESCE(s.sale_qty, 0)                 AS sale_qty,
                    COALESCE(s.sale_piece_qty, 0)           AS sale_piece_qty,
                    COALESCE(s.return_sale_qty, 0)          AS return_sale_qty,
                    COALESCE(s.gift_qty, 0)                 AS gift_qty,
                    COALESCE(s.online_sale_qty, 0)          AS online_sale_qty,
                    COALESCE(s.offline_sale_qty, 0)         AS offline_sale_qty,
                    COALESCE(s.bf19_sale_qty, 0)            AS bf19_sale_qty,
                    COALESCE(s.af19_sale_qty, 0)            AS af19_sale_qty,
                    COALESCE(s.bf12_sale_qty, 0)            AS bf12_sale_qty,
                    COALESCE(s.sales_weight, 0)             AS sales_weight,
                    COALESCE(s.sale_amt, 0)                 AS sale_amt,
                    COALESCE(s.original_price_sale_amt, 0)  AS original_price_sale_amt,
                    COALESCE(s.vip_discount_amt, 0)         AS vip_discount_amt,
                    COALESCE(s.hour_discount_amt, 0)        AS hour_discount_amt,
                    COALESCE(s.actual_amount, 0)            AS actual_amount,
                    COALESCE(s.return_sale_amt, 0)          AS return_sale_amt,
                    COALESCE(s.member_discount_amt, 0)      AS member_discount_amt,
                    COALESCE(s.discount_amt, 0)             AS discount_amt,
                    COALESCE(s.member_sale_amt, 0)          AS member_sale_amt,
                    COALESCE(s.bf19_member_sale_amt, 0)     AS bf19_member_sale_amt,
                    COALESCE(s.offline_original_amt, 0)     AS offline_original_amt,
                    COALESCE(s.store_paylevel_discount, 0)  AS store_paylevel_discount,
                    COALESCE(s.company_paylevel_discount, 0) AS company_paylevel_discount,
                    COALESCE(s.af19_sale_amt, 0)            AS af19_sale_amt,
                    COALESCE(s.bf19_sale_amt, 0)            AS bf19_sale_amt,
                    COALESCE(s.bf19_offline_sale_amt, 0)    AS bf19_offline_sale_amt,
                    COALESCE(s.bf12_sale_amt, 0)            AS bf12_sale_amt,
                    COALESCE(s.bf19_sale_piece_qty, 0)      AS bf19_sale_piece_qty,
                    s.last_sysdate,
                    -- 域② 库存域 (v10: 只取 init_stock + avg_inbound_price)
                    COALESCE(p.init_stock_qty, 0)           AS init_stock_qty_src,
                    COALESCE(p.init_stock_amt, 0)           AS init_stock_amt_src,
                    COALESCE(p.avg_inbound_price, 0)        AS avg_inbound_price
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
                INNER JOIN chdj_stores cs
                    ON COALESCE(s.store_id, p.store_id) = cs.store_id
            )

            SELECT
                base.*,
                -- v10: 自购数据 (主源 receive_sale_di, 回退 purchase_di 但排除 BOM 子品)
                CASE WHEN COALESCE(sr.self_receive_qty, 0) > 0
                     THEN sr.self_receive_qty
                     WHEN COALESCE(bs.article_id, '') != '' THEN 0
                     ELSE COALESCE(inv.purchase_receive_qty, 0)
                END                                        AS self_receive_qty,
                CASE WHEN COALESCE(sr.self_receive_amt, 0) > 0
                     THEN sr.self_receive_amt
                     WHEN COALESCE(bs.article_id, '') != '' THEN 0
                     ELSE COALESCE(inv.purchase_receive_amt, 0)
                END                                        AS self_receive_amt,
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
                loss.unknow_lost_qty_src,
                loss.know_lost_amt_src,
                loss.unknow_lost_amt_src,
                -- 域⑤ 加工转换域
                COALESCE(cmp.compose_in_qty, 0)            AS compose_in_qty,
                COALESCE(cmp.compose_out_qty, 0)           AS compose_out_qty,
                COALESCE(cmp.compose_in_amt, 0)            AS compose_in_amt_src,
                COALESCE(cmp.compose_out_amt, 0)           AS compose_out_amt_src,
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
            LEFT JOIN _tmp_self_receive sr
                ON base.store_id = sr.store_id
                AND base.business_date = sr.business_date
                AND base.article_id = sr.article_id
            LEFT JOIN _tmp_bom_subs bs
                ON base.store_id = bs.store_id
                AND base.business_date = bs.business_date
                AND base.article_id = bs.article_id
            LEFT JOIN (
                SELECT store_id, business_date, article_id,
                       purchase_receive_qty, purchase_receive_amt
                FROM atomic_inventory
                WHERE business_date BETWEEN '{start}' AND '{end}'
            ) inv ON base.store_id = inv.store_id
                 AND base.business_date = inv.business_date
                 AND base.article_id = inv.article_id
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
        """)
        # Step C: 补全 BOM 父品行（父品不在 atomic_sales/atomic_inventory 中，
        #         但需要出现在 t_atomic_wide 以便 downstream 处理其进货和 BOM 流出）
        self._duck.execute(f"""
            INSERT INTO {self.TARGET_TABLE} BY NAME
            SELECT
                sr.store_id,
                sr.business_date,
                sr.article_id,
                CAST('1' AS VARCHAR)                              AS day_clear,
                -- 销售域 (全0)
                0                                                   AS sale_qty,
                0                                                   AS sale_piece_qty,
                0                                                   AS return_sale_qty,
                0                                                   AS gift_qty,
                0                                                   AS online_sale_qty,
                0                                                   AS offline_sale_qty,
                0                                                   AS bf19_sale_qty,
                0                                                   AS af19_sale_qty,
                0                                                   AS bf12_sale_qty,
                0                                                   AS sales_weight,
                0                                                   AS sale_amt,
                0                                                   AS original_price_sale_amt,
                0                                                   AS vip_discount_amt,
                0                                                   AS hour_discount_amt,
                0                                                   AS actual_amount,
                0                                                   AS return_sale_amt,
                0                                                   AS member_discount_amt,
                0                                                   AS discount_amt,
                0                                                   AS member_sale_amt,
                0                                                   AS bf19_member_sale_amt,
                0                                                   AS offline_original_amt,
                0                                                   AS store_paylevel_discount,
                0                                                   AS company_paylevel_discount,
                0                                                   AS af19_sale_amt,
                0                                                   AS bf19_sale_amt,
                0                                                   AS bf19_offline_sale_amt,
                0                                                   AS bf12_sale_amt,
                0                                                   AS bf19_sale_piece_qty,
                CAST(NULL AS VARCHAR)                              AS last_sysdate,
                -- 库存域 (全0)
                0                                                   AS init_stock_qty_src,
                0                                                   AS init_stock_amt_src,
                0                                                   AS avg_inbound_price,
                -- 进货 (BOM 父品自己的 inbound)
                sr.self_receive_qty                                AS self_receive_qty,
                sr.self_receive_amt                                AS self_receive_amt,
                -- SCM 域 (全0)
                0 AS original_outstock_qty, 0 AS promotion_outstock_qty,
                0 AS gift_outstock_qty, 0 AS return_stock_qty,
                0 AS store_return_qty_shop, 0 AS store_order_qty,
                0 AS order_qty_payean,
                0 AS outstock_unit_price, 0 AS outstock_unit_price_notax,
                0 AS outstock_cost_price, 0 AS outstock_cost_price_notax,
                0 AS return_unit_price, 0 AS return_unit_price_notax,
                0 AS return_cost_price, 0 AS return_cost_price_notax,
                0 AS order_unit_price,
                0 AS scm_promotion_amt_total, 0 AS scm_promotion_amt_gift,
                0 AS scm_bear_amt, 0 AS vendor_bear_amt,
                0 AS business_bear_amt, 0 AS market_bear_amt,
                0 AS vender_bear_gift_amt, 0 AS scm_bear_gift_amt,
                0 AS adjustment_amt,
                -- 损耗域 (全0)
                0 AS know_lost_qty,
                0 AS unknow_lost_qty_src,
                0 AS know_lost_amt_src,
                0 AS unknow_lost_amt_src,
                -- 加工转换 (全0)
                0 AS compose_in_qty,
                0 AS compose_out_qty,
                0 AS compose_in_amt_src,
                0 AS compose_out_amt_src,
                -- 补贴
                0 AS allowance_amt,
                -- 促销
                0 AS member_coupon_shop_amt, 0 AS member_promo_amt,
                0 AS member_coupon_company_amt, 0 AS shop_promo_amt,
                0 AS no_ordercoupon_company_promotion_amt,
                0 AS ordercoupon_shop_promotion_amt,
                0 AS ordercoupon_company_promotion_amt,
                -- 成本价
                0 AS cost_price,
                -- 价格
                0 AS current_price, 0 AS yesterday_price,
                0 AS dc_original_price, 0 AS original_price
            FROM _tmp_self_receive sr
            LEFT JOIN {self.TARGET_TABLE} w
                ON  sr.store_id = w.store_id
                AND sr.business_date = w.business_date
                AND sr.article_id = w.article_id
            WHERE w.article_id IS NULL
              AND sr.self_receive_qty > 0
        """)
        # 猪肉类业务日清: dim标非日清但鲜肉不过夜, 强制day_clear='0'
        # 牛肉类不在此列: purchase_di的init_stock不为0, 日清会导致巨额亏损
        try:
            self._duck.execute(f"""
                UPDATE {self.TARGET_TABLE}
                SET day_clear = '0'
                WHERE article_id IN (
                    SELECT article_id FROM dim_goods WHERE category_level1_description = '猪肉类'
                )
            """)
            self._log.info("猪肉类日清覆盖完成")
        except Exception as e:
            self._log.warning(f"猪肉类日清覆盖跳过: {e}")

        # 熟食类业务日清: 即食熟食当日不过夜, 强制day_clear='0'
        try:
            self._duck.execute(f"""
                UPDATE {self.TARGET_TABLE}
                SET day_clear = '0'
                WHERE article_id IN (
                    SELECT article_id FROM dim_goods
                    WHERE category_level3_description LIKE '%熟食'
                )
            """)
            self._log.info("熟食类日清覆盖完成")
        except Exception as e:
            self._log.warning(f"熟食类日清覆盖跳过: {e}")

        # 鲜牛肉业务日清: 鲜黄牛部位肉当日不过夜
        try:
            self._duck.execute(f"""
                UPDATE {self.TARGET_TABLE}
                SET day_clear = '0'
                WHERE article_id IN (
                    SELECT DISTINCT article_id FROM dim_goods
                    WHERE category_level1_description = '肉禽蛋类'
                      AND article_name LIKE '鲜黄牛%'
                )
            """)
            self._log.info("鲜牛肉日清覆盖完成")
        except Exception as e:
            self._log.warning(f"鲜牛肉日清覆盖跳过: {e}")

        self._duck.execute("DROP TABLE IF EXISTS _tmp_self_receive")
        self._duck.execute("DROP TABLE IF EXISTS _tmp_bom_subs")
        rows = self._duck.row_count(self.TARGET_TABLE)
        self._log.info(f"t_atomic_wide: {rows} rows")

"""
域① 销售域取数器

源表: hive.dsl.dsl_transaction_non_daily_store_order_details_di
目标: DuckDB atomic_sales
原子字段: sale_qty, sale_piece_qty, return_sale_qty, gift_qty, online/offline/bf19/af19/bf12 qtys,
          sales_weight, sale_amt, original_price_sale_amt, vip_discount_amt, hour_discount_amt,
          actual_amount, return_sale_amt, member_discount_amt, discount_amt,
          member_sale_amt, bf19_member_sale_amt

WAF 注意: 所有 CASE WHEN 已替换为 IF()。
"""

from ._base import BaseExtractor


class SalesExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_sales"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        mat_excl = "('70','71','72','73','74','75','76','77')"
        return f"""
        SELECT
            m1.store_id,
            m1.inc_day                                  AS business_date,
            m1.abi_article_id                           AS article_id,
            SUM(m1.qty_spec)                            AS sale_qty,
            SUM(m1.qty)                                 AS sale_piece_qty,
            SUM(COALESCE(m1.return_sale_qty, 0))        AS return_sale_qty,
            SUM(COALESCE(m1.gift_qty, 0))               AS gift_qty,
            SUM(IF(m1.online_flag='Y', m1.qty*m1.spec_num, 0))
                                                        AS online_sale_qty,
            SUM(IF(m1.online_flag='N', m1.qty_spec, 0))
                                                        AS offline_sale_qty,
            SUM(m1.qty_spec - COALESCE(m1.af19_sales_qty*m1.spec_num,0))
                                                        AS bf19_sale_qty,
            SUM(COALESCE(m1.af19_sales_qty*m1.spec_num, 0))
                                                        AS af19_sale_qty,
            SUM(IF(m1.trans_hour < '12', m1.qty*m1.spec_num, 0))
                                                        AS bf12_sale_qty,
            SUM(IF(m2.sale_unit='千克' OR COALESCE(m2.unit_weight,0)=0,
                   m1.qty_spec,
                   m1.qty_spec * m2.unit_weight))       AS sales_weight,
            SUM(m1.sales_amt)                           AS sale_amt,
            SUM(m1.p_lp_sub_amt)                        AS original_price_sale_amt,
            SUM(m1.vip_discount_amt)                    AS vip_discount_amt,
            SUM(m1.hour_discount_amt)                   AS hour_discount_amt,
            SUM(m1.actual_amount - COALESCE(m1.f_sub_amt,0) - COALESCE(m1.f_promo_sub_amt,0))
                                                        AS actual_amount,
            SUM(COALESCE(m1.return_sale_amt, 0))        AS return_sale_amt,
            SUM(m1.vip_discount_amt)                    AS member_discount_amt,
            SUM(m1.discount_amt)                        AS discount_amt,
            SUM(IF(m1.customer_phone IS NOT NULL AND m1.customer_phone <> '',
                   m1.sales_amt, 0))                    AS member_sale_amt,
            SUM(IF(m1.customer_phone IS NOT NULL AND m1.customer_phone <> '',
                   COALESCE(m1.sales_amt,0) - COALESCE(m1.af19_sales_amt,0),
                   0))                                  AS bf19_member_sale_amt,
            SUM(IF(m1.online_flag='N', m1.p_lp_sub_amt, 0))
                                                        AS offline_original_amt,
            SUM(COALESCE(m1.store_paylevel_discount, 0))
                                                        AS store_paylevel_discount,
            SUM(COALESCE(m1.company_paylevel_discount, 0))
                                                        AS company_paylevel_discount,
            SUM(COALESCE(m1.af19_sales_amt, 0))         AS af19_sale_amt,
            SUM(COALESCE(m1.sales_amt,0) - COALESCE(m1.af19_sales_amt,0))
                                                        AS bf19_sale_amt,
            SUM(IF(m1.online_flag='N',
                   COALESCE(m1.sales_amt,0) - COALESCE(m1.af19_sales_amt,0),
                   0))                                  AS bf19_offline_sale_amt,
            SUM(IF(m1.trans_hour < '12', m1.sales_amt, 0))
                                                        AS bf12_sale_amt,
            SUM(m1.qty) - SUM(COALESCE(m1.af19_sales_qty, 0))
                                                        AS bf19_sale_piece_qty,
            MAX(IF(m1.online_flag='N', m1.pay_at, NULL))
                                                        AS last_sysdate,
            m1.day_clear
        FROM (
            SELECT
                business_date,
                store_id,
                abi_article_id,
                online_flag,
                spec_num,
                customer_phone,
                qty,
                qty_spec,
                sales_amt,
                discount_amt,
                vip_discount_amt,
                hour_discount_amt,
                return_sale_qty,
                return_sale_amt,
                af19_sales_amt,
                af19_sales_qty,
                p_lp_sub_amt,
                gift_qty,
                actual_amount,
                f_sub_amt,
                f_promo_sub_amt,
                store_paylevel_discount,
                company_paylevel_discount,
                COALESCE(pay_at, NULL)                  AS pay_at,
                SUBSTR(inc_time, 12, 2)                 AS trans_hour,
                day_clear,
                inc_day
            FROM hive.dsl.dsl_transaction_non_daily_store_order_details_di
            WHERE inc_day BETWEEN '{start}' AND '{end}'
        ) m1
        LEFT JOIN (
            SELECT article_id, unit_weight, sale_unit
            FROM hive.dim.dim_goods_information_have_pt
            WHERE inc_day = '{yesterday}'
        ) m2 ON m1.abi_article_id = m2.article_id
        WHERE COALESCE(m2.category_level1_id, 'rd') NOT IN ('91')
          AND (
              (m2.category_level1_id NOT IN {mat_excl} AND m1.online_flag = 'N')
              OR m1.online_flag = 'Y'
          )
        GROUP BY
            m1.store_id,
            m1.inc_day,
            m1.abi_article_id,
            m1.day_clear
        """

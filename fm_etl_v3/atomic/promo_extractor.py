"""
域⑦ 促销优惠域取数器

源表: hive.dsl.dsl_promotion_order_item_article_sale_info_di
目标: DuckDB atomic_promo
原子字段: member_coupon_shop_amt, member_promo_amt, member_coupon_company_amt, shop_promo_amt

WAF 注意: 所有 CASE WHEN 已替换为 IF()。
"""

from ._base import BaseExtractor


class PromoExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_promo"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        mat_excl = "('70','71','72','73','74','75','76','77')"
        return f"""
        SELECT
            t1.store_id,
            t1.business_date,
            t1.article_id,
            -- 会员券 (门店承担)
            SUM(IF(t1.cost_center = 'shop'
                   AND t1.promotion_category = 'rule'
                   AND t1.promo_type = 'OrderCoupon'
                   AND t1.order_type = 'normal'
                   AND t1.online_flag = 'N',
                   t1.p_promo_amt, 0))
            + SUM(COALESCE(t1.store_paylevel_discount, 0))
                                                            AS member_coupon_shop_amt,
            -- 会员活动促销费
            SUM(IF(t1.cost_center NOT IN ('shop','vendor','customer')
                   AND t1.promotion_category = 'rule'
                   AND t1.promo_type = 'OrderCoupon'
                   AND SUBSTR(t1.promo_ext_prop, 3, 2) = '01',
                   t1.p_promo_amt, 0))
            + SUM(IF(t1.promo_sub_type = 'n.fold.point'
                     AND SUBSTR(COALESCE(t1.promo_ext_prop,''), 3, 2) = '01',
                     t1.p_promo_amt, 0))                   AS member_promo_amt,
            -- 会员券 (公司承担)
            SUM(IF(t1.cost_center NOT IN ('shop','vendor','customer')
                   AND t1.promotion_category = 'rule'
                   AND t1.promo_type = 'OrderCoupon'
                   AND t1.order_type = 'normal'
                   AND t1.online_flag = 'N'
                   AND SUBSTR(COALESCE(t1.promo_ext_prop,''), 1, 2) <> '01'
                   AND SUBSTR(COALESCE(t1.promo_ext_prop,''), 3, 2) <> '01',
                   t1.p_promo_amt, 0))
            + SUM(IF(t1.promo_sub_type = 'n.fold.point'
                     AND t1.order_type = 'normal'
                     AND t1.online_flag = 'N'
                     AND SUBSTR(COALESCE(t1.promo_ext_prop,''), 3, 2) <> '01',
                     t1.p_promo_amt, 0))                   AS member_coupon_company_amt,
            -- 门店发起促销额
            SUM(IF(t2.promotion_code IS NOT NULL,
                   t1.p_promo_amt + COALESCE(t1.f_promo_amt, 0),
                   0))                                     AS shop_promo_amt,
            -- 公司承担非券优惠
            SUM(IF(t1.online_flag = 'N'
                   AND COALESCE(t1.promotion_category,'') = 'rule'
                   AND COALESCE(t1.promo_type,'') IN ('O','I','Exchange')
                   AND t1.cost_center NOT IN ('shop','customer'),
                   t1.p_promo_amt, 0))                     AS no_ordercoupon_company_promotion_amt,
            -- 门店承担优惠券
            SUM(IF(t1.online_flag = 'N'
                   AND COALESCE(t1.promotion_category,'') = 'rule'
                   AND COALESCE(t1.promo_type,'') = 'OrderCoupon'
                   AND t1.cost_center = 'shop',
                   t1.p_promo_amt, 0))                     AS ordercoupon_shop_promotion_amt,
            -- 公司承担优惠券
            SUM(IF(t1.online_flag = 'N'
                   AND COALESCE(t1.promotion_category,'') = 'rule'
                   AND COALESCE(t1.promo_type,'') = 'OrderCoupon'
                   AND t1.cost_center NOT IN ('customer','shop'),
                   t1.p_promo_amt, 0))                     AS ordercoupon_company_promotion_amt
        FROM (
            SELECT
                inc_day          AS business_date,
                shop_id          AS store_id,
                sku_code         AS article_id,
                promotion_code2,
                cost_center,
                promotion_category,
                promo_type,
                promo_sub_type,
                promo_ext_prop,
                order_type,
                online_flag,
                p_promo_amt,
                f_promo_amt,
                store_paylevel_discount,
                inc_day
            FROM hive.dsl.dsl_promotion_order_item_article_sale_info_di
            WHERE inc_day BETWEEN '{start}' AND '{end}'
        ) t1
        LEFT JOIN (
            SELECT promotion_code
            FROM hive.dim.dim_store_promotion_info_da
            WHERE inc_day = '{yesterday}'
              AND act_type IN ('coupon','goods','orders','group_price')
              AND p_source = 'shop'
        ) t2 ON t1.promotion_code2 = t2.promotion_code
        INNER JOIN (
            SELECT article_id
            FROM hive.dim.dim_goods_information_have_pt
            WHERE inc_day = '{yesterday}'
              AND category_level1_id NOT IN {mat_excl}
        ) t3 ON t1.article_id = t3.article_id
        INNER JOIN (
            SELECT store_id, inc_day
            FROM hive.dim.dim_chdj_store_list_di
            WHERE inc_day BETWEEN '{start}' AND '{end}'
        ) t4 ON t1.store_id = t4.store_id AND t1.inc_day = t4.inc_day
        GROUP BY
            t1.store_id,
            t1.business_date,
            t1.article_id
        """

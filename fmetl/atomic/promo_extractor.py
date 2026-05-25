"""
域⑦ 促销优惠域取数器

源表: strategy_fm_promo_di  (订单行级，含 promotion_code/activity_type 等预关联字段)
辅表: strategy_fm_dim_goods (排除物料类品类)
目标: DuckDB atomic_promo
原子字段: member_coupon_shop_amt, member_promo_amt, member_coupon_company_amt, shop_promo_amt,
         no_ordercoupon_company_promotion_amt, ordercoupon_shop_promotion_amt,
         ordercoupon_company_promotion_amt

说明:
- 新表字段名：`shop_id` → store_id，`sku_code` → article_id，`inc_day` → business_date。
- 老 SQL 里 `LEFT JOIN dim_store_promotion_info_da` 用于判定 `shop_promo_amt` 门店发起促销；
  新表已预关联 `activity_type` / `cost_center`，此处改用
  `cost_center = 'shop' AND promotion_category = 'rule'` 判定（简化口径）。
  TODO: Phase D 跑完后与老 t_fm_sku_dim 对比 `shop_promo_amt`，确认是否 1:1。
- 翠花门店 INNER JOIN 已在下游 AtomicMerger 里做，此处不再 JOIN。

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
            SUM(IF(t1.cost_center = 'shop'
                   AND t1.promotion_category = 'rule'
                   AND t1.promo_type = 'OrderCoupon'
                   AND t1.order_type = 'normal'
                   AND t1.online_flag = 'N',
                   t1.p_promo_amt, 0))                     AS member_coupon_shop_amt,
            -- NOTE: 老 SQL 里这里还加了 + SUM(store_paylevel_discount)；
            -- 新表 strategy_fm_promo_di 不含此字段。下游 amounts.py 会再次把
            -- t_atomic_wide.store_paylevel_discount（来自 atomic_sales）加进
            -- member_coupon_shop_amt_adj，所以去掉这里的 SUM 反而避免双重计。
            SUM(IF(t1.cost_center NOT IN ('shop','vendor','customer')
                   AND t1.promotion_category = 'rule'
                   AND t1.promo_type = 'OrderCoupon'
                   AND SUBSTR(t1.promo_ext_prop, 3, 2) = '01',
                   t1.p_promo_amt, 0))
            + SUM(IF(t1.promo_sub_type = 'n.fold.point'
                     AND SUBSTR(COALESCE(t1.promo_ext_prop,''), 3, 2) = '01',
                     t1.p_promo_amt, 0))                   AS member_promo_amt,
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
            SUM(IF(t1.cost_center = 'shop'
                   AND COALESCE(t1.promotion_category, '') = 'rule',
                   t1.p_promo_amt + COALESCE(t1.f_promo_amt, 0),
                   0))                                     AS shop_promo_amt,
            SUM(IF(t1.online_flag = 'N'
                   AND COALESCE(t1.promotion_category,'') = 'rule'
                   AND COALESCE(t1.promo_type,'') IN ('O','I','Exchange')
                   AND t1.cost_center NOT IN ('shop','customer'),
                   t1.p_promo_amt, 0))                     AS no_ordercoupon_company_promotion_amt,
            SUM(IF(t1.online_flag = 'N'
                   AND COALESCE(t1.promotion_category,'') = 'rule'
                   AND COALESCE(t1.promo_type,'') = 'OrderCoupon'
                   AND t1.cost_center = 'shop',
                   t1.p_promo_amt, 0))                     AS ordercoupon_shop_promotion_amt,
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
                cost_center,
                promotion_category,
                promo_type,
                promo_sub_type,
                promo_ext_prop,
                order_type,
                online_flag,
                p_promo_amt,
                f_promo_amt
            FROM strategy_fm_promo_di
            WHERE inc_day BETWEEN '{start}' AND '{end}'
        ) t1
        INNER JOIN (
            SELECT DISTINCT article_id
            FROM strategy_fm_dim_goods
            WHERE inc_day = '{yesterday}'
              AND category_level1_id NOT IN {mat_excl}
        ) t3 ON t1.article_id = t3.article_id
        GROUP BY
            t1.store_id,
            t1.business_date,
            t1.article_id
        """

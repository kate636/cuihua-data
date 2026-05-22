"""
域① 销售域取数器

从 POS 交易系统提取销售相关的原子数据
- 源表: dsl.dsl_transaction_sotre_order_online_details_di + dsl.dsl_transaction_sotre_order_offline_details_di
- 原子字段: sale_qty, sale_amt, original_price_sale_amt, vip_discount_amt 等
"""

from typing import List
import pandas as pd

from .base_extractor import BaseExtractor


class SaleExtractor(BaseExtractor):
    """
    销售域取数器

    提取 POS 交易数据中的销售数量和金额
    这些是 POS 系统直接记录的实际货币收付，不可由数量×单价推导
    """

    @property
    def domain_name(self) -> str:
        return "sale"

    @property
    def atomic_fields(self) -> List[str]:
        """销售域原子字段列表"""
        return [
            # 数量类
            "sale_qty",              # 销售数量 (规格×份数)
            "sale_piece_qty",        # 销售件数 (份数)
            "return_sale_qty",       # 退货数量
            "gift_qty",              # 赠品数量
            "online_sale_qty",       # 线上销售数量
            "offline_sale_qty",      # 线下销售数量
            "bf19_sale_qty",         # 19点前销售数量
            "af19_sale_qty",         # 19点后销售数量
            "bf12_sale_qty",         # 12点前销售数量
            "sales_weight",          # 销售重量

            # 交易金额类 (POS 直录，不可推导)
            "sale_amt",              # 实际交易金额
            "original_price_sale_amt",  # 原价销售额
            "vip_discount_amt",      # 会员折扣额
            "hour_discount_amt",     # 时段折扣额
            "actual_amount",         # 实付金额 (不含运费)
            "return_sale_amt",       # 退货金额
            "member_discount_amt",   # 会员折扣额
            "promotion_discount_amt",  # 促销折扣额
            "discount_amt",          # 总折扣额

            # 会员销售金额
            "member_sale_amt",       # 会员销售额
            "bf19_member_sale_amt",  # 19点前会员销售额
        ]

    def get_sql(self, start_date: str, end_date: str) -> str:
        """生成销售域取数 SQL"""
        return f"""
        WITH online_sales AS (
            -- 线上订单明细
            SELECT
                store_id,
                business_date,
                article_id,
                -- 数量类
                qty_spec AS sale_qty,
                qty AS sale_piece_qty,
                0 AS return_sale_qty,
                0 AS gift_qty,
                CASE WHEN online_flag = 'Y' THEN qty_spec ELSE 0 END AS online_sale_qty,
                CASE WHEN online_flag = 'N' THEN qty_spec ELSE 0 END AS offline_sale_qty,
                CASE WHEN trans_hour < '19' THEN qty_spec ELSE 0 END AS bf19_sale_qty,
                CASE WHEN trans_hour >= '19' THEN qty_spec ELSE 0 END AS af19_sale_qty,
                CASE WHEN trans_hour < '12' THEN qty_spec ELSE 0 END AS bf12_sale_qty,
                COALESCE(actual_weight, qty_spec * unit_weight, 0) AS sales_weight,
                -- 交易金额类
                sales_amt AS sale_amt,
                p_lp_sub_amt AS original_price_sale_amt,
                COALESCE(vip_discount_amt, 0) AS vip_discount_amt,
                COALESCE(hour_discount_amt, 0) AS hour_discount_amt,
                COALESCE(actual_amount, 0) AS actual_amount,
                0 AS return_sale_amt,
                COALESCE(member_discount_amt, 0) AS member_discount_amt,
                COALESCE(p_promo_amt, 0) + COALESCE(f_promo_amt, 0) AS promotion_discount_amt,
                COALESCE(discount_amt, 0) AS discount_amt,
                -- 会员销售金额
                CASE WHEN customer_phone IS NOT NULL THEN sales_amt ELSE 0 END AS member_sale_amt,
                CASE WHEN customer_phone IS NOT NULL AND trans_hour < '19' THEN sales_amt ELSE 0 END AS bf19_member_sale_amt
            FROM hive.dsl.dsl_transaction_sotre_order_online_details_di
            WHERE business_date BETWEEN '{start_date}' AND '{end_date}'
              AND order_status = 'os.completed'
        ),
        offline_sales AS (
            -- 线下订单明细
            SELECT
                store_id,
                business_date,
                article_id,
                -- 数量类
                qty_spec AS sale_qty,
                qty AS sale_piece_qty,
                COALESCE(return_sale_qty, 0) AS return_sale_qty,
                COALESCE(gift_qty, 0) AS gift_qty,
                CASE WHEN online_flag = 'Y' THEN qty_spec ELSE 0 END AS online_sale_qty,
                CASE WHEN online_flag = 'N' THEN qty_spec ELSE 0 END AS offline_sale_qty,
                CASE WHEN pay_at < '19:00:00' THEN qty_spec ELSE 0 END AS bf19_sale_qty,
                CASE WHEN pay_at >= '19:00:00' THEN qty_spec ELSE 0 END AS af19_sale_qty,
                CASE WHEN pay_at < '12:00:00' THEN qty_spec ELSE 0 END AS bf12_sale_qty,
                COALESCE(actual_weight, qty_spec * unit_weight, 0) AS sales_weight,
                -- 交易金额类
                sales_amt AS sale_amt,
                p_lp_sub_amt AS original_price_sale_amt,
                COALESCE(vip_discount_amt, 0) AS vip_discount_amt,
                COALESCE(hour_discount_amt, 0) AS hour_discount_amt,
                COALESCE(actual_amount, 0) AS actual_amount,
                COALESCE(return_sale_amt, 0) AS return_sale_amt,
                COALESCE(member_discount_amt, 0) AS member_discount_amt,
                COALESCE(p_promo_amt, 0) + COALESCE(f_promo_amt, 0) AS promotion_discount_amt,
                COALESCE(discount_amt, 0) AS discount_amt,
                -- 会员销售金额
                CASE WHEN customer_phone IS NOT NULL THEN sales_amt ELSE 0 END AS member_sale_amt,
                CASE WHEN customer_phone IS NOT NULL AND pay_at < '19:00:00' THEN sales_amt ELSE 0 END AS bf19_member_sale_amt
            FROM hive.dsl.dsl_transaction_sotre_order_offline_details_di
            WHERE business_date BETWEEN '{start_date}' AND '{end_date}'
              AND order_status = 'os.completed'
        ),
        combined AS (
            SELECT * FROM online_sales
            UNION ALL
            SELECT * FROM offline_sales
        )
        -- 按门店+日期+商品汇总
        SELECT
            store_id,
            business_date,
            article_id,
            SUM(sale_qty) AS sale_qty,
            SUM(sale_piece_qty) AS sale_piece_qty,
            SUM(return_sale_qty) AS return_sale_qty,
            SUM(gift_qty) AS gift_qty,
            SUM(online_sale_qty) AS online_sale_qty,
            SUM(offline_sale_qty) AS offline_sale_qty,
            SUM(bf19_sale_qty) AS bf19_sale_qty,
            SUM(af19_sale_qty) AS af19_sale_qty,
            SUM(bf12_sale_qty) AS bf12_sale_qty,
            SUM(sales_weight) AS sales_weight,
            SUM(sale_amt) AS sale_amt,
            SUM(original_price_sale_amt) AS original_price_sale_amt,
            SUM(vip_discount_amt) AS vip_discount_amt,
            SUM(hour_discount_amt) AS hour_discount_amt,
            SUM(actual_amount) AS actual_amount,
            SUM(return_sale_amt) AS return_sale_amt,
            SUM(member_discount_amt) AS member_discount_amt,
            SUM(promotion_discount_amt) AS promotion_discount_amt,
            SUM(discount_amt) AS discount_amt,
            SUM(member_sale_amt) AS member_sale_amt,
            SUM(bf19_member_sale_amt) AS bf19_member_sale_amt
        FROM combined
        GROUP BY store_id, business_date, article_id
        """

    def _post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """后处理：数据类型转换"""
        # 确保数值列类型正确
        numeric_cols = self.atomic_fields
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df

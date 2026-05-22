"""
域⑦ 促销优惠域取数器

从促销优惠表提取促销相关的原子数据
- 源表: dsl.dsl_promotion_order_item_article_sale_info_di
- 原子字段: member_coupon_shop_amt, member_promo_amt, member_coupon_company_amt, shop_promo_amt
"""

from typing import List
import pandas as pd

from .base_extractor import BaseExtractor


class PromoExtractor(BaseExtractor):
    """
    促销优惠域取数器

    提取促销优惠金额，系统拆分后直接记录
    """

    @property
    def domain_name(self) -> str:
        return "promo"

    @property
    def atomic_fields(self) -> List[str]:
        """促销优惠域原子字段列表"""
        return [
            "member_coupon_shop_amt",    # 会员券-门店承担
            "member_promo_amt",          # 会员活动促销费
            "member_coupon_company_amt", # 会员券-公司承担
            "shop_promo_amt",            # 门店发起促销额
        ]

    def get_sql(self, start_date: str, end_date: str) -> str:
        """生成促销优惠域取数 SQL"""
        return f"""
        SELECT
            store_id,
            business_date,
            article_id,
            COALESCE(member_coupon_shop_amt, 0) AS member_coupon_shop_amt,
            COALESCE(member_promo_amt, 0) AS member_promo_amt,
            COALESCE(member_coupon_company_amt, 0) AS member_coupon_company_amt,
            COALESCE(shop_promo_amt, 0) AS shop_promo_amt
        FROM hive.dsl.dsl_promotion_order_item_article_sale_info_di
        WHERE business_date BETWEEN '{start_date}' AND '{end_date}'
        """

    def _post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """后处理：数据类型转换"""
        numeric_cols = self.atomic_fields
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df

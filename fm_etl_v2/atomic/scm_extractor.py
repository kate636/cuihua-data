"""
域③ 供应链域取数器

从 SAP 交付宽表提取供应链相关的原子数据
- 源表: dal_scm_dc_matnr_store_mode_delivery_daily_wide_sap_di
- 原子字段: 出库数量、退仓数量、订购数量、出库单价、退仓单价、SAP让利金额
"""

from typing import List
import pandas as pd

from .base_extractor import BaseExtractor


class SCMExtractor(BaseExtractor):
    """
    供应链域取数器

    提取出库、退仓、订购的数量、单价和 SAP 让利金额
    SAP 让利金额保留为原子，因为涉及多方分摊逻辑，不适合本地简化计算
    """

    @property
    def domain_name(self) -> str:
        return "scm"

    @property
    def atomic_fields(self) -> List[str]:
        """供应链域原子字段列表"""
        return [
            # 出库数量类
            "original_outstock_qty",      # 原价出库数量
            "promotion_outstock_qty",     # 促销出库数量
            "gift_outstock_qty",          # 赠品出库数量

            # 退仓数量类
            "return_stock_qty",           # 供应链退仓数量
            "store_return_qty_shop",      # 中台退货数量

            # 订购数量
            "store_order_qty",            # 门店订购数量 (订购单位)
            "order_qty_payean",           # 门店订购数量 (结算单位)

            # 出库单价类 (由金额/数量推导，但作为独立观测量使用)
            "outstock_unit_price",        # 含税出库单价
            "outstock_unit_price_notax",  # 不含税出库单价
            "outstock_cost_price",        # 含税出库成本单价
            "outstock_cost_price_notax",  # 不含税出库成本单价

            # 退仓单价类
            "return_unit_price",          # 含税退仓单价
            "return_unit_price_notax",    # 不含税退仓单价
            "return_cost_price",          # 含税退仓成本单价
            "return_cost_price_notax",    # 不含税退仓成本单价

            # 订购单价
            "order_unit_price",           # 订购单价

            # SAP 让利金额类 (SAP 直接记录，不可本地简化)
            "scm_promotion_amt_total",    # 出库让利总额
            "scm_promotion_amt_gift",     # 赠品出库让利
            "scm_bear_amt",               # 供应链承担非赠品让利
            "vendor_bear_amt",            # 供应商承担非赠品让利
            "business_bear_amt",          # 运营承担非赠品让利
            "market_bear_amt",            # 市场承担非赠品让利
            "vender_bear_gift_amt",       # 供应商承担赠品金额
            "scm_bear_gift_amt",          # 供应链承担赠品金额
            "adjustment_amt",             # 差异调整金额
        ]

    def get_sql(self, start_date: str, end_date: str) -> str:
        """生成供应链域取数 SQL"""
        return f"""
        SELECT
            store_id,
            business_date,
            article_id,

            -- 出库数量类
            COALESCE(original_outstock_qty, 0) AS original_outstock_qty,
            COALESCE(promotion_outstock_qty, 0) AS promotion_outstock_qty,
            COALESCE(gift_outstock_qty, 0) AS gift_outstock_qty,

            -- 退仓数量类
            COALESCE(store_return_scm_qty, 0) AS return_stock_qty,
            COALESCE(store_return_qty_shop, 0) AS store_return_qty_shop,

            -- 订购数量
            COALESCE(order_qty_order_unit, 0) AS store_order_qty,
            COALESCE(order_qty_payean, 0) AS order_qty_payean,

            -- 出库单价类
            CASE WHEN outstock_qty > 0 THEN outstock_amt / outstock_qty ELSE 0 END AS outstock_unit_price,
            CASE WHEN outstock_qty > 0 THEN outstock_amt_notax / outstock_qty ELSE 0 END AS outstock_unit_price_notax,
            CASE WHEN outstock_qty > 0 THEN outstock_cost / outstock_qty ELSE 0 END AS outstock_cost_price,
            CASE WHEN outstock_qty > 0 THEN outstock_cost_notax / outstock_qty ELSE 0 END AS outstock_cost_price_notax,

            -- 退仓单价类
            CASE WHEN store_return_scm_qty > 0 THEN store_return_scm_amt / store_return_scm_qty ELSE 0 END AS return_unit_price,
            CASE WHEN store_return_scm_qty > 0 THEN store_return_scm_amt_notax / store_return_scm_qty ELSE 0 END AS return_unit_price_notax,
            CASE WHEN store_return_scm_qty > 0 THEN return_stock_amt_cb / store_return_scm_qty ELSE 0 END AS return_cost_price,
            CASE WHEN store_return_scm_qty > 0 THEN store_return_scm_cost_notax / store_return_scm_qty ELSE 0 END AS return_cost_price_notax,

            -- 订购单价
            CASE WHEN order_qty_order_unit > 0 THEN order_amt / order_qty_order_unit ELSE 0 END AS order_unit_price,

            -- SAP 让利金额类
            COALESCE(total_benefit_amt, 0) AS scm_promotion_amt_total,
            COALESCE(total_gift_benefit_amt, 0) AS scm_promotion_amt_gift,
            COALESCE(scm_bear_nogift_benefit_amt, 0) AS scm_bear_amt,
            COALESCE(vendor_bear_nogift_benefit_amt, 0) AS vendor_bear_amt,
            COALESCE(business_bear_nogift_benefit_amt, 0) AS business_bear_amt,
            COALESCE(market_bear_nogift_benefit_amt, 0) AS market_bear_amt,
            COALESCE(vender_bear_gift_amt, 0) AS vender_bear_gift_amt,
            COALESCE(scm_bear_gift_amt, 0) AS scm_bear_gift_amt,
            COALESCE(adjustment_amt, 0) AS adjustment_amt

        FROM hive.dal_scm.dal_scm_dc_matnr_store_mode_delivery_daily_wide_sap_di
        WHERE business_date BETWEEN '{start_date}' AND '{end_date}'
        """

    def _post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """后处理：数据类型转换"""
        numeric_cols = self.atomic_fields
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df

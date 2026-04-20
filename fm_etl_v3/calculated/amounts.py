"""
金额计算器

所有金额 = 数量 × 单价，在 DuckDB 中完成，写入 t_calc_amounts。

覆盖 Layer -1 中:
  3.1 金额类计算 (receive_amt, init_stock_amt, end_stock_amt, ...)
  3.2 供应链金额计算 (out_stock_pay_amt, return_stock_pay_amt, ...)
  3.4 损耗类计算 (lost_amt, lost_qty)
  3.8 定价/预期类 (expect_outstock_amt, discount_amt_cate, scm_promotion_amt)
"""

from __future__ import annotations

from ..connectors import DuckDBStore
from ..utils import get_logger


class AmountsCalculator:
    TARGET_TABLE = "t_calc_amounts"

    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("AmountsCalculator")

    def run(self) -> None:
        """计算所有金额指标，写入 t_calc_amounts。"""
        self._log.info("calculating amounts ...")
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
        self._duck.execute(f"""
        CREATE TABLE {self.TARGET_TABLE} AS
        SELECT
            w.store_id,
            w.business_date,
            w.article_id,
            w.day_clear,

            -- ── 3.1 基本金额 ────────────────────────────────────────────
            -- 进货金额 = receive_qty × avg_purchase_price
            inv.receive_qty * ap.avg_purchase_price         AS receive_amt,
            -- 期初库存金额 = init_stock_qty × avg_price
            inv.init_stock_qty * ap.avg_price               AS init_stock_amt,
            -- 期末库存金额 = end_stock_qty × avg_price
            inv.end_stock_qty * ap.avg_price                AS end_stock_amt,
            -- 已知损耗金额 = know_lost_qty × cost_price
            inv.know_lost_qty * w.cost_price                AS know_lost_amt,
            -- 未知损耗金额 = unknow_lost_qty × cost_price
            inv.unknow_lost_qty * w.cost_price              AS unknow_lost_amt,
            -- 加工转入金额 = compose_in_qty × cost_price
            inv.compose_in_qty * w.cost_price               AS compose_in_amt,
            -- 加工转出金额 = compose_out_qty × cost_price
            inv.compose_out_qty * w.cost_price              AS compose_out_amt,

            -- ── 3.2 供应链金额 ──────────────────────────────────────────
            -- 总出库数量
            w.original_outstock_qty + w.promotion_outstock_qty + w.gift_outstock_qty
                                                            AS total_outstock_qty,
            -- 出库金额 (含税/不含税/成本)
            (w.original_outstock_qty + w.promotion_outstock_qty + w.gift_outstock_qty)
                * w.outstock_unit_price                     AS out_stock_pay_amt,
            (w.original_outstock_qty + w.promotion_outstock_qty + w.gift_outstock_qty)
                * w.outstock_unit_price_notax               AS out_stock_pay_amt_notax,
            (w.original_outstock_qty + w.promotion_outstock_qty + w.gift_outstock_qty)
                * w.outstock_cost_price                     AS out_stock_amt_cb,
            (w.original_outstock_qty + w.promotion_outstock_qty + w.gift_outstock_qty)
                * w.outstock_cost_price_notax               AS out_stock_amt_cb_notax,
            -- 退仓金额 (含税/不含税/成本)
            w.return_stock_qty * w.return_unit_price        AS return_stock_pay_amt,
            w.return_stock_qty * w.return_unit_price_notax  AS return_stock_pay_amt_notax,
            w.return_stock_qty * w.return_cost_price        AS return_stock_amt_cb,
            w.return_stock_qty * w.return_cost_price_notax  AS return_stock_amt_cb_notax,
            -- 中台退仓金额
            w.store_return_qty_shop * w.return_unit_price   AS store_return_amt_shop,
            -- 原价出库金额
            w.original_outstock_qty * w.dc_original_price   AS original_outstock_amt,
            -- 订购金额
            w.store_order_qty * w.order_unit_price           AS order_amt,
            -- 非赠品出库让利
            w.scm_promotion_amt_total - w.scm_promotion_amt_gift
                                                            AS scm_promotion_amt,

            -- ── 3.4 损耗类 ─────────────────────────────────────────────
            inv.know_lost_qty + inv.unknow_lost_qty         AS lost_qty,
            inv.know_lost_qty * w.cost_price
              + inv.unknow_lost_qty * w.cost_price          AS lost_amt,

            -- ── 3.8 定价/预期类 ─────────────────────────────────────────
            -- 预期出库金额 = out_stock_pay_amt + scm_promotion_amt_total
            (w.original_outstock_qty + w.promotion_outstock_qty + w.gift_outstock_qty)
                * w.outstock_unit_price + w.scm_promotion_amt_total
                                                            AS expect_outstock_amt,
            -- 促销折扣额 = discount_amt - hour_discount_amt
            w.discount_amt - w.hour_discount_amt            AS discount_amt_cate,
            -- 促进出库折扣
            w.member_coupon_shop_amt + w.store_paylevel_discount
                                                            AS member_coupon_shop_amt_adj,

            -- 公司费用合计
            w.no_ordercoupon_company_promotion_amt
              + w.ordercoupon_company_promotion_amt         AS company_cost_amt,

            -- 进货重量 (单位重量不在原子层，此处用 dim_goods)
            inv.receive_qty * COALESCE(g.unit_weight, 0)   AS purchase_weight

        FROM t_atomic_wide w
        JOIN t_calc_inventory inv
            ON w.store_id = inv.store_id
            AND w.business_date = inv.business_date
            AND w.article_id = inv.article_id
        JOIN t_calc_avg_price ap
            ON w.store_id = ap.store_id
            AND w.business_date = ap.business_date
            AND w.article_id = ap.article_id
        LEFT JOIN dim_goods g ON w.article_id = g.article_id
        """)
        rows = self._duck.row_count(self.TARGET_TABLE)
        self._log.info(f"t_calc_amounts: {rows} rows")

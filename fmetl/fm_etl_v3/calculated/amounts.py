"""
金额计算器（v9 · 加权平均 effective_unit_cost）

核心修正:
  1. receive_amt 全用 calculated (receive_qty × effective_unit_cost)，不再兜底源表
  2. lost_amt = know_lost_amt + unknow_lost_amt
  3. compose_in_amt / compose_out_amt 使用 avg_inbound_price，不用 effective_unit_cost
  4. know_lost_amt / unknow_lost_amt 各自用 effective_unit_cost 计算

覆盖 Layer -1:
  3.1 金额类  receive_amt / init_stock_amt / end_stock_amt / compose_in_amt / compose_out_amt
              know_lost_amt / unknow_lost_amt / lost_qty / lost_amt
  3.2 供应链  out_stock_*, return_stock_*
  3.8 定价类  expect_outstock_amt / discount_amt_cate / member_coupon_shop_amt_adj
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
        self._log.info("calculating amounts (v9) ...")
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
        self._duck.execute(f"""
        CREATE TABLE {self.TARGET_TABLE} AS
        SELECT
            w.store_id,
            w.business_date,
            w.article_id,
            w.day_clear,

            -- ── 3.1 基本金额 ──────────────────────────────────────────────
            -- 进货额：全用 qty × effective_unit_cost（v9 修正）
            inv.receive_qty * c.effective_unit_cost              AS receive_amt,
            -- 期初库存额：全用 qty × effective_unit_cost（v9 修正，双口径对齐）
            inv.init_stock_qty * c.effective_unit_cost           AS init_stock_amt,
            -- 期末库存额：全用 stock 自算值（v9 修正，双口径对齐）
            COALESCE(stk.end_stock_amt_self, 0)                  AS end_stock_amt,
            -- compose金额：用 effective_unit_cost（v9 修正，双口径对齐）
            inv.compose_in_qty  * c.effective_unit_cost            AS compose_in_amt,
            inv.compose_out_qty * c.effective_unit_cost            AS compose_out_amt,

            -- 已知损耗金额：全用 qty × effective_unit_cost（v9 修正，双口径对齐）
            inv.know_lost_qty * c.effective_unit_cost            AS know_lost_amt,
            -- 未知损耗金额：全用 stock 自算值（v9 修正，双口径对齐）
            COALESCE(stk.unknow_lost_amt_self, 0)                AS unknow_lost_amt,

            -- ── 3.4 损耗合计 ─────────────────────────────────────────────
            inv.know_lost_qty + COALESCE(stk.unknow_lost_qty, 0) AS lost_qty,
            -- lost_amt = know_lost_amt + unknow_lost_amt（全用计算值）
            inv.know_lost_qty * c.effective_unit_cost
              + COALESCE(stk.unknow_lost_amt_self, 0)            AS lost_amt,

            -- ── 3.2 供应链金额 ──────────────────────────────────────────
            w.original_outstock_qty + w.promotion_outstock_qty + w.gift_outstock_qty
                                                                  AS total_outstock_qty,
            (w.original_outstock_qty + w.promotion_outstock_qty + w.gift_outstock_qty)
                * w.outstock_unit_price                           AS out_stock_pay_amt,
            (w.original_outstock_qty + w.promotion_outstock_qty + w.gift_outstock_qty)
                * w.outstock_unit_price_notax                     AS out_stock_pay_amt_notax,
            (w.original_outstock_qty + w.promotion_outstock_qty + w.gift_outstock_qty)
                * w.outstock_cost_price                           AS out_stock_amt_cb,
            (w.original_outstock_qty + w.promotion_outstock_qty + w.gift_outstock_qty)
                * w.outstock_cost_price_notax                     AS out_stock_amt_cb_notax,
            w.return_stock_qty * w.return_unit_price              AS return_stock_pay_amt,
            w.return_stock_qty * w.return_unit_price_notax        AS return_stock_pay_amt_notax,
            w.return_stock_qty * w.return_cost_price              AS return_stock_amt_cb,
            w.return_stock_qty * w.return_cost_price_notax        AS return_stock_amt_cb_notax,
            w.store_return_qty_shop * w.return_unit_price         AS store_return_amt_shop,
            w.original_outstock_qty * w.dc_original_price         AS original_outstock_amt,
            w.store_order_qty * w.order_unit_price                AS order_amt,
            w.scm_promotion_amt_total - w.scm_promotion_amt_gift  AS scm_promotion_amt,
            w.scm_promotion_amt_total                             AS scm_promotion_amt_total,

            -- ── 3.8 定价 / 预期 ────────────────────────────────────────
            (w.original_outstock_qty + w.promotion_outstock_qty + w.gift_outstock_qty)
                * w.outstock_unit_price + w.scm_promotion_amt_total
                                                                  AS expect_outstock_amt,
            w.discount_amt - w.hour_discount_amt                  AS discount_amt_cate,
            w.member_coupon_shop_amt + w.store_paylevel_discount  AS member_coupon_shop_amt_adj,
            w.no_ordercoupon_company_promotion_amt
              + w.ordercoupon_company_promotion_amt               AS company_cost_amt,

            -- ── 进货重量 ─────────────────────────────────────────────────
            inv.receive_qty * COALESCE(g.unit_weight, 0)          AS purchase_weight

        FROM t_atomic_wide w
        JOIN t_calc_inventory inv
            ON w.store_id = inv.store_id
            AND w.business_date = inv.business_date
            AND w.article_id = inv.article_id
        LEFT JOIN t_calc_sku_cost c
            ON c.store_id = w.store_id
            AND c.business_date = w.business_date
            AND c.article_id = w.article_id
        LEFT JOIN t_calc_stock stk
            ON stk.store_id = w.store_id
            AND stk.business_date = w.business_date
            AND stk.article_id = w.article_id
            AND stk.day_clear = w.day_clear
        LEFT JOIN (SELECT DISTINCT article_id, unit_weight FROM dim_goods) g ON w.article_id = g.article_id
        """)
        rows = self._duck.row_count(self.TARGET_TABLE)
        self._log.info(f"t_calc_amounts: {rows} rows")

        # Phase 7: 关键节点 log — receive_amt / lost_amt 汇总
        summary = self._duck._conn.execute(f"""
            SELECT
                COUNT(*)                                          AS row_cnt,
                ROUND(SUM(receive_amt), 2)                        AS total_receive_amt,
                ROUND(SUM(lost_amt), 2)                           AS total_lost_amt,
                ROUND(SUM(know_lost_amt), 2)                      AS total_know_lost_amt,
                ROUND(SUM(unknow_lost_amt), 2)                    AS total_unknow_lost_amt
            FROM {self.TARGET_TABLE}
        """).fetchone()
        if summary:
            self._log.info(
                f"Amounts summary: {summary[0]} rows, "
                f"Σreceive_amt={summary[1]}, "
                f"Σlost_amt={summary[2]} (know={summary[3]} + unknow={summary[4]})"
            )

"""
毛利计算器

基于 t_atomic_wide + t_calc_inventory + t_calc_amounts + t_calc_avg_price
计算所有毛利类指标，写入 t_calc_profit。

覆盖 Layer -1 中:
  3.6 毛利类计算 (profit_amt, sale_cost_amt, pre_profit_amt, allowance_amt_profit)
  3.7 供应链毛利类 (scm_fin_article_income/cost/profit, full_link_article_profit)
  预期销售额 (pre_sale_amt)
"""

from __future__ import annotations

from ..connectors import DuckDBStore
from ..utils import get_logger


class ProfitCalculator:
    TARGET_TABLE = "t_calc_profit"

    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("ProfitCalculator")

    def run(self) -> None:
        """计算毛利类指标，写入 t_calc_profit。"""
        self._log.info("calculating profit metrics ...")
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
        self._duck.execute(f"""
        CREATE TABLE {self.TARGET_TABLE} AS
        SELECT
            w.store_id,
            w.business_date,
            w.article_id,
            w.day_clear,

            -- ── 运营毛利额 ───────────────────────────────────────────────
            -- profit_amt = sale_amt - (receive_amt + compose_in_amt - compose_out_amt)
            --              + (end_stock_amt - init_stock_amt)
            w.sale_amt
                - (amt.receive_amt + amt.compose_in_amt - amt.compose_out_amt)
                + (amt.end_stock_amt - amt.init_stock_amt)              AS profit_amt,

            -- ── 销售成本 ────────────────────────────────────────────────
            -- 最终销售成本
            CASE
                WHEN w.day_clear = '0'
                THEN amt.receive_amt + amt.compose_in_amt - amt.compose_out_amt - amt.lost_amt
                ELSE w.sale_qty * ap.avg_purchase_price
            END AS sale_cost_amt,

            -- ── 预期毛利额 ──────────────────────────────────────────────
            w.original_price_sale_amt
                - CASE
                    WHEN w.day_clear = '0'
                    THEN amt.receive_amt + amt.compose_in_amt - amt.compose_out_amt - amt.lost_amt
                    ELSE w.sale_qty * ap.avg_purchase_price
                  END                                                   AS pre_profit_amt,

            -- ── 补贴后毛利额 ────────────────────────────────────────────
            w.sale_amt - amt.receive_amt + w.allowance_amt
                + (amt.end_stock_amt - amt.init_stock_amt)              AS allowance_amt_profit,

            -- ── 供应链财务收入/成本/毛利 ────────────────────────────────
            -- scm_fin_article_income = out_stock_pay_amt_notax - |return_stock_pay_amt_notax|
            amt.out_stock_pay_amt_notax - ABS(amt.return_stock_pay_amt_notax)
                                                                        AS scm_fin_article_income,
            -- scm_fin_article_cost = out_stock_amt_cb_notax - |return_stock_amt_cb_notax|
            amt.out_stock_amt_cb_notax - ABS(amt.return_stock_amt_cb_notax)
                                                                        AS scm_fin_article_cost,
            -- scm_fin_article_profit
            (amt.out_stock_pay_amt_notax - ABS(amt.return_stock_pay_amt_notax))
            - (amt.out_stock_amt_cb_notax - ABS(amt.return_stock_amt_cb_notax))
                                                                        AS scm_fin_article_profit,

            -- ── 全链路毛利 ──────────────────────────────────────────────
            -- full_link_article_profit = article_profit_amt + scm_fin_income - scm_fin_cost
            (
                w.sale_amt
                - (amt.receive_amt + amt.compose_in_amt - amt.compose_out_amt)
                + (amt.end_stock_amt - amt.init_stock_amt)
            )
            + (amt.out_stock_pay_amt_notax - ABS(amt.return_stock_pay_amt_notax))
            - (amt.out_stock_amt_cb_notax - ABS(amt.return_stock_amt_cb_notax))
                                                                        AS full_link_article_profit,

            -- ── 预期销售额 ──────────────────────────────────────────────
            -- pre_sale_amt = (lost_qty × original_price) + original_price_sale_amt
            amt.lost_qty * w.original_price + w.original_price_sale_amt AS pre_sale_amt,

            -- ── 理论进货额 ──────────────────────────────────────────────
            -- pre_inbound_amount = receive_qty × dc_original_price
            inv.receive_qty * w.dc_original_price                       AS pre_inbound_amount

        FROM t_atomic_wide w
        JOIN t_calc_inventory inv
            ON w.store_id = inv.store_id
            AND w.business_date = inv.business_date
            AND w.article_id = inv.article_id
        JOIN t_calc_amounts amt
            ON w.store_id = amt.store_id
            AND w.business_date = amt.business_date
            AND w.article_id = amt.article_id
        JOIN t_calc_avg_price ap
            ON w.store_id = ap.store_id
            AND w.business_date = ap.business_date
            AND w.article_id = ap.article_id
        """)
        rows = self._duck.row_count(self.TARGET_TABLE)
        self._log.info(f"t_calc_profit: {rows} rows")

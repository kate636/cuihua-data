"""
毛利计算器

基于 t_atomic_wide + t_calc_inventory + t_calc_amounts + t_calc_sku_cost
计算所有毛利类指标，写入 t_calc_profit。

覆盖 Layer -1 中:
  3.6 毛利类计算 (profit_amt, sale_cost_amt, pre_profit_amt, allowance_amt_profit)
  3.7 供应链毛利类 (scm_fin_article_income/cost/profit, full_link_article_profit)
  预期销售额 (pre_sale_amt)

v9 修正:
  - 销售成本（非日清）改用 effective_unit_cost，不再用 avg_purchase_price
  - 日清分支保持原逻辑（金额流水）
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
        self._log.info("calculating profit metrics (v9 effective_unit_cost) ...")
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
            CASE
                WHEN w.day_clear = '0'
                THEN amt.receive_amt + amt.compose_in_amt - amt.compose_out_amt - amt.lost_amt
                ELSE w.sale_qty * sc.effective_unit_cost
            END AS sale_cost_amt,

            -- ── 预期毛利额 ──────────────────────────────────────────────
            w.original_price_sale_amt
                - CASE
                    WHEN w.day_clear = '0'
                    THEN amt.receive_amt + amt.compose_in_amt - amt.compose_out_amt - amt.lost_amt
                    ELSE w.sale_qty * sc.effective_unit_cost
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
            inv.receive_qty * w.dc_original_price                       AS pre_inbound_amount,

            -- ── 双口径毛利（v9 对齐）─────────────────────────────────────
            -- store_profit_sales = 销售方程毛利 = sale_amt - sale_cost_amt - lost_amt
            w.sale_amt
                - CASE
                    WHEN w.day_clear = '0'
                    THEN amt.receive_amt + amt.compose_in_amt - amt.compose_out_amt - amt.lost_amt
                    ELSE w.sale_qty * sc.effective_unit_cost
                  END
                - amt.lost_amt                                         AS store_profit_sales,
            -- store_profit_stock = 库存方程毛利 = profit_amt
            w.sale_amt
                - (amt.receive_amt + amt.compose_in_amt - amt.compose_out_amt)
                + (amt.end_stock_amt - amt.init_stock_amt)              AS store_profit_stock,
            -- 差异
            (w.sale_amt
                - CASE
                    WHEN w.day_clear = '0'
                    THEN amt.receive_amt + amt.compose_in_amt - amt.compose_out_amt - amt.lost_amt
                    ELSE w.sale_qty * sc.effective_unit_cost
                  END
                - amt.lost_amt)
            - (w.sale_amt
                - (amt.receive_amt + amt.compose_in_amt - amt.compose_out_amt)
                + (amt.end_stock_amt - amt.init_stock_amt))             AS store_profit_diff,

            -- ── 供应链毛利量（占位）────────────────────────────────────────
            0                                                         AS supply_chain_profit_qty,

            -- ── 成本来源标识 + effective_unit_cost（从 t_calc_sku_cost）────
            COALESCE(sc.cost_source, 'MISSING')                       AS cost_source,
            COALESCE(sc.effective_unit_cost, 0)                       AS effective_unit_cost

        FROM t_atomic_wide w
        JOIN t_calc_inventory inv
            ON w.store_id = inv.store_id
            AND w.business_date = inv.business_date
            AND w.article_id = inv.article_id
        JOIN t_calc_amounts amt
            ON w.store_id = amt.store_id
            AND w.business_date = amt.business_date
            AND w.article_id = amt.article_id
        LEFT JOIN t_calc_sku_cost sc
            ON sc.store_id = w.store_id
            AND sc.business_date = w.business_date
            AND sc.article_id = w.article_id
        """)
        rows = self._duck.row_count(self.TARGET_TABLE)
        self._log.info(f"t_calc_profit: {rows} rows")

        # Phase 7: 关键节点 log — 双口径毛利对齐检查
        diff_stats = self._duck._conn.execute(f"""
            SELECT
                COUNT(*)                                                AS total_skus,
                SUM(CASE WHEN ABS(store_profit_diff) > 0.01 THEN 1 ELSE 0 END) AS mismatch_cnt,
                ROUND(MAX(ABS(store_profit_diff)), 4)                   AS max_diff,
                ROUND(SUM(store_profit_sales), 2)                       AS total_sales_profit,
                ROUND(SUM(store_profit_stock), 2)                       AS total_stock_profit
            FROM {self.TARGET_TABLE}
        """).fetchone()
        if diff_stats:
            self._log.info(
                f"Profit dual-caliber: {diff_stats[0]} SKUs, "
                f"mismatch(>0.01)={diff_stats[1]}, max_diff={diff_stats[2]}, "
                f"Σsales_profit={diff_stats[3]}, Σstock_profit={diff_stats[4]}"
            )
            if diff_stats[1] > 0:
                self._log.warning(
                    f"Profit dual-caliber MISMATCH: {diff_stats[1]} SKUs "
                    f"with |diff| > 0.01, max={diff_stats[2]}"
                )

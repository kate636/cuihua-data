"""
t_calc_profit — 门店毛利 (v10 Python 重写)

v10 核心公式:
  profit = sale
         - receive - bom_in + bom_out
         - compose_in + compose_out
         + end - init
         - lost

sale_cost_amt:
  日清: receive + bom_in - bom_out + compose_in - compose_out - lost
  非日清: sale_qty × euc

v10 删除冗余: 不再区分 profit_amt / store_profit_stock，只保留一个 profit_amt
"""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd
from ..connectors import DuckDBStore
from ..utils import get_logger


class ProfitCalculator:
    TARGET_TABLE = "t_calc_profit"

    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("ProfitCalculator")

    def run(self) -> None:
        """计算门店毛利。大分类诊断已迁移至 FM 底表层 (sku_dim.py)。"""
        self._log.info("calculating profit (v10 Python) ...")
        conn = self._duck._conn

        # ── 1. 加载 t_calc_stock ─────────────────────────────────────
        stock_df = conn.execute("""
            SELECT
                store_id, business_date, article_id, day_clear,
                receive_qty, receive_amt,
                bom_in_qty, bom_in_amt,
                bom_out_qty, bom_out_amt,
                compose_in_qty, compose_in_amt,
                compose_out_qty, compose_out_amt,
                sale_qty, sale_amt,
                know_lost_qty, know_lost_amt,
                unknow_lost_qty, unknow_lost_amt,
                lost_qty, lost_amt,
                init_stock_qty, init_stock_amt,
                end_stock_qty, end_stock_amt,
                effective_unit_cost, cost_source,
                out_stock_pay_amt_notax, return_stock_pay_amt_notax,
                out_stock_amt_cb
            FROM t_calc_stock
        """).df()

        if stock_df.empty:
            self._log.warning("t_calc_stock is empty")
            self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
            return

        # ── 2. 加载 t_atomic_wide（需要原始价格和补贴字段）───────────
        wide_df = conn.execute("""
            SELECT
                store_id, business_date, article_id, day_clear,
                original_price_sale_amt, allowance_amt,
                original_price, dc_original_price,
                outstock_cost_price_notax,
                return_cost_price_notax,
                original_outstock_qty, return_stock_qty
            FROM t_atomic_wide
        """).df()

        # ── 3. Python merge ──────────────────────────────────────────
        df = stock_df.merge(wide_df,
                            on=['store_id', 'business_date', 'article_id',
                                'day_clear'],
                            how='left')

        # fill SCM fields
        for c in ['outstock_cost_price_notax', 'return_cost_price_notax',
                   'original_outstock_qty', 'return_stock_qty',
                   'original_price_sale_amt', 'allowance_amt',
                   'original_price', 'dc_original_price']:
            if c in df.columns:
                df[c] = df[c].fillna(0)

        # ── 4. Python: 核心毛利公式 ──────────────────────────────────
        # 损耗已通过库存方程反映在 end_stock 中(end减少→成本增加→利润减少)，
        # 不再额外减去 lost_amt，避免重复扣减。
        df['profit_amt'] = (
            df['sale_amt']
            - df['receive_amt']
            - df['bom_in_amt'] + df['bom_out_amt']
            - df['compose_in_amt'] + df['compose_out_amt']
            + df['end_stock_amt'] - df['init_stock_amt']
        )

        # ── 5. Python: 销售成本 ──────────────────────────────────────
        # 日清/非日清统一: sale_qty × euc
        # 日清差异仅在stock.py中(end强制=0→残差转unknow_lost), 不影响sale_cost公式
        df['sale_cost_amt'] = df['sale_qty'] * df['effective_unit_cost']

        # ── 6. Python: 预期毛利额（原价口径）─────────────────────────
        expected_cost = df['sale_qty'] * df['effective_unit_cost']
        df['pre_profit_amt'] = df['original_price_sale_amt'] - expected_cost

        # ── 7. Python: 补贴后毛利 ────────────────────────────────────
        df['allowance_amt_profit'] = (
            df['sale_amt'] - df['receive_amt'] + df['allowance_amt']
            + df['end_stock_amt'] - df['init_stock_amt']
        )

        # ── 8. Python: SCM 金融毛利 ──────────────────────────────────
        out_stock_amt_cb_notax = (
            df['outstock_cost_price_notax'] * df['original_outstock_qty'])
        return_stock_amt_cb_notax = (
            df['return_cost_price_notax'] * df['return_stock_qty'])

        df['scm_fin_article_income'] = (
            df['out_stock_pay_amt_notax'].abs()
            - df['return_stock_pay_amt_notax'].abs())
        df['scm_fin_article_cost'] = (
            out_stock_amt_cb_notax.abs() - return_stock_amt_cb_notax.abs())
        df['scm_fin_article_profit'] = (
            df['scm_fin_article_income'] - df['scm_fin_article_cost'])

        # ── 9. Python: 全链路毛利 ────────────────────────────────────
        df['full_link_article_profit'] = (
            df['profit_amt']
            + df['scm_fin_article_income']
            - df['scm_fin_article_cost']
        )

        # ── 10. Python: 预期销售额 / 理论进货额 ──────────────────────
        df['pre_sale_amt'] = (
            df['lost_qty'] * df['original_price']
            + df['original_price_sale_amt'])
        df['pre_inbound_amount'] = (
            df['receive_qty'] * df['dc_original_price'])

        # ── 11. 写出结果（分区覆盖，保留历史数据）───────────────────
        out_cols = {
            'store_id': df['store_id'],
            'business_date': df['business_date'],
            'article_id': df['article_id'],
            'day_clear': df['day_clear'],
            'profit_amt': df['profit_amt'],
            'sale_cost_amt': df['sale_cost_amt'],
            'pre_profit_amt': df['pre_profit_amt'],
            'allowance_amt_profit': df['allowance_amt_profit'],
            'scm_fin_article_income': df['scm_fin_article_income'],
            'scm_fin_article_cost': df['scm_fin_article_cost'],
            'scm_fin_article_profit': df['scm_fin_article_profit'],
            'full_link_article_profit': df['full_link_article_profit'],
            'pre_sale_amt': df['pre_sale_amt'],
            'pre_inbound_amount': df['pre_inbound_amount'],
            'cost_source': df['cost_source'],
            'effective_unit_cost': df['effective_unit_cost'],
        }
        out_df = pd.DataFrame(out_cols)
        # 首次建表（空结构）
        conn.execute(f"CREATE TABLE IF NOT EXISTS {self.TARGET_TABLE} AS SELECT * FROM out_df LIMIT 0")
        # 按日期分区覆盖
        date_min, date_max = out_df['business_date'].min(), out_df['business_date'].max()
        conn.execute(f"DELETE FROM {self.TARGET_TABLE} WHERE business_date BETWEEN '{date_min}' AND '{date_max}'")
        conn.execute(f"INSERT INTO {self.TARGET_TABLE} SELECT * FROM out_df")

        rows = self._duck.row_count(self.TARGET_TABLE)
        self._log.info(
            f"t_calc_profit: {rows} rows, "
            f"Σprofit={df['profit_amt'].sum():.2f}"
        )

        # v10 fix: 大分类毛利汇总已迁移到 FM 底表层 (sku_dim.py)
        # 计算层不再依赖 dim_goods 做分类诊断


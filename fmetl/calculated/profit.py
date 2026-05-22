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

    def run(self, debug_categories: list | None = None) -> None:
        """计算门店毛利。debug_categories 可传入需输出详细日志的大分类列表。"""
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

        # ── 11. 写出结果 ─────────────────────────────────────────────
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
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
        conn.execute(f"CREATE TABLE {self.TARGET_TABLE} AS SELECT * FROM out_df")

        rows = self._duck.row_count(self.TARGET_TABLE)
        self._log.info(
            f"t_calc_profit: {rows} rows, "
            f"Σprofit={df['profit_amt'].sum():.2f}"
        )

        # ── 12. 添加大分类标签 ──────────────────────────────────────────
        self._add_category_labels(df)

        # ── 13. 大分类毛利汇总（始终输出）───────────────────────────
        self._log_profit_summary(df)

        # ── 14. 详细结算日志（排查异常分类）───────────────────────────
        if debug_categories:
            self._log_detail(df, debug_categories)

    # ── 分类标签 ──────────────────────────────────────────────────────
    @staticmethod
    def _remap_category(l1: str, l2: str, l3: str) -> str:
        if l2 in ('蛋类', '烘焙类'):
            return l2
        if l2 in ('冷藏奶制品类', '饮料类'):
            return '乳制品及水饮类'
        if l1 == '肉禽蛋类' and l2 != '蛋类':
            return '肉禽类'
        if l3.endswith('熟食'):
            return '熟食类'
        if l1 in ('冷藏及加工类', '预制菜'):
            return '冷藏加工及预制菜类'
        return l1

    def _add_category_labels(self, df: pd.DataFrame) -> None:
        try:
            cat_df = self._duck._conn.execute("""
                SELECT DISTINCT
                    article_id,
                    category_level1_description AS l1,
                    category_level2_description AS l2,
                    category_level3_description AS l3
                FROM dim_goods
            """).df()
            cat_df['l1'] = cat_df['l1'].fillna('').astype(str)
            cat_df['l2'] = cat_df['l2'].fillna('').astype(str)
            cat_df['l3'] = cat_df['l3'].fillna('').astype(str)
            cat_df['cat'] = cat_df.apply(
                lambda r: self._remap_category(r['l1'], r['l2'], r['l3']), axis=1)
            cat_df = cat_df[['article_id', 'cat']].drop_duplicates(subset='article_id')
            df['cat'] = df.merge(cat_df, on='article_id', how='left')['cat'].fillna('(无分类)')
        except Exception as e:
            self._log.warning(f"添加大分类标签失败: {e}")
            df['cat'] = '(无分类)'

    # ── 大分类毛利汇总 ────────────────────────────────────────────────────
    def _log_profit_summary(self, df: pd.DataFrame) -> None:
        """按重映射大分类输出毛利汇总，用于对比 QDM 差距诊断。"""
        try:
            # 计算每行 EI，避免 groupby lambda 的复杂性
            df['_EI'] = df['end_stock_amt'] - df['init_stock_amt']
            summary = df.groupby('cat').agg(
                rows=('article_id', 'count'),
                profit=('profit_amt', 'sum'),
                sale=('sale_amt', 'sum'),
                receive=('receive_amt', 'sum'),
                bom_in=('bom_in_amt', 'sum'),
                bom_out=('bom_out_amt', 'sum'),
                compose_in=('compose_in_amt', 'sum'),
                compose_out=('compose_out_amt', 'sum'),
                EI=('_EI', 'sum'),
                lost=('lost_amt', 'sum'),
                euc_avg=('effective_unit_cost', 'mean'),
            ).round(0)
            self._log.info("─── 大分类毛利诊断 ───")
            self._log.info(
                f"{'分类':<16} {'rows':>5} {'profit':>8} {'sale':>8} {'recv':>8} "
                f"{'EI':>8} {'bomI':>6} {'bomO':>6} {'lost':>6} {'euc_avg':>7}"
            )
            for cat, r in summary.iterrows():
                self._log.info(
                    f"{cat:<16} {int(r['rows']):>5} {r['profit']:>8.0f} {r['sale']:>8.0f} "
                    f"{r['receive']:>8.0f} {r['EI']:>8.0f} "
                    f"{r['bom_in']:>6.0f} {r['bom_out']:>6.0f} {r['lost']:>6.0f} "
                    f"{r['euc_avg']:>7.1f}"
                )
        except Exception as e:
            self._log.warning(f"大分类毛利诊断跳过: {e}")

    # ── 详细日志 ────────────────────────────────────────────────────────
    def _log_detail(self, df: pd.DataFrame, categories: list) -> None:
        """输出指定大分类的每个 SKU 门店毛利计算明细。

        使用 run() 中 _add_category_labels 已添加的 'cat' 列。
        """
        for cat in categories:
            sub = df[df['cat'] == cat].copy()
            if sub.empty:
                self._log.info(f"[{cat}] 无SKU数据")
                continue

            sub = sub.sort_values('profit_amt')

            # 分类汇总
            total_sale = sub['sale_amt'].sum()
            total_profit = sub['profit_amt'].sum()
            total_receive = sub['receive_amt'].sum()
            total_bom_in = sub['bom_in_amt'].sum()
            total_bom_out = sub['bom_out_amt'].sum()
            total_compose_in = sub['compose_in_amt'].sum()
            total_compose_out = sub['compose_out_amt'].sum()
            total_lost = sub['lost_amt'].sum()
            total_init = sub['init_stock_amt'].sum()
            total_end = sub['end_stock_amt'].sum()

            self._log.info(
                f"[{cat}] ΣSKU={len(sub)} "
                f"profit={total_profit:.2f} "
                f"sale={total_sale:.2f} "
                f"receive={total_receive:.2f} "
                f"bom_in={total_bom_in:.2f} "
                f"bom_out={total_bom_out:.2f} "
                f"comp_in={total_compose_in:.2f} "
                f"comp_out={total_compose_out:.2f} "
                f"lost={total_lost:.2f} "
                f"init={total_init:.2f} "
                f"end={total_end:.2f}"
            )

            # 每个 SKU 一行
            lines = []
            for _, r in sub.iterrows():
                profit = r['profit_amt']
                euc = r['effective_unit_cost']
                cost_src = r.get('cost_source', '?')
                dc = r['day_clear']

                # 构建结算公式字符串
                parts = []
                parts.append(f"sale={r['sale_amt']:.2f}")
                if abs(r['receive_amt']) > 0.01:
                    parts.append(f"-recv={r['receive_amt']:.2f}")
                if abs(r['bom_in_amt']) > 0.01:
                    parts.append(f"-bom_in={r['bom_in_amt']:.2f}")
                if abs(r['bom_out_amt']) > 0.01:
                    parts.append(f"+bom_out={r['bom_out_amt']:.2f}")
                if abs(r['compose_in_amt']) > 0.01:
                    parts.append(f"-comp_in={r['compose_in_amt']:.2f}")
                if abs(r['compose_out_amt']) > 0.01:
                    parts.append(f"+comp_out={r['compose_out_amt']:.2f}")
                if abs(r['end_stock_amt'] - r['init_stock_amt']) > 0.01:
                    parts.append(f"+Δstk={r['end_stock_amt']-r['init_stock_amt']:.2f}")
                if abs(r['lost_amt']) > 0.01:
                    parts.append(f"-lost={r['lost_amt']:.2f}")
                eq_str = " ".join(parts) if parts else "all_zero"

                # 流入流出量
                flows = []
                if r['receive_qty']:
                    flows.append(f"recv_q={r['receive_qty']:.2f}")
                if r['bom_in_qty']:
                    flows.append(f"bomI_q={r['bom_in_qty']:.2f}")
                if r['bom_out_qty']:
                    flows.append(f"bomO_q={r['bom_out_qty']:.2f}")
                if r['compose_in_qty']:
                    flows.append(f"cmpI_q={r['compose_in_qty']:.2f}")
                if r['compose_out_qty']:
                    flows.append(f"cmpO_q={r['compose_out_qty']:.2f}")
                if r['sale_qty']:
                    flows.append(f"sale_q={r['sale_qty']:.2f}")
                if r['lost_qty']:
                    flows.append(f"lost_q={r['lost_qty']:.2f}")
                if r['init_stock_qty']:
                    flows.append(f"init_q={r['init_stock_qty']:.2f}")
                if r['end_stock_qty']:
                    flows.append(f"end_q={r['end_stock_qty']:.2f}")
                flow_str = " ".join(flows) if flows else "no_flow"

                lines.append(
                    f"  {r['article_id']} dc={dc} profit={profit:>10.2f} "
                    f"euc={euc:>10.4f} src={cost_src:<18} | {eq_str} | {flow_str}"
                )

            for line in lines:
                self._log.info(line)

            self._log.info(
                f"[{cat}] 合计 profit={total_profit:.2f} "
                f"= sale({total_sale:.2f}) "
                f"- recv({total_receive:.2f}) "
                f"- bom_in({total_bom_in:.2f}) + bom_out({total_bom_out:.2f}) "
                f"- comp_in({total_compose_in:.2f}) + comp_out({total_compose_out:.2f}) "
                f"+ Δstk({total_end-total_init:.2f}) "
                f"- lost({total_lost:.2f})"
            )

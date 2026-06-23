"""
t_calc_stock — 库存与金额中枢 (v0.11 Python 重写，合并原 stock + amounts)

四流合一的库存方程:
  eq = init + receive + bom_in - bom_out + compose_in - compose_out - sale - know_lost

分支:
  is_counted (人工盘点)    → end=actual, unknow=eq-actual
  day_clear='0'           → 软日清
  eq < 0                  → end=0, unknow=-eq (负库存保护)
  know_lost_qty > 0       → end=eq, unknow=0
  默认                     → end=eq, unknow=0

金额统一用 euc:
  end_stock_amt / unknow_lost_amt / know_lost_amt = qty × euc

按天串行处理：今天 init = 昨天 end（跨日链）
"""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd
from ..connectors import DuckDBStore
from ..utils import get_logger


class StockCalculator:
    TARGET_TABLE = "t_calc_stock"

    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("StockCalculator")

    def run(self) -> None:
        self._log.info("calculating stock & amounts (v0.11 Python, four-flow, sequential days) ...")
        conn = self._duck._conn

        wide_df = conn.execute("""
            SELECT
                store_id, business_date, article_id, day_clear,
                self_receive_qty, self_receive_amt,
                sale_qty, sale_amt,
                know_lost_qty,
                init_stock_qty_src, init_stock_amt_src,
                avg_inbound_price,
                outstock_unit_price, original_outstock_qty,
                outstock_unit_price_notax,
                outstock_cost_price,
                return_unit_price_notax, return_stock_qty,
                order_unit_price, store_order_qty,
                order_qty_payean,
                scm_promotion_amt_total
            FROM t_atomic_wide
        """).df()

        if wide_df.empty:
            self._log.warning("t_atomic_wide is empty")
            return

        dates = sorted(wide_df['business_date'].unique())
        self._log.info(
            f"wide_df: {len(wide_df)} rows, stores={wide_df['store_id'].nunique()}, "
            f"dates={len(dates)} ({dates[0]} ~ {dates[-1]})"
        )

        # ── 建表（首次）─────────────────────────────────────────────
        try:
            conn.execute(f"SELECT 1 FROM {self.TARGET_TABLE} LIMIT 0")
        except (duckdb.CatalogException, Exception):
            conn.execute(f"""
                CREATE TABLE {self.TARGET_TABLE} (
                    store_id VARCHAR, business_date VARCHAR, article_id VARCHAR,
                    day_clear VARCHAR,
                    receive_qty DOUBLE, receive_amt DOUBLE,
                    bom_in_qty DOUBLE, bom_in_amt DOUBLE,
                    compose_in_qty DOUBLE, compose_in_amt DOUBLE,
                    bom_out_qty DOUBLE, bom_out_amt DOUBLE,
                    compose_out_qty DOUBLE, compose_out_amt DOUBLE,
                    sale_qty DOUBLE, sale_amt DOUBLE,
                    know_lost_qty DOUBLE, know_lost_amt DOUBLE,
                    unknow_lost_qty DOUBLE, unknow_lost_amt DOUBLE,
                    lost_qty DOUBLE, lost_amt DOUBLE,
                    init_stock_qty DOUBLE, init_stock_amt DOUBLE,
                    end_stock_qty DOUBLE, end_stock_amt DOUBLE,
                    actual_stock_qty DOUBLE,
                    stock_transfer_out_qty DOUBLE, stock_transfer_out_amt DOUBLE,
                    stock_transfer_in_qty DOUBLE, stock_transfer_in_amt DOUBLE,
                    effective_unit_cost DOUBLE, cost_source VARCHAR,
                    out_stock_pay_amt DOUBLE, out_stock_pay_amt_notax DOUBLE,
                    out_stock_amt_cb DOUBLE, return_stock_pay_amt_notax DOUBLE,
                    scm_promotion_amt_total DOUBLE,
                    expect_outstock_amt DOUBLE, purchase_weight DOUBLE
                )
            """)
            # Add columns not in initial schema
            try: conn.execute(f"ALTER TABLE {self.TARGET_TABLE} ADD COLUMN is_first_day INTEGER")
            except: pass
            try: conn.execute(f"ALTER TABLE {self.TARGET_TABLE} ADD COLUMN eq_end_qty DOUBLE")
            except: pass
            self._log.info(f"created {self.TARGET_TABLE}")

        # ── 清理本次日期范围旧数据 ────────────────────────────────
        try:
            conn.execute(f"DELETE FROM {self.TARGET_TABLE} WHERE business_date >= '{dates[0]}' AND business_date <= '{dates[-1]}'")
        except Exception:
            pass

        # ── 按天串行 ─────────────────────────────────────────────
        total_rows = 0
        for d in dates:
            df_day = wide_df[wide_df['business_date'] == d].copy()
            rows = self._process_day(df_day, d)
            total_rows += rows
        self._log.info(f"t_calc_stock total: {total_rows} rows across {len(dates)} days")

    # ═══════════════════════════════════════════════════════════════
    # 单日处理：加载辅助数据 + merge + 核心计算
    # ═══════════════════════════════════════════════════════════════
    def _process_day(self, df: pd.DataFrame, business_date: str) -> int:
        conn = self._duck._conn
        merge_keys = ['store_id', 'business_date', 'article_id']

        # ── 防御性去重：按 (store, date, article, dc) 取首行 ──
        dedup_keys = ['store_id', 'business_date', 'article_id', 'day_clear']
        before = len(df)
        df = df.drop_duplicates(subset=dedup_keys, keep='first')
        if len(df) < before:
            self._log.info(f"  dedup: {before} → {len(df)} rows ({before - len(df)} duplicates removed)")

        # ── euc + compose 修正金额 ──
        euc_df = conn.execute(f"""
            SELECT store_id, business_date, article_id,
                   effective_unit_cost, cost_source,
                   compose_in_amt, compose_out_amt,
                   compose_in_qty, compose_out_qty
            FROM t_calc_sku_cost
            WHERE business_date = '{business_date}'
        """).df()

        # ── BOM ──
        bom_in_df = conn.execute(f"""
            SELECT store_id, business_date, sub_article_id AS article_id,
                   SUM(bom_alloc_qty_sub) AS bom_in_qty,
                   SUM(bom_alloc_amt)      AS bom_in_amt
            FROM t_calc_bom_alloc
            WHERE business_date = '{business_date}'
            GROUP BY store_id, business_date, sub_article_id
        """).df()

        bom_out_df = conn.execute(f"""
            SELECT store_id, business_date, parent_article_id AS article_id,
                   SUM(bom_alloc_qty)     AS bom_out_qty,
                   SUM(bom_alloc_amt)     AS bom_out_amt
            FROM t_calc_bom_alloc
            WHERE business_date = '{business_date}'
            GROUP BY store_id, business_date, parent_article_id
        """).df()

        # ── 前一营业日期末库存（跨日链，MAX business_date < 当天）──
        prev_df = None
        try:
            prev_df = conn.execute(f"""
                WITH prev_date AS (
                    SELECT store_id, article_id, day_clear,
                           MAX(business_date) AS prev_biz_date
                    FROM {self.TARGET_TABLE}
                    WHERE business_date < '{business_date}'
                    GROUP BY store_id, article_id, day_clear
                )
                SELECT s.store_id, s.article_id, s.day_clear,
                       s.business_date,
                       s.end_stock_qty, s.end_stock_amt
                FROM {self.TARGET_TABLE} s
                INNER JOIN prev_date p
                    ON s.store_id = p.store_id
                    AND s.article_id = p.article_id
                    AND s.day_clear = p.day_clear
                    AND s.business_date = p.prev_biz_date
            """).df()
            if prev_df.empty:
                prev_df = None
        except (duckdb.CatalogException, Exception):
            pass

        if prev_df is not None:
            self._log.info(f"  {business_date}: 继承前一天 end_stock → init_stock ({len(prev_df)} SKU)")
        else:
            self._log.info(f"  {business_date}: 首日，从 purchase_di 源表取 init_stock")

        # ── 实盘库存 ──
        try:
            actual_df = conn.execute(f"""
                SELECT store_id, business_date, article_id,
                       actual_stock_qty, created_by
                FROM atomic_inventory_detail
                WHERE business_date = '{business_date}'
            """).df()
        except (duckdb.CatalogException, Exception):
            actual_df = pd.DataFrame()

        # ── merge BOM parents ──
        parent_ids = set()
        for _, r in bom_out_df.iterrows():
            if r['bom_out_qty'] != 0 or r['bom_out_amt'] != 0:
                parent_ids.add((r['store_id'], r['business_date'], r['article_id']))
        existing = set((r.store_id, r.business_date, r.article_id) for r in df.itertuples())
        missing = parent_ids - existing
        if missing:
            extras = []
            for sid, bd, aid in missing:
                extras.append({
                    'store_id': sid, 'business_date': bd,
                    'article_id': aid, 'day_clear': '1',
                    'self_receive_qty': 0, 'self_receive_amt': 0,
                    'compose_in_qty': 0, 'compose_out_qty': 0,
                    'sale_qty': 0, 'sale_amt': 0, 'know_lost_qty': 0,
                    'init_stock_qty_src': 0, 'init_stock_amt_src': 0,
                    'avg_inbound_price': 0,
                })
            df = pd.concat([df, pd.DataFrame(extras)], ignore_index=True)

        df = df.merge(euc_df, on=merge_keys, how='left')
        df['effective_unit_cost'] = df['effective_unit_cost'].fillna(0)
        df['cost_source'] = df['cost_source'].fillna('MISSING')

        df = df.merge(bom_in_df, on=merge_keys, how='left')
        df['bom_in_qty'] = df['bom_in_qty'].fillna(0)
        df['bom_in_amt'] = df['bom_in_amt'].fillna(0)

        df = df.merge(bom_out_df, on=merge_keys, how='left')
        df['bom_out_qty'] = df['bom_out_qty'].fillna(0)
        df['bom_out_amt'] = df['bom_out_amt'].fillna(0)

        if not actual_df.empty:
            df = df.merge(actual_df, on=merge_keys, how='left')
            df['actual_stock_qty'] = df['actual_stock_qty'].fillna(0)
            df['created_by'] = df['created_by'].fillna('系统')
            # 人工盘点（非系统创建）→ 信任实盘值
            # 系统快照 (created_by='系统') 不作为盘点依据:
            #   系统快照每日覆盖 ~1,400 SKU, 其 actual_stock_qty 与账面常有偏差,
            #   若作为盘点会每天产生大量虚假 unknow_lost (如 5/16 +4,854)
            df['is_counted'] = (df['created_by'] != '系统')
        else:
            df['actual_stock_qty'] = 0
            df['created_by'] = '系统'
            df['is_counted'] = False

        for c in ['self_receive_qty', 'self_receive_amt', 'compose_in_qty',
                   'compose_out_qty',
                   'sale_qty', 'sale_amt', 'know_lost_qty',
                   'outstock_unit_price', 'original_outstock_qty',
                   'outstock_unit_price_notax', 'outstock_cost_price',
                   'return_unit_price_notax', 'return_stock_qty',
                   'order_unit_price', 'store_order_qty',
                   'order_qty_payean', 'scm_promotion_amt_total']:
            if c in df.columns:
                df[c] = df[c].fillna(0)

        df['receive_qty'] = df['self_receive_qty']
        df['receive_amt'] = df['self_receive_amt']
        for col in ['stock_transfer_out_qty', 'stock_transfer_out_amt',
                     'stock_transfer_in_qty', 'stock_transfer_in_amt']:
            if col not in df.columns:
                df[col] = 0.0

        # compose 金额和数量来自 sku_cost 加工关系推算，不再回退源表
        df['compose_in_amt_corrected'] = df['compose_in_amt'].fillna(0)
        df['compose_out_amt_corrected'] = df['compose_out_amt'].fillna(0)
        # compose 数量也来自 sku_cost 推导（成品=sale+loss-init, 原料=配方反推）
        df['compose_in_qty'] = df['compose_in_qty'].fillna(0)
        df['compose_out_qty'] = df['compose_out_qty'].fillna(0)

        return self._process_day_core(df, prev_df, business_date)

    # ═══════════════════════════════════════════════════════════════
    # 核心计算：init_stock → 方程 → 分支 → 金额 → BOM转移 → 写出
    # ═══════════════════════════════════════════════════════════════
    def _process_day_core(self, df: pd.DataFrame, prev_df, business_date: str) -> int:
        conn = self._duck._conn

        # ── 跨日 init_stock ────────────────────────────────────────
        if prev_df is not None and not prev_df.empty:
            prev_df = prev_df.rename(columns={
                'end_stock_qty': 'prev_end_qty',
                'end_stock_amt': 'prev_end_amt',
            })
            # prev_df already has the correct prev_biz_date; set to current date for merge
            prev_df['business_date'] = business_date
            prev_df = prev_df[['store_id', 'article_id', 'day_clear',
                               'business_date', 'prev_end_qty', 'prev_end_amt']]
            df = df.merge(prev_df, on=['store_id', 'article_id', 'day_clear',
                                       'business_date'], how='left')
            df['init_stock_qty'] = df['prev_end_qty'].fillna(
                df['init_stock_qty_src']).clip(lower=0)
            df['init_stock_amt'] = df['prev_end_amt'].fillna(
                df['init_stock_amt_src']).clip(lower=0)
            df['is_first_day'] = df['prev_end_qty'].isna().astype(int)
        else:
            df['init_stock_qty'] = df['init_stock_qty_src'].clip(lower=0)
            df['init_stock_amt'] = df['init_stock_amt_src'].clip(lower=0)
            df['is_first_day'] = 1

        # ── compose 金额 ───────────────────────────────────────────
        # v0.10 compose correction: 优先用 sku_cost 修正值（加工关系推算），回退源表
        df['compose_in_amt'] = df['compose_in_amt_corrected'].fillna(0)
        df['compose_out_amt'] = df['compose_out_amt_corrected'].fillna(0)
        df['receive_amt'] = df['self_receive_amt']

        # ── 库存方程 ──────────────────────────────────────────────
        df['eq_end_qty'] = (
            df['init_stock_qty']
            + df['receive_qty']
            + df['bom_in_qty'] - df['bom_out_qty']
            + df['compose_in_qty'] - df['compose_out_qty']
            - df['sale_qty'] - df['know_lost_qty']
        )

        # ── 分支逻辑 ──────────────────────────────────────────────
        end_qty = np.zeros(len(df))
        unknow_qty = np.zeros(len(df))
        used_actual = 0

        for idx in df.index:
            eq = df.at[idx, 'eq_end_qty']
            dc = str(df.at[idx, 'day_clear'])
            kl_qty = df.at[idx, 'know_lost_qty']
            is_counted = df.at[idx, 'is_counted']
            act_qty = df.at[idx, 'actual_stock_qty']
            init_q = df.at[idx, 'init_stock_qty']
            recv_q = df.at[idx, 'receive_qty']
            bi_q = df.at[idx, 'bom_in_qty']
            bo_q = df.at[idx, 'bom_out_qty']
            ci_q = df.at[idx, 'compose_in_qty']
            co_q = df.at[idx, 'compose_out_qty']
            sale_q = df.at[idx, 'sale_qty']

            if is_counted:
                end_qty[idx] = act_qty
                unknow_qty[idx] = eq - act_qty  # 允许负值=盘盈
                used_actual += 1
            elif dc == '0':
                new_supply = recv_q + bi_q - bo_q + ci_q - co_q
                consumed_from_init = max(0.0, (sale_q + kl_qty) - new_supply)
                end_qty[idx] = max(0.0, init_q - consumed_from_init)
                unknow_qty[idx] = new_supply - sale_q - kl_qty  # 允许负值=盘盈
            elif eq < 0:
                end_qty[idx] = 0
                unknow_qty[idx] = -eq
            elif kl_qty > 0:
                end_qty[idx] = eq
                unknow_qty[idx] = 0
            elif act_qty > 0 and act_qty > eq + 0.001:
                # 系统记录的实盘数超过方程计算值 → 盘盈
                end_qty[idx] = act_qty
                unknow_qty[idx] = eq - act_qty  # 负值 = 盘盈
                used_actual += 1
            else:
                end_qty[idx] = eq
                unknow_qty[idx] = 0

        df['end_stock_qty'] = np.round(end_qty, 6)
        df['unknow_lost_qty'] = np.round(unknow_qty, 6)

        self._log.info(
            f"  {business_date}: rows={len(df)}, actual_stock={used_actual}, "
            f"day_clear_0={int((df['day_clear']=='0').sum())}, "
            f"has_know_lost={int((df['know_lost_qty']>0).sum())}, "
            f"neg_eq→unknow={int(((df['eq_end_qty'] < 0) & (df['unknow_lost_qty'] > 0)).sum())}"
        )

        # ── 金额统一 euc ──────────────────────────────────────────
        euc = df['effective_unit_cost'].values
        df['end_stock_amt'] = df['end_stock_qty'] * euc
        df['unknow_lost_amt'] = df['unknow_lost_qty'] * euc
        df['know_lost_amt'] = df['know_lost_qty'] * euc
        df['lost_qty'] = df['know_lost_qty'] + df['unknow_lost_qty']
        df['lost_amt'] = df['know_lost_amt'] + df['unknow_lost_amt']

        # ── SCM 金融 ──────────────────────────────────────────────
        df['out_stock_pay_amt'] = (
            df['outstock_unit_price'].fillna(0) * df['original_outstock_qty'].fillna(0))
        df['out_stock_pay_amt_notax'] = (
            df['outstock_unit_price_notax'].fillna(0) * df['original_outstock_qty'].fillna(0))
        df['out_stock_amt_cb'] = (
            df['outstock_cost_price'].fillna(0) * df['original_outstock_qty'].fillna(0))
        df['return_stock_pay_amt_notax'] = (
            df['return_unit_price_notax'].fillna(0) * df['return_stock_qty'].fillna(0))
        df['expect_outstock_amt'] = (
            df['order_unit_price'].fillna(0) * df['store_order_qty'].fillna(0))
        df['purchase_weight'] = (
            df['order_qty_payean'].fillna(0) * df['outstock_unit_price'].fillna(0))

        # ── BOM 父品库存转移 ──────────────────────────────────────
        try:
            bom_alloc = conn.execute("""
                SELECT store_id, business_date, parent_article_id AS article_id,
                       sub_article_id, bom_alloc_amt
                FROM t_calc_bom_alloc
            """).df()
            if not bom_alloc.empty:
                parent_mask = (
                    (df['end_stock_qty'] > 0.001) &
                    ((df['bom_out_qty'] > 0.001) | (df['bom_out_amt'] > 0.01)) &
                    (df['sale_qty'] < 0.001)
                )
                transfer_parents = df[parent_mask].copy()
                if not transfer_parents.empty:
                    for _, pr in transfer_parents.iterrows():
                        sub_alloc = bom_alloc[
                            (bom_alloc['store_id'] == pr['store_id']) &
                            (bom_alloc['business_date'] == pr['business_date']) &
                            (bom_alloc['article_id'] == pr['article_id'])
                        ]
                        if sub_alloc.empty:
                            continue
                        total_alloc = sub_alloc['bom_alloc_amt'].sum()
                        if total_alloc < 0.01:
                            continue
                        transfer_qty = float(pr['end_stock_qty'])
                        transfer_amt = float(pr['end_stock_amt'])
                        df.loc[parent_mask & (df['article_id'] == pr['article_id']) &
                               (df['store_id'] == pr['store_id']) &
                               (df['business_date'] == pr['business_date']),
                               'stock_transfer_out_qty'] = transfer_qty
                        df.loc[parent_mask & (df['article_id'] == pr['article_id']) &
                               (df['store_id'] == pr['store_id']) &
                               (df['business_date'] == pr['business_date']),
                               'stock_transfer_out_amt'] = transfer_amt
                        df.loc[parent_mask & (df['article_id'] == pr['article_id']) &
                               (df['store_id'] == pr['store_id']) &
                               (df['business_date'] == pr['business_date']),
                               'end_stock_qty'] = 0.0
                        df.loc[parent_mask & (df['article_id'] == pr['article_id']) &
                               (df['store_id'] == pr['store_id']) &
                               (df['business_date'] == pr['business_date']),
                               'end_stock_amt'] = 0.0

                        # v0.11 fix: stock_transfer 清零父品 end_stock 时同步增加 bom_out
                        # 父品 profit = -receive + bom_out + end(0) - init
                        # 若 bom_out≈receive: profit ≈ -init → 虚亏
                        # 增加 bom_out += transfer(≈init) → profit ≈ 0
                        df.loc[parent_mask & (df['article_id'] == pr['article_id']) &
                               (df['store_id'] == pr['store_id']) &
                               (df['business_date'] == pr['business_date']),
                               'bom_out_qty'] += transfer_qty
                        df.loc[parent_mask & (df['article_id'] == pr['article_id']) &
                               (df['store_id'] == pr['store_id']) &
                               (df['business_date'] == pr['business_date']),
                               'bom_out_amt'] += transfer_amt

                        for _, sa in sub_alloc.iterrows():
                            sub_mask = (
                                (df['store_id'] == sa['store_id']) &
                                (df['business_date'] == sa['business_date']) &
                                (df['article_id'] == sa['sub_article_id'])
                            )
                            ratio = sa['bom_alloc_amt'] / total_alloc
                            if sub_mask.any():
                                df.loc[sub_mask, 'stock_transfer_in_qty'] += transfer_qty * ratio
                                df.loc[sub_mask, 'stock_transfer_in_amt'] += transfer_amt * ratio
                                df.loc[sub_mask, 'end_stock_qty'] += transfer_qty * ratio
                                df.loc[sub_mask, 'end_stock_amt'] += transfer_amt * ratio
                                # 同步更新子品 bom_in_amt/qty，抵消 end 增加的 profit 影响
                                df.loc[sub_mask, 'bom_in_amt'] += transfer_amt * ratio
                                df.loc[sub_mask, 'bom_in_qty'] += transfer_qty * ratio
        except (duckdb.CatalogException, Exception):
            pass

        # ── 写出（DELETE + INSERT，保留历史天数据）─────────────
        out_cols = {
            'store_id': df['store_id'],
            'business_date': df['business_date'],
            'article_id': df['article_id'],
            'day_clear': df['day_clear'],
            'receive_qty': df['receive_qty'],
            'receive_amt': df['receive_amt'],
            'bom_in_qty': df['bom_in_qty'],
            'bom_in_amt': df['bom_in_amt'],
            'compose_in_qty': df['compose_in_qty'],
            'compose_in_amt': df['compose_in_amt'],
            'bom_out_qty': df['bom_out_qty'],
            'bom_out_amt': df['bom_out_amt'],
            'compose_out_qty': df['compose_out_qty'],
            'compose_out_amt': df['compose_out_amt'],
            'sale_qty': df['sale_qty'],
            'sale_amt': df['sale_amt'],
            'know_lost_qty': df['know_lost_qty'],
            'know_lost_amt': df['know_lost_amt'],
            'unknow_lost_qty': df['unknow_lost_qty'],
            'unknow_lost_amt': df['unknow_lost_amt'],
            'lost_qty': df['lost_qty'],
            'lost_amt': df['lost_amt'],
            'init_stock_qty': df['init_stock_qty'],
            'init_stock_amt': df['init_stock_amt'],
            'end_stock_qty': df['end_stock_qty'],
            'end_stock_amt': df['end_stock_amt'],
            'actual_stock_qty': df['actual_stock_qty'],
            'stock_transfer_out_qty': df['stock_transfer_out_qty'],
            'stock_transfer_out_amt': df['stock_transfer_out_amt'],
            'stock_transfer_in_qty': df['stock_transfer_in_qty'],
            'stock_transfer_in_amt': df['stock_transfer_in_amt'],
            'effective_unit_cost': df['effective_unit_cost'],
            'cost_source': df['cost_source'],
            'out_stock_pay_amt': df['out_stock_pay_amt'],
            'out_stock_pay_amt_notax': df['out_stock_pay_amt_notax'],
            'out_stock_amt_cb': df['out_stock_amt_cb'],
            'return_stock_pay_amt_notax': df['return_stock_pay_amt_notax'],
            'scm_promotion_amt_total': df['scm_promotion_amt_total'],
            'expect_outstock_amt': df['expect_outstock_amt'],
            'purchase_weight': df['purchase_weight'],
            'is_first_day': df['is_first_day'],
            'eq_end_qty': df['eq_end_qty'],
        }
        out_df = pd.DataFrame(out_cols)

        # 幂等写入：先删当天，再插入
        conn.execute(f"""
            DELETE FROM {self.TARGET_TABLE}
            WHERE business_date = '{business_date}'
        """)
        conn.execute(f"INSERT INTO {self.TARGET_TABLE} SELECT * FROM out_df")

        rows = len(out_df)
        neg_end = int((df['end_stock_qty'] < 0).sum())
        if neg_end > 0:
            self._log.warning(f"NEGATIVE end_stock_qty: {neg_end} rows remain!")

        # ── 详细日志 ──────────────────────────────────────────────
        self._log_detail(df)

        return rows

    # ═══════════════════════════════════════════════════════════════
    # 详细日志
    # ═══════════════════════════════════════════════════════════════
    def _log_detail(self, df: pd.DataFrame) -> None:
        # v0.10 fix: dim_goods 分类关联延迟到 FM 底表层，计算层不再引用
        df['cat'] = '?'

        bom_parents = df[df['bom_out_amt'].abs() > 0.01]
        neg_eq = df[(df['eq_end_qty'] < 0) & (df['unknow_lost_qty'] > 0.01)]

        if bom_parents.empty and neg_eq.empty:
            return

        self._log.info("─── BOM父品库存方程明细 ───")
        for _, r in bom_parents.sort_values('bom_out_amt').iterrows():
            eq = (f"init({r['init_stock_qty']:.2f})"
                  f"+recv({r['receive_qty']:.2f})"
                  f"+bomI({r['bom_in_qty']:.2f})"
                  f"-bomO({r['bom_out_qty']:.2f})"
                  f"+cmpI({r['compose_in_qty']:.2f})"
                  f"-cmpO({r['compose_out_qty']:.2f})"
                  f"-sale({r['sale_qty']:.2f})"
                  f"-klost({r['know_lost_qty']:.2f})")
            result = (f"eq={r['eq_end_qty']:.2f} "
                      f"end={r['end_stock_qty']:.2f}/{r['end_stock_amt']:.2f} "
                      f"unknow={r['unknow_lost_qty']:.2f}/{r['unknow_lost_amt']:.2f}")
            self._log.info(
                f"  [{r['cat']}] {r['article_id']} dc={r['day_clear']} "
                f"euc={r['effective_unit_cost']:.4f} "
                f"recv={r['receive_qty']:.2f}/{r['receive_amt']:.2f} "
                f"bomO={r['bom_out_qty']:.2f}/{r['bom_out_amt']:.2f} "
                f"| {eq} | {result}"
            )

        if not neg_eq.empty:
            self._log.info(f"─── eq<0 转 unknow_lost 行 (共{len(neg_eq)}行) ───")
            for _, r in neg_eq.sort_values('unknow_lost_amt').head(20).iterrows():
                self._log.info(
                    f"  [{r['cat']}] {r['article_id']} dc={r['day_clear']} "
                    f"euc={r['effective_unit_cost']:.4f} "
                    f"eq={r['eq_end_qty']:.2f} "
                    f"→ unknow={r['unknow_lost_qty']:.2f}/{r['unknow_lost_amt']:.2f}"
                )

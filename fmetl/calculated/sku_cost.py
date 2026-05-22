"""
t_calc_sku_cost — SKU 有效单位成本 (v10 Python 重写)

v10 算法（加权平均，含期初库存）:
  cost_amt = init_stock_amt + self_receive_amt + compose_net_amt + bom_alloc_amt
  cost_qty = init_stock_qty + self_receive_qty + compose_net_qty + bom_alloc_qty
  euc = cost_amt / cost_qty

  - init_stock: 昨天 t_calc_stock.end_stock（首日: atomic_inventory 源表值）
  - compose: qty × avg_inbound_price
  - 不用 cost_price
"""

from __future__ import annotations

import duckdb
from ..connectors import DuckDBStore
from ..utils import get_logger


class SkuCostCalculator:
    TARGET_TABLE = "t_calc_sku_cost"

    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("SkuCostCalculator")

    def run(self) -> None:
        self._log.info("calculating SKU effective unit cost (v10 Python) ...")
        conn = self._duck._conn

        # 1. 从 t_atomic_wide 取基础数据
        wide_df = conn.execute("""
            SELECT
                store_id,
                business_date,
                article_id,
                self_receive_qty,
                self_receive_amt,
                compose_in_qty,
                compose_out_qty,
                compose_in_amt_src,
                compose_out_amt_src,
                init_stock_qty_src,
                init_stock_amt_src,
                avg_inbound_price
            FROM t_atomic_wide
        """).df()

        if wide_df.empty:
            self._log.warning("t_atomic_wide is empty, creating empty t_calc_sku_cost")
            self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
            self._duck.execute(f"""
                CREATE TABLE {self.TARGET_TABLE} (
                    store_id VARCHAR, business_date VARCHAR, article_id VARCHAR,
                    total_cost_amt DOUBLE, cost_qty DOUBLE,
                    effective_unit_cost DOUBLE, cost_source VARCHAR,
                    self_inbound_qty DOUBLE, self_inbound_amt DOUBLE,
                    compose_net_qty DOUBLE, compose_net_amt DOUBLE,
                    bom_alloc_amt DOUBLE, bom_alloc_qty DOUBLE,
                    init_stock_qty DOUBLE, init_stock_amt DOUBLE,
                    avg_inbound_price DOUBLE, is_first_day INTEGER
                )
            """)
            return

        # 2. 从 t_calc_bom_alloc 取 BOM 分摊（按 sub 聚合, 用子品单位 qty）
        bom_df = conn.execute("""
            SELECT
                store_id,
                business_date,
                sub_article_id     AS article_id,
                SUM(bom_alloc_amt) AS bom_alloc_amt,
                SUM(bom_alloc_qty_sub) AS bom_alloc_qty
            FROM t_calc_bom_alloc
            GROUP BY store_id, business_date, sub_article_id
        """).df()

        # 3. 从 t_calc_stock 取前一营业日期末库存（MAX business_date < 当天）
        prev_df = None
        try:
            prev_df = conn.execute("""
                WITH stock_pairs AS (
                    SELECT DISTINCT store_id, article_id, business_date
                    FROM t_atomic_wide
                ),
                prev_match AS (
                    SELECT sp.store_id, sp.article_id, sp.business_date,
                           MAX(cs.business_date) AS prev_biz_date
                    FROM stock_pairs sp
                    INNER JOIN t_calc_stock cs
                        ON sp.store_id = cs.store_id
                        AND sp.article_id = cs.article_id
                        AND cs.business_date < sp.business_date
                    GROUP BY sp.store_id, sp.article_id, sp.business_date
                ),
                prev_stock AS (
                    SELECT pm.store_id, pm.article_id, pm.business_date,
                           cs.end_stock_qty AS prev_end_qty,
                           cs.end_stock_amt AS prev_end_amt
                    FROM prev_match pm
                    INNER JOIN t_calc_stock cs
                        ON pm.store_id = cs.store_id
                        AND pm.article_id = cs.article_id
                        AND pm.prev_biz_date = cs.business_date
                )
                SELECT * FROM prev_stock
            """).df()
            if prev_df.empty:
                prev_df = None
        except (duckdb.CatalogException, Exception):
            self._log.info("no t_calc_stock from prior runs — all SKUs are first-day")

        # 4. Python: merge BOM
        df = wide_df.merge(bom_df, on=['store_id', 'business_date', 'article_id'],
                           how='left')
        df['bom_alloc_qty'] = df['bom_alloc_qty'].fillna(0)
        df['bom_alloc_amt'] = df['bom_alloc_amt'].fillna(0)

        # 5. Python: 计算 init_stock（前一营业日期末 或 首日源表值）
        if prev_df is not None and not prev_df.empty:
            df = df.merge(prev_df,
                          on=['store_id', 'article_id', 'business_date'],
                          how='left')
            df['init_stock_qty'] = df['prev_end_qty'].fillna(
                df['init_stock_qty_src']).clip(lower=0)
            df['init_stock_amt'] = df['prev_end_amt'].fillna(
                df['init_stock_amt_src']).clip(lower=0)
            df['is_first_day'] = df['prev_end_qty'].isna().astype(int)
        else:
            df['init_stock_qty'] = df['init_stock_qty_src'].clip(lower=0)
            df['init_stock_amt'] = df['init_stock_amt_src'].clip(lower=0)
            df['is_first_day'] = 1

        # 6. Python: compose 净额（源表amt, 无则回退到 avg_inbound_price）
        df['compose_net_qty'] = df['compose_in_qty'] - df['compose_out_qty']
        df['compose_net_amt'] = (
            df['compose_in_amt_src'].fillna(0) - df['compose_out_amt_src'].fillna(0))
        # 源表无值则回退
        fallback = (df['compose_net_amt'].abs() < 0.001) & (df['compose_net_qty'].abs() > 0.001)
        df.loc[fallback, 'compose_net_amt'] = (
            df.loc[fallback, 'compose_net_qty'] * df.loc[fallback, 'avg_inbound_price'])

        # 7. Python: 加权平均成本
        df['cost_amt'] = (df['init_stock_amt'] + df['self_receive_amt']
                          + df['compose_net_amt'] + df['bom_alloc_amt'])
        df['cost_qty'] = (df['init_stock_qty'] + df['self_receive_qty']
                          + df['compose_net_qty'] + df['bom_alloc_qty'])

        df['effective_unit_cost'] = 0.0
        mask = df['cost_qty'] > 0
        df.loc[mask, 'effective_unit_cost'] = (
            df.loc[mask, 'cost_amt'] / df.loc[mask, 'cost_qty']
        )
        # cost_qty=0时回退到avg_inbound_price(无任何来源的SKU)
        fallback = (~mask) & (df['avg_inbound_price'] > 0)
        df.loc[fallback, 'effective_unit_cost'] = df.loc[fallback, 'avg_inbound_price']
        df['cost_source'] = 'V10_WEIGHTED_AVG'
        df.loc[fallback, 'cost_source'] = 'V10_AVG_INBOUND_FALLBACK'

        # 8. 写出结果
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
        conn.execute(f"""
            CREATE TABLE {self.TARGET_TABLE} AS
            SELECT
                store_id,
                business_date,
                article_id,
                cost_amt                AS total_cost_amt,
                cost_qty,
                effective_unit_cost,
                cost_source,
                self_receive_qty        AS self_inbound_qty,
                self_receive_amt        AS self_inbound_amt,
                compose_net_qty,
                compose_net_amt,
                bom_alloc_amt,
                bom_alloc_qty,
                init_stock_qty,
                init_stock_amt,
                avg_inbound_price,
                is_first_day
            FROM df
        """)

        rows = self._duck.row_count(self.TARGET_TABLE)
        first_day_cnt = int(df['is_first_day'].sum())
        self._log.info(f"t_calc_sku_cost: {rows} rows, first_day={first_day_cnt}")

        # 统计 euc 分布
        pos = df[df['effective_unit_cost'] > 0]
        if len(pos) > 0:
            self._log.info(
                f"euc stats: n={len(pos)}, "
                f"min={pos['effective_unit_cost'].min():.4f}, "
                f"median={pos['effective_unit_cost'].median():.4f}, "
                f"max={pos['effective_unit_cost'].max():.4f}"
            )

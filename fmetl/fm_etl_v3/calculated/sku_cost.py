"""
t_calc_sku_cost — SKU 级有效单位成本（v9 加权平均，含期初库存）

目标:
    把每个销售 SKU 当天的"有效单位成本"固定到一行：
        粒度 = (store_id, business_date, article_id)

v9算法（加权平均，含期初库存）:
    成本额 = 期初库存额 + 自己进货额 + compose净额 + BOM分配额
    成本量 = 期初库存量 + 自己进货量 + compose净量 + BOM分配量

    effective_unit_cost = 成本额 / 成本量

    其中期初库存 = 昨天 t_calc_stock.end_stock (跨日链式传递)
    首日回退: atomic_inventory.init_stock_qty × avg_inbound_price

数据源:
    ① 期初库存: t_calc_stock (昨天) → end_stock_qty, end_stock_amt
    ② 自己进货: atomic_receive_sale (article_id == sale_article_id)
    ③ compose: atomic_compose + atomic_inventory.avg_inbound_price
    ④ BOM分配: t_calc_bom_alloc (split_need_qty, bom_alloc_amt)

注意:
    - 不使用 cost_price，标品也用并行叠加结果
    - compose成本用 avg_inbound_price，不用 effective_unit_cost
    - bom_alloc_qty 改用 split_need_qty (Phase 1 新增列)
"""

from __future__ import annotations

from ..connectors import DuckDBStore
from ..utils import get_logger


class SkuCostCalculator:
    TARGET_TABLE = "t_calc_sku_cost"

    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("SkuCostCalculator")

    def run(self) -> None:
        self._log.info("calculating SKU effective unit cost (v9 加权平均含期初库存) ...")
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
        self._duck.execute(f"""
        CREATE TABLE {self.TARGET_TABLE} AS
        WITH
        -- ── ① 自己进货数据（article_id == sale_article_id）──────────────────
        self_inbound AS (
            SELECT
                store_id,
                business_date,
                article_id                          AS article_id,
                SUM(inbound_qty)                    AS self_inbound_qty,
                SUM(inbound_amount)                 AS self_inbound_amt
            FROM atomic_receive_sale
            WHERE article_id = sale_article_id
            GROUP BY store_id, business_date, article_id
        ),

        -- ── ② compose数据（加工转换）──────────────────────────────────────
        compose_data AS (
            SELECT
                store_id,
                business_date,
                article_id,
                SUM(COALESCE(compose_in_qty, 0))    AS compose_in_qty,
                SUM(COALESCE(compose_out_qty, 0))   AS compose_out_qty
            FROM atomic_compose
            GROUP BY store_id, business_date, article_id
        ),

        -- ── ③ avg_inbound_price（compose成本单价）─────────────────────────
        avg_price AS (
            SELECT
                store_id,
                business_date,
                article_id,
                AVG(avg_inbound_price)              AS avg_inbound_price
            FROM atomic_inventory
            GROUP BY store_id, business_date, article_id
        ),

        -- ── ④ compose金额（用 avg_inbound_price）───────────────────────────
        compose_amt AS (
            SELECT
                cd.store_id,
                cd.business_date,
                cd.article_id,
                cd.compose_in_qty,
                cd.compose_out_qty,
                cd.compose_in_qty - cd.compose_out_qty  AS compose_net_qty,
                cd.compose_in_qty * COALESCE(ap.avg_inbound_price, 0) AS compose_in_amt,
                cd.compose_out_qty * COALESCE(ap.avg_inbound_price, 0) AS compose_out_amt,
                (cd.compose_in_qty - cd.compose_out_qty) * COALESCE(ap.avg_inbound_price, 0) AS compose_net_amt
            FROM compose_data cd
            LEFT JOIN avg_price ap
              ON ap.store_id = cd.store_id
             AND ap.business_date = cd.business_date
             AND ap.article_id = cd.article_id
        ),

        -- ── ⑤ BOM分配数据（从 t_calc_bom_alloc 合并到 sub）─────────────────
        -- v9: bom_alloc_qty 改用 split_need_qty (Phase 1 新增列)
        bom_alloc AS (
            SELECT
                store_id,
                business_date,
                sub_article_id                     AS article_id,
                SUM(bom_alloc_amt)                 AS bom_alloc_amt,
                SUM(split_need_qty)                AS bom_alloc_qty,
                SUM(self_inbound_qty)              AS bom_self_inbound_qty,
                SUM(self_inbound_amt)              AS bom_self_inbound_amt,
                MAX(is_type_a)                     AS is_type_a
            FROM t_calc_bom_alloc
            GROUP BY store_id, business_date, sub_article_id
        ),

        -- ── ⑥ 昨天期末库存（今天期初）────────────────────────────────────
        prev_end AS (
            SELECT
                store_id,
                article_id,
                day_clear,
                business_date,
                end_stock_qty,
                end_stock_amt_self                  AS end_stock_amt
            FROM t_calc_stock
        ),

        -- ── ⑦ 以 atomic_inventory 为 SKU 存在性主锚 ───────────────────────
        base AS (
            SELECT
                inv.store_id,
                inv.business_date,
                inv.article_id,
                COALESCE(si.self_inbound_qty, 0)   AS self_inbound_qty,
                COALESCE(si.self_inbound_amt, 0)   AS self_inbound_amt,
                COALESCE(ca.compose_net_qty, 0)    AS compose_net_qty,
                COALESCE(ca.compose_net_amt, 0)    AS compose_net_amt,
                COALESCE(ba.bom_alloc_amt, 0)      AS bom_alloc_amt,
                COALESCE(ba.bom_alloc_qty, 0)      AS bom_alloc_qty,
                -- 首日 fallback 所需原始字段
                inv.init_stock_qty,
                inv.avg_inbound_price,
                -- 昨天期末 = 今天期初（跨日链式传递）
                -- 首日 fallback: 用量和额都回退到源表值
                COALESCE(prev.end_stock_qty,
                         inv.init_stock_qty)            AS prev_end_stock_qty,
                COALESCE(prev.end_stock_amt,
                         inv.init_stock_qty * inv.avg_inbound_price) AS prev_end_stock_amt,
                -- 是否首日 (prev_end_stock_qty 为 NULL 即无昨天数据)
                CASE WHEN prev.end_stock_qty IS NULL THEN 1 ELSE 0 END AS is_first_day
            FROM atomic_inventory inv
            LEFT JOIN self_inbound si
              ON si.store_id = inv.store_id
             AND si.business_date = inv.business_date
             AND si.article_id = inv.article_id
            LEFT JOIN compose_amt ca
              ON ca.store_id = inv.store_id
             AND ca.business_date = inv.business_date
             AND ca.article_id = inv.article_id
            LEFT JOIN bom_alloc ba
              ON ba.store_id = inv.store_id
             AND ba.business_date = inv.business_date
             AND ba.article_id = inv.article_id
            LEFT JOIN prev_end prev
              ON prev.store_id = inv.store_id
             AND prev.article_id = inv.article_id
             AND prev.day_clear = '1'
             AND prev.business_date = CAST(
                 strftime(CAST(inv.business_date AS DATE) - INTERVAL 1 DAY,
                 '%%Y-%%m-%%d') AS VARCHAR
             )
        )

        -- ── ⑧ 最终输出：v9 加权平均算法 ────────────────────────────────
        SELECT
            b.store_id,
            b.business_date,
            b.article_id,
            -- 成本额 = 期初库存额 + 自己进货额 + compose净额 + BOM分配额
            b.prev_end_stock_amt
                + b.self_inbound_amt
                + b.compose_net_amt
                + b.bom_alloc_amt                           AS total_cost_amt,
            -- 成本量 = 期初库存量 + 自己进货量 + compose净量 + BOM分配量
            b.prev_end_stock_qty
                + b.self_inbound_qty
                + b.compose_net_qty
                + b.bom_alloc_qty                           AS cost_qty,
            -- v9: effective_unit_cost = 成本额 / 成本量
            CASE
                WHEN (b.prev_end_stock_qty
                      + b.self_inbound_qty
                      + b.compose_net_qty
                      + b.bom_alloc_qty) > 0
                THEN (b.prev_end_stock_amt
                      + b.self_inbound_amt
                      + b.compose_net_amt
                      + b.bom_alloc_amt)
                     / (b.prev_end_stock_qty
                        + b.self_inbound_qty
                        + b.compose_net_qty
                        + b.bom_alloc_qty)
                ELSE 0
            END                                             AS effective_unit_cost,
            -- 成本来源标识
            'V9_WEIGHTED_AVG'                               AS cost_source,
            -- 各来源明细
            b.self_inbound_qty,
            b.self_inbound_amt,
            b.compose_net_qty,
            b.compose_net_amt,
            b.bom_alloc_amt,
            b.bom_alloc_qty,
            b.prev_end_stock_qty                            AS init_stock_qty,
            b.prev_end_stock_amt                            AS init_stock_amt,
            -- compose单价
            COALESCE(ap.avg_inbound_price, 0)               AS avg_inbound_price,
            -- 是否首日
            b.is_first_day
        FROM base b
        LEFT JOIN avg_price ap
          ON ap.store_id = b.store_id
         AND ap.business_date = b.business_date
         AND ap.article_id = b.article_id
        """)
        rows = self._duck.row_count(self.TARGET_TABLE)
        self._log.info(f"t_calc_sku_cost: {rows} rows")

        # Phase 7: 关键节点 log — effective_unit_cost 分布
        stats = self._duck._conn.execute(f"""
            SELECT
                COUNT(*)                                          AS cnt,
                ROUND(MIN(effective_unit_cost), 4)                AS min_cost,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
                    (ORDER BY effective_unit_cost), 4)            AS p50_cost,
                ROUND(MAX(effective_unit_cost), 4)                AS max_cost,
                SUM(CASE WHEN is_first_day = 1 THEN 1 ELSE 0 END) AS first_day_cnt
            FROM {self.TARGET_TABLE}
            WHERE effective_unit_cost > 0
        """).fetchone()
        if stats and stats[0] > 0:
            self._log.info(
                f"effective_unit_cost distribution: "
                f"n={stats[0]}, min={stats[1]}, p50={stats[2]}, max={stats[3]}"
            )
            if stats[4] > 0:
                self._log.info(f"first-day fallback triggered: {stats[4]} SKUs")

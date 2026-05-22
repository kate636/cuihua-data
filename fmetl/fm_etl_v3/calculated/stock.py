"""
t_calc_stock — 库存与未知损耗推算（v9 库存滚动）

核心修正:
    1. 库存滚动: 今日init_stock = 昨日end_stock
    2. 首日处理: 无昨日数据时，使用 atomic_inventory.init_stock_qty

粒度: (store_id, business_date, article_id, day_clear)

三支分支:
    ⚠️ 语义：day_clear='0' = 日清；'1' = 非日清；'2' = 合计

    · day_clear = '0'  （日清商品）
        end_stock_qty  = 0
        unknow_lost_qty = init + receive + compose_in - compose_out - sale - know_lost

    · know_lost_amt > 0 （视为当天有盘点动作）
        end_stock_qty  = 盘点值（如有外部盘点数据）
        unknow_lost_qty = 方程反推

    · 否则（非日清 day_clear='1' 且无盘点）
        end_stock_qty  = 方程计算
        unknow_lost_qty = 0

库存滚动逻辑:
    今日init_stock_qty = 昨日end_stock_qty
    今日init_stock_amt = 昨日end_stock_amt
"""

from __future__ import annotations

from ..connectors import DuckDBStore
from ..utils import get_logger


class StockCalculator:
    TARGET_TABLE = "t_calc_stock"

    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("StockCalculator")

    def run(self) -> None:
        self._log.info("calculating stock & unknown loss (v9 库存滚动) ...")
        self._duck.execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")
        self._duck.execute(f"""
        CREATE TABLE {self.TARGET_TABLE} AS
        WITH
        -- ── ① 基础数据（从 t_atomic_wide）───────────────────────────────
        base AS (
            SELECT
                w.store_id,
                w.business_date,
                w.article_id,
                w.day_clear,
                -- 原始init（源表值，仅首日使用）
                w.init_stock_qty                              AS init_stock_qty_src,
                w.init_stock_amt_src                          AS init_stock_amt_src,
                -- 其他基础数据
                w.end_stock_qty_raw                           AS end_stock_qty_official,
                w.end_stock_amt_src                           AS end_stock_amt_official,
                w.receive_qty,
                w.receive_amt_src                             AS receive_amt_official,
                w.compose_in_qty,
                w.compose_out_qty,
                w.sale_qty,
                w.know_lost_qty,
                w.know_lost_amt_src                           AS know_lost_amt,
                w.unknow_lost_amt_src                         AS unknow_lost_amt_src,
                c.effective_unit_cost,
                c.cost_source
            FROM t_atomic_wide w
            LEFT JOIN t_calc_sku_cost c
              ON c.store_id = w.store_id
             AND c.business_date = w.business_date
             AND c.article_id = w.article_id
        ),

        -- ── ② 按日期排序，用 LAG 获取昨日 end_stock ───────────────────
        -- 需要先按 store_id, article_id, day_clear 分组并按 business_date 排序
        ordered AS (
            SELECT
                b.*,
                -- 昨日期末库存（LAG）
                LAG(b.end_stock_qty_official) OVER (
                    PARTITION BY b.store_id, b.article_id, b.day_clear
                    ORDER BY b.business_date
                )                                         AS prev_end_stock_qty,
                LAG(b.end_stock_amt_official) OVER (
                    PARTITION BY b.store_id, b.article_id, b.day_clear
                    ORDER BY b.business_date
                )                                         AS prev_end_stock_amt,
                -- 判断是否首日（无昨日数据）
                ROW_NUMBER() OVER (
                    PARTITION BY b.store_id, b.article_id, b.day_clear
                    ORDER BY b.business_date
                )                                         AS day_rank
            FROM base b
        ),

        -- ── ③ 库存滚动：今日init = 昨日end ────────────────────────────
        rolled AS (
            SELECT
                o.*,
                -- 今日期初库存量：昨日期末 或 源表初始值（首日）
                CASE
                    WHEN o.day_rank = 1 OR o.prev_end_stock_qty IS NULL
                        THEN COALESCE(o.init_stock_qty_src, 0)
                    ELSE o.prev_end_stock_qty
                END                                         AS init_stock_qty,
                -- 今日期初库存额：昨日期末 或 计算（首日）
                CASE
                    WHEN o.day_rank = 1 OR o.prev_end_stock_amt IS NULL
                        THEN COALESCE(o.init_stock_qty_src, 0) * o.effective_unit_cost
                    ELSE o.prev_end_stock_amt
                END                                         AS init_stock_amt
            FROM ordered o
        ),

        -- ── ④ 库存方程计算 ────────────────────────────────────────────
        equation AS (
            SELECT
                r.*,
                -- 方程推算期末库存量
                (r.init_stock_qty + r.receive_qty + r.compose_in_qty
                 - r.compose_out_qty - r.sale_qty - r.know_lost_qty) AS eq_end_qty
            FROM rolled r
        )

        -- ── ⑤ 最终输出 ──────────────────────────────────────────────
        SELECT
            store_id,
            business_date,
            article_id,
            day_clear,
            -- 期初库存（滚动后的值）
            init_stock_qty,
            init_stock_amt,
            init_stock_amt_src,
            receive_qty,
            receive_amt_official,
            compose_in_qty,
            compose_out_qty,
            sale_qty,
            know_lost_qty,
            know_lost_amt,
            effective_unit_cost,
            cost_source,

            -- ── end_stock_qty 三支（'0'=日清 '1'=非日清 '2'=合计）──
            CASE
                WHEN day_clear = '0' THEN 0
                ELSE eq_end_qty
            END                                         AS end_stock_qty,

            -- ── unknow_lost_qty 三支 ─────────────────────────────────
            CASE
                WHEN day_clear = '0'
                    THEN eq_end_qty                        -- 日清：剩余全部归入未知损耗
                WHEN COALESCE(know_lost_amt, 0) > 0
                    THEN 0                                 -- 盘点动作：未知损耗归零
                ELSE 0                                     -- B1-a 默认 0
            END                                         AS unknow_lost_qty,

            -- ── 金额侧：统一用 effective_unit_cost ─────────────────
            CASE WHEN day_clear = '0' THEN 0
                 ELSE eq_end_qty * effective_unit_cost
            END                                         AS end_stock_amt_self,
            CASE
                WHEN day_clear = '0'
                    THEN eq_end_qty * effective_unit_cost
                ELSE 0
            END                                         AS unknow_lost_amt_self,

            -- 源表金额（已有则直接用，未知损耗方程空则用自算）
            COALESCE(unknow_lost_amt_src,
                     CASE WHEN day_clear = '0' THEN eq_end_qty * effective_unit_cost
                          ELSE 0 END)                   AS unknow_lost_amt,

            -- ── 校验字段（D1-b）──────────────────────────────────
            end_stock_amt_official,
            (CASE WHEN day_clear = '0' THEN 0
                  ELSE eq_end_qty * effective_unit_cost
             END) - COALESCE(end_stock_amt_official, 0) AS end_stock_amt_diff,

            -- 是否首日（用于调试）
            day_rank

        FROM equation
        """)
        rows = self._duck.row_count(self.TARGET_TABLE)
        self._log.info(f"t_calc_stock: {rows} rows")
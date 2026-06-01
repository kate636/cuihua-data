"""
v4 三段对标脚本（E1-c）

输入: 已跑过 `python -m fm_etl_v3.executor 2026-04-20 2026-04-20`

对标口径:
    1. SKU 级毛利        : 我.store_profit_sales  vs  strategy_fm_allowance_di.profit_amt
                           （上游官方口径, 销售方程 profit_amt = split_sale_amt - sale_article_receive_amt）
    2. 大类级毛利        : 我.t_fm_levels_result  vs  strategy_fm_levels_result
                           （level_description='大分类'）
    3. 库存自算 vs 官方  : 我.t_calc_stock.end_stock_amt_self  vs  atomic_inventory.end_stock_amt
                           （D1-b 自算精度检查）

用法:
    python -m fm_etl_v3.scripts.verify_v4
"""

from __future__ import annotations

import duckdb
import pandas as pd

from fm_etl_v3.config import get_settings
from fm_etl_v3.connectors import ApiConnector


TARGET_DATE = "2026-04-20"
TOP_N = 20


def _fmt_pct(x: float) -> str:
    if pd.isna(x):
        return "-"
    return f"{x*100:+7.2f}%"


# ════════════════════════════════════════════════════════════════════════
# Segment 1: SKU 级对标 allowance_di
# ════════════════════════════════════════════════════════════════════════
def verify_sku_vs_allowance(con: duckdb.DuckDBPyConnection, api: ApiConnector) -> None:
    print("\n" + "=" * 100)
    print("【Segment 1】SKU 级对标 · strategy_fm_allowance_di (官方销售方程 profit_amt)")
    print("=" * 100)

    # 本地：每个 sku 当天销售方程 & 库存方程毛利
    local = con.execute(f"""
        SELECT
            store_id,
            business_date,
            article_id,
            SUM(sale_amt)                   AS sale_amt,
            SUM(store_profit_sales)         AS profit_sales,
            SUM(store_profit_stock)         AS profit_stock,
            MAX(cost_source)                AS cost_source
        FROM t_fm_sku_dim
        WHERE business_date = '{TARGET_DATE}'
          AND total_sale_amt > 0
        GROUP BY store_id, business_date, article_id
    """).df()

    # 官方基准：allowance_di 粒度 (store, date, abi_article_id)
    base = api.query(f"""
        SELECT
            store_id,
            inc_day                          AS business_date,
            abi_article_id                   AS article_id,
            SUM(split_sale_amt)              AS split_sale_amt,
            SUM(sale_article_receive_amt)    AS receive_amt,
            SUM(profit_amt)                  AS profit_amt_base
        FROM strategy_fm_allowance_di
        WHERE inc_day = '{TARGET_DATE}'
        GROUP BY store_id, inc_day, abi_article_id
    """)

    merged = local.merge(base, on=["store_id", "business_date", "article_id"],
                         how="left")
    merged["diff_sales"] = merged["profit_sales"] - merged["profit_amt_base"]
    merged["diff_stock"] = merged["profit_stock"] - merged["profit_amt_base"]

    n_total = len(merged)
    n_match = merged["profit_amt_base"].notna().sum()
    print(f"SKU 行数: local={n_total}, matched with allowance={n_match}")

    # 差异 top20
    top = merged.reindex(merged["diff_sales"].abs().sort_values(ascending=False).index)[
        ["store_id", "article_id", "sale_amt", "cost_source",
         "profit_sales", "profit_stock", "profit_amt_base",
         "diff_sales", "diff_stock"]
    ].head(TOP_N)
    print(f"\n── 销售方程差异 Top{TOP_N} ──")
    print(top.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))

    # 整体对齐度
    total_local_sales = merged["profit_sales"].sum()
    total_local_stock = merged["profit_stock"].sum()
    total_base = merged["profit_amt_base"].sum()
    print(f"\n合计:")
    print(f"  销售方程 local = {total_local_sales:>15,.2f}, base = {total_base:>15,.2f}, "
          f"diff = {total_local_sales-total_base:>+12,.2f} "
          f"({_fmt_pct((total_local_sales-total_base)/abs(total_base) if total_base else float('nan'))})")
    print(f"  库存方程 local = {total_local_stock:>15,.2f}, base = {total_base:>15,.2f}, "
          f"diff = {total_local_stock-total_base:>+12,.2f} "
          f"({_fmt_pct((total_local_stock-total_base)/abs(total_base) if total_base else float('nan'))})")


# ════════════════════════════════════════════════════════════════════════
# Segment 2: 大类级对标 levels_result
# ════════════════════════════════════════════════════════════════════════
def verify_category_vs_strategy(con: duckdb.DuckDBPyConnection, api: ApiConnector) -> None:
    print("\n" + "=" * 100)
    print("【Segment 2】大类级对标 · strategy_fm_levels_result")
    print("=" * 100)

    local = con.execute(f"""
        SELECT
            "大分类"        AS category_name,
            "非日清标识"     AS dc_flag,
            "全天销售额"     AS sale_amt,
            "门店毛利额"     AS profit_legacy,
            "门店毛利额_销售方程"  AS profit_sales,
            "门店毛利额_库存方程"  AS profit_stock,
            "门店毛利率"     AS rate_legacy,
            "门店毛利率_销售方程"  AS rate_sales,
            "门店毛利率_库存方程"  AS rate_stock
        FROM t_fm_levels_result
        WHERE "日期" = '{TARGET_DATE}'
          AND "分类等级" = '大类'
          AND "非日清标识" = '合计'
        ORDER BY "大分类"
    """).df()

    base = api.query(f"""
        SELECT
            category_name,
            total_sale_amount       AS sale_amt_base,
            store_profit_amount     AS profit_base,
            store_profit_rate       AS rate_base
        FROM strategy_fm_levels_result
        WHERE business_date = '{TARGET_DATE}'
          AND level_description = '大分类'
          AND day_clear_flag = '合计'
    """)

    merged = local.merge(base, on="category_name", how="outer")
    merged["diff_sales"] = merged["profit_sales"] - merged["profit_base"]
    merged["diff_stock"] = merged["profit_stock"] - merged["profit_base"]
    merged["rate_diff_sales"] = merged["rate_sales"] - merged["rate_base"]

    display = merged[[
        "category_name", "sale_amt", "sale_amt_base",
        "profit_sales", "profit_stock", "profit_base",
        "diff_sales", "diff_stock",
        "rate_sales", "rate_stock", "rate_base", "rate_diff_sales"
    ]]
    print(display.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))


# ════════════════════════════════════════════════════════════════════════
# Segment 3: 库存自算 vs 官方
# ════════════════════════════════════════════════════════════════════════
def verify_stock_self_vs_official(con: duckdb.DuckDBPyConnection) -> None:
    print("\n" + "=" * 100)
    print("【Segment 3】库存自算 vs 官方 purchase_di.end_stock_amt (D1-b 精度检查)")
    print("=" * 100)

    summary = con.execute(f"""
        SELECT
            COUNT(*)                                        AS n_total,
            SUM(CASE WHEN ABS(end_stock_amt_diff) < 0.01
                     THEN 1 ELSE 0 END)                     AS n_match,
            SUM(CASE WHEN ABS(end_stock_amt_diff) >= 0.01
                      AND ABS(end_stock_amt_diff) < 1
                     THEN 1 ELSE 0 END)                     AS n_small_diff,
            SUM(CASE WHEN ABS(end_stock_amt_diff) >= 1
                     THEN 1 ELSE 0 END)                     AS n_big_diff,
            SUM(end_stock_amt_self)                         AS total_self,
            SUM(end_stock_amt_official)                     AS total_official,
            SUM(end_stock_amt_diff)                         AS total_diff
        FROM t_calc_stock
        WHERE business_date = '{TARGET_DATE}'
    """).df()
    print(summary.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))

    top = con.execute(f"""
        SELECT
            store_id, article_id, day_clear, cost_source,
            init_stock_qty, receive_qty, compose_in_qty, compose_out_qty,
            sale_qty, know_lost_qty, end_stock_qty,
            end_stock_amt_self, end_stock_amt_official, end_stock_amt_diff
        FROM t_calc_stock
        WHERE business_date = '{TARGET_DATE}'
        ORDER BY ABS(end_stock_amt_diff) DESC
        LIMIT {TOP_N}
    """).df()
    print(f"\n── 自算差异 Top{TOP_N} ──")
    print(top.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))


def main() -> None:
    cfg = get_settings()
    con = duckdb.connect(cfg.duckdb_conn_str, read_only=True)
    api = ApiConnector(cfg)

    try:
        verify_sku_vs_allowance(con, api)
    except Exception as e:  # noqa: BLE001
        print(f"[Segment 1 skipped] {e}")

    try:
        verify_category_vs_strategy(con, api)
    except Exception as e:  # noqa: BLE001
        print(f"[Segment 2 skipped] {e}")

    try:
        verify_stock_self_vs_official(con)
    except Exception as e:  # noqa: BLE001
        print(f"[Segment 3 skipped] {e}")

    con.close()


if __name__ == "__main__":
    main()

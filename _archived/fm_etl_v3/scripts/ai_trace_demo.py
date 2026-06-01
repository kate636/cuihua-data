"""
v4 Phase 6 · AI 溯源工作流 Demo

演示：给定任意 (date, store_id, sku) 三元组，从 `t_fm_bom_breakdown` +
`t_fm_stock_roll` 两张 AI 溯源表里拼出"为什么 SKU-X 今天毛利为负"的完整证据链。

每个 SKU 输出 6 段结构化结果：
    1. 基础概况（销量 / 销售额 / 销售成本 / 毛利双口径）
    2. BOM 分摊来源（parent × 分摊额 × cost_rate_source × sub_qty_source）
    3. 有效单位成本构成（DIRECT / BOM_ALLOC / PURCHASE_AVG / MISSING）
    4. 库存八要素（init / receive / cin / cout / sale / know / unknow / end）
    5. 库存方程自算 vs 官方差异
    6. AI-friendly 的自然语言结论

用法:
    # 默认：挑 2026-04-20 毛利最负 top 3 SKU 自动溯源
    python -m fm_etl_v3.scripts.ai_trace_demo

    # 指定 SKU：
    python -m fm_etl_v3.scripts.ai_trace_demo --date 2026-04-20 --sku 20110932

依赖：已跑过 `python -m fm_etl_v3.executor 2026-04-20 2026-04-20`
"""

from __future__ import annotations

import argparse
from typing import List, Optional

import duckdb
import pandas as pd

from fm_etl_v3.config import get_settings


BAR = "─" * 78


def _fmt_money(x: float) -> str:
    if pd.isna(x):
        return "-"
    return f"{x:>10,.2f}"


def _fmt_qty(x: float) -> str:
    if pd.isna(x):
        return "-"
    return f"{x:>10,.3f}"


def _fmt_pct(x: float) -> str:
    if pd.isna(x):
        return "-"
    return f"{x * 100:+7.2f}%"


def pick_worst_profit_skus(con: duckdb.DuckDBPyConnection, date: str, n: int) -> List[str]:
    """挑当天毛利最负的 n 个 SKU（有销量的）。"""
    df = con.execute(
        f"""
        SELECT article_id
        FROM t_fm_sku_dim
        WHERE business_date = '{date}'
          AND total_sale_qty > 0
        ORDER BY store_profit_sales ASC
        LIMIT {n}
        """
    ).fetch_df()
    return df["article_id"].astype(str).tolist()


def trace_sku(con: duckdb.DuckDBPyConnection, date: str, sku: str) -> None:
    """溯源单个 SKU。"""

    # Segment 1: 基础概况
    sku_row = con.execute(
        f"""
        SELECT
            business_date, store_id, article_id, article_name, day_clear,
            category_level1_description AS category_l1,
            total_sale_qty, total_sale_amt,
            effective_unit_cost, cost_source,
            store_profit_sales, store_profit_stock, store_profit_diff
        FROM t_fm_sku_dim
        WHERE business_date = '{date}' AND article_id = '{sku}'
        LIMIT 1
        """
    ).fetch_df()

    if sku_row.empty:
        print(f"[WARN] SKU {sku} on {date} not found in t_fm_sku_dim")
        return

    r = sku_row.iloc[0]
    print(BAR)
    print(f"  SKU {r['article_id']} · {r['article_name']}")
    print(f"  {r['business_date']} · store {r['store_id']} · day_clear={r['day_clear']}")
    print(f"  大类: {r['category_l1']}")
    print(BAR)

    print("\n【1】基础概况")
    print(f"  销量                    {_fmt_qty(r['total_sale_qty'])}")
    print(f"  销售额                  {_fmt_money(r['total_sale_amt'])}")
    print(f"  有效单位成本            {_fmt_money(r['effective_unit_cost'])}  (来源: {r['cost_source']})")
    print(f"  门店毛利额_销售方程     {_fmt_money(r['store_profit_sales'])}")
    print(f"  门店毛利额_库存方程     {_fmt_money(r['store_profit_stock'])}")
    print(f"  双口径差异              {_fmt_money(r['store_profit_diff'])}")

    # Segment 2: BOM 分摊
    bom_df = con.execute(
        f"""
        SELECT
            parent_article_id, parent_article_name,
            parent_inbound_qty, parent_unit_price,
            dressing_rate, cost_rate_effective, cost_rate_source,
            sub_qty_actual, sub_qty_source,
            sub_alloc_amt, sub_unit_cost
        FROM t_fm_bom_breakdown
        WHERE business_date = '{date}' AND sub_article_id = '{sku}'
        ORDER BY sub_alloc_amt DESC NULLS LAST
        """
    ).fetch_df()

    print("\n【2】BOM 分摊来源（parent × sub）")
    if bom_df.empty:
        print("  (该 sku 当天无 BOM 分摊行，走 DIRECT 或 PURCHASE_AVG 分支)")
    else:
        print(f"  共 {len(bom_df)} 个 parent 贡献分摊：")
        for _, b in bom_df.iterrows():
            print(
                f"  • {b['parent_article_name']:<20s} "
                f"父量={_fmt_qty(b['parent_inbound_qty'])} "
                f"父均价={_fmt_money(b['parent_unit_price'])} "
                f"出肉率={_fmt_pct(b['dressing_rate'])} "
                f"cost_rate={_fmt_pct(b['cost_rate_effective'])} "
                f"({b['cost_rate_source']})"
            )
            print(
                f"    → sub量={_fmt_qty(b['sub_qty_actual'])} "
                f"({b['sub_qty_source']}) "
                f"分摊额={_fmt_money(b['sub_alloc_amt'])} "
                f"子单价={_fmt_money(b['sub_unit_cost'])}"
            )

    # Segment 3: 库存八要素
    stock_row = con.execute(
        f"""
        SELECT
            init_stock_qty, init_stock_amt,
            receive_qty, receive_amt,
            compose_in_qty, compose_in_amt,
            compose_out_qty, compose_out_amt,
            sale_qty, sale_amt,
            know_lost_qty, know_lost_amt,
            unknow_lost_qty, unknow_lost_amt,
            end_stock_qty, end_stock_amt, end_stock_amt_official, end_stock_amt_diff
        FROM t_fm_stock_roll
        WHERE business_date = '{date}' AND article_id = '{sku}'
        LIMIT 1
        """
    ).fetch_df()

    print("\n【3】库存八要素滚动")
    if stock_row.empty:
        print("  (该 sku 当天无库存行)")
    else:
        s = stock_row.iloc[0]
        rows = [
            ("期初", s["init_stock_qty"], s["init_stock_amt"]),
            ("+ 进货", s["receive_qty"], s["receive_amt"]),
            ("+ 加工入", s["compose_in_qty"], s["compose_in_amt"]),
            ("− 加工出", s["compose_out_qty"], s["compose_out_amt"]),
            ("− 销售", s["sale_qty"], s["sale_amt"]),
            ("− 已知损", s["know_lost_qty"], s["know_lost_amt"]),
            ("− 未知损", s["unknow_lost_qty"], s["unknow_lost_amt"]),
            ("= 期末(自算)", s["end_stock_qty"], s["end_stock_amt"]),
        ]
        print(f"  {'分量':<14s} {'数量':>10s}   {'金额':>10s}")
        for label, qty, amt in rows:
            print(f"  {label:<14s} {_fmt_qty(qty)}   {_fmt_money(amt)}")

        print("\n【4】库存方程自算 vs 官方 end_stock")
        print(f"  自算期末金额           {_fmt_money(s['end_stock_amt'])}")
        print(f"  官方期末金额(purchase) {_fmt_money(s['end_stock_amt_official'])}")
        print(f"  差异 (自算 − 官方)     {_fmt_money(s['end_stock_amt_diff'])}")

    # Segment 5: AI 结论
    print("\n【5】AI-friendly 结论")
    reasons = []

    if r["store_profit_sales"] < 0:
        reasons.append(f"门店毛利（销售方程）= {r['store_profit_sales']:.2f} < 0")

    if r["cost_source"] == "MISSING":
        reasons.append("effective_unit_cost 四级 fallback 全部失败（MISSING），成本被记 0，毛利口径失真")
    elif r["cost_source"] == "PURCHASE_AVG":
        reasons.append("走 PURCHASE_AVG 兜底（无 BOM 分摊），用 avg_inbound_price 做成本，可能低估部位加价率")
    elif r["cost_source"] == "BOM_ALLOC" and not bom_df.empty:
        infer_rows = bom_df[bom_df["cost_rate_source"] == "PRICE_WEIGHT_INFERRED"]
        if len(infer_rows) > 0:
            reasons.append(
                f"{len(infer_rows)}/{len(bom_df)} 个 parent 的 cost_rate 由 sub.original_price × dressing_rate 反推得到"
            )
        theory_rows = bom_df[bom_df["sub_qty_source"] == "BOM_THEORETICAL"]
        if len(theory_rows) > 0:
            reasons.append(
                f"{len(theory_rows)}/{len(bom_df)} 个 parent 的 sub_qty 来自理论值（receive_sale_di 缺失该 parent）"
            )

    if abs(r["store_profit_diff"] or 0) > max(100.0, abs(r["total_sale_amt"]) * 0.1):
        reasons.append(
            f"销售方程与库存方程差异 {r['store_profit_diff']:.2f} 偏大，"
            "暗示库存某个分量缺数据（init/receive/compose/loss）"
        )

    if not stock_row.empty:
        s = stock_row.iloc[0]
        if abs(s["end_stock_amt_diff"] or 0) > max(50.0, abs(s["end_stock_amt_official"] or 0) * 0.2):
            reasons.append(
                f"自算期末 {s['end_stock_amt']:.2f} vs 官方 {s['end_stock_amt_official']:.2f}，"
                f"差 {s['end_stock_amt_diff']:.2f} — 需要排查 purchase_di 与 loss_di 对齐"
            )
        if (s["unknow_lost_amt"] or 0) > 0 and r["day_clear"] == "0":
            reasons.append(f"日清商品当天产生未知损耗 {s['unknow_lost_amt']:.2f}（= 期末库存被当损耗强清 0）")

    if not reasons:
        print("  (无明显异常，毛利符合预期)")
    else:
        for i, reason in enumerate(reasons, 1):
            print(f"  {i}. {reason}")

    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="v4 AI 溯源工作流 demo")
    parser.add_argument("--date", default="2026-04-20", help="业务日期")
    parser.add_argument("--sku", default=None, help="单个 sale_article_id；默认挑毛利最负 top3")
    parser.add_argument("--top", type=int, default=3, help="未指定 sku 时取负毛利 top N")
    args = parser.parse_args()

    settings = get_settings()
    con = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        skus = [args.sku] if args.sku else pick_worst_profit_skus(con, args.date, args.top)
        if not skus:
            print(f"[WARN] {args.date} 未找到可追的 SKU")
            return

        print()
        print("═" * 78)
        print(f"  FM ETL v4 · AI 溯源工作流 demo · {args.date}")
        if args.sku:
            print(f"  指定 sku: {args.sku}")
        else:
            print(f"  自动挑选毛利最负 top {len(skus)}")
        print("═" * 78)

        for sku in skus:
            trace_sku(con, args.date, sku)

    finally:
        con.close()


if __name__ == "__main__":
    main()

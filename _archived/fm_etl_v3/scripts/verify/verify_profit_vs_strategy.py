"""
Phase 5 · 毛利率 & 损耗对齐验证（v3.1）

基准: strategy_fm_levels_result (商分基准, 门店/大分类 准确; SKU 维度不准)
对比: 本地 t_fm_levels_result (v3.1 产出)

对齐维度:
    1. 门店 · 合计 / 非日清
    2. 大分类 × 门店 · 合计 / 非日清

核对指标:
    销售额 | 全链路毛利额+率 | 门店毛利额+率 | 供应链毛利额+率 |
    损耗额+率 | 已知/未知损耗

依赖:
    已跑过 `python -m fm_etl_v3.executor 2026-04-20 2026-04-20`
"""

import duckdb
import pandas as pd

from fm_etl_v3.config import get_settings
from fm_etl_v3.connectors import ApiConnector


TARGET_DATE = "2026-04-20"


def _fetch_strategy(api: ApiConnector, level: str) -> pd.DataFrame:
    """从 strategy_fm_levels_result 拉基准数据（按 level 过滤）。"""
    sql = f"""
    SELECT
        business_date,
        level_description                      AS level_desc,
        day_clear_flag                         AS dc_flag,
        category_name,
        category_level3_description,
        store_name,
        total_sale_amount                      AS sale_amt,
        full_link_profit_amount                AS full_link_profit,
        full_link_profit_rate                  AS full_link_rate,
        store_profit_amount                    AS store_profit,
        store_profit_rate                      AS store_rate,
        supply_chain_profit_amount             AS scm_profit,
        supply_chain_profit_rate               AS scm_rate,
        loss_amount                            AS loss_amt,
        loss_rate,
        store_know_lost_amt                    AS know_lost,
        store_unknow_lost_amt                  AS unknow_lost
    FROM strategy_fm_levels_result
    WHERE business_date = '{TARGET_DATE}'
      AND level_description = '{level}'
      AND day_clear_flag IN ('合计', '非日清')
    """
    return api.query(sql)


def _fetch_local_store(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """本地 t_fm_levels_result 门店维度。"""
    return con.execute(f"""
    SELECT
        "日期"        AS business_date,
        "分类等级"    AS level_desc,
        "非日清标识"   AS dc_flag,
        "门店名称"    AS store_name,
        "全天销售额"   AS sale_amt,
        "全链路毛利额" AS full_link_profit,
        "全链路毛利率" AS full_link_rate,
        "门店毛利额"   AS store_profit,
        "门店毛利率"   AS store_rate,
        "供应链毛利额" AS scm_profit,
        "供应链毛利率" AS scm_rate,
        "损耗额"      AS loss_amt,
        "损耗率"      AS loss_rate,
        "已知损耗率"   AS know_lost_rate,
        "未知损耗率"   AS unknow_lost_rate
    FROM t_fm_levels_result
    WHERE "日期" = '{TARGET_DATE}'
      AND "分类等级" = '门店'
    ORDER BY dc_flag
    """).df()


def _fetch_local_big_cat(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """本地 t_fm_levels_result 大分类维度。"""
    return con.execute(f"""
    SELECT
        "日期"        AS business_date,
        "分类等级"    AS level_desc,
        "非日清标识"   AS dc_flag,
        "大分类"      AS category_name,
        "全天销售额"   AS sale_amt,
        "全链路毛利额" AS full_link_profit,
        "全链路毛利率" AS full_link_rate,
        "门店毛利额"   AS store_profit,
        "门店毛利率"   AS store_rate,
        "供应链毛利额" AS scm_profit,
        "供应链毛利率" AS scm_rate,
        "损耗额"      AS loss_amt,
        "损耗率"      AS loss_rate,
        "已知损耗率"   AS know_lost_rate,
        "未知损耗率"   AS unknow_lost_rate
    FROM t_fm_levels_result
    WHERE "日期" = '{TARGET_DATE}'
      AND "分类等级" = '大类'
    ORDER BY "大分类", dc_flag
    """).df()


def _diff_col(local: float, base: float) -> str:
    """生成 local - base (diff%) 列。"""
    if pd.isna(local) or pd.isna(base):
        return "-"
    diff = local - base
    if abs(base) < 1e-9:
        return f"{diff:+.2f} (base=0)"
    pct = 100 * diff / abs(base)
    return f"{diff:+.2f} ({pct:+.2f}%)"


def compare_store(strategy_df: pd.DataFrame, local_df: pd.DataFrame) -> None:
    print("\n" + "=" * 96)
    print("【对齐 A】门店维度 (应该 基本对齐)")
    print("=" * 96)

    cols = [
        "sale_amt", "full_link_profit", "full_link_rate",
        "store_profit", "store_rate",
        "scm_profit", "scm_rate",
        "loss_amt", "loss_rate",
    ]

    for dc in ["合计", "非日清"]:
        s_row = strategy_df[strategy_df["dc_flag"] == dc]
        l_row = local_df[local_df["dc_flag"] == dc]
        print(f"\n── day_clear = {dc} ──")
        if s_row.empty or l_row.empty:
            print(f"  (strategy rows={len(s_row)}, local rows={len(l_row)})")
            continue
        s = s_row.iloc[0]
        l = l_row.iloc[0]
        print(f"{'指标':20s} {'strategy (基准)':>18s} {'local (v3.1)':>18s} {'差值 (相对%)':>30s}")
        for c in cols:
            print(f"{c:20s} {float(s[c] or 0):>18.4f} {float(l[c] or 0):>18.4f} {_diff_col(l[c], s[c]):>30s}")


def compare_big_cat(strategy_df: pd.DataFrame, local_df: pd.DataFrame) -> None:
    print("\n" + "=" * 100)
    print("【对齐 B】大分类维度 × day_clear_flag='合计' (应该 基本对齐)")
    print("=" * 100)

    s = strategy_df[strategy_df["dc_flag"] == "合计"].copy()
    l = local_df[local_df["dc_flag"] == "合计"].copy()

    merged = l.merge(
        s, left_on="category_name", right_on="category_name",
        how="outer", suffixes=("_local", "_strategy"),
        indicator=True,
    )
    print(f"\n参与对比: local={len(l)}  strategy={len(s)}  匹配后={len(merged)}")

    show_cols = [
        ("category_name", "大分类"),
        ("sale_amt_local", "我.销售额"),
        ("sale_amt_strategy", "基.销售额"),
        ("full_link_profit_local", "我.全链毛利"),
        ("full_link_profit_strategy", "基.全链毛利"),
        ("store_profit_local", "我.门店毛利"),
        ("store_profit_strategy", "基.门店毛利"),
        ("scm_profit_local", "我.供链毛利"),
        ("scm_profit_strategy", "基.供链毛利"),
        ("loss_amt_local", "我.损耗额"),
        ("loss_amt_strategy", "基.损耗额"),
    ]
    col_keys = [k for k, _ in show_cols]
    display = merged[col_keys + ["_merge"]].copy()
    display.columns = [v for _, v in show_cols] + ["merge"]
    for col in display.columns:
        if col not in ("大分类", "merge"):
            display[col] = pd.to_numeric(display[col], errors="coerce").round(2)
    print(display.to_string(index=False))

    # 毛利率 & 损耗率对齐
    print("\n── 毛利率 & 损耗率对比（百分比）──")
    rate_cols = [
        ("full_link_rate_local",    "full_link_rate_strategy",    "全链路毛利率"),
        ("store_rate_local",        "store_rate_strategy",        "门店毛利率"),
        ("scm_rate_local",          "scm_rate_strategy",          "供应链毛利率"),
        ("loss_rate_local",         "loss_rate_strategy",         "损耗率"),
    ]
    rate_display = merged[["category_name"] + [c for pair in rate_cols for c in pair[:2]]].copy()
    for col in rate_display.columns:
        if col != "category_name":
            rate_display[col] = pd.to_numeric(rate_display[col], errors="coerce").round(4)
    print(rate_display.to_string(index=False))


def main() -> None:
    cfg = get_settings()
    con = duckdb.connect(cfg.duckdb_conn_str, read_only=True)
    api = ApiConnector(cfg)

    # 门店维度
    strategy_store = _fetch_strategy(api, "门店")
    local_store = _fetch_local_store(con)
    print(f"strategy 门店行数: {len(strategy_store)}")
    print(f"local    门店行数: {len(local_store)}")
    compare_store(strategy_store, local_store)

    # 大分类维度
    strategy_cat = _fetch_strategy(api, "大分类")
    local_cat = _fetch_local_big_cat(con)
    print(f"\nstrategy 大分类行数: {len(strategy_cat)}")
    print(f"local    大分类行数: {len(local_cat)}")
    compare_big_cat(strategy_cat, local_cat)

    con.close()


if __name__ == "__main__":
    main()

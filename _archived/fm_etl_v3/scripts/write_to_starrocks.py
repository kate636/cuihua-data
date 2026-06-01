"""
将 DuckDB 数据写入 StarRocks 商分数据库

运行方式：
    python -m fm_etl_v3.scripts.write_to_starrocks --date 2026-04-23 --tables all
    python -m fm_etl_v3.scripts.write_to_starrocks --date 2026-04-23 --tables atomic,calc,fm

注意：
    - 需要先在 StarRocks 创建目标表（见 docs/sql/ 目录下的建表 SQL）
    - 写入模式为 REPLACE_PARTITION（按日期分区覆盖）
    - 原子层表已有，计算层/FM底表需要新建
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

from fm_etl_v3.config import get_settings
from fm_etl_v3.connectors import ApiConnector
from fm_etl_v3.utils import get_logger

log = get_logger("write_to_starrocks")

# ============================================================================
# 表映射配置
# ============================================================================

# 原子层 → 商分库目标表名（已存在）
ATOMIC_TABLES = {
    "atomic_sales": "strategy_fm_sales_di",
    "atomic_inventory": "strategy_fm_purchase_di",
    "atomic_receive_sale": "strategy_fm_receive_sale_di",
    "atomic_bom_relation": "strategy_dim_store_article_bom_relation",
    "atomic_cost_price": "strategy_fm_inventory_pool_di",
    "atomic_loss": "strategy_fm_loss_di",
    "atomic_compose": "strategy_fm_compose_di",
    "atomic_scm": "strategy_fm_scm_di",
    "atomic_scm_adjust": "strategy_fm_scm_adjust_di",
    "atomic_allowance": "strategy_fm_allowance_di",
    "atomic_promo": "strategy_fm_promo_di",
    "atomic_price": "strategy_fm_price_da",
    # 维度表
    "dim_day_clear": "strategy_fm_dim_day_clear",
    "dim_store_profile": "strategy_fm_dim_store_profile",
    "dim_goods": "strategy_fm_dim_goods",
    "dim_saleable": "strategy_fm_dim_saleable",
    "dim_calendar": "strategy_fm_dim_calendar",
}

# 计算层 → 商分库目标表名（需新建）
CALC_TABLES = {
    "t_calc_bom_alloc": "strategy_fm_calc_bom_alloc_di",
    "t_calc_sku_cost": "strategy_fm_calc_sku_cost_di",
    "t_calc_stock": "strategy_fm_calc_stock_di",
    "t_calc_amounts": "strategy_fm_calc_amounts_di",
    "t_calc_profit": "strategy_fm_calc_profit_di",
}

# FM 底表 → 商分库目标表名（需新建）
FM_TABLES = {
    "t_fm_sku_dim": "strategy_fm_sku_dim_di",
    "t_fm_cust": "strategy_fm_cust",
    "t_fm_levels_sum": "strategy_fm_levels_sum",
    "t_fm_levels_result": "strategy_fm_levels_result",
    "t_fm_bom_breakdown": "strategy_fm_bom_breakdown_di",
    "t_fm_stock_roll": "strategy_fm_stock_roll_di",
}


# ============================================================================
# 列名转换：中文 → 英文（StarRocks 要求）
# ============================================================================

FM_RESULT_COLUMN_MAP = {
    "日期": "business_date",
    "门店号": "store_id",
    "门店名称": "store_name",
    "大分类": "category_level1",
    "中分类": "category_level2",
    "小分类": "category_level3",
    "SPU": "spu_name",
    "黑白猪": "blackwhite_pig_name",
    "SKU编码": "article_id",
    "SKU名称": "article_name",
    "分类等级": "level_description",
    "非日清标识": "day_clear_flag",
    "全天销售额": "total_sale_amount",
    "全天销售数量": "total_sale_qty",
    "门店毛利额": "store_profit_amount",
    "门店毛利率": "store_profit_rate",
    "门店毛利额_销售方程": "store_profit_sales",
    "门店毛利额_库存方程": "store_profit_stock",
    "门店毛利口径差异": "store_profit_diff",
    "门店毛利率_销售方程": "store_profit_rate_sales",
    "门店毛利率_库存方程": "store_profit_rate_stock",
    "全链路毛利额": "full_link_profit_amount",
    "全链路毛利率": "full_link_profit_rate",
    "供应链毛利额": "supply_chain_profit_amount",
    "供应链毛利率": "supply_chain_profit_rate",
    "损耗额": "loss_amount",
    "损耗率": "loss_rate",
    "已知损耗额": "known_loss_amount",
    "未知损耗额": "unknown_loss_amount",
    "进货额": "receive_amount",
    "进货数量": "receive_qty",
    "期初库存额": "init_stock_amount",
    "期末库存额": "end_stock_amount",
    "加工转入额": "compose_in_amount",
    "加工转出额": "compose_out_amount",
}

FM_SUM_COLUMN_MAP = {
    "日期": "business_date",
    "门店号": "store_id",
    "门店名称": "store_name",
    "大分类": "category_level1",
    "中分类": "category_level2",
    "小分类": "category_level3",
    "SPU": "spu_name",
    "黑白猪": "blackwhite_pig_name",
    "SKU编码": "article_id",
    "SKU名称": "article_name",
    "分类等级": "level_description",
    "非日清标识": "day_clear_flag",
    "全天销售额": "total_sale_amount",
    "全天销售数量": "total_sale_qty",
    "门店毛利额": "store_profit_amount",
    "门店毛利率": "store_profit_rate",
    "全链路毛利额": "full_link_profit_amount",
    "全链路毛利率": "full_link_profit_rate",
    "供应链毛利额": "supply_chain_profit_amount",
    "供应链毛利率": "supply_chain_profit_rate",
    "损耗额": "loss_amount",
    "损耗率": "loss_rate",
}


def rename_columns(df: pd.DataFrame, src_table: str) -> pd.DataFrame:
    """将 DuckDB 中文列名转换为 StarRocks 英文列名"""
    if src_table == "t_fm_levels_result":
        return df.rename(columns=FM_RESULT_COLUMN_MAP)
    elif src_table == "t_fm_levels_sum":
        return df.rename(columns=FM_SUM_COLUMN_MAP)
    # 其他表列名已是英文，无需转换
    return df


# ============================================================================
# 写入逻辑
# ============================================================================


def write_table(
    duck: duckdb.DuckDBPyConnection,
    api: ApiConnector,
    src_table: str,
    target_table: str,
    date: str,
    chunk_size: int = 5000,
) -> int:
    """
    从 DuckDB 读取 src_table，写入 StarRocks target_table

    返回：写入行数
    """
    # 读取 DuckDB 数据
    try:
        df = duck.execute(f"SELECT * FROM {src_table} WHERE business_date = '{date}'").df()
    except duckdb.BinderError:
        # 尝试其他日期字段名
        try:
            df = duck.execute(f"SELECT * FROM {src_table} WHERE inc_day = '{date}'").df()
        except duckdb.BinderError:
            log.warning(f"{src_table}: 无 business_date/inc_day 字段，读取全表")
            df = duck.execute(f"SELECT * FROM {src_table}").df()

    if df.empty:
        log.warning(f"{src_table} → {target_table}: 无数据（日期={date}）")
        return 0

    # 列名转换
    df = rename_columns(df, src_table)

    # 确保 business_date 列存在（分区键）
    if "business_date" not in df.columns:
        if "inc_day" in df.columns:
            df["business_date"] = df["inc_day"]
        elif "日期" in df.columns:
            df["business_date"] = df["日期"]
        else:
            df["business_date"] = date

    # 写入 StarRocks（通过 API）
    total_rows = len(df)
    log.info(f"{src_table} → {target_table}: {total_rows} 行，开始写入...")

    # 分块写入（避免一次性过大）
    offset = 0
    written = 0
    while offset < total_rows:
        chunk = df.iloc[offset:offset + chunk_size]
        try:
            # 调用 API 写入（replace_partition 模式）
            api.write_table(
                table_name=target_table,
                df=chunk,
                mode="replace_partition",
                partition_col="business_date",
                partition_value=date,
            )
            written += len(chunk)
            log.debug(f"  已写入 {written}/{total_rows}")
        except Exception as e:
            log.error(f"写入失败: {e}")
            raise

    log.info(f"{src_table} → {target_table}: 写入完成 {written} 行")
    return written


def main():
    parser = argparse.ArgumentParser(description="将 DuckDB 数据写入 StarRocks")
    parser.add_argument("--date", required=True, help="业务日期（如 2026-04-23）")
    parser.add_argument(
        "--tables",
        default="all",
        help="写入哪些表: all / atomic / calc / fm / atomic,calc,fm",
    )
    parser.add_argument("--dry-run", action="store_true", help="只打印要写入的表，不实际执行")

    args = parser.parse_args()

    cfg = get_settings()
    duck = duckdb.connect(cfg.duckdb_conn_str, read_only=True)
    api = ApiConnector(cfg)

    # 确定要写入的表
    table_groups = args.tables.split(",")
    tables_to_write: dict[str, str] = {}

    for group in table_groups:
        if group == "all":
            tables_to_write.update(ATOMIC_TABLES)
            tables_to_write.update(CALC_TABLES)
            tables_to_write.update(FM_TABLES)
        elif group == "atomic":
            tables_to_write.update(ATOMIC_TABLES)
        elif group == "calc":
            tables_to_write.update(CALC_TABLES)
        elif group == "fm":
            tables_to_write.update(FM_TABLES)

    log.info(f"日期={args.date}, 表组={args.tables}, 共 {len(tables_to_write)} 张表")

    if args.dry_run:
        log.info("=== DRY RUN: 不实际写入 ===")
        for src, target in tables_to_write.items():
            try:
                cnt = duck.execute(
                    f"SELECT COUNT(*) FROM {src} WHERE business_date = '{args.date}'"
                ).fetchone()[0]
            except:
                cnt = "N/A"
            print(f"  {src} → {target}: {cnt} 行")
        duck.close()
        return

    # 实际写入
    summary = {}
    for src_table, target_table in tables_to_write.items():
        try:
            rows = write_table(duck, api, src_table, target_table, args.date)
            summary[src_table] = rows
        except Exception as e:
            log.error(f"{src_table} 写入失败: {e}")
            summary[src_table] = -1

    # 打印汇总
    print("\n=== 写入汇总 ===")
    total_ok = sum(v for v in summary.values() if v > 0)
    total_fail = sum(1 for v in summary.values() if v < 0)
    print(f"成功写入: {total_ok} 行")
    print(f"失败表数: {total_fail}")
    for src, rows in summary.items():
        status = "✓" if rows >= 0 else "✗"
        print(f"  {status} {src}: {rows} 行")

    duck.close()


if __name__ == "__main__":
    main()
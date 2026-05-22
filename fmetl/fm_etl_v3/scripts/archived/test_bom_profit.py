"""
临时测试脚本：BOM拆分毛利计算（猪肉品类）

用法：
    python -m fm_etl_v3.scripts.test_bom_profit 2026-04-23

目的：
    快速迭代验证 BOM 拆分入/出额计算逻辑，不修改项目代码
"""

import sys
import duckdb
from pathlib import Path


def get_duckdb_path() -> Path:
    """获取 DuckDB 文件路径"""
    # 默认路径
    return Path(__file__).parent.parent.parent / "data" / "fm_etl_v3.duckdb"


def test_bom_split(con: duckdb.DuckDBPyConnection, business_date: str):
    """测试 BOM 拆分入/出额计算"""

    print(f"\n{'='*60}")
    print(f"【测试】BOM拆分入/出额计算 - {business_date}")
    print(f"{'='*60}\n")

    # 1. 查看猪肉品类 atomic_receive_sale 数据
    print("1. 猪肉品类 atomic_receive_sale 数据：")
    df = con.execute(f"""
        SELECT
            store_id,
            article_id AS parent_article_id,
            sale_article_id AS sub_article_id,
            split_sale_article_amt,
            category_level1_id
        FROM atomic_receive_sale
        WHERE business_date = '{business_date}'
          AND category_level1_id = '13'
          AND split_sale_article_amt > 0
        ORDER BY store_id, article_id, sale_article_id
        LIMIT 20
    """).df()
    print(df.to_string())
    print(f"  共 {len(df)} 行（限制20行）\n")

    # 2. 计算拆分入额（sub视角）
    print("2. 拆分入额（sub视角）：")
    df_in = con.execute(f"""
        SELECT
            store_id,
            sub_article_id AS article_id,
            SUM(split_sale_article_amt) AS bom_split_in_amt,
            COUNT(*) AS parent_count
        FROM (
            SELECT
                store_id,
                article_id AS parent_article_id,
                sale_article_id AS sub_article_id,
                split_sale_article_amt
            FROM atomic_receive_sale
            WHERE business_date = '{business_date}'
              AND category_level1_id = '13'
              AND split_sale_article_amt > 0
        )
        GROUP BY store_id, sub_article_id
        ORDER BY bom_split_in_amt DESC
        LIMIT 20
    """).df()
    print(df_in.to_string())
    print(f"\n")

    # 3. 计算拆分出额（parent视角）
    print("3. 拆分出额（parent视角）：")
    df_out = con.execute(f"""
        SELECT
            store_id,
            article_id,
            SUM(split_sale_article_amt) AS bom_split_out_amt,
            COUNT(DISTINCT sale_article_id) AS sub_count
        FROM atomic_receive_sale
        WHERE business_date = '{business_date}'
          AND category_level1_id = '13'
          AND split_sale_article_amt > 0
        GROUP BY store_id, article_id
        ORDER BY bom_split_out_amt DESC
        LIMIT 20
    """).df()
    print(df_out.to_string())
    print(f"\n")

    # 4. 验证：拆分出总额 = 拆分入总额
    print("4. 验证拆分平衡：")
    df_balance = con.execute(f"""
        SELECT
            SUM(bom_split_out_amt) AS total_out,
            SUM(bom_split_in_amt) AS total_in,
            SUM(bom_split_out_amt) - SUM(bom_split_in_amt) AS diff
        FROM (
            -- parent出
            SELECT
                store_id,
                article_id,
                SUM(split_sale_article_amt) AS bom_split_out_amt,
                0 AS bom_split_in_amt
            FROM atomic_receive_sale
            WHERE business_date = '{business_date}'
              AND category_level1_id = '13'
              AND split_sale_article_amt > 0
            GROUP BY store_id, article_id

            UNION ALL

            -- sub入
            SELECT
                store_id,
                sale_article_id AS article_id,
                0 AS bom_split_out_amt,
                SUM(split_sale_article_amt) AS bom_split_in_amt
            FROM atomic_receive_sale
            WHERE business_date = '{business_date}'
              AND category_level1_id = '13'
              AND split_sale_article_amt > 0
            GROUP BY store_id, sale_article_id
        )
    """).df()
    print(df_balance.to_string())
    print(f"\n")


def test_profit_comparison(con: duckdb.DuckDBPyConnection, business_date: str):
    """测试毛利计算对比（含BOM拆分 vs 不含）"""

    print(f"\n{'='*60}")
    print(f"【测试】毛利计算对比 - {business_date}")
    print(f"{'='*60}\n")

    # 新公式（含BOM拆分，不含损耗扣减）
    print("5. 新毛利公式（含BOM拆分，不含损耗扣减）：")
    df_new = con.execute(f"""
        WITH bom_split AS (
            -- 拆分入（sub）
            SELECT
                store_id,
                sale_article_id AS article_id,
                SUM(split_sale_article_amt) AS bom_split_in_amt,
                0 AS bom_split_out_amt
            FROM atomic_receive_sale
            WHERE business_date = '{business_date}'
              AND category_level1_id = '13'
              AND split_sale_article_amt > 0
            GROUP BY store_id, sale_article_id

            UNION ALL

            -- 拆分出（parent）
            SELECT
                store_id,
                article_id,
                0 AS bom_split_in_amt,
                SUM(split_sale_article_amt) AS bom_split_out_amt
            FROM atomic_receive_sale
            WHERE business_date = '{business_date}'
              AND category_level1_id = '13'
              AND split_sale_article_amt > 0
            GROUP BY store_id, article_id
        ),

        -- 猪肉品类的article_id列表
        pork_articles AS (
            SELECT DISTINCT store_id, article_id
            FROM atomic_receive_sale
            WHERE business_date = '{business_date}'
              AND category_level1_id = '13'
        )

        SELECT
            w.store_id,
            w.article_id,
            w.sale_amt,
            amt.receive_amt,
            COALESCE(bs.bom_split_in_amt, 0) AS bom_split_in_amt,
            COALESCE(bs.bom_split_out_amt, 0) AS bom_split_out_amt,
            amt.compose_in_amt,
            amt.compose_out_amt,
            amt.end_stock_amt - amt.init_stock_amt AS stock_change,

            -- 原公式（含损耗扣减）
            w.sale_amt
              - (amt.receive_amt + amt.compose_in_amt - amt.compose_out_amt)
              + (amt.end_stock_amt - amt.init_stock_amt)
              - COALESCE(amt.know_lost_amt, 0) - COALESCE(amt.unknow_lost_amt, 0)
              AS store_profit_old,

            -- 新公式（含BOM拆分，不含损耗扣减）
            w.sale_amt
              - (amt.receive_amt + COALESCE(bs.bom_split_in_amt, 0) - COALESCE(bs.bom_split_out_amt, 0)
                 + amt.compose_in_amt - amt.compose_out_amt)
              + (amt.end_stock_amt - amt.init_stock_amt)
              AS store_profit_new,

            -- 差额
            COALESCE(amt.know_lost_amt, 0) + COALESCE(amt.unknow_lost_amt, 0) AS lost_amt_total

        FROM t_atomic_wide w
        LEFT JOIN t_calc_amounts amt
          ON w.store_id = amt.store_id
         AND w.business_date = amt.business_date
         AND w.article_id = amt.article_id
         AND w.day_clear = amt.day_clear
        LEFT JOIN bom_split bs
          ON w.store_id = bs.store_id
         AND w.article_id = bs.article_id
        JOIN pork_articles pa
          ON w.store_id = pa.store_id
         AND w.article_id = pa.article_id

        WHERE w.business_date = '{business_date}'
          AND w.day_clear = '2'  -- 合计

        ORDER BY w.store_id, w.article_id
        LIMIT 30
    """).df()
    print(df_new.to_string())
    print(f"\n")

    # 6. 汇总对比
    print("6. 猪肉品类毛利汇总对比：")
    df_sum = con.execute(f"""
        WITH bom_split AS (
            SELECT
                store_id,
                sale_article_id AS article_id,
                SUM(split_sale_article_amt) AS bom_split_in_amt,
                0 AS bom_split_out_amt
            FROM atomic_receive_sale
            WHERE business_date = '{business_date}'
              AND category_level1_id = '13'
              AND split_sale_article_amt > 0
            GROUP BY store_id, sale_article_id

            UNION ALL

            SELECT
                store_id,
                article_id,
                0 AS bom_split_in_amt,
                SUM(split_sale_article_amt) AS bom_split_out_amt
            FROM atomic_receive_sale
            WHERE business_date = '{business_date}'
              AND category_level1_id = '13'
              AND split_sale_article_amt > 0
            GROUP BY store_id, article_id
        ),

        pork_articles AS (
            SELECT DISTINCT store_id, article_id
            FROM atomic_receive_sale
            WHERE business_date = '{business_date}'
              AND category_level1_id = '13'
        )

        SELECT
            SUM(w.sale_amt) AS total_sale_amt,
            SUM(amt.receive_amt) AS total_receive_amt,
            SUM(COALESCE(bs.bom_split_in_amt, 0)) AS total_split_in,
            SUM(COALESCE(bs.bom_split_out_amt, 0)) AS total_split_out,
            SUM(amt.end_stock_amt - amt.init_stock_amt) AS total_stock_change,
            SUM(COALESCE(amt.know_lost_amt, 0) + COALESCE(amt.unknow_lost_amt, 0)) AS total_lost,

            -- 原口径合计
            SUM(w.sale_amt
              - (amt.receive_amt + amt.compose_in_amt - amt.compose_out_amt)
              + (amt.end_stock_amt - amt.init_stock_amt)
              - COALESCE(amt.know_lost_amt, 0) - COALESCE(amt.unknow_lost_amt, 0))
              AS sum_profit_old,

            -- 新口径合计
            SUM(w.sale_amt
              - (amt.receive_amt + COALESCE(bs.bom_split_in_amt, 0) - COALESCE(bs.bom_split_out_amt, 0)
                 + amt.compose_in_amt - amt.compose_out_amt)
              + (amt.end_stock_amt - amt.init_stock_amt))
              AS sum_profit_new,

            -- 差额
            SUM(w.sale_amt
              - (amt.receive_amt + amt.compose_in_amt - amt.compose_out_amt)
              + (amt.end_stock_amt - amt.init_stock_amt)
              - COALESCE(amt.know_lost_amt, 0) - COALESCE(amt.unknow_lost_amt, 0))
            - SUM(w.sale_amt
              - (amt.receive_amt + COALESCE(bs.bom_split_in_amt, 0) - COALESCE(bs.bom_split_out_amt, 0)
                 + amt.compose_in_amt - amt.compose_out_amt)
              + (amt.end_stock_amt - amt.init_stock_amt))
              AS sum_diff

        FROM t_atomic_wide w
        LEFT JOIN t_calc_amounts amt
          ON w.store_id = amt.store_id
         AND w.business_date = amt.business_date
         AND w.article_id = amt.article_id
         AND w.day_clear = amt.day_clear
        LEFT JOIN bom_split bs
          ON w.store_id = bs.store_id
         AND w.article_id = bs.article_id
        JOIN pork_articles pa
          ON w.store_id = pa.store_id
         AND w.article_id = pa.article_id

        WHERE w.business_date = '{business_date}'
          AND w.day_clear = '2'
    """).df()
    print(df_sum.to_string())

    # 验证差额 = 损耗总额
    if len(df_sum) > 0 and df_sum['sum_diff'].iloc[0] is not None:
        print(f"\n验证：差额 ({df_sum['sum_diff'].iloc[0]:.2f}) 应等于损耗总额 ({df_sum['total_lost'].iloc[0]:.2f})")


def main():
    db_path = get_duckdb_path()

    if not db_path.exists():
        print(f"❌ DuckDB 文件不存在: {db_path}")
        print("请先运行 ETL")
        sys.exit(1)

    print(f"连接 DuckDB: {db_path}")
    con = duckdb.connect(str(db_path), read_only=True)

    # 自动检测数据日期
    dates_df = con.execute("SELECT DISTINCT business_date FROM atomic_receive_sale ORDER BY business_date").df()
    if len(dates_df) == 0:
        print("❌ atomic_receive_sale 表无数据")
        sys.exit(1)

    business_date = dates_df['business_date'].iloc[-1]
    print(f"使用数据日期: {business_date}")

    try:
        test_bom_split(con, business_date)
        test_profit_comparison(con, business_date)
    finally:
        con.close()

    print(f"\n{'='*60}")
    print("测试完成")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
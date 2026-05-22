"""
Phase 5 BOM 分摊冒烟验证脚本（只读）

跑完 executor 2026-04-20 2026-04-20 后，运行:
    python -m fm_etl_v3.scripts.verify_bom_smoke

验证项:
  1. atomic_bom_relation / t_calc_bom_cost 行数 & rate_source 分布
  2. t_calc_amounts.cost_source 分布
  3. 猪肉大类毛利率前后对比
  4. 今天有哪些 parent 做了 BOM 拆分、贡献了多少成本
"""

import duckdb

from fm_etl_v3.config import get_settings


def main() -> None:
    cfg = get_settings()
    con = duckdb.connect(cfg.duckdb_conn_str, read_only=True)

    def show(title: str, sql: str) -> None:
        print(f"\n=== {title} ===")
        print(con.execute(sql).df().to_string(index=False))

    # 1. BOM 原子表与计算表概览
    show("1. atomic_bom_relation 行数（按 bom_type）", """
        SELECT bom_type, COUNT(*) AS rows,
               COUNT(DISTINCT parent_article_id) AS parent_cnt,
               COUNT(DISTINCT sub_article_id)    AS sub_cnt
        FROM atomic_bom_relation
        WHERE business_date = '2026-04-20'
        GROUP BY bom_type
        ORDER BY bom_type
    """)

    show("2. t_calc_bom_cost (rate_source 分布)", """
        SELECT rate_source, COUNT(*) AS sub_cnt,
               ROUND(SUM(bom_in_qty_kg), 2) AS total_qty_kg,
               ROUND(SUM(bom_in_amt), 2)    AS total_amt,
               ROUND(AVG(bom_unit_cost), 2) AS avg_unit_cost
        FROM t_calc_bom_cost
        GROUP BY rate_source
        ORDER BY sub_cnt DESC
    """)

    show("3. t_calc_bom_cost 详情（明细）", """
        SELECT bom.article_id,
               g.article_name,
               bom.parent_count,
               ROUND(bom.bom_in_qty_kg, 3) AS bom_in_qty_kg,
               ROUND(bom.bom_in_amt, 2)    AS bom_in_amt,
               ROUND(bom.bom_unit_cost, 2) AS bom_unit_cost,
               bom.rate_source
        FROM t_calc_bom_cost bom
        LEFT JOIN dim_goods g USING (article_id)
        ORDER BY bom.bom_in_amt DESC
        LIMIT 20
    """)

    # 2. t_calc_amounts 里 cost_source 分布
    show("4. t_calc_amounts.cost_source 分布（2026-04-20）", """
        SELECT cost_source, COUNT(*) AS n,
               ROUND(SUM(compose_in_amt), 2)  AS compose_in_amt,
               ROUND(SUM(compose_out_amt), 2) AS compose_out_amt,
               ROUND(SUM(lost_amt), 2)        AS lost_amt
        FROM t_calc_amounts
        WHERE business_date = '2026-04-20'
          AND day_clear IN ('0', '1')
        GROUP BY cost_source
        ORDER BY n DESC
    """)

    # 3. 猪肉大类毛利率对比（v3.1 生效后）
    show("5. 猪肉大类 / 全店 毛利率（day_clear='2' 合计口径）", """
        SELECT
            '猪肉类'       AS scope,
            ROUND(SUM(sale_amt), 2)            AS sale_amt,
            ROUND(SUM(sale_cost_amt), 2)       AS sale_cost_amt,
            ROUND(SUM(profit_amt), 2)          AS profit_amt,
            CASE WHEN SUM(sale_amt) > 0
                 THEN ROUND(100 * SUM(profit_amt) / SUM(sale_amt), 2)
                 ELSE NULL END                 AS profit_rate_pct
        FROM t_fm_sku_dim d
        LEFT JOIN dim_goods g USING (article_id)
        WHERE d.business_date = '2026-04-20'
          AND d.day_clear = '2'
          AND g.category_level1_description = '猪肉类'

        UNION ALL

        SELECT
            '全店合计'      AS scope,
            ROUND(SUM(sale_amt), 2),
            ROUND(SUM(sale_cost_amt), 2),
            ROUND(SUM(profit_amt), 2),
            CASE WHEN SUM(sale_amt) > 0
                 THEN ROUND(100 * SUM(profit_amt) / SUM(sale_amt), 2)
                 ELSE NULL END
        FROM t_fm_sku_dim d
        WHERE d.business_date = '2026-04-20'
          AND d.day_clear = '2'
    """)

    # 4. 今天所有 cost_source='BOM' 的 SKU 毛利率（抽查前 10）
    show("6. cost_source='BOM' 猪肉 SKU 的毛利率抽样", """
        SELECT
            d.article_id,
            g.article_name,
            g.category_level1_description AS l1_name,
            ROUND(d.sale_amt, 2)        AS sale_amt,
            ROUND(d.sale_cost_amt, 2)   AS sale_cost_amt,
            ROUND(d.profit_amt, 2)      AS profit_amt,
            CASE WHEN d.sale_amt > 0
                 THEN ROUND(100 * d.profit_amt / d.sale_amt, 2)
                 ELSE NULL END          AS profit_rate_pct
        FROM t_fm_sku_dim d
        LEFT JOIN t_calc_amounts amt
            ON d.store_id = amt.store_id AND d.business_date = amt.business_date
           AND d.article_id = amt.article_id AND d.day_clear = amt.day_clear
        LEFT JOIN dim_goods g USING (article_id)
        WHERE d.business_date = '2026-04-20'
          AND d.day_clear = '2'
          AND amt.cost_source = 'BOM'
          AND d.sale_amt > 0
        ORDER BY d.sale_amt DESC
        LIMIT 15
    """)

    # 5. 100% 毛利率的 SKU：有多少还剩下
    show("7. 仍 100% 毛利率 SKU（按大类）", """
        SELECT
            g.category_level1_description AS l1_name,
            COUNT(*) AS sku_cnt,
            ROUND(SUM(d.sale_amt), 2)  AS sale_amt
        FROM t_fm_sku_dim d
        LEFT JOIN dim_goods g USING (article_id)
        WHERE d.business_date = '2026-04-20'
          AND d.day_clear = '2'
          AND d.sale_amt > 0
          AND (d.profit_amt >= d.sale_amt * 0.999)
        GROUP BY g.category_level1_description
        ORDER BY sale_amt DESC
        LIMIT 20
    """)

    con.close()


if __name__ == "__main__":
    main()

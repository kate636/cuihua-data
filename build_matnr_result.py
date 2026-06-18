"""
独立脚本 — 从 t_fm_levels_sum (SKU级) 按 matnr 合并输出 t_fm_levels_result_matnr。

用法:
    source .venv/bin/activate
    python build_matnr_result.py 2026-06-01 2026-06-09
    python build_matnr_result.py 2026-06-09 2026-06-09

前提: ETL 已跑完 (t_fm_levels_sum + t_fm_cust + dim_goods 就绪, dim_goods 需含 matnr 列)
"""

import sys
import duckdb

DB_PATH = "data/fm.duckdb"
TARGET_TABLE = "t_fm_levels_result_matnr"


def safe_div(num, den):
    return f"CASE WHEN COALESCE({den}, 0) = 0 THEN NULL ELSE ({num}) / ({den}) END"


def build(conn: duckdb.DuckDBPyConnection, start: str, end: str):
    select_sql = f"""
    WITH matnr_sum AS (
        SELECT
            s.store_flag,
            s.store_no,
            s.business_date,
            s.store_name,
            s.store_id,
            g.matnr                                  AS level_id,
            '物料号'                                  AS level_description,
            s.day_clear,
            MAX(s.category_level1_description)       AS category_level1_description,
            MAX(s.category_level2_description)       AS category_level2_description,
            MAX(s.category_level3_description)       AS category_level3_description,
            MAX(s.spu_name)                          AS spu_name,
            ''                                       AS article_name,
            s.manage_area_name,
            s.sap_area_name,
            s.city_description,
            s.week_no,
            s.week_start_date,
            s.week_end_date,
            s.month_wid,
            s.year_wid,
            SUM(s.full_link_article_profit)          AS full_link_article_profit,
            SUM(s.scm_fin_article_profit)            AS scm_fin_article_profit,
            SUM(s.article_profit_amt)                AS article_profit_amt,
            SUM(s.pre_profit_amt)                    AS pre_profit_amt,
            SUM(s.sales_weight)                      AS sales_weight,
            SUM(s.bf19_sales_weight)                 AS bf19_sales_weight,
            SUM(s.total_sale_qty)                    AS total_sale_qty,
            SUM(s.bf19_sale_qty)                     AS bf19_sale_qty,
            SUM(s.inbound_amount)                    AS inbound_amount,
            SUM(s.purchase_weight)                   AS purchase_weight,
            SUM(s.total_sale_amt)                    AS total_sale_amt,
            SUM(s.bf19_sale_amt)                     AS bf19_sale_amt,
            SUM(s.expect_outstock_amt)               AS expect_outstock_amt,
            SUM(s.out_stock_amt_cb)                  AS out_stock_amt_cb,
            SUM(s.pre_sale_amt)                      AS pre_sale_amt,
            SUM(s.pre_inbound_amount)                AS pre_inbound_amount,
            SUM(s.scm_promotion_amt_total)           AS scm_promotion_amt_total,
            SUM(s.lp_sale_amt)                       AS lp_sale_amt,
            SUM(s.discount_amt)                      AS discount_amt,
            SUM(s.hour_discount_amt)                 AS hour_discount_amt,
            SUM(s.discount_amt_cate)                 AS discount_amt_cate,
            SUM(s.store_lost_amt)                    AS store_lost_amt,
            SUM(s.return_amt)                        AS return_amt,
            SUM(s.out_stock_pay_amt)                 AS out_stock_pay_amt,
            SUM(s.out_stock_pay_amt_notax)           AS out_stock_pay_amt_notax,
            SUM(s.return_stock_pay_amt_notax)        AS return_stock_pay_amt_notax,
            SUM(s.bf19_sale_piece_qty)               AS bf19_sale_piece_qty,
            SUM(s.lost_denominator)                  AS lost_denominator,
            SUM(s.end_stock_qty)                     AS end_stock_qty,
            SUM(s.avg_7d_sale_qty)                   AS avg_7d_sale_qty,
            SUM(s.init_stock_amt)                    AS init_stock_amt,
            SUM(s.end_stock_amt)                     AS end_stock_amt,
            SUM(s.init_stock_qty)                    AS init_stock_qty,
            SUM(s.inbound_qty)                       AS inbound_qty,
            SUM(s.is_stock_sku)                      AS is_stock_sku,
            SUM(s.store_lost_qty)                    AS store_lost_qty,
            SUM(s.sale_piece_qty)                    AS sale_piece_qty,
            SUM(s.store_know_lost_amt)               AS store_know_lost_amt,
            SUM(s.store_unknow_lost_amt)             AS store_unknow_lost_amt,
            SUM(s.purchase_weight_kg)                AS purchase_weight_kg,
            SUM(s.is_soldout_16)                     AS is_soldout_16,
            SUM(s.is_soldout_20)                     AS is_soldout_20,
            SUM(s.is_soldout_16_salesku)             AS is_soldout_16_salesku,
            SUM(s.is_soldout_20_salesku)             AS is_soldout_20_salesku
        FROM t_fm_levels_sum s
        JOIN dim_goods g ON s.level_id = g.article_id
        WHERE s.level_description = 'SKU'
          AND s.business_date BETWEEN '{start}' AND '{end}'
        GROUP BY
            s.store_flag, s.store_no, s.business_date,
            s.store_name, s.store_id,
            g.matnr, s.day_clear,
            s.manage_area_name, s.sap_area_name,
            s.city_description,
            s.week_no, s.week_start_date, s.week_end_date,
            s.month_wid, s.year_wid
    ),
    matnr_cust AS (
        SELECT
            c.business_date,
            c.store_id,
            c.day_clear,
            g.matnr,
            MAX(c.cust_num_cate)                    AS cust_num_cate,
            MAX(c.bf19_cust_num_cate)               AS bf19_cust_num_cate,
            COUNT(DISTINCT c.level_id)              AS sale_article_num_cate
        FROM t_fm_cust c
        JOIN dim_goods g ON c.level_id = g.article_id
        WHERE c.level_description = 'SKU'
          AND c.business_date BETWEEN '{start}' AND '{end}'
        GROUP BY c.business_date, c.store_id, c.day_clear, g.matnr
    ),
    matnr_base AS (
        SELECT
            m.*,
            COALESCE(c.cust_num_cate, 0)            AS cust_num_cate,
            COALESCE(c.bf19_cust_num_cate, 0)       AS bf19_cust_num_cate,
            COALESCE(c.sale_article_num_cate, 0)    AS sale_article_num_cate
        FROM matnr_sum m
        LEFT JOIN matnr_cust c
          ON  c.business_date = m.business_date
          AND c.store_id      = m.store_id
          AND c.day_clear     = m.day_clear
          AND c.matnr         = m.level_id
    )
    SELECT
        store_flag                          AS 标签,
        store_no                            AS 门店号,
        business_date                       AS 日期,
        CASE WHEN store_flag IS NULL THEN '广州' ELSE store_name END
                                            AS 门店名称,
        level_id                            AS 商品编码,
        ''                                  AS 分类名称,
        category_level1_description         AS 大分类,
        category_level2_description         AS 中分类,
        category_level3_description         AS 小分类,
        level_description                   AS 分类等级,
        day_clear,
        CASE
            WHEN day_clear = '0' THEN '日清'
            WHEN day_clear = '1' THEN '非日清'
            WHEN day_clear = '2' THEN '合计'
        END                                 AS 非日清标识,
        COUNT(store_id)                     AS 营业店日数,
        COUNT(DISTINCT store_id)            AS 营业店数,
        SUM(full_link_article_profit)       AS 全链路毛利额,
        SUM(scm_fin_article_profit)         AS 供应链毛利额,
        SUM(article_profit_amt)             AS 门店毛利额,
        {safe_div('SUM(full_link_article_profit)', 'SUM(total_sale_amt)')}
                                            AS 全链路毛利率,
        {safe_div('SUM(scm_fin_article_profit)', 'SUM(out_stock_pay_amt_notax) + SUM(return_stock_pay_amt_notax)')}
                                            AS 供应链毛利率,
        {safe_div('SUM(article_profit_amt)', 'SUM(total_sale_amt)')}
                                            AS 门店毛利率,
        SUM(sales_weight)                   AS 销售重量,
        SUM(bf19_sales_weight)              AS "19点前销售重量",
        SUM(total_sale_qty)                 AS 销售数量,
        SUM(bf19_sale_qty)                  AS "19点前销售数量",
        SUM(inbound_amount)                 AS 进货额,
        SUM(total_sale_amt)                 AS 全天销售额,
        SUM(cust_num_cate)                  AS 全天来客数,
        {safe_div('SUM(total_sale_amt)', 'SUM(cust_num_cate)')}
                                            AS 全天客单价,
        SUM(bf19_sale_amt)                  AS "19点前销售额",
        SUM(bf19_cust_num_cate)             AS "19点前客数",
        {safe_div('SUM(bf19_sale_amt)', 'SUM(bf19_cust_num_cate)')}
                                            AS "19点前客单价",
        {safe_div('SUM(bf19_sale_amt)', 'SUM(bf19_sale_piece_qty)')}
                                            AS "19点前件单价",
        {safe_div('SUM(bf19_sale_piece_qty)', 'SUM(bf19_cust_num_cate)')}
                                            AS "19点前单件数",
        SUM(sale_article_num_cate)          AS 动销sku数,
        {safe_div('SUM(expect_outstock_amt) - SUM(out_stock_amt_cb)', 'SUM(expect_outstock_amt)')}
                                            AS 供应链预期毛利率,
        {safe_div('SUM(pre_profit_amt)', 'SUM(lp_sale_amt)')}
                                            AS 门店预期毛利率,
        {safe_div('SUM(pre_sale_amt) - SUM(pre_inbound_amount) - COALESCE(SUM(init_stock_amt),0) + COALESCE(SUM(end_stock_amt),0)',
                  'SUM(pre_sale_amt)')}
                                            AS 门店定价毛利率,
        {safe_div('SUM(out_stock_amt_cb)', 'SUM(purchase_weight)')}
                                            AS 采购价,
        {safe_div('SUM(total_sale_amt)', 'SUM(sales_weight)')}
                                            AS 平均售价,
        {safe_div('SUM(scm_promotion_amt_total)', 'SUM(scm_promotion_amt_total) + SUM(out_stock_pay_amt_notax)')}
                                            AS 供应链折让率,
        {safe_div('SUM(discount_amt)', 'SUM(lp_sale_amt)')}
                                            AS 折扣率,
        {safe_div('SUM(discount_amt_cate)', 'SUM(lp_sale_amt)')}
                                            AS 促销折扣率,
        {safe_div('SUM(hour_discount_amt)', 'SUM(lp_sale_amt)')}
                                            AS 时段折扣率,
        SUM(store_lost_amt)                 AS 损耗额,
        {safe_div('SUM(store_lost_amt)', 'SUM(lost_denominator)')}
                                            AS 损耗率,
        {safe_div('SUM(store_know_lost_amt)', 'SUM(lost_denominator)')}
                                            AS 已知损耗率,
        {safe_div('SUM(store_unknow_lost_amt)', 'SUM(lost_denominator)')}
                                            AS 未知损耗率,
        {safe_div('SUM(is_soldout_16)', 'SUM(is_soldout_16_salesku)')}
                                            AS 售罄率16,
        {safe_div('SUM(is_soldout_20)', 'SUM(is_soldout_20_salesku)')}
                                            AS 售罄率20,
        SUM(is_stock_sku)                   AS 上架sku数,
        {safe_div('SUM(sale_article_num_cate)', 'SUM(is_stock_sku)')}
                                            AS sku动销率,
        SUM(avg_7d_sale_qty)                AS "近7天日均销量",
        SUM(purchase_weight_kg)             AS 进货重量,
        SUM(init_stock_qty)                 AS 期初库存量,
        SUM(inbound_qty)                    AS 进货数量,
        SUM(end_stock_qty)                  AS 期末库存量,
        SUM(store_know_lost_amt)            AS 门店已知损耗额,
        SUM(store_unknow_lost_amt)          AS 门店未知损耗额,
        SUM(store_lost_qty)                 AS 损耗数量,
        {safe_div('SUM(store_lost_amt)', 'SUM(total_sale_amt)')}
                                            AS "损耗率_销售额",
        {safe_div('SUM(store_lost_qty)', 'SUM(init_stock_qty) + SUM(inbound_qty)')}
                                            AS "损耗率_数量",
        {safe_div('SUM(total_sale_amt)', 'SUM(sale_article_num_cate)')}
                                            AS 品效,
        {safe_div('SUM(init_stock_qty) + SUM(inbound_qty)', 'SUM(avg_7d_sale_qty)')}
                                            AS 周转天数,
        {safe_div('SUM(return_amt)', 'SUM(out_stock_pay_amt) + SUM(return_amt)')}
                                            AS 退货率,
        {safe_div('SUM(inbound_amount)', 'SUM(purchase_weight_kg)')}
                                            AS 门店进货价,
        {safe_div('SUM(lp_sale_amt)', 'SUM(sales_weight)')}
                                            AS 平均销售原价,
        SUM(lp_sale_amt)                    AS 原价销售额,
        SUM(pre_profit_amt)                 AS 门店预期毛利额,
        SUM(discount_amt)                   AS 折扣额,
        SUM(return_amt)                     AS 退货额,
        SUM(bf19_sale_piece_qty)            AS "19点前销售件数",
        SUM(sale_piece_qty)                 AS 销售件数
    FROM matnr_base
    GROUP BY
        store_flag, store_no, business_date, store_name, store_id,
        level_id, category_level1_description, category_level2_description,
        category_level3_description, spu_name, article_name,
        level_description, day_clear,
        manage_area_name, sap_area_name, city_description,
        week_no, week_start_date, week_end_date, month_wid, year_wid
    """

    # 首次建表
    conn.execute(f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} AS {select_sql} LIMIT 0")
    # 分区覆盖
    conn.execute(f"DELETE FROM {TARGET_TABLE} WHERE 日期 BETWEEN '{start}' AND '{end}'")
    conn.execute(f"INSERT INTO {TARGET_TABLE} {select_sql}")
    rows = conn.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()[0]
    print(f"[done] {TARGET_TABLE}: {rows} rows (date range: {start} ~ {end})")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python build_matnr_result.py <start_date> <end_date>")
        print("  e.g.: python build_matnr_result.py 2026-06-01 2026-06-09")
        sys.exit(1)

    start, end = sys.argv[1], sys.argv[2]
    conn = duckdb.connect(DB_PATH)
    try:
        build(conn, start, end)
    finally:
        conn.close()

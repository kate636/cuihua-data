"""
FM 结果层构建器

目标表: ads_business_analysis.strategy_fm_levels_result
粒度: 同 levels_sum

计算所有比率型 KPI，输出中文列名。
与 fm_结果.sql 保持一致。
"""

from __future__ import annotations

from ..connectors import DuckDBStore
from ..utils import get_logger

TARGET_DUCK_TABLE = "t_fm_levels_result"


class LevelsResultBuilder:
    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("LevelsResultBuilder")

    def build(self, start: str, end: str) -> None:
        self._log.info(f"building {TARGET_DUCK_TABLE}: {start} ~ {end}")
        self._duck.execute(f"DROP TABLE IF EXISTS {TARGET_DUCK_TABLE}")
        self._duck.execute(f"CREATE TABLE {TARGET_DUCK_TABLE} AS\n{self._build_sql(start, end)}")
        rows = self._duck.row_count(TARGET_DUCK_TABLE)
        self._log.info(f"{TARGET_DUCK_TABLE}: {rows} rows built")

    def _build_sql(self, start: str, end: str) -> str:
        def safe_div(num: str, den: str) -> str:
            return f"CASE WHEN COALESCE({den}, 0) = 0 THEN NULL ELSE ({num}) / ({den}) END"

        return f"""
        SELECT
            store_flag                          AS 标签,
            store_no                            AS 门店号,
            business_date                       AS 日期,
            CASE WHEN store_flag IS NULL THEN '广州' ELSE store_name END
                                                AS 门店名称,
            level_id                            AS 商品编码,
            CASE
                WHEN level_description = '门店'  THEN ''
                WHEN level_description = '大类'  THEN category_level1_description
                WHEN level_description = '中类'  THEN category_level2_description
                WHEN level_description = '小类'  THEN category_level3_description
                WHEN level_description IN ('SPU','黑白猪') THEN spu_name
                WHEN level_description = 'SKU'  THEN article_name
            END                                 AS 分类名称,
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
            AVG(full_link_article_profit)       AS 全链路毛利额,
            AVG(scm_fin_article_profit)         AS 供应链毛利额,
            AVG(article_profit_amt)             AS 门店毛利额,
            {safe_div('SUM(full_link_article_profit)', 'SUM(total_sale_amt)')}
                                                AS 全链路毛利率,
            {safe_div('SUM(scm_fin_article_profit)', 'SUM(out_stock_pay_amt_notax) + SUM(return_stock_pay_amt_notax)')}
                                                AS 供应链毛利率,
            {safe_div('SUM(article_profit_amt)', 'SUM(total_sale_amt)')}
                                                AS 门店毛利率,
            AVG(sales_weight)                   AS 销售重量,
            AVG(bf19_sales_weight)              AS "19点前销售重量",
            AVG(total_sale_qty)                 AS 销售数量,
            AVG(bf19_sale_qty)                  AS "19点前销售数量",
            AVG(inbound_amount)                 AS 进货额,
            AVG(total_sale_amt)                 AS 全天销售额,
            AVG(cust_num_cate)                  AS 全天来客数,
            {safe_div('AVG(total_sale_amt)', 'AVG(cust_num_cate)')}
                                                AS 全天客单价,
            AVG(bf19_sale_amt)                  AS "19点前销售额",
            AVG(bf19_cust_num_cate)             AS "19点前客数",
            {safe_div('AVG(bf19_sale_amt)', 'AVG(bf19_cust_num_cate)')}
                                                AS "19点前客单价",
            {safe_div('SUM(bf19_sale_amt)', 'SUM(bf19_sale_piece_qty)')}
                                                AS "19点前件单价",
            {safe_div('SUM(bf19_sale_piece_qty)', 'SUM(bf19_cust_num_cate)')}
                                                AS "19点前单件数",
            AVG(sale_article_num_cate)          AS 动销sku数,
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
            AVG(store_lost_amt)                 AS 损耗额,
            {safe_div('SUM(store_lost_amt)', 'SUM(lost_denominator)')}
                                                AS 损耗率,
            {safe_div('SUM(store_know_lost_amt)', 'SUM(lost_denominator)')}
                                                AS 已知损耗率,
            {safe_div('SUM(store_unknow_lost_amt)', 'SUM(lost_denominator)')}
                                                AS 未知损耗率,
            AVG(is_soldout_16)                  AS 售罄率16,
            AVG(is_soldout_20)                  AS 售罄率20,
            SUM(is_stock_sku)                   AS 上架sku数,
            AVG(avg_7d_sale_qty)                AS "近7天日均销量"
        FROM t_fm_levels_sum
        WHERE business_date BETWEEN '{start}' AND '{end}'
        GROUP BY
            store_flag, store_no, business_date, store_name, store_id,
            level_id, category_level1_description, category_level2_description,
            category_level3_description, spu_name, article_name,
            level_description, day_clear,
            manage_area_name, sap_area_name, city_description,
            week_no, week_start_date, week_end_date, month_wid, year_wid
        """

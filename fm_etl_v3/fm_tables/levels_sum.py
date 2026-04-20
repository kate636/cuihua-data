"""
FM 分类汇总底表构建器

目标表: ads_business_analysis.strategy_fm_levels_sum
粒度: 门店 × 日期 × 多层级聚合 (门店/大类/中类/小类/SPU/黑白猪/SKU 级别)

输入: DuckDB t_fm_sku_dim + t_fm_cust
输出: DuckDB t_fm_levels_sum → StarRocks strategy_fm_levels_sum

与 fm_分类汇总.sql 的 UNION ALL 逻辑对应。
"""

from __future__ import annotations

from ..connectors import DuckDBStore
from ..utils import get_logger

TARGET_DUCK_TABLE = "t_fm_levels_sum"

# (level_description, level_id_col, category_dim_cols)
# category_dim_cols: 在 GROUP BY 中保留哪些分类列（其余置空）
_LEVELS = [
    ("门店",   None,                   [], []),
    ("大类",   "category_level1_id",   ["category_level1_id", "category_level1_description"], []),
    ("中类",   "category_level2_id",   ["category_level1_id", "category_level1_description",
                                         "category_level2_id", "category_level2_description"], []),
    ("小类",   "category_level3_id",   ["category_level1_id", "category_level1_description",
                                         "category_level2_id", "category_level2_description",
                                         "category_level3_id", "category_level3_description"], []),
    ("SPU",    "spu_id",               ["category_level1_id", "category_level1_description",
                                         "category_level2_id", "category_level2_description",
                                         "category_level3_id", "category_level3_description",
                                         "spu_id", "spu_name"], []),
    ("黑白猪", "blackwhite_pig_name",  ["category_level1_id", "category_level1_description",
                                         "category_level2_id", "category_level2_description",
                                         "category_level3_id", "category_level3_description",
                                         "spu_id", "spu_name", "blackwhite_pig_name"], []),
    ("SKU",    "article_id",           ["category_level1_id", "category_level1_description",
                                         "category_level2_id", "category_level2_description",
                                         "category_level3_id", "category_level3_description",
                                         "spu_id", "spu_name", "blackwhite_pig_name",
                                         "article_id", "article_name"], []),
]

# 固定维度列 (每个层级都保留)
_DIM_COLS = [
    "business_date", "week_no", "week_start_date", "week_end_date", "month_wid", "year_wid",
    "manage_area_name", "sap_area_name", "city_description",
    "store_id", "store_name", "store_flag", "store_no", "day_clear",
]

# 所有可能的类别列
_ALL_CAT_COLS = [
    "category_level1_id", "category_level1_description",
    "category_level2_id", "category_level2_description",
    "category_level3_id", "category_level3_description",
    "spu_id", "spu_name", "blackwhite_pig_name",
    "article_id", "article_name",
]

# 指标列 (SUM)
_METRIC_COLS = [
    "full_link_article_profit", "scm_fin_article_profit", "article_profit_amt",
    "pre_profit_amt", "sales_weight", "bf19_sales_weight", "total_sale_qty",
    "bf19_sale_qty", "inbound_amount", "purchase_weight", "total_sale_amt",
    "bf19_sale_amt", "expect_outstock_amt", "out_stock_amt_cb", "pre_sale_amt",
    "pre_inbound_amount", "scm_promotion_amt_total", "lp_sale_amt", "discount_amt",
    "hour_discount_amt", "discount_amt_cate", "store_lost_amt", "return_amt",
    "out_stock_pay_amt", "out_stock_pay_amt_notax", "return_stock_pay_amt_notax",
    "bf19_sale_piece_qty", "lost_denominator", "end_stock_qty", "avg_7d_sale_qty",
    "init_stock_amt", "end_stock_amt", "init_stock_qty", "inbound_qty",
    "is_stock_sku", "store_lost_qty", "sale_piece_qty",
    "store_know_lost_amt", "store_unknow_lost_amt",
]


class LevelsSumBuilder:
    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("LevelsSumBuilder")

    def build(self, start: str, end: str) -> None:
        self._log.info(f"building {TARGET_DUCK_TABLE}: {start} ~ {end}")
        self._duck.execute(f"DROP TABLE IF EXISTS {TARGET_DUCK_TABLE}")
        self._duck.execute(f"CREATE TABLE {TARGET_DUCK_TABLE} AS\n{self._build_sql(start, end)}")
        rows = self._duck.row_count(TARGET_DUCK_TABLE)
        self._log.info(f"{TARGET_DUCK_TABLE}: {rows} rows built")

    def _build_sql(self, start: str, end: str) -> str:
        parts = []
        metrics = ", ".join([f"SUM({c}) AS {c}" for c in _METRIC_COLS])
        soldout = (
            "SUM(is_soldout_16) AS is_soldout_16, "
            "SUM(is_soldout_20) AS is_soldout_20, "
            "SUM(CASE WHEN is_soldout_16 IS NOT NULL THEN 1 END) AS is_soldout_16_salesku, "
            "SUM(CASE WHEN is_soldout_20 IS NOT NULL THEN 1 END) AS is_soldout_20_salesku"
        )
        for level_desc, level_id_col, keep_cols, _ in _LEVELS:
            # day_clear 取两遍：原值(0/1) + '2'(日清+非日清合计)
            for dc_mode in ("exact", "total"):
                sel_dim = []
                grp_cols = []

                for col in _DIM_COLS:
                    if col == "day_clear":
                        if dc_mode == "exact":
                            sel_dim.append("day_clear")
                            grp_cols.append("day_clear")
                        else:
                            # 合计行：固定输出 '2'，不加入 GROUP BY
                            sel_dim.append("'2' AS day_clear")
                    else:
                        sel_dim.append(col)
                        grp_cols.append(col)

                for col in _ALL_CAT_COLS:
                    if col in keep_cols:
                        sel_dim.append(col)
                        grp_cols.append(col)
                    else:
                        sel_dim.append(f"'' AS {col}")

                level_id_expr = level_id_col if level_id_col else "''"
                grp = ", ".join(grp_cols)
                sel_str = ", ".join(sel_dim)

                parts.append(f"""
                SELECT
                    {sel_str},
                    '{level_desc}' AS level_description,
                    {level_id_expr} AS level_id,
                    {metrics},
                    {soldout},
                    0 AS cust_num_cate,
                    0 AS bf19_cust_num_cate,
                    0 AS sale_article_num_cate
                FROM t_fm_sku_dim
                WHERE business_date BETWEEN '{start}' AND '{end}'
                GROUP BY {grp}
                """)

        return "\nUNION ALL\n".join(parts)

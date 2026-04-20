"""
FM 商品维度底表构建器

目标表: ads_business_analysis.strategy_fm_flag_sku_di
粒度: 门店 × 日期 × article_id × day_clear

输入: DuckDB 中的 t_atomic_wide + t_calc_* + dim_* 表
输出: DuckDB t_fm_sku_dim → StarRocks strategy_fm_flag_sku_di
"""

from __future__ import annotations

from ..connectors import DuckDBStore
from ..utils import get_logger

TARGET_DUCK_TABLE = "t_fm_sku_dim"


class SkuDimBuilder:
    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("SkuDimBuilder")

    def build(self, start: str, end: str) -> None:
        """构建 SKU 维度底表，结果留在 DuckDB/MotherDuck。"""
        self._log.info(f"building {TARGET_DUCK_TABLE}: {start} ~ {end}")
        self._duck.execute(f"DROP TABLE IF EXISTS {TARGET_DUCK_TABLE}")
        self._duck.execute(f"CREATE TABLE {TARGET_DUCK_TABLE} AS\n{self._build_sql(start, end)}")
        rows = self._duck.row_count(TARGET_DUCK_TABLE)
        self._log.info(f"{TARGET_DUCK_TABLE}: {rows} rows built")

    def _build_sql(self, start: str, end: str) -> str:
        """
        类别重映射逻辑与 fm_商品维度底表.sql 保持一致。
        7天滑动销量通过 DuckDB 窗口函数实现。
        """
        return f"""
        WITH base AS (
            SELECT
                w.store_id,
                w.business_date,
                w.article_id,
                w.day_clear,
                -- 销售数量
                w.sale_qty                              AS total_sale_qty,
                w.bf19_sale_qty,
                w.sale_piece_qty,
                w.bf19_sale_piece_qty,
                w.online_sale_qty,
                w.offline_sale_qty,
                w.bf12_sale_qty,
                -- 销售金额
                w.sale_amt                              AS total_sale_amt,
                w.bf19_sale_amt,
                w.original_price_sale_amt               AS lp_sale_amt,
                w.discount_amt,
                w.hour_discount_amt,
                w.discount_amt - w.hour_discount_amt    AS discount_amt_cate,
                w.member_discount_amt,
                w.return_sale_qty                       AS return_qty,
                w.return_sale_amt                       AS return_amt,
                w.member_sale_amt,
                w.bf19_member_sale_amt,
                w.online_sale_qty * 0                   AS online_cust_num,  -- placeholder, from cust table
                -- 进货
                inv.receive_qty                         AS inbound_qty,
                amt.receive_amt                         AS inbound_amount,
                amt.purchase_weight,
                -- 库存
                inv.init_stock_qty,
                inv.end_stock_qty,
                amt.init_stock_amt,
                amt.end_stock_amt,
                -- 损耗
                amt.lost_qty                            AS store_lost_qty,
                amt.lost_amt                            AS store_lost_amt,
                amt.know_lost_amt                       AS store_know_lost_amt,
                amt.unknow_lost_amt                     AS store_unknow_lost_amt,
                -- 供应链
                amt.out_stock_pay_amt,
                amt.out_stock_pay_amt_notax,
                amt.out_stock_amt_cb,
                amt.return_stock_pay_amt_notax,
                amt.scm_promotion_amt_total,
                amt.expect_outstock_amt,
                -- 毛利
                pft.profit_amt                          AS article_profit_amt,
                pft.pre_profit_amt,
                pft.scm_fin_article_profit,
                pft.full_link_article_profit,
                pft.sale_cost_amt,
                pft.pre_sale_amt,
                pft.pre_inbound_amount,
                -- 损耗率分母
                amt.receive_amt + amt.init_stock_amt    AS lost_denominator,
                -- 重量 (千克品种 qty=重量，否则 qty×unit_weight)
                CASE WHEN g.sale_unit = '千克' THEN w.sale_qty
                     ELSE w.sale_qty * CASE WHEN COALESCE(g.unit_weight,0) = 0 THEN 1 ELSE g.unit_weight END
                END                                     AS sales_weight,
                CASE WHEN g.sale_unit = '千克' THEN w.bf19_sale_qty
                     ELSE w.bf19_sale_qty * CASE WHEN COALESCE(g.unit_weight,0) = 0 THEN 1 ELSE g.unit_weight END
                END                                     AS bf19_sales_weight,
                -- 上架 SKU 标识
                CASE
                    WHEN sal.article_id IS NOT NULL
                     AND (w.sale_amt > 0 OR (w.sale_amt = 0 AND (amt.end_stock_amt <> 0 OR amt.lost_amt <> 0)))
                    THEN 1 ELSE 0
                END                                     AS is_stock_sku,
                -- 售罄
                w.last_sysdate,
                -- 末交易时间
                CASE
                    WHEN sal.article_id IS NULL THEN NULL
                    WHEN inv.end_stock_qty = 0 AND SUBSTR(CAST(w.last_sysdate AS VARCHAR), 12, 8) < '16:00:00' THEN 1
                    WHEN w.last_sysdate IS NOT NULL OR inv.end_stock_qty > 0 THEN 0
                    ELSE NULL
                END                                     AS is_soldout_16,
                CASE
                    WHEN sal.article_id IS NULL THEN NULL
                    WHEN inv.end_stock_qty = 0 AND SUBSTR(CAST(w.last_sysdate AS VARCHAR), 12, 8) < '20:00:00' THEN 1
                    WHEN w.last_sysdate IS NOT NULL OR inv.end_stock_qty > 0 THEN 0
                    ELSE NULL
                END                                     AS is_soldout_20,
                -- 门店维度
                COALESCE(sp.manage_area_name, '')       AS manage_area_name,
                COALESCE(sp.sap_area_name, '')          AS sap_area_name,
                COALESCE(sp.city_description, '')       AS city_description,
                COALESCE(sp.store_name, '')             AS store_name,
                COALESCE(ch.store_flag, '')             AS store_flag,
                COALESCE(ch.store_no, '')               AS store_no,
                -- 商品维度 (类别重映射)
                CASE
                    WHEN g.category_level2_description IN ('蛋类','烘焙类') THEN ''
                    WHEN g.category_level2_description IN ('冷藏奶制品类','饮料类') THEN ''
                    WHEN g.category_level1_description = '肉禽蛋类' AND g.category_level2_description <> '蛋类' THEN ''
                    WHEN RIGHT(g.category_level3_description, 2) = '熟食' THEN ''
                    WHEN g.category_level1_description IN ('冷藏及加工类','预制菜') THEN ''
                    ELSE g.category_level1_id
                END                                     AS category_level1_id,
                CASE
                    WHEN g.category_level2_description IN ('蛋类','烘焙类')
                        THEN g.category_level2_description
                    WHEN g.category_level2_description IN ('冷藏奶制品类','饮料类')
                        THEN '乳制品及水饮类'
                    WHEN g.category_level1_description = '肉禽蛋类' AND g.category_level2_description <> '蛋类'
                        THEN '肉禽类'
                    WHEN RIGHT(g.category_level3_description, 2) = '熟食'
                        THEN '熟食类'
                    WHEN g.category_level1_description IN ('冷藏及加工类','预制菜')
                        THEN '冷藏加工及预制菜类'
                    ELSE g.category_level1_description
                END                                     AS category_level1_description,
                COALESCE(g.category_level2_id, '')      AS category_level2_id,
                COALESCE(g.category_level2_description,'') AS category_level2_description,
                COALESCE(g.category_level3_id, '')      AS category_level3_id,
                COALESCE(g.category_level3_description,'') AS category_level3_description,
                COALESCE(g.spu_id, '')                  AS spu_id,
                COALESCE(g.spu_name, '')                AS spu_name,
                COALESCE(g.blackwhite_pig_name, '')     AS blackwhite_pig_name,
                COALESCE(g.article_name, '')            AS article_name,
                -- 日历维度
                COALESCE(cal.week_no, '')               AS week_no,
                cal.week_start_date,
                cal.week_end_date,
                COALESCE(cal.month_wid, '')             AS month_wid,
                COALESCE(cal.year_wid, '')              AS year_wid,
                -- 可订可售
                CASE WHEN sal.article_id IS NOT NULL THEN 1 ELSE 0 END AS saleable
            FROM t_atomic_wide w
            JOIN t_calc_inventory inv
                ON w.store_id = inv.store_id AND w.business_date = inv.business_date AND w.article_id = inv.article_id
            JOIN t_calc_amounts amt
                ON w.store_id = amt.store_id AND w.business_date = amt.business_date AND w.article_id = amt.article_id
            JOIN t_calc_profit pft
                ON w.store_id = pft.store_id AND w.business_date = pft.business_date AND w.article_id = pft.article_id
            LEFT JOIN dim_goods g ON w.article_id = g.article_id
            LEFT JOIN dim_store_profile sp ON w.store_id = sp.store_id
            LEFT JOIN dim_chdj_store_info ch ON w.store_id = ch.store_id
            LEFT JOIN dim_calendar cal ON w.business_date = cal.business_date
            LEFT JOIN dim_saleable sal ON w.store_id = sal.store_id AND w.article_id = sal.article_id
            WHERE w.business_date BETWEEN '{start}' AND '{end}'
        ),

        -- 7天滑动销量
        rolling AS (
            SELECT
                store_id,
                business_date,
                article_id,
                day_clear,
                AVG(total_sale_qty) OVER (
                    PARTITION BY store_id, article_id, day_clear
                    ORDER BY business_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                )                                       AS avg_7d_sale_qty
            FROM base
        )

        SELECT
            b.*,
            COALESCE(r.avg_7d_sale_qty, 0)             AS avg_7d_sale_qty
        FROM base b
        LEFT JOIN rolling r
            ON b.store_id = r.store_id
            AND b.business_date = r.business_date
            AND b.article_id = r.article_id
            AND b.day_clear = r.day_clear
        """

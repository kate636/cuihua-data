"""
FM 商品维度底表 (v10)

粒度: 门店 × 日期 × article_id × day_clear
输入: t_atomic_wide + t_calc_stock + t_calc_profit + dim_*
输出: t_fm_sku_dim

v10 变更:
  - 类别重映射从 SQL CASE WHEN 移到 Python
  - inbound 统一从 t_calc_stock 取
"""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd
from ..connectors import DuckDBStore
from ..utils import get_logger

TARGET_DUCK_TABLE = "t_fm_sku_dim"


def _remap_category(row):
    """类别重映射 — 与 v3 sku_dim SQL CASE WHEN 完全一致"""
    c2 = str(row.get('category_level2_description', ''))
    c1_desc = str(row.get('category_level1_description', ''))
    c3 = str(row.get('category_level3_description', ''))
    c1_id = str(row.get('category_level1_id', ''))

    # category_level1_id
    if c2 in ('蛋类', '烘焙类'):
        new_id = ''
    elif c2 in ('冷藏奶制品类', '饮料类'):
        new_id = ''
    elif c1_desc == '肉禽蛋类' and c2 != '蛋类':
        new_id = ''
    elif c3.endswith('熟食'):
        new_id = ''
    elif c1_desc in ('冷藏及加工类', '预制菜'):
        new_id = ''
    else:
        new_id = c1_id

    # category_level1_description
    if c2 in ('蛋类', '烘焙类'):
        new_desc = c2
    elif c2 in ('冷藏奶制品类', '饮料类'):
        new_desc = '乳制品及水饮类'
    elif c1_desc == '肉禽蛋类' and c2 != '蛋类':
        new_desc = '肉禽类'
    elif c3.endswith('熟食'):
        new_desc = '熟食类'
    elif c1_desc in ('冷藏及加工类', '预制菜'):
        new_desc = '冷藏加工及预制菜类'
    else:
        new_desc = c1_desc

    return new_id, new_desc


class SkuDimBuilder:
    def __init__(self, duck: DuckDBStore):
        self._duck = duck
        self._log = get_logger("SkuDimBuilder")

    def build(self, start: str, end: str) -> None:
        self._log.info(f"building {TARGET_DUCK_TABLE}: {start} ~ {end}")
        conn = self._duck._conn

        # 1. 加载主数据 (去重)
        wide_df = conn.execute(f"""
            SELECT * FROM (
                SELECT
                    w.store_id, w.business_date, w.article_id, w.day_clear,
                    w.sale_qty AS total_sale_qty, w.bf19_sale_qty,
                    w.sale_piece_qty, w.bf19_sale_piece_qty,
                    w.online_sale_qty, w.offline_sale_qty, w.bf12_sale_qty,
                    w.sale_amt AS total_sale_amt, w.bf19_sale_amt,
                    w.original_price_sale_amt AS lp_sale_amt,
                    w.discount_amt, w.hour_discount_amt,
                    w.member_discount_amt,
                    w.return_sale_qty AS return_qty,
                    w.return_sale_amt AS return_amt,
                    w.member_sale_amt, w.bf19_member_sale_amt,
                    w.last_sysdate,
                    ROW_NUMBER() OVER (PARTITION BY w.store_id, w.business_date, w.article_id, w.day_clear) AS rn
                FROM t_atomic_wide w
                WHERE w.business_date BETWEEN '{start}' AND '{end}'
            ) WHERE rn = 1
        """).df()

        # 2. 加载 t_calc_stock (仅目标日期范围, 去重)
        stock_df = conn.execute(f"""
            SELECT * FROM (
                SELECT
                    store_id, business_date, article_id, day_clear,
                    receive_qty, receive_amt,
                    init_stock_qty, init_stock_amt,
                    end_stock_qty, end_stock_amt,
                    know_lost_amt, unknow_lost_amt,
                    lost_qty, lost_amt,
                    effective_unit_cost, cost_source,
                    out_stock_pay_amt, out_stock_pay_amt_notax,
                    out_stock_amt_cb, return_stock_pay_amt_notax,
                    scm_promotion_amt_total, expect_outstock_amt,
                    purchase_weight,
                    sale_amt,
                    ROW_NUMBER() OVER (PARTITION BY store_id, business_date, article_id, day_clear) AS rn
                FROM t_calc_stock
                WHERE business_date BETWEEN '{start}' AND '{end}'
            ) WHERE rn = 1
        """).df()

        # 3. 加载 t_calc_profit (仅目标日期范围, 去重)
        profit_df = conn.execute(f"""
            SELECT * FROM (
                SELECT
                    store_id, business_date, article_id, day_clear,
                    profit_amt, sale_cost_amt, pre_profit_amt,
                    scm_fin_article_profit, full_link_article_profit,
                    pre_sale_amt, pre_inbound_amount,
                    allowance_amt_profit,
                    ROW_NUMBER() OVER (PARTITION BY store_id, business_date, article_id, day_clear) AS rn
                FROM t_calc_profit
                WHERE business_date BETWEEN '{start}' AND '{end}'
            ) WHERE rn = 1
        """).df()

        # 4. 加载维度表
        goods_df = conn.execute("""
            SELECT DISTINCT article_id, article_name,
                category_level1_id, category_level1_description,
                category_level2_id, category_level2_description,
                category_level3_id, category_level3_description,
                sale_unit, unit_weight,
                spu_id, spu_name, blackwhite_pig_name
            FROM dim_goods
        """).df()

        store_profile_df = conn.execute("""
            SELECT DISTINCT store_id, manage_area_name, sap_area_name,
                   city_description, store_name
            FROM dim_store_profile
        """).df()

        chdj_df = conn.execute("SELECT store_id, store_flag, store_no FROM dim_chdj_store_info").df()
        cal_df = conn.execute("""
            SELECT DISTINCT business_date, week_no, week_start_date,
                   week_end_date, month_wid, year_wid
            FROM dim_calendar
        """).df()
        saleable_df = conn.execute("""
            SELECT DISTINCT store_id, article_id, 1 AS is_saleable FROM dim_saleable
        """).df()

        # 5. Python merge
        df = wide_df
        df = df.merge(stock_df,
                      on=['store_id', 'business_date', 'article_id', 'day_clear'],
                      how='left')
        df = df.merge(profit_df,
                      on=['store_id', 'business_date', 'article_id', 'day_clear'],
                      how='left')
        df = df.merge(goods_df, on='article_id', how='left')
        df = df.merge(store_profile_df, on='store_id', how='left')
        df = df.merge(chdj_df, on='store_id', how='left')
        df = df.merge(cal_df, on='business_date', how='left')
        df = df.merge(saleable_df, on=['store_id', 'article_id'], how='left')

        # 6. Python: fill defaults
        for c in df.columns:
            if df[c].dtype in ('float64', 'float32'):
                df[c] = df[c].fillna(0)

        for c in ['manage_area_name', 'sap_area_name', 'city_description',
                   'store_name', 'store_flag', 'store_no', 'week_no',
                   'week_start_date', 'week_end_date', 'month_wid', 'year_wid',
                   'article_name', 'category_level1_id', 'category_level1_description',
                   'category_level2_id', 'category_level2_description',
                   'category_level3_id', 'category_level3_description',
                   'spu_id', 'spu_name', 'blackwhite_pig_name',
                   'sale_unit', 'cost_source']:
            if c in df.columns:
                df[c] = df[c].fillna('')

        # 7. Python: 类别重映射 (pandas 向量化, 避免 iterrows)
        c2 = df['category_level2_description'].fillna('')
        c1_desc = df['category_level1_description'].fillna('')
        c3 = df['category_level3_description'].fillna('')
        c1_id = df['category_level1_id'].fillna('')

        is_egg_bake = c2.isin(['蛋类', '烘焙类'])
        is_dairy_drink = c2.isin(['冷藏奶制品类', '饮料类'])
        is_meat = (c1_desc == '肉禽蛋类') & (c2 != '蛋类')
        is_cooked = c3.str.endswith('熟食').fillna(False)
        is_cold_prep = c1_desc.isin(['冷藏及加工类', '预制菜'])
        is_remapped = is_egg_bake | is_dairy_drink | is_meat | is_cooked | is_cold_prep

        df['category_level1_id_remap'] = c1_id.where(~is_remapped, '')
        df['category_level1_description_remap'] = (
            c2.where(is_egg_bake,
            np.where(is_dairy_drink, '乳制品及水饮类',
            np.where(is_meat, '肉禽类',
            np.where(is_cooked, '熟食类',
            np.where(is_cold_prep, '冷藏加工及预制菜类',
                     c1_desc))))))

        # 8. Python: 衍生字段
        df['discount_amt_cate'] = df['discount_amt'] - df['hour_discount_amt']
        df['online_cust_num'] = 0  # placeholder

        # 重量
        is_kg = (df['sale_unit'].fillna('') == '千克')
        unit_w = df['unit_weight'].fillna(0)
        unit_w = np.where(unit_w == 0, 1, unit_w)
        df['sales_weight'] = np.where(is_kg, df['total_sale_qty'],
                                       df['total_sale_qty'] * unit_w)
        df['bf19_sales_weight'] = np.where(is_kg, df['bf19_sale_qty'],
                                            df['bf19_sale_qty'] * unit_w)

        # is_stock_sku
        has_saleable = df['is_saleable'].notna()
        has_activity = (df['sale_amt'] > 0) | (
            (df['sale_amt'] == 0) & ((df['end_stock_amt'] != 0) | (df['lost_amt'] != 0)))
        df['is_stock_sku'] = np.where(has_saleable & has_activity, 1, 0)

        # is_soldout
        last_time = df['last_sysdate'].astype(str).str[11:19]
        df['is_soldout_16'] = np.where(
            ~has_saleable, None,
            np.where((df['end_stock_qty'] == 0) & (last_time < '16:00:00'), 1,
                     np.where(df['last_sysdate'].notna() | (df['end_stock_qty'] > 0), 0, None)))
        df['is_soldout_20'] = np.where(
            ~has_saleable, None,
            np.where((df['end_stock_qty'] == 0) & (last_time < '20:00:00'), 1,
                     np.where(df['last_sysdate'].notna() | (df['end_stock_qty'] > 0), 0, None)))

        # lost_denominator
        df['lost_denominator'] = df['receive_amt'] + df['init_stock_amt']

        # saleable flag
        df['saleable'] = np.where(has_saleable, 1, 0)

        # 9. 写出结果（分区覆盖，保留历史数据）
        out_cols = {
            'store_id': df['store_id'],
            'business_date': df['business_date'],
            'article_id': df['article_id'],
            'day_clear': df['day_clear'],
            'total_sale_qty': df['total_sale_qty'],
            'bf19_sale_qty': df['bf19_sale_qty'],
            'sale_piece_qty': df['sale_piece_qty'],
            'bf19_sale_piece_qty': df['bf19_sale_piece_qty'],
            'online_sale_qty': df['online_sale_qty'],
            'offline_sale_qty': df['offline_sale_qty'],
            'bf12_sale_qty': df['bf12_sale_qty'],
            'total_sale_amt': df['total_sale_amt'],
            'bf19_sale_amt': df['bf19_sale_amt'],
            'lp_sale_amt': df['lp_sale_amt'],
            'discount_amt': df['discount_amt'],
            'hour_discount_amt': df['hour_discount_amt'],
            'discount_amt_cate': df['discount_amt_cate'],
            'member_discount_amt': df['member_discount_amt'],
            'return_qty': df['return_qty'],
            'return_amt': df['return_amt'],
            'member_sale_amt': df['member_sale_amt'],
            'bf19_member_sale_amt': df['bf19_member_sale_amt'],
            'online_cust_num': df['online_cust_num'],
            'inbound_qty': df['receive_qty'],
            'inbound_amount': df['receive_amt'],
            'purchase_weight': df['purchase_weight'],
            'init_stock_qty': df['init_stock_qty'],
            'end_stock_qty': df['end_stock_qty'],
            'init_stock_amt': df['init_stock_amt'],
            'end_stock_amt': df['end_stock_amt'],
            'store_lost_qty': df['lost_qty'],
            'store_lost_amt': df['lost_amt'],
            'store_know_lost_amt': df['know_lost_amt'],
            'store_unknow_lost_amt': df['unknow_lost_amt'],
            'out_stock_pay_amt': df['out_stock_pay_amt'],
            'out_stock_pay_amt_notax': df['out_stock_pay_amt_notax'],
            'out_stock_amt_cb': df['out_stock_amt_cb'],
            'return_stock_pay_amt_notax': df['return_stock_pay_amt_notax'],
            'scm_promotion_amt_total': df['scm_promotion_amt_total'],
            'expect_outstock_amt': df['expect_outstock_amt'],
            'article_profit_amt': df['profit_amt'],
            'supply_chain_profit_qty': 0,
            'pre_profit_amt': df['pre_profit_amt'],
            'scm_fin_article_profit': df['scm_fin_article_profit'],
            'full_link_article_profit': df['full_link_article_profit'],
            'sale_cost_amt': df['sale_cost_amt'],
            'pre_sale_amt': df['pre_sale_amt'],
            'pre_inbound_amount': df['pre_inbound_amount'],
            'cost_source': df['cost_source'],
            'effective_unit_cost': df['effective_unit_cost'],
            'lost_denominator': df['lost_denominator'],
            'sales_weight': df['sales_weight'],
            'bf19_sales_weight': df['bf19_sales_weight'],
            'is_stock_sku': df['is_stock_sku'],
            'last_sysdate': df['last_sysdate'],
            'is_soldout_16': df['is_soldout_16'],
            'is_soldout_20': df['is_soldout_20'],
            'manage_area_name': df['manage_area_name'],
            'sap_area_name': df['sap_area_name'],
            'city_description': df['city_description'],
            'store_name': df['store_name'],
            'store_flag': df['store_flag'],
            'store_no': df['store_no'],
            'category_level1_id': df['category_level1_id_remap'],
            'category_level1_description': df['category_level1_description_remap'],
            'category_level2_id': df['category_level2_id'],
            'category_level2_description': df['category_level2_description'],
            'category_level3_id': df['category_level3_id'],
            'category_level3_description': df['category_level3_description'],
            'spu_id': df['spu_id'],
            'spu_name': df['spu_name'],
            'blackwhite_pig_name': df['blackwhite_pig_name'],
            'article_name': df['article_name'],
            'week_no': df['week_no'],
            'week_start_date': df['week_start_date'],
            'week_end_date': df['week_end_date'],
            'month_wid': df['month_wid'],
            'year_wid': df['year_wid'],
            'saleable': df['saleable'],
            'avg_7d_sale_qty': 0.0,
        }
        out_df = pd.DataFrame(out_cols)
        # 首次建表（空结构）
        conn.execute(f"CREATE TABLE IF NOT EXISTS {TARGET_DUCK_TABLE} AS SELECT * FROM out_df LIMIT 0")
        # 分区覆盖
        conn.execute(f"DELETE FROM {TARGET_DUCK_TABLE} WHERE business_date BETWEEN '{start}' AND '{end}'")
        conn.execute(f"INSERT INTO {TARGET_DUCK_TABLE} SELECT * FROM out_df")

        # 7-day rolling average (全量重算，window function 需要全部历史数据)
        conn.execute(f"""
            CREATE OR REPLACE TABLE {TARGET_DUCK_TABLE} AS
            SELECT
                * EXCLUDE (avg_7d_sale_qty),
                COALESCE(
                    AVG(total_sale_qty) OVER (
                        PARTITION BY store_id, article_id, day_clear
                        ORDER BY business_date
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ), 0) AS avg_7d_sale_qty
            FROM {TARGET_DUCK_TABLE}
        """)

        rows = self._duck.row_count(TARGET_DUCK_TABLE)
        self._log.info(f"{TARGET_DUCK_TABLE}: {rows} rows built")

"""
FM 客数底表 (v0.10)

粒度: 门店 × 日期 × day_clear × level_description × level_id
输入: strategy_fm_sales_di (via API) + dim_goods
输出: t_fm_cust

v0.10 修复 (A16): Python JOIN dim_goods 后排除 70-77 品类物料
v0.10 修复 (B1): 分类映射从 SQL IF 嵌套迁移到 Python pandas（避免 QDM API 12 层嵌套失败）
"""

from __future__ import annotations

import duckdb
import numpy as np
from ..connectors import ApiConnector, DuckDBStore
from ..utils import get_logger

TARGET_DUCK_TABLE = "t_fm_cust"

_LEVELS = [
    ("门店",   "",                   []),
    ("大类",   "category_level1_id", ["category_level1_id"]),
    ("中类",   "category_level2_id", ["category_level1_id", "category_level2_id"]),
    ("小类",   "category_level3_id", ["category_level1_id", "category_level2_id", "category_level3_id"]),
    ("SPU",    "spu_id",             ["spu_id"]),
    ("黑白猪", "blackwhite_pig_name",  ["blackwhite_pig_name"]),
]

_EXCLUDED_CATS = ('70', '71', '72', '73', '74', '75', '76', '77')


def _remap_category_level1(df):
    """品类重映射 — master-data v2.1 中分类映射，与 sku_dim.py 完全一致。

    在 DataFrame 上直接修改，新增 category_level1_id 列（存储重映射后的品类名称）。
    """
    c2 = df['category_level2_description'].fillna('')
    c1 = df['category_level1_description'].fillna('')

    cond_egg         = (c2 == '蛋类')
    cond_bake        = (c2 == '烘焙类')
    cond_dairy       = (c2 == '冷藏奶制品类')
    cond_drink       = (c2.isin(['酒类', '饮料类']))
    cond_staple      = (c2.isin(['方便速食类', '调味品类', '粮油副食类']))
    cond_snack       = (c2 == '休闲零食类')
    cond_daily       = (c2 == '日杂用品类')
    cond_ice         = (c2 == '冰品类')
    cond_beef_mutton = (c2.isin(['牛肉类', '羊肉类']))
    cond_poultry     = (c2.isin(['鸡类', '鸭类', '其他禽类']))
    cond_cooked_l2   = (c2.isin(['即烹类', '即热类']))
    cond_cold        = (c1.isin(['冷藏及加工类', '预制菜', '冷藏加工及预制菜类']))

    desc = c1.copy()
    desc = np.where(cond_cooked_l2,  '熟食类', desc)
    desc = np.where(cond_poultry,     '禽类', desc)
    desc = np.where(cond_beef_mutton, '牛羊类', desc)
    desc = np.where(cond_daily,       '日杂用品类', desc)
    desc = np.where(cond_ice,         '冷冻类', desc)
    desc = np.where(cond_snack,       '休闲食品类', desc)
    desc = np.where(cond_staple,      '基础食品类', desc)
    desc = np.where(cond_drink,       '水饮类', desc)
    desc = np.where(cond_dairy,       '冷藏乳品类', desc)
    desc = np.where(cond_bake,        '烘焙类', desc)
    desc = np.where(cond_egg,         '蛋类', desc)
    desc = np.where(cond_cold & ~(cond_cooked_l2 | cond_egg | cond_bake | cond_dairy |
                     cond_drink | cond_staple | cond_snack | cond_daily | cond_ice |
                     cond_beef_mutton | cond_poultry), '冷藏加工及预制菜类', desc)

    df['category_level1_id'] = desc


class CustBuilder:
    def __init__(self, duck: DuckDBStore, api: ApiConnector):
        self._duck = duck
        self._api  = api
        self._log  = get_logger("CustBuilder")

    def build(self, start: str, end: str, yesterday: str) -> None:
        self._log.info(f"building {TARGET_DUCK_TABLE}: {start} ~ {end}")
        self._extract_orders(start, end, yesterday)
        self._compute_cust(start, end)
        rows = self._duck.row_count(TARGET_DUCK_TABLE)
        self._log.info(f"{TARGET_DUCK_TABLE}: {rows} rows built")

    def _extract_orders(self, start: str, end: str, yesterday: str) -> None:
        """从 API 拉订单，Python 端 JOIN dim_goods + 品类过滤 (A16) + 分类映射 (v2.1)

        strategy_fm_dim_goods 在 StarRocks 中只存最新一天快照，API SQL 中 JOIN
        会因为 inc_day 不匹配导致所有分类字段为 NULL。改为 Python 端 JOIN 本地 DuckDB
        dim_goods（已由 Step 1 全量加载），保证分类数据始终可用。
        """
        sql = f"""
        SELECT
            t1.business_date, t1.store_id, t1.order_id, t1.pay_at,
            t1.article_id, t1.order_status, t1.jielong_flag,
            t1.actual_amount, t1.channel, t1.day_clear,
            h.store_flag
        FROM (
            SELECT
                business_date, store_id,
                IF(online_flag = 'Y', CONCAT(order_id, '*'), order_id) AS order_id,
                pay_at,
                abi_article_id                  AS article_id,
                order_status, jielong_flag, actual_amount,
                IF(online_flag = 'Y', 'online', 'offline')              AS channel,
                day_clear
            FROM strategy_fm_sales_di
            WHERE inc_day BETWEEN '{start}' AND '{end}'
        ) t1
        LEFT JOIN (
            SELECT store_id, store_flag
            FROM default_catalog.ads_business_analysis.chdj_store_info
        ) h ON t1.store_id = h.store_id
        WHERE t1.order_status = 'os.completed'
        """

        df = self._api.query(sql)

        # Python 端 JOIN 本地 dim_goods（避免 StarRocks 快照日期不匹配问题）
        goods_df = self._duck._conn.execute("""
            SELECT DISTINCT article_id, category_level1_description, category_level1_id,
                   category_level2_id, category_level2_description,
                   category_level3_id, category_level3_description,
                   spu_id, blackwhite_pig_id, blackwhite_pig_name
            FROM dim_goods
        """).df()
        df = df.merge(goods_df, on='article_id', how='left')

        # v0.10 A16 fix: Python 端排除物料类 (70-77)
        before = len(df)
        if 'category_level1_id' in df.columns:
            df['category_level1_id_raw'] = df['category_level1_id'].astype(str)
            df = df[~df['category_level1_id_raw'].isin(_EXCLUDED_CATS)]
        after = len(df)
        self._log.info(
            f"order_detail: {before} rows fetched, "
            f"{after} after category filter (excluded {before - after})"
        )

        # v0.10 B1 fix: Python 端品类重映射 (master-data v2.1)
        _remap_category_level1(df)

        self._duck.load_df(df, "order_detail", mode="replace")

    def _compute_cust(self, start: str, end: str) -> None:
        """在 DuckDB 聚合客数（分区覆盖，保留历史数据）"""
        parts = []
        for level_desc, level_id_col, extra_cols in _LEVELS:
            level_id_expr = f"COALESCE({level_id_col}::VARCHAR, '')" if level_id_col else "''"
            group_extra   = (", " + ", ".join(extra_cols)) if extra_cols else ""
            parts.append(f"""
            SELECT
                business_date, store_id,
                COALESCE(day_clear::VARCHAR, '0')   AS day_clear,
                '{level_desc}'                      AS level_description,
                {level_id_expr}                     AS level_id,
                COUNT(DISTINCT order_id)            AS cust_num_cate,
                COUNT(DISTINCT IF(
                    (store_flag = '翠花店' AND SUBSTR(CAST(pay_at AS VARCHAR), 12, 8) < '20:00:00')
                    OR (store_flag <> '翠花店' AND SUBSTR(CAST(pay_at AS VARCHAR), 12, 8) < '19:00:00'),
                    order_id, NULL))                AS bf19_cust_num_cate,
                COUNT(DISTINCT IF(order_id LIKE '%*', order_id, NULL))
                                                    AS online_order_num_cate,
                COUNT(DISTINCT article_id)          AS sale_article_num_cate
            FROM order_detail
            GROUP BY business_date, store_id, day_clear {group_extra}
            """)
        union_sql = "\nUNION ALL\n".join(parts)
        # 首次建表（空结构，UNION ALL 需包在子查询中）
        self._duck.execute(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_DUCK_TABLE} AS
            SELECT * FROM ({union_sql}) _sub LIMIT 0
        """)
        # 分区覆盖
        self._duck.execute(f"DELETE FROM {TARGET_DUCK_TABLE} WHERE business_date BETWEEN '{start}' AND '{end}'")
        self._duck.execute(f"INSERT INTO {TARGET_DUCK_TABLE}\n{union_sql}")

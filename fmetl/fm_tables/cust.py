"""
FM 客数底表 (v10)

粒度: 门店 × 日期 × day_clear × level_description × level_id
输入: strategy_fm_sales_di (via API) + dim_goods
输出: t_fm_cust

v10 修复 (A16): Python JOIN dim_goods 后排除 70-77 品类物料
"""

from __future__ import annotations

import duckdb
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
        """从 API 拉订单，Python 端做品类过滤 (A16 fix)"""
        sql = f"""
        SELECT
            t1.business_date, t1.store_id, t1.order_id, t1.pay_at,
            t1.article_id, t1.order_status, t1.jielong_flag,
            t1.actual_amount, t1.channel, t1.day_clear,
            t3.category_level2_description,
            t3.category_level1_description,
            IF(t3.category_level2_description IN ('蛋类','烘焙类'),
               t3.category_level2_description,
               IF(t3.category_level2_description IN ('冷藏奶制品类','饮料类'),
                  '乳制品及水饮类',
                  IF(t3.category_level1_description = '肉禽蛋类'
                     AND t3.category_level2_description <> '蛋类',
                     '肉禽类',
                     IF(RIGHT(t3.category_level3_description, 2) = '熟食',
                        '熟食类',
                        IF(t3.category_level1_description IN ('冷藏及加工类','预制菜'),
                           '冷藏加工及预制菜类',
                           t3.category_level1_description
                        )
                     )
                  )
               )
            )                                AS category_level1_id,
            t3.category_level2_id,
            t3.category_level3_id,
            t3.category_level1_id            AS category_level1_id_raw,
            t3.spu_id,
            t3.blackwhite_pig_id,
            t3.blackwhite_pig_name,
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
            SELECT article_id, category_level1_description, category_level1_id,
                   category_level2_id, category_level2_description,
                   category_level3_id, category_level3_description,
                   spu_id, blackwhite_pig_id, blackwhite_pig_name
            FROM strategy_fm_dim_goods
            WHERE inc_day = '{end}'
        ) t3 ON t1.article_id = t3.article_id
        LEFT JOIN (
            SELECT store_id, store_flag
            FROM default_catalog.ads_business_analysis.chdj_store_info
        ) h ON t1.store_id = h.store_id
        WHERE t1.order_status = 'os.completed'
        """

        df = self._api.query(sql)

        # v10 A16 fix: Python 端排除物料类 (70-77)
        before = len(df)
        if 'category_level1_id_raw' in df.columns:
            df = df[~df['category_level1_id_raw'].isin(_EXCLUDED_CATS)]
        after = len(df)
        self._log.info(
            f"order_detail: {before} rows fetched, "
            f"{after} after category filter (excluded {before - after})"
        )

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

"""
FM 客数底表构建器

粒度: 门店 × 日期 × day_clear × level_description × level_id
结果留在 DuckDB/MotherDuck t_fm_cust。

客数来源: 线下+线上订单明细（两张 hive 表 UNION ALL）
按多个层级聚合客数，level_description 取值：
  门店 | 大类 | 中类 | 小类 | SPU | 黑白猪

WAF 注意: _extract_orders SQL 中 CASE WHEN 已替换为嵌套 IF()。
"""

from __future__ import annotations

from ..connectors import ApiConnector, DuckDBStore
from ..utils import get_logger

TARGET_DUCK_TABLE = "t_fm_cust"

_LEVELS = [
    ("门店",   "",                   []),
    ("大类",   "category_level1_id", ["category_level1_id"]),
    ("中类",   "category_level2_id", ["category_level1_id", "category_level2_id"]),
    ("小类",   "category_level3_id", ["category_level1_id", "category_level2_id", "category_level3_id"]),
    ("SPU",    "spu_id",             ["spu_id"]),
    ("黑白猪", "blackwhite_pig_id",  ["blackwhite_pig_id"]),
]


class CustBuilder:
    def __init__(self, duck: DuckDBStore, api: ApiConnector):
        self._duck = duck
        self._api  = api
        self._log  = get_logger("CustBuilder")

    def build(self, start: str, end: str, yesterday: str) -> None:
        """从 API 拉取订单明细，在 DuckDB 计算客数，结果留在 DuckDB/MotherDuck。"""
        self._log.info(f"building {TARGET_DUCK_TABLE}: {start} ~ {end}")
        self._extract_orders(start, end, yesterday)
        self._compute_cust()
        rows = self._duck.row_count(TARGET_DUCK_TABLE)
        self._log.info(f"{TARGET_DUCK_TABLE}: {rows} rows built")

    # ── 拉取订单 ──────────────────────────────────────────────────────────────
    def _extract_orders(self, start: str, end: str, yesterday: str) -> None:
        """从 API 提取订单明细，预处理后存入 DuckDB order_detail。"""
        sql = f"""
        SELECT
            t1.business_date,
            t1.store_id,
            t1.order_id,
            t1.pay_at,
            t1.abi_article_id               AS article_id,
            t1.order_status,
            t1.jielong_flag,
            t1.actual_amount,
            'offline'                        AS channel,
            dc.day_clear,
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
            t3.spu_id,
            t3.blackwhite_pig_id,
            h.store_flag
        FROM (
            SELECT business_date, store_id, order_id, pay_at, abi_article_id,
                   inc_day, order_status, jielong_flag, actual_amount
            FROM hive.dsl.dsl_transaction_sotre_order_offline_details_di
            WHERE inc_day BETWEEN '{start}' AND '{end}'
            UNION ALL
            SELECT business_date, store_id, CONCAT(order_id,'*') AS order_id,
                   pay_at, abi_article_id, inc_day, order_status, jielong_flag, actual_amount
            FROM hive.dsl.dsl_transaction_sotre_order_online_details_di
            WHERE inc_day BETWEEN '{start}' AND '{end}'
        ) t1
        LEFT JOIN (
            SELECT store_id, inc_day, article_id, day_clear
            FROM hive.dal.dal_transaction_chdj_store_sale_article_sale_info_di
            WHERE inc_day BETWEEN '{start}' AND '{end}'
        ) dc ON t1.store_id = dc.store_id AND t1.inc_day = dc.inc_day AND t1.abi_article_id = dc.article_id
        LEFT JOIN (
            SELECT article_id, category_level1_description, category_level1_id,
                   category_level2_id, category_level2_description,
                   category_level3_id, category_level3_description,
                   spu_id, blackwhite_pig_id
            FROM hive.dim.dim_goods_information_have_pt
            WHERE inc_day = '{yesterday}'
              AND category_level1_id NOT IN ('70','71','72','73','74','75','76','77')
        ) t3 ON t1.abi_article_id = t3.article_id
        LEFT JOIN (
            SELECT store_id, store_flag
            FROM default_catalog.ads_business_analysis.chdj_store_info
        ) h ON t1.store_id = h.store_id
        WHERE t1.order_status = 'os.completed'
        """
        df = self._api.query(sql)
        self._log.info(f"order_detail: {len(df)} rows fetched")
        self._duck.load_df(df, "order_detail", mode="replace")

    # ── 计算客数 ──────────────────────────────────────────────────────────────
    def _compute_cust(self) -> None:
        """在 DuckDB 中按多层级聚合客数，输出 t_fm_cust。"""
        self._duck.execute(f"DROP TABLE IF EXISTS {TARGET_DUCK_TABLE}")
        parts = []
        for level_desc, level_id_col, extra_cols in _LEVELS:
            level_id_expr = f"COALESCE({level_id_col}::VARCHAR, '')" if level_id_col else "''"
            group_extra   = (", " + ", ".join(extra_cols)) if extra_cols else ""
            parts.append(f"""
            SELECT
                business_date,
                store_id,
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
            GROUP BY
                business_date, store_id, day_clear {group_extra}
            """)
        union_sql = "\nUNION ALL\n".join(parts)
        self._duck.execute(f"CREATE TABLE {TARGET_DUCK_TABLE} AS\n{union_sql}")

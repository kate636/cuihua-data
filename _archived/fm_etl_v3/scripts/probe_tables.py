"""权限探测：枚举 fm_etl_v3 用到的所有底表，用当前 Key 试查，输出哪些有权限/哪些没有。

用法:
    python -m fm_etl_v3.scripts.probe_tables [yyyy-mm-dd]

不传日期默认用昨天。只执行 SELECT 1 ... LIMIT 1，不读数据。
"""
from __future__ import annotations

import sys
from datetime import date, timedelta

from ..connectors import ApiConnector

TABLES: list[tuple[str, bool]] = [
    ("hive.dim.dim_goods_information_have_pt", True),
    ("hive.dim.dim_chdj_store_list_di", True),
    ("hive.dim.dim_day_clear_article_list_di", True),
    ("hive.dim.dim_store_profile", False),
    ("hive.dim.dim_calendar", False),
    ("default_catalog.ads_business_analysis.chdj_store_info", False),
    ("hive.ods_sc_db.t_purchase_order_item_tmp", False),
    ("hive.ods_sc_db.t_shop_inventory_sku_pool", False),
    ("hive.dim.dim_store_promotion_info_da", True),
    ("hive.dim.dim_store_article_price_info_da", True),
    ("hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di", True),
    ("hive.dal_bi_rpt.dal_debit_store_dc_difference_adjustment_di", True),
    ("hive.dal.dal_transaction_chdj_store_sale_article_sale_info_di", True),
    ("hive.dal.dal_activity_article_order_sale_info_di", True),
    ("hive.dal.dal_transaction_store_article_lost_di", True),
    ("hive.dsl.dsl_transaction_non_daily_store_order_details_di", True),
    ("hive.dsl.dsl_transaction_non_daily_store_article_purchase_di", True),
    ("hive.dsl.dsl_transaction_sotre_order_offline_details_di", True),
    ("hive.dsl.dsl_transaction_sotre_order_online_details_di", True),
    ("hive.dsl.dsl_transaction_sotre_article_compose_info_di", True),
    ("hive.dsl.dsl_promotion_order_item_article_sale_info_di", True),
]


def probe(day: str) -> None:
    api = ApiConnector()
    api._log_level = "ERROR"

    ok: list[str] = []
    denied: list[tuple[str, str]] = []
    other: list[tuple[str, str]] = []

    for table, has_inc_day in TABLES:
        where = f"WHERE inc_day='{day}'" if has_inc_day else ""
        sql = f"SELECT 1 FROM {table} {where} LIMIT 1"
        try:
            api._fetch_all(sql)
            ok.append(table)
            print(f"[OK]     {table}")
        except Exception as e:
            msg = str(e)
            if "Access denied" in msg or "you need" in msg or "privilege" in msg.lower():
                denied.append((table, _brief(msg)))
                print(f"[DENIED] {table}")
            else:
                other.append((table, _brief(msg)))
                print(f"[ERROR]  {table}  → {_brief(msg)[:120]}")

    print()
    print("=" * 72)
    print(f"OK     : {len(ok):>3}")
    print(f"DENIED : {len(denied):>3}")
    print(f"OTHER  : {len(other):>3}")
    print("=" * 72)

    if denied:
        print("\n无权限表（需要找 QDM admin 授权）：")
        for t, _ in denied:
            print(f"  - {t}")

    if other:
        print("\n其他错误（可能是表名/字段名变了）：")
        for t, m in other:
            print(f"  - {t}")
            print(f"      {m[:150]}")


def _brief(msg: str) -> str:
    for prefix in ("API error: ", "code="):
        if prefix in msg:
            msg = msg.split(prefix, 1)[-1]
    return msg.replace("\n", " ").strip()


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        day = sys.argv[1]
    else:
        day = (date.today() - timedelta(days=1)).isoformat()
    print(f"Probing with inc_day='{day}' …\n")
    probe(day)

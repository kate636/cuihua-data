"""按层级比较固定验证周的本地 v0.13 试算结果与 v1.5 参考结果。"""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pandas as pd

from fmetl.connectors import QdmApi
from fmetl.validation.manifest import (
    SourceManifestSpec,
    build_source_manifest,
    stable_frame_checksum,
)


START = "2026-07-08"
END = "2026-07-14"
STORE = "A3XV"
DB = "data/fm_v013.duckdb"
api = QdmApi()


level_columns = """
business_date,store_id,store_name,day_clear,level_description,
category_level1_id,category_level1_description,
category_level2_id,category_level2_description,
category_level3_id,category_level3_description,
total_sale_qty,total_sale_amt,inbound_qty,inbound_amount,
init_stock_qty,init_stock_amt,end_stock_qty,end_stock_amt,
store_lost_qty,store_lost_amt,store_know_lost_amt,store_unknow_lost_amt,
article_profit_amt,full_link_article_profit
"""
where = (
    f"store_id='{STORE}' AND business_date BETWEEN '{START}' AND '{END}' "
    "AND day_clear='2' AND level_description IN ('门店','大分类','中分类','小分类')"
)
expected_levels = int(api.query(
    "SELECT COUNT(*) row_count FROM default_catalog.ads_business_analysis."
    f"strategy_fm_levels_sum_v1_5 WHERE {where}"
).iloc[0]["row_count"])
levels = api.query(
    f"SELECT {level_columns} FROM default_catalog.ads_business_analysis."
    f"strategy_fm_levels_sum_v1_5 WHERE {where}"
)
if len(levels) != expected_levels:
    raise RuntimeError(f"v1.5 levels expected {expected_levels}, got {len(levels)}")

sku_parts = []
for day in pd.date_range(START, END).strftime("%Y-%m-%d"):
    expected = int(api.query(f"""
        SELECT COUNT(*) row_count
        FROM default_catalog.ads_business_analysis.strategy_fm_flag_sku_di_v1_5
        WHERE store_id='{STORE}' AND business_date='{day}'
    """).iloc[0]["row_count"])
    part = api.query(f"""
        SELECT business_date,store_id,day_clear,article_id,article_name,
               category_level1_id,category_level1_description,
               category_level2_id,category_level2_description,
               category_level3_id,category_level3_description,
               total_sale_qty,total_sale_amt,inbound_qty,inbound_amount,
               init_stock_qty,init_stock_amt,end_stock_qty,end_stock_amt,
               store_lost_qty,store_lost_amt,store_know_lost_amt,store_unknow_lost_amt,
               article_profit_amt,full_link_article_profit,ccj_waste_money,ssls_waste_money
        FROM default_catalog.ads_business_analysis.strategy_fm_flag_sku_di_v1_5
        WHERE store_id='{STORE}' AND business_date='{day}'
    """)
    if len(part) != expected:
        raise RuntimeError(f"v1.5 sku {day} expected {expected}, got {len(part)}")
    sku_parts.append(part)
sku = pd.concat(sku_parts, ignore_index=True)
for frame in (levels, sku):
    if pd.api.types.is_numeric_dtype(frame["business_date"]):
        frame["business_date"] = pd.to_datetime(
            frame["business_date"], unit="ms", utc=True
        ).dt.tz_convert("Asia/Shanghai").dt.strftime("%Y-%m-%d")
requested_dates = set(pd.date_range(START, END).strftime("%Y-%m-%d"))
level_dates = set(levels["business_date"].astype(str))
if level_dates != requested_dates:
    raise RuntimeError(
        f"v1.5 levels date coverage mismatch: missing={sorted(requested_dates-level_dates)}, "
        f"extra={sorted(level_dates-requested_dates)}"
    )
covered_dates = set(sku["business_date"].astype(str))
reference_manifest = build_source_manifest([
    SourceManifestSpec(
        "strategy_fm_levels_sum_v1_5", "STARROCKS_V1_5_REFERENCE", levels,
        partition_column="business_date", business_date_column="business_date",
        expected_partitions=tuple(sorted(requested_dates)),
    ),
    SourceManifestSpec(
        "strategy_fm_flag_sku_di_v1_5", "STARROCKS_V1_5_REFERENCE", sku,
        partition_column="business_date", business_date_column="business_date",
        expected_partitions=tuple(sorted(requested_dates)),
    ),
])

conn = duckdb.connect(DB)
try:
    for table, frame in (("reference_v15_levels", levels), ("reference_v15_sku", sku)):
        conn.register("_frame", frame)
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM _frame")
        conn.unregister("_frame")

    conn.execute("DROP TABLE IF EXISTS comparison_v013_v15_levels")
    conn.execute("""
        CREATE TABLE comparison_v013_v15_levels AS
        WITH v13 AS (
            SELECT CAST(business_date AS VARCHAR) business_date,
                   CAST(level_description AS VARCHAR) level_description,
                   COALESCE(CAST(category_level1_id AS VARCHAR),'') category_level1_id,
                   COALESCE(CAST(category_level1_description AS VARCHAR),'') category_level1_description,
                   COALESCE(CAST(category_level2_id AS VARCHAR),'') category_level2_id,
                   COALESCE(CAST(category_level2_description AS VARCHAR),'') category_level2_description,
                   COALESCE(CAST(category_level3_id AS VARCHAR),'') category_level3_id,
                   COALESCE(CAST(category_level3_description AS VARCHAR),'') category_level3_description,
                   total_sale_amount sale, inbound_amount receive,
                   init_stock_amount init_stock, end_stock_amount end_stock,
                   known_lost_amt known_loss, unknown_lost_amt unknown_loss,
                   known_lost_amt + unknown_lost_amt total_loss, store_profit_amount profit,
                   adjusted_known_lost_amt adjusted_known_loss,
                   adjusted_lost_amt adjusted_total_loss,
                   adjusted_profit
            FROM shadow_levels_daily WHERE day_clear='2'
        ), v15 AS (
            SELECT CAST(business_date AS VARCHAR) business_date,
                   CAST(level_description AS VARCHAR) level_description,
                   COALESCE(CAST(category_level1_id AS VARCHAR),'') category_level1_id,
                   COALESCE(CAST(category_level1_description AS VARCHAR),'') category_level1_description,
                   COALESCE(CAST(category_level2_id AS VARCHAR),'') category_level2_id,
                   COALESCE(CAST(category_level2_description AS VARCHAR),'') category_level2_description,
                   COALESCE(CAST(category_level3_id AS VARCHAR),'') category_level3_id,
                   COALESCE(CAST(category_level3_description AS VARCHAR),'') category_level3_description,
                   total_sale_amt sale, inbound_amount receive,
                   init_stock_amt init_stock, end_stock_amt end_stock,
                   store_know_lost_amt known_loss, store_unknow_lost_amt unknown_loss,
                   store_lost_amt total_loss, article_profit_amt profit
                   ,store_know_lost_amt adjusted_known_loss,
                   store_lost_amt adjusted_total_loss,
                   article_profit_amt adjusted_profit
            FROM reference_v15_levels
        )
        SELECT COALESCE(a.business_date,b.business_date) business_date,
               COALESCE(a.level_description,b.level_description) level_description,
               COALESCE(a.category_level1_id,b.category_level1_id) category_level1_id,
               COALESCE(a.category_level1_description,b.category_level1_description) category_level1_description,
               COALESCE(a.category_level2_id,b.category_level2_id) category_level2_id,
               COALESCE(a.category_level2_description,b.category_level2_description) category_level2_description,
               COALESCE(a.category_level3_id,b.category_level3_id) category_level3_id,
               COALESCE(a.category_level3_description,b.category_level3_description) category_level3_description,
               a.sale v013_sale,b.sale v15_sale,a.sale-b.sale sale_diff,
               a.receive v013_receive,b.receive v15_receive,a.receive-b.receive receive_diff,
               a.init_stock v013_init_stock,b.init_stock v15_init_stock,a.init_stock-b.init_stock init_stock_diff,
               a.end_stock v013_end_stock,b.end_stock v15_end_stock,a.end_stock-b.end_stock end_stock_diff,
               a.known_loss v013_known_loss,b.known_loss v15_known_loss,a.known_loss-b.known_loss known_loss_diff,
               a.unknown_loss v013_unknown_loss,b.unknown_loss v15_unknown_loss,a.unknown_loss-b.unknown_loss unknown_loss_diff,
               a.total_loss v013_total_loss,b.total_loss v15_total_loss,a.total_loss-b.total_loss total_loss_diff,
               a.profit v013_profit,b.profit v15_profit,a.profit-b.profit profit_diff
               ,a.adjusted_known_loss v013_adjusted_known_loss,
               b.adjusted_known_loss v15_adjusted_known_loss,
               a.adjusted_known_loss-b.adjusted_known_loss adjusted_known_loss_diff,
               a.adjusted_total_loss v013_adjusted_total_loss,
               b.adjusted_total_loss v15_adjusted_total_loss,
               a.adjusted_profit v013_adjusted_profit,
               b.adjusted_profit v15_adjusted_profit,
               a.adjusted_profit-b.adjusted_profit adjusted_profit_diff
        FROM v13 a FULL OUTER JOIN v15 b USING (
            business_date,level_description,category_level1_id,category_level1_description,
            category_level2_id,category_level2_description,category_level3_id,category_level3_description
        )
    """)
    conn.execute("DROP TABLE IF EXISTS comparison_v013_v15_sku")
    conn.execute("""
        CREATE TABLE comparison_v013_v15_sku AS
        WITH covered_dates AS (
            SELECT DISTINCT CAST(business_date AS VARCHAR) business_date FROM reference_v15_sku
        ), v13 AS (
            SELECT s.* FROM shadow_sku_daily s JOIN covered_dates d USING (business_date)
            WHERE s.is_reportable
        ), v15 AS (
            SELECT * REPLACE (CAST(business_date AS VARCHAR) AS business_date,
                              CAST(article_id AS VARCHAR) AS article_id)
            FROM reference_v15_sku
        )
        SELECT COALESCE(a.business_date,b.business_date) business_date,
               COALESCE(a.article_id,b.article_id) article_id,
               COALESCE(a.article_name,b.article_name) article_name,
               COALESCE(a.report_category_name,b.category_level1_description) category,
               COALESCE(a.category_level2_description,b.category_level2_description) middle_category,
               COALESCE(a.category_level3_description,b.category_level3_description) small_category,
               COALESCE(a.net_sale_amt,0) v013_sale,COALESCE(b.total_sale_amt,0) v15_sale,
               COALESCE(a.store_receive_amt,0) v013_receive,COALESCE(b.inbound_amount,0) v15_receive,
               COALESCE(a.init_stock_amt,0) v013_init_stock,COALESCE(b.init_stock_amt,0) v15_init_stock,
               COALESCE(a.end_amt,0) v013_end_stock,COALESCE(b.end_stock_amt,0) v15_end_stock,
               COALESCE(a.accounting_known_lost_amt,0) v013_known_loss,
               COALESCE(b.store_know_lost_amt,0) v15_known_loss,
               COALESCE(a.unknown_lost_amt,0) v013_unknown_loss,
               COALESCE(b.store_unknow_lost_amt,0) v15_unknown_loss,
               COALESCE(a.accounting_profit,0) v013_profit,
               COALESCE(b.article_profit_amt,0) v15_profit,
               COALESCE(a.accounting_profit,0)-COALESCE(b.article_profit_amt,0) profit_diff,
               COALESCE(a.accounting_known_lost_amt,0)-COALESCE(b.store_know_lost_amt,0) known_loss_diff,
               COALESCE(a.unknown_lost_amt,0)-COALESCE(b.store_unknow_lost_amt,0) unknown_loss_diff,
               COALESCE(a.adjusted_profit_before_ssls,0) v013_adjusted_profit,
               COALESCE(a.adjusted_profit_before_ssls,0)-COALESCE(b.article_profit_amt,0) adjusted_profit_diff,
               a.issue_unit_cost,a.branch,a.is_counted,a.opening_source,
               COALESCE(a.bom_in_amt,0) bom_in_amt,COALESCE(a.bom_out_amt,0) bom_out_amt,
               COALESCE(a.pack_in_amt,0) pack_in_amt,COALESCE(a.pack_out_amt,0) pack_out_amt,
               COALESCE(a.compose_in_amt,0) compose_in_amt,COALESCE(a.compose_out_amt,0) compose_out_amt,
               COALESCE(b.ccj_waste_money,0) ccj_waste_money,
               COALESCE(b.ssls_waste_money,0) ssls_waste_money
        FROM v13 a FULL OUTER JOIN v15 b USING (business_date,article_id)
    """)
    conn.execute("DROP TABLE IF EXISTS comparison_v013_v15_sku_covered")
    conn.execute(
        "CREATE TABLE comparison_v013_v15_sku_covered AS "
        "SELECT * FROM comparison_v013_v15_sku"
    )
    run_row = conn.execute(
        "SELECT run_id,source_manifest_checksum FROM shadow_run_manifest LIMIT 1"
    ).fetchone()
    comparison_manifest = pd.DataFrame([{
        "comparison_id": f"v013-v15:{STORE}:{START}:{END}:{stable_frame_checksum(reference_manifest)[:12]}",
        "run_id": run_row[0],
        "v013_source_manifest_checksum": run_row[1],
        "store_id": STORE,
        "requested_start": START,
        "requested_end": END,
        "levels_requested_dates": len(requested_dates),
        "levels_covered_dates": len(level_dates),
        "levels_missing_dates": ",".join(sorted(requested_dates - level_dates)),
        "levels_row_count": len(levels),
        "levels_sha256": stable_frame_checksum(levels),
        "sku_requested_dates": len(requested_dates),
        "sku_covered_dates": len(covered_dates),
        "sku_missing_dates": ",".join(sorted(requested_dates - covered_dates)),
        "sku_row_count": len(sku),
        "sku_sha256": stable_frame_checksum(sku),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }])
    for table, frame in (
        ("shadow_reference_source_manifest", reference_manifest),
        ("shadow_comparison_manifest", comparison_manifest),
    ):
        conn.register("_frame", frame)
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM _frame")
        conn.unregister("_frame")
    print("SKU_REFERENCE_MISSING_DATES", sorted(requested_dates - covered_dates))
    print("REFERENCE", {"levels": len(levels), "sku": len(sku)})
    print("WEEK_LEVEL", conn.execute("""
        SELECT level_description,COUNT(*) nrows,
               SUM(v013_sale) v013_sale,SUM(v15_sale) v15_sale,SUM(sale_diff) sale_diff,
               SUM(v013_receive) v013_receive,SUM(v15_receive) v15_receive,SUM(receive_diff) receive_diff,
               SUM(v013_known_loss) v013_known_loss,SUM(v15_known_loss) v15_known_loss,
               SUM(v013_unknown_loss) v013_unknown_loss,SUM(v15_unknown_loss) v15_unknown_loss,
               SUM(v013_profit) v013_profit,SUM(v15_profit) v15_profit,SUM(profit_diff) profit_diff
               ,SUM(v013_adjusted_profit) v013_adjusted_profit,
               SUM(v15_adjusted_profit) v15_adjusted_profit,
               SUM(adjusted_profit_diff) adjusted_profit_diff,
               SUM(v013_adjusted_known_loss) v013_adjusted_known_loss,
               SUM(v15_adjusted_known_loss) v15_adjusted_known_loss
        FROM comparison_v013_v15_levels GROUP BY level_description ORDER BY level_description
    """).df().to_string(index=False))
    print("LARGE", conn.execute("""
        SELECT category_level1_description category,
               SUM(v013_sale) v013_sale,SUM(v15_sale) v15_sale,
               SUM(v013_receive) v013_receive,SUM(v15_receive) v15_receive,
               SUM(v013_known_loss) v013_known_loss,SUM(v15_known_loss) v15_known_loss,
               SUM(v013_unknown_loss) v013_unknown_loss,SUM(v15_unknown_loss) v15_unknown_loss,
               SUM(v013_profit) v013_profit,SUM(v15_profit) v15_profit,
               SUM(profit_diff) profit_diff,
               SUM(v013_adjusted_profit) v013_adjusted_profit,
               SUM(v15_adjusted_profit) v15_adjusted_profit,
               SUM(adjusted_profit_diff) adjusted_profit_diff
        FROM comparison_v013_v15_levels WHERE level_description='大分类'
        GROUP BY category ORDER BY ABS(SUM(profit_diff)) DESC
    """).df().to_string(index=False))
finally:
    conn.close()

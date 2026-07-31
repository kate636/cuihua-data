"""重建 A3XV 2026-07-08 至 2026-07-14 的本地 v0.13 影子账本。"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, timedelta
import hashlib
from pathlib import Path
import subprocess

import duckdb
import pandas as pd

from fmetl.calculations.ledger import run_weighted_ledger
from fmetl.calculations.special_wastage import adjust_sku_wastage, build_wastage_trace
from fmetl.connectors import QdmApi
from fmetl.connectors.processing_relations import ProcessingRelationSource
from fmetl.contracts import RunManifest
from fmetl.facts.bom_plan import build_disassembly_plan
from fmetl.facts.pack_plan import build_pack_plan
from fmetl.facts.processing_plan import build_processing_plan
from fmetl.facts.shadow_assembly import (
    assemble_dense_activities,
    build_bootstrap_openings,
    build_internal_event_legs,
    filter_cost_funded_internal_events,
)
from fmetl.facts.sku_day import (
    attach_authoritative_day_clear,
    normalize_chdj_day_clear,
    normalize_inventory_counts,
    normalize_known_loss,
    normalize_sales_events,
)
from fmetl.facts.store_receipts import build_store_receipts
from fmetl.master_data.category import RULE_PATH, load_category_mapper
from fmetl.outputs.shadow_levels import build_shadow_levels_daily
from fmetl.relations.resolver import resolve_relations
from fmetl.validation.manifest import (
    SourceManifestSpec,
    build_source_manifest,
    stable_frame_checksum,
)
from fmetl.validation.preflight import SYNC_SCRIPT_SHA256


START = "2026-07-08"
END = "2026-07-14"
STORE = "A3XV"
DB_PATH = Path("data/fm_v013.duckdb").resolve()
api = QdmApi()
days = [(date.fromisoformat(START) + timedelta(days=i)).isoformat() for i in range(7)]


def fetch(sql: str, count_sql: str, label: str) -> pd.DataFrame:
    expected = int(api.query(count_sql).iloc[0]["row_count"])
    if expected >= api.settings.page_size:
        raise RuntimeError(f"{label}: {expected} rows exceeds safe single-page fetch")
    frame = api.query(sql)
    if len(frame) != expected:
        raise RuntimeError(f"{label}: expected {expected}, got {len(frame)}")
    return frame


def fetch_days(table: str, columns: str, store_column: str = "store_id") -> pd.DataFrame:
    parts = []
    for day in days:
        where = f"{store_column}='{STORE}' AND inc_day='{day}'"
        parts.append(fetch(
            f"SELECT {columns} FROM default_catalog.ads_business_analysis.{table} WHERE {where}",
            f"SELECT COUNT(*) AS row_count FROM default_catalog.ads_business_analysis.{table} WHERE {where}",
            f"{table}/{day}",
        ))
    return pd.concat(parts, ignore_index=True)


print("FETCH start")
sales_raw = fetch_days(
    "strategy_fm_sales_di",
    "store_id,business_date,inc_day,abi_article_id,day_clear,qty_spec,sales_amt,return_sale_qty,return_sale_amt",
)
purchase = fetch_days(
    "strategy_fm_purchase_di",
    "store_id,business_date,inc_day,day_clear,article_id,article_name,sale_article_id,sale_article_name,"
    "sale_article_qty,sale_article_purchase_amt,avg_inbound_price,init_stock_qty,init_stock_amt,end_stock_qty,end_stock_amt,inventory_cost",
)
loss_raw = fetch_days(
    "strategy_fm_loss_di",
    "store_id,inc_day,article_id,know_lost_qty,know_lost_amt,unknow_lost_qty,unknow_lost_amt",
)
count_raw = fetch_days(
    "strategy_fm_store_article_inventory_detail_di",
    "shop_id,inventory_date,inc_day,sku_code,sale_stock_qty,actual_stock_qty,profit_loss_qty,"
    "created_by,created_at,updated_by,updated_at",
    store_column="shop_id",
)
day_clear_raw = fetch_days(
    "strategy_fm_chdj_article_di", "store_id,inc_day,article_id,day_clear"
)
receive_sale = fetch_days(
    "strategy_fm_receive_sale_di",
    "store_id,inc_day,article_id,article_name,sale_article_id,sale_article_name,inbound_qty,inbound_amount,"
    "sum_article_qty,sum_sub_article_qty,sale_article_qty,spilit_sale_article_amt,rate,sale_recev_rate,"
    "category_level1_description",
)
bom_relation = fetch_days(
    "strategy_dim_store_article_bom_relation",
    "store_id,inc_day,parent_article_id,parent_article_unit,sub_article_id,sub_article_unit,"
    "dressing_rate,cost_rate,bom_type,split_mode,sp_level,category_level1_description",
)
article_convert = fetch_days(
    "strategy_fm_dim_article_convert",
    "store_id,inc_day,parent_article_id,parent_article_name,sub_article_id,sub_article_name,"
    "parent_rate,sub_rate,ctype",
)
compose_raw = fetch_days(
    "strategy_fm_compose_di",
    "store_id,business_date,inc_day,article_id,compose_in_qty,compose_out_qty,compose_in_amt,compose_out_amt",
)
wastage_latest = api.query("""
    SELECT MAX(inc_day) AS source_day
    FROM default_catalog.ads_business_analysis.cuihua_t_purchase_wastage
    WHERE is_deleted=0
""").iloc[0]["source_day"]
wastage_raw = fetch(
    "SELECT inc_day,sku_code,created_at,reason,waste_money,waste_num,is_deleted "
    "FROM default_catalog.ads_business_analysis.cuihua_t_purchase_wastage "
    f"WHERE inc_day='{wastage_latest}' AND is_deleted=0 "
    f"AND DATE(created_at) BETWEEN '{START}' AND '{END}' "
    "AND reason IN ('炒菜机成本','生熟联动')",
    "SELECT COUNT(*) AS row_count "
    "FROM default_catalog.ads_business_analysis.cuihua_t_purchase_wastage "
    f"WHERE inc_day='{wastage_latest}' AND is_deleted=0 "
    f"AND DATE(created_at) BETWEEN '{START}' AND '{END}' "
    "AND reason IN ('炒菜机成本','生熟联动')",
    "special_wastage",
)

latest_goods = api.query("""
    SELECT MAX(inc_day) AS source_day
    FROM default_catalog.ads_business_analysis.strategy_fm_dim_goods
""").iloc[0]["source_day"]
goods_parts = []
for bucket in range(32):
    where = (
        f"inc_day='{latest_goods}' AND "
        f"MOD(CRC32(COALESCE(CAST(article_id AS STRING), '')), 32)={bucket}"
    )
    goods_parts.append(fetch(
        "SELECT inc_day,article_id,article_name,sale_unit,unit_weight,matnr,matnr_unit,order_unit,"
        "atob_value,zglfz,zglfm,category_level1_id,category_level1_description,category_level2_id,"
        "category_level2_description,category_level3_id,category_level3_description,spu_id,spu_name,"
        "blackwhite_pig_id,blackwhite_pig_name "
        f"FROM default_catalog.ads_business_analysis.strategy_fm_dim_goods WHERE {where}",
        "SELECT COUNT(*) AS row_count FROM default_catalog.ads_business_analysis.strategy_fm_dim_goods "
        f"WHERE {where}",
        f"goods/{bucket}",
    ))
goods = pd.concat(goods_parts, ignore_index=True)
print("FETCH done", {"sales":len(sales_raw),"purchase":len(purchase),"goods":len(goods)})

sales = normalize_sales_events(sales_raw)
losses = normalize_known_loss(loss_raw)
counts = normalize_inventory_counts(count_raw)
day_clear = normalize_chdj_day_clear(day_clear_raw)
sales = attach_authoritative_day_clear(sales, day_clear)

recipes_snapshot = ProcessingRelationSource().fetch_once()
recipes = recipes_snapshot.frame
relation_component_checksums = {
    "processing_recipe": recipes_snapshot.snapshot.checksum,
    "bom_relation": stable_frame_checksum(bom_relation),
    "article_convert": stable_frame_checksum(article_convert),
}
relation_checksum = hashlib.sha256(
    "|".join(
        f"{name}:{checksum}" for name, checksum in sorted(relation_component_checksums.items())
    ).encode("utf-8")
).hexdigest()
snapshot_id = f"shadow-relation-bundle:{relation_checksum[:16]}"

cross = receive_sale.loc[
    receive_sale["article_id"].astype(str).ne(receive_sale["sale_article_id"].astype(str))
].copy()
relation_candidates = cross[["store_id", "inc_day", "article_id", "sale_article_id"]].rename(columns={
    "inc_day":"business_date", "article_id":"from_article_id", "sale_article_id":"to_article_id",
})
recipe_candidates = pd.DataFrame([
    {"store_id": STORE, "business_date": day, "from_article_id": row.raw_article_id,
     "to_article_id": row.finished_article_id}
    for day in days for row in recipes.itertuples(index=False)
])
candidates = pd.concat([relation_candidates, recipe_candidates], ignore_index=True).astype(str)
candidates = candidates.drop_duplicates(["store_id","business_date","from_article_id","to_article_id"])
resolution = resolve_relations(
    candidates,
    relation_snapshot_id=snapshot_id,
    processing_recipes=recipes,
    article_convert=article_convert,
    bom_edges=bom_relation,
)

formal = resolution.loc[resolution["formal_flow_allowed"]].copy()
bom_pairs = formal.loc[formal["relation_type"].eq("DISASSEMBLY_BOM"), [
    "store_id","business_date","from_article_id","to_article_id",
]]
bom_input = receive_sale.merge(
    bom_pairs,
    left_on=["store_id","inc_day","article_id","sale_article_id"],
    right_on=["store_id","business_date","from_article_id","to_article_id"],
    how="inner", validate="one_to_one",
).drop(columns=["business_date","from_article_id","to_article_id"])
bom_plan = build_disassembly_plan(bom_input, resolution)

pack_pairs = formal.loc[formal["relation_type"].eq("PACK_CONVERT"), [
    "store_id","business_date","from_article_id","to_article_id",
]]
pack_events = receive_sale.merge(
    pack_pairs,
    left_on=["store_id","inc_day","article_id","sale_article_id"],
    right_on=["store_id","business_date","from_article_id","to_article_id"],
    how="inner", validate="one_to_one",
).rename(columns={
    "article_id":"parent_article_id", "sale_article_id":"sub_article_id",
    "inbound_qty":"parent_qty", "sale_article_qty":"sub_qty",
})
pack_events["event_source"] = "UPSTREAM_DAL_RECEIVE_SALE"
pack_events = pack_events[[
    "store_id","business_date","parent_article_id","sub_article_id","parent_qty","sub_qty","event_source",
]]
convert_for_pack = article_convert.rename(columns={"inc_day":"business_date"}).copy()
pack_probe = pack_events.merge(
    convert_for_pack[["store_id","business_date","parent_article_id","sub_article_id","parent_rate","sub_rate"]],
    on=["store_id","business_date","parent_article_id","sub_article_id"],
    how="left", validate="many_to_one",
)
for col in ("parent_qty","sub_qty","parent_rate","sub_rate"):
    pack_probe[col] = pd.to_numeric(pack_probe[col], errors="coerce")
pack_good = (
    pack_probe[["parent_qty","sub_qty","parent_rate","sub_rate"]].notna().all(axis=1)
    & pack_probe["parent_rate"].gt(0) & pack_probe["sub_rate"].gt(0)
    & (pack_probe["parent_rate"] * pack_probe["sub_rate"] - 1).abs().le(0.001)
    & (pack_probe["parent_qty"] - pack_probe["sub_qty"] * pack_probe["sub_rate"]).abs().le(0.001)
)
pack_quarantine = pack_probe.loc[~pack_good].copy()
pack_plan = build_pack_plan(pack_events.loc[pack_good], article_convert, resolution)

compose_actual = compose_raw[[
    "store_id","business_date","article_id","compose_in_qty","compose_out_qty",
]].copy()
processing_plan = build_processing_plan(compose_actual, recipes, resolution)

internal_sources, internal_targets = build_internal_event_legs(
    bom_parent=bom_plan.parent_postings,
    bom_trace=bom_plan.trace,
    pack_plan=pack_plan,
    compose_trace=processing_plan.trace,
)
reconstructable_sources = internal_sources.loc[
    internal_sources["relation_type"].isin(
        {"DISASSEMBLY_BOM", "PACK_CONVERT", "RECIPE_COMPOSE"}
    )
]
formal_parent_keys = set(
    zip(
        reconstructable_sources["store_id"],
        reconstructable_sources["business_date"],
        reconstructable_sources["source_article_id"],
    )
)
receipt_build = build_store_receipts(
    purchase,
    receive_sale,
    parent_reconstruction_keys=formal_parent_keys,
)

goods_source = goods.copy()
goods["article_id"] = goods["article_id"].astype(str)
if goods["article_id"].duplicated().any():
    duplicates = goods.loc[goods["article_id"].duplicated(keep=False), "article_id"].head(20)
    raise RuntimeError(f"latest goods snapshot contains duplicate article_id: {duplicates.tolist()}")
goods["is_reportable"] = ~goods["category_level1_id"].astype(str).isin(
    {"70", "71", "72", "73", "74", "75", "76", "77"}
)
category_mapper = load_category_mapper()
goods = category_mapper.map_frame(goods)
goods_ids = set(goods["article_id"])
reportable_ids = set(goods.loc[goods["is_reportable"], "article_id"])
internal_ids = set(internal_sources["source_article_id"].astype(str)) | set(
    internal_targets["target_article_id"].astype(str)
)
first_purchase = purchase.loc[purchase["business_date"].astype(str).eq(START)].copy()
first_purchase["sale_article_id"] = first_purchase["sale_article_id"].astype(str)
first_active = set(first_purchase.loc[
    pd.to_numeric(first_purchase["init_stock_qty"], errors="coerce").fillna(0).abs().gt(0.000001)
    | pd.to_numeric(first_purchase["init_stock_amt"], errors="coerce").fillna(0).abs().gt(0.01),
    "sale_article_id",
]) & (reportable_ids | internal_ids)
universe = set(first_active) | internal_ids
for frame, column in (
    (sales,"article_id"),(losses,"article_id"),(counts,"article_id"),
    (receipt_build.postings,"article_id"),
):
    universe.update(set(frame[column].dropna().astype(str)) & reportable_ids)
missing_goods = sorted(universe - goods_ids)
if missing_goods:
    raise RuntimeError(f"formal activity SKUs missing Foodmart goods: {missing_goods[:30]}")
universe &= goods_ids

def keep(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.loc[frame["article_id"].astype(str).isin(universe)].copy()

sales = keep(sales)
losses = keep(losses)
counts = keep(counts)
day_clear = keep(day_clear)
receipts = receipt_build.postings.loc[
    receipt_build.postings["article_id"].astype(str).isin(universe)
].copy()
label_keys = set(
    day_clear[["store_id", "business_date", "article_id"]]
    .astype(str).itertuples(index=False, name=None)
)
day_clear_default_rows = [
    {
        "store_id": STORE,
        "business_date": day,
        "article_id": article_id,
        "day_clear": "1",
        "default_reason": (
            "INTERNAL_MATERIAL_WITHOUT_CHDJ_LABEL"
            if article_id in internal_ids and article_id not in reportable_ids
            else "NON_SALES_STOCK_WITHOUT_CHDJ_LABEL"
        ),
    }
    for day in days
    for article_id in sorted(universe)
    if (STORE, day, article_id) not in label_keys
]
day_clear_defaults = pd.DataFrame(
    day_clear_default_rows,
    columns=["store_id", "business_date", "article_id", "day_clear", "default_reason"],
)
if not day_clear_defaults.empty:
    day_clear = pd.concat(
        [day_clear, day_clear_defaults[["store_id", "business_date", "article_id", "day_clear"]]],
        ignore_index=True,
    )
openings = build_bootstrap_openings(purchase, sorted(universe), start_day=START)
activities = assemble_dense_activities(
    days=days,
    article_ids=sorted(universe),
    sales=sales,
    losses=losses,
    counts=counts,
    day_clear=day_clear,
    receipts=receipts,
)
cost_funded = filter_cost_funded_internal_events(
    activities=activities,
    openings=openings,
    sources=internal_sources,
    targets=internal_targets,
)
internal_sources = cost_funded.sources
internal_targets = cost_funded.targets
preledger_path = Path("/private/tmp/fm_v013_preledger.duckdb")
preledger = duckdb.connect(str(preledger_path))
try:
    for table, frame in (
        ("activities", activities),
        ("openings", openings),
        ("internal_sources", internal_sources),
        ("internal_targets", internal_targets),
        ("goods", goods),
        ("day_clear_defaults", day_clear_defaults),
        ("receipt_postings", receipt_build.postings),
        ("receipt_reconciliation", receipt_build.reconciliation),
        ("receipt_quarantine", receipt_build.quarantined),
        ("relation_resolution", resolution),
        ("pack_quarantine", pack_quarantine),
        ("compose_quarantine", processing_plan.quarantined),
        ("internal_cost_quarantine", cost_funded.quarantined),
    ):
        preledger.register("_frame", frame)
        preledger.execute(f'DROP TABLE IF EXISTS "{table}"')
        preledger.execute(f'CREATE TABLE "{table}" AS SELECT * FROM _frame')
        preledger.unregister("_frame")
finally:
    preledger.close()
ledger = run_weighted_ledger(activities, openings, internal_sources, internal_targets)
accounting_sku = ledger.sku_daily.copy()
accounting_sku["accounting_full_profit"] = accounting_sku["accounting_profit"]
adjusted_sku = adjust_sku_wastage(accounting_sku, wastage_raw)
wastage_trace = build_wastage_trace(wastage_raw)
sku = adjusted_sku.merge(
    goods, on="article_id", how="left", validate="many_to_one"
)
sku["store_name"] = "广州滨江宏岸店"
levels = build_shadow_levels_daily(sku)

wastage_manifest_frame = wastage_raw.copy()
wastage_manifest_frame["business_date"] = pd.to_datetime(
    wastage_manifest_frame["created_at"], errors="raise"
).dt.date.astype(str)
source_manifest = build_source_manifest([
    SourceManifestSpec("strategy_fm_sales_di", "STARROCKS_MIRROR", sales_raw, "inc_day", business_date_column="business_date", expected_partitions=tuple(days)),
    SourceManifestSpec("strategy_fm_purchase_di", "STARROCKS_MIRROR", purchase, "inc_day", business_date_column="business_date", expected_partitions=tuple(days)),
    SourceManifestSpec("strategy_fm_loss_di", "STARROCKS_MIRROR", loss_raw, "inc_day", business_date_column="inc_day", expected_partitions=tuple(days)),
    SourceManifestSpec("strategy_fm_store_article_inventory_detail_di", "STARROCKS_MIRROR", count_raw, "inc_day", business_date_column="inventory_date", expected_partitions=tuple(days)),
    SourceManifestSpec("strategy_fm_chdj_article_di", "STARROCKS_MIRROR", day_clear_raw, "inc_day", business_date_column="inc_day", expected_partitions=tuple(days)),
    SourceManifestSpec("strategy_fm_receive_sale_di", "STARROCKS_MIRROR", receive_sale, "inc_day", business_date_column="inc_day", expected_partitions=tuple(days)),
    SourceManifestSpec("strategy_dim_store_article_bom_relation", "STARROCKS_MIRROR", bom_relation, "inc_day", business_date_column="inc_day", expected_partitions=tuple(days)),
    SourceManifestSpec("strategy_fm_dim_article_convert", "STARROCKS_MIRROR", article_convert, "inc_day", business_date_column="inc_day", expected_partitions=tuple(days)),
    SourceManifestSpec("strategy_fm_compose_di", "STARROCKS_MIRROR", compose_raw, "inc_day", business_date_column="business_date", expected_partitions=tuple(days)),
    SourceManifestSpec("strategy_fm_dim_goods", "STARROCKS_MIRROR", goods_source, "inc_day"),
    SourceManifestSpec("cuihua_t_purchase_wastage", "STARROCKS_AUXILIARY", wastage_manifest_frame, fixed_partition=str(wastage_latest), business_date_column="business_date"),
    SourceManifestSpec("processing_relation_export", "FOODMART_API", recipes, fixed_partition=recipes_snapshot.exported_at or recipes_snapshot.snapshot.created_at.isoformat()),
])
source_manifest_checksum = stable_frame_checksum(source_manifest)
try:
    git_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
    ).strip()
    git_dirty = bool(subprocess.check_output(
        ["git", "status", "--porcelain", "--", "fmetl"], text=True,
        stderr=subprocess.DEVNULL,
    ).strip())
except (OSError, subprocess.CalledProcessError):
    git_commit = "UNKNOWN"
    git_dirty = True
run_manifest = pd.DataFrame([asdict(RunManifest(
    run_id=f"v0.13:{STORE}:{START}:{END}:{source_manifest_checksum[:12]}",
    # This fixed historical replay remains a v0.13 diagnostic after the main
    # package version advances to v0.14.
    version="0.13",
    git_commit=git_commit,
    requested_start=START,
    requested_end=END,
    affected_start=START,
    affected_end=END,
    store_id=STORE,
    mirror_sync_version="v1_5/sync_strategy_fm.sh",
    mirror_sync_checksum=SYNC_SCRIPT_SHA256,
    category_rule_version=category_mapper.version,
    category_rule_checksum=hashlib.sha256(RULE_PATH.read_bytes()).hexdigest(),
    relation_snapshot_id=snapshot_id,
    relation_snapshot_checksum=relation_checksum,
    source_manifest_checksum=source_manifest_checksum,
    git_dirty=git_dirty,
    status="completed",
))])

print("SHADOW", {
    "universe":len(universe), "sku_days":len(sku), "levels":len(levels),
    "internal_postings":len(ledger.internal_postings),
    "pack_quarantine":len(pack_quarantine), "compose_quarantine":len(processing_plan.quarantined),
    "opening_warnings":openings["opening_warning"].ne("").sum(),
})
store_daily = levels.loc[
    levels["level_description"].eq("门店") & levels["day_clear"].eq("2")
][[
    "business_date","total_sale_amount","inbound_amount","init_stock_amount","end_stock_amount",
    "known_lost_amt","unknown_lost_amt","store_profit_amount",
]]
print("STORE_DAILY", store_daily.to_dict(orient="records"))
print("STORE_WEEK", {
    "sale":float(store_daily["total_sale_amount"].sum()),
    "receive":float(store_daily["inbound_amount"].sum()),
    "opening":float(store_daily.loc[store_daily["business_date"].eq(START),"init_stock_amount"].sum()),
    "closing":float(store_daily.loc[store_daily["business_date"].eq(END),"end_stock_amount"].sum()),
    "known_lost":float(store_daily["known_lost_amt"].sum()),
    "unknown_lost":float(store_daily["unknown_lost_amt"].sum()),
    "profit":float(store_daily["store_profit_amount"].sum()),
})
adjusted_store = levels.loc[
    levels["level_description"].eq("门店") & levels["day_clear"].eq("2")
]
print("STORE_WEEK_ADJUSTED", {
    "adjusted_known_loss": float(adjusted_store["adjusted_known_lost_amt"].sum()),
    "adjusted_loss": float(adjusted_store["adjusted_lost_amt"].sum()),
    "adjusted_profit": float(adjusted_store["adjusted_profit"].sum()),
    "ccj": float(adjusted_store["ccj_amt"].sum()),
    "ssls": float(adjusted_store["ssls_amt"].sum()),
})

DB_PATH.parent.mkdir(parents=True, exist_ok=True)
conn = duckdb.connect(str(DB_PATH))
try:
    conn.execute("BEGIN")
    for table, frame in (
        ("shadow_opening_stock", openings),
        ("shadow_external_receipt", receipt_build.postings),
        ("shadow_external_receipt_reconciliation", receipt_build.reconciliation),
        ("shadow_external_receipt_quarantine", receipt_build.quarantined),
        ("shadow_internal_posting", ledger.internal_postings),
        ("shadow_daily_state", ledger.sku_daily),
        ("shadow_sku_daily", sku),
        ("shadow_special_wastage_trace", wastage_trace),
        ("shadow_levels_daily", levels),
        ("shadow_relation_resolution", resolution),
        ("shadow_relation_processing_snapshot", recipes),
        ("shadow_relation_bom_snapshot", bom_relation),
        ("shadow_relation_convert_snapshot", article_convert),
        ("shadow_run_manifest", run_manifest),
        ("shadow_source_manifest", source_manifest),
        ("shadow_day_clear_defaults", day_clear_defaults),
        ("shadow_pack_quarantine", pack_quarantine),
        ("shadow_compose_quarantine", processing_plan.quarantined),
        ("shadow_internal_cost_quarantine", cost_funded.quarantined),
    ):
        conn.register("_frame", frame)
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM _frame')
        conn.unregister("_frame")
    conn.execute("COMMIT")
except Exception:
    conn.execute("ROLLBACK")
    raise
finally:
    conn.close()
print("PUBLISHED", str(DB_PATH))

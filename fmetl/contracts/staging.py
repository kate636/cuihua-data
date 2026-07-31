from __future__ import annotations

from dataclasses import dataclass

import duckdb


@dataclass(frozen=True)
class StageTableContract:
    name: str
    required_columns: tuple[str, ...]
    store_scoped: bool = True


STAGE_CONTRACTS = (
    StageTableContract(
        "v014_stage_source_manifest",
        (
            "source_name", "source_table", "source_partition", "row_count",
            "checksum", "authority", "source_system", "hive_source_tables",
        ),
        store_scoped=False,
    ),
    StageTableContract(
        "v014_stage_source_completeness",
        ("store_id", "business_date", "source_name", "is_complete"),
    ),
    StageTableContract(
        "v014_stage_product_group",
        ("inc_day", "area_name", "article_group_id", "article_group_name", "article_id"),
        store_scoped=False,
    ),
    StageTableContract(
        "v014_stage_relation_candidates",
        ("store_id", "business_date", "source_article_id", "target_article_id"),
    ),
    StageTableContract(
        "v014_stage_bom",
        (
            "store_id", "business_date", "parent_article_id", "sub_article_id",
            "effective_from", "effective_to", "category_level1_description",
            "dressing_rate", "cost_rate", "approved",
        ),
    ),
    StageTableContract(
        "v014_stage_processing",
        (
            "store_id", "business_date", "relation_id", "raw_article_id",
            "finished_article_id", "raw_qty", "yield_qty", "effective_from",
            "effective_to", "approved",
        ),
    ),
    StageTableContract(
        "v014_stage_explicit_convert",
        (
            "store_id", "business_date", "source_article_id", "target_article_id",
            "effective_from", "effective_to", "actual_event", "fixed_rule",
            "convert_rate", "cost_rate", "approved",
        ),
    ),
    StageTableContract(
        "v014_stage_activities",
        (
            "store_id", "business_date", "article_id", "day_clear",
            "gross_sale_qty", "sale_return_qty", "net_sale_qty", "net_sale_amt",
            "known_lost_qty", "actual_stock_qty", "is_counted",
            "store_receive_qty", "store_receive_amt", "previous_stock_qty",
            "count_group_id", "code_role",
        ),
    ),
    StageTableContract(
        "v014_stage_openings",
        (
            "store_id", "article_id", "opening_qty", "opening_amt",
            "opening_source", "opening_source_day",
        ),
    ),
    StageTableContract(
        "v014_stage_conversion_events",
        (
            "store_id", "business_date", "event_group_id",
            "source_article_id", "target_article_id", "source_qty", "target_qty",
            "source_common_qty", "target_common_qty", "amount_allocation_ratio",
            "quantity_source",
        ),
    ),
    StageTableContract(
        "v014_stage_finished_processing_daily",
        (
            "store_id", "business_date", "article_id", "init_stock_qty",
            "end_stock_qty", "external_receive_qty", "net_sale_qty",
            "known_lost_qty", "other_internal_out_qty", "other_internal_in_qty",
            "has_valid_count",
        ),
    ),
    StageTableContract(
        "v014_stage_raw_available",
        ("store_id", "business_date", "article_id", "available_qty"),
    ),
    StageTableContract(
        "v014_stage_reporting_metrics",
        (
            "store_id", "business_date", "article_id", "store_flag", "store_no",
            "store_name", "sku_id", "sku_name", "category_level1_description",
            "category_level2_description", "category_level3_description", "spu_id",
            "spu_name", "sale_unit", "day_clear", "is_processing_raw",
        ),
    ),
    StageTableContract(
        "v014_stage_customer_events",
        (
            "store_id", "store_flag", "store_no", "business_date", "store_name", "sku_id",
            "sku_name", "category_level1_description",
            "category_level2_description", "category_level3_description",
            "spu_id", "spu_name", "day_clear", "order_id", "is_before_19",
            "is_online", "is_offline", "is_jielong", "is_jsd", "is_new",
            "is_old",
        ),
    ),
)


def validate_stage_database(
    conn: duckdb.DuckDBPyConnection,
    *,
    store_id: str,
) -> None:
    """Fail before calculation if the local original-fact stage is incomplete.

    This validates only the normalized local boundary. It performs no network
    reads and deliberately does not accept any v1.5 result table as an input.
    """
    existing = {
        str(row[0])
        for row in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
    }
    for contract in STAGE_CONTRACTS:
        if contract.name not in existing:
            raise KeyError(f"required local stage table does not exist: {contract.name}")
        columns = {
            str(row[0])
            for row in conn.execute(f'DESCRIBE "{contract.name}"').fetchall()
        }
        missing = sorted(set(contract.required_columns) - columns)
        if missing:
            raise KeyError(f"{contract.name} missing columns: {missing}")
        if contract.store_scoped:
            count = int(conn.execute(
                f'SELECT COUNT(*) FROM "{contract.name}" '
                "WHERE CAST(store_id AS VARCHAR)=?",
                [store_id],
            ).fetchone()[0])
            if count == 0 and contract.name not in {
                "v014_stage_bom", "v014_stage_processing",
                "v014_stage_explicit_convert", "v014_stage_conversion_events",
                "v014_stage_finished_processing_daily",
                "v014_stage_raw_available",
            }:
                raise ValueError(f"{contract.name} has no rows for store {store_id}")

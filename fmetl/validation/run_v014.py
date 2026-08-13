from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import gc
import hashlib
import json
import os
from pathlib import Path
import tempfile

import duckdb
import numpy as np
import pandas as pd

from fmetl import __version__
from fmetl.calculations.ledger import run_weighted_ledger
from fmetl.connectors.category_mapping import CategoryMappingSnapshot
from fmetl.master_data.category import (
    CategoryMapper, load_category_mapper, mapper_from_latest_snapshot,
)
from fmetl.validation.manifest import stable_frame_checksum
from fmetl.contracts.v014 import OUTPUT_CONTRACT

from fmetl.facts.inventory_inputs import normalize_inventory_inputs
from fmetl.outputs.levels_result import ADDITIVE_INPUTS, DIMENSION_COLUMNS, build_v014_levels_result
from fmetl.outputs.persistence import persist_v014_shadow
from fmetl.facts.processing_inference import infer_processing_postings
from fmetl.facts.formal_events import build_formal_event_legs
from fmetl.relations.registry import (
    build_product_group_candidates,
    freeze_product_group_snapshot,
    resolve_relation_registry,
)
from fmetl.validation.v014 import (
    assert_hard_gates,
    is_publishable,
    validate_category_evidence,
    validate_publishability,
    validate_v014_ledger,
)
from fmetl.contracts.staging import validate_stage_database
from fmetl.mirror.v014_source import DAILY_SOURCE_KEYS, LATEST_SOURCE_KEYS


REQUIRED_SOURCE_NAMES = frozenset({*DAILY_SOURCE_KEYS, *LATEST_SOURCE_KEYS})


@dataclass(frozen=True)
class ShadowWindow:
    start: str
    end: str
    days: tuple[str, ...]


@dataclass(frozen=True)
class V014RunResult:
    window: ShadowWindow
    db_path: Path
    row_count: int
    quarantine_count: int
    validation: pd.DataFrame


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?", [table]
    ).fetchone()[0])


def select_complete_window(
    completeness: pd.DataFrame,
    *,
    store_id: str,
    end: str,
    start: str | None = None,
    required_sources: frozenset[str] = REQUIRED_SOURCE_NAMES,
    lookback_days: int = 90,
) -> ShadowWindow:
    required = {"store_id", "business_date", "source_name", "is_complete"}
    missing = sorted(required - set(completeness.columns))
    if missing:
        raise KeyError(f"source completeness missing columns: {missing}")
    work = completeness.loc[completeness["store_id"].astype(str).eq(store_id)].copy()
    work = work.loc[work["is_complete"].map(
        lambda value: value if isinstance(value, bool)
        else str(value).strip().lower() in {"1", "true", "yes"}
    )]
    coverage = work.groupby(work["business_date"].astype(str))["source_name"].agg(set)
    complete_days = {day for day, sources in coverage.items() if required_sources.issubset(sources)}
    if not complete_days:
        raise ValueError("no day has all required v0.14 sources")
    if start is not None:
        start_day = date.fromisoformat(start)
        end_day = date.fromisoformat(end)
        if start_day > end_day:
            raise ValueError(f"v0.14 shadow start must not exceed end: {start} > {end}")
        days = tuple(
            (start_day + timedelta(days=offset)).isoformat()
            for offset in range((end_day - start_day).days + 1)
        )
        warmup_day = (start_day - timedelta(days=1)).isoformat()
        missing_days = sorted(set((warmup_day, *days)) - complete_days)
        if missing_days:
            raise ValueError(
                "explicit v0.14 window is incomplete; "
                f"missing required source days={missing_days}"
            )
        return ShadowWindow(days[0], days[-1], days)
    if end == "auto":
        upper = min(date.today() - timedelta(days=1), max(date.fromisoformat(day) for day in complete_days))
    else:
        upper = date.fromisoformat(end)
    for offset in range(lookback_days + 1):
        candidate_end = upper - timedelta(days=offset)
        days = tuple((candidate_end - timedelta(days=6 - index)).isoformat() for index in range(7))
        warmup_day = (date.fromisoformat(days[0]) - timedelta(days=1)).isoformat()
        if set((*days, warmup_day)).issubset(complete_days):
            return ShadowWindow(days[0], days[-1], days)
    raise ValueError(
        "no consecutive seven-day window plus D-1 warm-up has complete required sources"
    )


def _read_window(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    window: ShadowWindow,
    store_id: str,
    *,
    date_column: str = "business_date",
) -> pd.DataFrame:
    if not _table_exists(conn, table):
        raise KeyError(f"required local stage table does not exist: {table}")
    return conn.execute(
        f'SELECT * FROM "{table}" WHERE CAST(store_id AS VARCHAR)=? '
        f'AND CAST("{date_column}" AS VARCHAR) BETWEEN ? AND ?',
        [store_id, window.start, window.end],
    ).df()


def _read_store(conn: duckdb.DuckDBPyConnection, table: str, store_id: str) -> pd.DataFrame:
    if not _table_exists(conn, table):
        raise KeyError(f"required local stage table does not exist: {table}")
    return conn.execute(
        f'SELECT * FROM "{table}" WHERE CAST(store_id AS VARCHAR)=?', [store_id]
    ).df()


def _read_product_group(
    conn: duckdb.DuckDBPyConnection,
    window: ShadowWindow,
    *,
    article_ids: pd.Series | set[str] | None = None,
) -> pd.DataFrame:
    table = "v014_stage_product_group"
    if not _table_exists(conn, table):
        raise KeyError(f"required local stage table does not exist: {table}")
    filter_sql = ""
    registered = False
    if article_ids is not None:
        article_filter = pd.DataFrame({
            "article_id": sorted(set(map(str, article_ids)))
        })
        conn.register("_product_group_article_filter", article_filter)
        registered = True
        filter_sql = (
            " AND CAST(article_id AS VARCHAR) IN "
            "(SELECT article_id FROM _product_group_article_filter)"
        )
    try:
        return conn.execute(
            f'SELECT * FROM "{table}" '
            "WHERE CAST(inc_day AS VARCHAR) BETWEEN ? AND ? "
            "AND area_name IS NULL" + filter_sql,
            [window.start, window.end],
        ).df()
    finally:
        if registered:
            conn.unregister("_product_group_article_filter")


def _combine_quarantine(frames: list[pd.DataFrame]) -> pd.DataFrame:
    present = [frame for frame in frames if frame is not None and not frame.empty]
    if not present:
        return pd.DataFrame(columns=["store_id", "business_date", "article_id", "reason_code", "detail"])
    columns = sorted(set().union(*(frame.columns for frame in present)))
    return pd.concat([frame.reindex(columns=columns) for frame in present], ignore_index=True)


def _ledger_cost_quarantine(sku_daily: pd.DataFrame) -> pd.DataFrame:
    """Make every zero-cost issue explicit instead of silently accepting it."""
    outflow_qty = (
        sku_daily["gross_sale_qty"]
        + sku_daily["known_lost_qty"]
        + sku_daily["bom_out_qty"]
        + sku_daily["pack_out_qty"]
        + sku_daily["compose_out_qty"]
        + sku_daily["residual_transfer_out_qty"]
    )
    gap = sku_daily["issue_unit_cost"].le(0.000001) & outflow_qty.gt(0.001)
    if not gap.any():
        return pd.DataFrame(
            columns=["store_id", "business_date", "article_id", "reason_code", "detail"]
        )
    rows = sku_daily.loc[
        gap,
        [
            "store_id", "business_date", "article_id", "gross_sale_qty",
            "known_lost_qty", "store_receive_qty", "store_receive_amt",
            "init_stock_qty", "init_stock_amt",
        ],
    ].copy()
    rows["reason_code"] = "MISSING_COST_EVIDENCE"
    rows["detail"] = rows.apply(
        lambda row: (
            f"init=({row.init_stock_qty},{row.init_stock_amt});"
            f"receive=({row.store_receive_qty},{row.store_receive_amt});"
            f"sale={row.gross_sale_qty};known_loss={row.known_lost_qty}"
        ),
        axis=1,
    )
    return rows[["store_id", "business_date", "article_id", "reason_code", "detail"]]


def _spill_relation_audit(
    directory: Path,
    registry: pd.DataFrame,
    resolution: pd.DataFrame,
) -> Path:
    """Persist relation audit frames locally before the memory-heavy ledger."""
    directory.mkdir(parents=True, exist_ok=True)
    descriptor, raw_path = tempfile.mkstemp(
        prefix=".v014-relation-audit-", suffix=".duckdb", dir=directory
    )
    os.close(descriptor)
    path = Path(raw_path)
    path.unlink()
    audit = duckdb.connect(str(path))
    try:
        for table, frame in (
            ("v014_relation_registry", registry),
            ("v014_relation_resolution", resolution),
        ):
            audit.register("_frame", frame)
            audit.execute(f'CREATE TABLE "{table}" AS SELECT * FROM _frame')
            audit.unregister("_frame")
    finally:
        audit.close()
    return path


def _assemble_report_input(
    ledger: pd.DataFrame,
    metrics: pd.DataFrame,
    *,
    category_mapper: CategoryMapper | None = None,
) -> pd.DataFrame:
    keys = ["store_id", "business_date", "article_id"]
    ledger_derived = {
        "accounting_full_profit", "accounting_profit", "loss_amount", "loss_qty",
        "store_know_lost_amt", "store_unknow_lost_amt", "inbound_amount",
        "inbound_qty", "total_sale_qty", "total_sale_amount",
        "loss_rate_qty_denominator", "loss_rate_denominator",
        "store_expected_profit_amount", "store_pricing_profit_rate_numerator",
        "soldout_16_numerator", "soldout_16_denominator",
        "soldout_20_numerator", "soldout_20_denominator",
    }
    required_metrics = (
        (DIMENSION_COLUMNS - {"article_group_id", "article_group_name"})
        | (ADDITIVE_INPUTS - ledger_derived)
        | {
            "sale_unit", "has_observed_inventory", "original_price",
            "dc_original_price", "last_sale_hour",
        }
    )
    missing = sorted(required_metrics - set(metrics.columns))
    if missing:
        raise KeyError(f"v014 reporting metrics missing independent inputs: {missing}")
    if metrics.duplicated(keys).any():
        raise ValueError("v014 reporting metrics must be unique per SKU day")
    output = ledger.merge(metrics, on=keys, how="left", validate="one_to_one", suffixes=("", "_metric"))
    missing_rows = output["store_no"].isna()
    if missing_rows.any():
        sample = output.loc[missing_rows, keys].head(20).to_dict("records")
        raise ValueError(f"ledger rows are missing reporting metrics: {sample}")
    # The weighted ledger is the only public inventory and cost source.  Hive
    # purchase_di balances remain in the metric frame as audit observations,
    # but may never overwrite a rolled opening, counted ending, or accounting
    # profit.
    output["initial_inventory_amount"] = output["init_stock_amt"]
    output["ending_inventory_amount"] = output["end_amt"]
    output["end_stock_qty"] = output["end_qty"]
    output = output.drop(
        columns=[
            "init_stock_qty_metric", "end_stock_qty_metric",
            "initial_inventory_amount_metric", "ending_inventory_amount_metric",
        ],
        errors="ignore",
    )
    output["accounting_full_profit"] = (
        output["accounting_profit"] + output["supply_chain_profit_amount"]
    )
    output["loss_amount"] = output["accounting_lost_amt"]
    output["loss_qty"] = output["accounting_lost_qty"]
    output["store_know_lost_amt"] = output["accounting_known_lost_amt"]
    output["store_unknow_lost_amt"] = output["unknown_lost_amt"]
    output["inbound_amount"] = output["store_receive_amt"]
    output["inbound_qty"] = output["store_receive_qty"]
    output["total_sale_qty"] = output["net_sale_qty"]
    output["total_sale_amount"] = output["net_sale_amt"]
    output["loss_rate_qty_denominator"] = output["init_stock_qty"] + output["inbound_qty"]
    output["loss_rate_denominator"] = (
        output["initial_inventory_amount"] + output["inbound_amount"]
    )
    output["store_expected_profit_amount"] = (
        output["original_sale_amount"] - output["sale_cost_amt"]
    )
    expected_sale = (
        output["original_sale_amount"]
        + output["loss_qty"] * output["original_price"].fillna(0.0)
    )
    expected_inbound = np.where(
        output["dc_original_price"].fillna(0.0).gt(0),
        output["inbound_qty"] * output["dc_original_price"].fillna(0.0),
        output["inbound_amount"],
    )
    output["store_pricing_profit_rate_numerator"] = (
        expected_sale - expected_inbound
        - output["initial_inventory_amount"] + output["ending_inventory_amount"]
    )
    listed = output["stock_sku_flag"].gt(0)
    eligible = listed & ~output["sku_name"].fillna("").str.contains("J|ZC", regex=True)
    has_sale_time = output["last_sale_hour"].notna()
    output["soldout_16_denominator"] = (eligible & (has_sale_time | output["end_stock_qty"].gt(0.001))).astype(float)
    output["soldout_20_denominator"] = output["soldout_16_denominator"]
    output["soldout_16_numerator"] = (
        eligible & output["end_stock_qty"].abs().le(0.001)
        & has_sale_time & output["last_sale_hour"].lt(16)
    ).astype(float)
    output["soldout_20_numerator"] = (
        eligible & output["end_stock_qty"].abs().le(0.001)
        & has_sale_time & output["last_sale_hour"].lt(20)
    ).astype(float)
    mapper = category_mapper or load_category_mapper()
    mapped = mapper.map_frame(output)
    mapped["category_level1_description"] = mapped["report_category_name"]
    output = mapped.drop(
        columns=["report_category_name", "report_category_code", "category_rule_reason"],
        errors="ignore",
    )
    return output


def run_v014_shadow_week(
    *,
    store_id: str,
    end: str,
    start: str | None = None,
    source_db: Path | str,
    output_db: Path | str,
    category_snapshot: CategoryMappingSnapshot | None = None,
) -> V014RunResult:
    """Execute the current engine against compatible v014 physical tables.

    The runner performs no network calls and cannot write any server table.
    The stage is built by :mod:`fmetl.mirror.v014_stage` from Hive-backed
    mirrors. It is an internal calculation boundary, never a manually prepared
    business input or a v1.5 result-table dependency.
    """
    source_path = Path(source_db).resolve()
    output_path = Path(output_db).resolve()
    if source_path == output_path:
        raise ValueError(
            f"v{__version__} stage DB and output shadow DB must be different files"
        )
    if not source_path.exists():
        raise FileNotFoundError(
            f"local v{__version__} source DB not found: {source_path}"
        )
    relation_audit_path: Path | None = None
    conn = duckdb.connect(str(source_path), read_only=True)
    try:
        validate_stage_database(conn, store_id=store_id)
        source_manifest = conn.execute(
            'SELECT * FROM "v014_stage_source_manifest" ORDER BY source_name'
        ).df()
        bad_hive_lineage = source_manifest["source_system"].eq("HIVE_MIRROR") & source_manifest[
            "hive_source_tables"
        ].fillna("").eq("")
        if bad_hive_lineage.any():
            raise ValueError(
                f"v{__version__} Hive-mirror source missing field-manual lineage: "
                f"{source_manifest.loc[bad_hive_lineage, 'source_name'].tolist()}"
            )
        if not _table_exists(conn, "v014_stage_source_completeness"):
            raise KeyError("v014_stage_source_completeness is required for whole-window selection")
        completeness = conn.execute("SELECT * FROM v014_stage_source_completeness").df()
        window = select_complete_window(
            completeness, store_id=store_id, end=end, start=start
        )
        warmup_day = (
            date.fromisoformat(window.start) - timedelta(days=1)
        ).isoformat()
        compute_window = ShadowWindow(
            warmup_day, window.end, (warmup_day, *window.days)
        )
        candidates = _read_window(
            conn, "v014_stage_relation_candidates", compute_window, store_id
        )
        relation_articles = set(candidates["source_article_id"].astype(str)) | set(
            candidates["target_article_id"].astype(str)
        )
        product_group = freeze_product_group_snapshot(
            _read_product_group(
                conn, compute_window, article_ids=relation_articles
            )
        )
        del relation_articles
        bom = _read_window(conn, "v014_stage_bom", compute_window, store_id)
        processing = _read_window(
            conn, "v014_stage_processing", compute_window, store_id
        )
        explicit_convert = _read_window(
            conn, "v014_stage_explicit_convert", compute_window, store_id
        )
        group_candidates = build_product_group_candidates(candidates, product_group)
        relation_version = hashlib.sha256(
            pd.util.hash_pandas_object(
                pd.concat([bom, processing, explicit_convert], ignore_index=True, sort=False), index=True
            ).values.tobytes()
        ).hexdigest()[:20]
        registry, relation_quarantine = resolve_relation_registry(
            candidates,
            bom=bom,
            processing=processing,
            explicit_convert=explicit_convert,
            product_group_pairs=group_candidates,
            relation_version=relation_version,
        )
        relation_resolution = group_candidates.loc[
            group_candidates["same_product_group"].map(bool)
        ].copy()
        del group_candidates
        relation_audit_path = _spill_relation_audit(
            output_path.parent, registry, relation_resolution
        )
        del registry, relation_resolution
        gc.collect()
        stage_checksums = {
            "source_manifest": stable_frame_checksum(source_manifest),
            "completeness": stable_frame_checksum(completeness),
            "product_group": stable_frame_checksum(product_group),
            "relation_candidates": stable_frame_checksum(candidates),
            "bom": stable_frame_checksum(bom),
            "processing": stable_frame_checksum(processing),
            "explicit_convert": stable_frame_checksum(explicit_convert),
        }
        del candidates, bom, explicit_convert, product_group
        gc.collect()
        activities = _read_window(
            conn, "v014_stage_activities", compute_window, store_id
        )
        count_probe = activities[[
            "store_id", "business_date", "article_id", "actual_stock_qty",
            "previous_stock_qty", "count_group_id", "code_role",
        ]]
        normalized_counts = normalize_inventory_inputs(count_probe)
        activities = activities.drop(columns=["actual_stock_qty", "is_counted"], errors="ignore").merge(
            normalized_counts.normalized[[
                "store_id", "business_date", "article_id", "actual_stock_qty", "is_counted",
            ]],
            on=["store_id", "business_date", "article_id"], how="left", validate="one_to_one",
        )
        openings = _read_store(conn, "v014_stage_openings", store_id)
        conversion_events = _read_window(
            conn, "v014_stage_conversion_events", compute_window, store_id
        )
        stage_checksums["conversion_events"] = stable_frame_checksum(conversion_events)
        audit = duckdb.connect(str(relation_audit_path), read_only=True)
        try:
            active_registry = audit.execute(
                "SELECT * FROM v014_relation_registry "
                "WHERE formal_flow_allowed"
            ).df()
        finally:
            audit.close()
        formal_events = build_formal_event_legs(conversion_events, active_registry)
        del conversion_events
        gc.collect()
        base_sources = formal_events.sources.copy()
        base_targets = formal_events.targets.copy()
        finished_processing = _read_window(
            conn, "v014_stage_finished_processing_daily", compute_window, store_id
        )
        reserved_raw_loss = conn.execute(
            'SELECT CAST(store_id AS VARCHAR) AS store_id, '
            'CAST(business_date AS VARCHAR) AS business_date, '
            'CAST(article_id AS VARCHAR) AS article_id, '
            'SUM(COALESCE(ssls_qty, 0)) AS reserved_loss_qty '
            'FROM v014_stage_reporting_metrics '
            'WHERE CAST(store_id AS VARCHAR)=? '
            'AND CAST(business_date AS VARCHAR) BETWEEN ? AND ? '
            'GROUP BY 1, 2, 3 HAVING SUM(COALESCE(ssls_qty, 0)) > 0.001',
            [store_id, compute_window.start, compute_window.end],
        ).df()
        inferred = infer_processing_postings(
            finished_processing,
            processing,
            active_registry,
            relation_snapshot_id=relation_version,
            reserved_raw_loss=reserved_raw_loss,
        )
        sources = (
            pd.concat([base_sources, inferred.sources], ignore_index=True)
            if not inferred.sources.empty else base_sources.copy()
        )
        targets = (
            pd.concat([base_targets, inferred.targets], ignore_index=True)
            if not inferred.targets.empty else base_targets.copy()
        )
        stage_quarantine = (
            _read_window(conn, "v014_stage_quarantine", compute_window, store_id)
            if _table_exists(conn, "v014_stage_quarantine")
            else pd.DataFrame()
        )
        quarantine = _combine_quarantine([
            stage_quarantine,
            relation_quarantine, normalized_counts.quarantined,
            formal_events.quarantined, inferred.quarantined,
        ])
        del (
            finished_processing,
            base_sources, base_targets, formal_events,
            inferred, normalized_counts, relation_quarantine,
            active_registry,
        )
        gc.collect()
        ledger = run_weighted_ledger(activities, openings, sources, targets)
        quarantine = _combine_quarantine([
            quarantine, _ledger_cost_quarantine(ledger.sku_daily),
        ])
        validation = validate_v014_ledger(
            ledger.sku_daily,
            ledger.internal_postings,
            source_activities=activities,
            reserved_raw_loss=reserved_raw_loss,
            receipt_backed_processing=processing,
        )
        del reserved_raw_loss, processing
        assert_hard_gates(validation)
        stage_checksums["activities"] = stable_frame_checksum(activities)
        stage_checksums["openings"] = stable_frame_checksum(openings)
        sku_daily = ledger.sku_daily.loc[
            ledger.sku_daily["business_date"].astype(str).between(
                window.start, window.end
            )
        ].copy()
        internal_postings = ledger.internal_postings.loc[
            ledger.internal_postings["business_date"].astype(str).between(
                window.start, window.end
            )
        ].copy()
        if not quarantine.empty and "business_date" in quarantine:
            dated = quarantine["business_date"].notna()
            keep = ~dated | quarantine["business_date"].astype(str).between(
                window.start, window.end
            )
            quarantine = quarantine.loc[keep].copy()
        category_mapper = (
            mapper_from_latest_snapshot(category_snapshot)
            if category_snapshot is not None else load_category_mapper()
        )
        validation = pd.concat(
            [
                validation,
                validate_publishability(sku_daily),
                validate_category_evidence(
                    category_mapper,
                    start_date=window.start,
                    end_date=window.end,
                ),
            ],
            ignore_index=True,
        )
        # The seven-day report frame is wide. Release relation and inference
        # intermediates before building it so local shadow runs do not depend
        # on the workstation's transient memory pressure.
        del (
            activities, openings, sources, targets, ledger,
        )
        gc.collect()
        metrics = _read_window(conn, "v014_stage_reporting_metrics", window, store_id)
        stage_checksums["reporting_metrics"] = stable_frame_checksum(metrics)
        report_input = _assemble_report_input(
            sku_daily, metrics, category_mapper=category_mapper
        )
        del metrics
        gc.collect()
        activity_qty = (
            report_input["gross_sale_qty"].abs()
            + report_input["known_lost_qty"].abs()
            + report_input["store_receive_qty"].abs()
            + report_input["bom_out_qty"].abs()
            + report_input["pack_out_qty"].abs()
            + report_input["compose_out_qty"].abs()
            + report_input["residual_transfer_out_qty"].abs()
        )
        category_mapping_audit = (
            report_input.assign(_active=activity_qty.gt(0.000001))
            .groupby("sku_id", as_index=False, dropna=False)
            .agg(
                sku_name=("sku_name", "first"),
                category_level1_description=("category_level1_description", "first"),
                category_level2_description=("category_level2_description", "first"),
                category_level3_description=("category_level3_description", "first"),
                category_mapping_source=("category_mapping_source", "first"),
                active_in_window=("_active", "max"),
            )
        )
        platform_mapped_active = int((
            category_mapping_audit["active_in_window"].map(bool)
            & category_mapping_audit["category_mapping_source"].eq(
                "MONITORING_PLATFORM_LATEST"
            )
        ).sum())
        active_category_count = int(
            category_mapping_audit["active_in_window"].map(bool).sum()
        )
        latest_fallback_active = active_category_count - platform_mapped_active
        product_group = freeze_product_group_snapshot(
            _read_product_group(
                conn, window, article_ids=set(report_input["sku_id"].astype(str))
            )
        )
        report_input = report_input.drop(columns=["article_group_id", "article_group_name"], errors="ignore").merge(
            product_group.rename(columns={"article_id": "sku_id"}),
            on=["business_date", "sku_id"], how="left", validate="many_to_one",
        )
        customer_events = _read_window(
            conn, "v014_stage_customer_events", window, store_id
        )
        mapped_dimensions = report_input[[
            "business_date", "sku_id", "category_level1_description",
            "category_level2_description", "category_level3_description",
            "spu_id", "spu_name", "article_group_id", "article_group_name",
        ]].drop_duplicates(["business_date", "sku_id"])
        customer_events = customer_events.drop(
            columns=[
                "category_level1_description", "category_level2_description",
                "category_level3_description", "spu_id", "spu_name",
                "article_group_id", "article_group_name",
            ],
            errors="ignore",
        ).merge(
            mapped_dimensions,
            on=["business_date", "sku_id"],
            how="left",
            validate="many_to_one",
        )
        category_rule_version = str(report_input["category_rule_version"].iloc[0])
        levels = build_v014_levels_result(report_input, customer_events)
        del report_input, product_group, customer_events
        gc.collect()
        publishable = is_publishable(validation)
        publish_blockers = validation.loc[
            validation["gate_type"].eq("PUBLISH") & ~validation["passed"],
            "check_name",
        ].astype(str).tolist()
        manifest = pd.DataFrame([{
            "run_id": f"v{__version__}-{store_id}-{window.start}-{window.end}-{relation_version}",
            "engine_version": __version__,
            "store_id": store_id, "start_date": window.start, "end_date": window.end,
            "source_db": str(source_path), "output_db": str(output_path),
            "relation_version": relation_version, "required_sources": json.dumps(sorted(REQUIRED_SOURCE_NAMES)),
            "category_rule_version": category_rule_version,
            "category_rule_source": category_mapper.source,
            "category_override_source": category_mapper.cooked_override_source,
            "category_rule_checksum": category_mapper.rule_checksum,
            "category_evidence_status": category_mapper.evidence_status,
            "category_snapshot_start": category_mapper.snapshot_start,
            "category_snapshot_end": category_mapper.snapshot_end,
            "category_active_sku_count": active_category_count,
            "category_platform_mapped_active_sku_count": platform_mapped_active,
            "category_latest_goods_fallback_active_sku_count": latest_fallback_active,
            "stage_checksums": json.dumps(stage_checksums, sort_keys=True),
            "output_contract_sha256": hashlib.sha256(
                "\n".join(
                    f"{field.name}|{field.duckdb_type}|{field.label}" for field in OUTPUT_CONTRACT
                ).encode("utf-8")
            ).hexdigest(),
            "status": (
                "VALIDATED_LOCAL_SHADOW"
                if publishable else "DIAGNOSTIC_ONLY_PUBLISH_BLOCKED"
            ),
            "publish_eligible": publishable,
            "publish_blockers": json.dumps(publish_blockers, ensure_ascii=False),
            "server_write_count": 0,
            "source_provenance": "HIVE_MIRRORS_PLUS_RELATION_EVIDENCE",
        }])
        # Relation evidence for D-1 is needed to calculate the warm-up ledger,
        # but it is not part of the published seven-day output.  Materialize
        # only the selected report window into the final shadow database.
        audit = duckdb.connect(str(relation_audit_path), read_only=True)
        try:
            relation_registry = audit.execute(
                "SELECT * FROM v014_relation_registry "
                "WHERE business_date BETWEEN ? AND ?",
                [window.start, window.end],
            ).df()
            relation_resolution = audit.execute(
                "SELECT * FROM v014_relation_resolution "
                "WHERE business_date BETWEEN ? AND ?",
                [window.start, window.end],
            ).df()
        finally:
            audit.close()
        persist_v014_shadow(
            output_path,
            levels_result=levels,
            relation_registry=relation_registry,
            relation_resolution=relation_resolution,
            internal_posting=internal_postings,
            sku_daily=sku_daily,
            quarantine=quarantine,
            run_manifest=manifest,
            validation_result=validation,
            category_snapshot=(
                category_snapshot.frame if category_snapshot is not None else None
            ),
            category_mapping_audit=category_mapping_audit,
        )
        return V014RunResult(window, output_path, len(levels), len(quarantine), validation)
    finally:
        conn.close()
        if relation_audit_path is not None:
            relation_audit_path.unlink(missing_ok=True)

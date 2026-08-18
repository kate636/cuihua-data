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
from fmetl.connectors.category_mapping import CategoryMappingSnapshot
from fmetl.master_data.category import (
    CategoryMapper, load_category_mapper, mapper_from_latest_snapshot,
)
from fmetl.validation.manifest import stable_frame_checksum
from fmetl.contracts.v014 import OUTPUT_CONTRACT

from fmetl.facts.inventory_inputs import normalize_inventory_inputs
from fmetl.outputs.levels_result import ADDITIVE_INPUTS, DIMENSION_COLUMNS, build_v014_levels_result
from fmetl.outputs.persistence import persist_v014_shadow
from fmetl.facts.demand_backflush import (
    NON_BOM_RELATION_TYPES,
    audit_ssls_target_cost_coverage,
    run_ledger_with_demand_backflush,
)
from fmetl.facts.formal_events import build_formal_event_legs
from fmetl.outputs.category_adjustment import build_ssls_category_adjustments
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
from fmetl.validation.v018 import validate_v018_release_evidence
from fmetl.contracts.staging import validate_stage_database
from fmetl.mirror.v014_source import REQUIRED_CALCULATION_SOURCE_KEYS


REQUIRED_SOURCE_NAMES = REQUIRED_CALCULATION_SOURCE_KEYS


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


def _attach_processing_cost_audit(
    trace: pd.DataFrame,
    internal_postings: pd.DataFrame,
) -> pd.DataFrame:
    """Add the ledger amount and cost source for every posted demand conversion leg."""
    if trace.empty:
        return trace.assign(
            source_out_amt=pd.Series(dtype=float),
            source_issue_unit_cost=pd.Series(dtype=float),
            source_cost_source=pd.Series(dtype=str),
            target_in_amt=pd.Series(dtype=float),
            target_cost_source=pd.Series(dtype=str),
            event_source_out_amt=pd.Series(dtype=float),
            event_target_in_amt=pd.Series(dtype=float),
            event_amount_residual=pd.Series(dtype=float),
        )
    keys = ["store_id", "business_date", "event_group_id"]
    source = internal_postings.loc[
        internal_postings["posting_role"].eq("OUT"),
        keys + ["article_id", "amt", "cost_source"],
    ].rename(columns={
        "article_id": "source_article_id",
        "amt": "source_out_amt",
        "cost_source": "source_cost_source",
    })
    target = internal_postings.loc[
        internal_postings["posting_role"].eq("IN"),
        keys + ["article_id", "amt", "cost_source"],
    ].rename(columns={
        "article_id": "target_article_id",
        "amt": "target_in_amt",
        "cost_source": "target_cost_source",
    })
    result = trace.merge(
        source,
        on=keys + ["source_article_id"],
        how="left",
        validate="many_to_one",
    ).merge(
        target,
        on=keys + ["target_article_id"],
        how="left",
        validate="many_to_one",
    )
    if result[["source_out_amt", "target_in_amt"]].isna().any().any():
        raise ValueError("加工和换码审计明细无法逐腿匹配正式内部过账金额")
    result["source_issue_unit_cost"] = (
        result["source_out_amt"] / result["source_out_qty"]
    ).where(result["source_out_qty"].abs().gt(0.000001), 0.0)
    event_amounts = internal_postings.loc[
        internal_postings["event_group_id"].isin(result["event_group_id"])
    ].groupby(keys + ["posting_role"], as_index=False)["amt"].sum()
    event_amounts = event_amounts.pivot(
        index=keys, columns="posting_role", values="amt"
    ).reset_index().rename(columns={
        "OUT": "event_source_out_amt", "IN": "event_target_in_amt",
    })
    result = result.merge(event_amounts, on=keys, how="left", validate="many_to_one")
    result["event_amount_residual"] = (
        result["event_target_in_amt"] - result["event_source_out_amt"]
    )
    return result


def _build_bom_cost_quantity_audit(
    bom_events: pd.DataFrame,
    internal_postings: pd.DataFrame,
) -> pd.DataFrame:
    """Return one auditable row per BOM child leg with quantity and amount evidence."""
    if bom_events.empty:
        return bom_events.copy()
    keys = ["store_id", "business_date", "event_group_id"]
    source = internal_postings.loc[
        internal_postings["relation_type"].eq("DISASSEMBLY_BOM")
        & internal_postings["posting_role"].eq("OUT"),
        keys + ["article_id", "qty", "amt", "cost_source", "relation_snapshot_id"],
    ].rename(columns={
        "article_id": "source_article_id",
        "qty": "ledger_parent_out_qty",
        "amt": "ledger_parent_out_amt",
        "cost_source": "parent_cost_source",
    })
    target = internal_postings.loc[
        internal_postings["relation_type"].eq("DISASSEMBLY_BOM")
        & internal_postings["posting_role"].eq("IN"),
        keys + ["article_id", "qty", "amt", "cost_source"],
    ].rename(columns={
        "article_id": "target_article_id",
        "qty": "ledger_child_in_qty",
        "amt": "ledger_child_in_amt",
        "cost_source": "child_cost_source",
    })
    result = bom_events.merge(
        source,
        on=keys + ["source_article_id"],
        how="left",
        validate="many_to_one",
    ).merge(
        target,
        on=keys + ["target_article_id"],
        how="left",
        validate="one_to_one",
    )
    required_amounts = [
        "ledger_parent_out_qty", "ledger_parent_out_amt",
        "ledger_child_in_qty", "ledger_child_in_amt",
    ]
    if result[required_amounts].isna().any().any():
        raise ValueError("BOM 审计明细无法逐腿匹配正式内部过账数量或金额")
    result["ledger_child_parent_unit_qty"] = (
        result["ledger_child_in_qty"] * result["sub_rate"]
    )
    ledger_parent_unit_sum = result.groupby(keys)[
        "ledger_child_parent_unit_qty"
    ].transform("sum")
    result["ledger_quantity_residual"] = (
        result["ledger_parent_out_qty"] - ledger_parent_unit_sum
    )
    ledger_child_amt_sum = result.groupby(keys)["ledger_child_in_amt"].transform("sum")
    result["ledger_amount_residual"] = (
        result["ledger_parent_out_amt"] - ledger_child_amt_sum
    )
    return result


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
        raise ValueError("no day has all required v0.18 sources")
    if start is not None:
        start_day = date.fromisoformat(start)
        end_day = date.fromisoformat(end)
        if start_day > end_day:
            raise ValueError(f"v0.18 local run start must not exceed end: {start} > {end}")
        days = tuple(
            (start_day + timedelta(days=offset)).isoformat()
            for offset in range((end_day - start_day).days + 1)
        )
        warmup_day = (start_day - timedelta(days=1)).isoformat()
        missing_days = sorted(set((warmup_day, *days)) - complete_days)
        if missing_days:
            raise ValueError(
                "explicit v0.18 window is incomplete; "
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


def _relation_evidence_version(**frames: pd.DataFrame) -> str:
    """Hash every input that can change a relation direction or ratio."""
    payload = "\n".join(
        f"{name}:{stable_frame_checksum(frame)}"
        for name, frame in sorted(frames.items())
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def _annotate_relation_audit(
    registry: pd.DataFrame,
    *,
    processing_trace: pd.DataFrame,
    demand_quarantine: pd.DataFrame,
    bom_events: pd.DataFrame,
) -> pd.DataFrame:
    """State the observed quantity or the direct reason each relation did not post."""
    output = registry.drop(
        columns=["trigger_demand_qty", "exclusion_reason"], errors="ignore"
    ).copy()
    output["trigger_demand_qty"] = 0.0
    active = output["status"].eq("ACTIVE")
    formal = output["formal_flow_allowed"].map(bool)
    evidence_only_bom = (
        output["relation_type"].eq("BOM")
        & output["status"].eq("EVIDENCE_ONLY")
    )
    output["exclusion_reason"] = np.where(
        evidence_only_bom,
        "NO_OBSERVED_RECEIVE_SALE_BOM_EVENT",
        np.where(
            ~active,
            "RELATION_STATUS_" + output["status"].fillna("UNKNOWN").astype(str),
            np.where(
                ~formal,
                "FORMAL_FLOW_NOT_ALLOWED",
                np.where(
                    output["relation_type"].eq("BOM"),
                    "NO_OBSERVED_RECEIVE_SALE_BOM_EVENT",
                    np.where(
                        output["relation_type"].eq("SAME_SKU"),
                        "SAME_SKU_NO_INTERNAL_POSTING",
                        "NO_REMAINING_DEMAND",
                    ),
                ),
            ),
        ),
    )

    if not processing_trace.empty:
        posted = processing_trace.groupby("relation_id", as_index=False)[
            "trigger_demand_qty"
        ].sum().rename(columns={"trigger_demand_qty": "_posted_trigger"})
        output = output.merge(posted, on="relation_id", how="left", validate="one_to_one")
        posted_mask = output["_posted_trigger"].notna()
        output.loc[posted_mask, "trigger_demand_qty"] = output.loc[
            posted_mask, "_posted_trigger"
        ]
        output.loc[posted_mask, "exclusion_reason"] = ""
        output = output.drop(columns="_posted_trigger")

    if not demand_quarantine.empty:
        excluded = demand_quarantine.groupby(
            ["store_id", "business_date", "article_id"], as_index=False
        ).agg(
            _excluded_trigger=("trigger_demand_qty", "max"),
            _excluded_reason=(
                "reason_code",
                lambda values: ",".join(sorted(set(map(str, values)))),
            ),
        ).rename(columns={"article_id": "target_article_id"})
        relation_keys = ["store_id", "business_date", "target_article_id"]
        output = output.merge(
            excluded, on=relation_keys, how="left", validate="many_to_one"
        )
        excluded_mask = (
            output["_excluded_reason"].notna()
            & output["trigger_demand_qty"].le(0.001)
            & output["status"].eq("ACTIVE")
            & output["formal_flow_allowed"].map(bool)
            & output["relation_type"].isin(NON_BOM_RELATION_TYPES)
        )
        output.loc[excluded_mask, "trigger_demand_qty"] = output.loc[
            excluded_mask, "_excluded_trigger"
        ]
        output.loc[excluded_mask, "exclusion_reason"] = output.loc[
            excluded_mask, "_excluded_reason"
        ]
        output = output.drop(columns=["_excluded_trigger", "_excluded_reason"])

    if not bom_events.empty:
        bom_trigger = bom_events.groupby(
            [
                "store_id", "business_date", "source_article_id",
                "target_article_id",
            ],
            as_index=False,
        )["target_qty"].sum().rename(columns={"target_qty": "_bom_event_qty"})
        relation_keys = [
            "store_id", "business_date", "source_article_id", "target_article_id",
        ]
        output = output.merge(
            bom_trigger, on=relation_keys, how="left", validate="one_to_one"
        )
        bom_mask = output["relation_type"].eq("BOM") & output[
            "_bom_event_qty"
        ].notna()
        output.loc[bom_mask, "trigger_demand_qty"] = output.loc[
            bom_mask, "_bom_event_qty"
        ]
        output.loc[bom_mask, "exclusion_reason"] = ""
        output = output.drop(columns="_bom_event_qty")
    return output


def _ledger_cost_quarantine(
    sku_daily: pd.DataFrame,
    *,
    ssls_covered_targets: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Make every zero-cost issue explicit instead of silently accepting it."""
    regular_issue_qty = (
        sku_daily["gross_sale_qty"]
        + sku_daily["known_lost_qty"]
        + sku_daily["pack_out_qty"]
        + sku_daily["compose_out_qty"]
        + sku_daily["residual_transfer_out_qty"]
    )
    missing_issue_cost = (
        sku_daily["issue_unit_cost"].le(0.000001)
        & regular_issue_qty.gt(0.001)
    )
    if ssls_covered_targets is not None and not ssls_covered_targets.empty:
        covered = set(map(tuple, ssls_covered_targets[[
            "store_id", "business_date", "article_id",
        ]].astype(str).to_numpy()))
        row_keys = list(zip(
            sku_daily["store_id"].astype(str),
            sku_daily["business_date"].astype(str),
            sku_daily["article_id"].astype(str),
        ))
        missing_issue_cost &= ~pd.Series(
            [key in covered for key in row_keys], index=sku_daily.index
        )
    missing_return_cost = (
        sku_daily["sale_return_qty"].gt(0.001)
        & sku_daily["sale_return_cost_basis"].le(0.000001)
    )
    missing_bom_cost = (
        sku_daily["bom_out_qty"].gt(0.001)
        & sku_daily["bom_out_amt"].le(0.000001)
    )
    gap = missing_issue_cost | missing_return_cost | missing_bom_cost
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
            f"期初数量={row.init_stock_qty}，期初金额={row.init_stock_amt}；"
            f"实际验收量={row.store_receive_qty}，实际验收额={row.store_receive_amt}；"
            f"正向销售量={row.gross_sale_qty}，已知报损量={row.known_lost_qty}；"
            f"普通流出缺成本={'是' if bool(missing_issue_cost.loc[row.name]) else '否'}；"
            f"BOM转出缺金额={'是' if bool(missing_bom_cost.loc[row.name]) else '否'}；"
            f"销售退货缺成本={'是' if bool(missing_return_cost.loc[row.name]) else '否'}。"
            "该记录保留事实但阻止发布"
        ),
        axis=1,
    )
    return rows[["store_id", "business_date", "article_id", "reason_code", "detail"]]


def _missing_cost_audit(
    quarantine: pd.DataFrame,
    sku_daily: pd.DataFrame,
    category_mapping_audit: pd.DataFrame,
) -> pd.DataFrame:
    """List the outflow and same-day cost evidence behind each missing-cost row."""
    missing = quarantine.loc[
        quarantine.get("reason_code", "").astype(str).str.contains(
            "MISSING_COST_EVIDENCE", na=False
        ),
        ["store_id", "business_date", "article_id", "reason_code", "detail"],
    ].drop_duplicates(["store_id", "business_date", "article_id"]).copy()
    evidence_columns = [
        "store_id", "business_date", "article_id", "gross_sale_qty",
        "sale_return_qty", "known_lost_qty", "bom_out_qty", "bom_out_amt",
        "pack_out_qty", "compose_out_qty", "residual_transfer_out_qty",
        "init_stock_qty", "init_stock_amt", "store_receive_qty",
        "store_receive_amt", "bom_in_qty", "bom_in_amt", "pack_in_qty",
        "pack_in_amt", "compose_in_qty", "compose_in_amt",
        "residual_transfer_in_qty", "residual_transfer_in_amt",
        "fallback_cost", "fallback_cost_source", "issue_unit_cost",
        "issue_cost_source", "sale_return_cost_basis",
    ]
    dimensions = category_mapping_audit.rename(columns={"sku_id": "article_id"})[[
        "article_id", "sku_name", "category_level1_description",
        "category_level2_description", "category_level3_description",
        "category_mapping_source",
    ]].drop_duplicates("article_id")
    output = missing.merge(
        sku_daily[evidence_columns],
        on=["store_id", "business_date", "article_id"],
        how="left", validate="one_to_one",
    ).merge(dimensions, on="article_id", how="left", validate="many_to_one")
    regular_out = output[
        [
            "gross_sale_qty", "known_lost_qty", "pack_out_qty",
            "compose_out_qty", "residual_transfer_out_qty",
        ]
    ].fillna(0.0).sum(axis=1)
    output.insert(
        5,
        "missing_cost_scope",
        np.select(
            [
                output["bom_out_qty"].fillna(0.0).gt(0.001)
                & output["bom_out_amt"].fillna(0.0).le(0.000001),
                output["sale_return_qty"].fillna(0.0).gt(0.001)
                & output["sale_return_cost_basis"].fillna(0.0).le(0.000001),
                regular_out.gt(0.001)
                & output["issue_unit_cost"].fillna(0.0).le(0.000001),
            ],
            ["BOM_TRANSFER", "SALE_RETURN", "REGULAR_OUTFLOW"],
            default="MULTIPLE_OR_UNCLASSIFIED",
        ),
    )
    return output.sort_values(
        ["business_date", "category_level1_description", "article_id"]
    ).reset_index(drop=True)


def _day_clear_evidence_audit(
    day_clear_audit: pd.DataFrame,
    sku_daily: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Mark active SKU-days whose non-day-clear default has no chdj evidence."""
    keys = ["store_id", "business_date", "article_id"]
    required = {*keys, "day_clear", "day_clear_source"}
    missing = sorted(required - set(day_clear_audit.columns))
    if missing:
        raise KeyError(f"day-clear audit missing columns: {missing}")
    flow_columns = [
        "init_stock_qty", "end_qty", "gross_sale_qty", "sale_return_qty",
        "known_lost_qty", "store_receive_qty", "bom_in_qty", "bom_out_qty",
        "pack_in_qty", "pack_out_qty", "compose_in_qty", "compose_out_qty",
        "residual_transfer_in_qty", "residual_transfer_out_qty",
    ]
    active = sku_daily[keys + flow_columns].copy()
    active["active_qty"] = active[flow_columns].abs().sum(axis=1)
    output = day_clear_audit.merge(
        active[keys + ["active_qty"]], on=keys, how="left", validate="one_to_one"
    )
    output["active_qty"] = output["active_qty"].fillna(0.0)
    output["active_in_compute_window"] = output["active_qty"].gt(0.000001)
    gap = (
        output["active_in_compute_window"]
        & output["day_clear_source"].eq(
            "MISSING_AUTHORITATIVE_DEFAULT_NON_DAY_CLEAR"
        )
    )
    quarantined = output.loc[gap, keys].copy()
    quarantined["reason_code"] = "MISSING_AUTHORITATIVE_DAY_CLEAR"
    quarantined["detail"] = (
        "该 SKU 日期存在库存或业务流动，但 chdj_article 没有权威日清标签；"
        "为生成诊断结果暂按非日清计算，并阻止发布"
    )
    return output, quarantined


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
        "ccj_ledger_cost_amt", "ssls_ledger_cost_amt",
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
    output["ccj_ledger_cost_amt"] = output["ccj_qty"] * output["issue_unit_cost"]
    output["ssls_ledger_cost_amt"] = output["ssls_qty"] * output["issue_unit_cost"]
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
    mirror_source_db: Path | str | None = None,
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
            f"local v{__version__} stage DB not found: {source_path}"
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
        relation_version = _relation_evidence_version(
            bom=bom,
            processing=processing,
            explicit_convert=explicit_convert,
            purchase_direction_candidates=candidates,
            product_group_snapshot=product_group,
        )
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
        inventory_count_source_audit = _read_window(
            conn, "v014_stage_inventory_count_audit", compute_window, store_id
        )
        special_loss_coverage = _read_window(
            conn, "v014_stage_special_loss_coverage", compute_window, store_id
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
        bom_events = conversion_events.loc[
            conversion_events["event_group_id"].astype(str).str.startswith("BOM|")
        ].copy()
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
        stage_quarantine = (
            _read_window(conn, "v014_stage_quarantine", compute_window, store_id)
            if _table_exists(conn, "v014_stage_quarantine")
            else pd.DataFrame()
        )
        quarantine = _combine_quarantine([
            stage_quarantine,
            relation_quarantine, normalized_counts.quarantined,
            formal_events.quarantined,
        ])
        demand_result = run_ledger_with_demand_backflush(
            activities,
            openings,
            base_sources,
            base_targets,
            active_registry,
            reserved_raw_loss=reserved_raw_loss,
        )
        ledger = demand_result.ledger
        sources = demand_result.sources
        targets = demand_result.targets
        processing_trace = demand_result.trace
        demand_quarantine = demand_result.quarantined.copy()
        ssls_priority_targets = demand_result.quarantined.loc[
            demand_result.quarantined["reason_code"].eq(
                "PROCESSING_RAW_LOSS_PRIORITY"
            ),
            [
                "store_id", "business_date", "article_id", "detail",
                "trigger_demand_qty",
            ],
        ].copy()
        ssls_target_cost_audit = audit_ssls_target_cost_coverage(
            ssls_priority_targets,
            active_registry,
            reserved_raw_loss,
            ledger.sku_daily,
        )
        ssls_target_cost_coverage = ssls_target_cost_audit.loc[
            ssls_target_cost_audit["covered"].map(bool)
        ].copy()
        quarantine = _combine_quarantine([
            quarantine, demand_result.quarantined,
        ])
        del (
            base_sources, base_targets, formal_events, demand_result,
            normalized_counts, relation_quarantine, active_registry,
        )
        gc.collect()
        quarantine = _combine_quarantine([
            quarantine,
            _ledger_cost_quarantine(
                ledger.sku_daily,
                ssls_covered_targets=ssls_target_cost_coverage,
            ),
        ])
        day_clear_evidence, day_clear_evidence_quarantine = (
            _day_clear_evidence_audit(
                _read_window(
                    conn, "v014_stage_day_clear_audit", compute_window, store_id
                ),
                ledger.sku_daily,
            )
        )
        quarantine = _combine_quarantine([
            quarantine, day_clear_evidence_quarantine,
        ])
        compute_sku_daily = ledger.sku_daily
        compute_internal_postings = ledger.internal_postings.copy()
        processing_trace = _attach_processing_cost_audit(
            processing_trace, compute_internal_postings
        )
        bom_posting_detail = _build_bom_cost_quantity_audit(
            bom_events, compute_internal_postings
        )
        count_keys = ["store_id", "business_date", "article_id"]
        inventory_count_ledger_audit = inventory_count_source_audit.merge(
            compute_sku_daily[count_keys + [
                "is_counted", "actual_stock_qty", "end_qty", "branch",
            ]].rename(columns={
                "is_counted": "ledger_is_counted",
                "actual_stock_qty": "ledger_actual_stock_qty",
                "end_qty": "ledger_end_qty",
                "branch": "ledger_branch",
            }),
            on=count_keys,
            how="left",
            validate="one_to_one",
        )
        inventory_count_ledger_audit["ledger_is_counted"] = (
            inventory_count_ledger_audit["ledger_is_counted"].fillna(False).map(bool)
        )
        inventory_count_ledger_audit["ledger_use_status"] = "未进入账本，保留来源记录供审计"
        inventory_count_ledger_audit.loc[
            inventory_count_ledger_audit["ledger_is_counted"], "ledger_use_status"
        ] = "正式人工盘点覆盖当日期末数量"
        compute_quarantine = quarantine
        compute_ssls_target_cost_coverage = ssls_target_cost_coverage
        compute_ssls_target_cost_audit = ssls_target_cost_audit
        validation = validate_v014_ledger(
            compute_sku_daily,
            ledger.internal_postings,
            source_activities=activities,
            reserved_raw_loss=reserved_raw_loss,
            processing_trace=processing_trace,
            special_loss_coverage=special_loss_coverage,
        )
        del reserved_raw_loss, processing
        assert_hard_gates(validation)
        stage_checksums["activities"] = stable_frame_checksum(activities)
        stage_checksums["openings"] = stable_frame_checksum(openings)
        opening_issues = openings.loc[
            openings.get("opening_status", "VALID").astype(str).ne("VALID")
        ].copy()
        sku_daily = compute_sku_daily.loc[
            compute_sku_daily["business_date"].astype(str).between(
                window.start, window.end
            )
        ].copy()
        ssls_target_cost_coverage = compute_ssls_target_cost_coverage.loc[
            compute_ssls_target_cost_coverage["business_date"].astype(str).between(
                window.start, window.end
            )
        ].copy()
        ssls_target_cost_audit = compute_ssls_target_cost_audit.loc[
            compute_ssls_target_cost_audit["business_date"].astype(str).between(
                window.start, window.end
            )
        ].copy()
        special_loss_coverage = special_loss_coverage.loc[
            special_loss_coverage["business_date"].astype(str).between(
                window.start, window.end
            )
        ].copy()
        internal_postings = ledger.internal_postings.loc[
            ledger.internal_postings["business_date"].astype(str).between(
                window.start, window.end
            )
        ].copy()
        quarantine = compute_quarantine
        category_mapper = (
            mapper_from_latest_snapshot(category_snapshot)
            if category_snapshot is not None else load_category_mapper()
        )
        validation = pd.concat(
            [
                validation,
                validate_publishability(
                    compute_sku_daily,
                    ssls_covered_targets=compute_ssls_target_cost_coverage,
                ),
                validate_category_evidence(
                    category_mapper,
                    start_date=compute_window.start,
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
        metrics = _read_window(
            conn, "v014_stage_reporting_metrics", compute_window, store_id
        )
        stage_checksums["reporting_metrics"] = stable_frame_checksum(metrics)
        scm_margin_audit = _read_window(
            conn, "v014_stage_scm_margin_audit", window, store_id
        )
        stage_checksums["scm_margin_audit"] = stable_frame_checksum(
            scm_margin_audit
        )
        compute_report_input = _assemble_report_input(
            compute_sku_daily, metrics, category_mapper=category_mapper
        )
        del metrics
        gc.collect()
        activity_qty = (
            compute_report_input["init_stock_qty"].abs()
            + compute_report_input["end_stock_qty"].abs()
            + compute_report_input["gross_sale_qty"].abs()
            + compute_report_input["sale_return_qty"].abs()
            + compute_report_input["known_lost_qty"].abs()
            + compute_report_input["store_receive_qty"].abs()
            + compute_report_input["bom_in_qty"].abs()
            + compute_report_input["bom_out_qty"].abs()
            + compute_report_input["pack_in_qty"].abs()
            + compute_report_input["pack_out_qty"].abs()
            + compute_report_input["compose_in_qty"].abs()
            + compute_report_input["compose_out_qty"].abs()
            + compute_report_input["residual_transfer_in_qty"].abs()
            + compute_report_input["residual_transfer_out_qty"].abs()
        )
        category_mapping_audit = (
            compute_report_input.assign(_active=activity_qty.gt(0.000001))
            .groupby("sku_id", as_index=False, dropna=False)
            .agg(
                sku_name=("sku_name", "first"),
                category_level1_description=("category_level1_description", "first"),
                category_level2_description=("category_level2_description", "first"),
                category_level3_description=("category_level3_description", "first"),
                category_mapping_source=("category_mapping_source", "first"),
                category_authoritative_level1_description=(
                    "category_authoritative_level1_description", "first"
                ),
                active_in_window=("_active", "max"),
            )
        )
        missing_cost_audit = _missing_cost_audit(
            compute_quarantine, compute_sku_daily, category_mapping_audit
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
        report_input = compute_report_input.loc[
            compute_report_input["business_date"].astype(str).between(
                window.start, window.end
            )
        ].copy()
        del compute_report_input
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
        special_loss_cost_audit = report_input[[
            "store_id", "business_date", "sku_id", "sku_name",
            "category_level1_description", "ccj_qty", "ccj_amt",
            "ccj_ledger_cost_amt", "ssls_qty", "ssls_amt",
            "ssls_ledger_cost_amt",
        ]].copy()
        special_loss_cost_audit = special_loss_cost_audit.loc[
            special_loss_cost_audit[["ccj_qty", "ssls_qty"]].sum(axis=1).gt(0.001)
        ]
        special_loss_cost_audit["ccj_source_minus_ledger_amt"] = (
            special_loss_cost_audit["ccj_amt"]
            - special_loss_cost_audit["ccj_ledger_cost_amt"]
        )
        special_loss_cost_audit["ssls_source_minus_ledger_amt"] = (
            special_loss_cost_audit["ssls_amt"]
            - special_loss_cost_audit["ssls_ledger_cost_amt"]
        )
        category_adjustments = build_ssls_category_adjustments(report_input)
        levels = build_v014_levels_result(report_input, customer_events)
        del report_input, product_group, customer_events
        gc.collect()
        # The warm-up day determines the first published opening. Keep its
        # relation evidence and diagnostic rows even though public levels start
        # on window.start.
        audit = duckdb.connect(str(relation_audit_path), read_only=True)
        try:
            relation_registry = audit.execute(
                "SELECT * FROM v014_relation_registry "
                "WHERE business_date BETWEEN ? AND ?",
                [compute_window.start, compute_window.end],
            ).df()
            relation_resolution = audit.execute(
                "SELECT * FROM v014_relation_resolution "
                "WHERE business_date BETWEEN ? AND ?",
                [compute_window.start, compute_window.end],
            ).df()
        finally:
            audit.close()
        relation_registry = _annotate_relation_audit(
            relation_registry,
            processing_trace=processing_trace,
            demand_quarantine=demand_quarantine,
            bom_events=bom_events,
        )
        source_completeness = completeness.loc[
            completeness["business_date"].astype(str).between(
                compute_window.start, compute_window.end
            )
        ].copy()
        validation = pd.concat([
            validation,
            validate_v018_release_evidence(
                relation_registry=relation_registry,
                quarantine=quarantine,
                category_adjustments=category_adjustments,
                source_completeness=source_completeness,
                category_mapping_audit=category_mapping_audit,
                levels_result=levels,
                ssls_target_cost_audit=compute_ssls_target_cost_audit,
                inventory_count_audit=inventory_count_source_audit,
                sku_daily=compute_sku_daily,
            ),
        ], ignore_index=True)
        assert_hard_gates(validation)
        publishable = is_publishable(validation)
        failed_publish = validation.loc[
            validation["gate_type"].eq("PUBLISH") & ~validation["passed"]
        ].copy()
        publish_blockers = failed_publish["check_name"].astype(str).tolist()
        publish_blocking_issues = failed_publish.rename(columns={
            "check_name": "reason_code",
        })[["reason_code", "failure_count", "detail"]]
        manifest = pd.DataFrame([{
            "run_id": f"v{__version__}-{store_id}-{window.start}-{window.end}-{relation_version}",
            "engine_version": __version__,
            "store_id": store_id, "start_date": window.start, "end_date": window.end,
            "compute_start_date": compute_window.start,
            # source_db is retained as a compatibility field and is the stage
            # database consumed by this runner. The two explicit fields remove
            # any ambiguity between the raw mirror cache and normalized stage.
            "source_db": str(source_path),
            "mirror_source_db": (
                str(Path(mirror_source_db).resolve())
                if mirror_source_db is not None else ""
            ),
            "stage_db": str(source_path),
            "output_db": str(output_path),
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
            "source_provenance": "LOCAL_STARROCKS_MIRROR_SNAPSHOT",
        }])
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
            audit_tables={
                "v018_compute_sku_daily": compute_sku_daily,
                "v018_compute_internal_posting": compute_internal_postings,
                "v018_compute_ssls_target_cost_audit": compute_ssls_target_cost_audit,
                "v018_inventory_count_audit": inventory_count_ledger_audit,
                "v018_category_profit_adjustment": category_adjustments,
                "v018_processing_backflush_detail": processing_trace,
                "v018_ssls_target_cost_coverage": ssls_target_cost_coverage,
                "v018_ssls_target_cost_audit": ssls_target_cost_audit,
                "v018_special_loss_coverage": special_loss_coverage,
                "v018_special_loss_cost_audit": special_loss_cost_audit,
                "v018_scm_margin_audit": scm_margin_audit,
                "v018_opening_issue": opening_issues,
                "v018_source_completeness": source_completeness,
                "v018_day_clear_audit": day_clear_evidence,
                "v018_publish_blocking_issue": publish_blocking_issues,
                "v018_missing_cost": missing_cost_audit,
                "v018_isolation_detail": quarantine,
                "v018_bom_posting_detail": bom_posting_detail,
            },
        )
        return V014RunResult(window, output_path, len(levels), len(quarantine), validation)
    finally:
        conn.close()
        if relation_audit_path is not None:
            relation_audit_path.unlink(missing_ok=True)

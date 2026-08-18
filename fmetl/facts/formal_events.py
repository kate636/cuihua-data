from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from fmetl.calculations.ledger import (
    SOURCE_COLUMNS,
    SOURCE_OPTIONAL_COLUMNS,
    TARGET_COLUMNS,
)


@dataclass(frozen=True)
class FormalEventPlan:
    sources: pd.DataFrame
    targets: pd.DataFrame
    trace: pd.DataFrame
    quarantined: pd.DataFrame


FLOW_TYPES = {
    "BOM": "DISASSEMBLY_BOM",
    "EXPLICIT_CONVERT": "PACK_CONVERT",
}


def build_formal_event_legs(
    events: pd.DataFrame,
    relation_registry: pd.DataFrame,
    *,
    qty_tolerance: float = 0.002,
) -> FormalEventPlan:
    """Turn observed BOM and fixed-rule conversion events into ledger entries.

    Parent and child quantities keep their source units. For BOM, common target
    quantity is the reported child quantity converted back to the parent unit
    with article_convert ctype=1. Relation type comes from the dated registry.
    An invalid group is excluded as a whole.
    """
    event_required = {
        "store_id", "business_date", "event_group_id", "source_article_id",
        "target_article_id", "source_qty", "source_amount", "target_qty", "source_common_qty",
        "target_common_qty", "amount_allocation_ratio", "quantity_source",
    }
    registry_required = {
        "store_id", "business_date", "source_article_id", "target_article_id",
        "relation_type", "relation_version", "status", "formal_flow_allowed",
    }
    for label, frame, required in (
        ("conversion_events", events, event_required),
        ("relation_registry", relation_registry, registry_required),
    ):
        missing = sorted(required - set(frame.columns))
        if missing:
            raise KeyError(f"{label} missing columns: {missing}")
    if events.empty:
        return FormalEventPlan(
            pd.DataFrame(columns=[*SOURCE_COLUMNS, *SOURCE_OPTIONAL_COLUMNS]),
            pd.DataFrame(columns=TARGET_COLUMNS),
            pd.DataFrame(columns=sorted(event_required) + ["relation_type"]),
            pd.DataFrame(columns=["store_id", "business_date", "event_group_id", "reason_code", "detail"]),
        )
    keys = ["store_id", "business_date", "source_article_id", "target_article_id"]
    event = events.copy()
    if event[keys + ["event_group_id", "quantity_source"]].isna().any().any():
        raise ValueError("conversion event keys and quantity evidence cannot contain NULL")
    event[[*keys, "event_group_id", "quantity_source"]] = event[
        [*keys, "event_group_id", "quantity_source"]
    ].astype(str)
    for column in (
        "source_qty", "source_amount", "target_qty", "source_common_qty", "target_common_qty",
        "amount_allocation_ratio",
    ):
        event[column] = pd.to_numeric(event[column], errors="raise")
        if event[column].isna().any() or not np.isfinite(event[column].to_numpy(dtype=float)).all():
            raise ValueError(f"conversion_events.{column} must be finite and non-null")
        if event[column].lt(-qty_tolerance).any():
            raise ValueError(f"conversion_events.{column} cannot be negative")
    formal_mask = (
        relation_registry["status"].eq("ACTIVE")
        & relation_registry["formal_flow_allowed"].map(bool)
        & relation_registry["relation_type"].isin(FLOW_TYPES)
    )
    registry = relation_registry.loc[
        formal_mask, keys + ["relation_type", "relation_version"]
    ].copy()
    registry[keys] = registry[keys].astype(str)
    if registry.duplicated(keys).any():
        raise ValueError("formal relation registry must be unique per dated pair")
    joined = event.merge(registry, on=keys, how="left", validate="many_to_one")

    source_rows: list[dict[str, object]] = []
    target_rows: list[dict[str, object]] = []
    trace_rows: list[dict[str, object]] = []
    quarantine_rows: list[dict[str, object]] = []
    event_keys = ["store_id", "business_date", "event_group_id"]
    for event_key, group in joined.groupby(event_keys, sort=False, dropna=False):
        store, day, event_id = map(str, event_key)
        reasons: list[str] = []
        if group["relation_type"].isna().any():
            reasons.append("EVENT_RELATION_NOT_FORMAL")
        relation_types = group["relation_type"].dropna().astype(str).unique()
        snapshots = group["relation_version"].dropna().astype(str).unique()
        if len(relation_types) != 1 or len(snapshots) != 1:
            reasons.append("EVENT_RELATION_CONFLICT")
        if group["quantity_source"].str.strip().eq("").any():
            reasons.append("EVENT_QUANTITY_EVIDENCE_MISSING")
        source_consistency = group.groupby("source_article_id").agg(
            qty_min=("source_qty", "min"), qty_max=("source_qty", "max"),
            amount_min=("source_amount", "min"), amount_max=("source_amount", "max"),
            common_min=("source_common_qty", "min"), common_max=("source_common_qty", "max"),
        )
        if (
            source_consistency["qty_max"].sub(source_consistency["qty_min"]).abs().gt(qty_tolerance).any()
            or source_consistency["amount_max"].sub(source_consistency["amount_min"]).abs().gt(0.01).any()
            or source_consistency["common_max"].sub(source_consistency["common_min"]).abs().gt(qty_tolerance).any()
        ):
            reasons.append("EVENT_SOURCE_QUANTITY_CONFLICT")
        source_common = float(source_consistency["common_max"].sum())
        target_common = float(group["target_common_qty"].sum())
        if abs(source_common - target_common) > qty_tolerance:
            reasons.append("EVENT_SOURCE_ALLOCATION_INCOMPLETE")
        allocation_sum = float(group["amount_allocation_ratio"].sum())
        if abs(allocation_sum - 1.0) > 0.000001:
            reasons.append("EVENT_AMOUNT_ALLOCATION_NOT_ONE")
        if reasons:
            quarantine_rows.append({
                "store_id": store, "business_date": day, "event_group_id": event_id,
                "reason_code": ",".join(dict.fromkeys(reasons)),
                "detail": (
                    f"来源折算数量={source_common};目标折算数量={target_common};"
                    f"目标金额分配比例合计={allocation_sum}。数量或金额分配未通过校验，因此整组不入账"
                ),
            })
            continue
        relation_type = str(relation_types[0])
        ledger_type = FLOW_TYPES[relation_type]
        snapshot = str(snapshots[0])
        quantity_source = ",".join(sorted(set(group["quantity_source"])))
        for source_id, source in source_consistency.iterrows():
            source_rows.append({
                "store_id": store, "business_date": day, "event_group_id": event_id,
                "relation_type": ledger_type, "source_article_id": str(source_id),
                "source_out_qty": float(source["qty_max"]),
                "quantity_source": quantity_source, "relation_snapshot_id": snapshot,
                "specified_source_out_amt": float(source["amount_max"]),
                "specified_cost_source": "RECEIVE_SALE_PARENT_RECEIPT_AMOUNT",
            })
        for row in group.itertuples(index=False):
            target_rows.append({
                "store_id": store, "business_date": day, "event_group_id": event_id,
                "relation_type": ledger_type, "target_article_id": str(row.target_article_id),
                "target_in_qty": float(row.target_qty),
                "amount_allocation_ratio": float(row.amount_allocation_ratio),
                "quantity_source": quantity_source, "relation_snapshot_id": snapshot,
            })
            trace_rows.append({
                **row._asdict(), "ledger_relation_type": ledger_type,
                "common_qty_residual": source_common - target_common,
            })
    return FormalEventPlan(
        pd.DataFrame(source_rows, columns=[*SOURCE_COLUMNS, *SOURCE_OPTIONAL_COLUMNS]),
        pd.DataFrame(target_rows, columns=TARGET_COLUMNS),
        pd.DataFrame(trace_rows),
        pd.DataFrame(quarantine_rows),
    )

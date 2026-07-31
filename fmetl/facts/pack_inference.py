from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from fmetl.calculations.ledger import SOURCE_COLUMNS, TARGET_COLUMNS


@dataclass(frozen=True)
class InferredPackPlan:
    sources: pd.DataFrame
    targets: pd.DataFrame
    trace: pd.DataFrame
    quarantined: pd.DataFrame


def _empty(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def infer_fixed_pack_postings(
    activities: pd.DataFrame,
    relation_registry: pd.DataFrame,
    *,
    tolerance: float = 0.001,
) -> InferredPackPlan:
    """Infer a fixed package conversion from the source-SKU count equation.

    A same-SPU relationship plus a ratio is not itself a daily flow.  Posting
    occurs only when two consecutive valid source counts show a positive,
    otherwise unexplained reduction.  The reduction is the source quantity and
    the dated fixed ratio determines the target quantity.
    """
    activity_required = {
        "store_id", "business_date", "article_id", "gross_sale_qty",
        "sale_return_qty", "known_lost_qty", "store_receive_qty",
        "actual_stock_qty", "previous_stock_qty", "is_counted",
    }
    registry_required = {
        "store_id", "business_date", "source_article_id", "target_article_id",
        "relation_type", "quantity_rate", "relation_version", "status",
        "formal_flow_allowed",
    }
    for label, frame, required in (
        ("activities", activities, activity_required),
        ("relation_registry", relation_registry, registry_required),
    ):
        missing = sorted(required - set(frame.columns))
        if missing:
            raise KeyError(f"{label} missing columns: {missing}")

    formal = relation_registry.loc[
        relation_registry["relation_type"].eq("EXPLICIT_CONVERT")
        & relation_registry["status"].eq("ACTIVE")
        & relation_registry["formal_flow_allowed"].map(bool)
    ].copy()
    if formal.empty:
        return InferredPackPlan(
            _empty(SOURCE_COLUMNS), _empty(TARGET_COLUMNS), pd.DataFrame(),
            pd.DataFrame(),
        )
    keys = ["store_id", "business_date", "source_article_id"]
    formal[keys + ["target_article_id", "relation_version"]] = formal[
        keys + ["target_article_id", "relation_version"]
    ].astype(str)
    formal["quantity_rate"] = pd.to_numeric(formal["quantity_rate"], errors="raise")
    if (
        formal["quantity_rate"].isna().any()
        or ~np.isfinite(formal["quantity_rate"].to_numpy(dtype=float)).all()
        or formal["quantity_rate"].le(0).any()
    ):
        raise ValueError("active fixed-pack relations require a positive finite quantity_rate")

    activity = activities.copy().rename(columns={"article_id": "source_article_id"})
    activity[keys] = activity[keys].astype(str)
    for column in (
        "gross_sale_qty", "sale_return_qty", "known_lost_qty",
        "store_receive_qty", "actual_stock_qty", "previous_stock_qty",
    ):
        activity[column] = pd.to_numeric(activity[column], errors="coerce")
    joined = formal.merge(activity, on=keys, how="left", validate="many_to_one")

    source_rows: list[dict[str, object]] = []
    target_rows: list[dict[str, object]] = []
    trace_rows: list[dict[str, object]] = []
    quarantine_rows: list[dict[str, object]] = []
    for key, group in joined.groupby(keys, sort=False, dropna=False):
        store, day, source_id = map(str, key)
        targets = group["target_article_id"].dropna().astype(str).unique()
        if len(targets) != 1 or len(group) != 1:
            quarantine_rows.append({
                "store_id": store, "business_date": day, "article_id": source_id,
                "reason_code": "PACK_SOURCE_RELATION_AMBIGUOUS",
                "detail": ",".join(sorted(targets)),
            })
            continue
        row = group.iloc[0]
        current = float(row["actual_stock_qty"]) if pd.notna(row["actual_stock_qty"]) else np.nan
        previous = float(row["previous_stock_qty"]) if pd.notna(row["previous_stock_qty"]) else np.nan
        valid_count = bool(row.get("is_counted", False))
        if not valid_count or not np.isfinite(current) or not np.isfinite(previous):
            continue
        source_out = (
            previous
            + float(row["store_receive_qty"])
            + float(row["sale_return_qty"])
            - float(row["gross_sale_qty"])
            - float(row["known_lost_qty"])
            - current
        )
        if source_out < -tolerance:
            quarantine_rows.append({
                "store_id": store, "business_date": day, "article_id": source_id,
                "reason_code": "PACK_SOURCE_COUNT_EQUATION_NEGATIVE",
                "detail": str(source_out),
            })
            continue
        if source_out <= tolerance:
            continue
        target_id = str(row["target_article_id"])
        target_in = source_out * float(row["quantity_rate"])
        snapshot = str(row["relation_version"])
        event_id = f"PACK_INFER|{store}|{day}|{source_id}|{target_id}"
        quantity_source = "SOURCE_INVENTORY_EQUATION"
        source_rows.append({
            "store_id": store, "business_date": day,
            "event_group_id": event_id, "relation_type": "PACK_CONVERT",
            "source_article_id": source_id, "source_out_qty": source_out,
            "quantity_source": quantity_source,
            "relation_snapshot_id": snapshot,
        })
        target_rows.append({
            "store_id": store, "business_date": day,
            "event_group_id": event_id, "relation_type": "PACK_CONVERT",
            "target_article_id": target_id, "target_in_qty": target_in,
            "amount_allocation_ratio": 1.0,
            "quantity_source": quantity_source,
            "relation_snapshot_id": snapshot,
        })
        trace_rows.append({
            "store_id": store, "business_date": day,
            "event_group_id": event_id, "source_article_id": source_id,
            "target_article_id": target_id, "previous_stock_qty": previous,
            "current_stock_qty": current, "source_out_qty": source_out,
            "quantity_rate": float(row["quantity_rate"]),
            "target_in_qty": target_in, "quantity_source": quantity_source,
        })
    return InferredPackPlan(
        pd.DataFrame(source_rows, columns=SOURCE_COLUMNS),
        pd.DataFrame(target_rows, columns=TARGET_COLUMNS),
        pd.DataFrame(trace_rows),
        pd.DataFrame(quarantine_rows),
    )

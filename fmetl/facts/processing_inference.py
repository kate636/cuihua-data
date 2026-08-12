from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from fmetl.calculations.ledger import SOURCE_COLUMNS, TARGET_COLUMNS


@dataclass(frozen=True)
class InferredProcessingPlan:
    sources: pd.DataFrame
    targets: pd.DataFrame
    trace: pd.DataFrame
    quarantined: pd.DataFrame


def _empty(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _truthy(value: object) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes"}


def infer_processing_postings(
    finished_daily: pd.DataFrame,
    recipes: pd.DataFrame,
    relation_registry: pd.DataFrame,
    *,
    relation_snapshot_id: str,
    reserved_raw_loss: pd.DataFrame | None = None,
    tolerance: float = 0.001,
) -> InferredProcessingPlan:
    """Backflush processing from consumed finished quantity only.

    Finished sales plus ordinary known loss proves consumed output.  A dated,
    active recipe converts that output to raw usage with ``raw_qty/yield_qty``.
    Raw book balance does not gate the event: if the confirmed usage overdraws
    the raw inventory pool, the main ledger records a negative-clamp quantity
    and prices the shortage with the same auditable cost fallback used for
    sales.  This preserves the real usage instead of leaving sold finished
    goods at zero cost.

    SSLS raw loss and processing are mutually exclusive, with SSLS taking
    priority. Multiple active relation IDs for one finished SKU remain
    quarantined because the API does not identify a recipe group or state
    whether the raw materials are additive or alternatives.
    """
    finished_required = {
        "store_id", "business_date", "article_id", "net_sale_qty",
        "known_lost_qty",
    }
    recipe_required = {
        "store_id", "business_date", "relation_id", "raw_article_id",
        "finished_article_id", "raw_qty", "yield_qty", "effective_from",
        "effective_to", "approved",
    }
    registry_required = {
        "store_id", "business_date", "source_article_id", "target_article_id",
        "relation_type", "status",
    }
    for label, frame, required in (
        ("finished_daily", finished_daily, finished_required),
        ("recipes", recipes, recipe_required),
        ("relation_registry", relation_registry, registry_required),
    ):
        missing = sorted(required - set(frame.columns))
        if missing:
            raise KeyError(f"{label} missing columns: {missing}")
    if not relation_snapshot_id:
        raise ValueError("relation_snapshot_id is required")
    keys = ["store_id", "business_date", "article_id"]
    if finished_daily.duplicated(keys).any():
        raise ValueError("finished_daily must be unique per SKU day")

    reserved_columns = ("store_id", "business_date", "article_id", "reserved_loss_qty")
    if reserved_raw_loss is None:
        reserved = _empty(reserved_columns)
    else:
        missing = sorted(set(reserved_columns) - set(reserved_raw_loss.columns))
        if missing:
            raise KeyError(f"reserved_raw_loss missing columns: {missing}")
        reserved = reserved_raw_loss[list(reserved_columns)].copy()
        if reserved.duplicated(keys).any():
            raise ValueError("reserved_raw_loss must be unique per SKU day")
        reserved["reserved_loss_qty"] = pd.to_numeric(
            reserved["reserved_loss_qty"], errors="raise"
        )
        if (
            reserved["reserved_loss_qty"].isna().any()
            or not np.isfinite(reserved["reserved_loss_qty"].to_numpy(dtype=float)).all()
            or reserved["reserved_loss_qty"].lt(-tolerance).any()
        ):
            raise ValueError("reserved_raw_loss.reserved_loss_qty must be finite and nonnegative")
        reserved[keys] = reserved[keys].astype(str)
    reserved_index = reserved.set_index(keys)["reserved_loss_qty"]

    finished = finished_daily.copy()
    finished[keys] = finished[keys].astype(str)
    finished[["net_sale_qty", "known_lost_qty"]] = finished[[
        "net_sale_qty", "known_lost_qty"
    ]].apply(pd.to_numeric, errors="raise")
    recipe = recipes.copy()
    for column in (
        "external_finished_receipt_qty", "external_finished_receipt_amt",
    ):
        if column not in recipe:
            recipe[column] = 0.0
    recipe[["raw_qty", "yield_qty"]] = recipe[["raw_qty", "yield_qty"]].apply(
        pd.to_numeric, errors="raise"
    )
    recipe[[
        "external_finished_receipt_qty", "external_finished_receipt_amt",
    ]] = recipe[[
        "external_finished_receipt_qty", "external_finished_receipt_amt",
    ]].apply(pd.to_numeric, errors="raise")
    if (
        not np.isfinite(recipe[["raw_qty", "yield_qty"]].to_numpy(dtype=float)).all()
        or recipe[["raw_qty", "yield_qty"]].le(0).any().any()
    ):
        raise ValueError("recipe raw_qty and yield_qty must be finite and positive")

    formal = relation_registry.loc[
        relation_registry["relation_type"].eq("PROCESSING")
        & relation_registry["status"].eq("ACTIVE")
    ]
    formal_pairs = set(
        formal[["store_id", "business_date", "source_article_id", "target_article_id"]]
        .astype(str).itertuples(index=False, name=None)
    )
    source_rows: list[dict[str, object]] = []
    target_rows: list[dict[str, object]] = []
    trace_rows: list[dict[str, object]] = []
    quarantine_rows: list[dict[str, object]] = []

    for item in finished.itertuples(index=False):
        store = str(item.store_id)
        day = str(item.business_date)
        finished_id = str(item.article_id)
        active = recipe.loc[
            recipe["store_id"].astype(str).eq(store)
            & recipe["business_date"].astype(str).eq(day)
            & recipe["finished_article_id"].astype(str).eq(finished_id)
            & recipe["effective_from"].astype(str).le(day)
            & (
                recipe["effective_to"].isna()
                | recipe["effective_to"].astype(str).ge(day)
            )
            & recipe["approved"].map(_truthy)
        ]
        if active.empty:
            continue
        relation_ids = active["relation_id"].astype(str).unique()
        if len(relation_ids) != 1:
            quarantine_rows.append({
                "store_id": store, "business_date": day,
                "article_id": finished_id,
                "reason_code": "MULTIPLE_ACTIVE_PROCESSING_RECIPES",
                "detail": ",".join(sorted(relation_ids)),
            })
            continue
        raw_loss_priority = []
        for edge in active.itertuples(index=False):
            raw_id = str(edge.raw_article_id)
            reserved_qty = float(reserved_index.get((store, day, raw_id), 0.0))
            if reserved_qty > tolerance:
                raw_loss_priority.append(f"{raw_id}:reserved_loss={reserved_qty}")
        if raw_loss_priority:
            quarantine_rows.append({
                "store_id": store, "business_date": day,
                "article_id": finished_id,
                "reason_code": "PROCESSING_RAW_LOSS_PRIORITY",
                "detail": ";".join(raw_loss_priority),
            })
            continue
        receipt_backed = active.loc[
            active["external_finished_receipt_qty"].gt(tolerance)
        ]
        if not receipt_backed.empty:
            evidence = ";".join(
                f"{row.raw_article_id}->{finished_id}:"
                f"qty={row.external_finished_receipt_qty}:"
                f"amt={row.external_finished_receipt_amt}"
                for row in receipt_backed.itertuples(index=False)
            )
            quarantine_rows.append({
                "store_id": store, "business_date": day,
                "article_id": finished_id,
                "reason_code": "PROCESSING_EXTERNAL_RECEIPT_PRIORITY",
                "detail": evidence,
            })
            continue
        finished_reserved = float(
            reserved_index.get((store, day, finished_id), 0.0)
        )
        ordinary_loss = max(
            0.0, float(item.known_lost_qty) - finished_reserved
        )
        output_qty = max(0.0, float(item.net_sale_qty) + ordinary_loss)
        if output_qty <= tolerance:
            continue
        invalid = [
            str(edge.raw_article_id)
            for edge in active.itertuples(index=False)
            if (store, day, str(edge.raw_article_id), finished_id)
            not in formal_pairs
        ]
        if invalid:
            quarantine_rows.append({
                "store_id": store, "business_date": day,
                "article_id": finished_id,
                "reason_code": "PROCESSING_RELATION_NOT_FORMAL",
                "detail": ",".join(sorted(invalid)),
            })
            continue
        relation_id = relation_ids[0]
        event_id = f"PROCESSING|{store}|{day}|{relation_id}"
        target_rows.append({
            "store_id": store, "business_date": day,
            "event_group_id": event_id, "relation_type": "RECIPE_COMPOSE",
            "target_article_id": finished_id, "target_in_qty": output_qty,
            "amount_allocation_ratio": 1.0,
            "quantity_source": "FINISHED_USAGE_BACKFLUSH",
            "relation_snapshot_id": relation_snapshot_id,
        })
        for edge in active.itertuples(index=False):
            raw_id = str(edge.raw_article_id)
            required_qty = output_qty * float(edge.raw_qty) / float(edge.yield_qty)
            source_rows.append({
                "store_id": store, "business_date": day,
                "event_group_id": event_id, "relation_type": "RECIPE_COMPOSE",
                "source_article_id": raw_id, "source_out_qty": required_qty,
                "quantity_source": "FINISHED_USAGE_BACKFLUSH",
                "relation_snapshot_id": relation_snapshot_id,
            })
            trace_rows.append({
                "store_id": store, "business_date": day,
                "event_group_id": event_id, "relation_id": relation_id,
                "raw_article_id": raw_id, "finished_article_id": finished_id,
                "raw_out_qty": required_qty, "finished_in_qty": output_qty,
                "raw_qty": float(edge.raw_qty), "yield_qty": float(edge.yield_qty),
                "amount_allocation_ratio": 1.0,
                "quantity_source": "FINISHED_USAGE_BACKFLUSH",
            })

    return InferredProcessingPlan(
        pd.DataFrame(source_rows, columns=SOURCE_COLUMNS),
        pd.DataFrame(target_rows, columns=TARGET_COLUMNS),
        pd.DataFrame(trace_rows),
        pd.DataFrame(quarantine_rows),
    )

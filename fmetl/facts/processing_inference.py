from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class InferredProcessingPlan:
    sources: pd.DataFrame
    targets: pd.DataFrame
    trace: pd.DataFrame
    quarantined: pd.DataFrame


SOURCE_COLUMNS = (
    "store_id", "business_date", "event_group_id", "relation_type",
    "source_article_id", "source_out_qty", "quantity_source", "relation_snapshot_id",
)
TARGET_COLUMNS = (
    "store_id", "business_date", "event_group_id", "relation_type",
    "target_article_id", "target_in_qty", "amount_allocation_ratio",
    "quantity_source", "relation_snapshot_id",
)


def _empty(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def infer_processing_postings(
    finished_daily: pd.DataFrame,
    raw_available: pd.DataFrame,
    recipes: pd.DataFrame,
    relation_registry: pd.DataFrame,
    *,
    relation_snapshot_id: str,
    tolerance: float = 0.001,
) -> InferredProcessingPlan:
    """Infer finished output from the finished-SKU inventory equation.

    `compose_di` is deliberately not an input.  A formal posting requires a
    nonnegative count for the finished SKU, an active dated recipe, no external
    finished receipt, and enough raw material.  Failure is quarantined before
    it can be absorbed by unknown loss.
    """
    finished_required = {
        "store_id", "business_date", "article_id", "init_stock_qty",
        "end_stock_qty", "external_receive_qty", "net_sale_qty", "known_lost_qty",
        "other_internal_out_qty", "other_internal_in_qty", "has_valid_count",
    }
    raw_required = {"store_id", "business_date", "article_id", "available_qty"}
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
        ("raw_available", raw_available, raw_required),
        ("recipes", recipes, recipe_required),
        ("relation_registry", relation_registry, registry_required),
    ):
        missing = sorted(required - set(frame.columns))
        if missing:
            raise KeyError(f"{label} missing columns: {missing}")
    if not relation_snapshot_id:
        raise ValueError("relation_snapshot_id is required")
    if finished_daily.duplicated(["store_id", "business_date", "article_id"]).any():
        raise ValueError("finished_daily must be unique per SKU day")
    if raw_available.duplicated(["store_id", "business_date", "article_id"]).any():
        raise ValueError("raw_available must be unique per SKU day")

    finished = finished_daily.copy()
    raw = raw_available.copy()
    numeric_finished = [
        "init_stock_qty", "end_stock_qty", "external_receive_qty", "net_sale_qty",
        "known_lost_qty", "other_internal_out_qty", "other_internal_in_qty",
    ]
    for column in numeric_finished:
        finished[column] = pd.to_numeric(finished[column], errors="raise")
    raw["available_qty"] = pd.to_numeric(raw["available_qty"], errors="raise")
    recipe = recipes.copy()
    recipe["raw_qty"] = pd.to_numeric(recipe["raw_qty"], errors="raise")
    recipe["yield_qty"] = pd.to_numeric(recipe["yield_qty"], errors="raise")
    if ((recipe["raw_qty"] <= 0) | (recipe["yield_qty"] <= 0)).any():
        raise ValueError("recipe raw_qty and yield_qty must be positive")
    raw_index = raw.set_index(["store_id", "business_date", "article_id"])["available_qty"]
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
        store, day, finished_id = str(item.store_id), str(item.business_date), str(item.article_id)
        active = recipe.loc[
            recipe["store_id"].astype(str).eq(store)
            & recipe["business_date"].astype(str).eq(day)
            & recipe["finished_article_id"].astype(str).eq(finished_id)
            & recipe["effective_from"].astype(str).le(day)
            & (recipe["effective_to"].isna() | recipe["effective_to"].astype(str).ge(day))
            & recipe["approved"].map(
                lambda value: value if isinstance(value, (bool, np.bool_))
                else str(value).strip().lower() in {"1", "true", "yes"}
            )
        ]
        if active.empty:
            continue
        relation_ids = active["relation_id"].astype(str).unique()
        if len(relation_ids) != 1:
            quarantine_rows.append({
                "store_id": store, "business_date": day, "article_id": finished_id,
                "reason_code": "MULTIPLE_ACTIVE_PROCESSING_RECIPES",
                "detail": ",".join(sorted(relation_ids)),
            })
            continue
        if not bool(item.has_valid_count):
            quarantine_rows.append({
                "store_id": store, "business_date": day, "article_id": finished_id,
                "reason_code": "PROCESSING_MISSING_FINISHED_COUNT", "detail": "",
            })
            continue
        if float(item.external_receive_qty) > tolerance:
            quarantine_rows.append({
                "store_id": store, "business_date": day, "article_id": finished_id,
                "reason_code": "PROCESSING_FINISHED_EXTERNAL_RECEIPT", "detail": str(item.external_receive_qty),
            })
            continue
        output_qty = (
            float(item.end_stock_qty)
            - float(item.init_stock_qty)
            - float(item.external_receive_qty)
            + float(item.net_sale_qty)
            + float(item.known_lost_qty)
            + float(item.other_internal_out_qty)
            - float(item.other_internal_in_qty)
        )
        if output_qty < -tolerance:
            quarantine_rows.append({
                "store_id": store, "business_date": day, "article_id": finished_id,
                "reason_code": "NEGATIVE_INFERRED_PROCESSING_OUTPUT", "detail": str(output_qty),
            })
            continue
        if output_qty <= tolerance:
            continue
        relation_id = relation_ids[0]
        event_id = f"PROCESSING|{store}|{day}|{relation_id}"
        relation_edges: list[tuple[object, float, float]] = []
        insufficient: list[str] = []
        for edge in active.itertuples(index=False):
            raw_id = str(edge.raw_article_id)
            pair = (store, day, raw_id, finished_id)
            if pair not in formal_pairs:
                insufficient.append(f"{raw_id}:relation_not_formal")
                continue
            required_qty = output_qty * float(edge.raw_qty) / float(edge.yield_qty)
            key = (store, day, raw_id)
            available_qty = float(raw_index.get(key, np.nan))
            if not np.isfinite(available_qty) or available_qty + tolerance < required_qty:
                insufficient.append(f"{raw_id}:need={required_qty}:available={available_qty}")
            relation_edges.append((edge, required_qty, available_qty))
        if insufficient:
            quarantine_rows.append({
                "store_id": store, "business_date": day, "article_id": finished_id,
                "reason_code": "PROCESSING_RAW_UNAVAILABLE", "detail": ";".join(insufficient),
            })
            continue
        allocation = 1.0 / len(relation_edges)
        # Allocation is only a temporary target-leg placeholder. The ledger
        # sums all raw source values and transfers the complete amount to the
        # single finished target, so the target ratio must be exactly one.
        target_rows.append({
            "store_id": store, "business_date": day, "event_group_id": event_id,
            "relation_type": "RECIPE_COMPOSE", "target_article_id": finished_id,
            "target_in_qty": output_qty, "amount_allocation_ratio": 1.0,
            "quantity_source": "FINISHED_INVENTORY_EQUATION",
            "relation_snapshot_id": relation_snapshot_id,
        })
        for edge, required_qty, available_qty in relation_edges:
            raw_id = str(edge.raw_article_id)
            source_rows.append({
                "store_id": store, "business_date": day, "event_group_id": event_id,
                "relation_type": "RECIPE_COMPOSE", "source_article_id": raw_id,
                "source_out_qty": required_qty,
                "quantity_source": "FINISHED_INVENTORY_EQUATION",
                "relation_snapshot_id": relation_snapshot_id,
            })
            trace_rows.append({
                "store_id": store, "business_date": day, "event_group_id": event_id,
                "relation_id": relation_id, "raw_article_id": raw_id,
                "finished_article_id": finished_id, "raw_out_qty": required_qty,
                "finished_in_qty": output_qty, "raw_available_qty": available_qty,
                "raw_qty": float(edge.raw_qty), "yield_qty": float(edge.yield_qty),
                "amount_allocation_ratio": allocation,
                "quantity_source": "FINISHED_INVENTORY_EQUATION",
            })
    return InferredProcessingPlan(
        sources=pd.DataFrame(source_rows, columns=SOURCE_COLUMNS),
        targets=pd.DataFrame(target_rows, columns=TARGET_COLUMNS),
        trace=pd.DataFrame(trace_rows),
        quarantined=pd.DataFrame(quarantine_rows),
    )

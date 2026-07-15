from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd

from fmetl.calculations.ledger import SOURCE_COLUMNS, TARGET_COLUMNS


@dataclass(frozen=True)
class CostFundedInternalEvents:
    sources: pd.DataFrame
    targets: pd.DataFrame
    quarantined: pd.DataFrame


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise KeyError(f"{label} missing columns: {missing}")


def build_bootstrap_openings(
    purchase: pd.DataFrame,
    article_ids: Sequence[str],
    *,
    start_day: str,
    store_id: str = "A3XV",
) -> pd.DataFrame:
    """Select one article-level opening tuple and explicitly clamp negative source balances."""
    required = {
        "store_id", "business_date", "sale_article_id", "init_stock_qty", "init_stock_amt",
    }
    _require(purchase, required, "purchase_bootstrap")
    source = purchase[list(required)].copy()
    source = source.loc[
        source["store_id"].astype(str).eq(store_id)
        & source["business_date"].astype(str).eq(str(start_day))
    ]
    source["sale_article_id"] = source["sale_article_id"].astype(str)
    for column in ("init_stock_qty", "init_stock_amt"):
        source[column] = pd.to_numeric(source[column], errors="raise")
        present = source[column].notna()
        if not np.isfinite(source.loc[present, column].to_numpy(dtype=float)).all():
            raise ValueError(f"purchase_bootstrap.{column} must be finite when present")

    rows: list[dict[str, object]] = []
    for article_id in sorted(set(map(str, article_ids))):
        candidates = source.loc[source["sale_article_id"].eq(article_id)]
        if (
            candidates["init_stock_qty"].isna()
            & candidates["init_stock_amt"].notna()
        ).any():
            raise ValueError(f"opening amount without quantity for {article_id}")
        normalized = candidates[["init_stock_qty", "init_stock_amt"]].copy()
        normalized = normalized.loc[normalized["init_stock_qty"].notna()]
        normalized["missing_cost"] = normalized["init_stock_amt"].isna()
        normalized["init_stock_amt"] = normalized["init_stock_amt"].fillna(0.0)
        nonzero = normalized.loc[
            normalized["init_stock_qty"].abs().gt(0.000001)
            | normalized["init_stock_amt"].abs().gt(0.01)
        ].drop_duplicates()
        if len(nonzero) > 1:
            raise ValueError(f"conflicting purchase bootstrap tuples for {article_id}")
        if len(nonzero) == 1:
            qty = float(nonzero.iloc[0]["init_stock_qty"])
            amt = float(nonzero.iloc[0]["init_stock_amt"])
            if bool(nonzero.iloc[0]["missing_cost"]) and qty > 0.000001:
                source_name = "PURCHASE_DI_UNPRICED_OPENING"
                warning = f"POSITIVE_OPENING_WITHOUT_COST:{qty}"
            else:
                source_name = "PURCHASE_DI_BOOTSTRAP"
                warning = ""
        elif not candidates.empty:
            qty = amt = 0.0
            if candidates[["init_stock_qty", "init_stock_amt"]].isna().all(axis=None):
                source_name = "PURCHASE_DI_NULL_BOOTSTRAP_ZERO"
                warning = "NULL_SOURCE_OPENING_ASSUMED_ZERO"
            else:
                source_name = "PURCHASE_DI_BOOTSTRAP_ZERO"
                warning = ""
        else:
            qty = amt = 0.0
            source_name = "NEW_SKU_NO_BOOTSTRAP_ROW"
            warning = "MISSING_SOURCE_OPENING_ASSUMED_ZERO"
        if qty < -0.000001 or amt < -0.01:
            warning = f"NEGATIVE_SOURCE_OPENING_CLAMPED:{qty}:{amt}"
            qty = amt = 0.0
            source_name = "PURCHASE_DI_NEGATIVE_CLAMP"
        if qty <= 0.001 and abs(amt) > 0.01:
            raise ValueError(f"opening amount without quantity for {article_id}")
        if qty < 0 or amt < 0:
            raise ValueError(f"opening stock cannot be negative for {article_id}")
        rows.append({
            "store_id": store_id,
            "article_id": article_id,
            "opening_qty": qty,
            "opening_amt": amt,
            "opening_source": source_name,
            "opening_source_day": str(start_day),
            "opening_warning": warning,
        })
    return pd.DataFrame(rows)


def assemble_dense_activities(
    *,
    days: Sequence[str],
    article_ids: Sequence[str],
    sales: pd.DataFrame,
    losses: pd.DataFrame,
    counts: pd.DataFrame,
    day_clear: pd.DataFrame,
    receipts: pd.DataFrame,
    store_id: str = "A3XV",
) -> pd.DataFrame:
    """Build the dense SKU/day input grid consumed by the serial ledger."""
    sales_required = {
        "store_id", "business_date", "article_id", "gross_sale_qty", "sale_return_qty",
        "net_sale_qty", "net_sale_amt",
    }
    loss_required = {"store_id", "business_date", "article_id", "known_lost_qty"}
    count_required = {
        "store_id", "business_date", "article_id", "actual_stock_qty", "is_counted",
    }
    day_clear_required = {"store_id", "business_date", "article_id", "day_clear"}
    receipt_required = {
        "store_id", "business_date", "article_id", "receive_qty", "receive_amt",
    }
    for frame, columns, label in (
        (sales, sales_required, "sales"),
        (losses, loss_required, "losses"),
        (counts, count_required, "counts"),
        (day_clear, day_clear_required, "day_clear"),
        (receipts, receipt_required, "receipts"),
    ):
        _require(frame, columns, label)

    days = [str(day) for day in days]
    articles = sorted(set(map(str, article_ids)))
    if not days or not articles:
        raise ValueError("dense activities require at least one day and article")
    grid = pd.MultiIndex.from_product(
        [[store_id], days, articles],
        names=["store_id", "business_date", "article_id"],
    ).to_frame(index=False)
    keys = ["store_id", "business_date", "article_id"]

    def normalize_keys(frame: pd.DataFrame, label: str) -> pd.DataFrame:
        result = frame.copy()
        result[keys] = result[keys].astype(str)
        if result.duplicated(keys).any():
            raise ValueError(f"{label} must be unique per SKU-day before dense assembly")
        unexpected_store = sorted(set(result["store_id"]) - {store_id})
        if unexpected_store:
            raise ValueError(f"{label} contains stores outside {store_id}: {unexpected_store}")
        return result

    sale = normalize_keys(sales[list(sales_required)], "sales")
    loss = normalize_keys(losses[list(loss_required)], "losses")
    count = normalize_keys(counts[list(count_required)], "counts")
    labels = normalize_keys(day_clear[list(day_clear_required)], "day_clear")
    receipt = receipts[list(receipt_required)].copy()
    receipt[keys] = receipt[keys].astype(str)
    receipt = receipt.groupby(keys, as_index=False).agg(
        store_receive_qty=("receive_qty", "sum"),
        store_receive_amt=("receive_amt", "sum"),
    )

    result = grid.merge(labels, on=keys, how="left", validate="one_to_one")
    if result["day_clear"].isna().any():
        sample = result.loc[result["day_clear"].isna(), keys].head(20).to_dict("records")
        raise ValueError(f"activity universe is missing chdj day_clear labels: {sample}")
    for frame in (sale, loss, count, receipt):
        result = result.merge(frame, on=keys, how="left", validate="one_to_one")
    numeric_zero = [
        "gross_sale_qty", "sale_return_qty", "net_sale_qty", "net_sale_amt",
        "known_lost_qty", "store_receive_qty", "store_receive_amt",
    ]
    for column in numeric_zero:
        result[column] = pd.to_numeric(result[column], errors="raise").fillna(0.0)
    def strict_count_flag(value: object) -> bool:
        if pd.isna(value):
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in (0, 1):
            return bool(value)
        raise ValueError(f"is_counted must be bool/0/1, got {value!r}")

    result["is_counted"] = result["is_counted"].map(strict_count_flag)
    return result


def filter_cost_funded_internal_events(
    *,
    activities: pd.DataFrame,
    openings: pd.DataFrame,
    sources: pd.DataFrame,
    targets: pd.DataFrame,
    tolerance: float = 0.01,
) -> CostFundedInternalEvents:
    """Keep only events whose every source is reachable from observed cost money.

    This is a coverage gate, not a quantity sufficiency check. The serial daily
    ledger remains responsible for proving that each formal out quantity fits
    the available pool.
    """
    _require(
        activities,
        {"store_id", "business_date", "article_id", "store_receive_amt"},
        "cost_funding_activities",
    )
    _require(
        openings, {"store_id", "article_id", "opening_amt"}, "cost_funding_openings"
    )
    _require(sources, set(SOURCE_COLUMNS), "cost_funding_sources")
    _require(targets, set(TARGET_COLUMNS), "cost_funding_targets")
    source = sources.copy()
    target = targets.copy()
    event_keys = ["store_id", "business_date", "event_group_id"]
    source_events = set(source[event_keys].astype(str).itertuples(index=False, name=None))
    target_events = set(target[event_keys].astype(str).itertuples(index=False, name=None))
    source_event_key = source[event_keys].astype(str).apply(tuple, axis=1)
    target_event_key = target[event_keys].astype(str).apply(tuple, axis=1)
    if source_events != target_events:
        raise ValueError("cost funding requires matching source and target events")

    funded: dict[str, set[str]] = {}
    for store_id, group in openings.groupby(openings["store_id"].astype(str)):
        amounts = pd.to_numeric(group["opening_amt"], errors="raise")
        funded[str(store_id)] = set(
            group.loc[amounts.gt(tolerance), "article_id"].astype(str)
        )

    accepted: set[tuple[str, str, str]] = set()
    quarantine_rows: list[dict[str, object]] = []
    days = sorted(activities["business_date"].astype(str).unique())
    stores = sorted(activities["store_id"].astype(str).unique())
    for day in days:
        for store_id in stores:
            available = funded.setdefault(store_id, set())
            activity_day = activities.loc[
                activities["store_id"].astype(str).eq(store_id)
                & activities["business_date"].astype(str).eq(day)
            ]
            receipt_amt = pd.to_numeric(activity_day["store_receive_amt"], errors="raise")
            available.update(
                activity_day.loc[receipt_amt.gt(tolerance), "article_id"].astype(str)
            )
            pending = {
                event
                for event in source_events
                if event[0] == store_id and event[1] == day
            }
            progressed = True
            while pending and progressed:
                progressed = False
                for event in sorted(pending):
                    source_ids = set(
                        source.loc[
                            source_event_key.map(lambda value: value == event),
                            "source_article_id",
                        ].astype(str)
                    )
                    if source_ids.issubset(available):
                        target_ids = set(
                            target.loc[
                                target_event_key.map(lambda value: value == event),
                                "target_article_id",
                            ].astype(str)
                        )
                        available.update(target_ids)
                        accepted.add(event)
                        pending.remove(event)
                        progressed = True
            for event in sorted(pending):
                source_ids = set(
                    source.loc[
                        source_event_key.map(lambda value: value == event),
                        "source_article_id",
                    ].astype(str)
                )
                quarantine_rows.append({
                    "store_id": event[0],
                    "business_date": event[1],
                    "event_group_id": event[2],
                    "reason": "NO_COST_FUNDED_SOURCE",
                    "missing_source_article_ids": ",".join(sorted(source_ids - available)),
                })

    quarantine = pd.DataFrame(
        quarantine_rows,
        columns=[*event_keys, "reason", "missing_source_article_ids"],
    )
    return CostFundedInternalEvents(
        sources=source.loc[source_event_key.isin(accepted)].reset_index(drop=True),
        targets=target.loc[target_event_key.isin(accepted)].reset_index(drop=True),
        quarantined=quarantine,
    )


def build_internal_event_legs(
    *,
    bom_parent: pd.DataFrame,
    bom_trace: pd.DataFrame,
    pack_plan: pd.DataFrame,
    compose_trace: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convert formal relation plans to one source/target event contract without pricing amounts."""
    sources: list[dict[str, object]] = []
    targets: list[dict[str, object]] = []

    if not bom_parent.empty or not bom_trace.empty:
        _require(
            bom_parent,
            {"store_id", "business_date", "parent_article_id", "parent_out_qty", "relation_snapshot_id"},
            "bom_parent",
        )
        _require(
            bom_trace,
            {
                "store_id", "business_date", "parent_article_id", "sub_article_id",
                "sub_in_qty", "allocation_ratio", "relation_snapshot_id", "quantity_source",
            },
            "bom_trace",
        )
        for parent in bom_parent.itertuples(index=False):
            event = f"BOM|{parent.store_id}|{parent.business_date}|{parent.parent_article_id}"
            sources.append({
                "store_id": parent.store_id, "business_date": parent.business_date,
                "event_group_id": event, "relation_type": "DISASSEMBLY_BOM",
                "source_article_id": parent.parent_article_id,
                "source_out_qty": parent.parent_out_qty,
                "quantity_source": "UPSTREAM_DAL_RECEIVE_SALE",
                "relation_snapshot_id": parent.relation_snapshot_id,
            })
        for leg in bom_trace.itertuples(index=False):
            event = f"BOM|{leg.store_id}|{leg.business_date}|{leg.parent_article_id}"
            targets.append({
                "store_id": leg.store_id, "business_date": leg.business_date,
                "event_group_id": event, "relation_type": "DISASSEMBLY_BOM",
                "target_article_id": leg.sub_article_id,
                "target_in_qty": leg.sub_in_qty,
                "amount_allocation_ratio": leg.allocation_ratio,
                "quantity_source": leg.quantity_source,
                "relation_snapshot_id": leg.relation_snapshot_id,
            })

    if not pack_plan.empty:
        _require(
            pack_plan,
            {
                "store_id", "business_date", "parent_article_id", "sub_article_id",
                "parent_qty", "sub_qty", "event_source", "relation_snapshot_id",
            },
            "pack_plan",
        )
        for leg in pack_plan.itertuples(index=False):
            event = (
                f"PACK|{leg.store_id}|{leg.business_date}|"
                f"{leg.parent_article_id}|{leg.sub_article_id}"
            )
            sources.append({
                "store_id": leg.store_id, "business_date": leg.business_date,
                "event_group_id": event, "relation_type": "PACK_CONVERT",
                "source_article_id": leg.parent_article_id,
                "source_out_qty": leg.parent_qty,
                "quantity_source": leg.event_source,
                "relation_snapshot_id": leg.relation_snapshot_id,
            })
            targets.append({
                "store_id": leg.store_id, "business_date": leg.business_date,
                "event_group_id": event, "relation_type": "PACK_CONVERT",
                "target_article_id": leg.sub_article_id,
                "target_in_qty": leg.sub_qty,
                "amount_allocation_ratio": 1.0,
                "quantity_source": leg.event_source,
                "relation_snapshot_id": leg.relation_snapshot_id,
            })

    if not compose_trace.empty:
        _require(
            compose_trace,
            {
                "store_id", "business_date", "relation_id", "raw_article_id",
                "finished_article_id", "raw_out_qty", "finished_in_qty",
                "raw_quantity_source", "relation_snapshot_id", "formal_flow_allowed",
            },
            "compose_trace",
        )
        formal = compose_trace.loc[compose_trace["formal_flow_allowed"].astype(bool)].copy()
        for (store, day, relation_id), group in formal.groupby(
            ["store_id", "business_date", "relation_id"], sort=False
        ):
            event = f"COMPOSE|{store}|{day}|{relation_id}"
            snapshots = group["relation_snapshot_id"].astype(str).unique()
            finished = group["finished_article_id"].astype(str).unique()
            finished_qty = pd.to_numeric(group["finished_in_qty"], errors="raise").unique()
            if len(snapshots) != 1 or len(finished) != 1 or len(finished_qty) != 1:
                raise ValueError("compose event metadata or finished quantity is inconsistent")
            for raw_id, raw_group in group.groupby("raw_article_id", sort=False):
                sources.append({
                    "store_id": store, "business_date": day,
                    "event_group_id": event, "relation_type": "RECIPE_COMPOSE",
                    "source_article_id": str(raw_id),
                    "source_out_qty": float(raw_group["raw_out_qty"].sum()),
                    "quantity_source": "+".join(sorted(set(raw_group["raw_quantity_source"].astype(str)))),
                    "relation_snapshot_id": snapshots[0],
                })
            targets.append({
                "store_id": store, "business_date": day,
                "event_group_id": event, "relation_type": "RECIPE_COMPOSE",
                "target_article_id": finished[0],
                "target_in_qty": float(finished_qty[0]),
                "amount_allocation_ratio": 1.0,
                "quantity_source": "FORMAL_RECIPE_TRACE",
                "relation_snapshot_id": snapshots[0],
            })
    return (
        pd.DataFrame(sources, columns=SOURCE_COLUMNS),
        pd.DataFrame(targets, columns=TARGET_COLUMNS),
    )

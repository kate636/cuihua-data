from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from fmetl.calculations.daily_cost_stock import DailyFlow, transition_day
from fmetl.calculations.profit import calculate_accounting_profit
from fmetl.relations.graph import topological_order


RELATION_FLOW = {
    "DISASSEMBLY_BOM": "bom",
    "PACK_CONVERT": "pack",
    "RECIPE_COMPOSE": "compose",
    "RESIDUAL_TRANSFER": "residual_transfer",
}

ACTIVITY_COLUMNS = (
    "store_id", "business_date", "article_id", "day_clear",
    "gross_sale_qty", "sale_return_qty", "net_sale_qty", "net_sale_amt",
    "known_lost_qty", "actual_stock_qty", "is_counted",
    "store_receive_qty", "store_receive_amt",
)
OPENING_COLUMNS = (
    "store_id", "article_id", "opening_qty", "opening_amt",
    "opening_source", "opening_source_day",
)
SOURCE_COLUMNS = (
    "store_id", "business_date", "event_group_id", "relation_type",
    "source_article_id", "source_out_qty", "quantity_source", "relation_snapshot_id",
)
TARGET_COLUMNS = (
    "store_id", "business_date", "event_group_id", "relation_type",
    "target_article_id", "target_in_qty", "amount_allocation_ratio",
    "quantity_source", "relation_snapshot_id",
)


@dataclass(frozen=True)
class LedgerResult:
    sku_daily: pd.DataFrame
    internal_postings: pd.DataFrame


def _require(frame: pd.DataFrame, columns: tuple[str, ...], label: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise KeyError(f"{label} missing columns: {missing}")


def _keys(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    if frame[columns].isna().any().any():
        raise ValueError(f"{label} keys cannot contain NULL")
    blank = frame[columns].astype(str).apply(
        lambda column: column.str.strip().str.lower().isin({"", "nan", "none", "null"})
    )
    if blank.any().any():
        raise ValueError(f"{label} keys cannot be blank")


def _required_numeric(
    frame: pd.DataFrame,
    columns: tuple[str, ...],
    label: str,
    *,
    nonnegative: bool = True,
) -> None:
    for column in columns:
        frame[column] = pd.to_numeric(frame[column], errors="raise")
        if frame[column].isna().any() or not np.isfinite(frame[column].to_numpy(dtype=float)).all():
            raise ValueError(f"{label}.{column} must be finite and non-NULL")
        if nonnegative and frame[column].lt(-0.000001).any():
            raise ValueError(f"{label}.{column} cannot be negative")


def _validate_internal(
    sources: pd.DataFrame,
    targets: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    _require(sources, SOURCE_COLUMNS, "internal_sources")
    _require(targets, TARGET_COLUMNS, "internal_targets")
    source = sources[list(SOURCE_COLUMNS)].copy()
    target = targets[list(TARGET_COLUMNS)].copy()
    source_keys = ["store_id", "business_date", "event_group_id", "source_article_id"]
    target_keys = ["store_id", "business_date", "event_group_id", "target_article_id"]
    _keys(
        source,
        [*source_keys, "relation_type", "quantity_source", "relation_snapshot_id"],
        "internal_sources",
    )
    _keys(
        target,
        [*target_keys, "relation_type", "quantity_source", "relation_snapshot_id"],
        "internal_targets",
    )
    source[["store_id", "business_date", "event_group_id", "source_article_id"]] = source[
        ["store_id", "business_date", "event_group_id", "source_article_id"]
    ].astype(str)
    target[["store_id", "business_date", "event_group_id", "target_article_id"]] = target[
        ["store_id", "business_date", "event_group_id", "target_article_id"]
    ].astype(str)
    if source.duplicated(source_keys).any() or target.duplicated(target_keys).any():
        raise ValueError("internal event legs must be unique per event/article")
    _required_numeric(source, ("source_out_qty",), "internal_sources")
    _required_numeric(
        target, ("target_in_qty", "amount_allocation_ratio"), "internal_targets"
    )
    allowed = set(RELATION_FLOW)
    if not set(source["relation_type"].astype(str)).issubset(allowed):
        raise ValueError("internal_sources contains an unsupported relation_type")
    if not set(target["relation_type"].astype(str)).issubset(allowed):
        raise ValueError("internal_targets contains an unsupported relation_type")
    event_keys = ["store_id", "business_date", "event_group_id"]
    source_events = source.groupby(event_keys).agg(
        source_relation=("relation_type", "nunique"),
        source_snapshot=("relation_snapshot_id", "nunique"),
    )
    target_events = target.groupby(event_keys).agg(
        target_relation=("relation_type", "nunique"),
        target_snapshot=("relation_snapshot_id", "nunique"),
        allocation_sum=("amount_allocation_ratio", "sum"),
    )
    events = source_events.join(target_events, how="outer")
    if events.isna().any().any():
        raise ValueError("each internal event requires at least one source and target leg")
    if (
        events[["source_relation", "target_relation", "source_snapshot", "target_snapshot"]]
        .ne(1).any().any()
    ):
        raise ValueError("each internal event requires one relation type and snapshot")
    if events["allocation_sum"].sub(1.0).abs().gt(0.000001).any():
        raise ValueError("internal target amount allocation ratios must sum to one per event")
    source_meta = source[event_keys + ["relation_type", "relation_snapshot_id"]].drop_duplicates()
    target_meta = target[event_keys + ["relation_type", "relation_snapshot_id"]].drop_duplicates()
    if len(source_meta.merge(target_meta, on=[*event_keys, "relation_type", "relation_snapshot_id"])) != len(events):
        raise ValueError("internal source and target event metadata do not match")
    return source, target


def run_weighted_ledger(
    activities: pd.DataFrame,
    openings: pd.DataFrame,
    internal_sources: pd.DataFrame,
    internal_targets: pd.DataFrame,
) -> LedgerResult:
    """Run a dense SKU/day ledger and price all same-day internal flows along one DAG."""
    _require(activities, ACTIVITY_COLUMNS, "activities")
    _require(openings, OPENING_COLUMNS, "openings")
    activity = activities[list(ACTIVITY_COLUMNS)].copy()
    opening = openings[list(OPENING_COLUMNS)].copy()
    source, target = _validate_internal(internal_sources, internal_targets)

    activity_keys = ["store_id", "business_date", "article_id"]
    opening_keys = ["store_id", "article_id"]
    _keys(activity, activity_keys, "activities")
    _keys(opening, [*opening_keys, "opening_source", "opening_source_day"], "openings")
    if activity.duplicated(activity_keys).any():
        raise ValueError("activities must be unique per store/date/article")
    if opening.duplicated(opening_keys).any():
        raise ValueError("openings must be unique per store/article")
    activity[[*activity_keys, "day_clear"]] = activity[[*activity_keys, "day_clear"]].astype(str)
    opening[opening_keys] = opening[opening_keys].astype(str)
    if not activity["day_clear"].isin({"0", "1"}).all():
        raise ValueError("activities.day_clear must be '0' or '1'")
    _required_numeric(
        activity,
        (
            "gross_sale_qty", "sale_return_qty", "known_lost_qty",
            "store_receive_qty", "store_receive_amt",
        ),
        "activities",
    )
    _required_numeric(activity, ("net_sale_qty", "net_sale_amt"), "activities", nonnegative=False)
    def strict_bool(value: object) -> bool:
        if isinstance(value, (bool, np.bool_)):
            return bool(value)
        if isinstance(value, (int, np.integer)) and value in {0, 1}:
            return bool(value)
        raise ValueError(f"activities.is_counted must be bool/0/1, got {value!r}")

    activity["is_counted"] = activity["is_counted"].map(strict_bool)
    activity["actual_stock_qty"] = pd.to_numeric(activity["actual_stock_qty"], errors="raise")
    present_actual = activity["actual_stock_qty"].notna()
    if not np.isfinite(activity.loc[present_actual, "actual_stock_qty"].to_numpy(dtype=float)).all():
        raise ValueError("activities.actual_stock_qty must be finite when present")
    if activity.loc[present_actual, "actual_stock_qty"].lt(0).any():
        raise ValueError("activities.actual_stock_qty cannot be negative")
    if (activity["is_counted"] & ~present_actual).any():
        raise ValueError("is_counted requires actual_stock_qty")
    _required_numeric(opening, ("opening_qty", "opening_amt"), "openings")
    if (opening["opening_qty"].le(0.001) & opening["opening_amt"].abs().gt(0.01)).any():
        raise ValueError("opening amount requires positive opening quantity")

    activity_domain = set(activity[activity_keys].itertuples(index=False, name=None))
    source_domain = set(
        source[["store_id", "business_date", "source_article_id"]]
        .astype(str).itertuples(index=False, name=None)
    )
    target_domain = set(
        target[["store_id", "business_date", "target_article_id"]]
        .astype(str).itertuples(index=False, name=None)
    )
    outside = sorted((source_domain | target_domain) - activity_domain)
    if outside:
        raise ValueError(f"internal event legs fall outside the dense activity grid: {outside[:20]}")

    stores = sorted(activity["store_id"].unique())
    days = sorted(activity["business_date"].unique())
    article_sets = activity.groupby("store_id")["article_id"].apply(set).to_dict()
    for store in stores:
        expected = {(store, article) for article in article_sets[store]}
        present = set(opening.loc[opening["store_id"].eq(store), opening_keys].itertuples(index=False, name=None))
        if present != expected:
            raise ValueError(f"openings must exactly cover the activity universe for {store}")
        expected_rows = len(article_sets[store]) * len(days)
        if len(activity.loc[activity["store_id"].eq(store)]) != expected_rows:
            raise ValueError(f"activities must be a dense article/day grid for {store}")

    current = {
        (row.store_id, row.article_id): (float(row.opening_qty), float(row.opening_amt))
        for row in opening.itertuples(index=False)
    }
    opening_meta = {
        (row.store_id, row.article_id): (str(row.opening_source), str(row.opening_source_day))
        for row in opening.itertuples(index=False)
    }
    state_rows: list[dict[str, object]] = []
    posting_rows: list[dict[str, object]] = []

    for day in days:
        for store in stores:
            day_activity = activity.loc[
                activity["store_id"].eq(store) & activity["business_date"].eq(day)
            ].set_index("article_id", drop=False)
            day_source = source.loc[source["store_id"].eq(store) & source["business_date"].eq(day)]
            day_target = target.loc[target["store_id"].eq(store) & target["business_date"].eq(day)]
            edges: list[tuple[str, str]] = []
            for event_id, source_legs in day_source.groupby("event_group_id", sort=False):
                target_legs = day_target.loc[day_target["event_group_id"].eq(event_id)]
                edges.extend(
                    (str(source_id), str(target_id))
                    for source_id in source_legs["source_article_id"]
                    for target_id in target_legs["target_article_id"]
                )
            graph_order = topological_order(edges) if edges else []
            isolated = sorted(set(day_activity.index.astype(str)) - set(graph_order))
            order = [*graph_order, *isolated]
            if set(order) != set(day_activity.index.astype(str)):
                raise ValueError("internal relation references an article outside the dense activity grid")

            event_source_amounts: dict[str, dict[str, float]] = {}
            day_results: dict[str, dict[str, object]] = {}
            for article_id in order:
                row = day_activity.loc[article_id]
                init_qty, init_amt = current[(store, article_id)]
                incoming_qty = {flow: 0.0 for flow in RELATION_FLOW.values()}
                incoming_amt = {flow: 0.0 for flow in RELATION_FLOW.values()}
                incoming_legs = day_target.loc[day_target["target_article_id"].astype(str).eq(article_id)]
                for leg in incoming_legs.itertuples(index=False):
                    source_amounts = event_source_amounts.get(str(leg.event_group_id), {})
                    expected_sources = day_source.loc[
                        day_source["event_group_id"].eq(leg.event_group_id), "source_article_id"
                    ].astype(str)
                    if set(source_amounts) != set(expected_sources):
                        raise ValueError("target reached before every internal source leg was priced")
                    event_amt = sum(source_amounts.values())
                    flow_name = RELATION_FLOW[str(leg.relation_type)]
                    incoming_qty[flow_name] += float(leg.target_in_qty)
                    incoming_amt[flow_name] += event_amt * float(leg.amount_allocation_ratio)

                outgoing_qty = {flow: 0.0 for flow in RELATION_FLOW.values()}
                outgoing_legs = day_source.loc[day_source["source_article_id"].astype(str).eq(article_id)]
                for leg in outgoing_legs.itertuples(index=False):
                    outgoing_qty[RELATION_FLOW[str(leg.relation_type)]] += float(leg.source_out_qty)

                return_qty = float(row.sale_return_qty)
                if return_qty > 0:
                    pre_return_qty = (
                        init_qty + float(row.store_receive_qty) + sum(incoming_qty.values())
                    )
                    pre_return_amt = (
                        init_amt + float(row.store_receive_amt) + sum(incoming_amt.values())
                    )
                    if pre_return_qty <= 0.001:
                        raise ValueError(
                            f"sale return has no inventory cost evidence: {store}/{day}/{article_id}"
                        )
                    return_cost_basis = pre_return_amt / pre_return_qty
                    sale_return_amt = return_qty * return_cost_basis
                else:
                    return_cost_basis = 0.0
                    sale_return_amt = 0.0

                actual = None if pd.isna(row.actual_stock_qty) else float(row.actual_stock_qty)
                flow = DailyFlow(
                    init_qty=init_qty,
                    init_amt=init_amt,
                    store_receive_qty=float(row.store_receive_qty),
                    store_receive_amt=float(row.store_receive_amt),
                    bom_in_qty=incoming_qty["bom"],
                    bom_in_amt=incoming_amt["bom"],
                    bom_out_qty=outgoing_qty["bom"],
                    pack_in_qty=incoming_qty["pack"],
                    pack_in_amt=incoming_amt["pack"],
                    pack_out_qty=outgoing_qty["pack"],
                    compose_in_qty=incoming_qty["compose"],
                    compose_in_amt=incoming_amt["compose"],
                    compose_out_qty=outgoing_qty["compose"],
                    residual_transfer_in_qty=incoming_qty["residual_transfer"],
                    residual_transfer_in_amt=incoming_amt["residual_transfer"],
                    residual_transfer_out_qty=outgoing_qty["residual_transfer"],
                    sale_return_qty=return_qty,
                    sale_return_amt=sale_return_amt,
                    sale_qty=float(row.gross_sale_qty),
                    known_lost_qty=float(row.known_lost_qty),
                    actual_stock_qty=actual,
                    is_counted=bool(row.is_counted),
                    day_clear=str(row.day_clear),
                )
                try:
                    state = transition_day(flow)
                except ValueError as exc:
                    raise ValueError(
                        f"daily state failed for {store}/{day}/{article_id}: {exc}; "
                        f"init=({init_qty},{init_amt}), "
                        f"receive=({row.store_receive_qty},{row.store_receive_amt}), "
                        f"internal_in_qty={sum(incoming_qty.values())}, "
                        f"internal_out_qty={sum(outgoing_qty.values())}, "
                        f"sale_qty={row.gross_sale_qty}, known_lost_qty={row.known_lost_qty}, "
                        f"actual_stock_qty={actual}, day_clear={row.day_clear}"
                    ) from exc
                out_amounts = {
                    "bom": state.bom_out_amt,
                    "pack": state.pack_out_amt,
                    "compose": state.compose_out_amt,
                    "residual_transfer": state.residual_transfer_out_amt,
                }
                for leg in outgoing_legs.itertuples(index=False):
                    leg_amt = float(leg.source_out_qty) * state.issue_unit_cost
                    event_source_amounts.setdefault(str(leg.event_group_id), {})[article_id] = leg_amt
                    posting_rows.append({
                        "posting_id": f"{store}|{day}|{leg.event_group_id}|OUT|{article_id}",
                        "event_group_id": str(leg.event_group_id),
                        "relation_snapshot_id": str(leg.relation_snapshot_id),
                        "store_id": store,
                        "business_date": day,
                        "relation_type": str(leg.relation_type),
                        "article_id": article_id,
                        "posting_role": "OUT",
                        "qty": float(leg.source_out_qty),
                        "amt": leg_amt,
                        "quantity_source": str(leg.quantity_source),
                        "cost_source": "DAILY_WEIGHTED_ISSUE_COST",
                        "formal_flow_allowed": True,
                    })

                profit = calculate_accounting_profit(
                    sale_amt=float(row.net_sale_amt),
                    store_receive_amt=float(row.store_receive_amt),
                    bom_in_amt=incoming_amt["bom"],
                    bom_out_amt=out_amounts["bom"],
                    pack_in_amt=incoming_amt["pack"],
                    pack_out_amt=out_amounts["pack"],
                    compose_in_amt=incoming_amt["compose"],
                    compose_out_amt=out_amounts["compose"],
                    residual_transfer_in_amt=incoming_amt["residual_transfer"],
                    residual_transfer_out_amt=out_amounts["residual_transfer"],
                    init_stock_amt=init_amt,
                    end_stock_amt=state.end_amt,
                    neg_clamp_cost_amt=state.neg_clamp_cost_amt,
                )
                first_day = day == days[0]
                source_name, source_day = opening_meta[(store, article_id)]
                day_results[article_id] = {
                    **row.to_dict(),
                    "init_stock_qty": init_qty,
                    "init_stock_amt": init_amt,
                    "opening_source": source_name if first_day else "ROLL_FORWARD",
                    "opening_source_day": source_day if first_day else days[days.index(day) - 1],
                    "sale_return_cost_basis": return_cost_basis,
                    "sale_return_cost_amt": sale_return_amt,
                    "bom_in_qty": incoming_qty["bom"], "bom_in_amt": incoming_amt["bom"],
                    "bom_out_qty": outgoing_qty["bom"], "bom_out_amt": out_amounts["bom"],
                    "pack_in_qty": incoming_qty["pack"], "pack_in_amt": incoming_amt["pack"],
                    "pack_out_qty": outgoing_qty["pack"], "pack_out_amt": out_amounts["pack"],
                    "compose_in_qty": incoming_qty["compose"], "compose_in_amt": incoming_amt["compose"],
                    "compose_out_qty": outgoing_qty["compose"], "compose_out_amt": out_amounts["compose"],
                    "residual_transfer_in_qty": incoming_qty["residual_transfer"],
                    "residual_transfer_in_amt": incoming_amt["residual_transfer"],
                    "residual_transfer_out_qty": outgoing_qty["residual_transfer"],
                    "residual_transfer_out_amt": out_amounts["residual_transfer"],
                    **state.__dict__,
                    "accounting_known_lost_qty": float(row.known_lost_qty),
                    "accounting_known_lost_amt": state.known_lost_amt,
                    "accounting_lost_qty": float(row.known_lost_qty) + state.unknown_lost_qty,
                    "accounting_lost_amt": state.known_lost_amt + state.unknown_lost_amt,
                    "accounting_profit": profit,
                }

            for leg in day_target.itertuples(index=False):
                event_amount = sum(event_source_amounts[str(leg.event_group_id)].values())
                leg_amt = event_amount * float(leg.amount_allocation_ratio)
                posting_rows.append({
                    "posting_id": (
                        f"{store}|{day}|{leg.event_group_id}|IN|{leg.target_article_id}"
                    ),
                    "event_group_id": str(leg.event_group_id),
                    "relation_snapshot_id": str(leg.relation_snapshot_id),
                    "store_id": store,
                    "business_date": day,
                    "relation_type": str(leg.relation_type),
                    "article_id": str(leg.target_article_id),
                    "posting_role": "IN",
                    "qty": float(leg.target_in_qty),
                    "amt": leg_amt,
                    "quantity_source": str(leg.quantity_source),
                    "cost_source": "DAILY_WEIGHTED_INTERNAL_ALLOCATION",
                    "formal_flow_allowed": True,
                })
            state_rows.extend(day_results.values())
            for article_id, row in day_results.items():
                current[(store, article_id)] = (float(row["end_qty"]), float(row["end_amt"]))

    postings = pd.DataFrame(posting_rows)
    if not postings.empty:
        if postings["posting_id"].duplicated().any():
            raise ValueError("internal posting_id must be unique")
        conservation = postings.pivot_table(
            index=["store_id", "business_date", "event_group_id"],
            columns="posting_role", values="amt", aggfunc="sum", fill_value=0.0,
        )
        if conservation.get("OUT", 0.0).sub(conservation.get("IN", 0.0)).abs().gt(0.01).any():
            raise ValueError("internal event amounts do not conserve")
    return LedgerResult(
        sku_daily=pd.DataFrame(state_rows).sort_values(
            ["store_id", "business_date", "article_id"], kind="stable"
        ).reset_index(drop=True),
        internal_postings=postings.sort_values(
            ["store_id", "business_date", "event_group_id", "posting_role", "article_id"],
            kind="stable",
        ).reset_index(drop=True) if not postings.empty else postings,
    )

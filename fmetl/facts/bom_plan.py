from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from fmetl.facts._resolution import assert_formal_pairs
from fmetl.relations.graph import connected_components


@dataclass(frozen=True)
class DisassemblyPlan:
    parent_postings: pd.DataFrame
    child_postings: pd.DataFrame
    trace: pd.DataFrame


@dataclass(frozen=True)
class PricedDisassemblyPlan:
    parent_postings: pd.DataFrame
    child_postings: pd.DataFrame
    trace: pd.DataFrame


def validate_bom_plan(plan: pd.DataFrame, resolution: pd.DataFrame) -> pd.DataFrame:
    """Validate disassembly quantities without mixing parent/sub units."""
    required = {
        "store_id", "business_date", "parent_article_id", "sub_article_id",
        "parent_out_qty", "sub_in_qty", "allocation_weight", "quantity_source",
    }
    missing = sorted(required - set(plan.columns))
    if missing:
        raise KeyError(f"BOM plan missing columns: {missing}")
    key_columns = ["store_id", "business_date", "parent_article_id", "sub_article_id"]
    if plan[key_columns].isna().any().any():
        raise ValueError("BOM plan keys cannot contain NULL")
    if (
        plan[key_columns].astype(str)
        .apply(lambda column: column.str.strip().str.lower().isin({"", "nan", "none", "null"}))
        .any().any()
    ):
        raise ValueError("BOM plan keys cannot be blank")
    snapshot_id = assert_formal_pairs(
        plan,
        resolution,
        expected_type="DISASSEMBLY_BOM",
        source_column="parent_article_id",
        target_column="sub_article_id",
    )
    out = plan.copy()
    for column in ("parent_out_qty", "sub_in_qty", "allocation_weight"):
        out[column] = pd.to_numeric(out[column], errors="raise")
        if out[column].isna().any() or not np.isfinite(out[column].to_numpy(dtype=float)).all():
            raise ValueError(f"BOM {column} must be finite")
        if (out[column] < 0).any():
            raise ValueError(f"BOM {column} cannot be negative")
    if out[["store_id", "business_date", "parent_article_id", "sub_article_id"]].duplicated().any():
        raise ValueError("BOM plan contains duplicate parent/sub/day edges")
    weight_sums = out.groupby(
        ["store_id", "business_date", "parent_article_id"]
    )["allocation_weight"].sum()
    if weight_sums.isna().any() or (weight_sums <= 0).any():
        raise ValueError("each BOM parent/day requires a positive allocation weight sum")
    out["relation_type"] = "DISASSEMBLY_BOM"
    out["relation_snapshot_id"] = snapshot_id
    out["bom_group_id"] = pd.NA
    for business_date, day in out.groupby("business_date", sort=False):
        edges = list(day[["parent_article_id", "sub_article_id"]].astype(str).itertuples(index=False, name=None))
        for component in connected_components(edges):
            component_key = "-".join(sorted(component))
            mask = out["business_date"].eq(business_date) & (
                out["parent_article_id"].astype(str).isin(component)
                | out["sub_article_id"].astype(str).isin(component)
            )
            out.loc[mask, "bom_group_id"] = f"{business_date}:bom:{component_key}"
    return out


def build_disassembly_plan(receive_sale: pd.DataFrame, resolution: pd.DataFrame) -> DisassemblyPlan:
    """Create one parent out leg and N child in legs from the daily compatibility bridge."""
    required = {
        "store_id", "inc_day", "article_id", "sale_article_id", "inbound_qty",
        "inbound_amount", "sale_article_qty", "spilit_sale_article_amt",
        "sale_recev_rate",
    }
    missing = sorted(required - set(receive_sale.columns))
    if missing:
        raise KeyError(f"receive_sale disassembly input missing columns: {missing}")
    trace = receive_sale[list(required)].copy().rename(columns={
        "inc_day": "business_date", "article_id": "parent_article_id",
        "sale_article_id": "sub_article_id", "inbound_qty": "parent_out_qty",
        "inbound_amount": "source_parent_amt", "sale_article_qty": "sub_in_qty",
        "spilit_sale_article_amt": "source_child_amt",
        "sale_recev_rate": "source_rate",
    })
    trace_keys = ["store_id", "business_date", "parent_article_id", "sub_article_id"]
    if trace[trace_keys].isna().any().any():
        raise ValueError("disassembly keys cannot contain NULL")
    if (
        trace[trace_keys].astype(str)
        .apply(lambda column: column.str.strip().str.lower().isin({"", "nan", "none", "null"}))
        .any().any()
    ):
        raise ValueError("disassembly keys cannot be blank")
    trace[["store_id", "business_date", "parent_article_id", "sub_article_id"]] = trace[
        ["store_id", "business_date", "parent_article_id", "sub_article_id"]
    ].astype(str)
    trace = trace.loc[trace["parent_article_id"].ne(trace["sub_article_id"])].copy()
    for column in ("parent_out_qty", "source_parent_amt", "sub_in_qty", "source_child_amt", "source_rate"):
        trace[column] = pd.to_numeric(trace[column], errors="raise")
        if np.isinf(trace[column].to_numpy(dtype=float)).any():
            raise ValueError(f"disassembly {column} must be finite when present")
        trace[column] = trace[column].fillna(0.0)
        if trace[column].lt(-0.000001).any():
            raise ValueError(f"disassembly {column} cannot be negative")
    trace = trace.loc[
        trace["parent_out_qty"].gt(0.000001)
        | trace["source_parent_amt"].gt(0.000001)
        | trace["sub_in_qty"].gt(0.000001)
        | trace["source_child_amt"].gt(0.000001)
    ].copy()
    if trace.empty:
        empty_parent = pd.DataFrame(columns=[
            "store_id", "business_date", "parent_article_id", "parent_out_qty",
            "source_parent_amt", "relation_snapshot_id",
        ])
        empty_child = pd.DataFrame(columns=[
            "store_id", "business_date", "sub_article_id", "sub_in_qty",
            "relation_snapshot_id",
        ])
        return DisassemblyPlan(empty_parent, empty_child, trace)
    if trace.duplicated(["store_id", "business_date", "parent_article_id", "sub_article_id"]).any():
        raise ValueError("disassembly bridge contains duplicate parent/sub/day edges")
    snapshot_id = assert_formal_pairs(
        trace, resolution, expected_type="DISASSEMBLY_BOM",
        source_column="parent_article_id", target_column="sub_article_id",
    )
    parent_keys = ["store_id", "business_date", "parent_article_id"]
    consistency = trace.groupby(parent_keys).agg(
        qty_min=("parent_out_qty", "min"), qty_max=("parent_out_qty", "max"),
        amt_min=("source_parent_amt", "min"), amt_max=("source_parent_amt", "max"),
        source_child_amt_sum=("source_child_amt", "sum"),
        source_rate_sum=("source_rate", "sum"),
    ).reset_index()
    if (
        consistency["qty_max"].sub(consistency["qty_min"]).abs().gt(0.000001)
        | consistency["amt_max"].sub(consistency["amt_min"]).abs().gt(0.000001)
    ).any():
        raise ValueError("disassembly parent receipt values are inconsistent across child rows")
    if consistency["source_child_amt_sum"].sub(consistency["amt_max"]).abs().gt(0.01).any():
        raise ValueError("disassembly source child amounts do not conserve parent receipt amount")
    if (
        consistency["source_child_amt_sum"].le(0)
        & consistency["source_rate_sum"].le(0)
    ).any():
        raise ValueError("disassembly requires a positive amount or rate allocation weight")

    trace = trace.merge(
        consistency[parent_keys + ["source_child_amt_sum", "source_rate_sum"]],
        on=parent_keys, how="left", validate="many_to_one",
    )
    mixed_zero = trace.assign(
        zero_value_active=trace["sub_in_qty"].gt(0.000001) & trace["source_child_amt"].le(0.000001)
    ).groupby(parent_keys)["zero_value_active"].transform("any")
    has_amount_weight = trace["source_child_amt_sum"].gt(0) & ~mixed_zero
    if ((~has_amount_weight) & trace["source_rate_sum"].le(0)).any():
        raise ValueError("rate fallback requires a positive sale_recev_rate sum")
    trace["allocation_ratio"] = 0.0
    trace.loc[has_amount_weight, "allocation_ratio"] = (
        trace.loc[has_amount_weight, "source_child_amt"]
        / trace.loc[has_amount_weight, "source_child_amt_sum"]
    )
    trace.loc[~has_amount_weight, "allocation_ratio"] = (
        trace.loc[~has_amount_weight, "source_rate"]
        / trace.loc[~has_amount_weight, "source_rate_sum"]
    )
    trace["allocation_source"] = "SOURCE_SPLIT_AMOUNT"
    trace.loc[~has_amount_weight, "allocation_source"] = "SALE_RECEV_RATE_ZERO_OR_MIXED_GIFT"
    trace["relation_snapshot_id"] = snapshot_id
    trace["quantity_source"] = "UPSTREAM_DAL_RECEIVE_SALE"
    trace["consume_policy"] = "CONSUME_ALL"
    trace["bom_group_id"] = pd.NA
    for business_date, day in trace.groupby("business_date", sort=False):
        edges = day[["parent_article_id", "sub_article_id"]].itertuples(index=False, name=None)
        for component in connected_components(edges):
            component_key = "-".join(sorted(component))
            mask = trace["business_date"].eq(business_date) & (
                trace["parent_article_id"].isin(component) | trace["sub_article_id"].isin(component)
            )
            trace.loc[mask, "bom_group_id"] = f"{business_date}:bom:{component_key}"

    parent = consistency.rename(columns={
        "qty_max": "parent_out_qty", "amt_max": "source_parent_amt",
    })[parent_keys + ["parent_out_qty", "source_parent_amt"]]
    parent["relation_snapshot_id"] = snapshot_id
    child = trace.groupby(
        ["store_id", "business_date", "sub_article_id"], as_index=False
    ).agg(sub_in_qty=("sub_in_qty", "sum"))
    child["relation_snapshot_id"] = snapshot_id
    return DisassemblyPlan(parent, child, trace)


def price_disassembly_plan(
    plan: DisassemblyPlan,
    parent_costs: pd.DataFrame,
) -> PricedDisassemblyPlan:
    """Price parent out at rolling cost and allocate exactly the same amount to children."""
    required = {"store_id", "business_date", "parent_article_id", "issue_unit_cost"}
    missing = sorted(required - set(parent_costs.columns))
    if missing:
        raise KeyError(f"parent costs missing columns: {missing}")
    keys = ["store_id", "business_date", "parent_article_id"]
    costs = parent_costs[list(required)].copy()
    if costs[keys].isna().any().any():
        raise ValueError("parent cost keys cannot contain NULL")
    if (
        costs[keys].astype(str)
        .apply(lambda column: column.str.strip().str.lower().isin({"", "nan", "none", "null"}))
        .any().any()
    ):
        raise ValueError("parent cost keys cannot be blank")
    if costs.duplicated(keys).any():
        raise ValueError("parent issue costs must be unique per store/date/parent")
    costs["issue_unit_cost"] = pd.to_numeric(costs["issue_unit_cost"], errors="raise")
    if not np.isfinite(costs["issue_unit_cost"].to_numpy(dtype=float)).all():
        raise ValueError("parent issue cost must be finite")
    if costs["issue_unit_cost"].lt(0).any():
        raise ValueError("parent issue cost cannot be negative")
    parent = plan.parent_postings.merge(costs, on=keys, how="left", validate="one_to_one")
    if parent["issue_unit_cost"].isna().any():
        raise ValueError("disassembly parent is missing a rolling issue cost")
    parent["parent_out_amt"] = parent["parent_out_qty"] * parent["issue_unit_cost"]
    trace = plan.trace.merge(
        parent[keys + ["issue_unit_cost", "parent_out_amt"]],
        on=keys, how="left", validate="many_to_one",
    )
    trace["sub_in_amt"] = trace["parent_out_amt"] * trace["allocation_ratio"]
    child = trace.groupby(
        ["store_id", "business_date", "sub_article_id"], as_index=False
    ).agg(sub_in_qty=("sub_in_qty", "sum"), sub_in_amt=("sub_in_amt", "sum"))
    conservation = trace.groupby(keys).agg(
        parent_out_amt=("parent_out_amt", "first"), child_in_amt=("sub_in_amt", "sum")
    )
    if conservation["parent_out_amt"].sub(conservation["child_in_amt"]).abs().gt(0.01).any():
        raise ValueError("priced disassembly does not conserve amount")
    return PricedDisassemblyPlan(parent, child, trace)

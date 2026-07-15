from __future__ import annotations

import pandas as pd

from fmetl.facts._resolution import assert_formal_pairs
from fmetl.relations.graph import connected_components


def validate_bom_plan(plan: pd.DataFrame, resolution: pd.DataFrame) -> pd.DataFrame:
    """Validate disassembly quantities without mixing parent/sub units."""
    required = {
        "store_id", "business_date", "parent_article_id", "sub_article_id",
        "parent_out_qty", "sub_in_qty", "allocation_weight", "quantity_source",
    }
    missing = sorted(required - set(plan.columns))
    if missing:
        raise KeyError(f"BOM plan missing columns: {missing}")
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
        if out[column].isna().any() or (out[column] < 0).any():
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

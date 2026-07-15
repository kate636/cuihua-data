from __future__ import annotations

import pandas as pd


def assert_formal_pairs(
    pairs: pd.DataFrame,
    resolution: pd.DataFrame,
    *,
    expected_type: str,
    source_column: str,
    target_column: str,
) -> str:
    required_resolution = {
        "business_date", "from_article_id", "to_article_id", "relation_type",
        "formal_flow_allowed", "relation_snapshot_id",
    }
    missing = sorted(required_resolution - set(resolution.columns))
    if missing:
        raise KeyError(f"relation resolution missing columns: {missing}")
    snapshot_ids = resolution["relation_snapshot_id"].dropna().astype(str).unique()
    if len(snapshot_ids) != 1:
        raise ValueError("one plan must consume exactly one relation snapshot")
    keys = pairs[["business_date", source_column, target_column]].copy()
    keys = keys.rename(columns={source_column: "from_article_id", target_column: "to_article_id"})
    keys[["from_article_id", "to_article_id"]] = keys[["from_article_id", "to_article_id"]].astype(str)
    approved = resolution[list(required_resolution)].copy()
    approved[["from_article_id", "to_article_id"]] = approved[
        ["from_article_id", "to_article_id"]
    ].astype(str)
    checked = keys.merge(
        approved,
        on=["business_date", "from_article_id", "to_article_id"],
        how="left",
        validate="many_to_one",
    )
    invalid = checked["relation_type"].ne(expected_type) | ~checked["formal_flow_allowed"].fillna(False)
    if invalid.any():
        sample = checked.loc[invalid].head(10).to_dict("records")
        raise ValueError(f"plan attempted to bypass {expected_type} resolution: {sample}")
    return snapshot_ids[0]

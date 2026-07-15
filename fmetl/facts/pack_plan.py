from __future__ import annotations

import pandas as pd

from fmetl.facts._resolution import assert_formal_pairs


def build_pack_plan(
    observed_events: pd.DataFrame,
    article_convert: pd.DataFrame,
    resolution: pd.DataFrame,
) -> pd.DataFrame:
    """Build one-to-one package flows only from observed conversion events."""
    event_columns = {
        "store_id", "business_date", "parent_article_id", "sub_article_id",
        "parent_qty", "sub_qty", "event_source",
    }
    convert_columns = {"parent_article_id", "sub_article_id", "parent_rate", "sub_rate"}
    missing_events = sorted(event_columns - set(observed_events.columns))
    missing_convert = sorted(convert_columns - set(article_convert.columns))
    if missing_events:
        raise KeyError(f"pack events missing columns: {missing_events}")
    if missing_convert:
        raise KeyError(f"article_convert missing columns: {missing_convert}")
    if observed_events.empty:
        return pd.DataFrame(columns=[
            *sorted(event_columns), "parent_rate", "sub_rate", "common_weight_residual",
            "relation_type", "relation_snapshot_id",
        ])

    snapshot_id = assert_formal_pairs(
        observed_events,
        resolution,
        expected_type="PACK_CONVERT",
        source_column="parent_article_id",
        target_column="sub_article_id",
    )

    convert = article_convert[list(convert_columns)].copy()
    convert["parent_rate"] = pd.to_numeric(convert["parent_rate"], errors="raise")
    convert["sub_rate"] = pd.to_numeric(convert["sub_rate"], errors="raise")
    if convert.duplicated(["parent_article_id", "sub_article_id"]).any():
        duplicates = convert.loc[
            convert.duplicated(["parent_article_id", "sub_article_id"], keep=False)
        ]
        rate_counts = duplicates.groupby(["parent_article_id", "sub_article_id"])[
            ["parent_rate", "sub_rate"]
        ].nunique()
        if (rate_counts > 1).any().any():
            raise ValueError("article_convert contains conflicting rates for the same pair")
        convert = convert.drop_duplicates(["parent_article_id", "sub_article_id"])
    if ((convert["parent_rate"] <= 0) | (convert["sub_rate"] <= 0)).any():
        raise ValueError("pack conversion rates must be positive")
    reciprocal_error = (convert["parent_rate"] * convert["sub_rate"] - 1.0).abs()
    if (reciprocal_error > 0.001).any():
        bad = convert.loc[reciprocal_error > 0.001].head(10).to_dict("records")
        raise ValueError(f"pack reciprocal factors are inconsistent: {bad}")

    plan = observed_events.copy()
    if plan.duplicated(["store_id", "business_date", "parent_article_id", "sub_article_id"]).any():
        raise ValueError("pack events must be unique per store/date/pair")
    plan["parent_article_id"] = plan["parent_article_id"].astype(str)
    plan["sub_article_id"] = plan["sub_article_id"].astype(str)
    convert["parent_article_id"] = convert["parent_article_id"].astype(str)
    convert["sub_article_id"] = convert["sub_article_id"].astype(str)
    plan = plan.merge(
        convert,
        on=["parent_article_id", "sub_article_id"],
        how="left",
        validate="many_to_one",
        indicator=True,
    )
    if plan["_merge"].ne("both").any():
        raise ValueError("observed pack event has no valid article_convert relation")
    plan = plan.drop(columns="_merge")
    plan["parent_qty"] = pd.to_numeric(plan["parent_qty"], errors="coerce")
    plan["sub_qty"] = pd.to_numeric(plan["sub_qty"], errors="coerce")
    has_parent = plan["parent_qty"].notna() & plan["parent_qty"].gt(0)
    has_sub = plan["sub_qty"].notna() & plan["sub_qty"].gt(0)
    if (~(has_parent | has_sub)).any():
        raise ValueError("pack event must observe at least one positive side")
    plan.loc[has_parent & ~has_sub, "sub_qty"] = (
        plan.loc[has_parent & ~has_sub, "parent_qty"]
        * plan.loc[has_parent & ~has_sub, "parent_rate"]
    )
    plan.loc[has_sub & ~has_parent, "parent_qty"] = (
        plan.loc[has_sub & ~has_parent, "sub_qty"]
        * plan.loc[has_sub & ~has_parent, "sub_rate"]
    )
    plan["common_weight_residual"] = plan["parent_qty"] - plan["sub_qty"] * plan["sub_rate"]
    if (plan["common_weight_residual"].abs() > 0.001).any():
        raise ValueError("observed pack quantities violate article_convert common-unit balance")
    plan["relation_type"] = "PACK_CONVERT"
    plan["relation_snapshot_id"] = snapshot_id
    return plan

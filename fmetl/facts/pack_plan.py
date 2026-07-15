from __future__ import annotations

import numpy as np
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
    convert_columns = {
        "store_id", "parent_article_id", "sub_article_id", "parent_rate", "sub_rate",
    }
    missing_events = sorted(event_columns - set(observed_events.columns))
    missing_convert = sorted(convert_columns - set(article_convert.columns))
    if "inc_day" not in article_convert.columns and "business_date" not in article_convert.columns:
        missing_convert.append("inc_day (or business_date)")
    if missing_events:
        raise KeyError(f"pack events missing columns: {missing_events}")
    if missing_convert:
        raise KeyError(f"article_convert missing columns: {missing_convert}")
    if observed_events.empty:
        return pd.DataFrame(columns=[
            *sorted(event_columns), "parent_rate", "sub_rate", "common_weight_residual",
            "relation_type", "relation_snapshot_id",
        ])

    event_keys = ["store_id", "business_date", "parent_article_id", "sub_article_id"]
    if observed_events[event_keys].isna().any().any():
        raise ValueError("pack event keys cannot contain NULL")
    if (
        observed_events[event_keys]
        .astype(str)
        .apply(lambda column: column.str.strip().str.lower().isin({"", "nan", "none", "null"}))
        .any().any()
    ):
        raise ValueError("pack event keys cannot be blank")

    snapshot_id = assert_formal_pairs(
        observed_events,
        resolution,
        expected_type="PACK_CONVERT",
        source_column="parent_article_id",
        target_column="sub_article_id",
    )

    convert_date_column = "inc_day" if "inc_day" in article_convert.columns else "business_date"
    convert = article_convert[[*sorted(convert_columns), convert_date_column]].copy()
    convert = convert.rename(columns={convert_date_column: "business_date"})
    convert_keys = ["store_id", "business_date", "parent_article_id", "sub_article_id"]
    if convert[convert_keys].isna().any().any():
        raise ValueError("article_convert keys cannot contain NULL")
    convert[convert_keys] = convert[convert_keys].astype(str)
    if (
        convert[convert_keys]
        .apply(lambda column: column.str.strip().str.lower().isin({"", "nan", "none", "null"}))
        .any().any()
    ):
        raise ValueError("article_convert keys cannot be blank")
    event_key_frame = observed_events[convert_keys].astype(str)
    observed_keys = set(event_key_frame.itertuples(index=False, name=None))
    convert = convert.loc[
        convert[convert_keys].apply(tuple, axis=1).isin(observed_keys)
    ].copy()
    convert["parent_rate"] = pd.to_numeric(convert["parent_rate"], errors="raise")
    convert["sub_rate"] = pd.to_numeric(convert["sub_rate"], errors="raise")
    if not np.isfinite(convert[["parent_rate", "sub_rate"]].to_numpy(dtype=float)).all():
        raise ValueError("pack conversion rates must be finite")
    if convert.duplicated(convert_keys).any():
        duplicates = convert.loc[
            convert.duplicated(convert_keys, keep=False)
        ]
        rate_counts = duplicates.groupby(convert_keys)[
            ["parent_rate", "sub_rate"]
        ].nunique()
        if (rate_counts > 1).any().any():
            raise ValueError("article_convert contains conflicting rates for the same store/date/pair")
        convert = convert.drop_duplicates(convert_keys)
    if ((convert["parent_rate"] <= 0) | (convert["sub_rate"] <= 0)).any():
        raise ValueError("pack conversion rates must be positive")
    reciprocal_error = (convert["parent_rate"] * convert["sub_rate"] - 1.0).abs()
    if (reciprocal_error > 0.001).any():
        bad = convert.loc[reciprocal_error > 0.001].head(10).to_dict("records")
        raise ValueError(f"pack reciprocal factors are inconsistent: {bad}")

    plan = observed_events.copy()
    if plan.duplicated(event_keys).any():
        raise ValueError("pack events must be unique per store/date/pair")
    plan[event_keys] = plan[event_keys].astype(str)
    plan = plan.merge(
        convert,
        on=convert_keys,
        how="left",
        validate="many_to_one",
        indicator=True,
    )
    if plan["_merge"].ne("both").any():
        raise ValueError("observed pack event has no valid article_convert relation")
    plan = plan.drop(columns="_merge")
    for quantity_column in ("parent_qty", "sub_qty"):
        if plan[quantity_column].map(
            lambda value: isinstance(value, str) and not value.strip()
        ).any():
            raise ValueError("pack observed quantities cannot use blank strings as missing values")
        plan[quantity_column] = pd.to_numeric(plan[quantity_column], errors="raise")
    quantities = plan[["parent_qty", "sub_qty"]].to_numpy(dtype=float)
    if np.isinf(quantities).any():
        raise ValueError("pack observed quantities must be finite when present")
    if (plan[["parent_qty", "sub_qty"]] < 0).any().any():
        raise ValueError("pack observed quantities cannot be negative")
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

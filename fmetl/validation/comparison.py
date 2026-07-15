from __future__ import annotations

import numpy as np
import pandas as pd


PATH_COLUMNS = (
    "category_level1_description",
    "category_level2_description",
    "category_level3_description",
)


def compare_v15_profit(
    v013: pd.DataFrame,
    v15: pd.DataFrame,
    *,
    large_category_tolerance: float = 0.05,
) -> pd.DataFrame:
    """Compare store profit on full hierarchy paths; only large-category ±5% is gating."""
    required = {
        "business_date", "store_name", "level_description", "day_clear",
        "store_profit_amount", "total_sale_amount", *PATH_COLUMNS,
    }
    for label, frame in (("v0.13", v013), ("v1.5", v15)):
        missing = sorted(required - set(frame.columns))
        if missing:
            raise KeyError(f"{label} comparison input missing columns: {missing}")
    keys = ["business_date", "store_name", "level_description", "day_clear", *PATH_COLUMNS]

    def aggregate(frame: pd.DataFrame, prefix: str) -> pd.DataFrame:
        work = frame[list(required)].copy()
        for column in keys:
            work[column] = work[column].fillna("").astype(str)
        work["store_profit_amount"] = pd.to_numeric(work["store_profit_amount"], errors="raise")
        work["total_sale_amount"] = pd.to_numeric(work["total_sale_amount"], errors="raise")
        return work.groupby(keys, as_index=False, dropna=False).agg(
            **{
                f"{prefix}_store_profit_amount": ("store_profit_amount", "sum"),
                f"{prefix}_total_sale_amount": ("total_sale_amount", "sum"),
            }
        )

    new = aggregate(v013, "v013")
    old = aggregate(v15, "v15")
    result = new.merge(old, on=keys, how="outer", validate="one_to_one", indicator=True)
    numeric = [
        "v013_store_profit_amount", "v013_total_sale_amount",
        "v15_store_profit_amount", "v15_total_sale_amount",
    ]
    result[numeric] = result[numeric].fillna(0.0)
    result["profit_diff_amount"] = (
        result["v013_store_profit_amount"] - result["v15_store_profit_amount"]
    )
    denominator = result["v15_store_profit_amount"].abs()
    result["profit_diff_pct"] = np.where(
        denominator.gt(0.01), result["profit_diff_amount"] / denominator, np.nan
    )
    result["is_gate_level"] = result["level_description"].eq("大分类")
    result["comparison_status"] = "LOCATE_ONLY"
    gate = result["is_gate_level"] & denominator.gt(0.01)
    result.loc[gate & result["profit_diff_pct"].abs().le(large_category_tolerance), "comparison_status"] = "PASS"
    result.loc[gate & result["profit_diff_pct"].abs().gt(large_category_tolerance), "comparison_status"] = "REVIEW"
    result.loc[result["_merge"].ne("both"), "comparison_status"] = "MISSING_SIDE"
    return result.drop(columns="_merge").sort_values(keys, kind="stable").reset_index(drop=True)

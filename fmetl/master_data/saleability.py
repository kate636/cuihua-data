from __future__ import annotations

import numpy as np
import pandas as pd


OUTPUT_COLUMNS = (
    "store_id",
    "business_date",
    "article_id",
    "is_orderable",
    "is_saleable",
    "saleability_status",
)


def normalize_order_saleability(frame: pd.DataFrame, *, store_id: str = "A3XV") -> pd.DataFrame:
    """Normalize v1.5 step-26 daily flags while preserving both dimensions."""
    required = ["store_id", "inc_day", "article_id", "is_order", "saleable", "status"]
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise KeyError(f"order saleability missing columns: {missing}")
    if frame.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    result = frame[required].copy()
    keys = ["store_id", "inc_day", "article_id"]
    if result[keys].isna().any().any():
        raise ValueError("order saleability keys cannot contain NULL")
    if (
        result[keys].astype(str)
        .apply(lambda column: column.str.strip().str.lower().isin({"", "nan", "none", "null"}))
        .any().any()
    ):
        raise ValueError("order saleability keys cannot be blank")
    result[["store_id", "inc_day", "article_id"]] = result[
        ["store_id", "inc_day", "article_id"]
    ].astype(str)
    unexpected = sorted(set(result["store_id"]) - {store_id})
    if unexpected:
        raise ValueError(f"order saleability contains stores outside {store_id}: {unexpected}")
    if result.duplicated(keys).any():
        raise ValueError(f"order saleability grain is not unique: {keys}")

    for column in ("is_order", "saleable", "status"):
        result[column] = pd.to_numeric(result[column], errors="raise")
        if not np.isfinite(result[column].to_numpy(dtype=float)).all():
            raise ValueError(f"{column} must be finite and cannot be NULL")
        bad = sorted(set(result[column]) - {0, 1})
        if bad:
            raise ValueError(f"{column} must be binary: {bad}")

    result = result.rename(columns={"inc_day": "business_date"})
    result["is_orderable"] = result.pop("is_order").eq(1)
    result["is_saleable"] = result.pop("saleable").eq(1)
    result["saleability_status"] = result.pop("status").astype("int8")
    return result[list(OUTPUT_COLUMNS)].sort_values(
        ["store_id", "business_date", "article_id"], kind="stable"
    ).reset_index(drop=True)

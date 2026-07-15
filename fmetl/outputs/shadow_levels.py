from __future__ import annotations

import numpy as np
import pandas as pd


SUM_COLUMNS = (
    "gross_sale_qty", "sale_return_qty", "net_sale_qty", "net_sale_amt",
    "store_receive_qty", "store_receive_amt",
    "init_stock_qty", "init_stock_amt", "end_qty", "end_amt",
    "accounting_known_lost_qty", "accounting_known_lost_amt",
    "unknown_lost_qty", "unknown_lost_amt",
    "balance_unknown_qty", "balance_unknown_amt",
    "bom_in_qty", "bom_in_amt", "bom_out_qty", "bom_out_amt",
    "pack_in_qty", "pack_in_amt", "pack_out_qty", "pack_out_amt",
    "compose_in_qty", "compose_in_amt", "compose_out_qty", "compose_out_amt",
    "residual_transfer_in_qty", "residual_transfer_in_amt",
    "residual_transfer_out_qty", "residual_transfer_out_amt",
    "neg_clamp_cost_amt", "accounting_profit",
    "accounting_full_profit", "ccj_amt", "ccj_qty", "ssls_amt", "ssls_qty",
    "adjusted_lost_amt", "adjusted_lost_qty", "adjusted_known_lost_amt",
    "adjusted_profit_before_ssls", "adjusted_full_profit_before_ssls",
)


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise KeyError(f"{label} missing columns: {missing}")


def build_shadow_levels_daily(sku_daily: pd.DataFrame) -> pd.DataFrame:
    """Aggregate finalized SKU facts to store and full-path category levels."""
    dimensions = {
        "store_id", "store_name", "business_date", "article_id", "day_clear",
        "is_reportable",
        "report_category_code", "report_category_name",
        "category_level2_id", "category_level2_description",
        "category_level3_id", "category_level3_description",
    }
    _require(sku_daily, dimensions | set(SUM_COLUMNS), "shadow_sku_daily")
    if sku_daily.empty:
        raise ValueError("shadow_sku_daily cannot be empty")
    grain = ["store_id", "business_date", "article_id"]
    if sku_daily[grain].isna().any().any() or sku_daily.duplicated(grain).any():
        raise ValueError("shadow_sku_daily must be unique with non-NULL SKU-day keys")
    work = sku_daily.copy()
    valid_reportable = work["is_reportable"].map(
        lambda value: isinstance(value, (bool, np.bool_))
    )
    if not valid_reportable.all():
        raise ValueError("shadow_sku_daily.is_reportable must be boolean")
    work = work.loc[work["is_reportable"]].copy()
    if work.empty:
        raise ValueError("shadow_sku_daily has no reportable SKU rows")
    for column in SUM_COLUMNS:
        work[column] = pd.to_numeric(work[column], errors="raise")
        if work[column].isna().any() or not np.isfinite(work[column].to_numpy(dtype=float)).all():
            raise ValueError(f"shadow_sku_daily.{column} must be finite and non-NULL")

    level_specs = {
        "门店": [],
        "大分类": ["category_level1_id", "category_level1_description"],
        "中分类": [
            "category_level1_id", "category_level1_description",
            "category_level2_id", "category_level2_description",
        ],
        "小分类": [
            "category_level1_id", "category_level1_description",
            "category_level2_id", "category_level2_description",
            "category_level3_id", "category_level3_description",
        ],
    }
    work["category_level1_id"] = work["report_category_code"]
    work["category_level1_description"] = work["report_category_name"]
    hierarchy = [
        "category_level1_id", "category_level1_description",
        "category_level2_id", "category_level2_description",
        "category_level3_id", "category_level3_description",
    ]
    rows: list[pd.DataFrame] = []
    for level, path in level_specs.items():
        for dc in ("0", "1", "2"):
            source = work if dc == "2" else work.loc[work["day_clear"].astype(str).eq(dc)]
            if source.empty:
                continue
            group = ["store_id", "store_name", "business_date", *path]
            result = source.groupby(group, as_index=False, dropna=False).agg(
                **{column: (column, "sum") for column in SUM_COLUMNS}
            )
            result["level_description"] = level
            result["day_clear"] = dc
            for column in hierarchy:
                if column not in result:
                    result[column] = ""
            result["total_sale_qty"] = result["net_sale_qty"]
            result["total_sale_amount"] = result["net_sale_amt"]
            result["inbound_qty"] = result["store_receive_qty"]
            result["inbound_amount"] = result["store_receive_amt"]
            result["init_stock_qty"] = result["init_stock_qty"]
            result["init_stock_amount"] = result["init_stock_amt"]
            result["end_stock_qty"] = result["end_qty"]
            result["end_stock_amount"] = result["end_amt"]
            result["known_lost_amt"] = result["accounting_known_lost_amt"]
            result["store_profit_amount"] = result["accounting_profit"]
            rows.append(result)
    output = pd.concat(rows, ignore_index=True)
    output["adjusted_profit"] = output["adjusted_profit_before_ssls"]
    output["adjusted_full_profit"] = output["adjusted_full_profit_before_ssls"]
    large = output["level_description"].eq("大分类")
    large_total = output.loc[large & output["day_clear"].eq("2")].groupby(
        ["store_id", "business_date"], as_index=False
    )["ssls_amt"].sum().rename(columns={"ssls_amt": "total_ssls_amt"})
    output = output.merge(
        large_total,
        on=["store_id", "business_date"],
        how="left",
        validate="many_to_one",
    )
    output["total_ssls_amt"] = output["total_ssls_amt"].fillna(0.0)
    cooked_debit = output["total_ssls_amt"].where(
        large & output["category_level1_description"].eq("熟食类"), 0.0
    )
    output.loc[large, "adjusted_profit"] = (
        output.loc[large, "adjusted_profit_before_ssls"]
        + output.loc[large, "ssls_amt"]
        - cooked_debit.loc[large]
    )
    output.loc[large, "adjusted_full_profit"] = (
        output.loc[large, "adjusted_full_profit_before_ssls"]
        + output.loc[large, "ssls_amt"]
        - cooked_debit.loc[large]
    )
    output["ssls_transfer_delta"] = output["adjusted_profit"] - output["adjusted_profit_before_ssls"]
    keys = ["store_id", "business_date", "level_description", "day_clear", *hierarchy]
    if output.duplicated(keys).any():
        raise ValueError("shadow levels output is not unique on the full hierarchy path")
    return output.sort_values(keys, kind="stable").reset_index(drop=True)

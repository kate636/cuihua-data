from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

from fmetl.contracts.v014 import OUTPUT_CONTRACT


DIMENSION_COLUMNS = {
    "store_flag", "store_no", "business_date", "store_name", "sku_id", "sku_name",
    "category_level1_description", "category_level2_description",
    "category_level3_description", "spu_id", "spu_name", "day_clear",
    "article_group_id", "article_group_name", "is_processing_raw", "is_reportable",
}

# Every item below is independently produced from v0.14 facts before report
# aggregation.  Missing inputs are a hard error; the output builder never
# backfills a v1.5 result or silently invents a zero-valued metric.
ADDITIVE_INPUTS = {
    "accounting_full_profit", "supply_chain_profit_amount", "accounting_profit",
    "sales_weight", "sales_weight_before_19", "total_sale_qty", "sale_qty_before_19",
    "inbound_amount", "total_sale_amount", "total_customer_count",
    "sale_amount_before_19", "customer_count_before_19", "piece_sales_before_19",
    "original_sale_amount", "initial_inventory_amount", "ending_inventory_amount",
    "outbound_cost", "purchase_weight_amount", "supply_chain_promotion_amount",
    "discount_amount", "time_period_discount_amount", "promotional_discount_amount",
    "store_expected_profit_amount", "supply_chain_profit_rate_denominator",
    "supply_chain_expected_profit_rate_numerator",
    "supply_chain_expected_profit_rate_denominator",
    "store_pricing_profit_rate_numerator", "supply_chain_discount_rate_denominator",
    "loss_rate_denominator", "return_rate_numerator", "return_rate_denominator",
    "lp_sale_amt", "init_stock_qty", "end_stock_qty", "avg_7d_sale_qty", "inbound_qty",
    "loss_amount", "loss_qty", "loss_rate_qty_denominator", "stock_sku_flag",
    "active_sku_flag", "sale_piece_qty", "store_know_lost_amt", "store_unknow_lost_amt",
    "soldout_16_numerator", "soldout_16_denominator", "soldout_20_numerator",
    "soldout_20_denominator", "online_customer_count", "offline_customer_count",
    "jielong_customer_count", "jsd_customer_count", "online_sale_amt", "offline_sale_amt",
    "jielong_sale_amt", "jsd_sale_amt", "online_sale_qty", "offline_sale_qty",
    "jielong_sale_qty", "jsd_sale_qty", "new_cust_num", "old_cust_num",
    "new_cust_sale_amt", "old_cust_sale_amt", "new_cust_sale_qty", "old_cust_sale_qty",
    "ccj_amt", "ccj_qty", "ssls_amt", "ssls_qty",
}

DISTINCT_COUNT_INPUTS = {
    "total_customer_count", "customer_count_before_19",
    "online_customer_count", "offline_customer_count",
    "jielong_customer_count", "jsd_customer_count",
    "new_cust_num", "old_cust_num",
}


def _divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator_values = denominator.to_numpy(dtype=float)
    # Additive money/quantity denominators can retain floating cancellation
    # residue after multi-level aggregation.  Values below the persisted
    # DECIMAL(18,4) precision are accounting zero and must not create an
    # unbounded rate.
    valid = np.isfinite(denominator_values) & (np.abs(denominator_values) >= 0.00005)
    values = np.divide(
        numerator.to_numpy(dtype=float),
        denominator_values,
        out=np.zeros(len(numerator), dtype=float),
        where=valid,
    )
    values[~np.isfinite(values)] = 0.0
    return pd.Series(values, index=numerator.index)


def _aggregate(source: pd.DataFrame, group: list[str]) -> pd.DataFrame:
    aggregations = {
        column: (column, "sum")
        for column in ADDITIVE_INPUTS - DISTINCT_COUNT_INPUTS
    }
    return source.groupby(group, as_index=False, dropna=False).agg(**aggregations)


def _aggregate_customer_counts(
    events: pd.DataFrame,
    group: list[str],
) -> pd.DataFrame:
    masks = {
        "total_customer_count": pd.Series(True, index=events.index),
        "customer_count_before_19": events["is_before_19"],
        "online_customer_count": events["is_online"],
        "offline_customer_count": events["is_offline"],
        "jielong_customer_count": events["is_jielong"],
        "jsd_customer_count": events["is_jsd"],
        "new_cust_num": events["is_new"],
        "old_cust_num": events["is_old"],
    }
    parts: list[pd.DataFrame] = []
    for column, mask in masks.items():
        part = (
            events.loc[mask, [*group, "order_id"]]
            .drop_duplicates()
            .groupby(group, as_index=False, dropna=False)["order_id"]
            .nunique()
            .rename(columns={"order_id": column})
        )
        parts.append(part)
    result = parts[0]
    for part in parts[1:]:
        result = result.merge(part, on=group, how="outer", validate="one_to_one")
    for column in DISTINCT_COUNT_INPUTS:
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0.0)
    return result


def _level_specs() -> tuple[tuple[str, list[str], str, str], ...]:
    return (
        ("门店", [], "", ""),
        ("大分类", ["category_level1_description"], "category_level1_description", ""),
        (
            "中分类",
            ["category_level1_description", "category_level2_description"],
            "category_level2_description", "",
        ),
        (
            "小分类",
            ["category_level1_description", "category_level2_description", "category_level3_description"],
            "category_level3_description", "",
        ),
        (
            "spu",
            ["category_level1_description", "category_level2_description", "category_level3_description", "spu_id", "spu_name"],
            "spu_name", "spu_id",
        ),
        (
            "sku",
            [
                "category_level1_description", "category_level2_description",
                "category_level3_description", "sku_id", "sku_name",
                "article_group_id", "article_group_name",
            ],
            "sku_name", "sku_id",
        ),
        (
            "商品集",
            ["article_group_id", "article_group_name"],
            "article_group_name", "article_group_id",
        ),
    )


def build_v014_levels_result(
    sku_daily: pd.DataFrame,
    customer_events: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build the 123-field compatible result plus the product-group level.

    Ratios are always recalculated after aggregation.  Processing raw SKUs are
    retained in monetary and quantity accounting, but excluded from active and
    listed SKU metrics as required by the master-data reporting rule.
    """
    missing = sorted((DIMENSION_COLUMNS | ADDITIVE_INPUTS) - set(sku_daily.columns))
    if missing:
        raise KeyError(f"v014 sku daily missing output inputs: {missing}")
    if sku_daily.empty:
        raise ValueError("v014 sku daily cannot be empty")
    key = ["store_no", "business_date", "sku_id"]
    if sku_daily[key].isna().any().any() or sku_daily.duplicated(key).any():
        raise ValueError("v014 sku daily must be unique with non-null store/date/SKU keys")
    work = sku_daily.copy()
    text_dimensions = (
        DIMENSION_COLUMNS
        - {"is_processing_raw", "is_reportable"}
    )
    for column in text_dimensions:
        work[column] = work[column].fillna("").astype(str)
    for column in ADDITIVE_INPUTS:
        work[column] = pd.to_numeric(work[column], errors="raise")
        if work[column].isna().any() or not np.isfinite(work[column].to_numpy(dtype=float)).all():
            raise ValueError(f"v014 sku daily {column} must be finite and non-null")
    reportable_mask = work["is_reportable"].map(
        lambda value: value if isinstance(value, (bool, np.bool_))
        else str(value).strip().lower() in {"1", "true", "yes"}
    )
    work = work.loc[reportable_mask].copy()
    if work.empty:
        raise ValueError("v014 public output has no reportable SKU rows")
    raw_mask = work["is_processing_raw"].map(
        lambda value: value if isinstance(value, (bool, np.bool_))
        else str(value).strip().lower() in {"1", "true", "yes"}
    )
    work.loc[raw_mask, ["stock_sku_flag", "active_sku_flag"]] = 0.0
    work["day_clear"] = work["day_clear"].astype(str)
    if not work["day_clear"].isin({"0", "1"}).all():
        raise ValueError("SKU day_clear must be 0 or 1; total is rebuilt from atomic rows")
    if customer_events is None:
        raise KeyError("v014 customer_events are required for distinct-order customer metrics")
    event_dimensions = {
        "store_flag", "store_no", "business_date", "store_name", "sku_id",
        "sku_name", "category_level1_description", "category_level2_description",
        "category_level3_description", "spu_id", "spu_name", "day_clear",
        "article_group_id", "article_group_name", "order_id", "is_before_19",
        "is_online", "is_offline", "is_jielong", "is_jsd", "is_new", "is_old",
    }
    missing_events = sorted(event_dimensions - set(customer_events.columns))
    if missing_events:
        raise KeyError(f"v014 customer events missing columns: {missing_events}")
    events = customer_events.copy()
    for column in event_dimensions - {
        "is_before_19", "is_online", "is_offline", "is_jielong",
        "is_jsd", "is_new", "is_old",
    }:
        events[column] = events[column].fillna("").astype(str)
    for column in (
        "is_before_19", "is_online", "is_offline", "is_jielong",
        "is_jsd", "is_new", "is_old",
    ):
        events[column] = events[column].map(
            lambda value: value if isinstance(value, (bool, np.bool_))
            else str(value).strip().lower() in {"1", "true", "yes"}
        )
    events = events.loc[
        events["store_no"].isin(work["store_no"])
        & events["business_date"].isin(work["business_date"])
    ].copy()

    rows: list[pd.DataFrame] = []
    common = ["store_flag", "store_no", "business_date", "store_name"]
    for level, path, name_column, id_column in _level_specs():
        if level == "商品集":
            level_source = work.loc[work["article_group_id"].str.strip().ne("")].copy()
        else:
            level_source = work
        for day_clear in ("0", "1", "2"):
            source = level_source if day_clear == "2" else level_source.loc[level_source["day_clear"].eq(day_clear)]
            if source.empty:
                continue
            result = _aggregate(source, [*common, *path])
            event_source = (
                events if day_clear == "2"
                else events.loc[events["day_clear"].eq(day_clear)]
            )
            if level == "商品集":
                event_source = event_source.loc[
                    event_source["article_group_id"].str.strip().ne("")
                ]
            counts = _aggregate_customer_counts(
                event_source, [*common, *path]
            )
            result = result.merge(
                counts, on=[*common, *path], how="left", validate="one_to_one"
            )
            for column in DISTINCT_COUNT_INPUTS:
                result[column] = pd.to_numeric(
                    result[column], errors="coerce"
                ).fillna(0.0)
            result["level_description"] = level
            result["day_clear"] = day_clear
            result["day_clear_flag"] = {"0": "日清", "1": "非日清", "2": "合计"}[day_clear]
            result["category_name"] = result[name_column] if name_column else result["store_name"]
            result["sku_id"] = result[id_column] if id_column else ""
            for column in (
                "category_level1_description", "category_level2_description",
                "category_level3_description", "article_group_id", "article_group_name",
            ):
                if column not in result:
                    result[column] = ""
            if level != "商品集":
                result["article_group_id"] = result.get("article_group_id", "")
                result["article_group_name"] = result.get("article_group_name", "")
            result["operating_store_days"] = 1
            result["operating_store_count"] = 1
            rows.append(result)
    output = pd.concat(rows, ignore_index=True)
    # Keep the ledger's accounting bottom separate from the v1.5-compatible
    # display convention. 炒菜机 and 生熟联动 amounts adjust only public loss
    # and profit fields; they never change inventory, cost, or internal posting.
    special_amt = output["ccj_amt"] + output["ssls_amt"]
    special_qty = output["ccj_qty"] + output["ssls_qty"]
    # CCJ is a compatible display add-back at every aggregation level.  SSLS
    # is a category-only transfer: credit the source large category and debit
    # 熟食.  Applying SSLS at SKU/SPU/store creates a non-additive hierarchy.
    output["accounting_profit"] += output["ccj_amt"]
    output["accounting_full_profit"] += output["ccj_amt"]
    output["loss_amount"] -= special_amt
    output["loss_qty"] -= special_qty
    output["store_know_lost_amt"] -= special_amt
    ssls_totals = output.loc[
        output["level_description"].eq("门店"),
        ["store_no", "business_date", "day_clear", "ssls_amt"],
    ].rename(columns={"ssls_amt": "_total_ssls_amt"})
    output = output.merge(
        ssls_totals,
        on=["store_no", "business_date", "day_clear"],
        how="left",
        validate="many_to_one",
    )
    large_category = output["level_description"].eq("大分类")
    output.loc[large_category, "accounting_profit"] += output.loc[
        large_category, "ssls_amt"
    ]
    output.loc[large_category, "accounting_full_profit"] += output.loc[
        large_category, "ssls_amt"
    ]
    ssls_debit = (
        large_category
        & output["category_level1_description"].eq("熟食类")
    )
    output.loc[ssls_debit, "accounting_profit"] -= output.loc[
        ssls_debit, "_total_ssls_amt"
    ].fillna(0.0)
    output.loc[ssls_debit, "accounting_full_profit"] -= output.loc[
        ssls_debit, "_total_ssls_amt"
    ].fillna(0.0)
    output = output.drop(columns="_total_ssls_amt")
    rename = {
        "accounting_full_profit": "full_link_profit_amount",
        "accounting_profit": "store_profit_amount",
        "stock_sku_flag": "is_stock_sku",
        "active_sku_flag": "active_sku_count",
    }
    output = output.rename(columns=rename)
    output["sale_article_num_cate"] = _divide(output["active_sku_count"], output["is_stock_sku"])
    output["full_link_profit_rate"] = _divide(output["full_link_profit_amount"], output["total_sale_amount"])
    output["supply_chain_profit_rate"] = _divide(
        output["supply_chain_profit_amount"], output["supply_chain_profit_rate_denominator"]
    )
    output["store_profit_rate"] = _divide(output["store_profit_amount"], output["total_sale_amount"])
    output["supply_chain_expected_profit_rate"] = _divide(
        output["supply_chain_expected_profit_rate_numerator"],
        output["supply_chain_expected_profit_rate_denominator"],
    )
    output["store_expected_profit_rate"] = _divide(
        output["store_expected_profit_amount"], output["lp_sale_amt"]
    )
    output["store_pricing_profit_rate"] = _divide(
        output["store_pricing_profit_rate_numerator"], output["total_sale_amount"]
    )
    output["total_per_customer_transaction"] = _divide(
        output["total_sale_amount"], output["total_customer_count"]
    )
    output["per_customer_transaction_before_19"] = _divide(
        output["sale_amount_before_19"], output["customer_count_before_19"]
    )
    output["per_item_price_before_19"] = _divide(
        output["sale_amount_before_19"], output["piece_sales_before_19"]
    )
    output["item_per_customer_before_19"] = _divide(
        output["piece_sales_before_19"], output["customer_count_before_19"]
    )
    output["purchase_price"] = _divide(output["outbound_cost"], output["purchase_weight_amount"])
    output["inbound_price"] = _divide(output["inbound_amount"], output["purchase_weight_amount"])
    output["average_selling_price"] = _divide(output["total_sale_amount"], output["sales_weight"])
    output["average_sales_original_price"] = _divide(output["lp_sale_amt"], output["sales_weight"])
    output["supply_chain_discount_rate"] = _divide(
        output["supply_chain_promotion_amount"], output["supply_chain_discount_rate_denominator"]
    )
    output["discount_rate"] = _divide(output["discount_amount"], output["lp_sale_amt"])
    output["promotional_discount_rate"] = _divide(
        output["promotional_discount_amount"], output["lp_sale_amt"]
    )
    output["time_period_discount_rate"] = _divide(
        output["time_period_discount_amount"], output["lp_sale_amt"]
    )
    output["loss_rate"] = _divide(output["loss_amount"], output["loss_rate_denominator"])
    output["loss_rate_sales_amount"] = _divide(output["loss_amount"], output["total_sale_amount"])
    output["loss_rate_qty"] = _divide(output["loss_qty"], output["loss_rate_qty_denominator"])
    output["return_rate"] = _divide(output["return_rate_numerator"], output["return_rate_denominator"])
    output["product_efficiency"] = _divide(output["total_sale_amount"], output["active_sku_count"])
    output["soldout_rate_16"] = _divide(output["soldout_16_numerator"], output["soldout_16_denominator"])
    output["soldout_rate_20"] = _divide(output["soldout_20_numerator"], output["soldout_20_denominator"])
    output["turnover_rate"] = _divide(
        output["init_stock_qty"] + output["inbound_qty"], output["avg_7d_sale_qty"]
    )
    output["original_sale_amount"] = output["original_sale_amount"]
    for prefix in ("online", "offline", "jielong", "jsd"):
        output[f"{prefix}_per_customer"] = _divide(
            output[f"{prefix}_sale_amt"], output[f"{prefix}_customer_count"]
        )
        output[f"{prefix}_per_item"] = _divide(
            output[f"{prefix}_sale_amt"], output[f"{prefix}_sale_qty"]
        )
        output[f"{prefix}_item_per_customer"] = _divide(
            output[f"{prefix}_sale_qty"], output[f"{prefix}_customer_count"]
        )
    for prefix in ("new_cust", "old_cust"):
        output[f"{prefix}_per_customer"] = _divide(
            output[f"{prefix}_sale_amt"], output[f"{prefix}_num"]
        )
        output[f"{prefix}_per_item"] = _divide(
            output[f"{prefix}_sale_amt"], output[f"{prefix}_sale_qty"]
        )
        output[f"{prefix}_item_per_customer"] = _divide(
            output[f"{prefix}_sale_qty"], output[f"{prefix}_num"]
        )
    share_group = ["business_date", "store_no", "level_description", "day_clear", "category_level1_description"]
    denominator = output.groupby(share_group, dropna=False)["total_sale_amount"].transform("sum")
    output["sales_proportion_within_group"] = _divide(output["total_sale_amount"], denominator)
    rank_mask = output["level_description"].isin({"spu", "sku", "商品集"})
    output["sales_rank_in_middle_category"] = ""
    output["sales_rank_in_large_category"] = ""
    output.loc[rank_mask, "sales_rank_in_middle_category"] = (
        output.loc[rank_mask].groupby(
            ["business_date", "store_no", "day_clear", "level_description", "category_level2_description"],
            dropna=False,
        )["total_sale_amount"].rank(method="first", ascending=False).astype(int).astype(str)
    )
    output.loc[rank_mask, "sales_rank_in_large_category"] = (
        output.loc[rank_mask].groupby(
            ["business_date", "store_no", "day_clear", "level_description", "category_level1_description"],
            dropna=False,
        )["total_sale_amount"].rank(method="first", ascending=False).astype(int).astype(str)
    )
    fields = [field.name for field in OUTPUT_CONTRACT]
    missing_output = sorted(set(fields) - set(output.columns))
    if missing_output:
        raise RuntimeError(f"v014 result builder did not produce contract fields: {missing_output}")
    result = output[fields].copy()
    if result.isna().any().any():
        bad = result.columns[result.isna().any()].tolist()
        raise ValueError(f"v014 output contract contains NULL values: {bad}")
    return result


def contract_names(fields: Iterable = OUTPUT_CONTRACT) -> list[str]:
    return [field.name for field in fields]

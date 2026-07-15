from __future__ import annotations

import numpy as np
import pandas as pd


SALES_OUTPUT_COLUMNS = (
    "store_id", "business_date", "article_id", "source_sales_day_clear",
    "gross_sale_qty", "sale_return_qty", "net_sale_qty",
    "gross_sale_amt", "sale_return_revenue_amt", "net_sale_amt",
    "source_return_label_signed_qty", "source_return_label_signed_amt",
)
LOSS_OUTPUT_COLUMNS = (
    "store_id", "business_date", "article_id", "known_lost_qty",
    "source_known_lost_amt", "source_unknown_lost_qty", "source_unknown_lost_amt",
)
COUNT_OUTPUT_COLUMNS = (
    "store_id", "business_date", "article_id", "source_actual_stock_qty",
    "actual_stock_qty", "source_sale_stock_qty", "source_profit_loss_qty",
    "created_by", "updated_by", "created_at", "updated_at",
    "is_counted", "is_explicit_operator_count", "count_status", "count_evidence",
)
DAY_CLEAR_OUTPUT_COLUMNS = ("store_id", "business_date", "article_id", "day_clear")


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise KeyError(f"{label} missing columns: {missing}")


def _validate_keys(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    if frame[columns].isna().any().any():
        raise ValueError(f"{label} keys cannot contain NULL")
    blank = frame[columns].astype(str).apply(
        lambda column: column.str.strip().str.lower().isin({"", "nan", "none", "null"})
    )
    if blank.any().any():
        raise ValueError(f"{label} keys cannot be blank")


def _only_store(frame: pd.DataFrame, store_id: str, label: str) -> None:
    unexpected = sorted(set(frame["store_id"].astype(str)) - {store_id})
    if unexpected:
        raise ValueError(f"{label} contains stores outside {store_id}: {unexpected}")


def _required_numeric(frame: pd.DataFrame, columns: tuple[str, ...], label: str) -> None:
    for column in columns:
        frame[column] = pd.to_numeric(frame[column], errors="raise")
        if frame[column].isna().any():
            raise ValueError(f"{label}.{column} cannot be NULL")
        if not np.isfinite(frame[column].to_numpy(dtype=float)).all():
            raise ValueError(f"{label}.{column} must be finite")


def _nullable_numeric(frame: pd.DataFrame, columns: tuple[str, ...], label: str) -> None:
    for column in columns:
        frame[column] = pd.to_numeric(frame[column], errors="raise")
        present = frame[column].notna()
        if not np.isfinite(frame.loc[present, column].to_numpy(dtype=float)).all():
            raise ValueError(f"{label}.{column} must be finite when present")


def normalize_sales_events(sales: pd.DataFrame, *, store_id: str = "A3XV") -> pd.DataFrame:
    """Aggregate signed source rows into separate nonnegative sale/return inventory flows."""
    required = {
        "store_id", "business_date", "inc_day", "abi_article_id", "day_clear",
        "qty_spec", "sales_amt", "return_sale_qty", "return_sale_amt",
    }
    _require(sales, required, "sales")
    if sales.empty:
        return pd.DataFrame(columns=SALES_OUTPUT_COLUMNS)
    frame = sales[list(required)].copy().rename(columns={"abi_article_id": "article_id"})
    _validate_keys(frame, ["business_date", "inc_day"], "sales")
    if frame["business_date"].astype(str).ne(frame["inc_day"].astype(str)).any():
        raise ValueError("sales.business_date must match its inc_day partition")
    frame = frame.drop(columns="inc_day")
    keys = ["store_id", "business_date", "article_id"]
    _validate_keys(frame, [*keys, "day_clear"], "sales")
    frame[[*keys, "day_clear"]] = frame[[*keys, "day_clear"]].astype(str)
    _only_store(frame, store_id, "sales")
    if not frame["day_clear"].isin({"0", "1"}).all():
        raise ValueError("sales.day_clear must be '0' or '1'")
    if frame.groupby(keys, dropna=False)["day_clear"].nunique().gt(1).any():
        raise ValueError("sales has conflicting source day_clear values for one SKU-day")
    _required_numeric(frame, ("qty_spec", "sales_amt"), "sales")
    _nullable_numeric(frame, ("return_sale_qty", "return_sale_amt"), "sales")
    frame[["return_sale_qty", "return_sale_amt"]] = frame[
        ["return_sale_qty", "return_sale_amt"]
    ].fillna(0.0)
    if frame["return_sale_qty"].gt(0.000001).any() or frame["return_sale_amt"].gt(0.000001).any():
        raise ValueError("sales return label fields are already signed and cannot be positive")

    frame["gross_sale_qty"] = frame["qty_spec"].clip(lower=0.0)
    frame["sale_return_qty"] = -frame["qty_spec"].clip(upper=0.0)
    frame["gross_sale_amt"] = frame["sales_amt"].clip(lower=0.0)
    frame["sale_return_revenue_amt"] = -frame["sales_amt"].clip(upper=0.0)
    frame["source_return_label_signed_qty"] = frame["return_sale_qty"]
    frame["source_return_label_signed_amt"] = frame["return_sale_amt"]
    result = frame.groupby(keys, as_index=False, dropna=False).agg(
        source_sales_day_clear=("day_clear", "first"),
        gross_sale_qty=("gross_sale_qty", "sum"),
        sale_return_qty=("sale_return_qty", "sum"),
        net_sale_qty=("qty_spec", "sum"),
        gross_sale_amt=("gross_sale_amt", "sum"),
        sale_return_revenue_amt=("sale_return_revenue_amt", "sum"),
        net_sale_amt=("sales_amt", "sum"),
        source_return_label_signed_qty=("source_return_label_signed_qty", "sum"),
        source_return_label_signed_amt=("source_return_label_signed_amt", "sum"),
    )
    qty_residual = result["gross_sale_qty"] - result["sale_return_qty"] - result["net_sale_qty"]
    amt_residual = result["gross_sale_amt"] - result["sale_return_revenue_amt"] - result["net_sale_amt"]
    if qty_residual.abs().gt(0.000001).any() or amt_residual.abs().gt(0.000001).any():
        raise ValueError("sales signed-flow split does not conserve source net values")
    return result[list(SALES_OUTPUT_COLUMNS)].sort_values(keys, kind="stable").reset_index(drop=True)


def normalize_known_loss(loss: pd.DataFrame, *, store_id: str = "A3XV") -> pd.DataFrame:
    """Keep known loss quantity as the only formal loss flow; preserve source amounts for audit."""
    required = {
        "store_id", "inc_day", "article_id", "know_lost_qty", "know_lost_amt",
        "unknow_lost_qty", "unknow_lost_amt",
    }
    _require(loss, required, "known_loss")
    if loss.empty:
        return pd.DataFrame(columns=LOSS_OUTPUT_COLUMNS)
    frame = loss[list(required)].copy().rename(columns={"inc_day": "business_date"})
    keys = ["store_id", "business_date", "article_id"]
    _validate_keys(frame, keys, "known_loss")
    frame[keys] = frame[keys].astype(str)
    _only_store(frame, store_id, "known_loss")
    _required_numeric(frame, ("know_lost_qty",), "known_loss")
    _nullable_numeric(
        frame, ("know_lost_amt", "unknow_lost_qty", "unknow_lost_amt"), "known_loss"
    )
    if frame["know_lost_qty"].lt(-0.000001).any():
        raise ValueError("known_loss.know_lost_qty cannot be negative")
    if frame.duplicated(keys).any():
        raise ValueError("known_loss source grain must be unique per store/date/article")
    frame = frame.rename(columns={
        "know_lost_qty": "known_lost_qty",
        "know_lost_amt": "source_known_lost_amt",
        "unknow_lost_qty": "source_unknown_lost_qty",
        "unknow_lost_amt": "source_unknown_lost_amt",
    })
    return frame[list(LOSS_OUTPUT_COLUMNS)].sort_values(keys, kind="stable").reset_index(drop=True)


def normalize_inventory_counts(detail: pd.DataFrame, *, store_id: str = "A3XV") -> pd.DataFrame:
    """Normalize valid end-balance observations without guessing who performed a count."""
    required = {
        "shop_id", "inventory_date", "inc_day", "sku_code", "sale_stock_qty",
        "actual_stock_qty", "profit_loss_qty", "created_by", "updated_by",
        "created_at", "updated_at",
    }
    _require(detail, required, "inventory_detail")
    if detail.empty:
        return pd.DataFrame(columns=COUNT_OUTPUT_COLUMNS)
    frame = detail[list(required)].copy()
    _validate_keys(frame, ["inventory_date", "inc_day"], "inventory_detail")
    if frame["inventory_date"].astype(str).ne(frame["inc_day"].astype(str)).any():
        raise ValueError("inventory_detail.inventory_date must match its inc_day partition")
    frame = frame.drop(columns="inc_day").rename(columns={
        "shop_id": "store_id", "inventory_date": "business_date", "sku_code": "article_id",
    })
    keys = ["store_id", "business_date", "article_id"]
    _validate_keys(frame, keys, "inventory_detail")
    frame[keys] = frame[keys].astype(str)
    _only_store(frame, store_id, "inventory_detail")
    if frame.duplicated(keys).any():
        raise ValueError("inventory_detail source grain must be unique per store/date/article")
    _nullable_numeric(
        frame, ("sale_stock_qty", "actual_stock_qty", "profit_loss_qty"), "inventory_detail"
    )
    complete_balance = frame[["sale_stock_qty", "actual_stock_qty", "profit_loss_qty"]].notna().all(axis=1)
    balance_residual = frame["actual_stock_qty"] - frame["sale_stock_qty"] - frame["profit_loss_qty"]
    if balance_residual[complete_balance].abs().gt(0.0001).any():
        raise ValueError("inventory_detail profit_loss_qty must equal actual_stock_qty - sale_stock_qty")
    actual_filled = frame["actual_stock_qty"].fillna(0.0).to_numpy(dtype=float)
    finite_actual = frame["actual_stock_qty"].notna() & np.isfinite(actual_filled)
    system_tokens = {"", "system", "系统", "nan", "none", "null"}
    creator = frame["created_by"].fillna("").astype(str).str.strip()
    updater = frame["updated_by"].fillna("").astype(str).str.strip()
    operator_creator = ~creator.str.lower().isin(system_tokens)
    operator_updater = ~updater.str.lower().isin(system_tokens)
    operator_evidence = operator_creator | operator_updater
    nonnegative_actual = finite_actual & frame["actual_stock_qty"].ge(0)
    formal_balance = nonnegative_actual
    frame["source_actual_stock_qty"] = frame["actual_stock_qty"]
    frame["actual_stock_qty"] = frame["actual_stock_qty"].where(formal_balance, np.nan)
    frame["source_sale_stock_qty"] = frame["sale_stock_qty"]
    frame["source_profit_loss_qty"] = frame["profit_loss_qty"]
    # A system row does not prove there was no manual count: upstream can reset both
    # sale_stock_qty and actual_stock_qty after a physical count.  Treat every valid
    # nonnegative snapshot as an observed end balance, while retaining provenance.
    frame["is_counted"] = formal_balance
    frame["is_explicit_operator_count"] = operator_evidence & formal_balance
    frame["count_status"] = "FORMAL_SYSTEM_BALANCE_SNAPSHOT"
    frame.loc[~operator_evidence & ~nonnegative_actual, "count_status"] = "INVALID_SYSTEM_ACTUAL"
    frame.loc[operator_evidence & ~nonnegative_actual, "count_status"] = "INVALID_OPERATOR_ACTUAL"
    frame.loc[operator_evidence & formal_balance, "count_status"] = "FORMAL_OPERATOR_COUNT"
    frame["count_evidence"] = "SYSTEM_METADATA_ONLY"
    frame.loc[operator_creator & ~operator_updater, "count_evidence"] = "CREATOR_OPERATOR"
    frame.loc[~operator_creator & operator_updater, "count_evidence"] = "UPDATER_OPERATOR"
    frame.loc[operator_creator & operator_updater, "count_evidence"] = "CREATOR_AND_UPDATER_OPERATOR"
    return frame[list(COUNT_OUTPUT_COLUMNS)].sort_values(keys, kind="stable").reset_index(drop=True)


def normalize_chdj_day_clear(frame: pd.DataFrame, *, store_id: str = "A3XV") -> pd.DataFrame:
    """Normalize the v1.5-compatible daily SKU label from chdj_article."""
    required = {"store_id", "inc_day", "article_id", "day_clear"}
    _require(frame, required, "chdj_day_clear")
    if frame.empty:
        return pd.DataFrame(columns=DAY_CLEAR_OUTPUT_COLUMNS)
    result = frame[list(required)].copy().rename(columns={"inc_day": "business_date"})
    keys = ["store_id", "business_date", "article_id"]
    _validate_keys(result, [*keys, "day_clear"], "chdj_day_clear")
    result[[*keys, "day_clear"]] = result[[*keys, "day_clear"]].astype(str)
    _only_store(result, store_id, "chdj_day_clear")
    if not result["day_clear"].isin({"0", "1"}).all():
        raise ValueError("chdj_day_clear.day_clear must be '0' or '1'")
    if result.duplicated(keys).any():
        raise ValueError("chdj_day_clear source grain must be unique per store/date/article")
    return result[list(DAY_CLEAR_OUTPUT_COLUMNS)].sort_values(keys, kind="stable").reset_index(drop=True)


def attach_authoritative_day_clear(
    sales: pd.DataFrame, day_clear: pd.DataFrame
) -> pd.DataFrame:
    """Attach the chdj_article label and fail closed on missing or conflicting labels."""
    keys = ["store_id", "business_date", "article_id"]
    _require(sales, {*SALES_OUTPUT_COLUMNS}, "normalized_sales")
    _require(day_clear, {*DAY_CLEAR_OUTPUT_COLUMNS}, "normalized_chdj_day_clear")
    if sales.duplicated(keys).any() or day_clear.duplicated(keys).any():
        raise ValueError("day_clear assembly inputs must be unique per store/date/article")
    result = sales.merge(day_clear, on=keys, how="left", validate="one_to_one", indicator=True)
    if result["_merge"].ne("both").any():
        raise ValueError("normalized sales is missing an authoritative chdj day_clear label")
    if result["source_sales_day_clear"].ne(result["day_clear"]).any():
        raise ValueError("sales source day_clear conflicts with authoritative chdj day_clear")
    return result.drop(columns="_merge")

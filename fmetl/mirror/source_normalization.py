from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise KeyError(f"{label} missing columns: {missing}")


def build_startup_openings(
    purchase: pd.DataFrame,
    article_ids: Sequence[str],
    *,
    start_day: str,
    store_id: str = "A3XV",
) -> pd.DataFrame:
    """Read startup quantity and amount from purchase_di without correction."""
    required = {
        "store_id", "business_date", "sale_article_id",
        "init_stock_qty", "init_stock_amt",
    }
    _require(purchase, required, "purchase_bootstrap")
    source = purchase[list(required)].copy()
    source = source.loc[
        source["store_id"].astype(str).eq(store_id)
        & source["business_date"].astype(str).eq(str(start_day))
    ]
    source["sale_article_id"] = source["sale_article_id"].astype(str)
    for column in ("init_stock_qty", "init_stock_amt"):
        source[column] = pd.to_numeric(source[column], errors="raise")
        present = source[column].notna()
        if not np.isfinite(source.loc[present, column].to_numpy(dtype=float)).all():
            raise ValueError(f"purchase_bootstrap.{column} must be finite when present")

    rows: list[dict[str, object]] = []
    for article_id in sorted(set(map(str, article_ids))):
        candidates = source.loc[source["sale_article_id"].eq(article_id)]
        normalized = candidates[["init_stock_qty", "init_stock_amt"]].copy()
        normalized["missing_qty"] = normalized["init_stock_qty"].isna()
        normalized["missing_cost"] = normalized["init_stock_amt"].isna()
        normalized["init_stock_qty"] = normalized["init_stock_qty"].fillna(0.0)
        normalized["init_stock_amt"] = normalized["init_stock_amt"].fillna(0.0)
        nonzero = normalized.loc[
            normalized["init_stock_qty"].abs().gt(0.000001)
            | normalized["init_stock_amt"].abs().gt(0.01)
        ].drop_duplicates()
        if len(nonzero) > 1:
            raise ValueError(f"conflicting purchase bootstrap tuples for {article_id}")
        if len(nonzero) == 1:
            qty = float(nonzero.iloc[0]["init_stock_qty"])
            amt = float(nonzero.iloc[0]["init_stock_amt"])
            if bool(nonzero.iloc[0]["missing_qty"]) and abs(amt) > 0.01:
                source_name = "PURCHASE_DI_AMOUNT_WITHOUT_QUANTITY"
                warning = f"AMOUNT_WITHOUT_OPENING_QUANTITY:{amt}"
            elif bool(nonzero.iloc[0]["missing_cost"]) and qty > 0.000001:
                source_name = "PURCHASE_DI_UNPRICED_OPENING"
                warning = f"POSITIVE_OPENING_WITHOUT_COST:{qty}"
            elif qty < -0.000001 or amt < -0.01:
                source_name = "PURCHASE_DI_NEGATIVE_OPENING"
                warning = f"NEGATIVE_SOURCE_OPENING_RETAINED:{qty}:{amt}"
            else:
                source_name = "PURCHASE_DI_BOOTSTRAP"
                warning = ""
        elif not candidates.empty:
            qty = amt = 0.0
            if candidates[["init_stock_qty", "init_stock_amt"]].isna().all(axis=None):
                source_name = "PURCHASE_DI_NULL_BOOTSTRAP_ZERO"
                warning = "NULL_SOURCE_OPENING_ASSUMED_ZERO"
            else:
                source_name = "PURCHASE_DI_BOOTSTRAP_ZERO"
                warning = ""
        else:
            qty = amt = 0.0
            source_name = "NEW_SKU_NO_BOOTSTRAP_ROW"
            warning = "MISSING_SOURCE_OPENING_ASSUMED_ZERO"
        rows.append({
            "store_id": store_id, "article_id": article_id,
            "opening_qty": qty, "opening_amt": amt,
            "opening_source": source_name, "opening_source_day": str(start_day),
            "opening_warning": warning,
            "opening_status": "ISSUE" if warning else "VALID",
        })
    return pd.DataFrame(rows)


def assemble_daily_activities(
    *,
    days: Sequence[str],
    article_ids: Sequence[str],
    sales: pd.DataFrame,
    losses: pd.DataFrame,
    counts: pd.DataFrame,
    day_clear: pd.DataFrame,
    receipts: pd.DataFrame,
    store_id: str = "A3XV",
) -> pd.DataFrame:
    """Create one normalized row for every store, date and SKU."""
    contracts = (
        (sales, {
            "store_id", "business_date", "article_id", "gross_sale_qty",
            "sale_return_qty", "net_sale_qty", "net_sale_amt",
        }, "sales"),
        (losses, {
            "store_id", "business_date", "article_id", "known_lost_qty",
        }, "losses"),
        (counts, {
            "store_id", "business_date", "article_id", "actual_stock_qty",
            "is_counted",
        }, "counts"),
        (day_clear, {
            "store_id", "business_date", "article_id", "day_clear",
        }, "day_clear"),
        (receipts, {
            "store_id", "business_date", "article_id", "receive_qty", "receive_amt",
        }, "receipts"),
    )
    for frame, columns, label in contracts:
        _require(frame, columns, label)

    days = [str(day) for day in days]
    articles = sorted(set(map(str, article_ids)))
    if not days or not articles:
        raise ValueError("dense activities require at least one day and article")
    keys = ["store_id", "business_date", "article_id"]
    result = pd.MultiIndex.from_product(
        [[store_id], days, articles], names=keys
    ).to_frame(index=False)

    def normalize_keys(frame: pd.DataFrame, label: str) -> pd.DataFrame:
        output = frame.copy()
        output[keys] = output[keys].astype(str)
        if output.duplicated(keys).any():
            raise ValueError(f"{label} must be unique per SKU-day before dense assembly")
        unexpected = sorted(set(output["store_id"]) - {store_id})
        if unexpected:
            raise ValueError(f"{label} contains stores outside {store_id}: {unexpected}")
        return output

    sale = normalize_keys(sales[list(contracts[0][1])], "sales")
    loss = normalize_keys(losses[list(contracts[1][1])], "losses")
    count = normalize_keys(counts[list(contracts[2][1])], "counts")
    labels = normalize_keys(day_clear[list(contracts[3][1])], "day_clear")
    receipt = receipts[list(contracts[4][1])].copy()
    receipt[keys] = receipt[keys].astype(str)
    receipt = receipt.groupby(keys, as_index=False).agg(
        store_receive_qty=("receive_qty", "sum"),
        store_receive_amt=("receive_amt", "sum"),
    )

    result = result.merge(labels, on=keys, how="left", validate="one_to_one")
    if result["day_clear"].isna().any():
        sample = result.loc[result["day_clear"].isna(), keys].head(20).to_dict("records")
        raise ValueError(f"activity universe is missing chdj day_clear labels: {sample}")
    for frame in (sale, loss, count, receipt):
        result = result.merge(frame, on=keys, how="left", validate="one_to_one")
    for column in (
        "gross_sale_qty", "sale_return_qty", "net_sale_qty", "net_sale_amt",
        "known_lost_qty", "store_receive_qty", "store_receive_amt",
    ):
        result[column] = pd.to_numeric(result[column], errors="raise").fillna(0.0)

    def count_flag(value: object) -> bool:
        if pd.isna(value):
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in (0, 1):
            return bool(value)
        raise ValueError(f"is_counted must be bool/0/1, got {value!r}")

    result["is_counted"] = result["is_counted"].map(count_flag)
    return result

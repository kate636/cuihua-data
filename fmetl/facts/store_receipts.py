from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


POSTING_COLUMNS = (
    "store_id", "business_date", "article_id", "source_parent_article_id",
    "external_event_group_id", "posting_role", "receive_qty", "receive_amt",
    "cost_source", "pool_effect",
)
RECONCILIATION_COLUMNS = (
    "store_id", "business_date", "parent_article_id", "child_count",
    "parent_receive_qty", "parent_receive_amt", "allocated_child_qty",
    "allocated_child_amt", "amount_residual",
)
QUARANTINE_COLUMNS = (
    "store_id", "business_date", "article_id", "sale_article_id",
    "sale_article_qty", "sale_article_purchase_amt", "reason",
)


@dataclass(frozen=True)
class StoreReceiptBuild:
    postings: pd.DataFrame
    reconciliation: pd.DataFrame
    quarantined: pd.DataFrame


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise KeyError(f"{label} missing columns: {missing}")


def _numeric_finite(frame: pd.DataFrame, columns: tuple[str, ...], label: str) -> None:
    for column in columns:
        frame[column] = pd.to_numeric(frame[column], errors="raise").fillna(0.0)
        if not np.isfinite(frame[column].to_numpy(dtype=float)).all():
            raise ValueError(f"{label}.{column} must be finite")


def _validate_signed_receipt(
    frame: pd.DataFrame,
    qty_column: str,
    amt_column: str,
    label: str,
) -> None:
    """Validate a signed net receipt without erasing purchase returns.

    The Hive receipt facts use negative quantity and amount for a purchase
    return/correction.  Quantity and amount therefore have to point in the
    same direction; zero-cost gifts remain valid.
    """
    _numeric_finite(frame, (qty_column, amt_column), label)
    qty = frame[qty_column]
    amt = frame[amt_column]
    sign_mismatch = (
        qty.abs().gt(0.000001)
        & amt.abs().gt(0.01)
        & np.sign(qty).ne(np.sign(amt))
    )
    if sign_mismatch.any():
        raise ValueError(
            f"{label} quantity and amount signs must match: "
            f"{frame.loc[sign_mismatch, [qty_column, amt_column]].head(10).to_dict('records')}"
        )


def _validate_keys(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    if frame[columns].isna().any().any():
        raise ValueError(f"{label} keys cannot contain NULL")
    if (
        frame[columns].astype(str)
        .apply(lambda column: column.str.strip().str.lower().isin({"", "nan", "none", "null"}))
        .any().any()
    ):
        raise ValueError(f"{label} keys cannot be blank")


def build_store_receipts(
    purchase: pd.DataFrame,
    receive_sale: pd.DataFrame,
    *,
    store_id: str = "A3XV",
) -> StoreReceiptBuild:
    """Post one actual A receipt from receive_sale and audit purchase allocation only."""
    purchase_required = {
        "store_id", "business_date", "article_id", "sale_article_id",
        "day_clear", "sale_article_qty", "sale_article_purchase_amt",
    }
    receive_required = {
        "store_id", "inc_day", "article_id", "sale_article_id",
        "inbound_qty", "inbound_amount",
    }
    _require(purchase, purchase_required, "purchase")
    _require(receive_sale, receive_required, "receive_sale")

    direction = purchase[list(purchase_required)].copy()
    _validate_keys(
        direction,
        ["store_id", "business_date", "article_id", "sale_article_id", "day_clear"],
        "purchase",
    )
    direction[["store_id", "business_date", "article_id", "sale_article_id", "day_clear"]] = direction[
        ["store_id", "business_date", "article_id", "sale_article_id", "day_clear"]
    ].astype(str)
    _numeric_finite(
        direction, ("sale_article_qty", "sale_article_purchase_amt"), "purchase"
    )

    bridge = receive_sale[list(receive_required)].copy()
    _validate_keys(
        bridge, ["store_id", "inc_day", "article_id", "sale_article_id"],
        "receive_sale",
    )
    bridge[["store_id", "inc_day", "article_id", "sale_article_id"]] = bridge[
        ["store_id", "inc_day", "article_id", "sale_article_id"]
    ].astype(str)
    bridge = bridge.loc[bridge["store_id"].eq(store_id)].copy()
    _validate_signed_receipt(
        bridge, "inbound_qty", "inbound_amount", "receive_sale"
    )
    keys = ["store_id", "inc_day", "article_id"]
    consistency = bridge.groupby(keys, dropna=False).agg(
        qty_min=("inbound_qty", "min"), qty_max=("inbound_qty", "max"),
        amt_min=("inbound_amount", "min"), amt_max=("inbound_amount", "max"),
        child_count=("sale_article_id", "nunique"),
    ).reset_index()
    inconsistent = consistency.loc[
        consistency["qty_max"].sub(consistency["qty_min"]).abs().gt(0.000001)
        | consistency["amt_max"].sub(consistency["amt_min"]).abs().gt(0.01)
    ]
    if not inconsistent.empty:
        raise ValueError(
            "receive_sale repeats different A receipt quantity or amount: "
            f"{inconsistent[keys].head(20).to_dict('records')}"
        )

    actual = consistency.rename(columns={
        "inc_day": "business_date", "qty_max": "receive_qty",
        "amt_max": "receive_amt", "article_id": "source_parent_article_id",
    })
    actual["article_id"] = actual["source_parent_article_id"]
    actual["external_event_group_id"] = (
        "RECEIVE_SALE|" + actual["store_id"] + "|" + actual["business_date"]
        + "|" + actual["article_id"]
    )
    actual["posting_role"] = "ACTUAL_RECEIPT_ON_SOURCE_ARTICLE"
    actual["cost_source"] = "RECEIVE_SALE_A_DEDUP"
    actual["pool_effect"] = "EXTERNAL_NET"
    postings = actual[list(POSTING_COLUMNS)].copy()

    allocation = direction.groupby(
        ["store_id", "business_date", "article_id"], dropna=False
    ).agg(
        child_count=("sale_article_id", "nunique"),
        allocated_child_qty=("sale_article_qty", "sum"),
        allocated_child_amt=("sale_article_purchase_amt", "sum"),
    ).reset_index().rename(columns={"article_id": "parent_article_id"})
    parent_values = postings.rename(columns={
        "article_id": "parent_article_id", "receive_qty": "parent_receive_qty",
        "receive_amt": "parent_receive_amt",
    })
    reconciliation = allocation.merge(
        parent_values[[
            "store_id", "business_date", "parent_article_id",
            "parent_receive_qty", "parent_receive_amt",
        ]],
        on=["store_id", "business_date", "parent_article_id"],
        how="left", validate="one_to_one",
    )
    reconciliation["amount_residual"] = (
        reconciliation["allocated_child_amt"]
        - reconciliation["parent_receive_amt"].fillna(0.0)
    )
    reconciliation = reconciliation[list(RECONCILIATION_COLUMNS)]

    purchase_active = allocation.loc[
        allocation["allocated_child_qty"].abs().gt(0.000001)
        | allocation["allocated_child_amt"].abs().gt(0.01)
    ]
    missing_receive = purchase_active.merge(
        postings[["store_id", "business_date", "article_id"]].rename(
            columns={"article_id": "parent_article_id"}
        ).assign(_receive_sale_present=True),
        on=["store_id", "business_date", "parent_article_id"],
        how="left", validate="one_to_one",
    )
    missing_receive = missing_receive.loc[
        missing_receive["_receive_sale_present"].isna()
    ]
    quarantined = pd.DataFrame(columns=QUARANTINE_COLUMNS)
    if not missing_receive.empty:
        quarantined = missing_receive.rename(columns={
            "parent_article_id": "article_id",
            "allocated_child_qty": "sale_article_qty",
            "allocated_child_amt": "sale_article_purchase_amt",
        })
        quarantined["sale_article_id"] = ""
        quarantined["reason"] = "PURCHASE_RECEIPT_MISSING_RECEIVE_SALE"
        quarantined = quarantined[list(QUARANTINE_COLUMNS)]

    return StoreReceiptBuild(
        postings=postings.sort_values(
            ["store_id", "business_date", "article_id"], kind="stable"
        ).reset_index(drop=True),
        reconciliation=reconciliation.sort_values(
            ["store_id", "business_date", "parent_article_id"], kind="stable"
        ).reset_index(drop=True),
        quarantined=quarantined.reset_index(drop=True),
    )

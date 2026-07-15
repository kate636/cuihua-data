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


@dataclass(frozen=True)
class StoreReceiptBuild:
    postings: pd.DataFrame
    reconciliation: pd.DataFrame


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise KeyError(f"{label} missing columns: {missing}")


def _numeric_nonnegative(frame: pd.DataFrame, columns: tuple[str, ...], label: str) -> None:
    for column in columns:
        frame[column] = pd.to_numeric(frame[column], errors="raise").fillna(0.0)
        if not np.isfinite(frame[column].to_numpy(dtype=float)).all():
            raise ValueError(f"{label}.{column} must be finite")
        if frame[column].lt(-0.000001).any():
            raise ValueError(f"{label}.{column} cannot be negative")


def _validate_keys(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    if frame[columns].isna().any().any():
        raise ValueError(f"{label} keys cannot contain NULL")
    if (
        frame[columns].astype(str)
        .apply(lambda column: column.str.strip().str.lower().isin({"", "nan", "none", "null"}))
        .any().any()
    ):
        raise ValueError(f"{label} keys cannot be blank")


def _parent_bridge(
    receive_sale: pd.DataFrame,
    expected_keys: set[tuple[str, str, str]],
) -> pd.DataFrame:
    required = {"store_id", "inc_day", "article_id", "sale_article_id", "inbound_qty", "inbound_amount"}
    _require(receive_sale, required, "receive_sale")
    bridge = receive_sale[list(required)].copy()
    _validate_keys(
        bridge, ["store_id", "inc_day", "article_id", "sale_article_id"], "receive_sale"
    )
    bridge[["store_id", "inc_day", "article_id", "sale_article_id"]] = bridge[
        ["store_id", "inc_day", "article_id", "sale_article_id"]
    ].astype(str)
    row_keys = pd.Series(
        list(zip(bridge["store_id"], bridge["inc_day"], bridge["article_id"])),
        index=bridge.index,
    )
    bridge = bridge.loc[row_keys.isin(expected_keys)].copy()
    if bridge.empty and expected_keys:
        raise ValueError(f"parent reconstruction missing receive_sale rows: {sorted(expected_keys)}")
    _numeric_nonnegative(bridge, ("inbound_qty", "inbound_amount"), "receive_sale")

    keys = ["store_id", "inc_day", "article_id"]
    consistency = bridge.groupby(keys, dropna=False).agg(
        qty_min=("inbound_qty", "min"), qty_max=("inbound_qty", "max"),
        amt_min=("inbound_amount", "min"), amt_max=("inbound_amount", "max"),
    ).reset_index()
    bad = consistency.loc[
        consistency["qty_max"].sub(consistency["qty_min"]).abs().gt(0.000001)
        | consistency["amt_max"].sub(consistency["amt_min"]).abs().gt(0.000001)
    ]
    if not bad.empty:
        raise ValueError(
            "receive_sale repeats inconsistent parent receipt values: "
            f"{bad[keys].head(10).to_dict(orient='records')}"
        )
    present = set(zip(consistency["store_id"], consistency["inc_day"], consistency["article_id"]))
    missing = sorted(expected_keys - present)
    if missing:
        raise ValueError(f"parent reconstruction missing receive_sale daily parents: {missing}")
    return consistency


def build_store_receipts(
    purchase: pd.DataFrame,
    receive_sale: pd.DataFrame,
    *,
    parent_reconstruction_keys: set[tuple[str, str, str]] | None = None,
    store_id: str = "A3XV",
) -> StoreReceiptBuild:
    """Build exactly-once external store receipts; SCM fields are deliberately absent."""
    required = {
        "store_id", "business_date", "article_id", "sale_article_id",
        "day_clear", "sale_article_qty", "sale_article_purchase_amt",
    }
    _require(purchase, required, "purchase")
    reconstruction_keys = {
        (str(store), str(day), str(parent))
        for store, day, parent in (parent_reconstruction_keys or set())
    }
    source = purchase[list(required)].copy()
    _validate_keys(
        source,
        ["store_id", "business_date", "article_id", "sale_article_id", "day_clear"],
        "purchase",
    )
    source[["store_id", "business_date", "article_id", "sale_article_id", "day_clear"]] = source[
        ["store_id", "business_date", "article_id", "sale_article_id", "day_clear"]
    ].astype(str)
    unexpected = sorted(set(source["store_id"]) - {store_id})
    if unexpected:
        raise ValueError(f"purchase contains stores outside {store_id}: {unexpected}")
    grain = ["store_id", "business_date", "article_id", "sale_article_id", "day_clear"]
    if source.duplicated(grain).any():
        raise ValueError(f"purchase grain is not unique: {grain}")
    _numeric_nonnegative(source, ("sale_article_qty", "sale_article_purchase_amt"), "purchase")
    active = source.loc[
        source["sale_article_qty"].abs().gt(0.000001)
        | source["sale_article_purchase_amt"].abs().gt(0.000001)
    ].copy()

    active_keys = pd.Series(
        list(zip(active["store_id"], active["business_date"], active["article_id"])),
        index=active.index,
    )
    reconstruct_mask = active_keys.isin(reconstruction_keys)
    direct = active.loc[~reconstruct_mask].copy()
    direct["source_parent_article_id"] = direct["article_id"]
    direct["article_id"] = direct["sale_article_id"]
    direct["external_event_group_id"] = (
        "PURCHASE|" + direct["store_id"] + "|" + direct["business_date"] + "|"
        + direct["source_parent_article_id"] + "|" + direct["article_id"] + "|" + direct["day_clear"]
    )
    direct["posting_role"] = "DIRECT_SALE_SKU_RECEIPT"
    direct["receive_qty"] = direct["sale_article_qty"]
    direct["receive_amt"] = direct["sale_article_purchase_amt"]
    direct["cost_source"] = "PURCHASE_DI_ALLOCATED_RECEIPT"
    direct["pool_effect"] = "EXTERNAL_IN"

    parent_source = active.loc[reconstruct_mask]
    expected_parent_keys = set(zip(
        parent_source["store_id"], parent_source["business_date"], parent_source["article_id"]
    ))
    bridge = _parent_bridge(receive_sale, expected_parent_keys)
    reconstructed = pd.DataFrame(columns=POSTING_COLUMNS)
    if not bridge.empty:
        reconstructed = pd.DataFrame({
            "store_id": bridge["store_id"],
            "business_date": bridge["inc_day"],
            "article_id": bridge["article_id"],
            "source_parent_article_id": bridge["article_id"],
            "external_event_group_id": (
                "RECEIVE_SALE_PARENT|" + bridge["store_id"] + "|" + bridge["inc_day"]
                + "|" + bridge["article_id"]
            ),
            "posting_role": "PARENT_RECEIPT_FOR_RECONSTRUCTION",
            "receive_qty": bridge["qty_max"],
            "receive_amt": bridge["amt_max"],
            "cost_source": "RECEIVE_SALE_PARENT_DEDUP",
            "pool_effect": "EXTERNAL_IN",
        })

    postings = pd.concat([direct[list(POSTING_COLUMNS)], reconstructed], ignore_index=True)
    if postings["external_event_group_id"].duplicated().any():
        raise ValueError("external receipt event would be posted more than once")

    reconciliation = pd.DataFrame(columns=RECONCILIATION_COLUMNS)
    allocated = active.loc[reconstruct_mask].copy()
    if not allocated.empty:
        allocated = allocated.groupby(
            ["store_id", "business_date", "article_id"], dropna=False
        ).agg(
            child_count=("sale_article_id", "nunique"),
            allocated_child_qty=("sale_article_qty", "sum"),
            allocated_child_amt=("sale_article_purchase_amt", "sum"),
        ).reset_index().rename(columns={"article_id": "parent_article_id"})
        parent_values = reconstructed.rename(columns={
            "article_id": "parent_article_id", "receive_qty": "parent_receive_qty",
            "receive_amt": "parent_receive_amt",
        })
        reconciliation = allocated.merge(
            parent_values[["store_id", "business_date", "parent_article_id", "parent_receive_qty", "parent_receive_amt"]],
            on=["store_id", "business_date", "parent_article_id"], how="left", validate="one_to_one",
        )
        reconciliation["amount_residual"] = (
            reconciliation["allocated_child_amt"] - reconciliation["parent_receive_amt"]
        )
        reconciliation = reconciliation[list(RECONCILIATION_COLUMNS)]

    return StoreReceiptBuild(
        postings=postings.sort_values(
            ["store_id", "business_date", "article_id", "external_event_group_id"], kind="stable"
        ).reset_index(drop=True),
        reconciliation=reconciliation.sort_values(
            ["store_id", "business_date", "parent_article_id"], kind="stable"
        ).reset_index(drop=True),
    )

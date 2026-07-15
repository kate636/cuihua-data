from __future__ import annotations

import pandas as pd


def build_first_orders(trade_user: pd.DataFrame, existing: pd.DataFrame | None = None) -> pd.DataFrame:
    required = {"thirdparty_user_identity", "trade_time"}
    missing = sorted(required - set(trade_user.columns))
    if missing:
        raise KeyError(f"trade_user missing columns: {missing}")
    observed = trade_user[["thirdparty_user_identity", "trade_time"]].copy()
    observed["first_order_date"] = pd.to_datetime(observed["trade_time"], errors="raise").dt.date
    observed = (
        observed.dropna(subset=["thirdparty_user_identity"])
        .groupby("thirdparty_user_identity", as_index=False)["first_order_date"].min()
    )
    if existing is None or existing.empty:
        return observed
    required_existing = {"thirdparty_user_identity", "first_order_date"}
    missing_existing = sorted(required_existing - set(existing.columns))
    if missing_existing:
        raise KeyError(f"existing first orders missing columns: {missing_existing}")
    old = existing[list(required_existing)].copy()
    old["first_order_date"] = pd.to_datetime(old["first_order_date"], errors="raise").dt.date
    return (
        pd.concat([old, observed], ignore_index=True)
        .groupby("thirdparty_user_identity", as_index=False)["first_order_date"].min()
    )


def classify_weekly_customer(
    first_order_date: object,
    week_start_date: object,
    business_date: object,
) -> str:
    if pd.isna(first_order_date) or pd.isna(week_start_date):
        return "其他"
    first = pd.Timestamp(first_order_date).date()
    week_start = pd.Timestamp(week_start_date).date()
    business = pd.Timestamp(business_date).date()
    if first > business:
        raise ValueError(f"first_order_date {first} is later than business_date {business}")
    return "老客" if first < week_start else "新客"


def _count_distinct_completed(group: pd.DataFrame) -> int:
    return int(group.loc[group["order_status"].eq("os.completed"), "canonical_order_key"].nunique())


def aggregate_customer_metrics(events: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    """Aggregate signed order events; customer counts mean completed order visits."""
    required = {
        *group_columns, "order_status", "canonical_order_key", "source_channel",
        "jielong_flag", "sales_amt", "qty", "customer_type",
    }
    missing = sorted(required - set(events.columns))
    if missing:
        raise KeyError(f"customer events missing columns: {missing}")

    rows: list[dict[str, object]] = []
    grouper: object = group_columns[0] if len(group_columns) == 1 else group_columns
    for key, group in events.groupby(grouper, dropna=False, sort=False):
        values = (key,) if len(group_columns) == 1 else key
        completed = group["order_status"].eq("os.completed")
        online = group["source_channel"].eq("online")
        jielong = group["jielong_flag"].fillna("-").ne("-")
        row = dict(zip(group_columns, values))
        row.update(
            {
                "cust_num": _count_distinct_completed(group),
                "online_order_num": int(group.loc[completed & online, "canonical_order_key"].nunique()),
                "offline_order_num": int(group.loc[completed & ~online, "canonical_order_key"].nunique()),
                "jielong_order_num": int(group.loc[completed & jielong, "canonical_order_key"].nunique()),
                "jsd_order_num": int(group.loc[completed & online & ~jielong, "canonical_order_key"].nunique()),
                "signed_sale_amt": float(group["sales_amt"].sum()),
                "signed_qty": float(group["qty"].sum()),
                "online_sale_amt": float(group.loc[online, "sales_amt"].sum()),
                "offline_sale_amt": float(group.loc[~online, "sales_amt"].sum()),
                "jielong_sale_amt": float(group.loc[jielong, "sales_amt"].sum()),
                "jsd_sale_amt": float(group.loc[online & ~jielong, "sales_amt"].sum()),
            }
        )
        for customer_type, prefix in (("新客", "new"), ("老客", "old"), ("其他", "other")):
            customer_rows = group["customer_type"].eq(customer_type)
            row[f"{prefix}_cust_num"] = int(
                group.loc[completed & customer_rows, "canonical_order_key"].nunique()
            )
            row[f"{prefix}_sale_amt"] = float(group.loc[customer_rows, "sales_amt"].sum())
            row[f"{prefix}_qty"] = float(group.loc[customer_rows, "qty"].sum())
        rows.append(row)
    return pd.DataFrame(rows)

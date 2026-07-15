from __future__ import annotations

import pandas as pd


def valid_business_days(store_daily: pd.DataFrame, threshold: float = 500.0) -> pd.DataFrame:
    required = {"store_id", "inc_day", "bf19_sale_amt"}
    missing = sorted(required - set(store_daily.columns))
    if missing:
        raise KeyError(f"store_daily missing columns: {missing}")
    out = store_daily.loc[
        (store_daily["store_id"].astype(str) == "A3XV")
        & (pd.to_numeric(store_daily["bf19_sale_amt"], errors="coerce") >= threshold),
        ["store_id", "inc_day", "bf19_sale_amt"],
    ].copy()
    out = out.rename(columns={"inc_day": "business_date"})
    return out.drop_duplicates(["store_id", "business_date"])

from __future__ import annotations

import pandas as pd


class GrainViolation(ValueError):
    pass


def assert_only_store(df: pd.DataFrame, store_id: str = "A3XV") -> None:
    if "store_id" not in df.columns or df.empty:
        return
    if df["store_id"].isna().any():
        raise GrainViolation("store_id contains NULL")
    unexpected = sorted(set(df["store_id"].dropna().astype(str)) - {store_id})
    if unexpected:
        raise GrainViolation(f"unexpected stores: {unexpected}")


def assert_unique(df: pd.DataFrame, keys: list[str], label: str) -> None:
    missing = [key for key in keys if key not in df.columns]
    if missing:
        raise GrainViolation(f"{label}: missing key columns {missing}")
    duplicated = df.duplicated(keys, keep=False)
    if duplicated.any():
        sample = df.loc[duplicated, keys].head(10).to_dict("records")
        raise GrainViolation(f"{label}: duplicate grain {keys}; sample={sample}")

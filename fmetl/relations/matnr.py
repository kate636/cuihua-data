from __future__ import annotations

import pandas as pd

from fmetl.relations.snapshots import Snapshot, build_snapshot


def build_matnr_member_snapshot(goods: pd.DataFrame) -> Snapshot:
    """Freeze matnr as a no-posting identity membership, never as an inventory flow."""
    required = {
        "inc_day", "article_id", "matnr", "sale_unit", "matnr_unit", "order_unit",
        "unit_weight", "atob_value", "zglfz", "zglfm", "category_level1_id",
        "category_level2_id", "category_level3_id", "report_category_code",
    }
    missing = sorted(required - set(goods.columns))
    if missing:
        raise KeyError(f"matnr member source missing columns: {missing}")
    frame = goods[list(required)].copy()
    identity_keys = ["inc_day", "article_id", "report_category_code"]
    if frame[identity_keys].isna().any().any():
        raise ValueError("matnr identity keys require a v1.5-mapped report category")
    frame[identity_keys] = frame[identity_keys].astype(str)
    if (
        frame[identity_keys]
        .apply(lambda column: column.str.strip().str.lower().isin({"", "nan", "none", "null"}))
        .any().any()
    ):
        raise ValueError("matnr identity keys cannot be blank")
    frame["matnr"] = frame["matnr"].fillna("").astype(str).str.strip()
    frame = frame.loc[frame["matnr"].ne("")].copy()
    if frame.duplicated(["inc_day", "article_id"]).any():
        raise ValueError("matnr member source must be unique per snapshot day/article")
    frame["relation_id"] = (
        "MATNR|" + frame["matnr"] + "|" + frame["report_category_code"]
    )
    frame["relation_kind"] = "MATNR_IDENTITY"
    frame["member_role"] = "MEMBER"
    frame["posting_policy"] = "NO_POSTING"
    frame["formal_flow_allowed"] = False
    return build_snapshot(
        frame,
        keys=["inc_day", "matnr", "report_category_code", "article_id"],
        namespace="matnr_member",
    )

from __future__ import annotations

from dataclasses import dataclass
import hashlib

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SpecialLossCoverage:
    activities: pd.DataFrame
    audit: pd.DataFrame


def build_wastage_trace(wastage: pd.DataFrame) -> pd.DataFrame:
    """Freeze latest active v1.5 CCJ/SSLS rows with a lossless technical id."""
    required = {"inc_day", "sku_code", "created_at", "reason", "waste_money", "waste_num", "is_deleted"}
    missing = sorted(required - set(wastage.columns))
    if missing:
        raise KeyError(f"wastage frame missing columns: {missing}")
    is_active = pd.to_numeric(wastage["is_deleted"], errors="coerce").fillna(
        wastage["is_deleted"].astype(str).str.lower().map({"false": 0, "true": 1})
    ).eq(0)
    active = wastage.loc[is_active].copy()
    if active.empty:
        source = active
    else:
        latest_snapshot = active["inc_day"].astype(str).max()
        source = active.loc[active["inc_day"].astype(str).eq(latest_snapshot)].copy()
    source = source.loc[source["reason"].isin({"炒菜机成本", "生熟联动"})].copy()
    source["business_date"] = pd.to_datetime(source["created_at"], errors="raise").dt.date.astype(str)
    source["article_id"] = source["sku_code"].astype(str)
    source["waste_money"] = pd.to_numeric(source["waste_money"], errors="raise")
    source["waste_num"] = pd.to_numeric(source["waste_num"], errors="raise")
    if (
        not np.isfinite(source[["waste_money", "waste_num"]].to_numpy(dtype=float)).all()
        or source["waste_num"].lt(0).any()
    ):
        raise ValueError("special wastage quantity must be finite and nonnegative; amount must be finite")
    source["store_id"] = "A3XV"
    source["reason_code"] = source["reason"].map({"炒菜机成本": "ccj", "生熟联动": "ssls"})
    hash_columns = sorted(wastage.columns)
    if source.empty:
        source["source_row_hash"] = pd.Series(dtype=str)
        source["duplicate_ordinal"] = pd.Series(dtype=int)
        source["source_record_id"] = pd.Series(dtype=str)
        return source

    def row_hash(row: pd.Series) -> str:
        payload = "\x1f".join(
            "<NULL>" if pd.isna(row.get(column)) else str(row.get(column))
            for column in hash_columns
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    source["source_row_hash"] = source.apply(row_hash, axis=1)
    source["duplicate_ordinal"] = source.groupby("source_row_hash", sort=False).cumcount() + 1
    source["source_record_id"] = (
        source["source_row_hash"] + ":" + source["duplicate_ordinal"].astype(str)
    )
    return source


def merge_special_loss_quantity(
    activities: pd.DataFrame,
    wastage_trace: pd.DataFrame,
) -> SpecialLossCoverage:
    """Merge general and special loss totals without adding both totals in full.

    The general loss source has no reason or record id, so row-level overlap
    cannot be proved.  The declared rule assumes its quantity covers special
    loss first, then adds only the special quantity above that total.  The
    audit output states this unproved overlap assumption explicitly.
    """
    keys = ["store_id", "business_date", "article_id"]
    required_activity = {*keys, "known_lost_qty"}
    missing = sorted(required_activity - set(activities.columns))
    if missing:
        raise KeyError(f"activities missing special-loss columns: {missing}")
    if activities.duplicated(keys).any():
        raise ValueError("activities must be unique per SKU-day before special-loss merge")
    output = activities.copy()
    output["known_lost_qty"] = pd.to_numeric(output["known_lost_qty"], errors="raise")
    if wastage_trace.empty:
        output["known_lost_qty_before_special"] = output["known_lost_qty"]
        output["special_loss_supplement_qty"] = 0.0
        return SpecialLossCoverage(output, pd.DataFrame(columns=[
            *keys, "general_known_lost_qty", "ccj_qty", "ssls_qty",
            "special_loss_qty", "covered_by_general_loss_qty",
            "supplemented_from_special_source_qty", "effective_known_lost_qty",
            "coverage_rule", "overlap_evidence_status",
        ]))
    required_wastage = {*keys, "reason_code", "waste_num"}
    missing = sorted(required_wastage - set(wastage_trace.columns))
    if missing:
        raise KeyError(f"wastage trace missing special-loss columns: {missing}")
    special = wastage_trace.loc[
        wastage_trace["reason_code"].isin({"ccj", "ssls"}),
        [*keys, "reason_code", "waste_num"],
    ].copy()
    special["waste_num"] = pd.to_numeric(special["waste_num"], errors="raise")
    special = special.pivot_table(
        index=keys,
        columns="reason_code",
        values="waste_num",
        aggfunc="sum",
        fill_value=0.0,
    ).reset_index()
    special = special.rename(columns={"ccj": "ccj_qty", "ssls": "ssls_qty"})
    for column in ("ccj_qty", "ssls_qty"):
        if column not in special:
            special[column] = 0.0
    general = output[keys + ["known_lost_qty"]].rename(
        columns={"known_lost_qty": "general_known_lost_qty"}
    )
    audit = special.merge(general, on=keys, how="left", validate="one_to_one")
    if audit["general_known_lost_qty"].isna().any():
        sample = audit.loc[audit["general_known_lost_qty"].isna(), keys].head(20)
        raise ValueError(
            "special-loss SKU-day is outside the activity ledger: "
            f"{sample.to_dict('records')}"
        )
    audit["special_loss_qty"] = audit["ccj_qty"] + audit["ssls_qty"]
    audit["covered_by_general_loss_qty"] = audit[[
        "general_known_lost_qty", "special_loss_qty",
    ]].min(axis=1)
    audit["supplemented_from_special_source_qty"] = (
        audit["special_loss_qty"] - audit["general_known_lost_qty"]
    ).clip(lower=0.0)
    audit["effective_known_lost_qty"] = (
        audit["general_known_lost_qty"]
        + audit["supplemented_from_special_source_qty"]
    )
    audit["coverage_rule"] = np.where(
        audit["supplemented_from_special_source_qty"].gt(0.000001),
        "GENERAL_LOSS_PLUS_UNCOVERED_SPECIAL_QUANTITY",
        "GENERAL_LOSS_AGGREGATE_COVERS_SPECIAL_QUANTITY",
    )
    audit["overlap_evidence_status"] = "OVERLAP_ASSUMPTION_UNPROVEN"
    supplement = audit[keys + ["supplemented_from_special_source_qty"]]
    output = output.merge(supplement, on=keys, how="left", validate="one_to_one")
    output["known_lost_qty_before_special"] = output["known_lost_qty"]
    output["special_loss_supplement_qty"] = output[
        "supplemented_from_special_source_qty"
    ].fillna(0.0)
    output["known_lost_qty"] += output["special_loss_supplement_qty"]
    output = output.drop(columns="supplemented_from_special_source_qty")
    return SpecialLossCoverage(output, audit)

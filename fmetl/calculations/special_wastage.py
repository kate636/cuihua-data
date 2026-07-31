from __future__ import annotations

import hashlib

import pandas as pd


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


def adjust_sku_wastage(accounting: pd.DataFrame, wastage: pd.DataFrame) -> pd.DataFrame:
    """Apply v1.5 CCJ/SSLS display adjustments while retaining accounting values."""
    required_accounting = {
        "store_id", "business_date", "article_id", "day_clear",
        "accounting_lost_amt", "accounting_lost_qty", "accounting_known_lost_amt",
        "accounting_profit", "accounting_full_profit",
    }
    missing = sorted(required_accounting - set(accounting.columns))
    if missing:
        raise KeyError(f"accounting frame missing columns: {missing}")
    if set(accounting["store_id"].dropna().astype(str)) - {"A3XV"}:
        raise ValueError("special wastage is scoped to A3XV")
    if accounting.duplicated(["store_id", "business_date", "article_id"]).any():
        raise ValueError("special wastage input must have one row per store/date/article")
    source = build_wastage_trace(wastage)
    if source.empty:
        wide = pd.DataFrame(columns=[
            "store_id", "business_date", "article_id", "ccj_amt", "ccj_qty", "ssls_amt", "ssls_qty",
        ])
    else:
        wide = source.pivot_table(
            index=["store_id", "business_date", "article_id"],
            columns="reason_code",
            values=["waste_money", "waste_num"],
            aggfunc="sum",
            fill_value=0.0,
        )
        wide.columns = [
            f"{reason}_{'amt' if metric == 'waste_money' else 'qty'}"
            for metric, reason in wide.columns
        ]
        wide = wide.reset_index()
    out = accounting.copy()
    out["business_date"] = out["business_date"].astype(str)
    out["article_id"] = out["article_id"].astype(str)
    out = out.merge(wide, on=["store_id", "business_date", "article_id"], how="left", validate="many_to_one")
    for column in ("ccj_amt", "ccj_qty", "ssls_amt", "ssls_qty"):
        if column not in out:
            out[column] = 0.0
        out[column] = out[column].fillna(0.0)
    out["adjusted_lost_amt"] = out["accounting_lost_amt"] - out["ccj_amt"] - out["ssls_amt"]
    out["adjusted_lost_qty"] = out["accounting_lost_qty"] - out["ccj_qty"] - out["ssls_qty"]
    out["adjusted_known_lost_amt"] = (
        out["accounting_known_lost_amt"] - out["ccj_amt"] - out["ssls_amt"]
    )
    out["adjusted_profit_before_ssls"] = out["accounting_profit"] + out["ccj_amt"]
    out["adjusted_full_profit_before_ssls"] = out["accounting_full_profit"] + out["ccj_amt"]
    return out


def apply_ssls_category_transfer(levels: pd.DataFrame) -> pd.DataFrame:
    """Reproduce v1.5 SSLS transfer: source credit and date-level cooked debit."""
    required = {
        "store_id", "business_date", "day_clear", "report_category_name",
        "level_description", "adjusted_profit_before_ssls",
        "adjusted_full_profit_before_ssls", "ssls_amt",
    }
    missing = sorted(required - set(levels.columns))
    if missing:
        raise KeyError(f"category levels missing columns: {missing}")
    if not levels["level_description"].eq("大分类").all():
        raise ValueError("SSLS transfer accepts only 大分类 rows")
    row_key = ["store_id", "business_date", "day_clear", "report_category_name"]
    if levels.duplicated(row_key).any():
        raise ValueError("SSLS transfer requires one row per store/date/day_clear/category")
    # Exact v1.5 semantics: total_ssls is grouped only by business_date. A3XV
    # is the only store, but the same total is intentionally reused for each
    # day_clear aggregation to preserve compatibility.
    total_key = ["store_id", "business_date"]
    totals = levels.groupby(total_key, as_index=False)["ssls_amt"].sum().rename(
        columns={"ssls_amt": "total_ssls_amt"}
    )
    out = levels.merge(totals, on=total_key, how="left", validate="many_to_one")
    source_credit = out["ssls_amt"]
    cooked_debit = out["total_ssls_amt"].where(out["report_category_name"].eq("熟食类"), 0.0)
    out["adjusted_profit"] = out["adjusted_profit_before_ssls"] + source_credit - cooked_debit
    out["adjusted_full_profit"] = (
        out["adjusted_full_profit_before_ssls"] + source_credit - cooked_debit
    )
    out["ssls_transfer_delta"] = source_credit - cooked_debit
    return out

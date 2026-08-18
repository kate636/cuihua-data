from __future__ import annotations

import pandas as pd


ADJUSTMENT_COLUMNS = (
    "store_no", "business_date", "day_clear",
    "category_level1_description", "adjustment_direction",
    "adjustment_amt", "reason_code", "calculation_rule",
)


def build_ssls_category_adjustments(sku_daily: pd.DataFrame) -> pd.DataFrame:
    """Move the ledger cost of SSLS raw quantity to 熟食 without changing store total."""
    required = {
        "store_no", "business_date", "day_clear",
        "category_level1_description", "ssls_ledger_cost_amt",
    }
    missing = sorted(required - set(sku_daily.columns))
    if missing:
        raise KeyError(f"SSLS category adjustment input missing columns: {missing}")
    source = sku_daily[list(required)].copy()
    source["ssls_transfer_cost_amt"] = pd.to_numeric(
        source["ssls_ledger_cost_amt"], errors="raise"
    )
    frames: list[pd.DataFrame] = []
    for day_clear in ("0", "1", "2"):
        part = source if day_clear == "2" else source.loc[source["day_clear"].astype(str).eq(day_clear)]
        grouped = part.groupby(
            ["store_no", "business_date", "category_level1_description"],
            as_index=False,
        )["ssls_transfer_cost_amt"].sum()
        grouped = grouped.loc[
            grouped["ssls_transfer_cost_amt"].abs().gt(0.01)
        ].copy()
        if grouped.empty:
            continue
        out = grouped.rename(columns={"ssls_transfer_cost_amt": "adjustment_amt"})
        out["day_clear"] = day_clear
        out["adjustment_direction"] = "RAW_CATEGORY_CREDIT"
        out["reason_code"] = "SSLS_CATEGORY_TRANSFER"
        out["calculation_rule"] = "熟食联动原料数量 × 原料账本当日单位成本；该金额从原料大分类毛利中加回"
        total = out.groupby(
            ["store_no", "business_date", "day_clear"], as_index=False
        )["adjustment_amt"].sum()
        target = total.copy()
        target["category_level1_description"] = "熟食类"
        target["adjustment_amt"] = -target["adjustment_amt"]
        target["adjustment_direction"] = "COOKED_CATEGORY_DEBIT"
        target["reason_code"] = "SSLS_CATEGORY_TRANSFER"
        target["calculation_rule"] = "将同一笔原料账本成本从熟食类毛利中扣除；门店调整合计为 0"
        frames.extend([out[list(ADJUSTMENT_COLUMNS)], target[list(ADJUSTMENT_COLUMNS)]])
    result = (
        pd.concat(frames, ignore_index=True)
        if frames else pd.DataFrame(columns=ADJUSTMENT_COLUMNS)
    )
    if not result.empty:
        residual = result.groupby(
            ["store_no", "business_date", "day_clear"]
        )["adjustment_amt"].sum()
        if residual.abs().gt(0.01).any():
            raise ValueError("SSLS category adjustment does not sum to zero")
    return result

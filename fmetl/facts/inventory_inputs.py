from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class InventoryInputResult:
    normalized: pd.DataFrame
    quarantined: pd.DataFrame


def normalize_inventory_inputs(
    counts: pd.DataFrame,
    *,
    decimal_shift_ratio: float = 10.0,
) -> InventoryInputResult:
    """Validate counts without allowing bad input to overwrite the ledger.

    Negative values are retained in quarantine and normalized to a missing
    count.  A decimal-shift rule is warning-only because a large legitimate
    inventory movement cannot be corrected safely without operator evidence.
    Decimal-shift suspicions are recorded but retained. Receipt-code and
    sale-code counts are independent observations and are both retained.
    """
    required = {
        "store_id", "business_date", "article_id", "actual_stock_qty",
        "previous_stock_qty", "count_group_id", "code_role",
    }
    missing = sorted(required - set(counts.columns))
    if missing:
        raise KeyError(f"inventory counts missing columns: {missing}")
    work = counts.copy()
    for column in ("actual_stock_qty", "previous_stock_qty"):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    quarantine: list[dict[str, object]] = []
    negative = work["actual_stock_qty"].lt(0)
    for row in work.loc[negative].itertuples(index=False):
        quarantine.append({
            "store_id": row.store_id, "business_date": row.business_date,
            "article_id": row.article_id, "reason_code": "NEGATIVE_COUNT_INPUT",
            "raw_value": row.actual_stock_qty,
        })
    work.loc[negative, "actual_stock_qty"] = np.nan
    current = work["actual_stock_qty"].abs()
    previous = work["previous_stock_qty"].abs()
    decimal_shift = (
        current.notna() & previous.gt(0)
        & ((current / previous).ge(decimal_shift_ratio) | (previous / current.replace(0, np.nan)).ge(decimal_shift_ratio))
    )
    for row in work.loc[decimal_shift].itertuples(index=False):
        quarantine.append({
            "store_id": row.store_id, "business_date": row.business_date,
            "article_id": row.article_id, "reason_code": "COUNT_DECIMAL_SHIFT_SUSPECTED",
            "raw_value": row.actual_stock_qty,
        })
    work["is_counted"] = work["actual_stock_qty"].notna()
    return InventoryInputResult(work, pd.DataFrame(quarantine))

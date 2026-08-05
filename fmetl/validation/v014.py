from __future__ import annotations

import pandas as pd

from fmetl.master_data.category import CategoryMapper


def validate_v014_ledger(
    sku_daily: pd.DataFrame,
    internal_postings: pd.DataFrame,
    *,
    source_activities: pd.DataFrame | None = None,
    qty_tolerance: float = 0.001,
    amount_tolerance: float = 0.01,
) -> pd.DataFrame:
    """Return machine-readable hard-gate results for a shadow run."""
    rows: list[dict[str, object]] = []

    def record(name: str, passed: bool, detail: str, failures: int = 0) -> None:
        rows.append({
            "check_name": name, "passed": bool(passed),
            "failure_count": int(failures), "detail": detail,
            "gate_type": "HARD",
        })

    negative_qty = int(sku_daily["end_qty"].lt(-qty_tolerance).sum())
    negative_amt = int(sku_daily["end_amt"].lt(-amount_tolerance).sum())
    record("NO_NEGATIVE_END_QTY", negative_qty == 0, "end_qty >= 0", negative_qty)
    record("NO_NEGATIVE_END_AMT", negative_amt == 0, "end_amt >= 0", negative_amt)
    qty_residual = int(sku_daily["qty_balance_residual"].abs().gt(qty_tolerance).sum())
    amt_residual = int(sku_daily["amount_balance_residual"].abs().gt(amount_tolerance).sum())
    record("SKU_QTY_BALANCE", qty_residual == 0, "abs residual <= tolerance", qty_residual)
    record("SKU_AMOUNT_BALANCE", amt_residual == 0, "abs residual <= tolerance", amt_residual)

    observation_failures = 0
    if source_activities is not None:
        keys = ["store_id", "business_date", "article_id"]
        observed_columns = (
            "gross_sale_qty", "sale_return_qty", "net_sale_qty", "net_sale_amt",
            "known_lost_qty", "store_receive_qty", "store_receive_amt",
        )
        missing = sorted(
            (set(keys) | set(observed_columns))
            - set(source_activities.columns)
        )
        if missing:
            raise KeyError(f"source activities missing validation columns: {missing}")
        source = source_activities[keys + list(observed_columns)].copy()
        if source.duplicated(keys).any():
            raise ValueError("source activities must be unique per SKU day")
        compared = source.merge(
            sku_daily[keys + list(observed_columns)],
            on=keys,
            how="outer",
            suffixes=("_source", "_ledger"),
            indicator=True,
            validate="one_to_one",
        )
        mismatch = compared["_merge"].ne("both")
        for column in observed_columns:
            tolerance = amount_tolerance if column.endswith("amt") else qty_tolerance
            mismatch |= (
                compared[f"{column}_source"].fillna(0.0)
                - compared[f"{column}_ledger"].fillna(0.0)
            ).abs().gt(tolerance)
        observation_failures = int(mismatch.sum())
    record(
        "EXTERNAL_OBSERVATION_CONSERVATION",
        observation_failures == 0,
        "sales, returns, receipts and known loss equal normalized source activities",
        observation_failures,
    )

    continuity_failures = 0
    ordered = sku_daily.sort_values(["store_id", "article_id", "business_date"])
    for _, group in ordered.groupby(["store_id", "article_id"], sort=False):
        previous_qty = group["end_qty"].shift()
        previous_amt = group["end_amt"].shift()
        mask = previous_qty.notna()
        continuity_failures += int(
            (
                group.loc[mask, "init_stock_qty"].sub(previous_qty.loc[mask]).abs().gt(qty_tolerance)
                | group.loc[mask, "init_stock_amt"].sub(previous_amt.loc[mask]).abs().gt(amount_tolerance)
            ).sum()
        )
    record("D1_EXACT_ROLL_FORWARD", continuity_failures == 0, "D+1 opening equals D ending", continuity_failures)

    posting_failures = 0
    if not internal_postings.empty:
        pivot = internal_postings.pivot_table(
            index=["store_id", "business_date", "event_group_id"],
            columns="posting_role", values="amt", aggfunc="sum", fill_value=0.0,
        )
        posting_failures = int(pivot.get("OUT", 0.0).sub(pivot.get("IN", 0.0)).abs().gt(amount_tolerance).sum())
    record("INTERNAL_AMOUNT_CONSERVATION", posting_failures == 0, "OUT amount equals IN amount", posting_failures)

    bom_parent_failures = 0
    if not internal_postings.empty:
        bom_sources = internal_postings.loc[
            internal_postings["relation_type"].eq("DISASSEMBLY_BOM")
            & internal_postings["posting_role"].eq("OUT"),
            ["store_id", "business_date", "article_id"],
        ].drop_duplicates()
        if not bom_sources.empty:
            parent_ends = bom_sources.merge(
                sku_daily[["store_id", "business_date", "article_id", "end_qty"]],
                on=["store_id", "business_date", "article_id"],
                how="left", validate="one_to_one",
            )
            bom_parent_failures = int(
                (parent_ends["end_qty"].isna() | parent_ends["end_qty"].abs().gt(qty_tolerance)).sum()
            )
    record(
        "BOM_PARENT_FULLY_TRANSFERRED",
        bom_parent_failures == 0,
        "formal BOM parent ending quantity equals zero",
        bom_parent_failures,
    )
    return pd.DataFrame(rows)


def validate_publishability(
    sku_daily: pd.DataFrame,
    *,
    qty_tolerance: float = 0.001,
    cost_tolerance: float = 0.000001,
) -> pd.DataFrame:
    """Return release gates that may fail without discarding diagnostics."""
    outflow_qty = (
        sku_daily["gross_sale_qty"]
        + sku_daily["known_lost_qty"]
        + sku_daily["bom_out_qty"]
        + sku_daily["pack_out_qty"]
        + sku_daily["compose_out_qty"]
        + sku_daily["residual_transfer_out_qty"]
    )
    missing_cost = sku_daily["issue_unit_cost"].le(cost_tolerance) & outflow_qty.gt(
        qty_tolerance
    )
    failures = int(missing_cost.sum())
    return pd.DataFrame([{
        "check_name": "ISSUE_COST_EVIDENCE",
        "passed": failures == 0,
        "failure_count": failures,
        "detail": "every sale, loss and internal outflow has a positive issue unit cost",
        "gate_type": "PUBLISH",
    }])


def validate_category_evidence(
    mapper: CategoryMapper,
    *,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Require dated immutable category evidence before release.

    Legacy static rules remain deterministic and can support diagnostics. They
    cannot prove the classification state of a later business day.
    """
    declared_immutable = mapper.evidence_status == "DATED_IMMUTABLE_SNAPSHOT"
    covers_window = bool(
        mapper.snapshot_start
        and mapper.snapshot_end
        and mapper.snapshot_start <= start_date
        and mapper.snapshot_end >= end_date
    )
    failures = 0 if declared_immutable and covers_window else 1
    return pd.DataFrame([{
        "check_name": "CATEGORY_SNAPSHOT_EVIDENCE",
        "passed": failures == 0,
        "failure_count": failures,
        "detail": (
            "category mapping must be a dated immutable snapshot covering "
            f"{start_date}..{end_date}; status={mapper.evidence_status}, "
            f"coverage={mapper.snapshot_start or '-'}..{mapper.snapshot_end or '-'}"
        ),
        "gate_type": "PUBLISH",
    }])


def assert_hard_gates(validation: pd.DataFrame) -> None:
    hard = (
        validation["gate_type"].eq("HARD")
        if "gate_type" in validation
        else pd.Series(True, index=validation.index)
    )
    failed = validation.loc[hard & ~validation["passed"]]
    if not failed.empty:
        names = ", ".join(failed["check_name"].astype(str))
        raise ValueError(f"v0.16 hard validation failed: {names}")


def is_publishable(validation: pd.DataFrame) -> bool:
    publish = (
        validation["gate_type"].eq("PUBLISH")
        if "gate_type" in validation
        else pd.Series(False, index=validation.index)
    )
    return bool(validation.loc[publish, "passed"].all())

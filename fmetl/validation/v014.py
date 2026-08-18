from __future__ import annotations

import pandas as pd

from fmetl import __version__
from fmetl.master_data.category import CategoryMapper


def validate_v014_ledger(
    sku_daily: pd.DataFrame,
    internal_postings: pd.DataFrame,
    *,
    source_activities: pd.DataFrame | None = None,
    reserved_raw_loss: pd.DataFrame | None = None,
    processing_trace: pd.DataFrame | None = None,
    special_loss_coverage: pd.DataFrame | None = None,
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
    record("NO_NEGATIVE_END_QTY", negative_qty == 0, "每个 SKU 日期的期末数量必须大于等于 0", negative_qty)
    record("NO_NEGATIVE_END_AMT", negative_amt == 0, "每个 SKU 日期的期末金额必须大于等于 0", negative_amt)
    qty_residual = int(sku_daily["qty_balance_residual"].abs().gt(qty_tolerance).sum())
    amt_residual = int(sku_daily["amount_balance_residual"].abs().gt(amount_tolerance).sum())
    record("SKU_QTY_BALANCE", qty_residual == 0, "每个 SKU 日期的数量方程残差绝对值必须不超过 0.001", qty_residual)
    record("SKU_AMOUNT_BALANCE", amt_residual == 0, "每个 SKU 日期的金额方程残差绝对值必须不超过 0.01 元", amt_residual)

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
        "账本中的销售、退货、实际验收和已知报损必须与标准化源数据逐 SKU 日期一致",
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
    record("D1_EXACT_ROLL_FORWARD", continuity_failures == 0, "每个 SKU 的下一日期初数量和金额必须等于上一日期末", continuity_failures)

    posting_failures = 0
    if not internal_postings.empty:
        pivot = internal_postings.pivot_table(
            index=["store_id", "business_date", "event_group_id"],
            columns="posting_role", values="amt", aggfunc="sum", fill_value=0.0,
        )
        posting_failures = int(pivot.get("OUT", 0.0).sub(pivot.get("IN", 0.0)).abs().gt(amount_tolerance).sum())
    record("INTERNAL_AMOUNT_CONSERVATION", posting_failures == 0, "每个内部事件的来源转出金额必须等于目标转入金额", posting_failures)

    raw_loss_priority_failures = 0
    if reserved_raw_loss is not None and not reserved_raw_loss.empty:
        required_reserved = {
            "store_id", "business_date", "article_id", "reserved_loss_qty",
        }
        missing = sorted(required_reserved - set(reserved_raw_loss.columns))
        if missing:
            raise KeyError(f"reserved raw loss missing validation columns: {missing}")
        processing_out = pd.DataFrame(
            columns=["store_id", "business_date", "article_id"]
        )
        if processing_trace is not None and not processing_trace.empty:
            trace_required = {
                "store_id", "business_date", "source_article_id",
                "relation_type", "source_out_qty",
            }
            missing_trace = sorted(trace_required - set(processing_trace.columns))
            if missing_trace:
                raise KeyError(
                    f"processing trace missing validation columns: {missing_trace}"
                )
            processing_out = processing_trace.loc[
                processing_trace["relation_type"].eq("PROCESSING")
                & pd.to_numeric(
                    processing_trace["source_out_qty"], errors="raise"
                ).gt(qty_tolerance),
                ["store_id", "business_date", "source_article_id"],
            ].rename(columns={"source_article_id": "article_id"}).drop_duplicates()
        reserved = reserved_raw_loss.loc[
            pd.to_numeric(
                reserved_raw_loss["reserved_loss_qty"], errors="raise"
            ).gt(qty_tolerance),
            ["store_id", "business_date", "article_id"],
        ].drop_duplicates()
        if not processing_out.empty and not reserved.empty:
            raw_loss_priority_failures = len(processing_out.merge(
                reserved,
                on=["store_id", "business_date", "article_id"],
                how="inner",
            ))
    record(
        "PROCESSING_RAW_LOSS_PRIORITY",
        raw_loss_priority_failures == 0,
        "原料当天已有熟食联动报损时，同一原料同一天不得再由加工计算扣减",
        raw_loss_priority_failures,
    )
    special_loss_failures = 0
    if special_loss_coverage is not None and not special_loss_coverage.empty:
        keys = ["store_id", "business_date", "article_id"]
        required_special = {
            *keys, "special_loss_qty", "effective_known_lost_qty",
        }
        missing = sorted(required_special - set(special_loss_coverage.columns))
        if missing:
            raise KeyError(f"special loss coverage missing validation columns: {missing}")
        expected = special_loss_coverage[
            keys + ["special_loss_qty", "effective_known_lost_qty"]
        ].copy()
        actual = sku_daily[keys + ["known_lost_qty"]].copy()
        compared = expected.merge(
            actual, on=keys, how="left", validate="one_to_one"
        )
        mismatch = (
            compared["known_lost_qty"].isna()
            | compared["known_lost_qty"].sub(
                compared["effective_known_lost_qty"]
            ).abs().gt(qty_tolerance)
            | compared["known_lost_qty"].add(qty_tolerance).lt(
                compared["special_loss_qty"]
            )
        )
        special_loss_failures = int(mismatch.sum())
    record(
        "SPECIAL_LOSS_LEDGER_COVERAGE",
        special_loss_failures == 0,
        "已知报损量取通用报损量与炒菜机加熟食联动数量中的较大值；通用报损没有原因和记录号，因此只能声明该覆盖假设，不能证明逐笔重合",
        special_loss_failures,
    )
    return pd.DataFrame(rows)


def validate_publishability(
    sku_daily: pd.DataFrame,
    *,
    ssls_covered_targets: pd.DataFrame | None = None,
    qty_tolerance: float = 0.001,
    cost_tolerance: float = 0.000001,
) -> pd.DataFrame:
    """Return release gates that may fail without discarding diagnostics."""
    regular_issue_qty = (
        sku_daily["gross_sale_qty"]
        + sku_daily["known_lost_qty"]
        + sku_daily["pack_out_qty"]
        + sku_daily["compose_out_qty"]
        + sku_daily["residual_transfer_out_qty"]
    )
    missing_issue_cost = (
        sku_daily["issue_unit_cost"].le(cost_tolerance)
        & regular_issue_qty.gt(qty_tolerance)
    )
    if ssls_covered_targets is not None and not ssls_covered_targets.empty:
        key_columns = ["store_id", "business_date", "article_id"]
        missing = sorted(set(key_columns) - set(ssls_covered_targets.columns))
        if missing:
            raise KeyError(f"SSLS target coverage missing columns: {missing}")
        covered = set(map(
            tuple, ssls_covered_targets[key_columns].astype(str).to_numpy()
        ))
        row_keys = list(zip(
            sku_daily["store_id"].astype(str),
            sku_daily["business_date"].astype(str),
            sku_daily["article_id"].astype(str),
        ))
        missing_issue_cost &= ~pd.Series(
            [key in covered for key in row_keys], index=sku_daily.index
        )
    missing_return_cost = (
        sku_daily["sale_return_qty"].gt(qty_tolerance)
        & sku_daily["sale_return_cost_basis"].le(cost_tolerance)
    )
    missing_bom_cost = (
        sku_daily["bom_out_qty"].gt(qty_tolerance)
        & sku_daily["bom_out_amt"].le(cost_tolerance)
    )
    missing_cost = missing_issue_cost | missing_return_cost | missing_bom_cost
    failures = int(missing_cost.sum())
    return pd.DataFrame([{
        "check_name": "ISSUE_COST_EVIDENCE",
        "passed": failures == 0,
        "failure_count": failures,
        "detail": (
            "销售、普通报损和非 BOM 内部转出必须有正的当日单位成本；"
            "BOM 转出必须有正的本次验收分配金额；销售退货必须有正的退货成本。"
            "任一条件不满足均阻止发布"
        ),
        "gate_type": "PUBLISH",
    }])


def validate_category_evidence(
    mapper: CategoryMapper,
    *,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Require auditable category evidence before release.

    Legacy static rules remain deterministic and can support diagnostics. They
    cannot prove the classification state of a later business day.
    """
    declared_immutable = mapper.evidence_status == "DATED_IMMUTABLE_SNAPSHOT"
    covers_window = bool(
        mapper.snapshot_start and mapper.snapshot_end
        and mapper.snapshot_start <= start_date and mapper.snapshot_end >= end_date
    )
    latest_platform = mapper.evidence_status == "MONITORING_PLATFORM_LATEST_SNAPSHOT"
    latest_not_older_than_run = bool(
        mapper.snapshot_end and mapper.snapshot_end >= end_date
    )
    failures = 0 if (
        (declared_immutable and covers_window)
        or (latest_platform and latest_not_older_than_run)
    ) else 1
    return pd.DataFrame([{
        "check_name": "CATEGORY_SNAPSHOT_EVIDENCE",
        "passed": failures == 0,
        "failure_count": failures,
        "detail": (
            "分类必须使用覆盖完整计算区间的固定历史快照，或统一使用监控平台最新健康快照；"
            f"计算区间={start_date}..{end_date}；证据状态={mapper.evidence_status}；"
            f"分类快照区间={mapper.snapshot_start or '-'}..{mapper.snapshot_end or '-'}。"
            "不满足时阻止发布"
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
        raise ValueError(f"v{__version__} hard validation failed: {names}")


def is_publishable(validation: pd.DataFrame) -> bool:
    publish = (
        validation["gate_type"].eq("PUBLISH")
        if "gate_type" in validation
        else pd.Series(False, index=validation.index)
    )
    return bool(validation.loc[publish, "passed"].all())

from __future__ import annotations

import pandas as pd

from fmetl.relations.graph import topological_order


BLOCKING_REASON_CODES = {
    "PURCHASE_RECEIPT_MISSING_RECEIVE_SALE",
    "BOM_QUANTITY_EVIDENCE_INCOMPLETE",
    "AMBIGUOUS_ALTERNATIVE_OR_CONVERSION_RELATION",
    "ALTERNATIVE_RECIPE_HAS_MULTIPLE_ACTIVE_EDGES",
    "MULTIPLE_FORMAL_RELATION_TYPES",
    "RELATION_RATIO_MISSING",
    "UNRESOLVED_RELATION",
    "MISSING_COST_EVIDENCE",
    "RECIPE_GROUP_MISSING",
    "RECIPE_GROUP_INCOMPLETE",
    "PROCESSING_RELATION_CONFLICT",
    "MISSING_AUTHORITATIVE_DAY_CLEAR",
}


def validate_v018_release_evidence(
    *,
    relation_registry: pd.DataFrame,
    quarantine: pd.DataFrame,
    category_adjustments: pd.DataFrame,
    source_completeness: pd.DataFrame,
    category_mapping_audit: pd.DataFrame,
    levels_result: pd.DataFrame,
    ssls_target_cost_audit: pd.DataFrame | None = None,
    inventory_count_audit: pd.DataFrame | None = None,
    sku_daily: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Return v0.18 relation, source, adjustment and blocker checks."""
    rows: list[dict[str, object]] = []

    def record(
        name: str, passed: bool, failures: int, detail: str, gate_type: str,
    ) -> None:
        rows.append({
            "check_name": name,
            "passed": bool(passed),
            "failure_count": int(failures),
            "detail": detail,
            "gate_type": gate_type,
        })

    active = relation_registry.loc[relation_registry["status"].eq("ACTIVE")].copy()
    source_rate = pd.to_numeric(
        active["source_qty_per_target_qty"], errors="coerce"
    )
    target_rate = pd.to_numeric(
        active["target_qty_per_source_qty"], errors="coerce"
    )
    reciprocal_failures = int(
        (
            source_rate.isna() | target_rate.isna()
            | source_rate.le(0) | target_rate.le(0)
            | source_rate.mul(target_rate).sub(1.0).abs().gt(0.000001)
        ).sum()
    )
    record(
        "RELATION_RATIO_RECIPROCAL",
        reciprocal_failures == 0,
        reciprocal_failures,
        "每条当天可用关系必须同时保存每 1 个 B 需要多少 A、每 1 个 A 可生成多少 B；两个正比例相乘必须等于 1",
        "HARD",
    )

    cycle_failures = 0
    if relation_registry.empty:
        cycle_relations = relation_registry
    else:
        evidence_only_bom = pd.Series(False, index=relation_registry.index)
        if "relation_type" in relation_registry:
            evidence_only_bom = (
                relation_registry["relation_type"].eq("BOM")
                & relation_registry["status"].eq("EVIDENCE_ONLY")
            )
        cycle_relations = relation_registry.loc[
            relation_registry["formal_flow_allowed"].map(bool)
            | evidence_only_bom
        ]
    if not cycle_relations.empty:
        formal = cycle_relations
        for _, group in formal.groupby(["store_id", "business_date"], sort=False):
            try:
                topological_order(zip(
                    group["source_article_id"].astype(str),
                    group["target_article_id"].astype(str),
                ))
            except ValueError:
                cycle_failures += 1
    record(
        "RELATION_GRAPH_ACYCLIC",
        cycle_failures == 0,
        cycle_failures,
        "按门店和日期检查全部可用关系及正式 BOM 方向；即使当天没有实际事件，也不允许形成 A 到 B 再回到 A 的循环",
        "HARD",
    )

    adjustment_failures = 0
    if not category_adjustments.empty:
        residual = category_adjustments.groupby(
            ["store_no", "business_date", "day_clear"]
        )["adjustment_amt"].sum()
        adjustment_failures = int(residual.abs().gt(0.01).sum())
    record(
        "SSLS_CATEGORY_ADJUSTMENT_CONSERVATION",
        adjustment_failures == 0,
        adjustment_failures,
        "每个门店、日期和日清口径内，原料大分类加回金额必须等于熟食类扣减金额，分类调整合计为 0",
        "HARD",
    )
    carrier_failures = 0
    if not category_adjustments.empty:
        carrier_keys = [
            "store_no", "business_date", "day_clear",
            "category_level1_description",
        ]
        required_levels = {*carrier_keys, "level_description"}
        missing = sorted(required_levels - set(levels_result.columns))
        if missing:
            raise KeyError(f"levels result missing category carrier columns: {missing}")
        required_carriers = category_adjustments[carrier_keys].drop_duplicates()
        existing_carriers = levels_result.loc[
            levels_result["level_description"].eq("大分类"), carrier_keys
        ].drop_duplicates()
        carrier_failures = len(required_carriers.merge(
            existing_carriers, on=carrier_keys, how="left", indicator=True
        ).loc[lambda frame: frame["_merge"].ne("both")])
    record(
        "SSLS_CATEGORY_OUTPUT_CARRIER",
        carrier_failures == 0,
        carrier_failures,
        "每条熟食联动原料分类加回和熟食类扣减都必须准确落到一条大分类输出记录",
        "HARD",
    )

    count_failures = 0
    if inventory_count_audit is not None or sku_daily is not None:
        if inventory_count_audit is None or sku_daily is None:
            raise KeyError("盘点来源门禁必须同时收到盘点源明细和完整 SKU 日账")
        count_required = {
            "store_id", "business_date", "article_id", "actual_stock_qty",
            "is_counted", "is_explicit_operator_count", "count_status",
        }
        ledger_required = {
            "store_id", "business_date", "article_id", "actual_stock_qty",
            "is_counted",
        }
        missing_count = sorted(count_required - set(inventory_count_audit.columns))
        missing_ledger = sorted(ledger_required - set(sku_daily.columns))
        if missing_count or missing_ledger:
            raise KeyError(
                f"盘点来源门禁缺字段：盘点源={missing_count}；SKU 日账={missing_ledger}"
            )
        keys = ["store_id", "business_date", "article_id"]
        source = inventory_count_audit[list(count_required)].copy()
        ledger = sku_daily[list(ledger_required)].copy()
        source[keys] = source[keys].astype(str)
        ledger[keys] = ledger[keys].astype(str)
        if source.duplicated(keys).any() or ledger.duplicated(keys).any():
            raise ValueError("盘点来源门禁要求盘点源和 SKU 日账在门店、日期、SKU 粒度唯一")
        formal = (
            source["is_explicit_operator_count"].map(bool)
            & source["is_counted"].map(bool)
            & source["count_status"].eq("FORMAL_OPERATOR_COUNT")
        )
        source = source.assign(_formal_count=formal)
        used = ledger.loc[ledger["is_counted"].map(bool)].copy()
        joined = source.merge(
            used[keys + ["actual_stock_qty"]].rename(
                columns={"actual_stock_qty": "ledger_actual_stock_qty"}
            ),
            on=keys,
            how="outer",
            indicator=True,
        )
        source_actual = pd.to_numeric(joined["actual_stock_qty"], errors="coerce")
        ledger_actual = pd.to_numeric(
            joined["ledger_actual_stock_qty"], errors="coerce"
        )
        expected_formal = joined["_formal_count"].fillna(False).map(bool)
        actually_used = joined["_merge"].eq("both") | joined["_merge"].eq("right_only")
        count_failures = int((expected_formal.ne(actually_used)).sum())
        count_failures += int((
            expected_formal
            & actually_used
            & source_actual.sub(ledger_actual).abs().gt(0.000001)
        ).sum())
    record(
        "OPERATOR_COUNT_SOURCE_INTEGRITY",
        count_failures == 0,
        count_failures,
        "账本只有在盘点记录含人工创建或人工更新、实盘数量非负且来源状态为正式人工盘点时才覆盖期末；系统快照只保留审计，不得进入账本",
        "HARD",
    )

    required_source_failures = 0
    if not source_completeness.empty:
        required = source_completeness.loc[
            source_completeness["source_tier"].isin(
                {"CORE_CALCULATION", "PUBLIC_REPORTING"}
            )
        ]
        required_source_failures = int((~required["is_complete"].map(bool)).sum())
    record(
        "REQUIRED_SOURCE_COMPLETENESS",
        required_source_failures == 0,
        required_source_failures,
        "计算源和公开报表源必须覆盖完整；只读审计源缺失只记录问题，不改变计算结果",
        "PUBLISH",
    )

    category_mapping_failures = 0
    if not category_mapping_audit.empty:
        required_category = {
            "active_in_window", "category_mapping_source",
        }
        missing = sorted(required_category - set(category_mapping_audit.columns))
        if missing:
            raise KeyError(f"category mapping audit missing columns: {missing}")
        active = category_mapping_audit["active_in_window"].map(bool)
        category_mapping_failures = int((
            active
            & ~category_mapping_audit["category_mapping_source"].eq(
                "MONITORING_PLATFORM_LATEST"
            )
        ).sum())
    record(
        "CATEGORY_MAPPING_COVERAGE",
        category_mapping_failures == 0,
        category_mapping_failures,
        "计算窗口内有库存或流动的 SKU 必须命中监控平台最新分类；回退到 dim_goods 只用于诊断并阻止发布",
        "PUBLISH",
    )

    category_value_failures = 0
    if not category_mapping_audit.empty:
        required_category_values = {
            "active_in_window", "category_mapping_source",
            "category_level1_description",
            "category_authoritative_level1_description",
        }
        missing = sorted(required_category_values - set(category_mapping_audit.columns))
        if missing:
            raise KeyError(f"category mapping value audit missing columns: {missing}")
        platform_active = (
            category_mapping_audit["active_in_window"].map(bool)
            & category_mapping_audit["category_mapping_source"].eq(
                "MONITORING_PLATFORM_LATEST"
            )
        )
        category_value_failures = int((
            platform_active
            & ~category_mapping_audit["category_level1_description"].fillna("").eq(
                category_mapping_audit[
                    "category_authoritative_level1_description"
                ].fillna("")
            )
        ).sum())
    record(
        "CATEGORY_MAPPING_VALUE_MATCH",
        category_value_failures == 0,
        category_value_failures,
        "命中监控平台的 SKU 直接保留平台最新大分类，不再套用旧分类规则",
        "PUBLISH",
    )

    ssls_target_failures = 0
    if ssls_target_cost_audit is not None and not ssls_target_cost_audit.empty:
        if "covered" not in ssls_target_cost_audit:
            raise KeyError("SSLS target cost audit missing column: covered")
        ssls_target_failures = int((
            ~ssls_target_cost_audit["covered"].map(bool)
        ).sum())
    record(
        "SSLS_TARGET_COST_COVERAGE",
        ssls_target_failures == 0,
        ssls_target_failures,
        "熟食联动优先停止加工时，必须能唯一确定配方，且当天每种原料的熟食联动数量必须覆盖该配方需求",
        "PUBLISH",
    )

    blocker_count = 0
    if not quarantine.empty and "reason_code" in quarantine:
        reason = quarantine["reason_code"].fillna("").astype(str)
        blocker_count = int(reason.map(
            lambda value: any(code in value.split(",") for code in BLOCKING_REASON_CODES)
        ).sum())
    record(
        "PUBLISH_BLOCKING_ISSUES",
        blocker_count == 0,
        blocker_count,
        "缺验收、BOM 证据不完整、配方不唯一、关系冲突、缺成本或缺权威日清字段均阻止发布",
        "PUBLISH",
    )
    return pd.DataFrame(rows)

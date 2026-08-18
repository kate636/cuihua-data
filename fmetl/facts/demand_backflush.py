from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from fmetl.calculations.ledger import (
    LedgerResult,
    SOURCE_COLUMNS,
    TARGET_COLUMNS,
    run_weighted_ledger,
)


NON_BOM_RELATION_TYPES = {
    "PROCESSING", "EXPLICIT_CONVERT", "PRODUCT_GROUP_CONVERT",
}
LEDGER_RELATION_TYPE = {
    "PROCESSING": "RECIPE_COMPOSE",
    "EXPLICIT_CONVERT": "PACK_CONVERT",
    "PRODUCT_GROUP_CONVERT": "PACK_CONVERT",
}
TRACE_COLUMNS = (
    "store_id", "business_date", "event_group_id", "source_article_id",
    "target_article_id", "relation_type", "relation_id", "direction_source",
    "ratio_source", "source_qty_per_target_qty", "target_qty_per_source_qty",
    "recipe_group_id", "recipe_mode", "trigger_demand_qty", "source_out_qty",
    "target_in_qty", "iteration", "exclusion_reason",
)
QUARANTINE_COLUMNS = (
    "store_id", "business_date", "article_id", "reason_code", "detail",
    "trigger_demand_qty",
)


@dataclass(frozen=True)
class DemandBackflushResult:
    ledger: LedgerResult
    sources: pd.DataFrame
    targets: pd.DataFrame
    trace: pd.DataFrame
    quarantined: pd.DataFrame


SSLS_TARGET_COVERAGE_COLUMNS = (
    "store_id", "business_date", "article_id", "trigger_demand_qty",
    "selected_recipe_group_id",
    "required_raw_qty", "available_ssls_raw_qty", "ledger_raw_loss_qty",
    "covered", "coverage_reason",
)


def _empty(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _build_iteration(
    sku_daily: pd.DataFrame,
    registry: pd.DataFrame,
    *,
    reserved_raw_loss: pd.DataFrame,
    iteration: int,
    tolerance: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    required_daily = {"store_id", "business_date", "article_id", "eq_qty"}
    required_registry = {
        "store_id", "business_date", "source_article_id", "target_article_id",
        "relation_type", "status", "formal_flow_allowed",
        "source_qty_per_target_qty", "target_qty_per_source_qty",
        "relation_id", "relation_version", "direction_source", "ratio_source",
        "recipe_group_id", "recipe_mode",
    }
    for label, frame, required in (
        ("sku_daily", sku_daily, required_daily),
        ("relation_registry", registry, required_registry),
    ):
        missing = sorted(required - set(frame.columns))
        if missing:
            raise KeyError(f"{label} missing demand-backflush columns: {missing}")
    active = registry.loc[
        registry["status"].eq("ACTIVE")
        & registry["formal_flow_allowed"].map(bool)
        & registry["relation_type"].isin(NON_BOM_RELATION_TYPES)
    ].copy()
    if active.empty:
        return (
            _empty(SOURCE_COLUMNS), _empty(TARGET_COLUMNS),
            _empty(TRACE_COLUMNS), _empty(QUARANTINE_COLUMNS),
        )
    for column in ("source_qty_per_target_qty", "target_qty_per_source_qty"):
        active[column] = pd.to_numeric(active[column], errors="raise")
    invalid_ratio = (
        ~np.isfinite(active[[
            "source_qty_per_target_qty", "target_qty_per_source_qty",
        ]].to_numpy(dtype=float)).all(axis=1)
        | active["source_qty_per_target_qty"].le(0)
        | active["target_qty_per_source_qty"].le(0)
        | active["source_qty_per_target_qty"].mul(
            active["target_qty_per_source_qty"]
        ).sub(1.0).abs().gt(0.000001)
    )
    if invalid_ratio.any():
        raise ValueError("active non-BOM relation ratios must be positive reciprocals")

    reserved_index: dict[tuple[str, str, str], float] = {}
    if not reserved_raw_loss.empty:
        reserved_index = {
            (str(row.store_id), str(row.business_date), str(row.article_id)): float(
                row.reserved_loss_qty
            )
            for row in reserved_raw_loss.itertuples(index=False)
        }
    deficit = sku_daily.loc[
        pd.to_numeric(sku_daily["eq_qty"], errors="raise").lt(-tolerance),
        ["store_id", "business_date", "article_id", "eq_qty"],
    ].copy()
    deficit["remaining_demand_qty"] = -pd.to_numeric(
        deficit["eq_qty"], errors="raise"
    )
    demand_index = {
        (str(row.store_id), str(row.business_date), str(row.article_id)): float(
            row.remaining_demand_qty
        )
        for row in deficit.itertuples(index=False)
    }

    source_rows: list[dict[str, object]] = []
    target_rows: list[dict[str, object]] = []
    trace_rows: list[dict[str, object]] = []
    quarantine_rows: list[dict[str, object]] = []
    target_keys = ["store_id", "business_date", "target_article_id"]
    for target_key, edges in active.groupby(target_keys, sort=False):
        store, day, target_id = map(str, target_key)
        output_qty = demand_index.get((store, day, target_id), 0.0)
        if output_qty <= tolerance:
            continue
        grouped = edges.copy()
        grouped["effective_recipe_group_id"] = grouped["recipe_group_id"].fillna("").astype(str)
        blank_group = grouped["effective_recipe_group_id"].str.strip().eq("")
        grouped.loc[blank_group, "effective_recipe_group_id"] = grouped.loc[
            blank_group, "relation_id"
        ].astype(str)
        # Raw-loss priority applies only when every active edge for this target
        # is a processing recipe.  It does not cancel PACK or product-group
        # conversion, and it remains decisive before alternative-recipe choice.
        if grouped["relation_type"].eq("PROCESSING").all():
            raw_loss_hits = [
                str(row.source_article_id)
                for row in grouped.itertuples(index=False)
                if reserved_index.get(
                    (store, day, str(row.source_article_id)), 0.0
                ) > tolerance
            ]
            if raw_loss_hits:
                quarantine_rows.append({
                    "store_id": store, "business_date": day,
                    "article_id": target_id,
                    "reason_code": "PROCESSING_RAW_LOSS_PRIORITY",
                    "detail": ",".join(sorted(raw_loss_hits)),
                    "trigger_demand_qty": output_qty,
                })
                continue
        recipe_groups = grouped["effective_recipe_group_id"].unique()
        if len(recipe_groups) != 1:
            quarantine_rows.append({
                "store_id": store, "business_date": day,
                "article_id": target_id,
                "reason_code": "AMBIGUOUS_ALTERNATIVE_OR_CONVERSION_RELATION",
                "detail": ",".join(sorted(recipe_groups)),
                "trigger_demand_qty": output_qty,
            })
            continue
        recipe_group_id = str(recipe_groups[0])
        selected = grouped.loc[
            grouped["effective_recipe_group_id"].eq(recipe_group_id)
        ].copy()
        selected_relation_types = set(selected["relation_type"].astype(str))
        if len(selected_relation_types) != 1:
            quarantine_rows.append({
                "store_id": store, "business_date": day,
                "article_id": target_id,
                "reason_code": "AMBIGUOUS_ALTERNATIVE_OR_CONVERSION_RELATION",
                "detail": ",".join(sorted(selected_relation_types)),
                "trigger_demand_qty": output_qty,
            })
            continue
        source_relation_type = next(iter(selected_relation_types))
        ledger_relation_type = LEDGER_RELATION_TYPE[source_relation_type]
        modes = set(selected["recipe_mode"].fillna("").astype(str).str.upper()) - {""}
        recipe_mode = next(iter(modes)) if len(modes) == 1 else "SINGLE"
        if recipe_mode == "ALTERNATIVE" and len(selected) != 1:
            quarantine_rows.append({
                "store_id": store, "business_date": day,
                "article_id": target_id,
                "reason_code": "ALTERNATIVE_RECIPE_HAS_MULTIPLE_ACTIVE_EDGES",
                "detail": recipe_group_id,
                "trigger_demand_qty": output_qty,
            })
            continue
        event_id = (
            f"DEMAND|{store}|{day}|{target_id}|{recipe_group_id}|{iteration}"
        )
        target_rows.append({
            "store_id": store, "business_date": day,
            "event_group_id": event_id, "relation_type": ledger_relation_type,
            "target_article_id": target_id, "target_in_qty": output_qty,
            "amount_allocation_ratio": 1.0,
            "quantity_source": "TARGET_REMAINING_DEMAND_AFTER_CONFIRMED_INVENTORY",
            "relation_snapshot_id": str(selected["relation_version"].iloc[0]),
        })
        for edge in selected.itertuples(index=False):
            source_id = str(edge.source_article_id)
            source_per_target = float(edge.source_qty_per_target_qty)
            source_qty = output_qty * source_per_target
            source_rows.append({
                "store_id": store, "business_date": day,
                "event_group_id": event_id, "relation_type": ledger_relation_type,
                "source_article_id": source_id, "source_out_qty": source_qty,
                "quantity_source": "TARGET_REMAINING_DEMAND_AFTER_CONFIRMED_INVENTORY",
                "relation_snapshot_id": str(edge.relation_version),
            })
            trace_rows.append({
                "store_id": store, "business_date": day,
                "event_group_id": event_id,
                "source_article_id": source_id,
                "target_article_id": target_id,
                "relation_type": str(edge.relation_type),
                "relation_id": str(edge.relation_id),
                "direction_source": str(edge.direction_source),
                "ratio_source": str(edge.ratio_source),
                "source_qty_per_target_qty": source_per_target,
                "target_qty_per_source_qty": float(edge.target_qty_per_source_qty),
                "recipe_group_id": recipe_group_id,
                "recipe_mode": recipe_mode,
                "trigger_demand_qty": output_qty,
                "source_out_qty": source_qty,
                "target_in_qty": output_qty,
                "iteration": iteration,
                "exclusion_reason": "",
            })
    return (
        pd.DataFrame(source_rows, columns=SOURCE_COLUMNS),
        pd.DataFrame(target_rows, columns=TARGET_COLUMNS),
        pd.DataFrame(trace_rows, columns=TRACE_COLUMNS),
        pd.DataFrame(quarantine_rows, columns=QUARANTINE_COLUMNS),
    )


def run_ledger_with_demand_backflush(
    activities: pd.DataFrame,
    openings: pd.DataFrame,
    base_sources: pd.DataFrame,
    base_targets: pd.DataFrame,
    relation_registry: pd.DataFrame,
    *,
    reserved_raw_loss: pd.DataFrame | None = None,
    tolerance: float = 0.001,
    max_iterations: int = 20,
) -> DemandBackflushResult:
    """Iterate until every convertible finished shortage has been supplied once."""
    sources = base_sources.copy()
    targets = base_targets.copy()
    reserved = (
        reserved_raw_loss.copy()
        if reserved_raw_loss is not None
        else pd.DataFrame(columns=[
            "store_id", "business_date", "article_id", "reserved_loss_qty",
        ])
    )
    traces: list[pd.DataFrame] = []
    quarantines: list[pd.DataFrame] = []
    ledger = run_weighted_ledger(activities, openings, sources, targets)
    for iteration in range(1, max_iterations + 1):
        add_sources, add_targets, trace, quarantine = _build_iteration(
            ledger.sku_daily,
            relation_registry,
            reserved_raw_loss=reserved,
            iteration=iteration,
            tolerance=tolerance,
        )
        if not quarantine.empty:
            quarantines.append(quarantine)
        if add_sources.empty:
            return DemandBackflushResult(
                ledger=ledger,
                sources=sources,
                targets=targets,
                trace=(
                    pd.concat(traces, ignore_index=True)
                    if traces else _empty(TRACE_COLUMNS)
                ),
                quarantined=(
                    pd.concat(quarantines, ignore_index=True).drop_duplicates()
                    if quarantines else _empty(QUARANTINE_COLUMNS)
                ),
            )
        sources = (
            add_sources.copy()
            if sources.empty else pd.concat([sources, add_sources], ignore_index=True)
        )
        targets = (
            add_targets.copy()
            if targets.empty else pd.concat([targets, add_targets], ignore_index=True)
        )
        traces.append(trace)
        ledger = run_weighted_ledger(activities, openings, sources, targets)
    raise ValueError(
        f"demand backflush did not converge within {max_iterations} iterations"
    )


def audit_ssls_target_cost_coverage(
    priority_rows: pd.DataFrame,
    relation_registry: pd.DataFrame,
    reserved_raw_loss: pd.DataFrame,
    sku_daily: pd.DataFrame,
    *,
    tolerance: float = 0.001,
) -> pd.DataFrame:
    """Check whether SSLS quantity covers one selected processing recipe.

    A target is covered only when every raw material in its one selected
    processing recipe has enough same-day SSLS quantity for the target demand.
    Requirements are summed by raw SKU before comparison so one SSLS quantity
    cannot be reused by two targets.
    """
    if priority_rows.empty:
        return _empty(SSLS_TARGET_COVERAGE_COLUMNS)
    target_keys = ["store_id", "business_date", "article_id"]
    registry_keys = ["store_id", "business_date", "target_article_id"]
    priority = priority_rows.copy()
    priority[target_keys] = priority[target_keys].astype(str)
    priority["trigger_demand_qty"] = pd.to_numeric(
        priority["trigger_demand_qty"], errors="raise"
    )
    # Iterative backflush can report the same skipped target more than once as
    # downstream demand grows.  The final demand is the maximum, not the sum of
    # intermediate iterations.
    priority = priority.groupby(target_keys, as_index=False)[
        "trigger_demand_qty"
    ].max()
    registry = relation_registry.loc[
        relation_registry["status"].eq("ACTIVE")
        & relation_registry["formal_flow_allowed"].map(bool)
        & relation_registry["relation_type"].eq("PROCESSING")
    ].copy()
    if registry.empty:
        audit = priority[target_keys + ["trigger_demand_qty"]].copy()
        audit["selected_recipe_group_id"] = ""
        audit["required_raw_qty"] = ""
        audit["available_ssls_raw_qty"] = ""
        audit["ledger_raw_loss_qty"] = ""
        audit["covered"] = False
        audit["coverage_reason"] = "当天没有与该成品匹配的有效加工配方，因此不能认定熟食联动已覆盖成品成本"
        return audit[list(SSLS_TARGET_COVERAGE_COLUMNS)]
    registry[registry_keys + ["source_article_id"]] = registry[
        registry_keys + ["source_article_id"]
    ].astype(str)
    registry["source_qty_per_target_qty"] = pd.to_numeric(
        registry["source_qty_per_target_qty"], errors="raise"
    )
    registry["effective_recipe_group_id"] = registry[
        "recipe_group_id"
    ].fillna("").astype(str)
    blank_group = registry["effective_recipe_group_id"].str.strip().eq("")
    registry.loc[blank_group, "effective_recipe_group_id"] = registry.loc[
        blank_group, "relation_id"
    ].astype(str)

    raw_keys = ["store_id", "business_date", "source_article_id"]
    reserved = reserved_raw_loss.rename(columns={
        "article_id": "source_article_id",
    }).copy()
    reserved[raw_keys] = reserved[raw_keys].astype(str)
    reserved["reserved_loss_qty"] = pd.to_numeric(
        reserved["reserved_loss_qty"], errors="raise"
    )
    reserved = reserved.groupby(raw_keys, as_index=False)["reserved_loss_qty"].sum()
    ledger_cost = sku_daily.rename(columns={"article_id": "source_article_id"})[
        raw_keys + ["known_lost_qty", "issue_unit_cost"]
    ].copy()
    ledger_cost[raw_keys] = ledger_cost[raw_keys].astype(str)
    ledger_cost[["known_lost_qty", "issue_unit_cost"]] = ledger_cost[
        ["known_lost_qty", "issue_unit_cost"]
    ].apply(pd.to_numeric, errors="raise")
    raw_evidence = reserved.merge(
        ledger_cost, on=raw_keys, how="outer", validate="one_to_one"
    ).fillna({
        "reserved_loss_qty": 0.0,
        "known_lost_qty": 0.0,
        "issue_unit_cost": 0.0,
    })
    raw_evidence_index = raw_evidence.set_index(raw_keys)

    requirement_rows: list[dict[str, object]] = []
    audit_rows: list[dict[str, object]] = []
    for row in priority.itertuples(index=False):
        key = (str(row.store_id), str(row.business_date), str(row.article_id))
        edges = registry.loc[
            registry["store_id"].eq(key[0])
            & registry["business_date"].eq(key[1])
            & registry["target_article_id"].eq(key[2])
        ].copy()
        candidates: list[tuple[str, pd.DataFrame, bool]] = []
        for group_id, group_edges in edges.groupby(
            "effective_recipe_group_id", sort=True
        ):
            modes = set(
                group_edges["recipe_mode"].fillna("").astype(str).str.upper()
            ) - {""}
            structurally_valid = not (
                "ALTERNATIVE" in modes and len(group_edges) != 1
            )
            raw_requirements = group_edges.groupby(
                "source_article_id", as_index=False
            )["source_qty_per_target_qty"].sum()
            raw_requirements["required_qty"] = (
                raw_requirements["source_qty_per_target_qty"]
                * float(row.trigger_demand_qty)
            )
            locally_covered = structurally_valid
            for raw in raw_requirements.itertuples(index=False):
                raw_key = (key[0], key[1], str(raw.source_article_id))
                if raw_key not in raw_evidence_index.index:
                    locally_covered = False
                    break
                evidence = raw_evidence_index.loc[raw_key]
                required_qty = float(raw.required_qty)
                if not (
                    float(evidence.reserved_loss_qty) + tolerance >= required_qty
                    and float(evidence.known_lost_qty) + tolerance >= required_qty
                    and float(evidence.issue_unit_cost) > 0.000001
                ):
                    locally_covered = False
                    break
            candidates.append((str(group_id), group_edges, locally_covered))
        if len(candidates) == 1:
            selected_group_id, selected_edges, _ = candidates[0]
        else:
            covered_candidates = [candidate for candidate in candidates if candidate[2]]
            if len(covered_candidates) == 1:
                selected_group_id, selected_edges, _ = covered_candidates[0]
            else:
                selected_group_id, selected_edges = "", pd.DataFrame()
        if selected_edges.empty:
            audit_rows.append({
                "store_id": key[0], "business_date": key[1],
                "article_id": key[2],
                "trigger_demand_qty": float(row.trigger_demand_qty),
                "selected_recipe_group_id": "",
                "required_raw_qty": "", "available_ssls_raw_qty": "",
                "ledger_raw_loss_qty": "", "covered": False,
                "coverage_reason": (
                    "当天没有任何一套配方被熟食联动原料完整覆盖，或有多套配方同时满足，无法唯一选择"
                ),
            })
            continue
        for source_id, source_edges in selected_edges.groupby(
            "source_article_id", sort=True
        ):
            required_qty = float(
                source_edges["source_qty_per_target_qty"].sum()
                * float(row.trigger_demand_qty)
            )
            requirement_rows.append({
                "store_id": key[0], "business_date": key[1],
                "article_id": key[2], "source_article_id": str(source_id),
                "selected_recipe_group_id": selected_group_id,
                "trigger_demand_qty": float(row.trigger_demand_qty),
                "required_qty": required_qty,
            })
    if not requirement_rows:
        return pd.DataFrame(audit_rows, columns=SSLS_TARGET_COVERAGE_COLUMNS)

    requirements = pd.DataFrame(requirement_rows)
    total_required = requirements.groupby(raw_keys, as_index=False)[
        "required_qty"
    ].sum().rename(columns={"required_qty": "total_required_qty"})
    raw_coverage = total_required.merge(
        reserved, on=raw_keys, how="left", validate="one_to_one"
    ).merge(
        ledger_cost, on=raw_keys, how="left", validate="one_to_one"
    )
    raw_coverage[["reserved_loss_qty", "known_lost_qty", "issue_unit_cost"]] = (
        raw_coverage[["reserved_loss_qty", "known_lost_qty", "issue_unit_cost"]]
        .fillna(0.0)
    )
    raw_coverage["raw_covered"] = (
        raw_coverage["reserved_loss_qty"].add(tolerance).ge(
            raw_coverage["total_required_qty"]
        )
        & raw_coverage["known_lost_qty"].add(tolerance).ge(
            raw_coverage["total_required_qty"]
        )
        & raw_coverage["issue_unit_cost"].gt(0.000001)
    )
    requirements = requirements.merge(
        raw_coverage,
        on=raw_keys,
        how="left",
        validate="many_to_one",
    )
    for target_key, rows in requirements.groupby(target_keys, sort=False):
        store, day, target_id = map(str, target_key)
        describe = lambda field: ";".join(
            f"{raw.source_article_id}:{float(getattr(raw, field)):.6f}"
            for raw in rows.sort_values("source_article_id").itertuples(index=False)
        )
        covered = bool(rows["raw_covered"].all())
        audit_rows.append({
            "store_id": store, "business_date": day, "article_id": target_id,
            "trigger_demand_qty": float(rows["trigger_demand_qty"].iloc[0]),
            "selected_recipe_group_id": str(
                rows["selected_recipe_group_id"].iloc[0]
            ),
            "required_raw_qty": describe("required_qty"),
            "available_ssls_raw_qty": describe("reserved_loss_qty"),
            "ledger_raw_loss_qty": describe("known_lost_qty"),
            "covered": covered,
            "coverage_reason": (
                "当天唯一选中配方的每种原料需求均由熟食联动数量覆盖，且原料账本单位成本为正"
                if covered else
                "至少一种配方原料的熟食联动数量不足、账本已扣数量不足或原料账本单位成本不为正"
            ),
        })
    return pd.DataFrame(audit_rows, columns=SSLS_TARGET_COVERAGE_COLUMNS)

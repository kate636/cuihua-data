from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd

from fmetl.relations.recipe_groups import attach_recipe_groups, load_recipe_group_config


BOM_QUANTITY_TOLERANCE = 0.002


def _as_text(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    output = frame.copy()
    for column in columns:
        if column in output:
            output[column] = output[column].astype(str)
    return output


def _bool(value: object) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if value is None or pd.isna(value):
        return False
    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value) == 1.0
    text = str(value).strip().lower()
    if text in {"true", "yes", "y"}:
        return True
    try:
        return float(text) == 1.0
    except ValueError:
        return False


def _empty(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _normalize_percent(value: object) -> float:
    number = float(pd.to_numeric(value, errors="raise"))
    return number / 100.0 if number > 1.0 + 1e-9 else number


def build_processing_relations(
    raw: pd.DataFrame | None,
    *,
    store_id: str,
    days: Sequence[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    columns = (
        "store_id", "business_date", "relation_id", "raw_article_id",
        "finished_article_id", "raw_qty", "raw_unit", "yield_qty", "yield_unit",
        "category_type", "effective_from", "effective_to", "approved",
        "relation_source", "recipe_group_id", "recipe_mode",
        "recipe_config_version", "source_qty_per_target_qty",
        "target_qty_per_source_qty", "external_finished_receipt_qty",
        "external_finished_receipt_amt",
    )
    if raw is None or raw.empty:
        configured = load_recipe_group_config()
        quarantine = pd.DataFrame([
            {
                "store_id": store_id, "business_date": str(day),
                "article_id": str(finished),
                "reason_code": "RECIPE_GROUP_INCOMPLETE",
                "detail": "版本化配方中声明的原料关系没有出现在当天加工关系快照中，因此整组加工不执行",
            }
            for day in days
            for finished in configured["finished_article_id"].drop_duplicates()
        ])
        return _empty(columns), quarantine
    required = {"raw_article_id", "finished_article_id", "raw_qty", "yield_qty"}
    missing = sorted(required - set(raw.columns))
    if missing:
        raise KeyError(f"processing relation export missing columns: {missing}")
    source = raw.copy()
    source["raw_article_id"] = source["raw_article_id"].astype(str)
    source["finished_article_id"] = source["finished_article_id"].astype(str)
    source["raw_qty"] = pd.to_numeric(source["raw_qty"], errors="raise")
    source["yield_qty"] = pd.to_numeric(source["yield_qty"], errors="raise")
    ratio = source[["raw_qty", "yield_qty"]].to_numpy(dtype=float)
    if not np.isfinite(ratio).all() or (ratio <= 0).any():
        raise ValueError("processing relation raw_qty and yield_qty must be finite and positive")
    source["relation_source"] = source.get(
        "relation_source", "UNSPECIFIED_PROCESSING_EXPORT"
    )
    source["relation_id"] = source.get(
        "relation_id",
        "PROCESSING|" + source["finished_article_id"] + "|" + source["raw_article_id"],
    )
    source = attach_recipe_groups(source)
    finished_edge_count = source.groupby("finished_article_id")[
        "raw_article_id"
    ].transform("nunique")
    unconfigured = source["recipe_config_version"].eq(
        "DEFAULT_ONE_RECIPE_PER_FINISHED_SKU"
    )
    finished_has_unconfigured = unconfigured.groupby(
        source["finished_article_id"]
    ).transform("any")
    recipe_group_missing = finished_edge_count.gt(1) & finished_has_unconfigured
    recipe_group_incomplete = ~source["recipe_config_complete"].map(bool)
    approved_source = (
        source["approved"].map(_bool)
        if "approved" in source
        else pd.Series(True, index=source.index)
    )
    rows: list[dict[str, object]] = []
    quarantine: list[dict[str, object]] = []
    for day in map(str, days):
        for index, row in source.iterrows():
            effective_from = row.get("effective_from")
            effective_to = row.get("effective_to")
            is_approved = bool(approved_source.loc[index])
            has_effective_from = not pd.isna(effective_from)
            # A current undated export must not rewrite historical recipe truth.
            active = has_effective_from and is_approved and str(effective_from) <= day and (
                pd.isna(effective_to) or str(effective_to) >= day
            ) and not bool(recipe_group_missing.loc[index]) and not bool(
                recipe_group_incomplete.loc[index]
            )
            if bool(recipe_group_incomplete.loc[index]):
                inactive_reason = "RECIPE_GROUP_INCOMPLETE"
            elif bool(recipe_group_missing.loc[index]):
                inactive_reason = "RECIPE_GROUP_MISSING"
            elif not has_effective_from:
                inactive_reason = "PROCESSING_RELATION_EFFECTIVE_DATE_MISSING"
            elif not is_approved:
                inactive_reason = "PROCESSING_RELATION_NOT_APPROVED"
            else:
                inactive_reason = "PROCESSING_RELATION_OUTSIDE_EFFECTIVE_WINDOW"
            if str(row["raw_article_id"]) == str(row["finished_article_id"]):
                quarantine.append({
                    "store_id": store_id,
                    "business_date": day,
                    "article_id": str(row["finished_article_id"]),
                    "reason_code": (
                        "PROCESSING_SELF_RELATION_NOOP"
                        if active else inactive_reason
                    ),
                    "detail": str(row["relation_id"]),
                })
                continue
            rows.append({
                "store_id": store_id,
                "business_date": day,
                "relation_id": str(row["relation_id"]),
                "raw_article_id": str(row["raw_article_id"]),
                "finished_article_id": str(row["finished_article_id"]),
                "raw_qty": float(row["raw_qty"]),
                "raw_unit": str(row.get("raw_unit", "")),
                "yield_qty": float(row["yield_qty"]),
                "yield_unit": str(row.get("yield_unit", "")),
                "category_type": str(row.get("category_type", "")),
                "effective_from": None if pd.isna(effective_from) else str(effective_from),
                "effective_to": None if pd.isna(effective_to) else str(effective_to),
                "approved": active,
                "relation_source": str(row["relation_source"]),
                "recipe_group_id": str(row["recipe_group_id"]),
                "recipe_mode": str(row["recipe_mode"]),
                "recipe_config_version": str(row["recipe_config_version"]),
                "source_qty_per_target_qty": float(row["raw_qty"]) / float(row["yield_qty"]),
                "target_qty_per_source_qty": float(row["yield_qty"]) / float(row["raw_qty"]),
                "external_finished_receipt_qty": 0.0,
                "external_finished_receipt_amt": 0.0,
            })
            if not active:
                quarantine.append({
                    "store_id": store_id,
                    "business_date": day,
                    "article_id": str(row["finished_article_id"]),
                    "reason_code": inactive_reason,
                    "detail": (
                        f"关系ID={row['relation_id']}；"
                        f"版本化配方中缺少的原料SKU={row.get('missing_configured_raw_article_ids', '')}。"
                        "当天不执行该配方"
                    ),
                })
    result = pd.DataFrame(rows, columns=columns)

    # A configured multi-input recipe is valid only when every configured raw
    # edge is active on the same business date.  Checking the export as a whole
    # would let one expired edge disappear while the remaining edge posts as a
    # complete additive recipe.
    configured = load_recipe_group_config()
    for (finished_id, group_id), expected_rows in configured.groupby(
        ["finished_article_id", "recipe_group_id"], sort=False
    ):
        expected_raw = set(expected_rows["raw_article_id"].astype(str))
        for day in map(str, days):
            group_mask = (
                result["business_date"].astype(str).eq(day)
                & result["finished_article_id"].astype(str).eq(str(finished_id))
                & result["recipe_group_id"].astype(str).eq(str(group_id))
            )
            active_raw = set(
                result.loc[
                    group_mask & result["approved"].map(_bool), "raw_article_id"
                ].astype(str)
            )
            if active_raw == expected_raw:
                continue
            result.loc[group_mask, "approved"] = False
            quarantine.append({
                "store_id": store_id,
                "business_date": day,
                "article_id": str(finished_id),
                "reason_code": "RECIPE_GROUP_INCOMPLETE",
                "detail": (
                    f"配方组={group_id}；配置要求的原料SKU="
                    f"{','.join(sorted(expected_raw))}；当天有效的原料SKU="
                    f"{','.join(sorted(active_raw))}。原料集合不一致，整组加工不执行"
                ),
            })
    active = result.loc[result["approved"].map(_bool)]
    pair_keys = [
        "store_id", "business_date", "raw_article_id", "finished_article_id",
    ]
    duplicate_day_pair = active.duplicated(pair_keys, keep=False)
    drop_indices: list[int] = []
    if duplicate_day_pair.any():
        semantics = [
            "source_qty_per_target_qty", "target_qty_per_source_qty",
            "recipe_group_id", "recipe_mode",
        ]
        for pair, group in active.loc[duplicate_day_pair].groupby(
            pair_keys, sort=False
        ):
            normalized = group[semantics].copy()
            normalized[[
                "source_qty_per_target_qty", "target_qty_per_source_qty",
            ]] = normalized[[
                "source_qty_per_target_qty", "target_qty_per_source_qty",
            ]].round(12)
            if len(normalized.drop_duplicates()) == 1:
                keep = group.sort_values("relation_id").index[0]
                drop_indices.extend(index for index in group.index if index != keep)
                continue
            result.loc[group.index, "approved"] = False
            store, day, raw_id, finished_id = map(str, pair)
            quarantine.append({
                "store_id": store, "business_date": day,
                "article_id": finished_id,
                "reason_code": "PROCESSING_RELATION_CONFLICT",
                "detail": (
                    f"原料SKU={raw_id}；同一原料到成品在当天出现不同的比例或配方组："
                    f"{group[['relation_id', *semantics]].to_dict('records')}。"
                    "无法唯一确定关系，全部不执行"
                ),
            })
    if drop_indices:
        result = result.drop(index=drop_indices).reset_index(drop=True)
    return result, pd.DataFrame(quarantine)


def build_bom_relations(raw: pd.DataFrame) -> pd.DataFrame:
    columns = (
        "store_id", "business_date", "parent_article_id", "sub_article_id",
        "effective_from", "effective_to", "category_level1_description",
        "dressing_rate", "cost_rate", "source_qty_per_target_qty",
        "target_qty_per_source_qty", "approved",
    )
    if raw.empty:
        return _empty(columns)
    frame = raw.copy().rename(columns={"inc_day": "business_date"})
    frame = _as_text(
        frame,
        ("store_id", "business_date", "parent_article_id", "sub_article_id"),
    )
    # Both fields are weights. Their raw scale differs across BOM groups, but
    # only the within-group normalized share is used; threshold-based percent
    # conversion would distort groups containing weights below and above 1.
    frame["dressing_rate"] = pd.to_numeric(
        frame["dressing_rate"], errors="coerce"
    )
    frame["cost_rate"] = pd.to_numeric(frame["cost_rate"], errors="coerce")
    frame["source_qty_per_target_qty"] = np.nan
    frame["target_qty_per_source_qty"] = np.nan
    frame["effective_from"] = frame["business_date"]
    frame["effective_to"] = frame["business_date"]
    # Every row in the formal BOM mirror is relation evidence.  Category is a
    # reporting attribute, not a permission switch; excluding non-pork rows
    # here would silently discard valid formal relations.
    frame["approved"] = True
    return frame[list(columns)]


def attach_bom_event_ratios(
    bom: pd.DataFrame, events: pd.DataFrame
) -> pd.DataFrame:
    """Attach the observed parent/child quantity ratio to formal BOM edges."""
    out = bom.copy()
    ratio_columns = (
        "source_qty_per_target_qty", "target_qty_per_source_qty",
    )
    for column in ratio_columns:
        if column not in out:
            out[column] = np.nan
    if events.empty:
        return out
    keys = ["store_id", "business_date", "source_article_id", "target_article_id"]
    ratio = events[keys + ["target_qty", "target_common_qty"]].copy()
    ratio["source_qty_per_target_qty"] = (
        ratio["target_common_qty"] / ratio["target_qty"]
    )
    ratio["target_qty_per_source_qty"] = (
        ratio["target_qty"] / ratio["target_common_qty"]
    )
    ratio = ratio.drop(columns=["target_qty", "target_common_qty"])
    if ratio.duplicated(keys).any():
        raise ValueError("one formal BOM edge/day has multiple observed quantity ratios")
    left_keys = ["store_id", "business_date", "parent_article_id", "sub_article_id"]
    ratio = ratio.rename(columns={
        "source_article_id": "parent_article_id",
        "target_article_id": "sub_article_id",
    })
    out = out.drop(columns=list(ratio_columns), errors="ignore").merge(
        ratio,
        on=left_keys,
        how="left",
        validate="one_to_one",
    )
    return out


def build_explicit_relations(
    relation: pd.DataFrame,
) -> pd.DataFrame:
    columns = (
        "store_id", "business_date", "source_article_id", "target_article_id",
        "effective_from", "effective_to", "actual_event", "fixed_rule",
        "convert_rate", "source_qty_per_target_qty",
        "target_qty_per_source_qty", "cost_rate", "approved", "convert_type",
        "source_target_count", "target_source_count", "exclusion_reason",
    )
    if relation.empty:
        return _empty(columns)
    frame = relation.rename(columns={
        "inc_day": "business_date", "parent_article_id": "source_article_id",
        "sub_article_id": "target_article_id",
    }).copy()
    keys = ["store_id", "business_date", "source_article_id", "target_article_id"]
    frame = _as_text(frame, keys)
    parent_rate = pd.to_numeric(frame["parent_rate"], errors="coerce")
    sub_rate = pd.to_numeric(frame["sub_rate"], errors="coerce")
    reciprocal = parent_rate.mul(sub_rate).sub(1.0).abs().le(0.001)
    frame["convert_rate"] = parent_rate.where(parent_rate.gt(0) & reciprocal)
    frame["target_qty_per_source_qty"] = frame["convert_rate"]
    frame["source_qty_per_target_qty"] = 1.0 / frame["convert_rate"]
    frame["cost_rate"] = 1.0
    frame["actual_event"] = False
    frame["convert_type"] = pd.to_numeric(frame["ctype"], errors="coerce")
    frame["source_target_count"] = frame.groupby(
        ["store_id", "business_date", "convert_type", "source_article_id"],
        dropna=False,
    )["target_article_id"].transform("nunique")
    frame["target_source_count"] = frame.groupby(
        ["store_id", "business_date", "convert_type", "target_article_id"],
        dropna=False,
    )["source_article_id"].transform("nunique")
    # ctype=1 is BOM unit evidence, not a separate inventory-conversion rule.
    # Only ctype=2 rows whose source and target are both unique define a fixed
    # one-to-one conversion. ctype=3 mixes meanings and remains audit-only.
    frame["fixed_rule"] = (
        frame["convert_type"].eq(2)
        & frame["convert_rate"].notna()
        & frame["source_target_count"].eq(1)
        & frame["target_source_count"].eq(1)
    )
    frame["approved"] = frame["fixed_rule"]
    frame["exclusion_reason"] = np.select(
        [
            frame["convert_type"].eq(1),
            frame["convert_type"].eq(3),
            frame["convert_type"].isna() | ~frame["convert_type"].isin([1, 2, 3]),
            frame["convert_rate"].isna(),
            frame["source_target_count"].ne(1)
            | frame["target_source_count"].ne(1),
        ],
        [
            "BOM_UNIT_RATIO_ONLY",
            "MIXED_CONVERT_TYPE_NOT_POSTABLE",
            "UNKNOWN_CONVERT_TYPE",
            "INVALID_RECIPROCAL_RATIO",
            "NOT_ONE_TO_ONE",
        ],
        default="",
    )
    frame["effective_from"] = frame["business_date"]
    frame["effective_to"] = frame["business_date"]
    return frame[list(columns)]


def exclude_bom_backed_explicit_relations(
    explicit: pd.DataFrame,
    bom: pd.DataFrame,
) -> pd.DataFrame:
    """Treat article-convert rows on official BOM edges as unit evidence only."""
    if explicit.empty or bom.empty:
        return explicit.copy()
    keys = ["store_id", "business_date", "source_article_id", "target_article_id"]
    official = bom.loc[bom["approved"].map(_bool)].rename(columns={
        "parent_article_id": "source_article_id",
        "sub_article_id": "target_article_id",
    })
    official = official[keys].drop_duplicates().assign(_official_bom=True)
    probed = explicit.merge(
        official, on=keys, how="left", validate="many_to_one",
    )
    return probed.loc[
        probed["_official_bom"].isna(), explicit.columns
    ].reset_index(drop=True)


def build_bom_events(
    receive_sale: pd.DataFrame,
    bom: pd.DataFrame,
    article_convert: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    columns = (
        "store_id", "business_date", "event_group_id", "source_article_id",
        "target_article_id", "source_qty", "source_amount", "target_qty", "source_common_qty",
        "target_common_qty", "amount_allocation_ratio", "quantity_source",
        "convert_type", "parent_rate", "sub_rate", "dressing_rate", "cost_rate",
        "inbound_parent_qty", "inbound_parent_amt",
        "retained_parent_qty", "retained_parent_amt",
        "receive_sale_child_amt", "amount_allocation_basis",
        "target_allocated_amt", "parent_unit_child_qty_sum",
        "quantity_balance_residual", "amount_balance_residual",
    )
    official = bom.loc[bom["approved"].map(_bool)].copy()
    if receive_sale.empty or official.empty:
        return _empty(columns), pd.DataFrame()
    required_receive = {
        "store_id", "inc_day", "article_id", "sale_article_id",
        "inbound_qty", "inbound_amount", "sale_article_qty",
        "spilit_sale_article_amt",
    }
    missing_receive = sorted(required_receive - set(receive_sale.columns))
    if missing_receive:
        raise KeyError(f"receive_sale BOM evidence missing columns: {missing_receive}")
    required_convert = {
        "store_id", "inc_day", "parent_article_id", "sub_article_id",
        "parent_rate", "sub_rate", "ctype",
    }
    missing_convert = sorted(required_convert - set(article_convert.columns))
    if missing_convert:
        raise KeyError(f"article_convert BOM unit evidence missing columns: {missing_convert}")
    if "cost_rate" not in official:
        official["cost_rate"] = official["dressing_rate"]
    bridge = receive_sale.rename(columns={
        "inc_day": "business_date", "article_id": "source_article_id",
        "sale_article_id": "target_article_id", "inbound_qty": "source_qty",
        "sale_article_qty": "target_qty",
    }).copy()
    keys = ["store_id", "business_date", "source_article_id", "target_article_id"]
    bridge = _as_text(bridge, keys)
    for column in (
        "source_qty", "target_qty", "inbound_amount", "spilit_sale_article_amt",
    ):
        bridge[column] = pd.to_numeric(bridge[column], errors="coerce")
    edges = official.rename(columns={
        "parent_article_id": "source_article_id", "sub_article_id": "target_article_id",
    })
    unit = article_convert.rename(columns={
        "inc_day": "business_date",
        "parent_article_id": "source_article_id",
        "sub_article_id": "target_article_id",
    }).copy()
    unit = _as_text(unit, keys)
    unit["convert_type"] = pd.to_numeric(unit["ctype"], errors="coerce")
    unit["parent_rate"] = pd.to_numeric(unit["parent_rate"], errors="coerce")
    unit["sub_rate"] = pd.to_numeric(unit["sub_rate"], errors="coerce")
    unit = unit.loc[unit["convert_type"].eq(1)].copy()
    if unit.empty:
        unit_evidence = pd.DataFrame(columns=[
            *keys, "bom_source_qty_per_target_qty", "unit_ratio_valid",
            "convert_type", "parent_rate", "sub_rate",
        ])
    else:
        unit_summary = unit.groupby(keys, as_index=False).agg(
            sub_rate_min=("sub_rate", "min"),
            sub_rate_max=("sub_rate", "max"),
            parent_rate_min=("parent_rate", "min"),
            parent_rate_max=("parent_rate", "max"),
        )
        unit_summary["unit_ratio_valid"] = (
            unit_summary[[
                "sub_rate_min", "sub_rate_max",
            ]].notna().all(axis=1)
            & unit_summary["sub_rate_min"].gt(0)
            & unit_summary["sub_rate_max"].sub(
                unit_summary["sub_rate_min"]
            ).abs().le(0.000001)
        )
        unit_summary["bom_source_qty_per_target_qty"] = unit_summary["sub_rate_max"]
        unit_summary["convert_type"] = 1
        unit_summary["parent_rate"] = unit_summary["parent_rate_max"].where(
            unit_summary["parent_rate_max"].sub(
                unit_summary["parent_rate_min"]
            ).abs().le(0.000001)
        )
        unit_summary["sub_rate"] = unit_summary["sub_rate_max"]
        unit_evidence = unit_summary[
            [
                *keys, "bom_source_qty_per_target_qty", "unit_ratio_valid",
                "convert_type", "parent_rate", "sub_rate",
            ]
        ]
    official_keys = edges[keys].drop_duplicates().assign(_official_edge=True)
    parent_keys = ["store_id", "business_date", "source_article_id"]
    official_parent_days = edges[parent_keys].drop_duplicates().assign(
        _official_parent=True
    )
    relevant_bridge = bridge.merge(
        official_parent_days, on=parent_keys, how="inner", validate="many_to_one"
    )
    # receive_sale can retain part of a receipt on the parent code.  A parent
    # -> parent row is that retained part, not a BOM child and not an error.
    self_rows = relevant_bridge.loc[
        relevant_bridge["source_article_id"].eq(
            relevant_bridge["target_article_id"]
        )
    ].copy()
    child_rows = relevant_bridge.loc[
        relevant_bridge["source_article_id"].ne(
            relevant_bridge["target_article_id"]
        )
    ].copy()
    unexpected = child_rows.merge(
        official_keys, on=keys, how="left", validate="many_to_one"
    )
    unexpected_groups = unexpected.loc[
        unexpected["_official_edge"].isna(),
        ["store_id", "business_date", "source_article_id"],
    ].drop_duplicates()
    merged = child_rows.merge(
        edges[keys + ["dressing_rate", "cost_rate"]], on=keys, how="inner",
        validate="one_to_one",
    )
    merged = merged.merge(unit_evidence, on=keys, how="left", validate="one_to_one")
    self_group = ["store_id", "business_date", "source_article_id"]
    self_summary = self_rows.groupby(self_group, as_index=False).agg(
        self_inbound_qty_min=("source_qty", "min"),
        self_inbound_qty_max=("source_qty", "max"),
        self_inbound_amt_min=("inbound_amount", "min"),
        self_inbound_amt_max=("inbound_amount", "max"),
        self_retained_qty=("target_qty", "sum"),
        self_retained_amt=("spilit_sale_article_amt", "sum"),
    )
    child_evidence_groups = merged[self_group].drop_duplicates()
    self_only = self_summary.merge(
        child_evidence_groups.assign(_has_child=True),
        on=self_group,
        how="left",
        validate="one_to_one",
    ).loc[lambda frame: frame["_has_child"].isna()]
    invalid_self_only = self_only.loc[
        self_only["self_inbound_qty_max"].sub(
            self_only["self_inbound_qty_min"]
        ).abs().gt(0.000001)
        | self_only["self_inbound_amt_max"].sub(
            self_only["self_inbound_amt_min"]
        ).abs().gt(0.01)
        | self_only["self_retained_qty"].sub(
            self_only["self_inbound_qty_max"]
        ).abs().gt(0.000001)
        | self_only["self_retained_amt"].sub(
            self_only["self_inbound_amt_max"]
        ).abs().gt(0.01)
        | self_only[[
            "self_inbound_qty_max", "self_inbound_amt_max",
            "self_retained_qty", "self_retained_amt",
        ]].isna().any(axis=1)
    ][self_group]
    if merged.empty:
        blocked_without_event = pd.concat(
            [unexpected_groups, invalid_self_only], ignore_index=True
        ).drop_duplicates()
        quarantine = blocked_without_event.assign(
            article_id=blocked_without_event["source_article_id"],
            reason_code="BOM_QUANTITY_EVIDENCE_INCOMPLETE",
            detail=(
                "receive_sale 只有父品同码行时，该行数量和金额必须等于本次父品全部验收；"
                "如只保留部分父品，剩余部分必须有正式子品行，否则整组不执行"
            ),
        )
        return _empty(columns), quarantine
    for column in ("dressing_rate", "cost_rate"):
        merged[column] = pd.to_numeric(merged[column], errors="coerce")
    group = ["store_id", "business_date", "source_article_id"]
    retained = self_rows.groupby(group, as_index=False).agg(
        retained_parent_qty=("target_qty", "sum"),
        retained_parent_amt=("spilit_sale_article_amt", "sum"),
    )
    parent_evidence = relevant_bridge.groupby(group, as_index=False).agg(
        inbound_qty_min=("source_qty", "min"),
        inbound_qty_max=("source_qty", "max"),
        inbound_amt_min=("inbound_amount", "min"),
        inbound_amt_max=("inbound_amount", "max"),
    )
    merged = merged.merge(retained, on=group, how="left", validate="many_to_one")
    merged = merged.merge(
        parent_evidence, on=group, how="left", validate="many_to_one"
    )
    merged[["retained_parent_qty", "retained_parent_amt"]] = merged[
        ["retained_parent_qty", "retained_parent_amt"]
    ].fillna(0.0)
    merged["source_qty"] = (
        merged["inbound_qty_max"] - merged["retained_parent_qty"]
    )
    merged["source_amount"] = (
        merged["inbound_amt_max"] - merged["retained_parent_amt"]
    )
    valid = merged[[
        "source_qty", "target_qty", "dressing_rate",
        "bom_source_qty_per_target_qty",
    ]].notna().all(axis=1)
    valid &= merged[[
        "source_qty", "target_qty", "dressing_rate",
        "bom_source_qty_per_target_qty",
    ]].gt(0).all(axis=1)
    valid &= merged["unit_ratio_valid"].fillna(False).map(bool)
    valid &= merged["source_amount"].notna() & np.isfinite(
        merged["source_amount"].to_numpy(dtype=float)
    )
    valid &= merged["source_amount"].ge(-0.01)
    valid &= merged["retained_parent_qty"].ge(0)
    valid &= merged["retained_parent_amt"].ge(-0.01)
    valid &= merged["inbound_qty_max"].sub(
        merged["inbound_qty_min"]
    ).abs().le(0.000001)
    valid &= merged["inbound_amt_max"].sub(
        merged["inbound_amt_min"]
    ).abs().le(0.01)
    merged["_valid"] = valid
    expected = edges.groupby(group).size().rename("expected_child_count")
    observed = merged.groupby(group).size().rename("observed_child_count")
    completeness = expected.to_frame().join(observed, how="inner").reset_index()
    incomplete_groups = completeness.loc[
        completeness["expected_child_count"].ne(completeness["observed_child_count"])
    ]
    invalid_groups = merged.loc[~valid, group].drop_duplicates()
    child_split_sum = merged.groupby(group)["spilit_sale_article_amt"].transform("sum")
    child_split_complete = merged["spilit_sale_article_amt"].gt(0.000001).groupby(
        [merged[column] for column in group]
    ).transform("all")
    amount_conflict_groups = merged.loc[
        child_split_complete
        & child_split_sum.sub(merged["source_amount"]).abs().gt(0.01),
        group,
    ].drop_duplicates()
    merged["source_common_qty"] = merged["source_qty"]
    merged["target_common_qty"] = (
        merged["target_qty"] * merged["bom_source_qty_per_target_qty"]
    )
    common_target_sum = merged.groupby(group)["target_common_qty"].transform("sum")
    merged["parent_unit_child_qty_sum"] = common_target_sum
    merged["quantity_balance_residual"] = (
        merged["source_qty"] - common_target_sum
    )
    quantity_conflict_groups = merged.loc[
        common_target_sum.sub(merged["source_qty"]).abs().gt(
            BOM_QUANTITY_TOLERANCE
        ),
        group,
    ].drop_duplicates()
    blocked = pd.concat(
        [
            incomplete_groups[group], invalid_groups, unexpected_groups,
            amount_conflict_groups, quantity_conflict_groups, invalid_self_only,
        ], ignore_index=True
    ).drop_duplicates()
    quarantine = blocked.assign(
        article_id=blocked["source_article_id"],
        reason_code="BOM_QUANTITY_EVIDENCE_INCOMPLETE",
        detail=(
            "receive_sale 必须同时给出父品保留量和全部正式子品数量；"
            "article_convert 中 ctype=1 的 sub_rate 必须能把子品数量折回本次父品转出量；"
            "子品金额齐全时，其合计必须等于本次父品转出金额，否则整组不执行"
        ),
    )
    if not blocked.empty:
        merged = merged.merge(
            blocked.assign(_blocked=True), on=group, how="left", validate="many_to_one"
        )
        merged = merged.loc[merged["_blocked"].isna()].drop(columns="_blocked")
    merged = merged.loc[merged["_valid"]].drop(columns="_valid").copy()
    if merged.empty:
        return _empty(columns), quarantine
    merged["event_group_id"] = (
        "BOM|" + merged["store_id"] + "|" + merged["business_date"] + "|"
        + merged["source_article_id"]
    )
    split_sum = merged.groupby(group)["spilit_sale_article_amt"].transform("sum")
    split_complete = merged["spilit_sale_article_amt"].gt(0.000001).groupby(
        [merged[column] for column in group]
    ).transform("all")
    weight = merged["cost_rate"].where(merged["cost_rate"].gt(0), merged["dressing_rate"])
    weight_sum = weight.groupby([merged[column] for column in group]).transform("sum")
    use_split = split_complete & split_sum.gt(0.000001)
    if ((~use_split) & weight_sum.le(0.000001)).any():
        bad = merged.loc[
            (~use_split) & weight_sum.le(0.000001), group
        ].drop_duplicates().head(20).to_dict("records")
        raise ValueError(f"BOM allocation lacks child amount and official weight: {bad}")
    merged["amount_allocation_ratio"] = 0.0
    merged.loc[use_split, "amount_allocation_ratio"] = (
        merged.loc[use_split, "spilit_sale_article_amt"]
        / split_sum.loc[use_split]
    )
    merged.loc[~use_split, "amount_allocation_ratio"] = (
        weight.loc[~use_split] / weight_sum.loc[~use_split]
    )
    merged["amount_allocation_basis"] = np.where(
        use_split,
        "RECEIVE_SALE_CHILD_SPLIT_AMOUNT",
        "OFFICIAL_BOM_COST_OR_DRESSING_WEIGHT",
    )
    merged["inbound_parent_qty"] = merged["inbound_qty_max"]
    merged["inbound_parent_amt"] = merged["inbound_amt_max"]
    merged["receive_sale_child_amt"] = merged["spilit_sale_article_amt"]
    merged["target_allocated_amt"] = (
        merged["source_amount"] * merged["amount_allocation_ratio"]
    )
    allocated_sum = merged.groupby(group)["target_allocated_amt"].transform("sum")
    merged["amount_balance_residual"] = merged["source_amount"] - allocated_sum
    merged["quantity_source"] = (
        "RECEIVE_SALE_CHILD_QTY_TIMES_ARTICLE_CONVERT_CTYPE1_SUB_RATE"
    )
    return merged[list(columns)], quarantine

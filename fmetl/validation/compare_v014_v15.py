"""Read-only current-engine versus v1.5 comparison for one local shadow window."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import duckdb
import numpy as np
import pandas as pd

from fmetl.connectors import QdmApi
from fmetl.contracts.v014 import V15_COMPATIBLE_FIELDS


REFERENCE_TABLE = "default_catalog.ads_business_analysis.strategy_fm_levels_result_v1_5"
REFERENCE_STORE_NAME = "广州滨江宏岸店"
LEVELS = ("门店", "大分类")
EXACT_FACT_METRICS = (
    "total_sale_amount", "total_sale_qty", "inbound_amount", "inbound_qty",
)
SKU_SALES_EXACT_METRICS = ("total_sale_amount", "total_sale_qty")
STORE_INBOUND_EXACT_METRICS = ("inbound_amount",)
PROFIT_GATE_METRICS = (
    "store_profit_amount", "full_link_profit_amount", "supply_chain_profit_amount",
)
DIAGNOSTIC_METRICS = (
    "loss_amount", "store_know_lost_amt", "store_unknow_lost_amt",
    "initial_inventory_amount", "ending_inventory_amount", "init_stock_qty", "end_stock_qty",
)
REFERENCE_COLUMNS = tuple(field.name for field in V15_COMPATIBLE_FIELDS)
TEXT_FIELDS = {
    field.name for field in V15_COMPATIBLE_FIELDS
    if field.duckdb_type.upper().startswith("VARCHAR")
}
FIELD_KEYS = (
    "business_date", "level_description", "day_clear", "sku_id",
    "category_name", "category_level1_description",
    "category_level2_description", "category_level3_description",
)


@dataclass(frozen=True)
class V014V15Comparison:
    reference: pd.DataFrame
    field_matrix: pd.DataFrame
    exact_facts: pd.DataFrame
    weekly_profit: pd.DataFrame
    daily_profit: pd.DataFrame
    reported_category_weekly_profit: pd.DataFrame
    reported_category_daily_profit: pd.DataFrame
    sku_profit: pd.DataFrame
    category_alignment: pd.DataFrame
    v15_parent_category_bridge: pd.DataFrame
    summary: pd.DataFrame


def _sql_list(values: Iterable[str]) -> str:
    clean = [str(value).replace("'", "''") for value in values]
    return ",".join(f"'{value}'" for value in clean)


def fetch_v15_reference(
    api: QdmApi,
    *,
    days: Iterable[str],
    store_name: str = REFERENCE_STORE_NAME,
) -> pd.DataFrame:
    """Fetch current v1.5 result rows for read-only local-engine diagnostics."""
    parts: list[pd.DataFrame] = []
    columns = ",".join(REFERENCE_COLUMNS)
    for day in tuple(map(str, days)):
        for predicate in (
            "level_description IN ('门店','大分类')",
            "level_description='sku'",
        ):
            frame = api.query(
                f"SELECT {columns} FROM {REFERENCE_TABLE} "
                f"WHERE business_date='{day}' AND store_name='{store_name}' "
                f"AND day_clear='2' AND {predicate}"
            )
            if not frame.empty:
                parts.append(frame)
    source_columns = set().union(*(set(frame.columns) for frame in parts))
    # The API can return contract columns that are entirely NULL. Drop them
    # only during concatenation to avoid pandas' deprecated all-NA dtype
    # inference, then restore the exact 123-field reference contract below.
    reference = (
        pd.concat(
            [frame.dropna(axis=1, how="all") for frame in parts],
            ignore_index=True,
        )
        if parts else pd.DataFrame()
    )
    reference = reference.reindex(columns=REFERENCE_COLUMNS)
    reference.attrs["source_columns"] = sorted(source_columns)
    requested = set(map(str, days))
    covered = set(reference["business_date"].astype(str)) if not reference.empty else set()
    if covered != requested:
        raise RuntimeError(
            f"v1.5 reference date coverage mismatch missing={sorted(requested-covered)} "
            f"extra={sorted(covered-requested)}"
        )
    keys = ["business_date", "level_description", "sku_id", "category_level1_description"]
    if reference[keys].fillna("").duplicated().any():
        raise RuntimeError("v1.5 reference contains duplicate comparison rows")
    return reference


def load_v014_result(path: Path | str) -> pd.DataFrame:
    conn = duckdb.connect(str(Path(path).resolve()), read_only=True)
    try:
        return conn.execute(
            "SELECT * FROM t_v014_levels_result "
            "WHERE day_clear='2' AND level_description IN ('门店','大分类','sku')"
        ).df()
    finally:
        conn.close()


def load_v014_relation_articles(path: Path | str) -> set[str]:
    conn = duckdb.connect(str(Path(path).resolve()), read_only=True)
    try:
        return {
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT article_id FROM v014_internal_posting"
            ).fetchall()
        }
    finally:
        conn.close()


def load_v014_relation_category_effects(path: Path | str) -> pd.DataFrame:
    conn = duckdb.connect(str(Path(path).resolve()), read_only=True)
    try:
        return conn.execute(
            """
            WITH categories AS (
                SELECT DISTINCT sku_id AS article_id,
                       category_level1_description
                FROM t_v014_levels_result
                WHERE level_description='sku' AND day_clear='2'
            )
            SELECT c.category_level1_description,
                   SUM(
                       CASE WHEN p.posting_role='OUT' THEN p.amt ELSE -p.amt END
                   ) AS expected_internal_relation_delta
            FROM v014_internal_posting p
            INNER JOIN categories c USING (article_id)
            GROUP BY c.category_level1_description
            """
        ).df()
    finally:
        conn.close()


def _number(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    out = frame.copy()
    for column in columns:
        out[column] = pd.to_numeric(out[column], errors="raise").fillna(0.0)
    return out


def _relative_diff(new: pd.Series, old: pd.Series) -> pd.Series:
    denominator = old.abs()
    return pd.Series(
        np.where(denominator.gt(0.01), (new - old) / denominator, np.where((new-old).abs().le(0.01), 0.0, np.nan)),
        index=new.index,
    )


def _long_metric_compare(
    new: pd.DataFrame,
    old: pd.DataFrame,
    *,
    keys: list[str],
    metrics: Iterable[str],
    aggregate_week: bool,
) -> pd.DataFrame:
    metric_list = list(metrics)
    new_work = _number(new[keys + metric_list], metric_list)
    old_work = _number(old[keys + metric_list], metric_list)
    if aggregate_week:
        group = [key for key in keys if key != "business_date"]
        new_work = new_work.groupby(group, as_index=False, dropna=False)[metric_list].sum()
        old_work = old_work.groupby(group, as_index=False, dropna=False)[metric_list].sum()
        keys = group
    merged = new_work.merge(old_work, on=keys, how="outer", suffixes=("_v014", "_v15"), indicator=True)
    rows: list[pd.DataFrame] = []
    for metric in metric_list:
        left = f"{metric}_v014"
        right = f"{metric}_v15"
        part = merged[keys + ["_merge", left, right]].copy()
        part[[left, right]] = part[[left, right]].fillna(0.0)
        part["metric"] = metric
        part["v014_value"] = part.pop(left)
        part["v15_value"] = part.pop(right)
        part["diff_amount"] = part["v014_value"] - part["v15_value"]
        part["diff_pct"] = _relative_diff(part["v014_value"], part["v15_value"])
        rows.append(part)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _normalized_category_levels(
    new_sku: pd.DataFrame,
    old_sku: pd.DataFrame,
    old_parent: pd.DataFrame,
    *,
    metrics: Iterable[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Reaggregate both versions with the local output SKU mapping.

    This view isolates ledger and relation deltas. It cannot prove that the
    v1.5 output used the same classification source.
    """
    metric_list = list(metrics)
    keys = ["business_date", "sku_id"]
    mapping = new_sku[keys + ["category_level1_description"]].drop_duplicates(keys)
    if mapping.duplicated(keys).any():
        raise ValueError("local SKU category mapping must be unique per business day")
    mapped_old = old_sku.drop(
        columns=["category_level1_description"], errors="ignore"
    ).merge(mapping, on=keys, how="left", validate="many_to_one")
    mapped_old["category_level1_description"] = mapped_old[
        "category_level1_description"
    ].fillna("__UNMAPPED__")
    mapped_new = new_sku.copy()
    mapped_new["category_level1_description"] = mapped_new[
        "category_level1_description"
    ].fillna("__UNMAPPED__")

    group = ["business_date", "category_level1_description"]
    new_category = _number(mapped_new[group + metric_list], metric_list).groupby(
        group, as_index=False, dropna=False
    )[metric_list].sum()
    old_category = _number(mapped_old[group + metric_list], metric_list).groupby(
        group, as_index=False, dropna=False
    )[metric_list].sum()
    for frame in (new_category, old_category):
        frame["level_description"] = "大分类"

    parent = _number(
        old_parent[group + metric_list], metric_list
    ).groupby(group, as_index=False, dropna=False)[metric_list].sum()
    bridge_rows: list[pd.DataFrame] = []
    merged = parent.merge(
        old_category,
        on=group,
        how="outer",
        suffixes=("_parent", "_sku_reaggregated"),
        indicator=True,
    )
    for metric in metric_list:
        part = merged[group + ["_merge", f"{metric}_parent", f"{metric}_sku_reaggregated"]].copy()
        part[[f"{metric}_parent", f"{metric}_sku_reaggregated"]] = part[[
            f"{metric}_parent", f"{metric}_sku_reaggregated"
        ]].fillna(0.0)
        part["metric"] = metric
        part["v15_parent_value"] = part.pop(f"{metric}_parent")
        part["v15_sku_reaggregated_value"] = part.pop(
            f"{metric}_sku_reaggregated"
        )
        part["parent_adjustment_bridge"] = (
            part["v15_parent_value"] - part["v15_sku_reaggregated_value"]
        )
        bridge_rows.append(part)
    bridge = pd.concat(bridge_rows, ignore_index=True) if bridge_rows else pd.DataFrame()
    return new_category, old_category, bridge


def _reported_category_levels(
    frame: pd.DataFrame,
    *,
    metrics: Iterable[str],
) -> pd.DataFrame:
    """Aggregate SKU rows using the category label reported by that version."""
    metric_list = list(metrics)
    group = ["business_date", "category_level1_description"]
    out = _number(frame[group + metric_list], metric_list).groupby(
        group, as_index=False, dropna=False
    )[metric_list].sum()
    out["level_description"] = "大分类"
    return out


def _full_field_matrix(new: pd.DataFrame, old: pd.DataFrame) -> pd.DataFrame:
    """Audit all 123 compatibility fields without treating v1.5 as a source.

    The matrix is diagnostic: additive and ratio fields have different
    aggregation semantics, so hard business gates remain explicit below.
    """
    merged = new[list(REFERENCE_COLUMNS)].merge(
        old[list(REFERENCE_COLUMNS)],
        on=list(FIELD_KEYS),
        how="outer",
        suffixes=("_v014", "_v15"),
        indicator=True,
    )
    rows: list[dict[str, object]] = []
    for field in V15_COMPATIBLE_FIELDS:
        name = field.name
        if name not in FIELD_KEYS and old[name].isna().all():
            rows.append({
                "field_name": name,
                "duckdb_type": field.duckdb_type,
                "comparison_mode": "REFERENCE_UNAVAILABLE",
                "matched_rows": 0,
                "different_rows": 0,
                "v014_value": np.nan,
                "v15_value": np.nan,
                "diff_amount": np.nan,
                "diff_pct": np.nan,
                "max_abs_row_diff": np.nan,
                "status": "REFERENCE_UNAVAILABLE",
            })
            continue
        if name in FIELD_KEYS:
            rows.append({
                "field_name": name,
                "duckdb_type": field.duckdb_type,
                "comparison_mode": "ROW_KEY",
                "matched_rows": int(merged["_merge"].eq("both").sum()),
                "different_rows": int(merged["_merge"].ne("both").sum()),
                "v014_value": np.nan,
                "v15_value": np.nan,
                "diff_amount": np.nan,
                "diff_pct": np.nan,
                "max_abs_row_diff": np.nan,
                "status": "PRESENT",
            })
            continue
        left = f"{name}_v014"
        right = f"{name}_v15"
        both = merged["_merge"].eq("both")
        if name in TEXT_FIELDS:
            equal = (
                merged.loc[both, left].fillna("").astype(str)
                .eq(merged.loc[both, right].fillna("").astype(str))
            )
            rows.append({
                "field_name": name,
                "duckdb_type": field.duckdb_type,
                "comparison_mode": "ROW_TEXT",
                "matched_rows": int(equal.sum()),
                "different_rows": int((~equal).sum() + (~both).sum()),
                "v014_value": np.nan,
                "v15_value": np.nan,
                "diff_amount": np.nan,
                "diff_pct": np.nan,
                "max_abs_row_diff": np.nan,
                "status": "MATCH" if bool(equal.all()) and bool(both.all()) else "DIAGNOSTIC_DIFF",
            })
            continue
        v014_values = pd.to_numeric(merged[left], errors="coerce").fillna(0.0)
        v15_values = pd.to_numeric(merged[right], errors="coerce").fillna(0.0)
        v014_total = float(v014_values.sum())
        v15_total = float(v15_values.sum())
        diff = v014_total - v15_total
        diff_pct = (
            diff / abs(v15_total)
            if abs(v15_total) > 0.01
            else (0.0 if abs(diff) <= 0.01 else np.nan)
        )
        rows.append({
            "field_name": name,
            "duckdb_type": field.duckdb_type,
            "comparison_mode": "NUMERIC_DIAGNOSTIC",
            "matched_rows": int(both.sum()),
            "different_rows": int((~both).sum()),
            "v014_value": v014_total,
            "v15_value": v15_total,
            "diff_amount": diff,
            "diff_pct": diff_pct,
            "max_abs_row_diff": float((v014_values - v15_values).abs().max()),
            "status": "MATCH" if bool(both.all()) and abs(diff) <= 0.01 else "DIAGNOSTIC_DIFF",
        })
    return pd.DataFrame(rows)


def compare_v014_to_v15(
    v014: pd.DataFrame,
    v15: pd.DataFrame,
    *,
    profit_tolerance: float = 0.02,
    money_tolerance: float = 0.01,
    quantity_tolerance: float = 1e-6,
    relation_article_ids: Iterable[str] = (),
    relation_category_effects: pd.DataFrame | None = None,
) -> V014V15Comparison:
    required = set(REFERENCE_COLUMNS)
    for label, frame in (("local engine", v014), ("v1.5", v15)):
        missing = sorted(required - set(frame.columns))
        if missing:
            raise KeyError(f"{label} comparison input missing columns: {missing}")
    new = v014.loc[v014["day_clear"].astype(str).eq("2")].copy()
    old = v15.loc[v15["day_clear"].astype(str).eq("2")].copy()
    field_matrix = _full_field_matrix(new, old)

    sku_keys = ["business_date", "sku_id"]
    new_sku = new.loc[new["level_description"].eq("sku")]
    old_sku = old.loc[old["level_description"].eq("sku")]
    sku_sales_exact = _long_metric_compare(
        new_sku, old_sku, keys=sku_keys,
        metrics=SKU_SALES_EXACT_METRICS, aggregate_week=False,
    )
    sku_sales_exact["comparison_scope"] = "SKU_SALES"
    new_store_inbound = new_sku.groupby("business_date", as_index=False)[
        list(STORE_INBOUND_EXACT_METRICS)
    ].sum()
    old_store_inbound = old_sku.groupby("business_date", as_index=False)[
        list(STORE_INBOUND_EXACT_METRICS)
    ].sum()
    store_inbound_exact = _long_metric_compare(
        new_store_inbound, old_store_inbound, keys=["business_date"],
        metrics=STORE_INBOUND_EXACT_METRICS, aggregate_week=False,
    )
    store_inbound_exact["sku_id"] = "__STORE_TOTAL__"
    store_inbound_exact["comparison_scope"] = "STORE_DAY_INBOUND_AMOUNT"
    exact = pd.concat(
        [sku_sales_exact, store_inbound_exact.reindex(columns=sku_sales_exact.columns)],
        ignore_index=True,
    )
    exact["tolerance"] = exact["metric"].map(
        lambda metric: money_tolerance if metric.endswith("amount") else quantity_tolerance
    )
    exact["status"] = np.where(
        exact["diff_amount"].abs().le(exact["tolerance"]), "PASS",
        np.where(exact["_merge"].ne("both"), "MISSING_SIDE", "FAIL"),
    )

    level_keys = ["business_date", "level_description", "category_level1_description"]
    comparison_metrics = (*PROFIT_GATE_METRICS, *DIAGNOSTIC_METRICS)
    new_category, old_category, parent_bridge = _normalized_category_levels(
        new_sku,
        old_sku,
        old.loc[old["level_description"].eq("大分类")],
        metrics=comparison_metrics,
    )
    new_levels = pd.concat([
        new.loc[new["level_description"].eq("门店")][
            level_keys + list(comparison_metrics)
        ],
        new_category[level_keys + list(comparison_metrics)],
    ], ignore_index=True)
    old_levels = pd.concat([
        old.loc[old["level_description"].eq("门店")][
            level_keys + list(comparison_metrics)
        ],
        old_category[level_keys + list(comparison_metrics)],
    ], ignore_index=True)
    daily_profit = _long_metric_compare(
        new_levels, old_levels, keys=level_keys,
        metrics=comparison_metrics, aggregate_week=False,
    )
    daily_profit["comparison_basis"] = np.where(
        daily_profit["level_description"].eq("门店"),
        "STORE_TOTAL",
        "CURRENT_ENGINE_CATEGORY_NORMALIZED",
    )
    daily_profit["is_gate_metric"] = (
        daily_profit["level_description"].eq("门店")
        & daily_profit["metric"].isin(PROFIT_GATE_METRICS)
    )
    daily_profit["status"] = np.where(
        daily_profit["is_gate_metric"]
        & daily_profit["diff_pct"].abs().le(profit_tolerance),
        "WITHIN_TOLERANCE",
        "DIAGNOSTIC",
    )
    weekly_profit = _long_metric_compare(
        new_levels, old_levels, keys=level_keys,
        metrics=comparison_metrics, aggregate_week=True,
    )
    weekly_profit["comparison_basis"] = np.where(
        weekly_profit["level_description"].eq("门店"),
        "STORE_TOTAL",
        "CURRENT_ENGINE_CATEGORY_NORMALIZED",
    )
    weekly_profit["is_gate_metric"] = (
        weekly_profit["level_description"].eq("门店")
        & weekly_profit["metric"].isin(PROFIT_GATE_METRICS)
    )
    relation_ids = set(map(str, relation_article_ids))
    expected_relation = pd.DataFrame(
        columns=[
            "level_description", "category_level1_description", "metric",
            "expected_relation_delta",
        ]
    )
    if relation_ids:
        relation_new = new_sku.loc[new_sku["sku_id"].astype(str).isin(relation_ids)]
        relation_mapping = new_sku[
            ["business_date", "sku_id", "category_level1_description"]
        ].drop_duplicates(["business_date", "sku_id"])
        relation_old = old_sku.loc[
            old_sku["sku_id"].astype(str).isin(relation_ids)
        ].drop(columns=["category_level1_description"], errors="ignore").merge(
            relation_mapping,
            on=["business_date", "sku_id"],
            how="left",
            validate="many_to_one",
        )
        expected_relation = _long_metric_compare(
            relation_new,
            relation_old,
            keys=[
                "business_date", "category_level1_description",
            ],
            metrics=PROFIT_GATE_METRICS,
            aggregate_week=True,
        ).rename(columns={"diff_amount": "expected_relation_delta"})
        expected_relation["level_description"] = "大分类"
        expected_relation = expected_relation[
            [
                "level_description", "category_level1_description", "metric",
                "expected_relation_delta",
            ]
        ]
    weekly_profit = weekly_profit.merge(
        expected_relation,
        on=["level_description", "category_level1_description", "metric"],
        how="left",
        validate="one_to_one",
    )
    weekly_profit["expected_relation_delta"] = pd.to_numeric(
        weekly_profit["expected_relation_delta"], errors="coerce"
    ).fillna(0.0)
    if relation_category_effects is not None and not relation_category_effects.empty:
        required_effect = {
            "category_level1_description",
            "expected_internal_relation_delta",
        }
        missing_effect = sorted(required_effect - set(relation_category_effects.columns))
        if missing_effect:
            raise KeyError(
                f"relation category effects missing columns: {missing_effect}"
            )
        effects = relation_category_effects[
            list(required_effect)
        ].drop_duplicates("category_level1_description")
        weekly_profit = weekly_profit.merge(
            effects,
            on="category_level1_description",
            how="left",
            validate="many_to_one",
        )
    else:
        weekly_profit["expected_internal_relation_delta"] = 0.0
    weekly_profit["expected_internal_relation_delta"] = pd.to_numeric(
        weekly_profit["expected_internal_relation_delta"], errors="coerce"
    ).fillna(0.0)
    matches_relation_sku_delta = (
        weekly_profit["diff_amount"]
        .sub(weekly_profit["expected_relation_delta"])
        .abs()
        .le(money_tolerance)
        & weekly_profit["expected_relation_delta"].abs().gt(money_tolerance)
    )
    matches_internal_category_effect = (
        weekly_profit["diff_amount"]
        .sub(weekly_profit["expected_internal_relation_delta"])
        .abs()
        .le(money_tolerance)
        & weekly_profit["expected_internal_relation_delta"].abs().gt(
            money_tolerance
        )
    )
    relation_explains_diff = (
        weekly_profit["level_description"].eq("大分类")
        & weekly_profit["metric"].isin(
            {"store_profit_amount", "full_link_profit_amount"}
        )
        & (matches_relation_sku_delta | matches_internal_category_effect)
    )
    empty_category = (
        weekly_profit["_merge"].ne("both")
        & weekly_profit["v014_value"].abs().le(money_tolerance)
        & weekly_profit["v15_value"].abs().le(money_tolerance)
    )
    weekly_profit["status"] = np.where(
        ~weekly_profit["is_gate_metric"],
        "DIAGNOSTIC",
        np.where(
            empty_category,
            "EMPTY_CATEGORY",
            np.where(
                weekly_profit["diff_pct"].abs().le(profit_tolerance),
                "PASS",
                np.where(
                    relation_explains_diff,
                    "EXPECTED_RELATION_DELTA",
                    np.where(
                        weekly_profit["_merge"].ne("both"),
                        "MISSING_SIDE",
                        "FAIL",
                    ),
                ),
            ),
        ),
    )
    sku_profit = _long_metric_compare(
        new_sku, old_sku, keys=sku_keys, metrics=("store_profit_amount",), aggregate_week=True,
    ).sort_values("diff_amount", key=lambda values: values.abs(), ascending=False)
    sku_profit["status"] = "LOCATE_ONLY"

    # Category parity is meaningful only for selling SKUs. Receipt-only source
    # codes are intentionally reallocated to a target sales code by v1.5, while
    # The local engine preserves the external receipt on the observed source code and posts
    # an explicit internal event. Treating inbound-only codes as active creates
    # false category mismatches.
    active_columns = list(SKU_SALES_EXACT_METRICS)
    new_active = new_sku.loc[
        _number(new_sku[active_columns], active_columns).abs().sum(axis=1).gt(quantity_tolerance)
    ]
    old_active = old_sku.loc[
        _number(old_sku[active_columns], active_columns).abs().sum(axis=1).gt(quantity_tolerance)
    ]
    category_alignment = new_active[sku_keys + ["category_level1_description"]].merge(
        old_active[sku_keys + ["category_level1_description"]],
        on=sku_keys, how="outer", suffixes=("_v014", "_v15"), indicator=True,
    )
    category_alignment["status"] = np.where(
        category_alignment["_merge"].ne("both"), "REFERENCE_SKU_MISSING",
        np.where(
            category_alignment["category_level1_description_v014"].fillna("").eq(
                category_alignment["category_level1_description_v15"].fillna("")
            ), "PASS", "REFERENCE_LABEL_DIFF",
        ),
    )
    category_diagnostics = int(category_alignment["status"].ne("PASS").sum())
    reported_new = _reported_category_levels(new_sku, metrics=comparison_metrics)
    reported_old = _reported_category_levels(old_sku, metrics=comparison_metrics)
    reported_category_daily = _long_metric_compare(
        reported_new,
        reported_old,
        keys=level_keys,
        metrics=comparison_metrics,
        aggregate_week=False,
    )
    reported_category_weekly = _long_metric_compare(
        reported_new,
        reported_old,
        keys=level_keys,
        metrics=comparison_metrics,
        aggregate_week=True,
    )
    for frame in (reported_category_daily, reported_category_weekly):
        frame["comparison_basis"] = "EACH_VERSION_REPORTED_CATEGORY"
        frame["is_gate_metric"] = False
        frame["status"] = "DIAGNOSTIC"
    reported_category_diagnostics = int(
        reported_category_weekly["diff_amount"].abs().gt(money_tolerance).sum()
    )
    summary = pd.DataFrame([
        {
            "check": "v15_sku_sales_parity",
            "failed_rows": int(
                exact.loc[exact["comparison_scope"].eq("SKU_SALES"), "status"]
                .ne("PASS")
                .sum()
            ),
            "diagnostic_rows": 0,
        },
        {
            "check": "v15_store_day_inbound_parity",
            "failed_rows": int(
                exact.loc[
                    exact["comparison_scope"].eq("STORE_DAY_INBOUND_AMOUNT"),
                    "status",
                ].ne("PASS").sum()
            ),
            "diagnostic_rows": 0,
        },
        {
            "check": "weekly_store_profit_within_2pct",
            "failed_rows": int(
                weekly_profit.loc[weekly_profit["is_gate_metric"], "status"]
                .isin({"FAIL", "MISSING_SIDE"})
                .sum()
            ),
            "diagnostic_rows": 0,
        },
        {
            "check": "normalized_category_profit_diagnostic",
            "failed_rows": 0,
            "diagnostic_rows": int(
                weekly_profit.loc[
                    weekly_profit["level_description"].eq("大分类"),
                    "diff_amount",
                ].abs().gt(money_tolerance).sum()
            ),
        },
        {
            "check": "selling_sku_category_alignment",
            "failed_rows": 0,
            "diagnostic_rows": category_diagnostics,
        },
        {
            "check": "reported_category_profit_diagnostic",
            "failed_rows": 0,
            "diagnostic_rows": reported_category_diagnostics,
        },
    ])
    summary["status"] = np.where(
        summary["failed_rows"].gt(0),
        "FAIL",
        np.where(summary["diagnostic_rows"].gt(0), "DIAGNOSTIC", "PASS"),
    )
    return V014V15Comparison(
        reference=old, field_matrix=field_matrix,
        exact_facts=exact, weekly_profit=weekly_profit,
        daily_profit=daily_profit,
        reported_category_weekly_profit=reported_category_weekly,
        reported_category_daily_profit=reported_category_daily,
        sku_profit=sku_profit,
        category_alignment=category_alignment,
        v15_parent_category_bridge=parent_bridge,
        summary=summary,
    )


def persist_comparison(path: Path | str, result: V014V15Comparison) -> Path:
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(target))
    try:
        for name, frame in (
            ("reference_v15_levels_result", result.reference),
            ("comparison_field_matrix", result.field_matrix),
            ("comparison_exact_facts", result.exact_facts),
            ("comparison_weekly_profit", result.weekly_profit),
            ("comparison_daily_profit", result.daily_profit),
            (
                "comparison_reported_category_weekly_profit",
                result.reported_category_weekly_profit,
            ),
            (
                "comparison_reported_category_daily_profit",
                result.reported_category_daily_profit,
            ),
            ("comparison_sku_profit", result.sku_profit),
            ("comparison_category_alignment", result.category_alignment),
            ("comparison_v15_parent_category_bridge", result.v15_parent_category_bridge),
            ("comparison_summary", result.summary),
        ):
            conn.register("_frame", frame)
            conn.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _frame")
            conn.unregister("_frame")
    finally:
        conn.close()
    return target

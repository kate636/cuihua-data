from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping

import numpy as np
import pandas as pd

from fmetl.contracts.v014 import RELATION_REGISTRY_COLUMNS, RelationType


class V014RelationError(ValueError):
    pass


def _truthy(value: object) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def freeze_product_group_snapshot(raw: pd.DataFrame) -> pd.DataFrame:
    """Return the shadow-only, date-specific generic product-group mapping.

    `area_name IS NULL` is an explicit v0.18 local-run assumption. The function
    rejects duplicate article/day mappings instead of selecting an arbitrary
    group and never forward-fills a current mapping into historical dates.
    """
    required = {
        "inc_day", "area_name", "article_group_id", "article_group_name", "article_id",
    }
    missing = sorted(required - set(raw.columns))
    if missing:
        raise V014RelationError(f"product group snapshot missing columns: {missing}")
    generic = raw["area_name"].isna()
    frame = (
        raw if bool(generic.all())
        else raw.loc[generic]
    ).rename(columns={"inc_day": "business_date"}, copy=False)
    keys = ["business_date", "article_id"]
    if frame[keys + ["article_group_id"]].isna().any().any():
        raise V014RelationError("product group snapshot keys cannot contain NULL")
    frame[keys + ["article_group_id", "article_group_name"]] = frame[
        keys + ["article_group_id", "article_group_name"]
    ].astype(str)
    duplicate_keys = frame.duplicated(keys, keep=False)
    if duplicate_keys.any():
        conflicts = (
            frame.loc[duplicate_keys]
            .groupby(keys, dropna=False)["article_group_id"]
            .nunique()
            .gt(1)
        )
        if conflicts.any():
            bad = conflicts[conflicts].index.tolist()[:20]
            raise V014RelationError(f"article maps to multiple groups on one day: {bad}")
    optional = [column for column in ("unit_weight", "sale_unit") if column in frame]
    return frame[
        ["business_date", "article_id", "article_group_id", "article_group_name", *optional]
    ].drop_duplicates(keys, ignore_index=True)


def build_product_group_candidates(
    observed_pairs: pd.DataFrame,
    product_groups: pd.DataFrame,
) -> pd.DataFrame:
    """Annotate observed source/target pairs; group identity alone is not flow."""
    pair_columns = {
        "store_id", "business_date", "source_article_id", "target_article_id",
    }
    missing = sorted(pair_columns - set(observed_pairs.columns))
    if missing:
        raise V014RelationError(f"observed pairs missing columns: {missing}")
    if observed_pairs.empty:
        return pd.DataFrame(columns=[*sorted(pair_columns), "article_group_id"])
    if observed_pairs[list(pair_columns)].isna().any().any():
        raise V014RelationError("observed pair keys cannot contain NULL")
    pairs = observed_pairs.copy()
    pairs[list(pair_columns)] = pairs[list(pair_columns)].astype(str)
    if pairs.duplicated(list(pair_columns)).any():
        raise V014RelationError("observed pairs must be unique per store/date/pair")
    relevant_articles = set(pairs["source_article_id"]) | set(
        pairs["target_article_id"]
    )
    relevant_days = set(pairs["business_date"])
    mapping = product_groups.loc[
        product_groups["article_id"].astype(str).isin(relevant_articles)
        & product_groups["business_date"].astype(str).isin(relevant_days)
    ].copy()
    for side in ("source", "target"):
        pairs = pairs.merge(
            mapping.rename(columns={
                "article_id": f"{side}_article_id",
                "article_group_id": f"{side}_article_group_id",
                "article_group_name": f"{side}_article_group_name",
                "unit_weight": f"{side}_unit_weight",
                "sale_unit": f"{side}_sale_unit",
            }),
            on=["business_date", f"{side}_article_id"],
            how="left",
            validate="many_to_one",
        )
    same_group = (
        pairs["source_article_group_id"].notna()
        & pairs["source_article_group_id"].eq(pairs["target_article_group_id"])
    )
    pairs["same_product_group"] = same_group
    pairs["article_group_id"] = pairs["source_article_group_id"].where(same_group)
    pairs["article_group_name"] = pairs["source_article_group_name"].where(same_group)
    if {"source_unit_weight", "target_unit_weight"}.issubset(pairs.columns):
        source_weight = pd.to_numeric(pairs["source_unit_weight"], errors="coerce")
        target_weight = pd.to_numeric(pairs["target_unit_weight"], errors="coerce")
        source_weight = source_weight.where(
            ~pairs["source_sale_unit"].astype(str).eq("千克"), 1.0
        )
        target_weight = target_weight.where(
            ~pairs["target_sale_unit"].astype(str).eq("千克"), 1.0
        )
        pairs["source_normalized_weight"] = source_weight
        pairs["target_normalized_weight"] = target_weight
        pairs["source_qty_per_target_qty"] = np.where(
            source_weight.gt(0) & target_weight.gt(0),
            target_weight / source_weight,
            np.nan,
        )
        pairs["target_qty_per_source_qty"] = np.where(
            source_weight.gt(0) & target_weight.gt(0),
            source_weight / target_weight,
            np.nan,
        )
        pairs["product_weight_quantity_rate"] = np.where(
            source_weight.gt(0) & target_weight.gt(0),
            source_weight / target_weight,
            np.nan,
        )
        observed_rate = pd.to_numeric(
            pairs.get("external_receipt_quantity_rate"), errors="coerce"
        )
        pairs["receipt_weight_rate_residual"] = (
            observed_rate - pairs["product_weight_quantity_rate"]
        )
    return pairs


def _active_rows(frame: pd.DataFrame | None, day: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    out = frame.copy()
    if "approved" in out:
        out = out.loc[out["approved"].map(_truthy)]
    if "status" in out:
        out = out.loc[out["status"].astype(str).str.upper().isin({"ACTIVE", "APPROVED", "1"})]
    if "effective_from" in out:
        out = out.loc[out["effective_from"].astype(str).le(day)]
    if "effective_to" in out:
        until = out["effective_to"]
        out = out.loc[until.isna() | until.astype(str).ge(day)]
    return out


def _build_pair_lookup(
    frame: pd.DataFrame | None,
    *,
    store_days: Iterable[tuple[str, str]],
    source_col: str,
    target_col: str,
    value_columns: Iterable[str],
) -> dict[tuple[str, str, str, str], dict[str, tuple[object, ...]]]:
    """Index dated relation evidence once instead of rescanning per candidate."""
    if frame is None or frame.empty:
        return {}
    required = {source_col, target_col}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise V014RelationError(f"relation evidence missing columns: {missing}")
    lookup: dict[tuple[str, str, str, str], dict[str, tuple[object, ...]]] = {}
    values = tuple(column for column in value_columns if column in frame.columns)

    def summarize(group: pd.DataFrame) -> dict[str, tuple[object, ...]]:
        return {
            column: tuple(group[column].dropna().drop_duplicates().tolist())
            for column in values
        }
    # Normalized v0.18 stage evidence is already dated. Filter it once instead
    # of copying and regrouping the complete relation frame for every day.
    if {"store_id", "business_date"}.issubset(frame.columns):
        mask = pd.Series(True, index=frame.index)
        if "approved" in frame:
            mask &= frame["approved"].map(_truthy)
        if "status" in frame:
            mask &= frame["status"].astype(str).str.upper().isin(
                {"ACTIVE", "APPROVED", "1"}
            )
        business_day = frame["business_date"].astype(str)
        if "effective_from" in frame:
            mask &= frame["effective_from"].astype(str).le(business_day)
        if "effective_to" in frame:
            until = frame["effective_to"]
            mask &= until.isna() | until.astype(str).ge(business_day)
        active = frame.loc[mask]
        allowed_days = set((str(store), str(day)) for store, day in store_days)
        dated_keys = list(zip(
            active["store_id"].astype(str),
            active["business_date"].astype(str),
        ))
        active = active.loc[[key in allowed_days for key in dated_keys]]
        for key, group in active.groupby(
            ["store_id", "business_date", source_col, target_col],
            sort=False,
            dropna=False,
        ):
            store, day, source, target = map(str, key)
            lookup[(store, day, source, target)] = summarize(group)
        return lookup
    for store, day in store_days:
        active = _active_rows(frame, day)
        if active.empty:
            continue
        if "store_id" in active:
            active = active.loc[active["store_id"].astype(str).eq(store)]
        if "business_date" in active:
            active = active.loc[active["business_date"].astype(str).eq(day)]
        if active.empty:
            continue
        for pair, group in active.groupby([source_col, target_col], sort=False, dropna=False):
            source, target = map(str, pair)
            lookup[(store, day, source, target)] = summarize(group)
    return lookup


def _positive_rate(
    hit: Mapping[str, tuple[object, ...]],
    names: Iterable[str],
    *,
    default: float | None = None,
) -> float | None:
    for name in names:
        if name not in hit:
            continue
        values = pd.to_numeric(pd.Series(hit[name]), errors="coerce").dropna().unique()
        if len(values) == 1 and float(values[0]) > 0:
            return float(values[0])
        if len(values) > 1:
            raise V014RelationError(f"conflicting {name} values for one relation pair")
    return default


def resolve_relation_registry(
    candidates: pd.DataFrame,
    *,
    bom: pd.DataFrame | None = None,
    processing: pd.DataFrame | None = None,
    explicit_convert: pd.DataFrame | None = None,
    product_group_pairs: pd.DataFrame | None = None,
    relation_version: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Choose one relation for each source, target and date.

    The registry records direction and ratio only. Quantity is calculated later
    from the target SKU's remaining demand. If different relation types identify
    the same pair on the same day, the pair is isolated instead of posted.
    """
    required = {"store_id", "business_date", "source_article_id", "target_article_id"}
    missing = sorted(required - set(candidates.columns))
    if missing:
        raise V014RelationError(f"candidates missing columns: {missing}")
    if not relation_version:
        raise V014RelationError("relation_version is required")
    work = candidates.copy()
    if work.empty:
        return pd.DataFrame(columns=RELATION_REGISTRY_COLUMNS), pd.DataFrame()
    keys = ["store_id", "business_date", "source_article_id", "target_article_id"]
    if work[keys].isna().any().any() or work.duplicated(keys).any():
        raise V014RelationError("candidate pair/date keys must be non-null and unique")
    work[keys] = work[keys].astype(str)
    store_days = tuple(
        work[["store_id", "business_date"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    bom_lookup = _build_pair_lookup(
        bom, store_days=store_days,
        source_col="parent_article_id", target_col="sub_article_id",
        value_columns=(
            "category_level1_description", "disassembly_allowed",
            "source_qty_per_target_qty", "target_qty_per_source_qty",
            "cost_rate", "amount_convert_rate",
        ),
    )
    processing_lookup = _build_pair_lookup(
        processing, store_days=store_days,
        source_col="raw_article_id", target_col="finished_article_id",
        value_columns=(
            "raw_qty", "yield_qty", "relation_source",
            "recipe_group_id", "recipe_mode",
        ),
    )
    convert_lookup = _build_pair_lookup(
        explicit_convert, store_days=store_days,
        source_col="source_article_id", target_col="target_article_id",
        value_columns=(
            "actual_event", "fixed_rule", "quantity_rate", "convert_rate",
            "source_qty_per_target_qty", "target_qty_per_source_qty",
            "dressing_rate", "yield_rate", "cost_rate", "amount_convert_rate",
        ),
    )
    pg_lookup: dict[tuple[str, str, str, str], dict[str, float]] = {}
    if product_group_pairs is not None and not product_group_pairs.empty:
        pg = product_group_pairs.loc[product_group_pairs["same_product_group"].map(_truthy)]
        for row in pg.itertuples(index=False):
            key = (
                str(row.store_id), str(row.business_date),
                str(row.source_article_id), str(row.target_article_id),
            )
            pg_lookup[key] = {
                "source_qty_per_target_qty": float(
                    getattr(row, "source_qty_per_target_qty", np.nan)
                ),
                "target_qty_per_source_qty": float(
                    getattr(row, "target_qty_per_source_qty", np.nan)
                ),
            }

    rows: list[dict[str, object]] = []
    quarantine: list[dict[str, object]] = []
    for item in work.itertuples(index=False):
        store, day = str(item.store_id), str(item.business_date)
        source, target = str(item.source_article_id), str(item.target_article_id)
        convert_hit: Mapping[str, tuple[object, ...]] | None = None
        formal: list[
            tuple[RelationType, Mapping[str, tuple[object, ...]], str]
        ] = []
        if source == target:
            formal.append((RelationType.SAME_SKU, {}, "same_sku"))
        else:
            lookup_key = (store, day, source, target)
            bom_hit = bom_lookup.get(lookup_key)
            proc_hit = processing_lookup.get(lookup_key)
            convert_hit = convert_lookup.get(lookup_key)
            if bom_hit is not None:
                formal.append((RelationType.BOM, bom_hit, "official_bom_relation"))
            if proc_hit is not None:
                formal.append((RelationType.PROCESSING, proc_hit, "approved_processing_relation"))
            if convert_hit is not None:
                event = any(
                    _truthy(value)
                    for value in convert_hit.get("actual_event", ())
                )
                fixed = any(
                    _truthy(value)
                    for value in convert_hit.get("fixed_rule", ())
                )
                if event or fixed:
                    formal.append((RelationType.EXPLICIT_CONVERT, convert_hit, "actual_or_fixed_convert"))
            if not formal and lookup_key in pg_lookup:
                formal.append((
                    RelationType.PRODUCT_GROUP_CONVERT,
                    {},
                    "purchase_di_direction_plus_latest_product_weight",
                ))

        pair_key = (store, day, source, target)
        if len(formal) > 1:
            relation_type = RelationType.CONFLICT
            evidence = ",".join(hit[2] for hit in formal)
            quantity_rate = cost_rate = None
            source_per_target = target_per_source = None
            direction_source = ratio_source = "CONFLICT"
            recipe_group_id = recipe_mode = ""
            status = "QUARANTINED"
            quarantine.append({
                "store_id": store, "business_date": day,
                "source_article_id": source, "target_article_id": target,
                "reason_code": "MULTIPLE_FORMAL_RELATION_TYPES", "detail": evidence,
            })
        elif len(formal) == 1:
            relation_type, hit, evidence = formal[0]
            recipe_group_id = recipe_mode = ""
            if relation_type is RelationType.SAME_SKU:
                source_per_target = target_per_source = 1.0
                cost_rate = 1.0
                direction_source = "SAME_ARTICLE_ID"
                ratio_source = "IDENTITY"
            elif relation_type is RelationType.PROCESSING:
                raw_qty = _positive_rate(hit, ("raw_qty",))
                yield_qty = _positive_rate(hit, ("yield_qty",))
                source_per_target = raw_qty / yield_qty if raw_qty and yield_qty else None
                target_per_source = yield_qty / raw_qty if raw_qty and yield_qty else None
                cost_rate = 1.0
                direction_source = "PROCESSING_RELATION_RAW_TO_FINISHED"
                ratio_source = "RAW_QTY_DIVIDED_BY_YIELD_QTY"
                recipe_group_values = tuple(map(str, hit.get("recipe_group_id", ())))
                recipe_mode_values = tuple(map(str, hit.get("recipe_mode", ())))
                recipe_group_id = recipe_group_values[0] if len(set(recipe_group_values)) == 1 else ""
                recipe_mode = recipe_mode_values[0] if len(set(recipe_mode_values)) == 1 else ""
                sources = tuple(map(str, hit.get("relation_source", ())))
                if len(set(sources)) == 1:
                    evidence = f"{evidence}:{sources[0]}"
            elif relation_type is RelationType.EXPLICIT_CONVERT:
                target_per_source = _positive_rate(
                    hit, ("target_qty_per_source_qty", "convert_rate")
                )
                source_per_target = 1.0 / target_per_source if target_per_source else None
                cost_rate = 1.0
                direction_source = "ARTICLE_CONVERT_PARENT_TO_SUB"
                ratio_source = "ONE_DIVIDED_BY_PARENT_RATE"
            elif relation_type is RelationType.PRODUCT_GROUP_CONVERT:
                ratio = pg_lookup[pair_key]
                source_per_target = ratio["source_qty_per_target_qty"]
                target_per_source = ratio["target_qty_per_source_qty"]
                cost_rate = 1.0
                direction_source = "PURCHASE_DI_ARTICLE_TO_SALE_ARTICLE"
                ratio_source = "TARGET_WEIGHT_DIVIDED_BY_SOURCE_WEIGHT"
            else:
                source_per_target = _positive_rate(
                    hit, ("source_qty_per_target_qty",)
                )
                target_per_source = _positive_rate(
                    hit, ("target_qty_per_source_qty",)
                )
                cost_rate = _positive_rate(
                    hit, ("cost_rate", "amount_convert_rate"),
                    default=1.0 if relation_type in {
                        RelationType.BOM, RelationType.EXPLICIT_CONVERT,
                    } else None,
                )
                direction_source = "OFFICIAL_BOM_PARENT_TO_SUB"
                ratio_source = "RECEIVE_SALE_ACTUAL_PARENT_CHILD_QTY"
            quantity_rate = source_per_target
            reciprocal_ok = bool(
                source_per_target and target_per_source
                and abs(source_per_target * target_per_source - 1.0) <= 0.000001
            )
            if relation_type is RelationType.BOM and not reciprocal_ok:
                # The official direction is known, but this date has no
                # receive_sale child quantities from which a unit ratio can be
                # measured.  Keep it in the audit without allowing posting.
                status = "EVIDENCE_ONLY"
                ratio_source = "NO_OBSERVED_RECEIVE_SALE_BOM_EVENT"
            else:
                status = "ACTIVE" if reciprocal_ok and cost_rate else "QUARANTINED"
            if status == "QUARANTINED":
                quarantine.append({
                    "store_id": store, "business_date": day,
                    "source_article_id": source, "target_article_id": target,
                    "reason_code": "RELATION_RATIO_MISSING", "detail": evidence,
                })
        else:
            relation_type = RelationType.UNRESOLVED
            evidence = "none"
            quantity_rate = cost_rate = None
            source_per_target = target_per_source = None
            direction_source = ratio_source = "MISSING"
            recipe_group_id = recipe_mode = ""
            status = "QUARANTINED"
            quarantine.append({
                "store_id": store, "business_date": day,
                "source_article_id": source, "target_article_id": target,
                "reason_code": "UNRESOLVED_RELATION", "detail": "没有正式关系表、有效比例或可核对的数量证据，因此不允许生成内部流",
            })
        digest = hashlib.sha256(
            f"{store}|{day}|{source}|{target}|{relation_type.value}|{relation_version}".encode()
        ).hexdigest()[:20]
        rows.append({
            "store_id": store,
            "business_date": day,
            "source_article_id": source,
            "target_article_id": target,
            "relation_type": relation_type.value,
            "quantity_rate": quantity_rate,
            "source_qty_per_target_qty": source_per_target,
            "target_qty_per_source_qty": target_per_source,
            "cost_rate": cost_rate,
            "posting_mode": (
                "EVIDENCE_ONLY" if status == "EVIDENCE_ONLY"
                else "DIRECT" if relation_type is RelationType.SAME_SKU
                else "INTERNAL_DEMAND_BACKFLUSH"
            ),
            "direction_source": direction_source,
            "ratio_source": ratio_source,
            "recipe_group_id": recipe_group_id,
            "recipe_mode": recipe_mode,
            "effective_from": day,
            "effective_to": day,
            "evidence_source": evidence,
            "relation_version": relation_version,
            "status": status,
            "relation_id": digest,
            "formal_flow_allowed": (
                status == "ACTIVE" and relation_type is not RelationType.SAME_SKU
            ),
        })
    return pd.DataFrame(rows), pd.DataFrame(quarantine)

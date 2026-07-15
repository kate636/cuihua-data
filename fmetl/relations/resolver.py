from __future__ import annotations

from enum import Enum

import pandas as pd


class RelationResolutionError(ValueError):
    pass


class RelationType(str, Enum):
    SELF_RECEIVE = "SELF_RECEIVE"
    DISASSEMBLY_BOM = "DISASSEMBLY_BOM"
    PACK_CONVERT = "PACK_CONVERT"
    RECIPE_COMPOSE = "RECIPE_COMPOSE"
    PROCUREMENT_ALIAS = "PROCUREMENT_ALIAS"
    UNRESOLVED = "UNRESOLVED"
    QUARANTINED = "QUARANTINED"


OUTPUT_COLUMNS = (
    "store_id", "business_date", "from_article_id", "to_article_id", "relation_type",
    "formal_flow_allowed", "resolution_evidence", "resolution_hit_count",
    "relation_snapshot_id",
)

DATED_KEY_COLUMNS = ("store_id", "business_date", "parent_article_id", "sub_article_id")


def _pairs(frame: pd.DataFrame | None, source: str, target: str) -> set[tuple[str, str]]:
    if frame is None or frame.empty:
        return set()
    missing = sorted({source, target} - set(frame.columns))
    if missing:
        raise RelationResolutionError(f"relation evidence missing columns: {missing}")
    values = frame[[source, target]].dropna().astype(str)
    return set(values.itertuples(index=False, name=None))


def _normalize_dated_evidence(frame: pd.DataFrame, *, label: str) -> pd.DataFrame:
    date_column = "inc_day" if "inc_day" in frame.columns else "business_date"
    required = {"store_id", date_column, "parent_article_id", "sub_article_id"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RelationResolutionError(f"{label} missing dated evidence columns: {missing}")
    out = frame.copy().rename(columns={date_column: "business_date"})
    columns = list(DATED_KEY_COLUMNS)
    if out[columns].isna().any().any():
        raise RelationResolutionError(f"{label} dated evidence keys cannot contain NULL")
    out[columns] = out[columns].astype(str)
    return out


def _dated_pairs(frame: pd.DataFrame) -> set[tuple[str, str, str, str]]:
    return set(frame[list(DATED_KEY_COLUMNS)].itertuples(index=False, name=None))


def _approved_mask(series: pd.Series) -> pd.Series:
    return series.map(
        lambda value: value if isinstance(value, bool) else str(value).strip().lower() in {"1", "true", "yes"}
    )


def _convert_evidence(
    article_convert: pd.DataFrame | None,
) -> tuple[set[tuple[str, str, str, str]], set[tuple[str, str, str, str]]]:
    if article_convert is None or article_convert.empty:
        return set(), set()
    required = {"parent_article_id", "sub_article_id", "parent_rate", "sub_rate"}
    missing = sorted(required - set(article_convert.columns))
    if missing:
        raise RelationResolutionError(f"article_convert missing columns: {missing}")
    frame = _normalize_dated_evidence(article_convert, label="article_convert")
    frame["parent_rate"] = pd.to_numeric(frame["parent_rate"], errors="coerce")
    frame["sub_rate"] = pd.to_numeric(frame["sub_rate"], errors="coerce")
    valid_mask = (
        frame["parent_rate"].gt(0)
        & frame["sub_rate"].gt(0)
        & (frame["parent_rate"] * frame["sub_rate"] - 1.0).abs().le(0.001)
    )
    valid = frame.loc[valid_mask].copy()
    # article_convert also carries unit normalization for one-to-many
    # disassembly edges. Only a genuinely one-to-one pair is a PACK_CONVERT;
    # otherwise BOM evidence must remain authoritative for the flow type.
    parent_fanout = valid.groupby(
        ["store_id", "business_date", "parent_article_id"]
    )["sub_article_id"].transform("nunique")
    child_fanin = valid.groupby(
        ["store_id", "business_date", "sub_article_id"]
    )["parent_article_id"].transform("nunique")
    one_to_one = (
        parent_fanout.eq(1) & child_fanin.eq(1)
    )
    valid_pairs = _dated_pairs(valid.loc[one_to_one])
    invalid_pairs = _dated_pairs(frame.loc[~valid_mask])
    return valid_pairs, invalid_pairs


def _eligible_bom_pairs(bom_edges: pd.DataFrame | None) -> set[tuple[str, str, str, str]]:
    if bom_edges is None or bom_edges.empty:
        return set()
    required = {"parent_article_id", "sub_article_id"}
    missing = sorted(required - set(bom_edges.columns))
    if missing:
        raise RelationResolutionError(f"bom_edges missing columns: {missing}")
    frame = _normalize_dated_evidence(bom_edges, label="bom_edges")
    if "disassembly_allowed" in frame:
        eligible = _approved_mask(frame["disassembly_allowed"])
    elif "category_level1_description" in frame:
        eligible = frame["category_level1_description"].astype(str).eq("猪肉类")
    else:
        raise RelationResolutionError(
            "bom_edges must declare disassembly_allowed or category_level1_description"
        )
    allowed = frame.loc[eligible].copy()
    if allowed.empty:
        return set()
    fanout = allowed.groupby(
        ["store_id", "business_date", "parent_article_id"]
    )["sub_article_id"].transform("nunique")
    return _dated_pairs(allowed.loc[fanout.ge(2)])


def resolve_relations(
    candidates: pd.DataFrame,
    *,
    relation_snapshot_id: str,
    processing_recipes: pd.DataFrame | None = None,
    article_convert: pd.DataFrame | None = None,
    bom_edges: pd.DataFrame | None = None,
    order_receive: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Resolve every candidate to one type; invalid/conflicting evidence is quarantined."""
    required = {"store_id", "business_date", "from_article_id", "to_article_id"}
    missing = sorted(required - set(candidates.columns))
    if missing:
        raise RelationResolutionError(f"candidates missing columns: {missing}")
    if not relation_snapshot_id:
        raise RelationResolutionError("relation_snapshot_id is required")
    if candidates.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    if candidates[list(required)].isna().any().any():
        raise RelationResolutionError("candidate keys cannot contain NULL")
    candidate_keys = ["store_id", "business_date", "from_article_id", "to_article_id"]
    if candidates.duplicated(candidate_keys).any():
        raise RelationResolutionError("candidate pairs must be unique per store and business date")

    recipe_all = _pairs(processing_recipes, "raw_article_id", "finished_article_id")
    if processing_recipes is not None and not processing_recipes.empty:
        if "approved" not in processing_recipes:
            raise RelationResolutionError("processing recipes must declare approved")
        recipe = _pairs(
            processing_recipes.loc[_approved_mask(processing_recipes["approved"])],
            "raw_article_id", "finished_article_id",
        )
    else:
        recipe = set()
    if processing_recipes is not None and not processing_recipes.empty:
        invalid_recipe = _pairs(
            processing_recipes.loc[~_approved_mask(processing_recipes["approved"])],
            "raw_article_id", "finished_article_id",
        )
    else:
        invalid_recipe = set()
    convert, invalid_convert = _convert_evidence(article_convert)
    bom = _eligible_bom_pairs(bom_edges)
    procurement = _pairs(order_receive, "order_article_id", "re_article_id")

    rows: list[dict[str, object]] = []
    for row in candidates.itertuples(index=False):
        store_id = str(row.store_id)
        business_date = str(row.business_date)
        source, target = str(row.from_article_id), str(row.to_article_id)
        pair = (source, target)
        dated_pair = (store_id, business_date, source, target)
        hits: list[RelationType] = []
        evidence: list[str] = []
        invalid: list[str] = []
        if source == target:
            hits = [RelationType.SELF_RECEIVE]
            evidence = ["same_sku"]
        else:
            if pair in recipe:
                hits.append(RelationType.RECIPE_COMPOSE)
                evidence.append("approved_recipe")
            if dated_pair in convert:
                hits.append(RelationType.PACK_CONVERT)
                evidence.append("reciprocal_article_convert")
            if dated_pair in bom:
                hits.append(RelationType.DISASSEMBLY_BOM)
                evidence.append("eligible_multi_output_bom")
            if pair in procurement:
                hits.append(RelationType.PROCUREMENT_ALIAS)
                evidence.append("order_receive")
            if pair in invalid_recipe:
                invalid.append("unapproved_recipe")
            if dated_pair in invalid_convert and dated_pair not in bom:
                invalid.append("invalid_article_convert_ratio")

        unique_hits = list(dict.fromkeys(hits))
        if invalid or len(unique_hits) > 1:
            relation_type = RelationType.QUARANTINED
        elif len(unique_hits) == 1:
            relation_type = unique_hits[0]
        else:
            relation_type = RelationType.UNRESOLVED
        rows.append({
            **row._asdict(),
            "relation_type": relation_type.value,
            "formal_flow_allowed": relation_type not in {RelationType.UNRESOLVED, RelationType.QUARANTINED},
            "resolution_evidence": ",".join(evidence + invalid) or "none",
            "resolution_hit_count": len(unique_hits),
            "relation_snapshot_id": relation_snapshot_id,
        })
    return pd.DataFrame(rows)

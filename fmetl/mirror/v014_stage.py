from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import duckdb
import numpy as np
import pandas as pd

from fmetl.calculations.special_wastage import build_wastage_trace
from fmetl.facts.shadow_assembly import assemble_dense_activities, build_bootstrap_openings
from fmetl.facts.sku_day import (
    normalize_inventory_counts,
    normalize_known_loss,
    normalize_sales_events,
)
from fmetl.facts.store_receipts import build_store_receipts
from fmetl.mirror.v014_source import MirrorSourceBundle
from fmetl.outputs.levels_result import ADDITIVE_INPUTS


STAGE_TABLE_PREFIX = "v014_stage_"


@dataclass(frozen=True)
class V014StageBundle:
    store_id: str
    days: tuple[str, ...]
    frames: Mapping[str, pd.DataFrame]
    quarantined: pd.DataFrame


def _empty(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _as_text(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    out = frame.copy()
    for column in columns:
        if column in out:
            out[column] = out[column].astype(str)
    return out


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


def _normalize_percent(value: object) -> float:
    number = float(pd.to_numeric(value, errors="raise"))
    return number / 100.0 if number > 1.0 + 1e-9 else number


def _normalize_day_clear(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if text in {"0", "1"}:
        return text
    try:
        number = float(text)
    except ValueError:
        return None
    if number in {0.0, 1.0}:
        return str(int(number))
    return None


def _build_day_clear_frame(
    *,
    store_id: str,
    days: Sequence[str],
    universe: set[str],
    raw_day_clear: pd.DataFrame,
    sales: pd.DataFrame,
    goods: pd.DataFrame,
) -> pd.DataFrame:
    """Resolve one day-clear label for every SKU day.

    The dedicated Hive label is authoritative when present. Sales supplies a
    secondary observation for selling SKUs. Pork BOM children often have
    neither row because they do not sell directly; v1.5 and the current FM
    policy treat those SKUs as day-clear, so the category policy is the final
    evidence-backed fallback before the normal non-day-clear default.
    """
    frame = raw_day_clear.rename(columns={"inc_day": "business_date"}).copy()
    if "business_date" in frame.columns and frame.columns.duplicated().any():
        frame = frame.loc[:, ~frame.columns.duplicated()]
    # strategy_fm_chdj_article_di is a membership list: a row means day-clear
    # even though the Hive mirror does not expose a separate flag column.
    if "day_clear" not in frame:
        frame["day_clear"] = "0"
    explicit: dict[tuple[str, str, str], str] = {}
    for row in frame.itertuples(index=False):
        value = _normalize_day_clear(row.day_clear)
        if value is not None:
            explicit[(str(row.store_id), str(row.business_date), str(row.article_id))] = value

    sale_labels: dict[tuple[str, str, str], str] = {}
    if not sales.empty and "source_sales_day_clear" in sales:
        for row in sales[
            ["store_id", "business_date", "article_id", "source_sales_day_clear"]
        ].drop_duplicates().itertuples(index=False):
            value = _normalize_day_clear(row.source_sales_day_clear)
            if value is not None:
                sale_labels[(str(row.store_id), str(row.business_date), str(row.article_id))] = value

    category = goods[["article_id", "category_level1_description"]].copy()
    category["article_id"] = category["article_id"].astype(str)
    category = category.drop_duplicates("article_id").set_index("article_id")[
        "category_level1_description"
    ].fillna("").astype(str).to_dict()

    rows: list[dict[str, str]] = []
    for day in map(str, days):
        for article in sorted(universe):
            key = (store_id, day, article)
            if key in explicit:
                value = explicit[key]
            elif category.get(article) == "猪肉类":
                value = "0"
            else:
                value = sale_labels.get(key, "1")
            rows.append({
                "store_id": store_id,
                "business_date": day,
                "article_id": article,
                "day_clear": value,
            })
    return pd.DataFrame(rows)


def _processing_stage(
    raw: pd.DataFrame | None,
    *,
    store_id: str,
    days: Sequence[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    columns = (
        "store_id", "business_date", "relation_id", "raw_article_id",
        "finished_article_id", "raw_qty", "raw_unit", "yield_qty", "yield_unit",
        "category_type", "effective_from", "effective_to", "approved",
    )
    if raw is None or raw.empty:
        return _empty(columns), _empty(
            ("store_id", "business_date", "article_id", "reason_code", "detail")
        )
    required = {"raw_article_id", "finished_article_id", "raw_qty", "yield_qty"}
    missing = sorted(required - set(raw.columns))
    if missing:
        raise KeyError(f"processing relation export missing columns: {missing}")
    source = raw.copy()
    source["raw_article_id"] = source["raw_article_id"].astype(str)
    source["finished_article_id"] = source["finished_article_id"].astype(str)
    source["raw_qty"] = pd.to_numeric(source["raw_qty"], errors="raise")
    source["yield_qty"] = pd.to_numeric(source["yield_qty"], errors="raise")
    source["relation_id"] = source.get(
        "relation_id",
        "PROCESSING|" + source["finished_article_id"] + "|" + source["raw_article_id"],
    )
    dated = "effective_from" in source and source["effective_from"].notna().all()
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
            # A current undated export must not rewrite historical recipe truth.
            active = dated and is_approved and str(effective_from) <= day and (
                pd.isna(effective_to) or str(effective_to) >= day
            )
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
            })
            if not active:
                quarantine.append({
                    "store_id": store_id,
                    "business_date": day,
                    "article_id": str(row["finished_article_id"]),
                    "reason_code": "PROCESSING_RELATION_EFFECTIVE_DATE_MISSING",
                    "detail": str(row["relation_id"]),
                })
    return pd.DataFrame(rows, columns=columns), pd.DataFrame(quarantine)


def _split_processing_and_packaging(
    processing: pd.DataFrame,
    goods: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reclassify procurement-confirmed same-SPU unit changes as packaging.

    The processing platform currently contains both recipes and simple
    package/unit conversions.  A source and target sharing a nonblank SPU is
    identity evidence, while the platform's approved dated ratio supplies the
    missing conversion rule.  Such rows are not recipes and therefore must not
    require finished-goods counts or enter processing-output inference.
    """
    convert_columns = (
        "store_id", "business_date", "source_article_id", "target_article_id",
        "effective_from", "effective_to", "actual_event", "fixed_rule",
        "convert_rate", "cost_rate", "approved",
    )
    if processing.empty:
        return processing.copy(), _empty(convert_columns)
    required_goods = {"article_id", "spu_id"}
    missing = sorted(required_goods - set(goods.columns))
    if missing:
        raise KeyError(f"goods snapshot missing packaging identity columns: {missing}")
    identity = goods[["article_id", "spu_id"]].copy()
    identity["article_id"] = identity["article_id"].astype(str)
    identity["spu_id"] = identity["spu_id"].fillna("").astype(str).str.strip()
    identity = identity.drop_duplicates("article_id")
    work = processing.merge(
        identity.rename(columns={
            "article_id": "raw_article_id", "spu_id": "raw_spu_id",
        }),
        on="raw_article_id", how="left", validate="many_to_one",
    ).merge(
        identity.rename(columns={
            "article_id": "finished_article_id", "spu_id": "finished_spu_id",
        }),
        on="finished_article_id", how="left", validate="many_to_one",
    )
    is_packaging = (
        work["raw_spu_id"].fillna("").ne("")
        & work["raw_spu_id"].eq(work["finished_spu_id"])
        & work["raw_article_id"].ne(work["finished_article_id"])
    )
    recipe = work.loc[~is_packaging, processing.columns].reset_index(drop=True)
    pack = work.loc[is_packaging].copy()
    if pack.empty:
        return recipe, _empty(convert_columns)
    raw_qty = pd.to_numeric(pack["raw_qty"], errors="raise")
    yield_qty = pd.to_numeric(pack["yield_qty"], errors="raise")
    pack["convert_rate"] = yield_qty / raw_qty
    pack["source_article_id"] = pack["raw_article_id"]
    pack["target_article_id"] = pack["finished_article_id"]
    pack["actual_event"] = False
    pack["fixed_rule"] = True
    pack["cost_rate"] = 1.0
    return recipe, pack[list(convert_columns)].reset_index(drop=True)


def _bom_stage(raw: pd.DataFrame) -> pd.DataFrame:
    columns = (
        "store_id", "business_date", "parent_article_id", "sub_article_id",
        "effective_from", "effective_to", "category_level1_description",
        "dressing_rate", "cost_rate", "approved",
    )
    if raw.empty:
        return _empty(columns)
    frame = raw.copy().rename(columns={"inc_day": "business_date"})
    frame = _as_text(
        frame,
        ("store_id", "business_date", "parent_article_id", "sub_article_id"),
    )
    frame["dressing_rate"] = frame["dressing_rate"].map(_normalize_percent)
    frame["cost_rate"] = frame["cost_rate"].map(_normalize_percent)
    frame["effective_from"] = frame["business_date"]
    frame["effective_to"] = frame["business_date"]
    frame["approved"] = frame["category_level1_description"].isin({"猪肉类", "牛肉类"})
    return frame[list(columns)]


def _explicit_convert_stage(
    relation: pd.DataFrame,
    events: pd.DataFrame,
) -> pd.DataFrame:
    columns = (
        "store_id", "business_date", "source_article_id", "target_article_id",
        "effective_from", "effective_to", "actual_event", "fixed_rule",
        "convert_rate", "cost_rate", "approved",
    )
    if relation.empty:
        return _empty(columns)
    frame = relation.rename(columns={
        "inc_day": "business_date", "parent_article_id": "source_article_id",
        "sub_article_id": "target_article_id",
    }).copy()
    keys = ["store_id", "business_date", "source_article_id", "target_article_id"]
    frame = _as_text(frame, keys)
    observed: set[tuple[str, str, str, str]] = set()
    if not events.empty:
        event = events.rename(columns={
            "shop_id": "store_id", "inc_day": "business_date",
            "receive_sku_code": "source_article_id", "sale_sku_code": "target_article_id",
        }).copy()
        event = _as_text(event, keys)
        observed = set(event[keys].itertuples(index=False, name=None))
    parent_rate = pd.to_numeric(frame["parent_rate"], errors="coerce")
    sub_rate = pd.to_numeric(frame["sub_rate"], errors="coerce")
    reciprocal = parent_rate.mul(sub_rate).sub(1.0).abs().le(0.001)
    frame["convert_rate"] = parent_rate.where(parent_rate.gt(0) & reciprocal)
    frame["cost_rate"] = 1.0
    frame["actual_event"] = [tuple(row) in observed for row in frame[keys].to_numpy()]
    # The mirror contains a static unit relation, not proof that every receipt converts.
    frame["fixed_rule"] = False
    frame["approved"] = frame["actual_event"] & frame["convert_rate"].notna()
    frame["effective_from"] = frame["business_date"]
    frame["effective_to"] = frame["business_date"]
    return frame[list(columns)]


def _exclude_bom_backed_explicit_relations(
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


def _stock_convert_events(raw: pd.DataFrame) -> pd.DataFrame:
    columns = (
        "store_id", "business_date", "event_group_id", "source_article_id",
        "target_article_id", "source_qty", "target_qty", "source_common_qty",
        "target_common_qty", "amount_allocation_ratio", "quantity_source",
    )
    if raw.empty:
        return _empty(columns)
    frame = raw.rename(columns={
        "shop_id": "store_id", "inc_day": "business_date", "id": "event_group_id",
        "receive_sku_code": "source_article_id", "sale_sku_code": "target_article_id",
        "receive_qty": "source_qty", "stock_qty": "target_qty",
    }).copy()
    frame["source_qty"] = pd.to_numeric(frame["source_qty"], errors="coerce")
    frame["target_qty"] = pd.to_numeric(frame["target_qty"], errors="coerce")
    rate = pd.to_numeric(frame["convert_rate"], errors="coerce")
    valid = (
        frame[["source_qty", "target_qty"]].notna().all(axis=1)
        & frame["source_qty"].ge(0) & frame["target_qty"].ge(0) & rate.gt(0)
    )
    frame = frame.loc[valid].copy()
    rate = rate.loc[valid]
    frame["source_common_qty"] = frame["source_qty"]
    frame["target_common_qty"] = frame["target_qty"] / rate
    frame["amount_allocation_ratio"] = 1.0
    frame["quantity_source"] = "STOCK_CONVERT_DETAIL"
    frame["event_group_id"] = "CONVERT|" + frame["event_group_id"].astype(str)
    return frame[list(columns)]


def _exclude_receipt_backed_conversion_events(
    events: pd.DataFrame,
    purchase: pd.DataFrame,
) -> pd.DataFrame:
    """Do not post a conversion twice when purchase already lands on the sale SKU.

    ``purchase_di`` is the authoritative store receipt fact.  For rows where
    its parent/child pair matches a same-day stock-convert event, the allocated
    child receipt already contains the converted quantity and cost.  The
    convert detail remains relation evidence, but is not another inventory
    movement.  A convert event without a matching receipt allocation remains a
    real internal movement and is kept for the ledger.
    """
    if events.empty or purchase.empty:
        return events.copy()
    keys = ["store_id", "business_date", "source_article_id", "target_article_id"]
    receipt = purchase.rename(columns={
        "article_id": "source_article_id",
        "sale_article_id": "target_article_id",
    }).copy()
    receipt = _as_text(receipt, keys)
    receipt_qty = pd.to_numeric(receipt["sale_article_qty"], errors="coerce").fillna(0.0)
    receipt_amt = pd.to_numeric(
        receipt["sale_article_purchase_amt"], errors="coerce"
    ).fillna(0.0)
    receipt_pairs = receipt.loc[
        receipt_qty.abs().gt(0.000001) | receipt_amt.abs().gt(0.000001), keys
    ].drop_duplicates()
    if receipt_pairs.empty:
        return events.copy()
    probe = events.merge(
        receipt_pairs.assign(_receipt_backed=True),
        on=keys,
        how="left",
        validate="many_to_one",
    )
    return probe.loc[probe["_receipt_backed"].isna(), events.columns].reset_index(drop=True)


def _bom_events(receive_sale: pd.DataFrame, bom: pd.DataFrame) -> pd.DataFrame:
    columns = (
        "store_id", "business_date", "event_group_id", "source_article_id",
        "target_article_id", "source_qty", "target_qty", "source_common_qty",
        "target_common_qty", "amount_allocation_ratio", "quantity_source",
    )
    official = bom.loc[bom["approved"].map(_bool)].copy()
    if receive_sale.empty or official.empty:
        return _empty(columns)
    bridge = receive_sale.rename(columns={
        "inc_day": "business_date", "article_id": "source_article_id",
        "sale_article_id": "target_article_id", "inbound_qty": "source_qty",
        "sale_article_qty": "target_qty",
    }).copy()
    keys = ["store_id", "business_date", "source_article_id", "target_article_id"]
    bridge = _as_text(bridge, keys)
    edges = official.rename(columns={
        "parent_article_id": "source_article_id", "sub_article_id": "target_article_id",
    })
    merged = bridge.merge(
        edges[keys + ["dressing_rate"]], on=keys, how="inner",
        validate="one_to_one",
    )
    if merged.empty:
        return _empty(columns)
    for column in (
        "source_qty", "target_qty", "dressing_rate",
        "spilit_sale_article_amt", "sale_recev_rate",
    ):
        merged[column] = pd.to_numeric(merged[column], errors="coerce")
    valid = merged[["source_qty", "target_qty", "dressing_rate"]].notna().all(axis=1)
    valid &= merged[["source_qty", "target_qty", "dressing_rate"]].gt(0).all(axis=1)
    merged = merged.loc[valid].copy()
    merged["event_group_id"] = (
        "BOM|" + merged["store_id"] + "|" + merged["business_date"] + "|"
        + merged["source_article_id"]
    )
    group = ["store_id", "business_date", "source_article_id"]
    split_sum = merged.groupby(group)["spilit_sale_article_amt"].transform("sum")
    rate_sum = merged.groupby(group)["sale_recev_rate"].transform("sum")
    mixed_zero = (
        merged["target_qty"].gt(0.000001)
        & merged["spilit_sale_article_amt"].fillna(0.0).le(0.000001)
    ).groupby([merged[column] for column in group]).transform("any")
    use_split = split_sum.gt(0.000001) & ~mixed_zero
    if ((~use_split) & rate_sum.le(0.000001)).any():
        bad = merged.loc[
            (~use_split) & rate_sum.le(0.000001), group
        ].drop_duplicates().head(20).to_dict("records")
        raise ValueError(f"BOM allocation lacks split amount and sale_recev_rate: {bad}")
    merged["amount_allocation_ratio"] = 0.0
    merged.loc[use_split, "amount_allocation_ratio"] = (
        merged.loc[use_split, "spilit_sale_article_amt"]
        / split_sum.loc[use_split]
    )
    merged.loc[~use_split, "amount_allocation_ratio"] = (
        merged.loc[~use_split, "sale_recev_rate"]
        / rate_sum.loc[~use_split]
    )
    quantity_weight = merged["sale_recev_rate"].fillna(0.0)
    quantity_sum = quantity_weight.groupby(
        [merged[column] for column in group]
    ).transform("sum")
    fallback_weight = merged["target_qty"]
    fallback_sum = fallback_weight.groupby(
        [merged[column] for column in group]
    ).transform("sum")
    use_rate = quantity_sum.gt(0.000001)
    merged["quantity_allocation_ratio"] = np.where(
        use_rate,
        quantity_weight / quantity_sum.replace(0, np.nan),
        fallback_weight / fallback_sum.replace(0, np.nan),
    )
    if merged["quantity_allocation_ratio"].isna().any():
        raise ValueError("BOM quantity allocation cannot be established")
    merged["source_common_qty"] = merged["source_qty"]
    merged["target_common_qty"] = (
        merged["source_qty"] * merged["quantity_allocation_ratio"]
    )
    merged["quantity_source"] = "HIVE_RECEIVE_SALE_WITH_OFFICIAL_BOM"
    return merged[list(columns)]


def _reporting_metrics(
    *,
    activities: pd.DataFrame,
    mirrors: Mapping[str, pd.DataFrame],
    goods: pd.DataFrame,
    processing: pd.DataFrame,
    store_id: str,
) -> pd.DataFrame:
    keys = ["store_id", "business_date", "article_id"]
    base = activities[keys + ["day_clear", "store_receive_qty"]].copy()
    dimensions = goods.copy()
    dimensions["article_id"] = dimensions["article_id"].astype(str)
    dimensions = dimensions.drop_duplicates("article_id")
    keep = [
        "article_id", "article_name", "category_level1_id", "category_level1_description",
        "category_level2_description", "category_level3_description", "spu_id",
        "spu_name", "sale_unit", "unit_weight",
    ]
    base = base.merge(dimensions[keep], on="article_id", how="left", validate="many_to_one")
    missing_goods = base["article_name"].isna()
    if missing_goods.any():
        raise ValueError(
            "v0.14 activity SKU missing Hive goods snapshot: "
            f"{base.loc[missing_goods, 'article_id'].head(20).tolist()}"
        )
    profile = mirrors["store_profile"]
    profile_row = profile.loc[profile["sp_store_id"].astype(str).eq(store_id)]
    if profile_row.empty:
        raise ValueError(f"store profile missing {store_id}")
    profile_row = profile_row.iloc[0]
    # v0.14's first release is deliberately scoped to A3XV.  The v1.5 output
    # contract identifies that store through chdj_store_info as a Food Mart
    # store, while dim_store_profile describes it as a generic "new business"
    # store.  Keep the public dimensions compatible with the current v1.5
    # result instead of leaking the different profile taxonomy into the 123
    # field output.
    base["store_flag"] = "翠花店" if store_id == "A3XV" else str(
        profile_row.get("store_flag_name", "")
    )
    base["store_no"] = "food mart" if store_id == "A3XV" else store_id
    base["store_name"] = str(profile_row["sp_store_name"])
    base["sku_id"] = base["article_id"]
    base["sku_name"] = base["article_name"]
    base["is_reportable"] = ~base["category_level1_id"].astype(str).isin(
        {"70", "71", "72", "73", "74", "75", "76", "77"}
    )
    raw_ids = set(processing["raw_article_id"].astype(str)) if not processing.empty else set()
    base["is_processing_raw"] = base["article_id"].isin(raw_ids)
    observed_inventory = mirrors["store_receipt"][[
        "store_id", "business_date", "sale_article_id",
        "init_stock_amt", "end_stock_amt", "init_stock_qty", "end_stock_qty",
    ]].copy().rename(columns={
        "sale_article_id": "article_id",
        "init_stock_amt": "initial_inventory_amount",
        "end_stock_amt": "ending_inventory_amount",
    })
    base["has_observed_inventory"] = False
    if not observed_inventory.empty:
        observed_inventory = _as_text(observed_inventory, keys)
        inventory_columns = [
            "initial_inventory_amount", "ending_inventory_amount",
            "init_stock_qty", "end_stock_qty",
        ]
        observed_inventory[inventory_columns] = observed_inventory[
            inventory_columns
        ].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        observed_inventory = observed_inventory.groupby(
            keys, as_index=False
        )[inventory_columns].sum()
        observed_inventory["has_observed_inventory_observed"] = True
        base = base.merge(
            observed_inventory, on=keys, how="left", validate="one_to_one"
        )
        base["has_observed_inventory"] = base[
            "has_observed_inventory_observed"
        ].eq(True)
        base = base.drop(columns="has_observed_inventory_observed")
    article_sale = mirrors["article_sale"].copy()
    if not article_sale.empty:
        article_sale = _as_text(article_sale, keys)
        numeric = [column for column in article_sale.columns if column not in keys + ["inc_day", "business_flag"]]
        article_sale[numeric] = article_sale[numeric].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        article_sale = article_sale.groupby(keys, as_index=False)[numeric].sum()
        base = base.merge(article_sale, on=keys, how="left", validate="one_to_one")
        mapping = {
            "sale_weight": "sales_weight", "bf19_sale_weight": "sales_weight_before_19",
            "cust_num": "total_customer_count", "bf19_cust_num": "customer_count_before_19",
            "bf19_sale_amt": "sale_amount_before_19", "bf19_sale_qty": "sale_qty_before_19",
            "bf19_sale_piece_qty": "piece_sales_before_19", "sale_piece_qty": "sale_piece_qty",
            "online_cust_num": "online_customer_count", "offline_cust_num": "offline_customer_count",
            "online_sale_amt": "online_sale_amt", "offline_sale_amt": "offline_sale_amt",
            "online_sale_qty": "online_sale_qty", "offline_sale_qty": "offline_sale_qty",
            "original_price_sale_amt": "original_sale_amount",
            "discount_amt": "discount_amount", "hour_discount_amt": "time_period_discount_amount",
            "promotion_discount_amt": "promotional_discount_amount",
            "return_sale_qty": "return_rate_numerator", "sale_qty": "return_rate_denominator",
        }
        for source, target in mapping.items():
            if source in base:
                base[target] = pd.to_numeric(base[source], errors="coerce").fillna(0.0)

    inventory_pool = mirrors["inventory_pool"].rename(columns={
        "shop_id": "store_id", "inventory_date": "business_date",
        "sku_code": "article_id", "seven_days_avg_sale": "avg_7d_sale_qty",
    }).copy()
    if not inventory_pool.empty:
        inventory_pool = _as_text(inventory_pool, keys)
        avg_7d = inventory_pool.groupby(keys, as_index=False)[
            "avg_7d_sale_qty"
        ].max()
        avg_7d["avg_7d_sale_qty"] = pd.to_numeric(
            avg_7d["avg_7d_sale_qty"], errors="raise"
        )
        base = base.merge(avg_7d, on=keys, how="left", validate="one_to_one")

    price = mirrors["price"].rename(columns={
        "shop_id": "store_id", "inc_day": "business_date", "sku_code": "article_id",
    }).copy()
    base["original_price"] = np.nan
    base["dc_original_price"] = np.nan
    if not price.empty:
        price = _as_text(price, keys)
        price_values = ["original_price", "dc_original_price"]
        price[price_values] = price[price_values].apply(
            pd.to_numeric, errors="coerce"
        )
        conflicts = price.groupby(keys)[price_values].nunique(dropna=True).gt(1).any(axis=1)
        if conflicts.any():
            raise ValueError(
                "price mirror contains conflicting original-price evidence: "
                f"{conflicts[conflicts].index.tolist()[:20]}"
            )
        price = price.groupby(keys, as_index=False)[price_values].max()
        base = base.drop(columns=["original_price", "dc_original_price"]).merge(
            price, on=keys, how="left", validate="one_to_one"
        )

    sales = mirrors["sales"].copy()
    if not sales.empty:
        sales = sales.rename(columns={"abi_article_id": "article_id"})
        sales = _as_text(sales, keys)
        sales["first_buy_flag"] = sales["first_buy_flag"].map(_bool)
        sales["qty"] = pd.to_numeric(sales["qty"], errors="coerce").fillna(0.0)
        sales["qty_spec"] = pd.to_numeric(
            sales["qty_spec"], errors="coerce"
        ).fillna(0.0)
        sales["sales_amt"] = pd.to_numeric(sales["sales_amt"], errors="coerce").fillna(0.0)
        # v1.5 defines customer counts as visits/orders, not distinct people.
        # One customer can place multiple orders and therefore contribute more
        # than one visit to customer count and average ticket metrics.
        new = sales.loc[sales["first_buy_flag"]].groupby(keys, as_index=False).agg(
            new_cust_num=("order_id", "nunique"), new_cust_sale_amt=("sales_amt", "sum"),
            new_cust_sale_qty=("qty", "sum"),
        )
        old = sales.loc[~sales["first_buy_flag"]].groupby(keys, as_index=False).agg(
            old_cust_num=("order_id", "nunique"), old_cust_sale_amt=("sales_amt", "sum"),
            old_cust_sale_qty=("qty", "sum"),
        )
        positive = sales["qty_spec"].gt(0)
        new_counts = (
            sales.loc[positive & sales["first_buy_flag"]]
            .groupby(keys, as_index=False)["order_id"].nunique()
            .rename(columns={"order_id": "new_cust_num_positive"})
        )
        old_counts = (
            sales.loc[positive & ~sales["first_buy_flag"]]
            .groupby(keys, as_index=False)["order_id"].nunique()
            .rename(columns={"order_id": "old_cust_num_positive"})
        )
        new = new.merge(new_counts, on=keys, how="left", validate="one_to_one")
        old = old.merge(old_counts, on=keys, how="left", validate="one_to_one")
        new["new_cust_num"] = new["new_cust_num_positive"].fillna(0.0)
        old["old_cust_num"] = old["old_cust_num_positive"].fillna(0.0)
        new = new.drop(columns="new_cust_num_positive")
        old = old.drop(columns="old_cust_num_positive")
        base = base.merge(new, on=keys, how="left", validate="one_to_one")
        base = base.merge(old, on=keys, how="left", validate="one_to_one")

    scm = mirrors["supply_chain"].copy()
    if not scm.empty:
        scm = _as_text(scm, keys)
        numeric = [column for column in scm.columns if column not in keys + ["inc_day"]]
        scm[numeric] = scm[numeric].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        scm = scm.groupby(keys, as_index=False)[numeric].sum()
        base = base.merge(scm, on=keys, how="left", validate="one_to_one")
        def value(name: str) -> pd.Series:
            if name in base:
                return pd.to_numeric(base[name], errors="coerce").fillna(0.0)
            return pd.Series(0.0, index=base.index)

        # SCM profit is the outbound distribution margin.  Return-stock
        # amounts are separate observations used by the return-rate metrics;
        # subtracting them here would count the return effect twice.
        out_cost = value("out_stock_amt_cb_notax")
        out_pay = value("out_stock_pay_amt_notax")
        base["outbound_cost"] = out_cost
        # purchase_weight is not a physical column in strategy_fm_scm_di.
        # Derive it from the actual receipt quantity and the authoritative
        # goods-unit conversion; order_qty_payean remains an order audit field.
        receive_qty = pd.to_numeric(
            base["store_receive_qty"], errors="raise"
        ).fillna(0.0)
        unit_weight = pd.to_numeric(
            base["unit_weight"], errors="coerce"
        ).fillna(0.0)
        unit_weight = unit_weight.mask(unit_weight.le(0), 1.0)
        base["purchase_weight_amount"] = np.where(
            base["sale_unit"].astype(str).eq("千克"),
            receive_qty,
            receive_qty * unit_weight,
        )
        base["supply_chain_promotion_amount"] = value("scm_promotion_amt_total")
        base["supply_chain_profit_amount"] = out_pay - out_cost
        base["supply_chain_profit_rate_denominator"] = out_pay
        expected_out = value("out_stock_pay_amt") + base["supply_chain_promotion_amount"]
        base["supply_chain_expected_profit_rate_numerator"] = expected_out - value(
            "out_stock_amt_cb"
        )
        base["supply_chain_expected_profit_rate_denominator"] = expected_out
        base["supply_chain_discount_rate_denominator"] = (
            base["supply_chain_promotion_amount"] + value("out_stock_pay_amt_notax")
        )
        base["return_rate_numerator"] = value("return_stock_pay_amt")
        base["return_rate_denominator"] = (
            value("out_stock_pay_amt") + value("return_stock_pay_amt")
        )

    saleable = mirrors["order_saleability"].rename(columns={"inc_day": "business_date"}).copy()
    if not saleable.empty:
        saleable = _as_text(saleable, keys)
        listed = saleable.groupby(keys)["saleable"].max().rename("listed").reset_index()
        base = base.merge(listed, on=keys, how="left", validate="one_to_one")
        base["stock_sku_flag"] = base["listed"].map(_bool).astype(float)
    sale_events = mirrors["sales"].rename(columns={"abi_article_id": "article_id"}).copy()
    if not sale_events.empty:
        sale_events = _as_text(sale_events, keys)
        positive_sale = pd.to_numeric(
            sale_events["qty_spec"], errors="coerce"
        ).fillna(0.0).gt(0)
        active_keys = sale_events.loc[positive_sale, keys].drop_duplicates().assign(
            active_sku_flag=1.0
        )
        base = base.merge(active_keys, on=keys, how="left", validate="one_to_one")
    if "active_sku_flag" not in base:
        base["active_sku_flag"] = 0.0
    base["active_sku_flag"] = pd.to_numeric(
        base["active_sku_flag"], errors="coerce"
    ).fillna(0.0)
    base["lp_sale_amt"] = (
        pd.to_numeric(base["original_price_sale_amt"], errors="coerce").fillna(0.0)
        if "original_price_sale_amt" in base else 0.0
    )
    order_parts = []
    for source_name in ("order_offline", "order_online"):
        orders = mirrors[source_name].copy()
        if orders.empty:
            continue
        orders = orders.loc[
            orders["order_status"].astype(str).isin({"os.completed", "os.split"})
            & pd.to_numeric(orders["qty"], errors="coerce").fillna(0.0).gt(0)
        ].rename(columns={"abi_article_id": "article_id"})
        order_parts.append(orders[["store_id", "business_date", "article_id", "pay_at"]])
    if order_parts:
        order_times = pd.concat(order_parts, ignore_index=True)
        order_times = _as_text(order_times, keys)
        order_times["pay_at"] = pd.to_datetime(order_times["pay_at"], errors="coerce")
        last_sale = order_times.groupby(keys, as_index=False)["pay_at"].max()
        last_sale["last_sale_hour"] = last_sale["pay_at"].dt.hour
        base = base.merge(
            last_sale[keys + ["last_sale_hour"]],
            on=keys, how="left", validate="one_to_one",
        )
    else:
        base["last_sale_hour"] = np.nan
    wastage = build_wastage_trace(mirrors["purchase_wastage"])
    if not wastage.empty:
        special = wastage.pivot_table(
            index=keys,
            columns="reason_code",
            values=["waste_money", "waste_num"],
            aggfunc="sum",
            fill_value=0.0,
        )
        special.columns = [
            f"{reason}_{'amt' if metric == 'waste_money' else 'qty'}"
            for metric, reason in special.columns
        ]
        special = special.reset_index()
        base = base.merge(special, on=keys, how="left", validate="one_to_one")
    # Event metrics may legitimately be zero when their authoritative mirror
    # has no row.  Formula metrics are calculated below or in the ledger
    # assembly and are never created by a catch-all zero fill.
    zero_when_no_event = {
        "sales_weight", "sales_weight_before_19", "sale_qty_before_19",
        "sale_amount_before_19", "piece_sales_before_19", "sale_piece_qty",
        "original_sale_amount", "discount_amount",
        "time_period_discount_amount", "promotional_discount_amount",
        "outbound_cost", "purchase_weight_amount",
        "supply_chain_promotion_amount", "supply_chain_profit_amount",
        "supply_chain_profit_rate_denominator",
        "supply_chain_expected_profit_rate_numerator",
        "supply_chain_expected_profit_rate_denominator",
        "supply_chain_discount_rate_denominator",
        "return_rate_numerator", "return_rate_denominator",
        "online_sale_amt", "offline_sale_amt", "online_sale_qty",
        "offline_sale_qty", "jielong_sale_amt", "jsd_sale_amt",
        "jielong_sale_qty", "jsd_sale_qty", "lp_sale_amt",
        "new_cust_num", "old_cust_num",
        "new_cust_sale_amt", "old_cust_sale_amt",
        "new_cust_sale_qty", "old_cust_sale_qty",
        "stock_sku_flag", "active_sku_flag", "avg_7d_sale_qty",
        "ccj_amt", "ccj_qty", "ssls_amt", "ssls_qty",
    }
    for column in zero_when_no_event:
        if column not in base:
            base[column] = 0.0
        base[column] = pd.to_numeric(
            base[column], errors="coerce"
        ).fillna(0.0)
    # Customer counts are replaced by distinct-order aggregation at each
    # public level; keeping zero placeholders here prevents accidental SKU
    # summation while preserving the frozen intermediate schema.
    for column in (
        "total_customer_count", "customer_count_before_19",
        "online_customer_count", "offline_customer_count",
        "jielong_customer_count", "jsd_customer_count",
    ):
        base[column] = 0.0
    formula_columns = {
        "store_expected_profit_amount", "store_pricing_profit_rate_numerator",
        "loss_rate_denominator", "loss_rate_qty_denominator",
        "soldout_16_numerator", "soldout_16_denominator",
        "soldout_20_numerator", "soldout_20_denominator",
        "initial_inventory_amount", "ending_inventory_amount",
        "init_stock_qty", "end_stock_qty", "loss_amount", "loss_qty",
        "store_know_lost_amt", "store_unknow_lost_amt", "inbound_amount",
        "inbound_qty", "total_sale_qty", "total_sale_amount",
        "accounting_profit", "accounting_full_profit",
    }
    unresolved = sorted(
        ADDITIVE_INPUTS
        - zero_when_no_event
        - formula_columns
        - {
            "total_customer_count", "customer_count_before_19",
            "online_customer_count", "offline_customer_count",
            "jielong_customer_count", "jsd_customer_count",
        }
    )
    if unresolved:
        raise RuntimeError(f"v0.14 reporting metric ownership is undefined: {unresolved}")
    required_dimensions = [
        "store_flag", "store_no", "store_name", "sku_id", "sku_name",
        "category_level1_description", "category_level2_description",
        "category_level3_description", "spu_id", "spu_name", "sale_unit", "day_clear",
        "is_processing_raw", "is_reportable", "has_observed_inventory",
    ]
    return base[
        keys + required_dimensions + sorted(ADDITIVE_INPUTS & set(base.columns))
        + ["original_price", "dc_original_price", "last_sale_hour"]
    ]


def _customer_events(
    *,
    mirrors: Mapping[str, pd.DataFrame],
    goods: pd.DataFrame,
    store_id: str,
) -> pd.DataFrame:
    """Build the order-level counting spine used by every reporting level."""
    sales = mirrors["sales"].copy().rename(columns={"abi_article_id": "sku_id"})
    if sales.empty:
        raise ValueError("sales mirror cannot build customer events")
    sales["store_id"] = sales["store_id"].astype(str)
    sales["business_date"] = sales["business_date"].astype(str)
    sales["sku_id"] = sales["sku_id"].astype(str)
    sales["order_id"] = sales["order_id"].astype(str)
    qty = pd.to_numeric(sales["qty_spec"], errors="raise")
    sales = sales.loc[
        qty.gt(0)
        & sales["order_status"].astype(str).isin({"os.completed", "os.split"})
    ].copy()
    if sales.empty:
        raise ValueError("sales mirror has no positive completed order events")

    order_frames: list[pd.DataFrame] = []
    for channel, name in (("offline", "order_offline"), ("online", "order_online")):
        source = mirrors[name].copy()
        if source.empty:
            continue
        source = source.loc[
            source["order_status"].astype(str).isin({"os.completed", "os.split"})
        ].copy()
        source["source_channel"] = channel
        order_frames.append(source[[
            "store_id", "business_date", "order_id", "abi_article_id",
            "pay_at", "source_channel",
        ]].rename(columns={"abi_article_id": "sku_id"}))
    if not order_frames:
        raise ValueError("order mirrors are required for customer time/channel metrics")
    orders = pd.concat(order_frames, ignore_index=True)
    for column in ("store_id", "business_date", "order_id", "sku_id"):
        orders[column] = orders[column].astype(str)
    orders["pay_at"] = pd.to_datetime(orders["pay_at"], errors="coerce")
    order_keys = ["store_id", "business_date", "order_id", "sku_id"]
    orders = orders.groupby(order_keys, as_index=False).agg(
        pay_at=("pay_at", "min"),
        source_channel=("source_channel", lambda values: sorted(set(values))[0]),
    )
    sales = sales.merge(
        orders, on=order_keys, how="left", validate="many_to_one"
    )
    missing_order = sales["pay_at"].isna() | sales["source_channel"].isna()
    if missing_order.any():
        sample = sales.loc[missing_order, order_keys].drop_duplicates().head(20).to_dict("records")
        raise ValueError(f"customer events missing order time/channel evidence: {sample}")

    dims = goods[[
        "article_id", "article_name", "category_level1_id",
        "category_level1_description", "category_level2_description",
        "category_level3_description", "spu_id", "spu_name",
    ]].copy().rename(columns={"article_id": "sku_id", "article_name": "sku_name"})
    dims["sku_id"] = dims["sku_id"].astype(str)
    dims = dims.drop_duplicates("sku_id")
    sales = sales.merge(dims, on="sku_id", how="left", validate="many_to_one")
    if sales["sku_name"].isna().any():
        raise ValueError("customer event SKU is missing goods dimensions")
    sales = sales.loc[
        ~sales["category_level1_id"].astype(str).isin(
            {"70", "71", "72", "73", "74", "75", "76", "77"}
        )
    ].copy()
    profile = mirrors["store_profile"]
    profile_row = profile.loc[profile["sp_store_id"].astype(str).eq(store_id)]
    if profile_row.empty:
        raise ValueError(f"store profile missing {store_id}")
    sales["store_flag"] = "翠花店" if store_id == "A3XV" else str(
        profile_row.iloc[0].get("store_flag_name", "")
    )
    sales["store_no"] = "food mart" if store_id == "A3XV" else store_id
    sales["store_name"] = str(profile_row.iloc[0]["sp_store_name"])
    sales["day_clear"] = sales["day_clear"].map(_normalize_day_clear)
    if sales["day_clear"].isna().any():
        raise ValueError("customer events contain an invalid day_clear label")
    sales["is_before_19"] = sales["pay_at"].dt.hour.lt(19)
    sales["is_online"] = sales["source_channel"].eq("online")
    sales["is_offline"] = ~sales["is_online"]
    sales["is_jielong"] = sales["jielong_flag"].map(
        lambda value: False
        if value is None or pd.isna(value) or str(value).strip() in {"", "-", "0", "0.0"}
        else True
    )
    sales["is_jsd"] = sales["is_online"] & ~sales["is_jielong"]
    sales["is_new"] = sales["first_buy_flag"].map(_bool)
    sales["is_old"] = ~sales["is_new"]
    columns = [
        "store_id", "store_flag", "store_no", "business_date", "store_name", "sku_id",
        "sku_name", "category_level1_description", "category_level2_description",
        "category_level3_description", "spu_id", "spu_name", "day_clear",
        "order_id", "is_before_19", "is_online", "is_offline", "is_jielong",
        "is_jsd", "is_new", "is_old",
    ]
    return sales[columns].drop_duplicates().reset_index(drop=True)


def build_v014_stage_bundle(
    mirrors: MirrorSourceBundle,
    *,
    processing_relations: pd.DataFrame | None = None,
) -> V014StageBundle:
    """Normalize Hive-backed mirror rows into the internal v0.14 calculation boundary."""
    store_id = mirrors.store_id
    days = mirrors.requested_days
    source = mirrors.frames
    required = {
        "sales", "store_receipt", "known_loss", "inventory_detail", "day_clear",
        "receive_sale", "bom_relation", "article_convert", "stock_convert_detail",
        "product_group", "goods", "store_profile", "article_sale", "supply_chain",
        "order_saleability", "purchase_wastage", "order_offline", "order_online",
        "inventory_pool", "price",
    }
    missing = sorted(required - set(source))
    if missing:
        raise KeyError(f"v0.14 Hive mirror bundle missing sources: {missing}")

    sales = normalize_sales_events(source["sales"], store_id=store_id)
    losses = normalize_known_loss(source["known_loss"], store_id=store_id)
    counts = normalize_inventory_counts(source["inventory_detail"], store_id=store_id)
    bom = _bom_stage(source["bom_relation"])
    processing, processing_quarantine = _processing_stage(
        processing_relations, store_id=store_id, days=days
    )
    goods = source["goods"].copy()
    goods["article_id"] = goods["article_id"].astype(str)
    processing, procurement_pack = _split_processing_and_packaging(processing, goods)
    explicit = pd.concat([
        _explicit_convert_stage(source["article_convert"], source["stock_convert_detail"]),
        procurement_pack,
    ], ignore_index=True)
    explicit = _exclude_bom_backed_explicit_relations(explicit, bom)
    purchase = source["store_receipt"].copy()
    stock_conversion_evidence = _stock_convert_events(source["stock_convert_detail"])
    stock_conversion_events = _exclude_receipt_backed_conversion_events(
        stock_conversion_evidence, purchase
    )
    conversion_events = pd.concat([
        _bom_events(source["receive_sale"], bom),
        stock_conversion_events,
    ], ignore_index=True)

    candidate_frames: list[pd.DataFrame] = []
    purchase_candidates = purchase[["store_id", "business_date", "article_id", "sale_article_id"]].rename(
        columns={"article_id": "source_article_id", "sale_article_id": "target_article_id"}
    )
    candidate_frames.append(purchase_candidates)
    if not stock_conversion_evidence.empty:
        candidate_frames.append(stock_conversion_evidence[[
            "store_id", "business_date", "source_article_id", "target_article_id",
        ]])
    if not processing.empty:
        candidate_frames.append(processing.rename(columns={
            "raw_article_id": "source_article_id", "finished_article_id": "target_article_id",
        })[["store_id", "business_date", "source_article_id", "target_article_id"]])
    if not explicit.empty:
        candidate_frames.append(explicit[[
            "store_id", "business_date", "source_article_id", "target_article_id",
        ]])
    candidates = pd.concat(candidate_frames, ignore_index=True)
    candidate_keys = ["store_id", "business_date", "source_article_id", "target_article_id"]
    candidates = _as_text(candidates, candidate_keys).drop_duplicates(candidate_keys)

    parent_keys = set(
        conversion_events.loc[
            conversion_events["quantity_source"].eq("HIVE_RECEIVE_SALE_WITH_OFFICIAL_BOM"),
            ["store_id", "business_date", "source_article_id"],
        ].itertuples(index=False, name=None)
    )
    receipt_result = build_store_receipts(
        purchase, source["receive_sale"], parent_reconstruction_keys=parent_keys,
        store_id=store_id,
    )

    reportable = goods.loc[
        ~goods["category_level1_id"].astype(str).isin({"70", "71", "72", "73", "74", "75", "76", "77"})
    ]
    universe: set[str] = set()
    for frame, column in (
        (sales, "article_id"), (losses, "article_id"), (counts, "article_id"),
        (receipt_result.postings, "article_id"), (candidates, "source_article_id"),
        (candidates, "target_article_id"), (purchase, "sale_article_id"),
    ):
        universe.update(frame[column].dropna().astype(str))
    active_saleable = source["order_saleability"].loc[
        source["order_saleability"]["saleable"].map(_bool), "article_id"
    ].dropna().astype(str)
    universe.update(active_saleable)
    universe &= set(reportable["article_id"]) | set(candidates["source_article_id"].astype(str))
    if not universe:
        raise ValueError("v0.14 Hive mirrors produced an empty SKU universe")

    count_keep = counts.loc[counts["article_id"].isin(universe)].copy()
    day_clear = _build_day_clear_frame(
        store_id=store_id,
        days=days,
        universe=universe,
        raw_day_clear=source["day_clear"],
        sales=sales,
        goods=goods,
    )
    activities = assemble_dense_activities(
        days=days, article_ids=sorted(universe),
        sales=sales.loc[sales["article_id"].isin(universe)],
        losses=losses.loc[losses["article_id"].isin(universe)],
        counts=count_keep,
        day_clear=day_clear,
        receipts=receipt_result.postings.loc[
            receipt_result.postings["article_id"].isin(universe)
        ],
        store_id=store_id,
    )

    count_aux = count_keep[["store_id", "business_date", "article_id", "actual_stock_qty"]].copy()
    count_aux = count_aux.sort_values(["article_id", "business_date"], kind="stable")
    count_aux["previous_stock_qty"] = count_aux.groupby("article_id")["actual_stock_qty"].shift(1)
    groups = source["product_group"][["inc_day", "article_id", "article_group_id"]].rename(
        columns={"inc_day": "business_date", "article_group_id": "count_group_id"}
    )
    groups = _as_text(groups, ["business_date", "article_id"])
    count_aux = count_aux.merge(groups, on=["business_date", "article_id"], how="left", validate="many_to_one")
    purchase_parent_ids = set(purchase["article_id"].dropna().astype(str))
    sale_ids = set(source["order_saleability"].loc[
        source["order_saleability"]["saleable"].map(_bool), "article_id"
    ].dropna().astype(str))
    count_aux["code_role"] = count_aux["article_id"].map(
        lambda article: "RECEIPT" if article in purchase_parent_ids and article not in sale_ids else "SALE"
    )
    activities = activities.merge(
        count_aux[["store_id", "business_date", "article_id", "previous_stock_qty", "count_group_id", "code_role"]],
        on=["store_id", "business_date", "article_id"], how="left", validate="one_to_one",
    )
    activities["previous_stock_qty"] = pd.to_numeric(
        activities["previous_stock_qty"], errors="coerce"
    )
    activities["code_role"] = activities["code_role"].fillna("SALE")

    openings = build_bootstrap_openings(
        purchase, sorted(universe), start_day=days[0], store_id=store_id
    )
    openings["opening_source"] = openings["opening_source"].replace({
        "PURCHASE_DI_BOOTSTRAP": "HIVE_PURCHASE_DI_BOOTSTRAP",
        "PURCHASE_DI_UNPRICED_OPENING": "HIVE_PURCHASE_DI_UNPRICED_BOOTSTRAP",
        "PURCHASE_DI_NULL_BOOTSTRAP_ZERO": "HIVE_PURCHASE_DI_NULL_BOOTSTRAP_ZERO",
        "PURCHASE_DI_BOOTSTRAP_ZERO": "HIVE_PURCHASE_DI_BOOTSTRAP_ZERO",
        "PURCHASE_DI_NEGATIVE_CLAMP": "HIVE_PURCHASE_DI_NEGATIVE_BOOTSTRAP_CLAMP",
    })

    finished_rows: list[dict[str, object]] = []
    raw_rows: list[dict[str, object]] = []
    activity_index = activities.set_index(["store_id", "business_date", "article_id"])
    count_index = count_keep.set_index(["store_id", "business_date", "article_id"])
    opening_index = openings.set_index(["store_id", "article_id"])
    recipes = processing.loc[processing["approved"].map(_bool)]
    for finished_id in sorted(set(recipes["finished_article_id"].astype(str))):
        previous_end: float | None = None
        for day in days:
            key = (store_id, day, finished_id)
            activity = activity_index.loc[key]
            has_count = key in count_index.index and bool(count_index.loc[key, "is_counted"])
            end_qty = float(count_index.loc[key, "actual_stock_qty"]) if has_count else 0.0
            init_qty = (
                float(opening_index.loc[(store_id, finished_id), "opening_qty"])
                if previous_end is None else previous_end
            )
            finished_rows.append({
                "store_id": store_id, "business_date": day, "article_id": finished_id,
                "init_stock_qty": init_qty, "end_stock_qty": end_qty,
                "external_receive_qty": float(activity.store_receive_qty),
                "net_sale_qty": float(activity.net_sale_qty),
                "known_lost_qty": float(activity.known_lost_qty),
                "other_internal_out_qty": 0.0, "other_internal_in_qty": 0.0,
                "has_valid_count": has_count,
            })
            previous_end = end_qty if has_count else None
    for raw_id in sorted(set(recipes["raw_article_id"].astype(str))):
        previous_end = None
        for day in days:
            key = (store_id, day, raw_id)
            activity = activity_index.loc[key]
            opening_qty = float(opening_index.loc[(store_id, raw_id), "opening_qty"])
            available = (opening_qty if previous_end is None else previous_end) + float(activity.store_receive_qty)
            raw_rows.append({
                "store_id": store_id, "business_date": day, "article_id": raw_id,
                "available_qty": max(0.0, available),
            })
            if key in count_index.index and bool(count_index.loc[key, "is_counted"]):
                previous_end = float(count_index.loc[key, "actual_stock_qty"])
            else:
                previous_end = None

    metrics = _reporting_metrics(
        activities=activities, mirrors=source, goods=goods, processing=processing,
        store_id=store_id,
    )
    customer_events = _customer_events(
        mirrors=source, goods=goods, store_id=store_id
    )
    completeness = mirrors.completeness.copy()
    completeness["source_name"] = completeness["source_name"].astype(str)
    frames = {
        "source_manifest": mirrors.manifest.copy(),
        "source_completeness": completeness,
        "product_group": source["product_group"].copy(),
        "relation_candidates": candidates,
        "bom": bom,
        "processing": processing,
        "explicit_convert": explicit,
        "activities": activities,
        "openings": openings,
        "conversion_events": conversion_events,
        "finished_processing_daily": pd.DataFrame(finished_rows, columns=(
            "store_id", "business_date", "article_id", "init_stock_qty", "end_stock_qty",
            "external_receive_qty", "net_sale_qty", "known_lost_qty", "other_internal_out_qty",
            "other_internal_in_qty", "has_valid_count",
        )),
        "raw_available": pd.DataFrame(raw_rows, columns=(
            "store_id", "business_date", "article_id", "available_qty",
        )),
        "reporting_metrics": metrics,
        "customer_events": customer_events,
    }
    quarantine = pd.concat([
        processing_quarantine,
        receipt_result.quarantined.rename(columns={"reason": "reason_code"}),
    ], ignore_index=True, sort=False)
    return V014StageBundle(store_id, days, frames, quarantine)


def persist_v014_stage_bundle(path: Path | str, bundle: V014StageBundle) -> Path:
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(target))
    try:
        conn.execute("BEGIN TRANSACTION")
        for name, frame in bundle.frames.items():
            table = f"{STAGE_TABLE_PREFIX}{name}"
            conn.register("_stage_frame", frame)
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM _stage_frame')
            conn.unregister("_stage_frame")
        conn.register("_stage_quarantine", bundle.quarantined)
        conn.execute('DROP TABLE IF EXISTS "v014_stage_quarantine"')
        conn.execute('CREATE TABLE "v014_stage_quarantine" AS SELECT * FROM _stage_quarantine')
        conn.unregister("_stage_quarantine")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
    return target

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import duckdb
import numpy as np
import pandas as pd

from fmetl.calculations.special_wastage import (
    build_wastage_trace,
    merge_special_loss_quantity,
)
from fmetl.mirror.report_metrics import (
    build_customer_events,
    build_reporting_metrics,
    build_scm_margin_audit,
)
from fmetl.mirror.source_normalization import (
    assemble_daily_activities,
    build_startup_openings,
)
from fmetl.mirror.relation_building import (
    attach_bom_event_ratios,
    build_bom_events,
    build_bom_relations,
    build_explicit_relations,
    build_processing_relations,
    exclude_bom_backed_explicit_relations,
)
from fmetl.facts.sku_day import (
    attach_authoritative_day_clear,
    normalize_inventory_counts,
    normalize_chdj_day_clear,
    normalize_known_loss,
    normalize_sales_events,
)
from fmetl.facts.store_receipts import build_store_receipts
from fmetl.mirror.v014_source import MirrorSourceBundle
from fmetl.mirror.v014_source import SOURCE_TIERS


STAGE_TABLE_PREFIX = "v014_stage_"


@dataclass(frozen=True)
class V014StageBundle:
    store_id: str
    days: tuple[str, ...]
    frames: Mapping[str, pd.DataFrame]
    quarantined: pd.DataFrame


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


def _tri_state(value: object) -> bool | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().lower()
    if text in {"1", "1.0", "true", "yes", "y"}:
        return True
    if text in {"0", "0.0", "false", "no", "n"}:
        return False
    return None



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
    """Use chdj_article for day-clear and mark every diagnostic default."""
    del goods
    del sales
    frame = raw_day_clear.rename(columns={"inc_day": "business_date"}).copy()
    if "business_date" in frame.columns and frame.columns.duplicated().any():
        frame = frame.loc[:, ~frame.columns.duplicated()]
    if "day_clear" not in frame:
        raise KeyError("chdj_day_clear source must contain day_clear")
    explicit: dict[tuple[str, str, str], str] = {}
    for row in frame.itertuples(index=False):
        value = _normalize_day_clear(row.day_clear)
        if value is not None:
            explicit[(str(row.store_id), str(row.business_date), str(row.article_id))] = value

    rows: list[dict[str, str]] = []
    for day in map(str, days):
        for article in sorted(universe):
            key = (store_id, day, article)
            if key in explicit:
                value = explicit[key]
                source_status = "CHDJ_ARTICLE_AUTHORITATIVE"
            else:
                value = "1"
                source_status = "MISSING_AUTHORITATIVE_DEFAULT_NON_DAY_CLEAR"
            rows.append({
                "store_id": store_id,
                "business_date": day,
                "article_id": article,
                "day_clear": value,
                "day_clear_source": source_status,
            })
    return pd.DataFrame(rows)


def build_v014_stage_bundle(
    mirrors: MirrorSourceBundle,
    *,
    processing_relations: pd.DataFrame | None = None,
) -> V014StageBundle:
    """Normalize Hive-backed rows into the v0.18 calculation boundary."""
    store_id = mirrors.store_id
    days = mirrors.requested_days
    source = mirrors.frames
    required = {
        "sales", "store_receipt", "known_loss", "inventory_detail",
        "chdj_day_clear", "day_clear",
        "receive_sale", "bom_relation", "article_convert",
        "product_group", "goods", "store_profile", "article_sale", "supply_chain",
        "order_saleability", "purchase_wastage", "order_offline", "order_online",
        "inventory_pool", "price",
    }
    missing = sorted(required - set(source))
    if missing:
        raise KeyError(f"v0.18 Hive mirror bundle missing sources: {missing}")

    sales = normalize_sales_events(source["sales"], store_id=store_id)
    chdj_day_clear = source["chdj_day_clear"].copy()
    chdj_required = ["store_id", "inc_day", "article_id", "day_clear"]
    invalid_chdj = chdj_day_clear[chdj_required].isna().any(axis=1)
    for column in chdj_required:
        invalid_chdj |= chdj_day_clear[column].astype(str).str.strip().str.lower().isin(
            {"", "nan", "none", "null"}
        )
    invalid_day_clear_rows = chdj_day_clear.loc[invalid_chdj].copy()
    day_clear_quarantine = pd.DataFrame({
        "store_id": invalid_day_clear_rows.get(
            "store_id", pd.Series(dtype=str)
        ).fillna(store_id).astype(str),
        "business_date": invalid_day_clear_rows.get(
            "inc_day", pd.Series(dtype=str)
        ).fillna("").astype(str),
        "article_id": invalid_day_clear_rows.get(
            "article_id", pd.Series(dtype=str)
        ).fillna("").astype(str),
        "reason_code": "INVALID_DAY_CLEAR_SOURCE_KEY",
        "detail": "权威日清表的门店、日期、SKU 或日清值为空，无法确定该记录属于哪个 SKU 日，因此隔离",
    })
    chdj_day_clear = chdj_day_clear.loc[~invalid_chdj].copy()
    authoritative_day_clear = normalize_chdj_day_clear(
        chdj_day_clear, store_id=store_id
    )
    sales = attach_authoritative_day_clear(sales, authoritative_day_clear)
    losses = normalize_known_loss(source["known_loss"], store_id=store_id)
    wastage_trace = build_wastage_trace(source["purchase_wastage"])
    wastage_trace = wastage_trace.loc[
        wastage_trace["business_date"].astype(str).isin(days)
    ].copy()
    counts = normalize_inventory_counts(source["inventory_detail"], store_id=store_id)
    bom = build_bom_relations(source["bom_relation"])
    processing, processing_quarantine = build_processing_relations(
        processing_relations, store_id=store_id, days=days
    )
    goods = source["goods"].copy()
    goods["article_id"] = goods["article_id"].astype(str)
    # The approved processing export defines processing.  article_convert only
    # defines a fixed conversion when ctype=2 and both codes are unique; ctype=1
    # remains BOM unit evidence and cannot create another inventory flow.
    explicit = build_explicit_relations(
        source["article_convert"]
    )
    explicit = exclude_bom_backed_explicit_relations(explicit, bom)
    purchase = source["store_receipt"].copy()
    bom_events, bom_quarantine = build_bom_events(
        source["receive_sale"], bom, source["article_convert"]
    )
    bom = attach_bom_event_ratios(bom, bom_events)
    conversion_events = bom_events.copy()

    # Keep every official BOM edge in the relation audit.  A BOM edge without
    # a same-day receive_sale event is relation evidence only: it does not post
    # inventory and it has no observed quantity ratio for that day.
    candidate_frames: list[pd.DataFrame] = [bom.rename(columns={
        "parent_article_id": "source_article_id",
        "sub_article_id": "target_article_id",
    })[[
        "store_id", "business_date", "source_article_id", "target_article_id",
    ]]]
    # purchase_di supplies only the A->B code direction. Its allocated B
    # quantity and amount do not enter the ledger; target demand determines the
    # conversion quantity later.
    purchase_candidates = purchase.loc[
        purchase["article_id"].astype(str).ne(
            purchase["sale_article_id"].astype(str)
        ),
        ["store_id", "business_date", "article_id", "sale_article_id"],
    ].rename(
        columns={"article_id": "source_article_id", "sale_article_id": "target_article_id"}
    )
    candidate_frames.append(purchase_candidates)
    if not explicit.empty:
        candidate_frames.append(explicit.loc[explicit["approved"].map(_bool), [
            "store_id", "business_date", "source_article_id", "target_article_id",
        ]])
    if not processing.empty:
        candidate_frames.append(processing.loc[
            processing["approved"].map(_bool)
        ].rename(columns={
            "raw_article_id": "source_article_id", "finished_article_id": "target_article_id",
        })[["store_id", "business_date", "source_article_id", "target_article_id"]])
    candidates = pd.concat(candidate_frames, ignore_index=True)
    candidate_keys = ["store_id", "business_date", "source_article_id", "target_article_id"]
    candidates = _as_text(candidates, candidate_keys).drop_duplicates(candidate_keys)

    receipt_result = build_store_receipts(
        purchase, source["receive_sale"], store_id=store_id,
    )

    reportable = goods.loc[
        ~goods["category_level1_id"].astype(str).isin({"70", "71", "72", "73", "74", "75", "76", "77"})
    ]
    universe: set[str] = set()
    for frame, column in (
        (sales, "article_id"), (losses, "article_id"), (counts, "article_id"),
        (receipt_result.postings, "article_id"), (candidates, "source_article_id"),
        (candidates, "target_article_id"), (purchase, "sale_article_id"),
        (wastage_trace, "article_id"),
    ):
        universe.update(frame[column].dropna().astype(str))
    active_saleable = source["order_saleability"].loc[
        source["order_saleability"]["saleable"].map(_bool), "article_id"
    ].dropna().astype(str)
    universe.update(active_saleable)
    universe &= set(reportable["article_id"]) | set(candidates["source_article_id"].astype(str))
    if not universe:
        raise ValueError("v0.18 Hive mirrors produced an empty SKU universe")

    count_keep = counts.loc[counts["article_id"].isin(universe)].copy()
    day_clear = _build_day_clear_frame(
        store_id=store_id,
        days=days,
        universe=universe,
        raw_day_clear=chdj_day_clear,
        sales=sales,
        goods=goods,
    )
    activities = assemble_daily_activities(
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
    special_loss = merge_special_loss_quantity(activities, wastage_trace)
    activities = special_loss.activities

    # ``inventory_pool.cost_price`` is reference-only evidence: it must never
    # replace a positive weighted inventory pool.  Carry the same-day value to
    # the serial ledger solely for the empty-pool overdraft branch, where the
    # alternative is an explicitly invalid zero-cost sale/loss.  The source
    # mirror contract is unique at store x inventory_date x SKU; keep an
    # independent assertion here because this value can affect reported cost.
    fallback_cost = source["inventory_pool"].rename(columns={
        "shop_id": "store_id",
        "inventory_date": "business_date",
        "sku_code": "article_id",
        "cost_price": "fallback_cost",
    })[["store_id", "business_date", "article_id", "fallback_cost"]].copy()
    fallback_cost = _as_text(fallback_cost, ["store_id", "business_date", "article_id"])
    fallback_cost = fallback_cost.loc[
        fallback_cost["store_id"].eq(store_id)
        & fallback_cost["business_date"].isin(days)
        & fallback_cost["article_id"].isin(universe)
    ]
    if fallback_cost.duplicated(["store_id", "business_date", "article_id"]).any():
        raise ValueError("inventory-pool fallback cost must be unique per SKU-day")
    fallback_cost["fallback_cost"] = pd.to_numeric(
        fallback_cost["fallback_cost"], errors="raise"
    )
    if (
        fallback_cost["fallback_cost"].isna().any()
        or ~np.isfinite(fallback_cost["fallback_cost"].to_numpy(dtype=float)).all()
        or fallback_cost["fallback_cost"].lt(0).any()
    ):
        raise ValueError("inventory-pool fallback cost must be finite and nonnegative")
    activities = activities.merge(
        fallback_cost,
        on=["store_id", "business_date", "article_id"],
        how="left",
        validate="one_to_one",
    )
    activities["fallback_cost"] = activities["fallback_cost"].fillna(0.0)

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

    openings = build_startup_openings(
        purchase, sorted(universe), start_day=days[0], store_id=store_id
    )
    openings["opening_source"] = openings["opening_source"].replace({
        "PURCHASE_DI_BOOTSTRAP": "HIVE_PURCHASE_DI_BOOTSTRAP",
        "PURCHASE_DI_UNPRICED_OPENING": "HIVE_PURCHASE_DI_UNPRICED_BOOTSTRAP",
        "PURCHASE_DI_NULL_BOOTSTRAP_ZERO": "HIVE_PURCHASE_DI_NULL_BOOTSTRAP_ZERO",
        "PURCHASE_DI_BOOTSTRAP_ZERO": "HIVE_PURCHASE_DI_BOOTSTRAP_ZERO",
        "PURCHASE_DI_NEGATIVE_OPENING": "HIVE_PURCHASE_DI_NEGATIVE_OPENING_RETAINED",
        "PURCHASE_DI_AMOUNT_WITHOUT_QUANTITY": "HIVE_PURCHASE_DI_AMOUNT_WITHOUT_QUANTITY",
    })

    recipes = processing.loc[processing["approved"].map(_bool)]
    finished_ids = set(recipes["finished_article_id"].astype(str))
    finished_usage = activities.loc[
        activities["article_id"].astype(str).isin(finished_ids),
        [
            "store_id", "business_date", "article_id",
            "net_sale_qty", "known_lost_qty",
        ],
    ].copy()

    metrics = build_reporting_metrics(
        activities=activities, mirrors=source, goods=goods, processing=processing,
        store_id=store_id,
    )
    scm_margin_audit = build_scm_margin_audit(source["supply_chain"])
    customer_events = build_customer_events(
        mirrors=source, goods=goods, day_clear=day_clear, store_id=store_id
    )
    completeness = mirrors.completeness.copy()
    completeness["source_name"] = completeness["source_name"].astype(str)
    completeness["source_tier"] = completeness["source_name"].map(SOURCE_TIERS)
    product_group = source["product_group"].copy()
    weight = goods[["article_id", "unit_weight", "sale_unit"]].copy()
    weight["article_id"] = weight["article_id"].astype(str)
    conflicting_weight = weight.groupby("article_id").agg(
        unit_weight_count=("unit_weight", "nunique"),
        sale_unit_count=("sale_unit", "nunique"),
    )
    if conflicting_weight.gt(1).any(axis=1).any():
        raise ValueError("latest goods has conflicting unit weight or sale unit per SKU")
    weight = weight.drop_duplicates("article_id")
    product_group["article_id"] = product_group["article_id"].astype(str)
    product_group = product_group.merge(
        weight, on="article_id", how="left", validate="many_to_one"
    )
    frames = {
        "source_manifest": mirrors.manifest.copy(),
        "source_completeness": completeness,
        "product_group": product_group,
        "relation_candidates": candidates,
        "bom": bom,
        "processing": processing,
        "explicit_convert": explicit,
        "activities": activities,
        "inventory_count_audit": count_keep,
        "special_loss_coverage": special_loss.audit,
        "day_clear_audit": day_clear,
        "openings": openings,
        "conversion_events": conversion_events,
        # Compatible physical name; the current engine contains only the two consumed-usage
        # facts needed by processing backflush.  Counts and opening balances are
        # deliberately absent because they do not determine consumed output.
        "finished_processing_daily": finished_usage,
        "reporting_metrics": metrics,
        "scm_margin_audit": scm_margin_audit,
        "customer_events": customer_events,
    }
    quarantine = pd.concat([
        processing_quarantine,
        bom_quarantine,
        day_clear_quarantine,
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

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from fmetl.relations.graph import topological_order


@dataclass(frozen=True)
class ProcessingPlan:
    """Separate posting ledger from many-to-many recipe trace edges."""

    observed_ledger: pd.DataFrame
    formal_posting_ledger: pd.DataFrame
    trace: pd.DataFrame
    quarantined: pd.DataFrame


def build_processing_plan(
    actual: pd.DataFrame,
    recipes: pd.DataFrame,
    resolution: pd.DataFrame,
) -> ProcessingPlan:
    """Preserve actual SKU flows once and use recipes only for trace/controlled gap fill.

    `atomic_compose` is SKU-grained and carries no relation id. Therefore an
    observed raw quantity cannot be allocated across multiple recipes without
    extra evidence. Such recipes are quarantined instead of duplicating the raw
    flow across recipe rows.
    """
    actual_required = {
        "store_id", "business_date", "article_id", "compose_in_qty", "compose_out_qty",
    }
    recipe_required = {
        "relation_id", "raw_article_id", "finished_article_id", "raw_qty", "yield_qty", "approved",
    }
    missing_actual = sorted(actual_required - set(actual.columns))
    missing_recipe = sorted(recipe_required - set(recipes.columns))
    if missing_actual:
        raise KeyError(f"actual compose missing columns: {missing_actual}")
    if missing_recipe:
        raise KeyError(f"processing recipes missing columns: {missing_recipe}")
    resolution_required = {
        "business_date", "from_article_id", "to_article_id", "relation_type",
        "formal_flow_allowed", "relation_snapshot_id",
    }
    missing_resolution = sorted(resolution_required - set(resolution.columns))
    if missing_resolution:
        raise KeyError(f"processing resolution missing columns: {missing_resolution}")
    snapshot_ids = resolution["relation_snapshot_id"].dropna().astype(str).unique()
    if len(snapshot_ids) != 1:
        raise ValueError("processing plan requires exactly one relation snapshot")
    valid_resolution = resolution.loc[
        resolution["formal_flow_allowed"].astype(bool)
        & resolution["relation_type"].eq("RECIPE_COMPOSE")
    ].copy()
    valid_pairs = set(
        valid_resolution[["business_date", "from_article_id", "to_article_id"]]
        .astype(str).itertuples(index=False, name=None)
    )

    facts = actual[list(actual_required)].copy()
    if facts.duplicated(["store_id", "business_date", "article_id"]).any():
        raise ValueError("actual compose must be unique per store/date/article")
    facts["article_id"] = facts["article_id"].astype(str)
    facts["compose_in_qty"] = pd.to_numeric(facts["compose_in_qty"], errors="raise").fillna(0.0)
    facts["compose_out_qty"] = pd.to_numeric(facts["compose_out_qty"], errors="raise").fillna(0.0)
    if (facts[["compose_in_qty", "compose_out_qty"]] < 0).any().any():
        raise ValueError("actual compose quantities cannot be negative")
    ledger = facts.loc[
        facts["compose_in_qty"].gt(0) | facts["compose_out_qty"].gt(0)
    ].copy()
    ledger["quantity_source"] = "ACTUAL_COMPOSE"

    approved = recipes["approved"].map(
        lambda value: value if isinstance(value, bool) else str(value).strip().lower() in {"1", "true", "yes"}
    )
    recipe = recipes.loc[approved].copy()
    recipe["raw_article_id"] = recipe["raw_article_id"].astype(str)
    recipe["finished_article_id"] = recipe["finished_article_id"].astype(str)
    for column in ("raw_qty", "yield_qty"):
        recipe[column] = pd.to_numeric(recipe[column], errors="raise")
    if ((recipe["raw_qty"] <= 0) | (recipe["yield_qty"] <= 0)).any():
        raise ValueError("approved processing recipe quantities must be positive")
    # Foodmart may contain identity rows used as catalogue/tagging metadata.
    # They are not inventory transformations and must never enter the formal
    # compose DAG, otherwise a harmless A -> A row is interpreted as a cycle.
    identity_recipe = recipe.loc[
        recipe["raw_article_id"].eq(recipe["finished_article_id"])
    ].copy()
    recipe = recipe.loc[
        recipe["raw_article_id"].ne(recipe["finished_article_id"])
    ].copy()
    days = facts[["store_id", "business_date"]].drop_duplicates()
    identity_quarantine = [
        {
            "store_id": day.store_id,
            "business_date": day.business_date,
            "relation_id": relation_id,
            "reason": "IDENTITY_RECIPE_NO_TRANSFORMATION",
        }
        for day in days.itertuples(index=False)
        for relation_id in identity_recipe["relation_id"].drop_duplicates()
    ]
    if recipe.empty:
        return ProcessingPlan(
            observed_ledger=ledger,
            formal_posting_ledger=ledger.iloc[:0].copy(),
            trace=recipe,
            quarantined=pd.DataFrame(identity_quarantine),
        )
    topological_order(
        recipe[["raw_article_id", "finished_article_id"]].itertuples(index=False, name=None)
    )

    # A finished SKU must identify one recipe group. A raw SKU may occur in
    # several groups, but its aggregate actual outflow cannot then be allocated.
    finished_groups = recipe.groupby("finished_article_id")["relation_id"].nunique()
    raw_groups = recipe.groupby("raw_article_id")["relation_id"].nunique()
    ambiguous_finished = set(finished_groups[finished_groups > 1].index.astype(str))
    ambiguous_raw = set(raw_groups[raw_groups > 1].index.astype(str))

    fact_index = facts.set_index(["store_id", "business_date", "article_id"])
    trace_rows: list[dict[str, object]] = []
    quarantine_rows: list[dict[str, object]] = identity_quarantine
    derived_rows: list[dict[str, object]] = []
    for day in days.itertuples(index=False):
        for relation_id, edges in recipe.groupby("relation_id", sort=False):
            finished_values = edges["finished_article_id"].unique()
            if len(finished_values) != 1:
                quarantine_rows.append({
                    "store_id": day.store_id, "business_date": day.business_date,
                    "relation_id": relation_id, "reason": "MULTIPLE_FINISHED_SKUS",
                })
                continue
            finished_id = str(finished_values[0])
            raw_ids = set(edges["raw_article_id"].astype(str))
            unresolved = [
                raw_id for raw_id in raw_ids
                if (str(day.business_date), raw_id, finished_id) not in valid_pairs
            ]
            if unresolved:
                quarantine_rows.append({
                    "store_id": day.store_id, "business_date": day.business_date,
                    "relation_id": relation_id, "reason": "RELATION_RESOLUTION_NOT_FORMAL",
                })
                continue
            shared_raw_with_actual = {
                raw_id for raw_id in raw_ids & ambiguous_raw
                if (day.store_id, day.business_date, raw_id) in fact_index.index
                and float(fact_index.loc[(day.store_id, day.business_date, raw_id), "compose_out_qty"]) > 0
            }
            if finished_id in ambiguous_finished or shared_raw_with_actual:
                quarantine_rows.append({
                    "store_id": day.store_id, "business_date": day.business_date,
                    "relation_id": relation_id, "reason": "ATOMIC_COMPOSE_RELATION_ALLOCATION_AMBIGUOUS",
                })
                continue

            def observed(article_id: str, column: str) -> float:
                key = (day.store_id, day.business_date, article_id)
                return float(fact_index.loc[key, column]) if key in fact_index.index else 0.0

            finished_in = observed(finished_id, "compose_in_qty")
            candidates: list[float] = []
            for edge in edges.itertuples(index=False):
                raw_out = observed(str(edge.raw_article_id), "compose_out_qty")
                if raw_out > 0:
                    candidates.append(raw_out * float(edge.yield_qty) / float(edge.raw_qty))
            if finished_in <= 0 and candidates:
                if max(candidates) - min(candidates) > max(0.001, 0.01 * max(candidates)):
                    quarantine_rows.append({
                        "store_id": day.store_id, "business_date": day.business_date,
                        "relation_id": relation_id, "reason": "OBSERVED_RAW_YIELDS_CONFLICT",
                    })
                    continue
                finished_in = sum(candidates) / len(candidates)
                derived_rows.append({
                    "store_id": day.store_id, "business_date": day.business_date,
                    "article_id": finished_id, "compose_in_qty": finished_in,
                    "compose_out_qty": 0.0, "quantity_source": "RECIPE_FROM_ACTUAL_RAW",
                })
            if finished_in <= 0:
                continue

            for edge in edges.itertuples(index=False):
                raw_id = str(edge.raw_article_id)
                raw_out = observed(raw_id, "compose_out_qty")
                source = "ACTUAL_COMPOSE"
                if raw_out <= 0:
                    raw_out = finished_in * float(edge.raw_qty) / float(edge.yield_qty)
                    source = "RECIPE_FROM_ACTUAL_FINISHED"
                    derived_rows.append({
                        "store_id": day.store_id, "business_date": day.business_date,
                        "article_id": raw_id, "compose_in_qty": 0.0,
                        "compose_out_qty": raw_out, "quantity_source": source,
                    })
                trace_rows.append({
                    "store_id": day.store_id, "business_date": day.business_date,
                    "relation_id": relation_id, "raw_article_id": raw_id,
                    "finished_article_id": finished_id, "raw_out_qty": raw_out,
                    "finished_in_qty": finished_in, "raw_quantity_source": source,
                    "relation_type": "RECIPE_COMPOSE",
                    "relation_snapshot_id": snapshot_ids[0],
                    "formal_flow_allowed": True,
                    "recipe_qty_residual": raw_out - finished_in * float(edge.raw_qty) / float(edge.yield_qty),
                })

    if derived_rows:
        derived = pd.DataFrame(derived_rows)
        # A shared raw with no actual outflow can be safely derived as the sum
        # of multiple finished recipes. This aggregation happens in the posting
        # ledger, while trace rows retain relation-level contributions.
        derived = derived.groupby(
            ["store_id", "business_date", "article_id"], as_index=False
        ).agg(
            compose_in_qty=("compose_in_qty", "sum"),
            compose_out_qty=("compose_out_qty", "sum"),
            quantity_source=("quantity_source", lambda values: "+".join(sorted(set(values)))),
        )
        actual_keys = set(ledger[["store_id", "business_date", "article_id"]].itertuples(index=False, name=None))
        derived = derived.loc[
            ~derived[["store_id", "business_date", "article_id"]].apply(tuple, axis=1).isin(actual_keys)
        ]
        ledger = pd.concat([ledger, derived], ignore_index=True)
    if ledger.duplicated(["store_id", "business_date", "article_id"]).any():
        raise ValueError("processing ledger would post an SKU flow more than once")
    trace = pd.DataFrame(trace_rows)
    if not trace.empty:
        mismatched = trace["recipe_qty_residual"].abs() > (
            0.001 + 0.01 * trace["raw_out_qty"].abs()
        )
        trace.loc[mismatched, "formal_flow_allowed"] = False
        quarantine_rows.extend(
            trace.loc[mismatched, ["store_id", "business_date", "relation_id"]]
            .drop_duplicates()
            .assign(reason="ACTUAL_COMPOSE_RECIPE_QTY_MISMATCH")
            .to_dict("records")
        )
    formal_trace = trace.loc[trace["formal_flow_allowed"]].copy() if not trace.empty else trace
    if formal_trace.empty:
        formal_ledger = ledger.iloc[:0].copy()
    else:
        raw_postings = formal_trace.rename(
            columns={"raw_article_id": "article_id", "raw_out_qty": "compose_out_qty"}
        )[["store_id", "business_date", "article_id", "compose_out_qty"]]
        raw_postings["compose_in_qty"] = 0.0
        # finished_in_qty is repeated for every raw edge of one recipe. Post it
        # once per relation/finished before aggregating to the SKU ledger.
        finished_postings = (
            formal_trace.groupby(
                ["store_id", "business_date", "relation_id", "finished_article_id"],
                as_index=False,
            )["finished_in_qty"].first()
            .rename(columns={
                "finished_article_id": "article_id",
                "finished_in_qty": "compose_in_qty",
            })
        )[["store_id", "business_date", "article_id", "compose_in_qty"]]
        finished_postings["compose_out_qty"] = 0.0
        formal_ledger = (
            pd.concat([raw_postings, finished_postings], ignore_index=True)
            .groupby(["store_id", "business_date", "article_id"], as_index=False)
            .agg(compose_in_qty=("compose_in_qty", "sum"), compose_out_qty=("compose_out_qty", "sum"))
        )
        formal_ledger["quantity_source"] = "FORMAL_RECIPE_TRACE"
    return ProcessingPlan(
        observed_ledger=ledger,
        formal_posting_ledger=formal_ledger,
        trace=trace,
        quarantined=pd.DataFrame(quarantine_rows),
    )

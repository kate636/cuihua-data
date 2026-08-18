from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


DEFAULT_RECIPE_GROUP_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "v018_processing_recipe_groups.json"
)


def load_recipe_group_config(path: Path | str = DEFAULT_RECIPE_GROUP_PATH) -> pd.DataFrame:
    """Return one versioned row for every configured finished/raw recipe edge."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    version = str(payload.get("version") or "")
    rows: list[dict[str, str]] = []
    for recipe in payload.get("recipes", []):
        finished = str(recipe["finished_article_id"])
        group_id = str(recipe["recipe_group_id"])
        mode = str(recipe["recipe_mode"]).upper()
        if mode not in {"ADDITIVE", "ALTERNATIVE"}:
            raise ValueError(f"unsupported recipe_mode: {mode}")
        for raw in recipe.get("raw_article_ids", []):
            rows.append({
                "finished_article_id": finished,
                "raw_article_id": str(raw),
                "recipe_group_id": group_id,
                "recipe_mode": mode,
                "recipe_config_version": version,
            })
    frame = pd.DataFrame(rows)
    keys = ["finished_article_id", "raw_article_id"]
    if frame.empty or frame.duplicated(keys).any():
        raise ValueError("recipe group config must contain unique finished/raw edges")
    return frame


def attach_recipe_groups(
    processing: pd.DataFrame,
    *,
    path: Path | str = DEFAULT_RECIPE_GROUP_PATH,
) -> pd.DataFrame:
    """Add recipe groups and report configured raw edges missing from the export."""
    required = {"finished_article_id", "raw_article_id"}
    missing = sorted(required - set(processing.columns))
    if missing:
        raise KeyError(f"processing relation missing recipe keys: {missing}")
    configured = load_recipe_group_config(path)
    observed = processing[["finished_article_id", "raw_article_id"]].copy()
    observed[["finished_article_id", "raw_article_id"]] = observed[[
        "finished_article_id", "raw_article_id",
    ]].astype(str)
    missing_edges = configured.merge(
        observed.drop_duplicates(),
        on=["finished_article_id", "raw_article_id"],
        how="left", indicator=True, validate="one_to_one",
    ).loc[lambda frame: frame["_merge"].eq("left_only")]
    missing_by_finished = missing_edges.groupby("finished_article_id")[
        "raw_article_id"
    ].agg(lambda values: ",".join(sorted(set(map(str, values)))))
    out = processing.drop(
        columns=["recipe_group_id", "recipe_mode", "recipe_config_version"],
        errors="ignore",
    ).merge(
        configured,
        on=["finished_article_id", "raw_article_id"],
        how="left",
        validate="many_to_one",
    )
    default_group = "RECIPE|" + out["finished_article_id"].astype(str)
    out["recipe_group_id"] = out["recipe_group_id"].fillna(default_group)
    out["recipe_mode"] = out["recipe_mode"].fillna("ADDITIVE")
    out["recipe_config_version"] = out["recipe_config_version"].fillna(
        "DEFAULT_ONE_RECIPE_PER_FINISHED_SKU"
    )
    out["missing_configured_raw_article_ids"] = out[
        "finished_article_id"
    ].astype(str).map(missing_by_finished).fillna("")
    out["recipe_config_complete"] = out[
        "missing_configured_raw_article_ids"
    ].eq("")
    return out

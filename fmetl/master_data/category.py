from __future__ import annotations

from dataclasses import dataclass, field, replace
import hashlib
import json
from pathlib import Path

import pandas as pd

from fmetl.connectors.category_mapping import CategoryMappingSnapshot


RULE_PATH = Path(__file__).resolve().parents[1] / "config" / "v1_5_category_rules.json"


@dataclass(frozen=True)
class CategoryDecision:
    name: str
    reason: str


@dataclass(frozen=True)
class CategoryMapper:
    version: str
    source: str
    cooked_override_source: str
    rule_checksum: str
    evidence_status: str
    snapshot_start: str
    snapshot_end: str
    frozen_skus: frozenset[str]
    cooked_override_skus: frozenset[str]
    cooked_override_effective_from: dict[str, str]
    latest_categories: dict[str, tuple[str, str, str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if len(self.frozen_skus) != 119:
            raise ValueError(f"v1_5 frozen SKU contract requires 119 unique IDs, got {len(self.frozen_skus)}")

    def decide(
        self,
        article_id: object,
        level1: object,
        level2: object,
        level3: object,
        sale_unit: object,
        business_date: object | None = None,
        use_static_sku_overrides: bool = True,
    ) -> CategoryDecision:
        sku = "" if pd.isna(article_id) else str(article_id)
        l1 = "" if pd.isna(level1) else str(level1)
        l2 = "" if pd.isna(level2) else str(level2)
        l3 = "" if pd.isna(level3) else str(level3)
        unit = "" if pd.isna(sale_unit) else str(sale_unit)

        if use_static_sku_overrides and sku in self.frozen_skus:
            return CategoryDecision("冷冻类", "frozen_sku_119")
        if l1 == "标品类" and l2 == "冰品类":
            return CategoryDecision("冷冻类", "ice_category")
        if l2 == "蛋类":
            return CategoryDecision("蛋类", "level2_egg")
        if l2 == "烘焙类":
            return CategoryDecision("烘焙类", "level2_bakery")
        if l2 == "冷藏奶制品类":
            return CategoryDecision("冷藏乳品类", "level2_chilled_dairy")
        if l2 in {"饮料类", "酒类"}:
            return CategoryDecision("水饮类", "level2_beverage")
        if l1 == "肉禽蛋类" and l2 in {"牛肉类", "羊肉类"}:
            return CategoryDecision("牛羊类", "meat_cattle_sheep")
        if l1 == "肉禽蛋类" and l2 in {"鸡类", "鸭类", "其他禽类"}:
            return CategoryDecision("禽类", "meat_poultry")
        if l1 == "标品类" and l2 in {"方便速食类", "调味品类", "粮油副食类"}:
            return CategoryDecision("基础食品类", "standard_basic_food")
        if l1 == "标品类" and l2 == "休闲零食类":
            return CategoryDecision("休闲食品类", "standard_snack")
        if l1 == "标品类" and l2 == "日杂用品类":
            return CategoryDecision("日杂用品类", "standard_household")
        if l1 == "熟食类":
            return CategoryDecision("熟食类", "source_cooked")
        if l1 in {"冷藏及加工类", "预制菜"} and l2 == "即食类":
            return CategoryDecision("熟食类", "ready_to_eat")
        if (
            l1 in {"冷藏及加工类", "预制菜"}
            and l2 in {"即烹类", "即热类", "米面制品类"}
            and unit == "千克"
        ):
            return CategoryDecision("熟食类", "weighted_ready_food")
        business_day = "" if business_date is None or pd.isna(business_date) else str(business_date)
        override_from = self.cooked_override_effective_from.get(sku, "")
        if use_static_sku_overrides and sku in self.cooked_override_skus and (
            not business_day or not override_from or business_day >= override_from
        ):
            return CategoryDecision("熟食类", "v1_5_cooked_override")
        if l3.endswith("熟食"):
            return CategoryDecision("熟食类", "level3_cooked_suffix")
        if l1 in {"冷藏及加工类", "预制菜"}:
            return CategoryDecision("冷藏加工及预制菜类", "remaining_chilled_prepared")
        return CategoryDecision(l1, "source_level1")

    def map_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        required = {
            "article_id",
            "category_level1_description",
            "category_level2_description",
            "category_level3_description",
            "sale_unit",
        }
        missing = sorted(required - set(df.columns))
        if missing:
            raise KeyError(f"category input missing columns: {missing}")
        out = df.copy()
        out["category_authoritative_level1_description"] = ""
        if self.latest_categories:
            latest = out["article_id"].astype(str).map(self.latest_categories)
            matched = latest.notna()
            for position, column in enumerate((
                "category_level1_description",
                "category_level2_description",
                "category_level3_description",
            )):
                out.loc[matched, column] = latest.loc[matched].map(
                    lambda values: values[position]
                )
            out.loc[
                matched, "category_authoritative_level1_description"
            ] = latest.loc[matched].map(lambda values: values[0])
            out["category_mapping_source"] = "LATEST_DIM_GOODS_FALLBACK"
            out.loc[matched, "category_mapping_source"] = "MONITORING_PLATFORM_LATEST"
        else:
            out["category_mapping_source"] = "STATIC_RULE_INPUT"
        decisions = []
        for index, row in enumerate(out.itertuples(index=False)):
            platform_matched = bool(matched.iloc[index]) if self.latest_categories else False
            if platform_matched:
                decisions.append(CategoryDecision(
                    str(row.category_level1_description),
                    "monitoring_platform_latest_level1",
                ))
            else:
                decisions.append(self.decide(
                    row.article_id,
                    row.category_level1_description,
                    row.category_level2_description,
                    row.category_level3_description,
                    row.sale_unit,
                    getattr(row, "business_date", None),
                ))
        out["report_category_name"] = [decision.name for decision in decisions]
        out["report_category_code"] = out["report_category_name"]
        out["category_rule_reason"] = [decision.reason for decision in decisions]
        out["category_rule_version"] = self.version
        return out


def mapper_from_latest_snapshot(
    snapshot: CategoryMappingSnapshot,
    *,
    base: CategoryMapper | None = None,
) -> CategoryMapper:
    """Overlay one latest platform snapshot across the complete run window."""
    base_mapper = base or load_category_mapper()
    frame = snapshot.frame
    mapping = {
        str(row.article_id): (
            str(row.category_level1_description),
            str(row.category_level2_description),
            str(row.category_level3_description),
        )
        for row in frame.itertuples(index=False)
    }
    return replace(
        base_mapper,
        version=(
            f"monitoring-platform-latest-{snapshot.business_date}"
            f"-v{snapshot.version or 'unknown'}"
        ),
        source=snapshot.source_url,
        cooked_override_source=snapshot.source_url,
        rule_checksum=snapshot.snapshot.checksum,
        evidence_status="MONITORING_PLATFORM_LATEST_SNAPSHOT",
        snapshot_start=snapshot.business_date,
        snapshot_end=snapshot.business_date,
        latest_categories=mapping,
    )


def load_category_mapper(path: Path = RULE_PATH) -> CategoryMapper:
    raw = path.read_bytes()
    payload = json.loads(raw.decode("utf-8"))
    return CategoryMapper(
        version=str(payload["version"]),
        source=str(payload.get("source", "")),
        cooked_override_source=str(payload.get("cooked_override_source", "")),
        rule_checksum=hashlib.sha256(raw).hexdigest(),
        evidence_status=str(payload.get("evidence_status", "UNDECLARED")),
        snapshot_start=str(payload.get("snapshot_start") or ""),
        snapshot_end=str(payload.get("snapshot_end") or ""),
        frozen_skus=frozenset(str(value) for value in payload["frozen_skus"]),
        cooked_override_skus=frozenset(
            str(value) for value in payload.get("cooked_override_skus", [])
        ),
        cooked_override_effective_from={
            str(key): str(value)
            for key, value in payload.get("cooked_override_effective_from", {}).items()
        },
    )

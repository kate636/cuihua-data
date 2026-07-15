from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

import pandas as pd


RULE_PATH = Path(__file__).resolve().parents[1] / "config" / "v1_5_category_rules.json"


@dataclass(frozen=True)
class CategoryDecision:
    name: str
    reason: str


@dataclass(frozen=True)
class CategoryMapper:
    version: str
    frozen_skus: frozenset[str]

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
    ) -> CategoryDecision:
        sku = "" if pd.isna(article_id) else str(article_id)
        l1 = "" if pd.isna(level1) else str(level1)
        l2 = "" if pd.isna(level2) else str(level2)
        l3 = "" if pd.isna(level3) else str(level3)
        unit = "" if pd.isna(sale_unit) else str(sale_unit)

        if sku in self.frozen_skus:
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
        if sku == "21315626":
            return CategoryDecision("熟食类", "explicit_cooked_sku")
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
        decisions = [
            self.decide(row.article_id, row.category_level1_description, row.category_level2_description,
                        row.category_level3_description, row.sale_unit)
            for row in out.itertuples(index=False)
        ]
        out["report_category_name"] = [decision.name for decision in decisions]
        out["report_category_code"] = out["report_category_name"]
        out["category_rule_reason"] = [decision.reason for decision in decisions]
        out["category_rule_version"] = self.version
        return out


def load_category_mapper(path: Path = RULE_PATH) -> CategoryMapper:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return CategoryMapper(
        version=str(payload["version"]),
        frozen_skus=frozenset(str(value) for value in payload["frozen_skus"]),
    )

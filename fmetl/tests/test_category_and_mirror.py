from __future__ import annotations

import unittest

import pandas as pd

from fmetl.master_data.category import load_category_mapper
from fmetl.master_data.valid_business_day import valid_business_days
from fmetl.validation.preflight import validate_mirror_registry


class CategoryAndMirrorTests(unittest.TestCase):
    def test_v15_category_priority(self) -> None:
        mapper = load_category_mapper()
        self.assertEqual(len(mapper.frozen_skus), 119)
        self.assertEqual(mapper.decide("20691561", "水果类", "水果", "水果", "千克").name, "冷冻类")
        self.assertEqual(mapper.decide("x", "冷藏及加工类", "即食类", "x", "件").name, "熟食类")
        self.assertEqual(
            mapper.decide("x", "冷藏及加工类", "即烹类", "x", "件").name,
            "冷藏加工及预制菜类",
        )

    def test_valid_business_day_matches_v15(self) -> None:
        frame = pd.DataFrame(
            {
                "store_id": ["A3XV", "A3XV", "OTHER"],
                "inc_day": ["2026-07-01", "2026-07-02", "2026-07-01"],
                "bf19_sale_amt": [500, 499.99, 1000],
            }
        )
        result = valid_business_days(frame)
        self.assertEqual(result["business_date"].tolist(), ["2026-07-01"])

    def test_registry_is_exact_authority_subset(self) -> None:
        validate_mirror_registry()


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

import pandas as pd

from fmetl.master_data.category import load_category_mapper
from fmetl.master_data.saleability import normalize_order_saleability
from fmetl.master_data.valid_business_day import valid_business_days
from fmetl.validation.preflight import validate_mirror_registry
from fmetl.mirror.registry import EXTRACTION_CONTRACTS


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

    def test_inventory_source_contracts_cannot_be_conflated(self) -> None:
        receipt = EXTRACTION_CONTRACTS["store_receipt"]
        scm = EXTRACTION_CONTRACTS["supply_chain"]
        self.assertIn("sale_article_purchase_amt", receipt.projection)
        self.assertNotIn("out_stock_amt_cb", receipt.projection)
        self.assertIn("out_stock_pay_amt", scm.projection)
        self.assertIn("out_stock_amt_cb", scm.projection)
        self.assertNotIn("sale_article_purchase_amt", scm.projection)
        self.assertIn("Never post", scm.note)

    def test_orderable_and_saleable_are_independent(self) -> None:
        source = pd.DataFrame({
            "store_id": ["A3XV"] * 4,
            "inc_day": ["2026-07-14"] * 4,
            "article_id": ["both", "sale-only", "order-only", "neither"],
            "is_order": [1, 0, 1, 0],
            "saleable": [1, 1, 0, 0],
            "status": [1, 1, 0, 0],
        })
        result = normalize_order_saleability(source).set_index("article_id")
        self.assertEqual(
            result[["is_orderable", "is_saleable"]].to_dict(orient="index"),
            {
                "both": {"is_orderable": True, "is_saleable": True},
                "sale-only": {"is_orderable": False, "is_saleable": True},
                "order-only": {"is_orderable": True, "is_saleable": False},
                "neither": {"is_orderable": False, "is_saleable": False},
            },
        )

    def test_order_saleability_rejects_duplicate_daily_sku(self) -> None:
        source = pd.DataFrame({
            "store_id": ["A3XV", "A3XV"],
            "inc_day": ["2026-07-14", "2026-07-14"],
            "article_id": ["sku", "sku"],
            "is_order": [1, 1], "saleable": [1, 1], "status": [1, 1],
        })
        with self.assertRaises(ValueError):
            normalize_order_saleability(source)

    def test_order_saleability_rejects_null_keys_and_nonfinite_flags(self) -> None:
        base = {
            "store_id": "A3XV", "inc_day": "2026-07-14", "article_id": "sku",
            "is_order": 1, "saleable": 1, "status": 1,
        }
        for override in ({"article_id": None}, {"saleable": float("inf")}):
            with self.subTest(override=override), self.assertRaises(ValueError):
                normalize_order_saleability(pd.DataFrame([{**base, **override}]))


if __name__ == "__main__":
    unittest.main()

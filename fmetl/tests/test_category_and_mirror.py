from __future__ import annotations

import unittest

import pandas as pd

from fmetl.master_data.category import load_category_mapper
from fmetl.master_data.saleability import normalize_order_saleability
from fmetl.master_data.valid_business_day import valid_business_days
from fmetl.validation.preflight import validate_mirror_registry
from fmetl.mirror.registry import (
    EXTRACTION_CONTRACTS,
    HIVE_SOURCE_BY_MIRROR,
    SYNC_MIRROR_TABLES,
)


class CategoryAndMirrorTests(unittest.TestCase):
    def test_v15_category_priority(self) -> None:
        mapper = load_category_mapper()
        self.assertEqual(len(mapper.frozen_skus), 119)
        self.assertEqual(
            mapper.cooked_override_skus,
            frozenset({"21315626", "21316166", "21316203", "21316227"}),
        )
        self.assertEqual(mapper.decide("20691561", "水果类", "水果", "水果", "千克").name, "冷冻类")
        self.assertEqual(mapper.decide("x", "冷藏及加工类", "即食类", "x", "件").name, "熟食类")
        self.assertEqual(
            mapper.decide("x", "冷藏及加工类", "即烹类", "x", "件").name,
            "冷藏加工及预制菜类",
        )
        self.assertEqual(
            mapper.decide("21316166", "预制菜", "即烹类", "x", "份").name,
            "熟食类",
        )

    def test_v15_cooked_override_respects_business_date(self) -> None:
        mapper = load_category_mapper()
        before = mapper.decide(
            "21316166", "预制菜", "即烹类", "其他即烹类", "份", "2026-07-18"
        )
        active = mapper.decide(
            "21316166", "预制菜", "即烹类", "其他即烹类", "份", "2026-07-19"
        )
        self.assertEqual("冷藏加工及预制菜类", before.name)
        self.assertEqual("熟食类", active.name)

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

    def test_every_hive_mirror_has_a_field_contract(self) -> None:
        contracted = {contract.name for contract in EXTRACTION_CONTRACTS.values()}
        self.assertEqual(set(SYNC_MIRROR_TABLES) - contracted, set())

    def test_every_hive_mirror_has_field_manual_lineage(self) -> None:
        self.assertEqual(set(HIVE_SOURCE_BY_MIRROR), set(SYNC_MIRROR_TABLES))
        self.assertTrue(all(
            sources and all(source.startswith("hive.") for source in sources)
            for sources in HIVE_SOURCE_BY_MIRROR.values()
        ))

    def test_v014_never_imports_v15_full_link_calculation_results(self) -> None:
        projection = set(EXTRACTION_CONTRACTS["full_link_article_reference"].projection)
        forbidden = {
            "article_profit_amt", "full_link_article_profit", "scm_fin_article_profit",
            "store_lost_qty", "store_lost_amt", "init_stock_qty", "init_stock_amt",
            "end_stock_qty", "end_stock_amt", "avg_purchase_price",
        }
        self.assertFalse(projection & forbidden)

    def test_purchase_snapshot_is_not_saleability(self) -> None:
        purchase = set(EXTRACTION_CONTRACTS["purchase_order_snapshot"].projection)
        saleability = set(EXTRACTION_CONTRACTS["order_saleability"].projection)
        self.assertIn("purchase_flag", purchase)
        self.assertNotIn("saleable", purchase)
        self.assertIn("saleable", saleability)

    def test_day_clear_mirror_is_a_membership_list(self) -> None:
        contract = EXTRACTION_CONTRACTS["day_clear"]
        projection = set(contract.projection)
        self.assertNotIn("day_clear", projection)
        self.assertIn("article_id IS NOT NULL", contract.base_predicates)
        self.assertIn("business_date", projection)
        self.assertIn("article_id", projection)

    def test_inventory_source_contracts_cannot_be_conflated(self) -> None:
        receipt = EXTRACTION_CONTRACTS["store_receipt"]
        scm = EXTRACTION_CONTRACTS["supply_chain"]
        self.assertIn("sale_article_purchase_amt", receipt.projection)
        self.assertNotIn("out_stock_amt_cb", receipt.projection)
        self.assertIn("out_stock_pay_amt", scm.projection)
        self.assertIn("out_stock_amt_cb", scm.projection)
        self.assertNotIn("sale_article_purchase_amt", scm.projection)
        self.assertIn("Never post", scm.note)

    def test_duplicate_price_snapshot_is_audit_only(self) -> None:
        price = EXTRACTION_CONTRACTS["price"]
        self.assertEqual(price.grain_stage, "derived")
        self.assertIn("not joined", price.note)

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

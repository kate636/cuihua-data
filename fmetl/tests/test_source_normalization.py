from __future__ import annotations

import unittest

import pandas as pd

from fmetl.mirror.source_normalization import (
    assemble_daily_activities,
    build_startup_openings,
)
from fmetl.mirror.relation_building import build_explicit_relations
from fmetl.mirror.relation_building import build_processing_relations


class SourceNormalizationTests(unittest.TestCase):
    def test_unconfigured_multiple_processing_raws_are_isolated(self) -> None:
        raw = pd.DataFrame([
            {
                "relation_id": relation_id, "raw_article_id": raw_id,
                "finished_article_id": "UNCONFIGURED-F",
                "raw_qty": 1.0, "yield_qty": 1.0,
                "effective_from": "2026-08-05", "approved": True,
            }
            for relation_id, raw_id in (("R1", "A"), ("R2", "B"))
        ])

        result, issues = build_processing_relations(
            raw, store_id="A3XV", days=["2026-08-05"]
        )

        self.assertFalse(result["approved"].any())
        target_issues = issues.loc[issues["article_id"].eq("UNCONFIGURED-F")]
        self.assertEqual({"RECIPE_GROUP_MISSING"}, set(target_issues["reason_code"]))

    def test_processing_relation_before_effective_date_is_not_approved(self) -> None:
        raw = pd.DataFrame([{
            "relation_id": "R1", "raw_article_id": "A",
            "finished_article_id": "B", "raw_qty": 1.0, "yield_qty": 3.0,
            "effective_from": "2026-08-17", "approved": True,
        }])
        result, issues = build_processing_relations(
            raw, store_id="A3XV", days=["2026-08-11", "2026-08-17"]
        )
        approval = result.set_index("business_date")["approved"].to_dict()
        self.assertFalse(bool(approval["2026-08-11"]))
        self.assertTrue(bool(approval["2026-08-17"]))
        self.assertIn(
            "PROCESSING_RELATION_OUTSIDE_EFFECTIVE_WINDOW",
            set(issues["reason_code"]),
        )

    def test_article_convert_posts_only_unique_ctype_2_pair(self) -> None:
        base = {
            "store_id": "A3XV", "inc_day": "2026-08-05",
            "parent_rate": 2.0, "sub_rate": 0.5,
        }
        raw = pd.DataFrame([
            {**base, "parent_article_id": "BOM_A", "sub_article_id": "BOM_B", "ctype": 1},
            {**base, "parent_article_id": "PACK_A", "sub_article_id": "PACK_B", "ctype": 2},
            {**base, "parent_article_id": "MIXED_A", "sub_article_id": "MIXED_B", "ctype": 3},
        ])
        result = build_explicit_relations(raw).set_index("source_article_id")
        self.assertFalse(bool(result.loc["BOM_A", "approved"]))
        self.assertEqual("BOM_UNIT_RATIO_ONLY", result.loc["BOM_A", "exclusion_reason"])
        self.assertTrue(bool(result.loc["PACK_A", "approved"]))
        self.assertEqual(0.5, result.loc["PACK_A", "source_qty_per_target_qty"])
        self.assertFalse(bool(result.loc["MIXED_A", "approved"]))
        self.assertEqual(
            "MIXED_CONVERT_TYPE_NOT_POSTABLE",
            result.loc["MIXED_A", "exclusion_reason"],
        )

    def test_null_missing_unpriced_and_negative_opening_are_distinct(self) -> None:
        purchase = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "sale_article_id": "NULL", "init_stock_qty": None,
                "init_stock_amt": None,
            },
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "sale_article_id": "UNPRICED", "init_stock_qty": 3.0,
                "init_stock_amt": None,
            },
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "sale_article_id": "NEGATIVE", "init_stock_qty": -1.0,
                "init_stock_amt": -5.0,
            },
        ])
        result = build_startup_openings(
            purchase,
            ["NULL", "UNPRICED", "NEGATIVE", "MISSING"],
            start_day="2026-08-05",
        ).set_index("article_id")

        self.assertEqual(0.0, result.loc["NULL", "opening_qty"])
        self.assertEqual(3.0, result.loc["UNPRICED", "opening_qty"])
        self.assertEqual(-1.0, result.loc["NEGATIVE", "opening_qty"])
        self.assertEqual(-5.0, result.loc["NEGATIVE", "opening_amt"])
        self.assertEqual(
            "NEW_SKU_NO_BOOTSTRAP_ROW",
            result.loc["MISSING", "opening_source"],
        )
        self.assertEqual("ISSUE", result.loc["UNPRICED", "opening_status"])
        self.assertEqual("ISSUE", result.loc["NEGATIVE", "opening_status"])

    def test_conflicting_startup_values_are_rejected(self) -> None:
        purchase = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "sale_article_id": "A", "init_stock_qty": qty,
                "init_stock_amt": qty * 10,
            }
            for qty in (2.0, 3.0)
        ])
        with self.assertRaisesRegex(ValueError, "conflicting"):
            build_startup_openings(
                purchase, ["A"], start_day="2026-08-05"
            )

    def test_daily_grid_requires_one_authoritative_day_clear_label(self) -> None:
        keys = {
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "A",
        }
        sales = pd.DataFrame([{
            **keys, "gross_sale_qty": 1.0, "sale_return_qty": 0.0,
            "net_sale_qty": 1.0, "net_sale_amt": 10.0,
        }])
        losses = pd.DataFrame(columns=[
            "store_id", "business_date", "article_id", "known_lost_qty",
        ])
        counts = pd.DataFrame(columns=[
            "store_id", "business_date", "article_id",
            "actual_stock_qty", "is_counted",
        ])
        receipts = pd.DataFrame(columns=[
            "store_id", "business_date", "article_id", "receive_qty", "receive_amt",
        ])
        labels = pd.DataFrame([{**keys, "day_clear": "1"}])

        result = assemble_daily_activities(
            days=["2026-08-05"], article_ids=["A"], sales=sales,
            losses=losses, counts=counts, day_clear=labels, receipts=receipts,
        )
        self.assertEqual(1.0, result.loc[0, "gross_sale_qty"])
        with self.assertRaisesRegex(ValueError, "missing chdj"):
            assemble_daily_activities(
                days=["2026-08-05"], article_ids=["A"], sales=sales,
                losses=losses, counts=counts, day_clear=labels.iloc[:0],
                receipts=receipts,
            )


if __name__ == "__main__":
    unittest.main()

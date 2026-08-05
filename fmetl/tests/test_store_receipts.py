from __future__ import annotations

import unittest

import pandas as pd

from fmetl.facts.store_receipts import build_store_receipts


class StoreReceiptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.purchase = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-14", "article_id": "P",
             "sale_article_id": "C1", "day_clear": "0", "sale_article_qty": 2, "sale_article_purchase_amt": 60},
            {"store_id": "A3XV", "business_date": "2026-07-14", "article_id": "P",
             "sale_article_id": "C2", "day_clear": "0", "sale_article_qty": 3, "sale_article_purchase_amt": 40},
            {"store_id": "A3XV", "business_date": "2026-07-14", "article_id": "S",
             "sale_article_id": "S", "day_clear": "1", "sale_article_qty": 1, "sale_article_purchase_amt": 20},
        ])
        self.bridge = pd.DataFrame([
            {"store_id": "A3XV", "inc_day": "2026-07-14", "article_id": "P",
             "sale_article_id": "C1", "inbound_qty": 4, "inbound_amount": 100},
            {"store_id": "A3XV", "inc_day": "2026-07-14", "article_id": "P",
             "sale_article_id": "C2", "inbound_qty": 4, "inbound_amount": 100},
        ])

    def test_parent_receipt_is_posted_once_and_children_are_shadow(self) -> None:
        result = build_store_receipts(
            self.purchase, self.bridge,
            parent_reconstruction_keys={("A3XV", "2026-07-14", "P")},
        )
        postings = result.postings.set_index("article_id")
        self.assertEqual(set(postings.index), {"P", "S"})
        self.assertEqual(postings.loc["P", "receive_qty"], 4)
        self.assertEqual(postings.loc["P", "receive_amt"], 100)
        self.assertEqual(postings.loc["S", "receive_amt"], 20)
        self.assertTrue(result.postings["external_event_group_id"].is_unique)
        self.assertAlmostEqual(result.reconciliation.loc[0, "amount_residual"], 0)

    def test_without_parent_mode_purchase_allocations_are_direct_receipts(self) -> None:
        result = build_store_receipts(self.purchase, self.bridge)
        self.assertEqual(set(result.postings["article_id"]), {"C1", "C2", "S"})
        self.assertEqual(result.postings["receive_amt"].sum(), 120)
        self.assertTrue(result.reconciliation.empty)
        self.assertTrue(result.quarantined.empty)

    def test_direct_amount_without_quantity_is_quarantined(self) -> None:
        broken = self.purchase.copy()
        broken.loc[broken["article_id"].eq("S"), "sale_article_qty"] = None

        result = build_store_receipts(broken, self.bridge)

        self.assertNotIn("S", set(result.postings["article_id"]))
        self.assertEqual(len(result.quarantined), 1)
        self.assertEqual(
            result.quarantined.loc[0, "reason"],
            "DIRECT_RECEIPT_AMOUNT_WITHOUT_QUANTITY",
        )

    def test_missing_same_code_purchase_is_filled_from_receipt_fact(self) -> None:
        missing = self.purchase.copy()
        missing.loc[missing["article_id"].eq("S"), [
            "sale_article_qty", "sale_article_purchase_amt",
        ]] = None
        bridge = pd.concat([
            self.bridge,
            pd.DataFrame([{
                "store_id": "A3XV", "inc_day": "2026-07-14",
                "article_id": "S", "sale_article_id": "S",
                "inbound_qty": 1.0, "inbound_amount": 20.0,
            }]),
        ], ignore_index=True)

        result = build_store_receipts(missing, bridge)

        posting = result.postings.loc[result.postings["article_id"].eq("S")].iloc[0]
        self.assertEqual(1.0, posting.receive_qty)
        self.assertEqual(20.0, posting.receive_amt)
        self.assertEqual("RECEIVE_SALE_DIRECT_FALLBACK", posting.cost_source)
        self.assertTrue(posting.external_event_group_id.startswith("RECEIVE_SALE_DIRECT|"))

    def test_zero_same_code_purchase_without_receipt_stays_zero(self) -> None:
        zero = self.purchase.copy()
        zero.loc[zero["article_id"].eq("S"), [
            "sale_article_qty", "sale_article_purchase_amt",
        ]] = 0.0

        result = build_store_receipts(zero, self.bridge)

        self.assertNotIn("S", set(result.postings["article_id"]))

    def test_signed_purchase_return_remains_a_negative_external_flow(self) -> None:
        returned = self.purchase.loc[self.purchase["article_id"].eq("S")].copy()
        returned[["sale_article_qty", "sale_article_purchase_amt"]] = [-16.0, -139.2]

        result = build_store_receipts(returned, self.bridge.iloc[:0])

        posting = result.postings.iloc[0]
        self.assertEqual(-16.0, posting.receive_qty)
        self.assertEqual(-139.2, posting.receive_amt)
        self.assertEqual("EXTERNAL_NET", posting.pool_effect)
        self.assertTrue(result.quarantined.empty)

    def test_receipt_quantity_and_amount_sign_mismatch_is_blocked(self) -> None:
        broken = self.purchase.loc[self.purchase["article_id"].eq("S")].copy()
        broken[["sale_article_qty", "sale_article_purchase_amt"]] = [-1.0, 20.0]

        with self.assertRaisesRegex(ValueError, "signs must match"):
            build_store_receipts(broken, self.bridge.iloc[:0])

    def test_inconsistent_repeated_parent_values_are_blocked(self) -> None:
        broken = self.bridge.copy()
        broken.loc[1, "inbound_amount"] = 99
        with self.assertRaises(ValueError):
            build_store_receipts(
                self.purchase, broken,
                parent_reconstruction_keys={("A3XV", "2026-07-14", "P")},
            )

    def test_parent_bridge_is_required_for_every_business_date(self) -> None:
        second_day = self.purchase.iloc[[0]].copy()
        second_day["business_date"] = "2026-07-13"
        source = pd.concat([self.purchase, second_day], ignore_index=True)
        with self.assertRaises(ValueError):
            build_store_receipts(
                source, self.bridge,
                parent_reconstruction_keys={
                    ("A3XV", "2026-07-13", "P"),
                    ("A3XV", "2026-07-14", "P"),
                },
            )

    def test_zero_flow_relation_does_not_require_a_daily_bridge(self) -> None:
        zero = self.purchase.iloc[[0]].copy()
        zero["business_date"] = "2026-07-13"
        zero[["sale_article_qty", "sale_article_purchase_amt"]] = 0
        source = pd.concat([self.purchase, zero], ignore_index=True)
        result = build_store_receipts(
            source, self.bridge,
            parent_reconstruction_keys={
                ("A3XV", "2026-07-13", "P"),
                ("A3XV", "2026-07-14", "P"),
            },
        )
        self.assertEqual(set(result.postings["business_date"]), {"2026-07-14"})

    def test_null_keys_and_nonfinite_receipts_are_blocked(self) -> None:
        for column, value in (("article_id", None), ("sale_article_qty", float("inf"))):
            broken = self.purchase.copy()
            broken[column] = broken[column].astype(object)
            broken.loc[0, column] = value
            with self.subTest(column=column), self.assertRaises(ValueError):
                build_store_receipts(broken, self.bridge)


if __name__ == "__main__":
    unittest.main()

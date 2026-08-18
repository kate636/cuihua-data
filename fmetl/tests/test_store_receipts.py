from __future__ import annotations

import unittest

import pandas as pd

from fmetl.facts.store_receipts import build_store_receipts


class StoreReceiptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.purchase = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-07-14",
                "article_id": "P", "sale_article_id": "C1", "day_clear": "0",
                "sale_article_qty": 2.0, "sale_article_purchase_amt": 60.0,
            },
            {
                "store_id": "A3XV", "business_date": "2026-07-14",
                "article_id": "P", "sale_article_id": "C2", "day_clear": "0",
                "sale_article_qty": 3.0, "sale_article_purchase_amt": 40.0,
            },
            {
                "store_id": "A3XV", "business_date": "2026-07-14",
                "article_id": "S", "sale_article_id": "S", "day_clear": "1",
                "sale_article_qty": 999.0, "sale_article_purchase_amt": 9999.0,
            },
        ])
        self.receive_sale = pd.DataFrame([
            {
                "store_id": "A3XV", "inc_day": "2026-07-14",
                "article_id": "P", "sale_article_id": "C1",
                "inbound_qty": 4.0, "inbound_amount": 100.0,
            },
            {
                "store_id": "A3XV", "inc_day": "2026-07-14",
                "article_id": "P", "sale_article_id": "C2",
                "inbound_qty": 4.0, "inbound_amount": 100.0,
            },
            {
                "store_id": "A3XV", "inc_day": "2026-07-14",
                "article_id": "S", "sale_article_id": "S",
                "inbound_qty": 1.0, "inbound_amount": 20.0,
            },
        ])

    def test_receive_sale_posts_each_source_article_once(self) -> None:
        result = build_store_receipts(self.purchase, self.receive_sale)
        postings = result.postings.set_index("article_id")

        self.assertEqual({"P", "S"}, set(postings.index))
        self.assertEqual(4.0, postings.loc["P", "receive_qty"])
        self.assertEqual(100.0, postings.loc["P", "receive_amt"])
        self.assertEqual(1.0, postings.loc["S", "receive_qty"])
        self.assertEqual("RECEIVE_SALE_A_DEDUP", postings.loc["P", "cost_source"])
        self.assertTrue(result.quarantined.empty)

    def test_purchase_allocated_child_values_are_audit_only(self) -> None:
        changed = self.purchase.copy()
        changed[["sale_article_qty", "sale_article_purchase_amt"]] = [
            [200.0, 6000.0], [300.0, 4000.0], [500.0, 5000.0],
        ]

        result = build_store_receipts(changed, self.receive_sale)

        self.assertEqual(5.0, result.postings["receive_qty"].sum())
        self.assertEqual(120.0, result.postings["receive_amt"].sum())
        self.assertNotEqual(
            0.0,
            result.reconciliation["amount_residual"].abs().sum(),
        )

    def test_repeated_source_values_must_agree(self) -> None:
        broken = self.receive_sale.copy()
        broken.loc[1, "inbound_amount"] = 99.0
        with self.assertRaisesRegex(ValueError, "different A receipt"):
            build_store_receipts(self.purchase, broken)

    def test_missing_receive_sale_isolated_when_purchase_has_activity(self) -> None:
        result = build_store_receipts(
            self.purchase,
            self.receive_sale.loc[self.receive_sale["article_id"].ne("S")],
        )

        self.assertNotIn("S", set(result.postings["article_id"]))
        issue = result.quarantined.loc[
            result.quarantined["article_id"].eq("S")
        ].iloc[0]
        self.assertEqual("PURCHASE_RECEIPT_MISSING_RECEIVE_SALE", issue.reason)

    def test_signed_receive_sale_return_remains_external_net_flow(self) -> None:
        purchase = self.purchase.loc[self.purchase["article_id"].eq("S")].copy()
        receive = self.receive_sale.loc[
            self.receive_sale["article_id"].eq("S")
        ].copy()
        receive[["inbound_qty", "inbound_amount"]] = [-16.0, -139.2]

        posting = build_store_receipts(purchase, receive).postings.iloc[0]

        self.assertEqual(-16.0, posting.receive_qty)
        self.assertEqual(-139.2, posting.receive_amt)
        self.assertEqual("EXTERNAL_NET", posting.pool_effect)

    def test_receive_quantity_and_amount_sign_must_agree(self) -> None:
        receive = self.receive_sale.copy()
        receive.loc[0, ["inbound_qty", "inbound_amount"]] = [-1.0, 20.0]
        with self.assertRaisesRegex(ValueError, "signs must match"):
            build_store_receipts(self.purchase, receive)


if __name__ == "__main__":
    unittest.main()

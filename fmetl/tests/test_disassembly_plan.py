from __future__ import annotations

import unittest

import pandas as pd

from fmetl.facts.bom_plan import build_disassembly_plan, price_disassembly_plan


class DisassemblyPlanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bridge = pd.DataFrame([
            {"store_id": "A3XV", "inc_day": "2026-07-14", "article_id": "P",
             "sale_article_id": "C1", "inbound_qty": 4, "inbound_amount": 100,
             "sale_article_qty": 2, "spilit_sale_article_amt": 60, "sale_recev_rate": 0.6},
            {"store_id": "A3XV", "inc_day": "2026-07-14", "article_id": "P",
             "sale_article_id": "C2", "inbound_qty": 4, "inbound_amount": 100,
             "sale_article_qty": 3, "spilit_sale_article_amt": 40, "sale_recev_rate": 0.4},
        ])
        self.resolution = pd.DataFrame([
            {"business_date": "2026-07-14", "from_article_id": "P", "to_article_id": child,
             "relation_type": "DISASSEMBLY_BOM", "formal_flow_allowed": True,
             "relation_snapshot_id": "snapshot-1"}
            for child in ("C1", "C2")
        ])

    def test_one_parent_leg_and_many_child_legs_conserve_rolling_cost(self) -> None:
        plan = build_disassembly_plan(self.bridge, self.resolution)
        self.assertEqual(len(plan.parent_postings), 1)
        self.assertEqual(plan.parent_postings.loc[0, "parent_out_qty"], 4)
        self.assertEqual(plan.child_postings["sub_in_qty"].sum(), 5)
        priced = price_disassembly_plan(plan, pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14",
            "parent_article_id": "P", "issue_unit_cost": 30,
        }]))
        self.assertEqual(priced.parent_postings.loc[0, "parent_out_amt"], 120)
        amounts = priced.child_postings.set_index("sub_article_id")["sub_in_amt"].to_dict()
        self.assertEqual(amounts, {"C1": 72.0, "C2": 48.0})
        self.assertAlmostEqual(priced.child_postings["sub_in_amt"].sum(), 120)

    def test_source_bridge_must_conserve_parent_amount(self) -> None:
        broken = self.bridge.copy()
        broken.loc[1, "spilit_sale_article_amt"] = 30
        with self.assertRaises(ValueError):
            build_disassembly_plan(broken, self.resolution)

    def test_zero_value_gift_uses_source_rate_for_future_rolling_cost(self) -> None:
        gift = self.bridge.copy()
        gift[["inbound_amount", "spilit_sale_article_amt"]] = 0
        plan = build_disassembly_plan(gift, self.resolution)
        self.assertEqual(set(plan.trace["allocation_source"]), {"SALE_RECEV_RATE_ZERO_OR_MIXED_GIFT"})
        priced = price_disassembly_plan(plan, pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14",
            "parent_article_id": "P", "issue_unit_cost": 10,
        }]))
        amounts = priced.child_postings.set_index("sub_article_id")["sub_in_amt"].to_dict()
        self.assertEqual(amounts, {"C1": 24.0, "C2": 16.0})

    def test_mixed_zero_value_child_uses_rate_for_the_whole_parent(self) -> None:
        mixed = self.bridge.copy()
        mixed.loc[0, ["spilit_sale_article_amt", "sale_recev_rate"]] = [100, 0.8]
        mixed.loc[1, ["spilit_sale_article_amt", "sale_recev_rate"]] = [0, 0.2]
        plan = build_disassembly_plan(mixed, self.resolution)
        priced = price_disassembly_plan(plan, pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14",
            "parent_article_id": "P", "issue_unit_cost": 25,
        }]))
        amounts = priced.child_postings.set_index("sub_article_id")["sub_in_amt"].to_dict()
        self.assertEqual(amounts, {"C1": 80.0, "C2": 20.0})
        self.assertEqual(set(priced.trace["allocation_source"]), {"SALE_RECEV_RATE_ZERO_OR_MIXED_GIFT"})

    def test_disassembly_rejects_blank_keys_and_nonfinite_values(self) -> None:
        for column, value in (("article_id", ""), ("inbound_qty", float("inf"))):
            broken = self.bridge.copy()
            broken[column] = broken[column].astype(object)
            broken.loc[0, column] = value
            with self.subTest(column=column), self.assertRaises(ValueError):
                build_disassembly_plan(broken, self.resolution)


if __name__ == "__main__":
    unittest.main()

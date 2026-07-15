from __future__ import annotations

import unittest

import pandas as pd

from fmetl.facts.shadow_assembly import (
    assemble_dense_activities,
    build_bootstrap_openings,
    build_internal_event_legs,
    filter_cost_funded_internal_events,
)


class ShadowAssemblyTests(unittest.TestCase):
    def test_internal_cost_funding_propagates_and_quarantines_unfunded_event(self) -> None:
        activities = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-08", "article_id": article,
             "store_receive_amt": 100.0 if article == "seed" else 0.0}
            for article in ("seed", "middle", "finished", "missing")
        ])
        openings = pd.DataFrame([
            {"store_id": "A3XV", "article_id": article, "opening_amt": 0.0}
            for article in ("seed", "middle", "finished", "missing")
        ])
        sources = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-08", "event_group_id": "1",
             "relation_type": "PACK_CONVERT", "source_article_id": "seed", "source_out_qty": 1,
             "quantity_source": "TEST", "relation_snapshot_id": "s"},
            {"store_id": "A3XV", "business_date": "2026-07-08", "event_group_id": "2",
             "relation_type": "RECIPE_COMPOSE", "source_article_id": "middle", "source_out_qty": 1,
             "quantity_source": "TEST", "relation_snapshot_id": "s"},
            {"store_id": "A3XV", "business_date": "2026-07-08", "event_group_id": "3",
             "relation_type": "RECIPE_COMPOSE", "source_article_id": "missing", "source_out_qty": 1,
             "quantity_source": "TEST", "relation_snapshot_id": "s"},
        ])
        targets = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-08", "event_group_id": "1",
             "relation_type": "PACK_CONVERT", "target_article_id": "middle", "target_in_qty": 1,
             "amount_allocation_ratio": 1, "quantity_source": "TEST", "relation_snapshot_id": "s"},
            {"store_id": "A3XV", "business_date": "2026-07-08", "event_group_id": "2",
             "relation_type": "RECIPE_COMPOSE", "target_article_id": "finished", "target_in_qty": 1,
             "amount_allocation_ratio": 1, "quantity_source": "TEST", "relation_snapshot_id": "s"},
            {"store_id": "A3XV", "business_date": "2026-07-08", "event_group_id": "3",
             "relation_type": "RECIPE_COMPOSE", "target_article_id": "finished", "target_in_qty": 1,
             "amount_allocation_ratio": 1, "quantity_source": "TEST", "relation_snapshot_id": "s"},
        ])

        result = filter_cost_funded_internal_events(
            activities=activities, openings=openings, sources=sources, targets=targets
        )

        self.assertEqual(set(result.sources["event_group_id"]), {"1", "2"})
        self.assertEqual(result.quarantined.loc[0, "event_group_id"], "3")
        self.assertEqual(result.quarantined.loc[0, "missing_source_article_ids"], "missing")
    def test_null_bootstrap_is_zero_but_positive_unpriced_qty_is_audited(self) -> None:
        purchase = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-08", "sale_article_id": "null",
             "init_stock_qty": None, "init_stock_amt": None},
            {"store_id": "A3XV", "business_date": "2026-07-08", "sale_article_id": "unpriced",
             "init_stock_qty": 3.0, "init_stock_amt": None},
        ])

        result = build_bootstrap_openings(
            purchase, ["null", "unpriced"], start_day="2026-07-08"
        ).set_index("article_id")

        self.assertEqual(result.loc["null", "opening_qty"], 0)
        self.assertEqual(
            result.loc["null", "opening_source"], "PURCHASE_DI_NULL_BOOTSTRAP_ZERO"
        )
        self.assertEqual(result.loc["unpriced", "opening_qty"], 3)
        self.assertEqual(result.loc["unpriced", "opening_amt"], 0)
        self.assertIn("WITHOUT_COST", result.loc["unpriced", "opening_warning"])

    def test_bootstrap_selects_one_nonzero_tuple_and_audits_negative_clamp(self) -> None:
        purchase = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-08", "sale_article_id": "A", "init_stock_qty": 0, "init_stock_amt": 0},
            {"store_id": "A3XV", "business_date": "2026-07-08", "sale_article_id": "A", "init_stock_qty": 2, "init_stock_amt": 20},
            {"store_id": "A3XV", "business_date": "2026-07-08", "sale_article_id": "B", "init_stock_qty": -1, "init_stock_amt": -5},
        ])
        result = build_bootstrap_openings(purchase, ["A", "B", "C"], start_day="2026-07-08").set_index("article_id")
        self.assertEqual(result.loc["A", "opening_qty"], 2)
        self.assertEqual(result.loc["B", "opening_qty"], 0)
        self.assertEqual(result.loc["B", "opening_source"], "PURCHASE_DI_NEGATIVE_CLAMP")
        self.assertEqual(result.loc["C", "opening_source"], "NEW_SKU_NO_BOOTSTRAP_ROW")

        conflict = pd.concat([purchase, pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-08", "sale_article_id": "A",
            "init_stock_qty": 3, "init_stock_amt": 30,
        }])], ignore_index=True)
        with self.assertRaisesRegex(ValueError, "conflicting"):
            build_bootstrap_openings(conflict, ["A"], start_day="2026-07-08")

    def test_dense_activity_requires_authoritative_day_clear_for_every_sku_day(self) -> None:
        keys = {"store_id": "A3XV", "business_date": "2026-07-08", "article_id": "A"}
        sales = pd.DataFrame([{**keys, "gross_sale_qty": 1, "sale_return_qty": 0, "net_sale_qty": 1, "net_sale_amt": 10}])
        losses = pd.DataFrame(columns=["store_id", "business_date", "article_id", "known_lost_qty"])
        counts = pd.DataFrame(columns=["store_id", "business_date", "article_id", "actual_stock_qty", "is_counted"])
        labels = pd.DataFrame([{**keys, "day_clear": "1"}])
        receipts = pd.DataFrame(columns=["store_id", "business_date", "article_id", "receive_qty", "receive_amt"])
        result = assemble_dense_activities(
            days=["2026-07-08"], article_ids=["A"], sales=sales, losses=losses,
            counts=counts, day_clear=labels, receipts=receipts,
        )
        self.assertEqual(result.loc[0, "gross_sale_qty"], 1)
        with self.assertRaisesRegex(ValueError, "missing chdj"):
            assemble_dense_activities(
                days=["2026-07-08"], article_ids=["A"], sales=sales, losses=losses,
                counts=counts, day_clear=labels.iloc[:0], receipts=receipts,
            )

        bad_counts = pd.DataFrame([{
            **keys, "actual_stock_qty": 0, "is_counted": "False",
        }])
        with self.assertRaisesRegex(ValueError, "is_counted must be bool/0/1"):
            assemble_dense_activities(
                days=["2026-07-08"], article_ids=["A"], sales=sales, losses=losses,
                counts=bad_counts, day_clear=labels, receipts=receipts,
            )

    def test_relation_plans_become_one_unpriced_source_and_target_contract(self) -> None:
        bom_parent = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-08", "parent_article_id": "P",
            "parent_out_qty": 10, "relation_snapshot_id": "s",
        }])
        bom_trace = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-08", "parent_article_id": "P",
            "sub_article_id": "C", "sub_in_qty": 8, "allocation_ratio": 1,
            "relation_snapshot_id": "s", "quantity_source": "BRIDGE",
        }])
        source, target = build_internal_event_legs(
            bom_parent=bom_parent, bom_trace=bom_trace,
            pack_plan=pd.DataFrame(), compose_trace=pd.DataFrame(),
        )
        self.assertEqual(len(source), 1)
        self.assertEqual(len(target), 1)
        self.assertNotIn("amt", source.columns)
        self.assertEqual(source.loc[0, "event_group_id"], target.loc[0, "event_group_id"])


if __name__ == "__main__":
    unittest.main()

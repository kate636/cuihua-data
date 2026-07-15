from __future__ import annotations

import unittest

import pandas as pd

from fmetl.facts.pack_plan import build_pack_plan
from fmetl.facts.processing_plan import build_processing_plan
from fmetl.relations.resolver import resolve_relations


class RelationTests(unittest.TestCase):
    def test_vegetable_convert_is_not_bom(self) -> None:
        candidate = pd.DataFrame([{
            "business_date": "2026-07-01", "from_article_id": "veg-kg", "to_article_id": "veg-500g"
        }])
        convert = pd.DataFrame([{
            "store_id": "A3XV", "inc_day": "2026-07-01",
            "parent_article_id": "veg-kg", "sub_article_id": "veg-500g",
            "parent_rate": 2.0, "sub_rate": 0.5,
        }])
        bom = pd.DataFrame([
            {"parent_article_id": "veg-kg", "sub_article_id": "veg-500g",
             "category_level1_description": "蔬菜类"},
        ])
        result = resolve_relations(
            candidate, relation_snapshot_id="s1", article_convert=convert, bom_edges=bom
        )
        self.assertEqual(result.loc[0, "relation_type"], "PACK_CONVERT")

    def test_conflicting_recipe_and_pack_is_quarantined(self) -> None:
        candidate = pd.DataFrame([{
            "business_date": "2026-07-01", "from_article_id": "raw", "to_article_id": "finished"
        }])
        recipe = pd.DataFrame([{
            "raw_article_id": "raw", "finished_article_id": "finished", "approved": True,
        }])
        convert = pd.DataFrame([{
            "parent_article_id": "raw", "sub_article_id": "finished",
            "parent_rate": 1.0, "sub_rate": 1.0,
        }])
        result = resolve_relations(
            candidate, relation_snapshot_id="s1", processing_recipes=recipe, article_convert=convert
        )
        self.assertEqual(result.loc[0, "relation_type"], "QUARANTINED")
        self.assertFalse(bool(result.loc[0, "formal_flow_allowed"]))

    def test_pack_plan_requires_event_and_conserves_common_unit(self) -> None:
        event = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-01",
            "parent_article_id": "veg-kg", "sub_article_id": "veg-500g",
            "parent_qty": 1.0, "sub_qty": None, "event_source": "receive_sale",
        }])
        convert = pd.DataFrame([{
            "store_id": "A3XV", "inc_day": "2026-07-01",
            "parent_article_id": "veg-kg", "sub_article_id": "veg-500g",
            "parent_rate": 2.0, "sub_rate": 0.5,
        }])
        resolution = resolve_relations(
            event.rename(columns={
                "parent_article_id": "from_article_id", "sub_article_id": "to_article_id",
            })[["business_date", "from_article_id", "to_article_id"]],
            relation_snapshot_id="s1",
            article_convert=convert,
        )
        plan = build_pack_plan(event, convert, resolution)
        self.assertEqual(plan.loc[0, "sub_qty"], 2.0)
        self.assertAlmostEqual(plan.loc[0, "common_weight_residual"], 0.0)

    def test_unrelated_invalid_convert_edge_does_not_block_observed_pack(self) -> None:
        event = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-01",
            "parent_article_id": "veg-kg", "sub_article_id": "veg-500g",
            "parent_qty": 1.0, "sub_qty": 2.0, "event_source": "receive_sale",
        }])
        convert = pd.DataFrame([
            {"store_id": "A3XV", "inc_day": "2026-07-01",
             "parent_article_id": "veg-kg", "sub_article_id": "veg-500g",
             "parent_rate": 2.0, "sub_rate": 0.5},
            {"store_id": "A3XV", "inc_day": "2026-07-01",
             "parent_article_id": "unrelated", "sub_article_id": "bad",
             "parent_rate": 2.0, "sub_rate": 0.4},
        ])
        resolution = resolve_relations(
            event.rename(columns={
                "parent_article_id": "from_article_id", "sub_article_id": "to_article_id",
            })[["business_date", "from_article_id", "to_article_id"]],
            relation_snapshot_id="s1", article_convert=convert.iloc[[0]],
        )
        plan = build_pack_plan(event, convert, resolution)
        self.assertEqual(len(plan), 1)

    def test_pack_plan_uses_store_and_day_specific_rate(self) -> None:
        events = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-01",
             "parent_article_id": "kg", "sub_article_id": "pack",
             "parent_qty": 1.0, "sub_qty": None, "event_source": "receive_sale"},
            {"store_id": "A3XV", "business_date": "2026-07-02",
             "parent_article_id": "kg", "sub_article_id": "pack",
             "parent_qty": 1.0, "sub_qty": None, "event_source": "receive_sale"},
        ])
        convert = pd.DataFrame([
            {"store_id": "A3XV", "inc_day": "2026-07-01",
             "parent_article_id": "kg", "sub_article_id": "pack",
             "parent_rate": 2.0, "sub_rate": 0.5},
            {"store_id": "A3XV", "inc_day": "2026-07-02",
             "parent_article_id": "kg", "sub_article_id": "pack",
             "parent_rate": 4.0, "sub_rate": 0.25},
        ])
        resolution = pd.DataFrame([
            {"business_date": day, "from_article_id": "kg", "to_article_id": "pack",
             "relation_type": "PACK_CONVERT", "formal_flow_allowed": True,
             "relation_snapshot_id": "s1"}
            for day in ("2026-07-01", "2026-07-02")
        ])
        plan = build_pack_plan(events, convert, resolution).set_index("business_date")
        self.assertEqual(plan.loc["2026-07-01", "sub_qty"], 2.0)
        self.assertEqual(plan.loc["2026-07-02", "sub_qty"], 4.0)

    def test_pack_plan_rejects_bad_or_negative_actual_quantity(self) -> None:
        base = {
            "store_id": "A3XV", "business_date": "2026-07-01",
            "parent_article_id": "kg", "sub_article_id": "pack",
            "sub_qty": 2.0, "event_source": "receive_sale",
        }
        convert = pd.DataFrame([{
            "store_id": "A3XV", "inc_day": "2026-07-01",
            "parent_article_id": "kg", "sub_article_id": "pack",
            "parent_rate": 2.0, "sub_rate": 0.5,
        }])
        resolution = pd.DataFrame([{
            "business_date": "2026-07-01", "from_article_id": "kg", "to_article_id": "pack",
            "relation_type": "PACK_CONVERT", "formal_flow_allowed": True,
            "relation_snapshot_id": "s1",
        }])
        for bad_qty in (-1, "bad", "", float("inf")):
            with self.subTest(bad_qty=bad_qty), self.assertRaises(ValueError):
                build_pack_plan(pd.DataFrame([{**base, "parent_qty": bad_qty}]), convert, resolution)

    def test_invalid_convert_ratio_is_quarantined(self) -> None:
        candidate = pd.DataFrame([{
            "business_date": "2026-07-01", "from_article_id": "a", "to_article_id": "b",
        }])
        convert = pd.DataFrame([{
            "parent_article_id": "a", "sub_article_id": "b", "parent_rate": 2.0, "sub_rate": 0.4,
        }])
        result = resolve_relations(candidate, relation_snapshot_id="s1", article_convert=convert)
        self.assertEqual(result.loc[0, "relation_type"], "QUARANTINED")

    def test_unapproved_recipe_is_quarantined(self) -> None:
        candidate = pd.DataFrame([{
            "business_date": "2026-07-01", "from_article_id": "a", "to_article_id": "b",
        }])
        recipe = pd.DataFrame([{
            "raw_article_id": "a", "finished_article_id": "b", "approved": "false",
        }])
        result = resolve_relations(
            candidate, relation_snapshot_id="s1", processing_recipes=recipe
        )
        self.assertEqual(result.loc[0, "relation_type"], "QUARANTINED")

    def test_actual_compose_quantities_are_not_overwritten(self) -> None:
        actual = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-01", "article_id": "raw",
             "compose_in_qty": 0.0, "compose_out_qty": 2.0},
            {"store_id": "A3XV", "business_date": "2026-07-01", "article_id": "finished",
             "compose_in_qty": 3.0, "compose_out_qty": 0.0},
        ])
        recipe = pd.DataFrame([{
            "relation_id": "r1", "raw_article_id": "raw", "finished_article_id": "finished",
            "raw_qty": 1.0, "yield_qty": 1.0, "approved": True,
        }])
        candidates = pd.DataFrame([{
            "business_date": "2026-07-01", "from_article_id": "raw",
            "to_article_id": "finished",
        }])
        resolution = resolve_relations(
            candidates,
            relation_snapshot_id="s1",
            processing_recipes=recipe,
        )
        plan = build_processing_plan(actual, recipe, resolution)
        ledger = plan.observed_ledger.set_index("article_id")
        self.assertEqual(ledger.loc["raw", "compose_out_qty"], 2.0)
        self.assertEqual(ledger.loc["finished", "compose_in_qty"], 3.0)
        self.assertEqual(len(plan.observed_ledger), 2)
        self.assertEqual(len(plan.formal_posting_ledger), 0)
        self.assertEqual(plan.trace.loc[0, "raw_quantity_source"], "ACTUAL_COMPOSE")

    def test_quarantined_direction_cannot_leak_through_shared_sku(self) -> None:
        actual = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-01", "article_id": "A",
             "compose_in_qty": 0.0, "compose_out_qty": 1.0},
            {"store_id": "A3XV", "business_date": "2026-07-01", "article_id": "B",
             "compose_in_qty": 1.0, "compose_out_qty": 2.0},
            {"store_id": "A3XV", "business_date": "2026-07-01", "article_id": "C",
             "compose_in_qty": 10.0, "compose_out_qty": 0.0},
        ])
        recipes = pd.DataFrame([
            {"relation_id": "r1", "raw_article_id": "A", "finished_article_id": "B",
             "raw_qty": 1.0, "yield_qty": 1.0, "approved": True},
            {"relation_id": "r2", "raw_article_id": "B", "finished_article_id": "C",
             "raw_qty": 1.0, "yield_qty": 1.0, "approved": True},
        ])
        candidates = pd.DataFrame([
            {"business_date": "2026-07-01", "from_article_id": "A", "to_article_id": "B"},
            {"business_date": "2026-07-01", "from_article_id": "B", "to_article_id": "C"},
        ])
        resolution = resolve_relations(
            candidates, relation_snapshot_id="s1", processing_recipes=recipes
        )
        plan = build_processing_plan(actual, recipes, resolution)
        formal = plan.formal_posting_ledger.set_index("article_id")
        self.assertEqual(formal.loc["B", "compose_in_qty"], 1.0)
        self.assertEqual(formal.loc["B", "compose_out_qty"], 0.0)
        self.assertNotIn("C", formal.index)


if __name__ == "__main__":
    unittest.main()

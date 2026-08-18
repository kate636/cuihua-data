from __future__ import annotations

import unittest

import pandas as pd

from fmetl.calculations.ledger import SOURCE_COLUMNS, TARGET_COLUMNS
from fmetl.calculations.special_wastage import merge_special_loss_quantity
from fmetl.facts.demand_backflush import (
    audit_ssls_target_cost_coverage,
    run_ledger_with_demand_backflush,
)
from fmetl.mirror.relation_building import (
    attach_bom_event_ratios,
    build_bom_events,
    build_bom_relations,
    build_processing_relations,
)
from fmetl.mirror.report_metrics import build_customer_events
from fmetl.outputs.category_adjustment import build_ssls_category_adjustments
from fmetl.relations.registry import (
    build_product_group_candidates,
    freeze_product_group_snapshot,
)


def _activities(article_ids: list[str]) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": article_id, "day_clear": "1",
            "gross_sale_qty": 0.0, "sale_return_qty": 0.0,
            "net_sale_qty": 0.0, "net_sale_amt": 0.0,
            "known_lost_qty": 0.0, "actual_stock_qty": None,
            "is_counted": False, "store_receive_qty": 0.0,
            "store_receive_amt": 0.0, "fallback_cost": 0.0,
        }
        for article_id in article_ids
    ])


def _openings(values: dict[str, tuple[float, float]]) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "store_id": "A3XV", "article_id": article_id,
            "opening_qty": qty, "opening_amt": amt,
            "opening_source": "TEST", "opening_source_day": "2026-08-05",
        }
        for article_id, (qty, amt) in values.items()
    ])


def _registry(
    rows: list[tuple[str, str, float, str, str]],
) -> pd.DataFrame:
    output = []
    for index, (source, target, source_per_target, group_id, mode) in enumerate(rows):
        output.append({
            "store_id": "A3XV", "business_date": "2026-08-05",
            "source_article_id": source, "target_article_id": target,
            "relation_type": "PROCESSING", "status": "ACTIVE",
            "formal_flow_allowed": True,
            "source_qty_per_target_qty": source_per_target,
            "target_qty_per_source_qty": 1.0 / source_per_target,
            "relation_id": f"R{index}", "relation_version": "v018-test",
            "direction_source": "PROCESSING_RELATION_RAW_TO_FINISHED",
            "ratio_source": "RAW_QTY_DIVIDED_BY_YIELD_QTY",
            "recipe_group_id": group_id, "recipe_mode": mode,
        })
    return pd.DataFrame(output)


def _bom_unit_evidence(
    day: str,
    parent: str,
    source_qty_per_target: dict[str, float],
) -> pd.DataFrame:
    columns = [
        "store_id", "inc_day", "parent_article_id", "sub_article_id",
        "parent_rate", "sub_rate", "ctype",
    ]
    return pd.DataFrame([
        {
            "store_id": "A3XV", "inc_day": day,
            "parent_article_id": parent, "sub_article_id": child,
            "parent_rate": 1.0 / ratio, "sub_rate": ratio, "ctype": 1,
        }
        for child, ratio in source_qty_per_target.items()
    ], columns=columns)


class DemandBackflushTests(unittest.TestCase):
    def test_finished_stock_and_direct_receipt_reduce_processing_quantity(self) -> None:
        activities = _activities(["A", "B"]).set_index("article_id")
        activities.loc["B", ["store_receive_qty", "store_receive_amt"]] = [1.0, 20.0]
        activities.loc["B", ["gross_sale_qty", "net_sale_qty", "net_sale_amt"]] = [
            5.0, 5.0, 100.0,
        ]
        result = run_ledger_with_demand_backflush(
            activities.reset_index(),
            _openings({"A": (3.0, 30.0), "B": (2.0, 40.0)}),
            pd.DataFrame(columns=SOURCE_COLUMNS),
            pd.DataFrame(columns=TARGET_COLUMNS),
            _registry([("A", "B", 2.0, "B-RECIPE", "ADDITIVE")]),
        )

        trace = result.trace.iloc[0]
        self.assertEqual(2.0, trace.trigger_demand_qty)
        self.assertEqual(4.0, trace.source_out_qty)
        sku = result.ledger.sku_daily.set_index("article_id")
        self.assertEqual(0.0, sku.loc["B", "end_qty"])
        self.assertEqual(1.0, sku.loc["A", "neg_clamp_qty"])
        self.assertEqual(2.0, sku.loc["B", "compose_in_qty"])
        self.assertEqual(0.0, sku.loc["B", "pack_in_qty"])

    def test_additive_recipe_consumes_every_raw_material(self) -> None:
        activities = _activities(["R1", "R2", "F"]).set_index("article_id")
        activities.loc["F", ["gross_sale_qty", "net_sale_qty", "net_sale_amt"]] = [
            1.0, 1.0, 100.0,
        ]
        result = run_ledger_with_demand_backflush(
            activities.reset_index(),
            _openings({"R1": (10.0, 20.0), "R2": (10.0, 30.0), "F": (0.0, 0.0)}),
            pd.DataFrame(columns=SOURCE_COLUMNS),
            pd.DataFrame(columns=TARGET_COLUMNS),
            _registry([
                ("R1", "F", 2.0, "F-RECIPE", "ADDITIVE"),
                ("R2", "F", 3.0, "F-RECIPE", "ADDITIVE"),
            ]),
        )
        used = result.trace.set_index("source_article_id")["source_out_qty"]
        self.assertEqual(2.0, used["R1"])
        self.assertEqual(3.0, used["R2"])
        self.assertTrue(result.quarantined.empty)

    def test_multiple_alternative_recipes_are_isolated(self) -> None:
        activities = _activities(["R1", "R2", "F"]).set_index("article_id")
        activities.loc["F", ["gross_sale_qty", "net_sale_qty", "net_sale_amt"]] = [
            1.0, 1.0, 100.0,
        ]
        result = run_ledger_with_demand_backflush(
            activities.reset_index(),
            _openings({"R1": (10.0, 20.0), "R2": (10.0, 30.0), "F": (0.0, 0.0)}),
            pd.DataFrame(columns=SOURCE_COLUMNS),
            pd.DataFrame(columns=TARGET_COLUMNS),
            _registry([
                ("R1", "F", 2.0, "F-ALT-1", "ALTERNATIVE"),
                ("R2", "F", 3.0, "F-ALT-2", "ALTERNATIVE"),
            ]),
        )
        self.assertTrue(result.trace.empty)
        self.assertEqual(
            "AMBIGUOUS_ALTERNATIVE_OR_CONVERSION_RELATION",
            result.quarantined.iloc[0].reason_code,
        )

    def test_raw_loss_prevents_the_whole_additive_recipe(self) -> None:
        activities = _activities(["R1", "R2", "F"]).set_index("article_id")
        activities.loc["F", ["gross_sale_qty", "net_sale_qty", "net_sale_amt"]] = [
            1.0, 1.0, 100.0,
        ]
        reserved = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "R2", "reserved_loss_qty": 1.0,
        }])
        result = run_ledger_with_demand_backflush(
            activities.reset_index(),
            _openings({"R1": (10.0, 20.0), "R2": (10.0, 30.0), "F": (0.0, 0.0)}),
            pd.DataFrame(columns=SOURCE_COLUMNS),
            pd.DataFrame(columns=TARGET_COLUMNS),
            _registry([
                ("R1", "F", 2.0, "F-RECIPE", "ADDITIVE"),
                ("R2", "F", 3.0, "F-RECIPE", "ADDITIVE"),
            ]),
            reserved_raw_loss=reserved,
        )
        self.assertTrue(result.trace.empty)
        self.assertEqual(
            "PROCESSING_RAW_LOSS_PRIORITY",
            result.quarantined.iloc[0].reason_code,
        )

    def test_raw_loss_takes_priority_before_alternative_recipe_selection(self) -> None:
        activities = _activities(["R1", "R2", "F"]).set_index("article_id")
        activities.loc["F", ["gross_sale_qty", "net_sale_qty", "net_sale_amt"]] = [
            1.0, 1.0, 100.0,
        ]
        reserved = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "R1", "reserved_loss_qty": 1.0,
        }])
        result = run_ledger_with_demand_backflush(
            activities.reset_index(),
            _openings({"R1": (10.0, 20.0), "R2": (10.0, 30.0), "F": (0.0, 0.0)}),
            pd.DataFrame(columns=SOURCE_COLUMNS),
            pd.DataFrame(columns=TARGET_COLUMNS),
            _registry([
                ("R1", "F", 2.0, "F-ALT-1", "ALTERNATIVE"),
                ("R2", "F", 3.0, "F-ALT-2", "ALTERNATIVE"),
            ]),
            reserved_raw_loss=reserved,
        )
        self.assertTrue(result.trace.empty)
        self.assertEqual(
            "PROCESSING_RAW_LOSS_PRIORITY",
            result.quarantined.iloc[0].reason_code,
        )

    def test_raw_loss_does_not_cancel_product_group_conversion(self) -> None:
        activities = _activities(["A", "B"]).set_index("article_id")
        activities.loc["B", ["gross_sale_qty", "net_sale_qty", "net_sale_amt"]] = [
            1.0, 1.0, 10.0,
        ]
        registry = _registry([("A", "B", 1.0, "G1", "SINGLE")])
        registry["relation_type"] = "PRODUCT_GROUP_CONVERT"
        reserved = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "A", "reserved_loss_qty": 1.0,
        }])

        result = run_ledger_with_demand_backflush(
            activities.reset_index(),
            _openings({"A": (1.0, 5.0), "B": (0.0, 0.0)}),
            pd.DataFrame(columns=SOURCE_COLUMNS),
            pd.DataFrame(columns=TARGET_COLUMNS),
            registry,
            reserved_raw_loss=reserved,
        )

        self.assertEqual(1, len(result.trace))
        self.assertEqual("PRODUCT_GROUP_CONVERT", result.trace.iloc[0].relation_type)
        sku = result.ledger.sku_daily.set_index("article_id")
        self.assertEqual(1.0, sku.loc["B", "pack_in_qty"])
        self.assertEqual(0.0, sku.loc["B", "compose_in_qty"])
        self.assertTrue(result.quarantined.empty)


class SpecialLossCoverageTests(unittest.TestCase):
    def test_general_loss_is_used_once_then_only_uncovered_special_qty_is_added(self) -> None:
        activities = _activities(["A"])
        activities["known_lost_qty"] = 1.0
        trace = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "article_id": "A", "reason_code": "ccj", "waste_num": 2.0,
            },
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "article_id": "A", "reason_code": "ssls", "waste_num": 1.0,
            },
        ])

        result = merge_special_loss_quantity(activities, trace)
        row = result.activities.iloc[0]
        audit = result.audit.iloc[0]

        self.assertEqual(3.0, row.known_lost_qty)
        self.assertEqual(2.0, row.special_loss_supplement_qty)
        self.assertEqual(1.0, audit.covered_by_general_loss_qty)
        self.assertEqual(2.0, audit.supplemented_from_special_source_qty)

    def test_ssls_target_requires_every_recipe_raw_quantity(self) -> None:
        priority = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "F", "trigger_demand_qty": 1.0,
        }])
        registry = _registry([
            ("R1", "F", 2.0, "F-RECIPE", "ADDITIVE"),
            ("R2", "F", 3.0, "F-RECIPE", "ADDITIVE"),
        ])
        reserved = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "R1", "reserved_loss_qty": 2.0,
        }])
        ledger = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "article_id": raw, "known_lost_qty": qty,
                "issue_unit_cost": 10.0,
            }
            for raw, qty in (("R1", 2.0), ("R2", 3.0))
        ])

        audit = audit_ssls_target_cost_coverage(
            priority, registry, reserved, ledger
        ).iloc[0]

        self.assertFalse(bool(audit.covered))
        self.assertIn("R2:3.000000", audit.required_raw_qty)

    def test_ssls_quantity_cannot_cover_two_targets_twice(self) -> None:
        priority = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "article_id": target, "trigger_demand_qty": 1.0,
            }
            for target in ("F1", "F2")
        ])
        registry = _registry([
            ("R", "F1", 1.0, "F1-RECIPE", "SINGLE"),
            ("R", "F2", 1.0, "F2-RECIPE", "SINGLE"),
        ])
        reserved = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "R", "reserved_loss_qty": 1.5,
        }])
        ledger = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "R", "known_lost_qty": 1.5,
            "issue_unit_cost": 10.0,
        }])

        audit = audit_ssls_target_cost_coverage(
            priority, registry, reserved, ledger
        )

        self.assertEqual(2, len(audit))
        self.assertFalse(audit["covered"].any())

    def test_ssls_repeated_iteration_uses_final_target_demand(self) -> None:
        priority = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "article_id": "F", "trigger_demand_qty": demand,
            }
            for demand in (5.0, 8.0)
        ])
        registry = _registry([
            ("R", "F", 1.0, "F-RECIPE", "SINGLE"),
        ])
        reserved = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "R", "reserved_loss_qty": 8.0,
        }])
        ledger = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "R", "known_lost_qty": 8.0,
            "issue_unit_cost": 10.0,
        }])

        audit = audit_ssls_target_cost_coverage(
            priority, registry, reserved, ledger
        )

        self.assertEqual(1, len(audit))
        self.assertEqual(8.0, audit.iloc[0].trigger_demand_qty)
        self.assertTrue(bool(audit.iloc[0].covered))

    def test_unique_ssls_covered_alternative_selects_the_recipe(self) -> None:
        priority = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "F", "trigger_demand_qty": 1.0,
        }])
        registry = _registry([
            ("GARLIC", "F", 1.0, "GARLIC-RECIPE", "ALTERNATIVE"),
            ("SPICY", "F", 1.0, "SPICY-RECIPE", "ALTERNATIVE"),
        ])
        reserved = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "GARLIC", "reserved_loss_qty": 2.0,
        }])
        ledger = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "article_id": raw, "known_lost_qty": loss,
                "issue_unit_cost": 10.0,
            }
            for raw, loss in (("GARLIC", 2.0), ("SPICY", 0.0))
        ])

        audit = audit_ssls_target_cost_coverage(
            priority, registry, reserved, ledger
        ).iloc[0]

        self.assertTrue(bool(audit.covered))
        self.assertEqual("GARLIC-RECIPE", audit.selected_recipe_group_id)
        self.assertEqual("GARLIC:1.000000", audit.required_raw_qty)


class RelationEvidenceTests(unittest.TestCase):
    def test_configured_recipe_missing_one_raw_is_not_active(self) -> None:
        raw = pd.DataFrame([{
            "relation_id": "R1", "raw_article_id": "21326066",
            "finished_article_id": "21282423", "raw_qty": 1.0,
            "yield_qty": 1.0, "effective_from": "2026-08-01",
            "effective_to": None, "approved": True,
        }])
        relations, quarantine = build_processing_relations(
            raw, store_id="A3XV", days=("2026-08-05",)
        )
        self.assertFalse(relations["approved"].any())
        self.assertIn("RECIPE_GROUP_INCOMPLETE", set(quarantine["reason_code"]))

    def test_configured_recipe_must_have_every_raw_active_each_day(self) -> None:
        raw = pd.DataFrame([
            {
                "relation_id": relation_id, "raw_article_id": raw_id,
                "finished_article_id": "21282423", "raw_qty": 1.0,
                "yield_qty": 1.0, "effective_from": effective_from,
                "effective_to": None, "approved": True,
            }
            for relation_id, raw_id, effective_from in (
                ("R1", "21326066", "2026-08-01"),
                ("R2", "21340840", "2026-08-06"),
            )
        ])

        relations, quarantine = build_processing_relations(
            raw, store_id="A3XV", days=("2026-08-05", "2026-08-06")
        )

        day5 = relations.loc[relations["business_date"].eq("2026-08-05")]
        day6 = relations.loc[relations["business_date"].eq("2026-08-06")]
        self.assertFalse(day5["approved"].any())
        self.assertTrue(day6["approved"].all())
        self.assertIn(
            "RECIPE_GROUP_INCOMPLETE",
            set(quarantine.loc[
                quarantine["business_date"].eq("2026-08-05"), "reason_code"
            ]),
        )

    def test_duplicate_processing_relation_with_same_ratio_is_deduplicated(self) -> None:
        raw = pd.DataFrame([
            {
                "relation_id": relation_id, "raw_article_id": "R",
                "finished_article_id": "F", "raw_qty": 2.0,
                "yield_qty": 1.0, "effective_from": "2026-08-01",
                "effective_to": None, "approved": True,
            }
            for relation_id in ("R1", "R2")
        ])
        relations, quarantine = build_processing_relations(
            raw, store_id="A3XV", days=("2026-08-05",)
        )
        self.assertEqual(1, int(relations["approved"].sum()))
        self.assertTrue(quarantine.loc[quarantine["article_id"].eq("F")].empty)

    def test_duplicate_processing_relation_with_conflicting_ratio_is_quarantined(self) -> None:
        raw = pd.DataFrame([
            {
                "relation_id": relation_id, "raw_article_id": "R",
                "finished_article_id": "F", "raw_qty": raw_qty,
                "yield_qty": 1.0, "effective_from": "2026-08-01",
                "effective_to": None, "approved": True,
            }
            for relation_id, raw_qty in (("R1", 1.0), ("R2", 2.0))
        ])
        relations, quarantine = build_processing_relations(
            raw, store_id="A3XV", days=("2026-08-05",)
        )
        self.assertFalse(relations["approved"].any())
        self.assertEqual(
            "PROCESSING_RELATION_CONFLICT",
            quarantine.loc[quarantine["article_id"].eq("F")].iloc[0].reason_code,
        )

    def test_formal_bom_is_not_filtered_by_category(self) -> None:
        result = build_bom_relations(pd.DataFrame([{
            "store_id": "A3XV", "inc_day": "2026-08-05",
            "parent_article_id": "VEG-PARENT", "sub_article_id": "VEG-CHILD",
            "category_level1_description": "蔬菜类",
            "dressing_rate": 0.5, "cost_rate": 0.5,
        }]))

        self.assertTrue(bool(result.iloc[0].approved))

    def test_bom_ratio_uses_child_to_parent_unit_conversion(self) -> None:
        bom = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "parent_article_id": "P", "sub_article_id": "C",
            "source_qty_per_target_qty": None,
            "target_qty_per_source_qty": None,
        }])
        events = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "source_article_id": "P", "target_article_id": "C",
            "source_qty": 12.0, "target_qty": 3.0,
            "target_common_qty": 6.0,
        }])

        result = attach_bom_event_ratios(bom, events).iloc[0]

        self.assertEqual(2.0, result.source_qty_per_target_qty)
        self.assertEqual(0.5, result.target_qty_per_source_qty)

    def test_weight_conversions_store_both_reciprocal_ratios(self) -> None:
        pairs = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "source_article_id": "西葫芦千克",
                "target_article_id": "西葫芦500克",
            },
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "source_article_id": "海大虾12千克箱",
                "target_article_id": "海大虾千克",
            },
        ])
        groups = freeze_product_group_snapshot(pd.DataFrame([
            {
                "inc_day": "2026-08-05", "area_name": None,
                "article_group_id": group, "article_group_name": group,
                "article_id": article, "sale_unit": unit, "unit_weight": weight,
            }
            for group, article, unit, weight in (
                ("G1", "西葫芦千克", "千克", 0.0),
                ("G1", "西葫芦500克", "份", 0.5),
                ("G2", "海大虾12千克箱", "箱", 12.0),
                ("G2", "海大虾千克", "千克", 0.0),
            )
        ]))
        result = build_product_group_candidates(pairs, groups).set_index(
            "source_article_id"
        )

        self.assertEqual(
            2.0,
            result.loc["西葫芦千克", "target_qty_per_source_qty"],
        )
        self.assertEqual(
            12.0,
            result.loc["海大虾12千克箱", "target_qty_per_source_qty"],
        )
        product = (
            result["source_qty_per_target_qty"]
            * result["target_qty_per_source_qty"]
        )
        self.assertTrue(product.sub(1.0).abs().lt(0.000001).all())

    def test_bom_uses_receive_sale_child_quantity(self) -> None:
        receive = pd.DataFrame([
            {
                "store_id": "A3XV", "inc_day": "2026-08-05",
                "article_id": "P", "sale_article_id": child,
                "inbound_qty": 10.0, "inbound_amount": 100.0,
                "sale_article_qty": qty,
                "spilit_sale_article_amt": amount,
            }
            for child, qty, amount in (("C1", 6.0, 60.0), ("C2", 4.0, 40.0))
        ])
        bom = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "parent_article_id": "P", "sub_article_id": child,
                "dressing_rate": rate, "cost_rate": rate, "approved": True,
            }
            for child, rate in (("C1", 0.6), ("C2", 0.4))
        ])

        events, quarantine = build_bom_events(
            receive, bom,
            _bom_unit_evidence("2026-08-05", "P", {"C1": 1.0, "C2": 1.0}),
        )

        self.assertTrue(quarantine.empty)
        self.assertEqual(
            {"C1": 6.0, "C2": 4.0},
            events.set_index("target_article_id")["target_qty"].to_dict(),
        )
        self.assertAlmostEqual(1.0, events["amount_allocation_ratio"].sum())

    def test_bom_retains_parent_same_code_and_transfers_only_child_part(self) -> None:
        receive = pd.DataFrame([
            {
                "store_id": "A3XV", "inc_day": "2026-08-08",
                "article_id": "20001360", "sale_article_id": "20001360",
                "inbound_qty": 3.2, "inbound_amount": 8.256,
                "sale_article_qty": 2.65, "spilit_sale_article_amt": 6.837,
            },
            {
                "store_id": "A3XV", "inc_day": "2026-08-08",
                "article_id": "20001360", "sale_article_id": "21296406",
                "inbound_qty": 3.2, "inbound_amount": 8.256,
                "sale_article_qty": 0.366669, "spilit_sale_article_amt": 1.419,
            },
        ])
        bom = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-08",
            "parent_article_id": "20001360", "sub_article_id": "21296406",
            "dressing_rate": 0.66667, "cost_rate": 0.66667,
            "approved": True,
        }])

        events, quarantine = build_bom_events(
            receive, bom,
            _bom_unit_evidence("2026-08-08", "20001360", {"21296406": 1.5}),
        )

        self.assertTrue(quarantine.empty)
        self.assertEqual(1, len(events))
        self.assertAlmostEqual(0.55, events.iloc[0].source_qty)
        self.assertAlmostEqual(1.419, events.iloc[0].source_amount)
        self.assertAlmostEqual(0.366669, events.iloc[0].target_qty)

    def test_bom_same_code_only_retains_parent_without_creating_event(self) -> None:
        receive = pd.DataFrame([{
            "store_id": "A3XV", "inc_day": "2026-08-08",
            "article_id": "P", "sale_article_id": "P",
            "inbound_qty": 3.2, "inbound_amount": 8.256,
            "sale_article_qty": 3.2, "spilit_sale_article_amt": 8.256,
        }])
        bom = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-08",
            "parent_article_id": "P", "sub_article_id": "C",
            "dressing_rate": 1.0, "cost_rate": 1.0, "approved": True,
        }])

        events, quarantine = build_bom_events(
            receive, bom, _bom_unit_evidence("2026-08-08", "P", {})
        )

        self.assertTrue(events.empty)
        self.assertTrue(quarantine.empty)

    def test_bom_partial_same_code_without_child_is_incomplete(self) -> None:
        receive = pd.DataFrame([{
            "store_id": "A3XV", "inc_day": "2026-08-08",
            "article_id": "P", "sale_article_id": "P",
            "inbound_qty": 3.2, "inbound_amount": 8.256,
            "sale_article_qty": 2.0, "spilit_sale_article_amt": 5.0,
        }])
        bom = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-08",
            "parent_article_id": "P", "sub_article_id": "C",
            "dressing_rate": 1.0, "cost_rate": 1.0, "approved": True,
        }])

        events, quarantine = build_bom_events(
            receive, bom, _bom_unit_evidence("2026-08-08", "P", {})
        )

        self.assertTrue(events.empty)
        self.assertEqual("BOM_QUANTITY_EVIDENCE_INCOMPLETE", quarantine.iloc[0].reason_code)

    def test_bom_same_code_cannot_exceed_parent_receipt(self) -> None:
        receive = pd.DataFrame([{
            "store_id": "A3XV", "inc_day": "2026-08-08",
            "article_id": "P", "sale_article_id": "P",
            "inbound_qty": 3.2, "inbound_amount": 8.256,
            "sale_article_qty": 4.0, "spilit_sale_article_amt": 9.0,
        }])
        bom = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-08",
            "parent_article_id": "P", "sub_article_id": "C",
            "dressing_rate": 1.0, "cost_rate": 1.0, "approved": True,
        }])

        events, quarantine = build_bom_events(
            receive, bom, _bom_unit_evidence("2026-08-08", "P", {})
        )

        self.assertTrue(events.empty)
        self.assertEqual("BOM_QUANTITY_EVIDENCE_INCOMPLETE", quarantine.iloc[0].reason_code)

    def test_bom_child_quantity_uses_receive_sale_not_bom_weight(self) -> None:
        receive = pd.DataFrame([
            {
                "store_id": "A3XV", "inc_day": "2026-08-05",
                "article_id": "P", "sale_article_id": child,
                "inbound_qty": 10.0, "inbound_amount": 100.0,
                "sale_article_qty": qty, "spilit_sale_article_amt": amount,
            }
            for child, qty, amount in (("C1", 7.0, 60.0), ("C2", 3.0, 40.0))
        ])
        bom = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "parent_article_id": "P", "sub_article_id": child,
                "dressing_rate": rate, "cost_rate": rate, "approved": True,
            }
            for child, rate in (("C1", 0.6), ("C2", 0.4))
        ])
        events, quarantine = build_bom_events(
            receive, bom,
            _bom_unit_evidence("2026-08-05", "P", {"C1": 1.0, "C2": 1.0}),
        )
        self.assertTrue(quarantine.empty)
        self.assertEqual(
            {"C1": 7.0, "C2": 3.0},
            events.set_index("target_article_id")["target_qty"].to_dict(),
        )
        self.assertAlmostEqual(10.0, events["target_common_qty"].sum())

    def test_bom_child_quantities_must_reconstruct_parent_quantity(self) -> None:
        receive = pd.DataFrame([
            {
                "store_id": "A3XV", "inc_day": "2026-08-05",
                "article_id": "P", "sale_article_id": child,
                "inbound_qty": 10.0, "inbound_amount": 100.0,
                "sale_article_qty": qty, "spilit_sale_article_amt": amount,
            }
            for child, qty, amount in (("C1", 6.0, 60.0), ("C2", 3.0, 40.0))
        ])
        bom = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "parent_article_id": "P", "sub_article_id": child,
                "dressing_rate": rate, "cost_rate": rate, "approved": True,
            }
            for child, rate in (("C1", 0.6), ("C2", 0.4))
        ])

        events, quarantine = build_bom_events(
            receive, bom,
            _bom_unit_evidence("2026-08-05", "P", {"C1": 1.0, "C2": 1.0}),
        )

        self.assertTrue(events.empty)
        self.assertEqual(
            "BOM_QUANTITY_EVIDENCE_INCOMPLETE",
            quarantine.iloc[0].reason_code,
        )


class ReportingRuleTests(unittest.TestCase):
    def test_ssls_category_transfer_is_zero_at_store_level(self) -> None:
        sku = pd.DataFrame([
            {
                "store_no": "food mart", "business_date": "2026-08-05",
                "day_clear": day_clear, "category_level1_description": "蔬菜类",
                "ssls_ledger_cost_amt": amount,
            }
            for day_clear, amount in (("0", 20.0), ("1", 10.0))
        ])
        adjustment = build_ssls_category_adjustments(sku)
        residual = adjustment.groupby(
            ["store_no", "business_date", "day_clear"]
        )["adjustment_amt"].sum()
        self.assertTrue(residual.abs().lt(0.01).all())
        cooked = adjustment.loc[
            adjustment["category_level1_description"].eq("熟食类")
            & adjustment["day_clear"].eq("2")
        ].iloc[0]
        self.assertEqual(-30.0, cooked.adjustment_amt)

    def test_customer_classification_uses_three_states_and_19_oclock(self) -> None:
        sales = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "order_id": order, "abi_article_id": "S1",
                "qty_spec": 1.0, "order_status": status, "day_clear": "0",
            }
            for order, status in (
                ("NEW", "os.completed"), ("OLD", "os.split"),
                ("UNKNOWN", "os.completed"),
            )
        ])
        orders = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "order_id": order, "abi_article_id": "S1",
                "order_status": status, "pay_at": pay_at,
                "first_buy_flag": first_buy, "jielong_flag": jielong,
                "thirdparty_user_identity": member,
            }
            for order, status, pay_at, first_buy, jielong, member in (
                ("NEW", "os.completed", "2026-08-05 18:59:59", 1, "shop", None),
                ("OLD", "os.split", "2026-08-05 19:00:00", 0, 0, "M1"),
                ("UNKNOWN", "os.completed", "2026-08-05 10:00:00", None, 0, "M2"),
            )
        ])
        goods = pd.DataFrame([{
            "article_id": "S1", "article_name": "测试商品",
            "category_level1_id": "10",
            "category_level1_description": "蔬菜类",
            "category_level2_description": "瓜类",
            "category_level3_description": "西葫芦",
            "spu_id": "SP1", "spu_name": "测试SPU",
        }])
        profile = pd.DataFrame([{
            "sp_store_id": "A3XV", "sp_store_name": "广州滨江宏岸店",
            "store_flag_name": "翠花店",
        }])
        day_clear = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "S1", "day_clear": "1",
        }])

        result = build_customer_events(
            mirrors={
                "sales": sales, "order_offline": orders,
                "order_online": orders.iloc[:0], "store_profile": profile,
            },
            goods=goods, day_clear=day_clear, store_id="A3XV",
        ).set_index("order_id")

        self.assertTrue(bool(result.loc["NEW", "is_new"]))
        self.assertTrue(bool(result.loc["OLD", "is_old"]))
        self.assertFalse(bool(result.loc["UNKNOWN", "is_new"]))
        self.assertFalse(bool(result.loc["UNKNOWN", "is_old"]))
        self.assertTrue(bool(result.loc["NEW", "is_jielong"]))
        self.assertTrue(bool(result.loc["NEW", "is_before_19"]))
        self.assertFalse(bool(result.loc["OLD", "is_before_19"]))
        self.assertTrue(result["day_clear"].eq("1").all())

    def test_customer_first_buy_conflict_is_checked_across_whole_order(self) -> None:
        sales = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "order_id": "O1", "abi_article_id": sku,
                "qty_spec": 1.0, "order_status": "os.completed",
                "day_clear": "1",
            }
            for sku in ("S1", "S2")
        ])
        orders = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "order_id": "O1", "abi_article_id": sku,
                "order_status": "os.completed",
                "pay_at": "2026-08-05 10:00:00",
                "first_buy_flag": flag, "jielong_flag": 0,
                "thirdparty_user_identity": "M1",
            }
            for sku, flag in (("S1", 1), ("S2", 0))
        ])
        goods = pd.DataFrame([
            {
                "article_id": sku, "article_name": sku,
                "category_level1_id": "10",
                "category_level1_description": "蔬菜类",
                "category_level2_description": "瓜类",
                "category_level3_description": "测试",
                "spu_id": sku, "spu_name": sku,
            }
            for sku in ("S1", "S2")
        ])
        profile = pd.DataFrame([{
            "sp_store_id": "A3XV", "sp_store_name": "广州滨江宏岸店",
            "store_flag_name": "翠花店",
        }])
        day_clear = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "article_id": sku, "day_clear": "1",
            }
            for sku in ("S1", "S2")
        ])

        with self.assertRaisesRegex(ValueError, "one order has conflicting"):
            build_customer_events(
                mirrors={
                    "sales": sales, "order_offline": orders,
                    "order_online": orders.iloc[:0], "store_profile": profile,
                },
                goods=goods, day_clear=day_clear, store_id="A3XV",
            )


if __name__ == "__main__":
    unittest.main()

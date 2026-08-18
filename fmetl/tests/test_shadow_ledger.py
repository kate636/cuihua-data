from __future__ import annotations

from dataclasses import replace
import unittest

import pandas as pd

from fmetl.calculations.ledger import SOURCE_COLUMNS, TARGET_COLUMNS, run_weighted_ledger
from fmetl.validation.v014 import (
    assert_hard_gates,
    is_publishable,
    validate_category_evidence,
    validate_publishability,
    validate_v014_ledger,
)
from fmetl.validation.v018 import validate_v018_release_evidence
from fmetl.master_data.category import load_category_mapper, mapper_from_latest_snapshot
from fmetl.connectors.category_mapping import _snapshot_from_payload


def _activity(article_ids: list[str]) -> pd.DataFrame:
    rows = []
    for article_id in article_ids:
        rows.append({
            "store_id": "A3XV", "business_date": "2026-07-14",
            "article_id": article_id, "day_clear": "1",
            "gross_sale_qty": 0.0, "sale_return_qty": 0.0,
            "net_sale_qty": 0.0, "net_sale_amt": 0.0,
            "known_lost_qty": 0.0, "actual_stock_qty": None,
            "is_counted": False, "store_receive_qty": 0.0, "store_receive_amt": 0.0,
        })
    return pd.DataFrame(rows)


def _openings(article_ids: list[str]) -> pd.DataFrame:
    return pd.DataFrame([{
        "store_id": "A3XV", "article_id": article_id,
        "opening_qty": 0.0, "opening_amt": 0.0,
        "opening_source": "PURCHASE_DI_BOOTSTRAP", "opening_source_day": "2026-07-14",
    } for article_id in article_ids])


class ShadowLedgerTests(unittest.TestCase):
    def test_active_sku_missing_platform_category_blocks_publish(self) -> None:
        validation = validate_v018_release_evidence(
            relation_registry=pd.DataFrame(columns=[
                "status", "source_qty_per_target_qty", "target_qty_per_source_qty",
            ]),
            quarantine=pd.DataFrame(),
            category_adjustments=pd.DataFrame(),
            source_completeness=pd.DataFrame(),
            category_mapping_audit=pd.DataFrame([{
                "sku_id": "A", "active_in_window": True,
                "category_mapping_source": "LATEST_DIM_GOODS_FALLBACK",
                "category_level1_description": "旧分类",
                "category_authoritative_level1_description": "",
            }]),
            levels_result=pd.DataFrame(),
        ).set_index("check_name")

        self.assertFalse(bool(validation.loc["CATEGORY_MAPPING_COVERAGE", "passed"]))
        self.assertEqual(
            1, int(validation.loc["CATEGORY_MAPPING_COVERAGE", "failure_count"])
        )

    def test_dormant_relation_cycle_fails_hard_gate(self) -> None:
        registry = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "source_article_id": source, "target_article_id": target,
                "status": "ACTIVE", "formal_flow_allowed": True,
                "source_qty_per_target_qty": 1.0,
                "target_qty_per_source_qty": 1.0,
            }
            for source, target in (("A", "B"), ("B", "A"))
        ])
        validation = validate_v018_release_evidence(
            relation_registry=registry,
            quarantine=pd.DataFrame(),
            category_adjustments=pd.DataFrame(),
            source_completeness=pd.DataFrame(),
            category_mapping_audit=pd.DataFrame(),
            levels_result=pd.DataFrame(),
        ).set_index("check_name")

        self.assertFalse(bool(validation.loc["RELATION_GRAPH_ACYCLIC", "passed"]))

    def test_system_inventory_snapshot_cannot_cover_ledger_ending(self) -> None:
        count_audit = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "A", "actual_stock_qty": None,
            "is_counted": False, "is_explicit_operator_count": False,
            "count_status": "SYSTEM_SNAPSHOT_AUDIT_ONLY",
        }])
        sku_daily = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-08-05",
            "article_id": "A", "actual_stock_qty": 5.0,
            "is_counted": True,
        }])
        validation = validate_v018_release_evidence(
            relation_registry=pd.DataFrame(columns=[
                "status", "source_qty_per_target_qty", "target_qty_per_source_qty",
            ]),
            quarantine=pd.DataFrame(),
            category_adjustments=pd.DataFrame(),
            source_completeness=pd.DataFrame(),
            category_mapping_audit=pd.DataFrame(),
            levels_result=pd.DataFrame(),
            inventory_count_audit=count_audit,
            sku_daily=sku_daily,
        ).set_index("check_name")

        self.assertFalse(bool(
            validation.loc["OPERATOR_COUNT_SOURCE_INTEGRITY", "passed"]
        ))

        empty_source = count_audit.iloc[0:0].copy()
        validation_without_source = validate_v018_release_evidence(
            relation_registry=pd.DataFrame(columns=[
                "status", "source_qty_per_target_qty", "target_qty_per_source_qty",
            ]),
            quarantine=pd.DataFrame(),
            category_adjustments=pd.DataFrame(),
            source_completeness=pd.DataFrame(),
            category_mapping_audit=pd.DataFrame(),
            levels_result=pd.DataFrame(),
            inventory_count_audit=empty_source,
            sku_daily=sku_daily,
        ).set_index("check_name")
        self.assertFalse(bool(
            validation_without_source.loc[
                "OPERATOR_COUNT_SOURCE_INTEGRITY", "passed"
            ]
        ))

    def test_dormant_official_bom_cycle_also_fails_hard_gate(self) -> None:
        registry = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-08-05",
                "source_article_id": source, "target_article_id": target,
                "relation_type": "BOM", "status": "EVIDENCE_ONLY",
                "formal_flow_allowed": False,
                "source_qty_per_target_qty": None,
                "target_qty_per_source_qty": None,
            }
            for source, target in (("A", "B"), ("B", "A"))
        ])
        validation = validate_v018_release_evidence(
            relation_registry=registry,
            quarantine=pd.DataFrame(),
            category_adjustments=pd.DataFrame(),
            source_completeness=pd.DataFrame(),
            category_mapping_audit=pd.DataFrame(),
            levels_result=pd.DataFrame(),
        ).set_index("check_name")

        self.assertTrue(bool(
            validation.loc["RELATION_RATIO_RECIPROCAL", "passed"]
        ))
        self.assertFalse(bool(
            validation.loc["RELATION_GRAPH_ACYCLIC", "passed"]
        ))

    def test_legacy_category_snapshot_blocks_publish_for_later_window(self) -> None:
        gate = validate_category_evidence(
            load_category_mapper(),
            start_date="2026-08-03",
            end_date="2026-08-04",
        )
        self.assertFalse(bool(gate.iloc[0]["passed"]))
        self.assertEqual("PUBLISH", gate.iloc[0]["gate_type"])
        self.assertEqual("CATEGORY_SNAPSHOT_EVIDENCE", gate.iloc[0]["check_name"])

    def test_dated_category_snapshot_covering_window_can_publish(self) -> None:
        mapper = replace(
            load_category_mapper(),
            evidence_status="DATED_IMMUTABLE_SNAPSHOT",
            snapshot_start="2026-08-03",
            snapshot_end="2026-08-04",
        )
        gate = validate_category_evidence(
            mapper,
            start_date="2026-08-03",
            end_date="2026-08-04",
        )
        self.assertTrue(bool(gate.iloc[0]["passed"]))

    def test_latest_platform_category_is_uniformly_applied_to_earlier_window(self) -> None:
        payload = {
            "business_date": "2026-08-12", "generated_at": "2026-08-13 09:48:17",
            "version": 1, "stale": False, "sync_error": None,
            "items": [{
                "article_id": "A",
                "category_level1_description": "水饮类",
                "category_level2_description": "冷藏奶制品类",
                "category_level3_description": "茶饮类",
            }],
        }
        mapper = mapper_from_latest_snapshot(
            _snapshot_from_payload(payload, source_url="test://category")
        )
        mapped = mapper.map_frame(pd.DataFrame([{
            "article_id": "A", "business_date": "2026-08-06",
            "category_level1_description": "旧类",
            "category_level2_description": "旧中类",
            "category_level3_description": "旧小类", "sale_unit": "瓶",
        }]))
        self.assertEqual("水饮类", mapped.iloc[0]["report_category_name"])
        self.assertEqual("冷藏奶制品类", mapped.iloc[0]["category_level2_description"])
        self.assertEqual("MONITORING_PLATFORM_LATEST", mapped.iloc[0]["category_mapping_source"])
        self.assertEqual(
            "水饮类",
            mapped.iloc[0]["category_authoritative_level1_description"],
        )
        gate = validate_category_evidence(
            mapper, start_date="2026-08-06", end_date="2026-08-11"
        )
        self.assertTrue(bool(gate.iloc[0]["passed"]))

    def test_platform_category_cannot_be_changed_by_old_rule(self) -> None:
        validation = validate_v018_release_evidence(
            relation_registry=pd.DataFrame(columns=[
                "status", "source_qty_per_target_qty", "target_qty_per_source_qty",
            ]),
            quarantine=pd.DataFrame(),
            category_adjustments=pd.DataFrame(),
            source_completeness=pd.DataFrame(),
            category_mapping_audit=pd.DataFrame([{
                "sku_id": "A", "active_in_window": True,
                "category_mapping_source": "MONITORING_PLATFORM_LATEST",
                "category_level1_description": "冷藏乳品类",
                "category_authoritative_level1_description": "水饮类",
            }]),
            levels_result=pd.DataFrame(),
        ).set_index("check_name")

        self.assertFalse(bool(
            validation.loc["CATEGORY_MAPPING_VALUE_MATCH", "passed"]
        ))

    def test_uncovered_ssls_target_blocks_even_when_finished_cost_is_positive(self) -> None:
        validation = validate_v018_release_evidence(
            relation_registry=pd.DataFrame(columns=[
                "status", "source_qty_per_target_qty", "target_qty_per_source_qty",
            ]),
            quarantine=pd.DataFrame(),
            category_adjustments=pd.DataFrame(),
            source_completeness=pd.DataFrame(),
            category_mapping_audit=pd.DataFrame(),
            levels_result=pd.DataFrame(),
            ssls_target_cost_audit=pd.DataFrame([{
                "store_id": "A3XV", "business_date": "2026-08-08",
                "article_id": "F", "covered": False,
                "coverage_reason": "processing recipe is not uniquely determined",
            }]),
        ).set_index("check_name")

        self.assertFalse(bool(
            validation.loc["SSLS_TARGET_COST_COVERAGE", "passed"]
        ))

    def test_zero_cost_outflow_blocks_publish_but_keeps_diagnostic_run(self) -> None:
        activity = _activity(["A"])
        activity.loc[0, ["gross_sale_qty", "net_sale_qty", "net_sale_amt"]] = [
            1.0, 1.0, 10.0,
        ]
        empty_source = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "source_article_id", "source_out_qty", "quantity_source", "relation_snapshot_id",
        ])
        empty_target = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "target_article_id", "target_in_qty", "amount_allocation_ratio",
            "quantity_source", "relation_snapshot_id",
        ])
        ledger = run_weighted_ledger(
            activity, _openings(["A"]), empty_source, empty_target
        )
        hard = validate_v014_ledger(ledger.sku_daily, ledger.internal_postings)
        publish = validate_publishability(ledger.sku_daily)
        validation = pd.concat([hard, publish], ignore_index=True)
        assert_hard_gates(validation)
        self.assertFalse(is_publishable(validation))
        self.assertEqual(
            1,
            int(
                validation.loc[
                    validation["check_name"].eq("ISSUE_COST_EVIDENCE"),
                    "failure_count",
                ].iloc[0]
            ),
        )

    def test_empty_pool_uses_reference_fallback_without_creating_inventory(self) -> None:
        activity = _activity(["A"])
        activity["fallback_cost"] = 6.5
        activity.loc[0, ["gross_sale_qty", "net_sale_qty", "net_sale_amt"]] = [
            2.0, 2.0, 20.0,
        ]
        empty_source = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "source_article_id", "source_out_qty", "quantity_source", "relation_snapshot_id",
        ])
        empty_target = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "target_article_id", "target_in_qty", "amount_allocation_ratio",
            "quantity_source", "relation_snapshot_id",
        ])

        row = run_weighted_ledger(
            activity, _openings(["A"]), empty_source, empty_target
        ).sku_daily.iloc[0]

        self.assertEqual(6.5, row.issue_unit_cost)
        self.assertEqual("INVENTORY_POOL_DAILY_COST_PRICE", row.issue_cost_source)
        self.assertEqual(0.0, row.end_qty)
        self.assertEqual(0.0, row.end_amt)
        self.assertEqual(13.0, row.neg_clamp_cost_amt)
        self.assertEqual(7.0, row.accounting_profit)

    def test_previous_day_issue_cost_is_not_reused(self) -> None:
        activity = pd.concat([_activity(["A"]), _activity(["A"])], ignore_index=True)
        activity.loc[0, [
            "store_receive_qty", "store_receive_amt", "gross_sale_qty",
            "net_sale_qty", "net_sale_amt",
        ]] = [1.0, 5.0, 1.0, 1.0, 10.0]
        activity.loc[1, "business_date"] = "2026-07-15"
        activity.loc[1, ["gross_sale_qty", "net_sale_qty", "net_sale_amt"]] = [
            1.0, 1.0, 10.0,
        ]
        activity["fallback_cost"] = [9.0, 9.0]
        empty_source = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "source_article_id", "source_out_qty", "quantity_source", "relation_snapshot_id",
        ])
        empty_target = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "target_article_id", "target_in_qty", "amount_allocation_ratio",
            "quantity_source", "relation_snapshot_id",
        ])

        rows = run_weighted_ledger(
            activity, _openings(["A"]), empty_source, empty_target
        ).sku_daily.set_index("business_date")

        self.assertEqual(5.0, rows.loc["2026-07-14", "issue_unit_cost"])
        self.assertEqual(9.0, rows.loc["2026-07-15", "issue_unit_cost"])
        self.assertEqual(
            "INVENTORY_POOL_DAILY_COST_PRICE",
            rows.loc["2026-07-15", "issue_cost_source"],
        )

    def test_signed_purchase_return_rolls_through_profit_and_ending_inventory(self) -> None:
        activity = _activity(["A"])
        activity.loc[0, [
            "store_receive_qty", "store_receive_amt", "gross_sale_qty",
            "net_sale_qty", "net_sale_amt",
        ]] = [-16.0, -139.2, 3.0, 3.0, 35.7]
        opening = _openings(["A"])
        opening.loc[0, ["opening_qty", "opening_amt"]] = [22.0, 191.4]
        empty_source = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "source_article_id", "source_out_qty", "quantity_source", "relation_snapshot_id",
        ])
        empty_target = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "target_article_id", "target_in_qty", "amount_allocation_ratio",
            "quantity_source", "relation_snapshot_id",
        ])

        row = run_weighted_ledger(
            activity, opening, empty_source, empty_target
        ).sku_daily.iloc[0]

        self.assertAlmostEqual(8.7, row.issue_unit_cost)
        self.assertAlmostEqual(3.0, row.end_qty)
        self.assertAlmostEqual(26.1, row.end_amt)
        self.assertAlmostEqual(9.6, row.accounting_profit)

    def test_bom_posting_keeps_old_parent_inventory_and_moves_receipt_amount(self) -> None:
        activity = _activity(["P", "C"])
        activity.loc[activity["article_id"].eq("P"), [
            "store_receive_qty", "store_receive_amt",
        ]] = [10.0, 100.0]
        opening = _openings(["P", "C"])
        opening.loc[opening["article_id"].eq("P"), [
            "opening_qty", "opening_amt",
        ]] = [10.0, 50.0]
        sources = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14",
            "event_group_id": "B1", "relation_type": "DISASSEMBLY_BOM",
            "source_article_id": "P", "source_out_qty": 10.0,
            "quantity_source": "RECEIVE_SALE_ACTUAL_PARENT_CHILD_QTY",
            "relation_snapshot_id": "r1", "specified_source_out_amt": 100.0,
            "specified_cost_source": "RECEIVE_SALE_PARENT_RECEIPT_AMOUNT",
        }])
        targets = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14",
            "event_group_id": "B1", "relation_type": "DISASSEMBLY_BOM",
            "target_article_id": "C", "target_in_qty": 10.0,
            "amount_allocation_ratio": 1.0,
            "quantity_source": "RECEIVE_SALE_ACTUAL_PARENT_CHILD_QTY",
            "relation_snapshot_id": "r1",
        }])

        result = run_weighted_ledger(activity, opening, sources, targets)
        daily = result.sku_daily.set_index("article_id")
        postings = result.internal_postings.set_index("posting_role")

        self.assertEqual(10.0, daily.loc["P", "end_qty"])
        self.assertEqual(50.0, daily.loc["P", "end_amt"])
        self.assertEqual(0.0, daily.loc["P", "accounting_profit"])
        self.assertEqual(10.0, daily.loc["C", "end_qty"])
        self.assertEqual(100.0, daily.loc["C", "end_amt"])
        self.assertEqual(100.0, postings.loc["OUT", "amt"])
        self.assertEqual(100.0, postings.loc["IN", "amt"])

    def test_external_observations_are_preserved_exactly(self) -> None:
        activity = _activity(["A"])
        activity.loc[0, [
            "gross_sale_qty", "sale_return_qty", "net_sale_qty", "net_sale_amt",
            "known_lost_qty", "store_receive_qty", "store_receive_amt",
        ]] = [2.0, 1.0, 1.0, 20.0, 0.5, 5.0, 50.0]
        empty_source = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "source_article_id", "source_out_qty", "quantity_source", "relation_snapshot_id",
        ])
        empty_target = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "target_article_id", "target_in_qty", "amount_allocation_ratio",
            "quantity_source", "relation_snapshot_id",
        ])
        ledger = run_weighted_ledger(
            activity, _openings(["A"]), empty_source, empty_target
        )
        validation = validate_v014_ledger(
            ledger.sku_daily,
            ledger.internal_postings,
            source_activities=activity,
        ).set_index("check_name")
        self.assertTrue(
            bool(validation.loc["EXTERNAL_OBSERVATION_CONSERVATION", "passed"])
        )

    def test_raw_loss_priority_gate_checks_processing_trace(self) -> None:
        activity = _activity(["R"])
        empty_source = pd.DataFrame(columns=SOURCE_COLUMNS)
        empty_target = pd.DataFrame(columns=TARGET_COLUMNS)
        ledger = run_weighted_ledger(
            activity, _openings(["R"]), empty_source, empty_target
        )
        trace = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14",
            "source_article_id": "R", "relation_type": "PROCESSING",
            "source_out_qty": 1.0,
        }])
        reserved = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14",
            "article_id": "R", "reserved_loss_qty": 1.0,
        }])

        validation = validate_v014_ledger(
            ledger.sku_daily,
            ledger.internal_postings,
            reserved_raw_loss=reserved,
            processing_trace=trace,
        ).set_index("check_name")

        self.assertFalse(bool(validation.loc["PROCESSING_RAW_LOSS_PRIORITY", "passed"]))

    def test_bom_pack_compose_chain_prices_once_and_conserves_internal_amount(self) -> None:
        articles = ["P", "C1", "C2", "K", "R", "F"]
        activity = _activity(articles).set_index("article_id")
        activity.loc["P", ["store_receive_qty", "store_receive_amt"]] = [10.0, 100.0]
        activity.loc["R", ["store_receive_qty", "store_receive_amt"]] = [2.0, 40.0]
        activity.loc["F", ["gross_sale_qty", "net_sale_qty", "net_sale_amt"]] = [1.0, 1.0, 50.0]
        activity = activity.reset_index()

        sources = pd.DataFrame([
            {"event_group_id": "B", "relation_type": "DISASSEMBLY_BOM", "source_article_id": "P", "source_out_qty": 10.0},
            {"event_group_id": "K", "relation_type": "PACK_CONVERT", "source_article_id": "C1", "source_out_qty": 6.0},
            {"event_group_id": "F", "relation_type": "RECIPE_COMPOSE", "source_article_id": "K", "source_out_qty": 6.0},
            {"event_group_id": "F", "relation_type": "RECIPE_COMPOSE", "source_article_id": "R", "source_out_qty": 2.0},
        ])
        sources["store_id"] = "A3XV"
        sources["business_date"] = "2026-07-14"
        sources["quantity_source"] = "TEST"
        sources["relation_snapshot_id"] = "snapshot"
        targets = pd.DataFrame([
            {"event_group_id": "B", "relation_type": "DISASSEMBLY_BOM", "target_article_id": "C1", "target_in_qty": 6.0, "amount_allocation_ratio": 0.6},
            {"event_group_id": "B", "relation_type": "DISASSEMBLY_BOM", "target_article_id": "C2", "target_in_qty": 4.0, "amount_allocation_ratio": 0.4},
            {"event_group_id": "K", "relation_type": "PACK_CONVERT", "target_article_id": "K", "target_in_qty": 6.0, "amount_allocation_ratio": 1.0},
            {"event_group_id": "F", "relation_type": "RECIPE_COMPOSE", "target_article_id": "F", "target_in_qty": 3.0, "amount_allocation_ratio": 1.0},
        ])
        targets["store_id"] = "A3XV"
        targets["business_date"] = "2026-07-14"
        targets["quantity_source"] = "TEST"
        targets["relation_snapshot_id"] = "snapshot"

        result = run_weighted_ledger(activity, _openings(articles), sources, targets)
        sku = result.sku_daily.set_index("article_id")
        self.assertAlmostEqual(sku.loc["P", "issue_unit_cost"], 10.0)
        self.assertAlmostEqual(sku.loc["C1", "bom_in_amt"], 60.0)
        self.assertAlmostEqual(sku.loc["K", "pack_in_amt"], 60.0)
        self.assertAlmostEqual(sku.loc["F", "compose_in_amt"], 100.0)
        self.assertAlmostEqual(sku.loc["F", "issue_unit_cost"], 100 / 3)
        self.assertAlmostEqual(sku.loc["F", "end_amt"], 200 / 3)
        posting = result.internal_postings.groupby("posting_role")["amt"].sum()
        self.assertAlmostEqual(posting["OUT"], posting["IN"])
        self.assertAlmostEqual(sku["accounting_profit"].sum(), 50 - 140 + 40 + 200 / 3)

    def test_return_uses_same_day_cost_and_missing_cost_blocks_publish(self) -> None:
        activity = _activity(["A"])
        activity.loc[0, ["sale_return_qty", "net_sale_qty", "net_sale_amt"]] = [1.0, -1.0, -12.0]
        opening = _openings(["A"])
        opening.loc[0, ["opening_qty", "opening_amt"]] = [2.0, 20.0]
        empty_sources = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "source_article_id", "source_out_qty", "quantity_source", "relation_snapshot_id",
        ])
        empty_targets = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "target_article_id", "target_in_qty", "amount_allocation_ratio",
            "quantity_source", "relation_snapshot_id",
        ])
        result = run_weighted_ledger(activity, opening, empty_sources, empty_targets)
        self.assertEqual(result.sku_daily.loc[0, "sale_return_cost_amt"], 10)
        self.assertEqual(result.sku_daily.loc[0, "end_amt"], 30)
        missing = run_weighted_ledger(
            activity, _openings(["A"]), empty_sources, empty_targets
        )
        publish = validate_publishability(missing.sku_daily).iloc[0]
        self.assertFalse(bool(publish.passed))

        mixed = _activity(["A"])
        mixed.loc[0, ["sale_return_qty", "net_sale_qty", "net_sale_amt"]] = [1.0, -1.0, -12.0]
        mixed.loc[0, ["store_receive_qty", "store_receive_amt"]] = [9.0, 180.0]
        mixed_opening = _openings(["A"])
        mixed_opening.loc[0, ["opening_qty", "opening_amt"]] = [1.0, 10.0]
        mixed_result = run_weighted_ledger(mixed, mixed_opening, empty_sources, empty_targets)
        self.assertEqual(mixed_result.sku_daily.loc[0, "sale_return_cost_basis"], 19)

    def test_fixed_amount_bom_does_not_require_daily_issue_unit_cost(self) -> None:
        activity = _activity(["P", "C"])
        activity.loc[activity["article_id"].eq("P"), [
            "store_receive_qty", "store_receive_amt",
        ]] = [1.0, 10.0]
        source = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14",
            "event_group_id": "B", "relation_type": "DISASSEMBLY_BOM",
            "source_article_id": "P", "source_out_qty": 1.0,
            "quantity_source": "RECEIVE_SALE", "relation_snapshot_id": "s",
            "specified_source_out_amt": 10.0,
            "specified_cost_source": "RECEIVE_SALE_PARENT_RECEIPT_AMOUNT",
        }])
        target = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14",
            "event_group_id": "B", "relation_type": "DISASSEMBLY_BOM",
            "target_article_id": "C", "target_in_qty": 1.0,
            "amount_allocation_ratio": 1.0, "quantity_source": "RECEIVE_SALE",
            "relation_snapshot_id": "s",
        }])

        result = run_weighted_ledger(
            activity, _openings(["P", "C"]), source, target
        ).sku_daily

        parent = result.loc[result["article_id"].eq("P")].iloc[0]
        self.assertEqual(10.0, parent.bom_out_amt)
        self.assertTrue(bool(validate_publishability(result).iloc[0].passed))

    def test_return_after_zero_stock_does_not_reuse_previous_day_cost(self) -> None:
        activity = pd.concat([_activity(["A"]), _activity(["A"])], ignore_index=True)
        activity.loc[0, ["store_receive_qty", "store_receive_amt", "gross_sale_qty",
                         "net_sale_qty", "net_sale_amt"]] = [1.0, 10.0, 1.0, 1.0, 20.0]
        activity.loc[1, "business_date"] = "2026-07-15"
        activity.loc[1, ["sale_return_qty", "net_sale_qty", "net_sale_amt"]] = [
            1.0, -1.0, -20.0
        ]
        empty_sources = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "source_article_id", "source_out_qty", "quantity_source", "relation_snapshot_id",
        ])
        empty_targets = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "target_article_id", "target_in_qty", "amount_allocation_ratio",
            "quantity_source", "relation_snapshot_id",
        ])
        result = run_weighted_ledger(
            activity, _openings(["A"]), empty_sources, empty_targets
        ).sku_daily.set_index("business_date")
        self.assertEqual(0.0, result.loc["2026-07-14", "end_qty"])
        self.assertEqual(0.0, result.loc["2026-07-15", "sale_return_cost_basis"])
        self.assertEqual(0.0, result.loc["2026-07-15", "end_amt"])
        publish = validate_publishability(result.reset_index()).iloc[0]
        self.assertFalse(bool(publish.passed))

    def test_ssls_raw_loss_covers_finished_sale_but_not_return_valuation(self) -> None:
        activity = _activity(["F"])
        activity.loc[0, ["gross_sale_qty", "net_sale_qty", "net_sale_amt"]] = [
            1.0, 1.0, 20.0,
        ]
        empty_sources = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "source_article_id", "source_out_qty", "quantity_source", "relation_snapshot_id",
        ])
        empty_targets = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "target_article_id", "target_in_qty", "amount_allocation_ratio",
            "quantity_source", "relation_snapshot_id",
        ])
        result = run_weighted_ledger(
            activity, _openings(["F"]), empty_sources, empty_targets
        )
        coverage = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14",
            "article_id": "F",
        }])
        publish = validate_publishability(
            result.sku_daily, ssls_covered_targets=coverage
        ).iloc[0]
        self.assertTrue(bool(publish.passed))

        returned = result.sku_daily.copy()
        returned["sale_return_qty"] = 1.0
        returned["sale_return_cost_basis"] = 0.0
        publish = validate_publishability(
            returned, ssls_covered_targets=coverage
        ).iloc[0]
        self.assertFalse(bool(publish.passed))

    def test_same_day_return_can_net_against_gross_sale_without_inventing_cost(self) -> None:
        activity = _activity(["A"])
        activity.loc[0, [
            "gross_sale_qty", "sale_return_qty", "net_sale_qty", "net_sale_amt",
        ]] = [2.608, 0.396, 2.212, 35.16]
        empty_sources = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "source_article_id", "source_out_qty", "quantity_source", "relation_snapshot_id",
        ])
        empty_targets = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type",
            "target_article_id", "target_in_qty", "amount_allocation_ratio",
            "quantity_source", "relation_snapshot_id",
        ])

        row = run_weighted_ledger(
            activity, _openings(["A"]), empty_sources, empty_targets
        ).sku_daily.iloc[0]

        self.assertEqual(0.0, row.sale_return_cost_basis)
        self.assertAlmostEqual(2.212, row.neg_clamp_qty)
        self.assertEqual(0.0, row.end_amt)

    def test_internal_cycle_is_blocked(self) -> None:
        activity = _activity(["A", "B"])
        source = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-14", "event_group_id": "AB", "relation_type": "PACK_CONVERT", "source_article_id": "A", "source_out_qty": 1, "quantity_source": "TEST", "relation_snapshot_id": "s"},
            {"store_id": "A3XV", "business_date": "2026-07-14", "event_group_id": "BA", "relation_type": "PACK_CONVERT", "source_article_id": "B", "source_out_qty": 1, "quantity_source": "TEST", "relation_snapshot_id": "s"},
        ])
        target = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-14", "event_group_id": "AB", "relation_type": "PACK_CONVERT", "target_article_id": "B", "target_in_qty": 1, "amount_allocation_ratio": 1, "quantity_source": "TEST", "relation_snapshot_id": "s"},
            {"store_id": "A3XV", "business_date": "2026-07-14", "event_group_id": "BA", "relation_type": "PACK_CONVERT", "target_article_id": "A", "target_in_qty": 1, "amount_allocation_ratio": 1, "quantity_source": "TEST", "relation_snapshot_id": "s"},
        ])
        with self.assertRaisesRegex(ValueError, "cycle"):
            run_weighted_ledger(activity, _openings(["A", "B"]), source, target)

    def test_daily_state_error_includes_sku_day_context(self) -> None:
        activity = _activity(["A", "B"])
        source = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14", "event_group_id": "AB",
            "relation_type": "RESIDUAL_TRANSFER", "source_article_id": "A", "source_out_qty": 1,
            "quantity_source": "TEST", "relation_snapshot_id": "s",
        }])
        target = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14", "event_group_id": "AB",
            "relation_type": "RESIDUAL_TRANSFER", "target_article_id": "B", "target_in_qty": 1,
            "amount_allocation_ratio": 1, "quantity_source": "TEST", "relation_snapshot_id": "s",
        }])

        with self.assertRaisesRegex(ValueError, "A3XV/2026-07-14/A"):
            run_weighted_ledger(activity, _openings(["A", "B"]), source, target)

    def test_cross_day_same_event_id_is_unique_and_orphan_event_is_blocked(self) -> None:
        first = _activity(["A", "B"])
        second = first.copy()
        second["business_date"] = "2026-07-15"
        activity = pd.concat([first, second], ignore_index=True)
        opening = _openings(["A", "B"])
        opening.loc[opening["article_id"].eq("A"), ["opening_qty", "opening_amt"]] = [2, 20]
        source_rows = []
        target_rows = []
        for day in ("2026-07-14", "2026-07-15"):
            source_rows.append({
                "store_id": "A3XV", "business_date": day, "event_group_id": "SAME",
                "relation_type": "PACK_CONVERT", "source_article_id": "A", "source_out_qty": 1,
                "quantity_source": "TEST", "relation_snapshot_id": "s",
            })
            target_rows.append({
                "store_id": "A3XV", "business_date": day, "event_group_id": "SAME",
                "relation_type": "PACK_CONVERT", "target_article_id": "B", "target_in_qty": 1,
                "amount_allocation_ratio": 1, "quantity_source": "TEST", "relation_snapshot_id": "s",
            })
        source = pd.DataFrame(source_rows)
        target = pd.DataFrame(target_rows)
        result = run_weighted_ledger(activity, opening, source, target)
        self.assertEqual(len(result.internal_postings), 4)
        self.assertTrue(result.internal_postings["posting_id"].is_unique)
        orphan = source.copy()
        orphan.loc[0, "business_date"] = "2026-07-16"
        orphan_target = target.copy()
        orphan_target.loc[0, "business_date"] = "2026-07-16"
        with self.assertRaisesRegex(ValueError, "outside the dense activity grid"):
            run_weighted_ledger(activity, opening, orphan, orphan_target)

    def test_count_flag_is_strict_boolean(self) -> None:
        activity = _activity(["A"])
        activity["is_counted"] = activity["is_counted"].astype(object)
        activity.loc[0, "is_counted"] = "False"
        empty_source = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type", "source_article_id",
            "source_out_qty", "quantity_source", "relation_snapshot_id",
        ])
        empty_target = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type", "target_article_id",
            "target_in_qty", "amount_allocation_ratio", "quantity_source", "relation_snapshot_id",
        ])
        with self.assertRaisesRegex(ValueError, "bool/0/1"):
            run_weighted_ledger(activity, _openings(["A"]), empty_source, empty_target)


if __name__ == "__main__":
    unittest.main()

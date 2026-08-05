from __future__ import annotations

import unittest

import pandas as pd

from fmetl.calculations.ledger import run_weighted_ledger
from fmetl.outputs.shadow_levels import build_shadow_levels_daily
from fmetl.validation.v014 import (
    assert_hard_gates,
    is_publishable,
    validate_publishability,
    validate_v014_ledger,
)


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

    def test_return_uses_opening_cost_and_return_without_cost_evidence_is_blocked(self) -> None:
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
        with self.assertRaises(ValueError):
            run_weighted_ledger(activity, _openings(["A"]), empty_sources, empty_targets)

        mixed = _activity(["A"])
        mixed.loc[0, ["sale_return_qty", "net_sale_qty", "net_sale_amt"]] = [1.0, -1.0, -12.0]
        mixed.loc[0, ["store_receive_qty", "store_receive_amt"]] = [9.0, 180.0]
        mixed_opening = _openings(["A"])
        mixed_opening.loc[0, ["opening_qty", "opening_amt"]] = [1.0, 10.0]
        mixed_result = run_weighted_ledger(mixed, mixed_opening, empty_sources, empty_targets)
        self.assertEqual(mixed_result.sku_daily.loc[0, "sale_return_cost_basis"], 19)

    def test_return_after_zero_stock_uses_last_observed_issue_cost(self) -> None:
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
        self.assertEqual(10.0, result.loc["2026-07-15", "sale_return_cost_basis"])
        self.assertEqual(10.0, result.loc["2026-07-15", "end_amt"])

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
            "relation_type": "PACK_CONVERT", "source_article_id": "A", "source_out_qty": 1,
            "quantity_source": "TEST", "relation_snapshot_id": "s",
        }])
        target = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14", "event_group_id": "AB",
            "relation_type": "PACK_CONVERT", "target_article_id": "B", "target_in_qty": 1,
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

    def test_full_parent_path_and_day_clear_two_are_reaggregated_from_sku(self) -> None:
        articles = ["A", "B"]
        activity = _activity(articles)
        activity.loc[0, ["net_sale_amt", "gross_sale_qty", "sale_return_qty", "net_sale_qty"]] = [10, 2, 1, 1]
        activity.loc[0, ["store_receive_qty", "store_receive_amt"]] = [1, 10]
        activity.loc[1, ["net_sale_amt", "gross_sale_qty", "net_sale_qty", "day_clear"]] = [20, 2, 2, "0"]
        empty_source = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type", "source_article_id",
            "source_out_qty", "quantity_source", "relation_snapshot_id",
        ])
        empty_target = pd.DataFrame(columns=[
            "store_id", "business_date", "event_group_id", "relation_type", "target_article_id",
            "target_in_qty", "amount_allocation_ratio", "quantity_source", "relation_snapshot_id",
        ])
        sku = run_weighted_ledger(activity, _openings(articles), empty_source, empty_target).sku_daily
        sku["store_name"] = "滨江宏岸店"
        sku["is_reportable"] = [True, True]
        sku["report_category_code"] = ["蔬菜类", "水果类"]
        sku["report_category_name"] = ["蔬菜类", "水果类"]
        sku["category_level2_id"] = "SAME_L2"
        sku["category_level2_description"] = "同名中类"
        sku["category_level3_id"] = ["L3A", "L3B"]
        sku["category_level3_description"] = "同名小类"
        sku["accounting_full_profit"] = sku["accounting_profit"]
        sku["ccj_amt"] = [2.0, 0.0]
        sku["ccj_qty"] = 0.0
        sku["ssls_amt"] = [3.0, 0.0]
        sku["ssls_qty"] = 0.0
        sku["adjusted_lost_amt"] = sku["accounting_lost_amt"] - sku["ccj_amt"] - sku["ssls_amt"]
        sku["adjusted_lost_qty"] = sku["accounting_lost_qty"]
        sku["adjusted_known_lost_amt"] = sku["accounting_known_lost_amt"] - sku["ccj_amt"] - sku["ssls_amt"]
        sku["adjusted_profit_before_ssls"] = sku["accounting_profit"] + sku["ccj_amt"]
        sku["adjusted_full_profit_before_ssls"] = sku["accounting_full_profit"] + sku["ccj_amt"]
        levels = build_shadow_levels_daily(sku)
        store_total = levels.loc[
            levels["level_description"].eq("门店") & levels["day_clear"].eq("2")
        ].iloc[0]
        self.assertEqual(store_total["total_sale_amount"], 30)
        self.assertEqual(store_total["total_sale_qty"], 3)
        self.assertEqual(store_total["adjusted_profit"], store_total["accounting_profit"] + 2)
        middle = levels.loc[
            levels["level_description"].eq("中分类") & levels["day_clear"].eq("2")
        ]
        self.assertEqual(len(middle), 2)

        sku.loc[1, "is_reportable"] = False
        reportable_only = build_shadow_levels_daily(sku)
        reportable_store = reportable_only.loc[
            reportable_only["level_description"].eq("门店")
            & reportable_only["day_clear"].eq("2")
        ].iloc[0]
        self.assertEqual(reportable_store["total_sale_amount"], 10)

        sku.loc[0, "accounting_profit"] = float("nan")
        with self.assertRaisesRegex(ValueError, "finite"):
            build_shadow_levels_daily(sku)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

import pandas as pd

from fmetl.validation.compare_v014_v15 import (
    DIAGNOSTIC_METRICS,
    EXACT_FACT_METRICS,
    PROFIT_GATE_METRICS,
    REFERENCE_COLUMNS,
    compare_v014_to_v15,
)


def _row(*, level: str, sku: str, category: str, profit: float) -> dict[str, object]:
    row = {column: 0.0 for column in REFERENCE_COLUMNS}
    row.update({
        "business_date": "2026-07-23", "store_name": "广州滨江宏岸店",
        "level_description": level, "day_clear": "2", "sku_id": sku,
        "category_name": category, "category_level1_description": category,
        "category_level2_description": "", "category_level3_description": "",
        "total_sale_amount": 100.0, "total_sale_qty": 10.0,
        "inbound_amount": 50.0, "inbound_qty": 5.0,
        "store_profit_amount": profit, "full_link_profit_amount": profit,
        "supply_chain_profit_amount": 0.0,
    })
    return {column: row[column] for column in REFERENCE_COLUMNS}


class V014V15ComparisonTests(unittest.TestCase):
    def test_exact_facts_and_reference_profit_diagnostics_are_separate(self) -> None:
        old = pd.DataFrame([
            _row(level="sku", sku="S1", category="熟食类", profit=20.0),
            _row(level="大分类", sku="", category="熟食类", profit=100.0),
            _row(level="门店", sku="", category="", profit=100.0),
        ])
        new = old.copy()
        new.loc[new["level_description"].eq("sku"), "store_profit_amount"] = 200.0
        new.loc[new["level_description"].isin({"大分类", "门店"}), "store_profit_amount"] = 101.9
        result = compare_v014_to_v15(new, old)
        sales = result.exact_facts.loc[
            result.exact_facts["comparison_scope"].eq("SKU_SALES")
        ]
        self.assertTrue(sales["status"].eq("PASS").all())
        gate = result.weekly_profit.loc[result.weekly_profit["metric"].eq("store_profit_amount")]
        store_gate = gate.loc[gate["level_description"].eq("门店")]
        category_gate = gate.loc[gate["level_description"].eq("大分类")]
        self.assertTrue(store_gate["status"].eq("PASS").all())
        self.assertTrue(category_gate["status"].eq("DIAGNOSTIC").all())
        self.assertFalse(category_gate["is_gate_metric"].any())
        self.assertTrue(result.sku_profit["status"].eq("LOCATE_ONLY").all())
        summary = result.summary.set_index("check")
        self.assertEqual("PASS", summary.loc["weekly_store_profit_within_2pct", "status"])
        self.assertEqual(
            "DIAGNOSTIC",
            summary.loc["normalized_category_profit_diagnostic", "status"],
        )
        self.assertEqual(123, len(result.field_matrix))

    def test_reference_category_label_mismatch_is_diagnostic(self) -> None:
        old = pd.DataFrame([_row(level="sku", sku="S1", category="熟食类", profit=20.0)])
        new = old.copy()
        new["category_level1_description"] = "预制菜"
        result = compare_v014_to_v15(new, old)
        self.assertEqual(
            result.category_alignment.loc[0, "status"],
            "REFERENCE_LABEL_DIFF",
        )
        summary = result.summary.set_index("check")
        self.assertEqual(
            "DIAGNOSTIC",
            summary.loc["selling_sku_category_alignment", "status"],
        )
        reported = result.reported_category_weekly_profit
        self.assertTrue(
            reported["comparison_basis"].eq("EACH_VERSION_REPORTED_CATEGORY").all()
        )
        self.assertTrue(reported["status"].eq("DIAGNOSTIC").all())

    def test_receipt_code_reallocation_does_not_break_category_alignment(self) -> None:
        old = pd.DataFrame([
            _row(level="sku", sku="SALE", category="熟食类", profit=20.0),
        ])
        old.loc[0, ["total_sale_amount", "total_sale_qty", "inbound_amount", "inbound_qty"]] = [
            100.0, 10.0, 50.0, 5.0
        ]
        new = old.copy()
        new.loc[0, ["inbound_amount", "inbound_qty"]] = 0.0
        receipt = _row(level="sku", sku="RECEIPT", category="冷藏加工及预制菜类", profit=0.0)
        receipt.update({
            "total_sale_amount": 0.0,
            "total_sale_qty": 0.0,
            "inbound_amount": 50.0,
            "inbound_qty": 1.0,
        })
        new = pd.concat([new, pd.DataFrame([receipt])], ignore_index=True)
        result = compare_v014_to_v15(new, old)
        self.assertTrue(result.category_alignment["status"].eq("PASS").all())
        inbound = result.exact_facts.loc[
            result.exact_facts["comparison_scope"].eq("STORE_DAY_INBOUND_AMOUNT")
        ]
        self.assertEqual(1, len(inbound))
        self.assertEqual("PASS", inbound.iloc[0]["status"])

    def test_weekly_profit_over_two_percent_is_a_hard_failure(self) -> None:
        old = pd.DataFrame([
            _row(level="sku", sku="S1", category="水果类", profit=100.0),
            _row(level="大分类", sku="", category="水果类", profit=100.0),
            _row(level="门店", sku="", category="", profit=100.0),
        ])
        new = old.copy()
        new.loc[:, "store_profit_amount"] = 103.0
        result = compare_v014_to_v15(new, old)
        failed = result.weekly_profit.loc[
            result.weekly_profit["is_gate_metric"]
            & result.weekly_profit["status"].eq("FAIL")
        ]
        self.assertEqual(1, len(failed))
        summary = result.summary.set_index("check")
        self.assertEqual("FAIL", summary.loc["weekly_store_profit_within_2pct", "status"])

    def test_category_delta_fully_explained_by_relation_skus_is_explicit(self) -> None:
        old = pd.DataFrame([
            _row(level="sku", sku="S1", category="水饮类", profit=100.0),
            _row(level="大分类", sku="", category="水饮类", profit=100.0),
            _row(level="门店", sku="", category="", profit=100.0),
        ])
        new = old.copy()
        new.loc[
            new["level_description"].isin({"sku", "大分类"}),
            ["store_profit_amount", "full_link_profit_amount"],
        ] = 90.0
        new.loc[
            new["level_description"].eq("门店"),
            ["store_profit_amount", "full_link_profit_amount"],
        ] = 100.0
        result = compare_v014_to_v15(
            new, old, relation_article_ids={"S1"}
        )
        category = result.weekly_profit.loc[
            result.weekly_profit["level_description"].eq("大分类")
            & result.weekly_profit["metric"].eq("store_profit_amount")
        ].iloc[0]
        self.assertEqual("DIAGNOSTIC", category["status"])
        self.assertFalse(bool(category["is_gate_metric"]))
        summary = result.summary.set_index("check")
        self.assertNotEqual("FAIL", summary.loc["weekly_store_profit_within_2pct", "status"])

    def test_v15_parent_category_adjustment_is_a_bridge_only(self) -> None:
        old = pd.DataFrame([
            _row(level="sku", sku="S1", category="猪肉类", profit=100.0),
            _row(level="大分类", sku="", category="猪肉类", profit=130.0),
            _row(level="门店", sku="", category="", profit=100.0),
        ])
        new = old.copy()
        new.loc[new["level_description"].eq("大分类"), "store_profit_amount"] = 100.0
        result = compare_v014_to_v15(new, old)
        category = result.weekly_profit.loc[
            result.weekly_profit["level_description"].eq("大分类")
            & result.weekly_profit["metric"].eq("store_profit_amount")
        ].iloc[0]
        self.assertEqual("DIAGNOSTIC", category["status"])
        self.assertEqual(
            "V016_OUTPUT_CATEGORY_NORMALIZED", category["comparison_basis"]
        )
        bridge = result.v15_parent_category_bridge.loc[
            result.v15_parent_category_bridge["metric"].eq("store_profit_amount")
        ].iloc[0]
        self.assertEqual(30.0, float(bridge["parent_adjustment_bridge"]))


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

import pandas as pd

from fmetl.validation.comparison import compare_v15_profit


def _row(level: str, middle: str, small: str, profit: float) -> dict[str, object]:
    return {
        "business_date": "2026-07-14", "store_name": "广州滨江宏岸店",
        "level_description": level, "day_clear": "2",
        "category_level1_description": "猪肉类",
        "category_level2_description": middle,
        "category_level3_description": small,
        "store_profit_amount": profit, "total_sale_amount": 1000,
    }


class ComparisonTests(unittest.TestCase):
    def test_full_parent_path_is_used_and_only_large_category_gates(self) -> None:
        v15 = pd.DataFrame([_row("大分类", "", "", 100), _row("中分类", "鲜肉", "", 50)])
        v013 = pd.DataFrame([_row("大分类", "", "", 104), _row("中分类", "鲜肉", "", 100)])
        result = compare_v15_profit(v013, v15)
        status = result.set_index("level_description")["comparison_status"].to_dict()
        self.assertEqual(status, {"中分类": "LOCATE_ONLY", "大分类": "PASS"})

    def test_missing_hierarchy_side_is_explicit(self) -> None:
        result = compare_v15_profit(
            pd.DataFrame([_row("小分类", "鲜肉", "排骨", 10)]),
            pd.DataFrame([_row("小分类", "鲜肉", "五花", 10)]),
        )
        self.assertEqual(set(result["comparison_status"]), {"MISSING_SIDE"})


if __name__ == "__main__":
    unittest.main()

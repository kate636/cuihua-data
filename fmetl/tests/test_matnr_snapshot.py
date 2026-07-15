from __future__ import annotations

import unittest

import pandas as pd

from fmetl.master_data.category import load_category_mapper
from fmetl.relations.matnr import build_matnr_member_snapshot


class MatnrSnapshotTests(unittest.TestCase):
    def test_same_matnr_cross_category_stays_separate_and_never_posts(self) -> None:
        common = {
            "inc_day": "2026-07-14", "matnr": "M", "sale_unit": "个",
            "matnr_unit": "KG", "order_unit": "件", "unit_weight": 1,
            "atob_value": 1, "zglfz": 1, "zglfm": 1,
        }
        goods = pd.DataFrame([
            {**common, "article_id": "A", "category_level1_id": "1",
             "category_level2_id": "11", "category_level3_id": "111",
             "report_category_code": "猪肉类"},
            {**common, "article_id": "B", "category_level1_id": "2",
             "category_level2_id": "22", "category_level3_id": "222",
             "report_category_code": "熟食类"},
        ])
        snapshot = build_matnr_member_snapshot(goods)
        frame = snapshot.frame
        self.assertEqual(frame["relation_id"].nunique(), 2)
        self.assertEqual(set(frame["posting_policy"]), {"NO_POSTING"})
        self.assertFalse(frame["formal_flow_allowed"].any())

    def test_matnr_requires_v15_report_category_mapping(self) -> None:
        goods = pd.DataFrame([{
            "inc_day": "2026-07-14", "article_id": "A", "matnr": "M",
            "sale_unit": "个", "matnr_unit": "KG", "order_unit": "件",
            "unit_weight": 1, "atob_value": 1, "zglfz": 1, "zglfm": 1,
            "category_level1_id": "1", "category_level2_id": "11",
            "category_level3_id": "111",
        }])
        with self.assertRaises(KeyError):
            build_matnr_member_snapshot(goods)

    def test_matnr_consumes_category_mapper_output(self) -> None:
        goods = pd.DataFrame([{
            "inc_day": "2026-07-14", "article_id": "20691561", "matnr": "M",
            "sale_unit": "千克", "matnr_unit": "KG", "order_unit": "件",
            "unit_weight": 1, "atob_value": 1, "zglfz": 1, "zglfm": 1,
            "category_level1_id": "1", "category_level2_id": "11",
            "category_level3_id": "111", "category_level1_description": "水果类",
            "category_level2_description": "水果", "category_level3_description": "水果",
        }])
        mapped = load_category_mapper().map_frame(goods)
        snapshot = build_matnr_member_snapshot(mapped)
        self.assertEqual(snapshot.frame.loc[0, "report_category_code"], "冷冻类")


if __name__ == "__main__":
    unittest.main()

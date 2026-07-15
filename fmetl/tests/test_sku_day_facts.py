from __future__ import annotations

import unittest

import pandas as pd

from fmetl.facts.sku_day import (
    attach_authoritative_day_clear,
    normalize_chdj_day_clear,
    normalize_inventory_counts,
    normalize_known_loss,
    normalize_sales_events,
)


class SkuDayFactTests(unittest.TestCase):
    def test_signed_sales_split_conserves_net_without_using_return_label_as_quantity(self) -> None:
        source = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-14", "inc_day": "2026-07-14", "abi_article_id": "A",
             "day_clear": "1", "qty_spec": 2, "sales_amt": 20,
             "return_sale_qty": 0, "return_sale_amt": 0},
            {"store_id": "A3XV", "business_date": "2026-07-14", "inc_day": "2026-07-14", "abi_article_id": "A",
             "day_clear": "1", "qty_spec": -0.5, "sales_amt": -6,
             "return_sale_qty": -1, "return_sale_amt": -6},
            {"store_id": "A3XV", "business_date": "2026-07-14", "inc_day": "2026-07-14", "abi_article_id": "A",
             "day_clear": "1", "qty_spec": 0, "sales_amt": 0,
             "return_sale_qty": -1, "return_sale_amt": -3},
        ])
        row = normalize_sales_events(source).iloc[0]
        self.assertEqual(row["gross_sale_qty"], 2)
        self.assertEqual(row["sale_return_qty"], 0.5)
        self.assertEqual(row["net_sale_qty"], 1.5)
        self.assertEqual(row["net_sale_amt"], 14)
        self.assertEqual(row["source_return_label_signed_qty"], -2)

    def test_formal_sales_null_and_conflicting_source_day_clear_are_blocked(self) -> None:
        base = {
            "store_id": "A3XV", "business_date": "2026-07-14", "inc_day": "2026-07-14",
            "abi_article_id": "A", "day_clear": "1", "qty_spec": 1, "sales_amt": 10,
            "return_sale_qty": None, "return_sale_amt": None,
        }
        self.assertEqual(normalize_sales_events(pd.DataFrame([base])).loc[0, "net_sale_qty"], 1)
        with self.assertRaises(ValueError):
            normalize_sales_events(pd.DataFrame([{**base, "qty_spec": None}]))
        with self.assertRaises(ValueError):
            normalize_sales_events(pd.DataFrame([base, {**base, "day_clear": "0"}]))

    def test_positive_return_label_is_blocked(self) -> None:
        source = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14", "inc_day": "2026-07-14", "abi_article_id": "A",
            "day_clear": "1", "qty_spec": 1, "sales_amt": 10,
            "return_sale_qty": 1, "return_sale_amt": 0,
        }])
        with self.assertRaises(ValueError):
            normalize_sales_events(source)

    def test_sales_business_date_must_match_partition(self) -> None:
        source = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-13", "inc_day": "2026-07-14",
            "abi_article_id": "A", "day_clear": "1", "qty_spec": 1, "sales_amt": 10,
            "return_sale_qty": 0, "return_sale_amt": 0,
        }])
        with self.assertRaises(ValueError):
            normalize_sales_events(source)

    def test_known_loss_uses_only_nonnegative_known_quantity_as_formal_flow(self) -> None:
        source = pd.DataFrame([{
            "store_id": "A3XV", "inc_day": "2026-07-14", "article_id": "A",
            "know_lost_qty": 2, "know_lost_amt": -1,
            "unknow_lost_qty": -3, "unknow_lost_amt": -30,
        }])
        row = normalize_known_loss(source).iloc[0]
        self.assertEqual(row["known_lost_qty"], 2)
        self.assertEqual(row["source_unknown_lost_qty"], -3)
        self.assertNotIn("unknown_lost_qty", row.index)

    def test_negative_known_loss_quantity_is_blocked(self) -> None:
        source = pd.DataFrame([{
            "store_id": "A3XV", "inc_day": "2026-07-14", "article_id": "A",
            "know_lost_qty": -1, "know_lost_amt": 0,
            "unknow_lost_qty": 0, "unknow_lost_amt": 0,
        }])
        with self.assertRaises(ValueError):
            normalize_known_loss(source)

        source.loc[0, "know_lost_qty"] = None
        with self.assertRaises(ValueError):
            normalize_known_loss(source)

    def test_valid_system_snapshot_is_formal_balance_but_not_explicit_operator_count(self) -> None:
        source = pd.DataFrame([
            {"shop_id": "A3XV", "inventory_date": "2026-07-14", "inc_day": "2026-07-14",
             "sku_code": "human",
             "sale_stock_qty": 1, "actual_stock_qty": 2, "profit_loss_qty": 1,
             "created_by": "162032", "updated_by": "162032",
             "created_at": "2026-07-14 23:00:00", "updated_at": "2026-07-14 23:00:00"},
            {"shop_id": "A3XV", "inventory_date": "2026-07-14", "inc_day": "2026-07-14",
             "sku_code": "system-zero",
             "sale_stock_qty": 0, "actual_stock_qty": 0, "profit_loss_qty": 0,
             "created_by": "系统", "updated_by": "系统",
             "created_at": "2026-07-14 23:05:00", "updated_at": "2026-07-14 23:05:00"},
            {"shop_id": "A3XV", "inventory_date": "2026-07-14", "inc_day": "2026-07-14",
             "sku_code": "bad-human",
             "sale_stock_qty": -1, "actual_stock_qty": -1, "profit_loss_qty": 0,
             "created_by": "162032", "updated_by": "162032",
             "created_at": None, "updated_at": None},
        ])
        result = normalize_inventory_counts(source).set_index("article_id")
        self.assertTrue(bool(result.loc["human", "is_counted"]))
        self.assertTrue(bool(result.loc["human", "is_explicit_operator_count"]))
        self.assertEqual(result.loc["human", "actual_stock_qty"], 2)
        self.assertTrue(bool(result.loc["system-zero", "is_counted"]))
        self.assertFalse(bool(result.loc["system-zero", "is_explicit_operator_count"]))
        self.assertEqual(
            result.loc["system-zero", "count_status"], "FORMAL_SYSTEM_BALANCE_SNAPSHOT"
        )
        self.assertEqual(result.loc["system-zero", "actual_stock_qty"], 0)
        self.assertEqual(result.loc["bad-human", "count_status"], "INVALID_OPERATOR_ACTUAL")

    def test_inventory_snapshot_infinity_is_blocked(self) -> None:
        source = pd.DataFrame([{
            "shop_id": "A3XV", "inventory_date": "2026-07-14", "inc_day": "2026-07-14",
            "sku_code": "A",
            "sale_stock_qty": 1, "actual_stock_qty": float("inf"), "profit_loss_qty": 0,
            "created_by": "162032", "updated_by": "162032",
            "created_at": None, "updated_at": None,
        }])
        with self.assertRaises(ValueError):
            normalize_inventory_counts(source)

    def test_inventory_partition_and_balance_equation_are_required(self) -> None:
        source = pd.DataFrame([{
            "shop_id": "A3XV", "inventory_date": "2026-07-14", "inc_day": "2026-07-13",
            "sku_code": "A", "sale_stock_qty": 1, "actual_stock_qty": 2,
            "profit_loss_qty": 1, "created_by": "系统", "updated_by": "系统",
            "created_at": None, "updated_at": None,
        }])
        with self.assertRaises(ValueError):
            normalize_inventory_counts(source)
        source.loc[0, "inc_day"] = "2026-07-14"
        source.loc[0, "profit_loss_qty"] = 0
        with self.assertRaises(ValueError):
            normalize_inventory_counts(source)

    def test_chdj_day_clear_requires_unique_binary_daily_label(self) -> None:
        source = pd.DataFrame([{
            "store_id": "A3XV", "inc_day": "2026-07-14",
            "article_id": "A", "day_clear": "0",
        }])
        result = normalize_chdj_day_clear(source)
        self.assertEqual(result.loc[0, "day_clear"], "0")
        with self.assertRaises(ValueError):
            normalize_chdj_day_clear(pd.concat([source, source], ignore_index=True))

    def test_chdj_day_clear_is_authoritative_when_attached_to_sales(self) -> None:
        sales_source = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14", "inc_day": "2026-07-14",
            "abi_article_id": "A", "day_clear": "1", "qty_spec": 1, "sales_amt": 10,
            "return_sale_qty": 0, "return_sale_amt": 0,
        }])
        sales = normalize_sales_events(sales_source)
        label = normalize_chdj_day_clear(pd.DataFrame([{
            "store_id": "A3XV", "inc_day": "2026-07-14", "article_id": "A", "day_clear": "1",
        }]))
        self.assertEqual(attach_authoritative_day_clear(sales, label).loc[0, "day_clear"], "1")
        with self.assertRaises(ValueError):
            attach_authoritative_day_clear(sales, label.assign(day_clear="0"))


if __name__ == "__main__":
    unittest.main()

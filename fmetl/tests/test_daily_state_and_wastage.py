from __future__ import annotations

import unittest

import pandas as pd

from fmetl.calculations.daily_cost_stock import DailyFlow, roll_forward_days, transition_day
from fmetl.calculations.profit import calculate_accounting_profit
from fmetl.calculations.special_wastage import (
    adjust_sku_wastage,
    apply_ssls_category_transfer,
    build_wastage_trace,
)


class DailyStateTests(unittest.TestCase):
    def test_normal_state_balances(self) -> None:
        state = transition_day(DailyFlow(init_qty=5, init_amt=50, store_receive_qty=5, store_receive_amt=60, sale_qty=3))
        self.assertAlmostEqual(state.issue_unit_cost, 11)
        self.assertAlmostEqual(state.end_qty, 7)
        self.assertAlmostEqual(state.end_amt, 77)
        self.assertAlmostEqual(state.qty_balance_residual, 0)
        self.assertAlmostEqual(state.amount_balance_residual, 0)

    def test_negative_clamp_cost_is_explicit(self) -> None:
        state = transition_day(DailyFlow(init_qty=1, init_amt=10, sale_qty=2))
        self.assertEqual(state.branch, "negative_clamp")
        self.assertEqual(state.end_qty, 0)
        self.assertEqual(state.unknown_lost_qty, -1)
        self.assertEqual(state.neg_clamp_cost_amt, 10)

    def test_soft_day_clear_opening_stock_consumption_is_not_double_posted(self) -> None:
        state = transition_day(DailyFlow(
            init_qty=5, init_amt=50, store_receive_qty=1, store_receive_amt=10,
            sale_qty=3, day_clear="0",
        ))
        self.assertEqual(state.unknown_lost_qty, -2)
        self.assertEqual(state.balance_unknown_qty, 0)
        self.assertEqual(state.end_qty, 3)
        self.assertAlmostEqual(state.qty_balance_residual, 0)

    def test_soft_day_clear_shortage_posts_equation_residual(self) -> None:
        state = transition_day(DailyFlow(
            init_qty=1, init_amt=10, store_receive_qty=1, store_receive_amt=10,
            sale_qty=3, day_clear="0",
        ))
        self.assertEqual(state.end_qty, 0)
        self.assertEqual(state.unknown_lost_qty, -2)
        self.assertEqual(state.balance_unknown_qty, -1)
        self.assertAlmostEqual(state.qty_balance_residual, 0)

    def test_profit_consumes_finalized_flows(self) -> None:
        profit = calculate_accounting_profit(
            sale_amt=100, store_receive_amt=60, bom_in_amt=5, bom_out_amt=5,
            pack_in_amt=2, pack_out_amt=2, compose_in_amt=3, compose_out_amt=3,
            init_stock_amt=10, end_stock_amt=20, neg_clamp_cost_amt=1,
        )
        self.assertEqual(profit, 49)

    def test_negative_count_and_overdrawn_internal_amount_are_blocked(self) -> None:
        with self.assertRaises(ValueError):
            transition_day(DailyFlow(init_qty=2, init_amt=20, actual_stock_qty=-1, is_counted=True))
        with self.assertRaises(ValueError):
            transition_day(DailyFlow(init_qty=10, init_amt=100, bom_out_qty=1, bom_out_amt=200))
        with self.assertRaises(ValueError):
            transition_day(DailyFlow(init_qty=0, init_amt=100))

    def test_system_end_balance_example_derives_unknown_loss_instead_of_copying_it(self) -> None:
        # A3XV / 2026-07-14 / 21279829: receive 10, sale 2, observed end balance 0.
        state = transition_day(DailyFlow(
            store_receive_qty=10,
            store_receive_amt=106.30,
            sale_qty=2,
            actual_stock_qty=0,
            is_counted=True,
        ))
        self.assertEqual(state.branch, "counted")
        self.assertAlmostEqual(state.issue_unit_cost, 10.63)
        self.assertAlmostEqual(state.sale_cost_amt, 21.26)
        self.assertAlmostEqual(state.unknown_lost_qty, 8)
        self.assertAlmostEqual(state.unknown_lost_amt, 85.04)
        self.assertEqual(state.end_qty, 0)

    def test_requested_weighted_average_and_exact_cross_day_roll(self) -> None:
        states = roll_forward_days(
            [
                DailyFlow(store_receive_qty=1, store_receive_amt=20, sale_qty=1),
                DailyFlow(store_receive_qty=1, store_receive_amt=30),
            ],
            initial_qty=1,
            initial_amt=10,
        )
        self.assertEqual(states[0].issue_unit_cost, 15)
        self.assertEqual(states[0].sale_cost_amt, 15)
        self.assertEqual(states[0].end_qty, 1)
        self.assertEqual(states[0].end_amt, 15)
        self.assertEqual(states[1].issue_unit_cost, 22.5)
        self.assertEqual(states[1].end_qty, 2)
        self.assertEqual(states[1].end_amt, 45)

    def test_roll_rejects_opening_amount_without_quantity_before_day_one(self) -> None:
        with self.assertRaises(ValueError):
            roll_forward_days(
                [DailyFlow(store_receive_qty=1, store_receive_amt=20)],
                initial_qty=0,
                initial_amt=10,
            )

    def test_internal_out_is_priced_by_current_weighted_cost(self) -> None:
        state = transition_day(DailyFlow(
            init_qty=1, init_amt=10, store_receive_qty=1, store_receive_amt=20,
            bom_out_qty=0.5,
        ))
        self.assertEqual(state.bom_out_amt, 7.5)
        self.assertEqual(state.end_qty, 1.5)
        self.assertEqual(state.end_amt, 22.5)

    def test_inventory_count_and_explicit_gain_cannot_both_post(self) -> None:
        with self.assertRaises(ValueError):
            transition_day(DailyFlow(
                init_qty=1, init_amt=10, actual_stock_qty=2, is_counted=True,
                inventory_gain_qty=1, inventory_gain_amt=10,
            ))

    def test_v15_wastage_and_ssls_conservation(self) -> None:
        accounting = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-01", "article_id": "source",
             "day_clear": "1", "accounting_lost_amt": 15.0, "accounting_lost_qty": 3.0,
             "accounting_known_lost_amt": 15.0, "accounting_profit": 100.0,
             "accounting_full_profit": 110.0},
        ])
        wastage = pd.DataFrame([
            {"inc_day": "2026-07-02", "sku_code": "source", "created_at": "2026-07-01 10:00:00", "reason": "炒菜机成本",
             "waste_money": 5.0, "waste_num": 1.0, "is_deleted": 0},
            {"inc_day": "2026-07-02", "sku_code": "source", "created_at": "2026-07-01 11:00:00", "reason": "生熟联动",
             "waste_money": 2.0, "waste_num": 0.5, "is_deleted": 0},
            {"inc_day": "2026-07-01", "sku_code": "source", "created_at": "2026-07-01 11:00:00", "reason": "生熟联动",
             "waste_money": 999.0, "waste_num": 999.0, "is_deleted": 0},
        ])
        adjusted = adjust_sku_wastage(accounting, wastage)
        trace = build_wastage_trace(wastage)
        self.assertEqual(len(trace), 2)
        self.assertTrue(trace["source_record_id"].is_unique)
        self.assertEqual(adjusted.loc[0, "adjusted_lost_amt"], 8)
        self.assertEqual(adjusted.loc[0, "adjusted_profit_before_ssls"], 105)
        levels = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-01", "day_clear": "1",
             "report_category_name": "蔬菜类", "adjusted_profit_before_ssls": 105.0,
             "adjusted_full_profit_before_ssls": 115.0, "ssls_amt": 2.0,
             "level_description": "大分类"},
            {"store_id": "A3XV", "business_date": "2026-07-01", "day_clear": "1",
             "report_category_name": "熟食类", "adjusted_profit_before_ssls": 20.0,
             "adjusted_full_profit_before_ssls": 22.0, "ssls_amt": 0.0,
             "level_description": "大分类"},
        ])
        transferred = apply_ssls_category_transfer(levels)
        self.assertEqual(transferred["adjusted_profit"].sum(), 125)
        self.assertEqual(transferred.loc[0, "adjusted_profit"], 107)
        self.assertEqual(transferred.loc[1, "adjusted_profit"], 18)


if __name__ == "__main__":
    unittest.main()

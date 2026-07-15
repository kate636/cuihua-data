from __future__ import annotations

import unittest

import pandas as pd

from fmetl.calculations.customers import aggregate_customer_metrics, classify_weekly_customer
from fmetl.facts.orders import join_trade_identity, normalize_order_events


def _orders(channel: str) -> pd.DataFrame:
    row = {
        "business_date": "2026-07-01",
        "inc_day": "2026-07-01",
        "store_id": "A3XV",
        "order_id": f"{channel}-1",
        "abi_article_id": "sku-1",
        "order_status": "os.completed",
        "pay_at": "2026-07-01 10:00:00",
        "jielong_flag": "-",
        "sales_amt": 10.0,
        "qty": 1.0,
        "thirdparty_user_identity": "u1",
    }
    return pd.DataFrame([row, row])


class OrderTests(unittest.TestCase):
    def test_duplicate_rows_and_signed_values_are_preserved(self) -> None:
        offline = _orders("off")
        online = _orders("on").iloc[:0].copy()
        refund = offline.iloc[[0]].copy()
        refund["order_id"] = "return-1"
        refund["order_status"] = "os.return.completed"
        refund["sales_amt"] = -3.0
        refund["qty"] = -1.0
        events = normalize_order_events(pd.concat([offline, refund], ignore_index=True), online)
        self.assertEqual(len(events), 3)
        self.assertEqual(events["sales_amt"].sum(), 17.0)
        duplicate = events.loc[events["order_id"].eq("off-1")]
        self.assertEqual(duplicate["duplicate_ordinal"].tolist(), [1, 2])

    def test_trade_join_cannot_expand(self) -> None:
        events = normalize_order_events(_orders("off"), _orders("on").iloc[:0].copy())
        trade = pd.DataFrame(
            [{
                "inc_day": "2026-07-01",
                "order_id": "off-1",
                "thirdparty_user_identity": "u1",
                "trade_time": "2026-07-01 09:59:00",
            }]
        )
        result = join_trade_identity(events, trade)
        self.assertEqual(len(result), len(events))
        self.assertEqual(result["customer_identity"].tolist(), ["u1", "u1"])

    def test_customer_count_is_order_visits_not_users(self) -> None:
        events = normalize_order_events(_orders("off"), _orders("on").iloc[:0].copy())
        events["customer_type"] = "新客"
        result = aggregate_customer_metrics(events, ["business_date"])
        self.assertEqual(result.loc[0, "cust_num"], 1)
        self.assertEqual(result.loc[0, "new_cust_num"], 1)
        self.assertEqual(result.loc[0, "signed_sale_amt"], 20.0)

    def test_v15_weekly_customer_rule(self) -> None:
        self.assertEqual(classify_weekly_customer("2026-06-28", "2026-06-29", "2026-07-01"), "老客")
        self.assertEqual(classify_weekly_customer("2026-06-29", "2026-06-29", "2026-07-01"), "新客")
        self.assertEqual(classify_weekly_customer(None, "2026-06-29", "2026-07-01"), "其他")
        with self.assertRaises(ValueError):
            classify_weekly_customer("2026-07-02", "2026-06-29", "2026-07-01")

    def test_non_finite_order_values_are_blocked(self) -> None:
        bad = _orders("off").iloc[:1].copy()
        bad.loc[0, "sales_amt"] = float("nan")
        with self.assertRaises(ValueError):
            normalize_order_events(bad, _orders("on").iloc[:0].copy())


if __name__ == "__main__":
    unittest.main()

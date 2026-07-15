from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import pandas as pd

from fmetl.connectors.duckdb_store import DuckDBStore, PartitionWrite
from fmetl.validation.balances import assert_daily_balances


class StoreAndValidationTests(unittest.TestCase):
    def test_empty_and_nan_balances_are_blocked(self) -> None:
        columns = [
            "qty_balance_residual", "amount_balance_residual", "end_qty", "end_amt",
            "issue_unit_cost", "ending_unit_cost",
        ]
        with self.assertRaises(ValueError):
            assert_daily_balances(pd.DataFrame(columns=columns))
        with self.assertRaises(ValueError):
            assert_daily_balances(pd.DataFrame([{column: float("nan") for column in columns}]))

    def test_legal_empty_partition_deletes_existing_rows(self) -> None:
        path = Path(tempfile.mkdtemp()) / "test.duckdb"
        store = DuckDBStore(path)
        try:
            row = pd.DataFrame({
                "store_id": ["A3XV"], "business_date": ["2026-07-01"], "value": [1],
            })
            store.replace_partition(
                "test_rows", row, ["store_id", "business_date"], ["A3XV", "2026-07-01"]
            )
            empty = row.iloc[:0].copy()
            store.replace_partition(
                "test_rows", empty, ["store_id", "business_date"], ["A3XV", "2026-07-01"]
            )
            self.assertEqual(store.connection.execute("SELECT COUNT(*) FROM test_rows").fetchone()[0], 0)
        finally:
            store.close()

    def test_batch_publish_rolls_back_all_tables(self) -> None:
        path = Path(tempfile.mkdtemp()) / "test.duckdb"
        store = DuckDBStore(path)
        try:
            first = pd.DataFrame({"business_date": ["2026-07-01"], "value": [1]})
            second = pd.DataFrame({"business_date": ["2026-07-01"], "value": [2]})
            store.replace_partitions_atomic([
                PartitionWrite("a", first, ["business_date"], ["2026-07-01"]),
                PartitionWrite("b", second, ["business_date"], ["2026-07-01"]),
            ])
            good = pd.DataFrame({"business_date": ["2026-07-01"], "value": [10]})
            bad = pd.DataFrame({"business_date": ["2026-07-01"], "wrong": [20]})
            with self.assertRaises(ValueError):
                store.replace_partitions_atomic([
                    PartitionWrite("a", good, ["business_date"], ["2026-07-01"]),
                    PartitionWrite("b", bad, ["business_date"], ["2026-07-01"]),
                ])
            self.assertEqual(store.connection.execute("SELECT value FROM a").fetchone()[0], 1)
            self.assertEqual(store.connection.execute("SELECT value FROM b").fetchone()[0], 2)
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()

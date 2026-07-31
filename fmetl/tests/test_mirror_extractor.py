from __future__ import annotations

from types import SimpleNamespace
import unittest

import pandas as pd

from fmetl.contracts.mirror import MirrorAuthority, MirrorContract, PartitionMode
from fmetl.mirror.extract import MirrorExtractor


class FakeApi:
    def __init__(self, frames: list[pd.DataFrame]):
        self.frames = list(frames)
        self.settings = SimpleNamespace(page_size=20_000)

    def query(self, sql: str) -> pd.DataFrame:
        if not self.frames:
            raise AssertionError(f"unexpected query: {sql}")
        return self.frames.pop(0)


class MirrorExtractorTests(unittest.TestCase):
    def test_legal_empty_keeps_projection(self) -> None:
        contract = MirrorContract(
            name="empty_source", authority=MirrorAuthority.OBSERVATION,
            partition_column="inc_day", store_column="store_id",
            projection=("store_id", "inc_day", "value"),
            expected_grain=("store_id", "inc_day", "value"), allow_empty=True,
        )
        api = FakeApi([pd.DataFrame({"row_count": [0]}), pd.DataFrame()])
        result = MirrorExtractor(api).extract_day(contract, "2026-07-01")
        self.assertEqual(result.columns.tolist(), list(contract.projection))
        self.assertTrue(result.empty)

    def test_latest_snapshot_is_distinct_from_requested_business_day(self) -> None:
        contract = MirrorContract(
            name="snapshot_source", authority=MirrorAuthority.DIMENSION,
            partition_column="inc_day", store_column=None,
            projection=("inc_day", "article_id"),
            expected_grain=("inc_day", "article_id"),
            partition_mode=PartitionMode.LATEST_SNAPSHOT,
        )
        api = FakeApi([
            pd.DataFrame({"source_day": ["2026-07-14"]}),
            pd.DataFrame({"row_count": [1]}),
            pd.DataFrame({"inc_day": ["2026-07-14"], "article_id": ["sku"]}),
        ])
        result = MirrorExtractor(api).extract_day(contract, "2026-01-01")
        self.assertEqual(result.attrs["requested_business_day"], "2026-01-01")
        self.assertEqual(result.attrs["source_snapshot_day"], "2026-07-14")

    def test_exact_duplicate_rows_are_deduplicated(self) -> None:
        # 上游门店改名导致 JOIN 双份：投影后完全重复的行应去重且不违反粒度
        contract = MirrorContract(
            name="strategy_fm_compose_di", authority=MirrorAuthority.OBSERVATION,
            partition_column="inc_day", store_column="store_id",
            projection=("store_id", "business_date", "inc_day", "article_id", "compose_in_qty"),
            expected_grain=("store_id", "business_date", "article_id"),
        )
        api = FakeApi([
            pd.DataFrame({"row_count": [2]}),
            pd.DataFrame({
                "store_id": ["A3XV", "A3XV"],
                "business_date": ["2026-07-24", "2026-07-24"],
                "inc_day": ["2026-07-24", "2026-07-24"],
                "article_id": ["20005016", "20005016"],
                "compose_in_qty": [1.0, 1.0],
            }),
        ])
        result = MirrorExtractor(api).extract_day(contract, "2026-07-24")
        self.assertEqual(len(result), 1)
        self.assertEqual(result.attrs["exact_duplicates_dropped"], 1)

    def test_conflicting_duplicate_grain_still_raises(self) -> None:
        # 同粒度但数值不同的行不是重复，必须继续报 GrainViolation
        contract = MirrorContract(
            name="strategy_fm_compose_di", authority=MirrorAuthority.OBSERVATION,
            partition_column="inc_day", store_column="store_id",
            projection=("store_id", "business_date", "inc_day", "article_id", "compose_in_qty"),
            expected_grain=("store_id", "business_date", "article_id"),
        )
        api = FakeApi([
            pd.DataFrame({"row_count": [2]}),
            pd.DataFrame({
                "store_id": ["A3XV", "A3XV"],
                "business_date": ["2026-07-24", "2026-07-24"],
                "inc_day": ["2026-07-24", "2026-07-24"],
                "article_id": ["20005016", "20005016"],
                "compose_in_qty": [1.0, 2.0],
            }),
        ])
        with self.assertRaises(ValueError):
            MirrorExtractor(api).extract_day(contract, "2026-07-24")

    def test_null_store_is_blocked(self) -> None:
        contract = MirrorContract(
            name="bad_store", authority=MirrorAuthority.OBSERVATION,
            partition_column="inc_day", store_column="store_id",
            projection=("store_id", "inc_day", "value"),
            expected_grain=("store_id", "inc_day", "value"),
        )
        api = FakeApi([
            pd.DataFrame({"row_count": [1]}),
            pd.DataFrame({"store_id": [None], "inc_day": ["2026-07-01"], "value": [1]}),
        ])
        with self.assertRaises(ValueError):
            MirrorExtractor(api).extract_day(contract, "2026-07-01")


if __name__ == "__main__":
    unittest.main()

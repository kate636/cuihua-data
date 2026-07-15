from __future__ import annotations

import unittest

import pandas as pd

from fmetl.validation.manifest import (
    SourceManifestSpec,
    build_source_manifest,
    stable_frame_checksum,
)


class ManifestTests(unittest.TestCase):
    def test_checksum_is_row_order_independent_but_preserves_multiplicity(self) -> None:
        frame = pd.DataFrame([{"a": 1, "b": None}, {"a": 2, "b": "x"}])
        self.assertEqual(stable_frame_checksum(frame), stable_frame_checksum(frame.iloc[::-1]))
        duplicated = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
        self.assertNotEqual(stable_frame_checksum(frame), stable_frame_checksum(duplicated))

    def test_source_manifest_is_partitioned_and_records_business_coverage(self) -> None:
        frame = pd.DataFrame([
            {"inc_day": "2026-07-08", "business_date": "2026-07-08", "value": 1},
            {"inc_day": "2026-07-09", "business_date": "2026-07-09", "value": 2},
        ])
        result = build_source_manifest([SourceManifestSpec(
            source_name="strategy_fm_x_di",
            source_namespace="STARROCKS_MIRROR",
            frame=frame,
            partition_column="inc_day",
            business_date_column="business_date",
            expected_partitions=("2026-07-08", "2026-07-09", "2026-07-10"),
        )])
        self.assertEqual(result["row_count"].tolist(), [1, 1, 0])
        self.assertEqual(
            result["source_partition"].tolist(),
            ["2026-07-08", "2026-07-09", "2026-07-10"],
        )
        self.assertEqual(result["business_date_min"].tolist()[:2], ["2026-07-08", "2026-07-09"])
        self.assertTrue(pd.isna(result.loc[2, "business_date_min"]))


if __name__ == "__main__":
    unittest.main()

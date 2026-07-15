from __future__ import annotations

from dataclasses import dataclass
import hashlib

import pandas as pd


@dataclass(frozen=True)
class SourceManifestSpec:
    source_name: str
    source_namespace: str
    frame: pd.DataFrame
    partition_column: str | None = None
    fixed_partition: str | None = None
    business_date_column: str | None = None
    expected_partitions: tuple[str, ...] | None = None


def stable_frame_checksum(frame: pd.DataFrame) -> str:
    """Hash values, columns and row multiplicity independent of source row order."""
    columns = sorted(map(str, frame.columns))
    canonical = frame.copy()
    canonical.columns = canonical.columns.map(str)
    canonical = canonical.reindex(columns=columns)
    for column in columns:
        canonical[column] = canonical[column].map(
            lambda value: "<NULL>" if pd.isna(value) else str(value)
        )
    if columns:
        canonical = canonical.sort_values(columns, kind="stable").reset_index(drop=True)
    payload = canonical.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_source_manifest(specs: list[SourceManifestSpec]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for spec in specs:
        if bool(spec.partition_column) == bool(spec.fixed_partition):
            raise ValueError(
                f"{spec.source_name}: declare exactly one of partition_column/fixed_partition"
            )
        if spec.partition_column and spec.partition_column not in spec.frame.columns:
            raise KeyError(f"{spec.source_name}: missing partition column {spec.partition_column}")
        if spec.business_date_column and spec.business_date_column not in spec.frame.columns:
            raise KeyError(
                f"{spec.source_name}: missing business date column {spec.business_date_column}"
            )
        if spec.partition_column:
            observed = {
                "<NULL>" if pd.isna(partition) else str(partition): part
                for partition, part in spec.frame.groupby(
                    spec.partition_column, dropna=False, sort=True
                )
            }
            partitions = set(observed)
            if spec.expected_partitions:
                partitions.update(map(str, spec.expected_partitions))
            groups = [
                (partition, observed.get(partition, spec.frame.iloc[:0].copy()))
                for partition in sorted(partitions)
            ]
        else:
            groups = [(str(spec.fixed_partition), spec.frame)]
        if not groups:
            groups = [("<EMPTY>", spec.frame)]
        for partition, part in groups:
            if spec.business_date_column and not part.empty:
                business_dates = part[spec.business_date_column].dropna().astype(str)
                business_min = business_dates.min() if not business_dates.empty else None
                business_max = business_dates.max() if not business_dates.empty else None
            else:
                business_min = None
                business_max = None
            rows.append({
                "source_name": spec.source_name,
                "source_namespace": spec.source_namespace,
                "source_partition": "<NULL>" if pd.isna(partition) else str(partition),
                "business_date_min": business_min,
                "business_date_max": business_max,
                "row_count": len(part),
                "sha256": stable_frame_checksum(part),
            })
    return pd.DataFrame(rows).sort_values(
        ["source_namespace", "source_name", "source_partition"], kind="stable"
    ).reset_index(drop=True)

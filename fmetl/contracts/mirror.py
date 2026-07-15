from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Sequence


CATALOG = "default_catalog.ads_business_analysis"


class MirrorAuthority(str, Enum):
    OBSERVATION = "observation"
    DIMENSION = "dimension"
    DERIVED_BRIDGE = "derived_bridge"
    REFERENCE_ONLY = "reference_only"


class PartitionMode(str, Enum):
    DAILY_PARTITION = "daily_partition"
    LATEST_SNAPSHOT = "latest_snapshot"
    STATIC_FULL = "static_full"


@dataclass(frozen=True)
class MirrorContract:
    """Field-level contract for one StarRocks mirror table."""

    name: str
    authority: MirrorAuthority
    partition_column: str | None
    store_column: str | None
    projection: Sequence[str]
    expected_grain: Sequence[str]
    required: bool = True
    shard_key: str | None = None
    shards: int = 1
    managed_by_sync_script: bool = True
    partition_mode: PartitionMode = PartitionMode.DAILY_PARTITION
    allow_empty: bool = False
    grain_stage: str = "source"
    base_predicates: Sequence[str] = ()
    note: str = ""

    @property
    def full_name(self) -> str:
        return f"{CATALOG}.{self.name}"

    def where_for(self, store_id: str, start: str, end: str) -> str:
        clauses: list[str] = []
        if self.store_column:
            clauses.append(f"{self.store_column} = '{store_id}'")
        if self.partition_column:
            clauses.append(f"{self.partition_column} BETWEEN '{start}' AND '{end}'")
        clauses.extend(self.base_predicates)
        return " AND ".join(clauses) if clauses else "1 = 1"

from __future__ import annotations

from datetime import date
import math

import pandas as pd

from fmetl.connectors import PaginationContractError, QdmApi
from fmetl.contracts.grains import assert_only_store, assert_unique
from fmetl.contracts.mirror import MirrorContract, PartitionMode


class MirrorExtractor:
    def __init__(self, api: QdmApi, store_id: str = "A3XV"):
        self.api = api
        self.store_id = store_id

    def extract_day(self, contract: MirrorContract, business_day: date | str) -> pd.DataFrame:
        requested_day = str(business_day)
        if not contract.partition_column:
            raise ValueError(f"{contract.name} is not date-partitioned")
        if contract.partition_mode == PartitionMode.LATEST_SNAPSHOT:
            predicate = " AND ".join(contract.base_predicates) if contract.base_predicates else "1 = 1"
            latest = self.api.query(
                f"SELECT MAX({contract.partition_column}) AS source_day "
                f"FROM {contract.full_name} WHERE {predicate}"
            )
            if latest.empty or pd.isna(latest.iloc[0]["source_day"]):
                if contract.allow_empty:
                    return pd.DataFrame(columns=contract.projection)
                raise RuntimeError(f"{contract.name}: latest snapshot is missing")
            source_day = str(latest.iloc[0]["source_day"])
        elif contract.partition_mode == PartitionMode.DAILY_PARTITION:
            source_day = requested_day
        else:
            raise ValueError(f"{contract.name}: use a full-table extractor for STATIC_FULL")
        base = contract.where_for(self.store_id, source_day, source_day)
        frames: list[pd.DataFrame] = []
        for bucket in range(contract.shards):
            where = base
            if contract.shards > 1:
                if not contract.shard_key:
                    raise ValueError(f"{contract.name}: shards require shard_key")
                where += (
                    f" AND MOD(CRC32(COALESCE(CAST({contract.shard_key} AS STRING), '')), "
                    f"{contract.shards}) = {bucket}"
                )
            count_sql = f"SELECT COUNT(*) AS row_count FROM {contract.full_name} WHERE {where}"
            count_frame = self.api.query(count_sql)
            expected = int(count_frame.iloc[0]["row_count"])
            if expected >= self.api.settings.page_size:
                raise PaginationContractError(
                    f"{contract.name} shard {bucket}/{contract.shards} has {expected} rows; increase shards"
                )
            projection = ", ".join(contract.projection)
            frame = self.api.query(f"SELECT {projection} FROM {contract.full_name} WHERE {where}")
            if len(frame) != expected:
                raise RuntimeError(
                    f"{contract.name} shard {bucket}: source count={expected}, extracted={len(frame)}"
                )
            frames.append(frame)
        result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        missing_columns = sorted(set(contract.projection) - set(result.columns))
        unexpected_columns = sorted(set(result.columns) - set(contract.projection))
        if not result.empty and (missing_columns or unexpected_columns):
            raise RuntimeError(
                f"{contract.name}: projection mismatch missing={missing_columns}, unexpected={unexpected_columns}"
            )
        result = result.reindex(columns=contract.projection)
        # 上游 Hive 表 JOIN 门店维表时，门店改名会产生仅 store_name 不同的双份记录；
        # 投影列不含 store_name，这类行是完全重复行，安全去重。
        # 数值不同的真粒度冲突仍会被下方 assert_unique 拦截。
        exact_duplicates = int(result.duplicated(keep="first").sum())
        if exact_duplicates:
            result = result.drop_duplicates(ignore_index=True)
        result.attrs["exact_duplicates_dropped"] = exact_duplicates
        if result.empty and not contract.allow_empty:
            raise RuntimeError(f"{contract.name}: required non-empty source partition {source_day} is empty")
        if contract.store_column:
            renamed = result.rename(columns={contract.store_column: "store_id"})
            assert_only_store(renamed, self.store_id)
        if not result.empty:
            partition_values = set(result[contract.partition_column].dropna().astype(str))
            if partition_values != {source_day}:
                raise RuntimeError(
                    f"{contract.name}: extracted partition values {partition_values} != {source_day}"
                )
            if contract.grain_stage == "source":
                grain = list(contract.expected_grain)
                if not set(grain).issubset(result.columns):
                    raise RuntimeError(f"{contract.name}: source grain columns missing: {grain}")
                if result[grain].isna().any().any():
                    raise RuntimeError(f"{contract.name}: source grain contains NULL")
                assert_unique(result, grain, contract.name)
        result.attrs["requested_business_day"] = requested_day
        result.attrs["source_snapshot_day"] = source_day
        return result

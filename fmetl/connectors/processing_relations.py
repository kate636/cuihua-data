from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import requests

from fmetl.config import Settings, get_settings
from fmetl.relations.snapshots import Snapshot, build_snapshot


@dataclass(frozen=True)
class ProcessingRelationSnapshot:
    snapshot: Snapshot
    exported_at: str
    source_count: int

    @property
    def frame(self) -> pd.DataFrame:
        return self.snapshot.frame


class ProcessingRelationSource:
    """Fetch the Foodmart recipe export once and freeze it for one ETL run."""

    def __init__(
        self,
        settings: Settings | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.session = session or requests.Session()
        self._cached: ProcessingRelationSnapshot | None = None

    def fetch_once(self) -> ProcessingRelationSnapshot:
        if self._cached is not None:
            return self._cached
        response = self.session.get(self.settings.processing_relation_url, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("relations"), list):
            raise ValueError("processing relation export must contain a relations list")
        relations = payload["relations"]
        source_count = int(payload.get("count", len(relations)))
        if source_count != len(relations):
            raise ValueError(
                f"processing relation count mismatch: declared={source_count}, actual={len(relations)}"
            )
        frame = pd.DataFrame(relations)
        required = {
            "finished_sku", "finished_name", "raw_sku", "raw_name",
            "raw_qty", "raw_unit", "yield_qty", "yield_unit", "category_type",
        }
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"processing relation export missing columns: {missing}")
        frame = frame[list(sorted(required))].rename(columns={
            "finished_sku": "finished_article_id",
            "finished_name": "finished_article_name",
            "raw_sku": "raw_article_id",
            "raw_name": "raw_article_name",
        })
        id_columns = ["finished_article_id", "raw_article_id"]
        if frame[id_columns].isna().any().any():
            raise ValueError("processing recipe article IDs cannot contain NULL")
        frame[id_columns] = frame[id_columns].astype(str)
        if (
            frame[id_columns]
            .apply(lambda column: column.str.strip().str.lower().isin({"", "nan", "none", "null"}))
            .any().any()
        ):
            raise ValueError("processing recipe article IDs cannot be blank")
        frame["raw_qty"] = pd.to_numeric(frame["raw_qty"], errors="raise")
        frame["yield_qty"] = pd.to_numeric(frame["yield_qty"], errors="raise")
        if not np.isfinite(frame[["raw_qty", "yield_qty"]].to_numpy(dtype=float)).all():
            raise ValueError("processing recipe raw_qty and yield_qty must be finite")
        if frame[["raw_qty", "yield_qty"]].le(0).any().any():
            raise ValueError("processing recipe raw_qty and yield_qty must be positive")
        if frame.duplicated(["finished_article_id", "raw_article_id"]).any():
            raise ValueError("processing relation export contains duplicate finished/raw pairs")
        frame["relation_id"] = "RECIPE|" + frame["finished_article_id"]
        frame["approved"] = True
        snapshot = build_snapshot(
            frame,
            keys=["finished_article_id", "raw_article_id"],
            namespace="processing_recipe",
        )
        self._cached = ProcessingRelationSnapshot(
            snapshot=snapshot,
            exported_at=str(payload.get("exported_at", "")),
            source_count=source_count,
        )
        return self._cached

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

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
        self._dated_cached: ProcessingRelationSnapshot | None = None

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
        optional = {
            "relation_id", "effective_from", "effective_to", "approved", "is_active",
        }
        frame = frame[list(sorted(required | (optional & set(frame.columns))))].rename(columns={
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
        if "relation_id" not in frame:
            frame["relation_id"] = "RECIPE|" + frame["finished_article_id"]
        if "approved" not in frame:
            frame["approved"] = (
                frame["is_active"].map(lambda value: str(value).strip().lower() in {"1", "true"})
                if "is_active" in frame else True
            )
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

    def fetch_dated_once(self) -> ProcessingRelationSnapshot:
        """Fetch the current relation version with conservative valid-from dates.

        The legacy ``/export`` endpoint deliberately omits database timestamps.
        The read-only ``/list`` endpoint exposes ``created_at`` and ``updated_at``
        for the same active rows.  v0.14 uses the later timestamp as the earliest
        date on which the *current* relation version is proven to exist.  This
        avoids applying a modified ratio to dates before that modification and
        does not pretend that the API contains a full history table.
        """
        if self._dated_cached is not None:
            return self._dated_cached
        list_url = self.settings.processing_relation_url
        if list_url.rstrip("/").endswith("/export"):
            list_url = list_url.rstrip("/")[:-len("export")] + "list?active_only=1"
        else:
            separator = "&" if "?" in list_url else "?"
            list_url = f"{list_url}{separator}active_only=1"
        response = self.session.get(list_url, timeout=30)
        response.raise_for_status()
        rows = response.json()
        if not isinstance(rows, list):
            raise ValueError("dated processing relation endpoint must return a row list")
        frame = pd.DataFrame(rows)
        required = {
            "id", "finished_sku", "finished_name", "raw_sku", "raw_name",
            "raw_qty", "raw_unit", "yield_qty", "yield_unit", "category_type",
            "is_active", "created_at", "updated_at",
        }
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"dated processing relation export missing columns: {missing}")
        if frame.empty:
            raise ValueError("dated processing relation export cannot be empty")
        for column in ("created_at", "updated_at"):
            parsed = pd.to_datetime(frame[column], errors="coerce")
            if parsed.isna().any():
                raise ValueError(f"processing relation {column} must be a valid timestamp")
            frame[column] = parsed
        valid_from = frame[["created_at", "updated_at"]].max(axis=1)
        frame["effective_from"] = valid_from.dt.strftime("%Y-%m-%d")
        frame["effective_to"] = None
        frame["relation_id"] = "PROCESSING|" + frame["id"].astype(str)
        frame["approved"] = frame["is_active"].map(
            lambda value: str(value).strip().lower() in {"1", "true", "yes"}
        )
        normalized = frame.rename(columns={
            "finished_sku": "finished_article_id",
            "finished_name": "finished_article_name",
            "raw_sku": "raw_article_id",
            "raw_name": "raw_article_name",
        })
        keep = [
            "relation_id", "finished_article_id", "finished_article_name",
            "raw_article_id", "raw_article_name", "raw_qty", "raw_unit",
            "yield_qty", "yield_unit", "category_type", "effective_from",
            "effective_to", "approved",
        ]
        normalized = normalized[keep].copy()
        normalized[["finished_article_id", "raw_article_id"]] = normalized[
            ["finished_article_id", "raw_article_id"]
        ].astype(str)
        normalized[["raw_qty", "yield_qty"]] = normalized[["raw_qty", "yield_qty"]].apply(
            pd.to_numeric, errors="raise"
        )
        if not np.isfinite(normalized[["raw_qty", "yield_qty"]].to_numpy(dtype=float)).all():
            raise ValueError("processing recipe raw_qty and yield_qty must be finite")
        if normalized[["raw_qty", "yield_qty"]].le(0).any().any():
            raise ValueError("processing recipe raw_qty and yield_qty must be positive")
        if normalized.duplicated(["finished_article_id", "raw_article_id"]).any():
            raise ValueError("processing relation list contains duplicate finished/raw pairs")
        snapshot = build_snapshot(
            normalized,
            keys=["finished_article_id", "raw_article_id"],
            namespace="dated_processing_recipe",
        )
        self._dated_cached = ProcessingRelationSnapshot(
            snapshot=snapshot,
            exported_at=datetime.now().isoformat(),
            source_count=len(normalized),
        )
        return self._dated_cached

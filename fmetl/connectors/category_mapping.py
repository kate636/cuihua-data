from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

import pandas as pd
import requests

from fmetl.config import Settings, get_settings
from fmetl.relations.snapshots import Snapshot, build_snapshot


@dataclass(frozen=True)
class CategoryMappingSnapshot:
    snapshot: Snapshot
    business_date: str
    generated_at: str
    version: str
    source_url: str
    stale: bool
    sync_error: str

    @property
    def frame(self) -> pd.DataFrame:
        return self.snapshot.frame


def _snapshot_from_payload(
    payload: object,
    *,
    source_url: str,
) -> CategoryMappingSnapshot:
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise ValueError("category mapping response must contain an items list")
    business_date = str(payload.get("business_date") or "")
    if not business_date:
        raise ValueError("category mapping response must declare business_date")
    stale = bool(payload.get("stale", False))
    sync_error = str(payload.get("sync_error") or "")
    if stale or sync_error:
        raise ValueError(
            f"latest category mapping is not healthy: stale={stale}, sync_error={sync_error!r}"
        )
    frame = pd.DataFrame(payload["items"])
    required = {
        "article_id", "category_level1_description",
        "category_level2_description", "category_level3_description",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"category mapping items missing columns: {missing}")
    if frame.empty:
        raise ValueError("category mapping items cannot be empty")
    frame = frame.copy()
    frame["article_id"] = frame["article_id"].astype(str)
    if frame["article_id"].str.strip().isin({"", "nan", "none", "null"}).any():
        raise ValueError("category mapping article_id cannot be blank")
    if frame.duplicated("article_id").any():
        raise ValueError("category mapping must be unique per article_id")
    for column in required - {"article_id"}:
        frame[column] = frame[column].fillna("").astype(str)
        if frame[column].str.strip().eq("").any():
            raise ValueError(f"category mapping {column} cannot be blank")
    frame["snapshot_business_date"] = business_date
    frame["snapshot_generated_at"] = str(payload.get("generated_at") or "")
    frame["snapshot_version"] = str(payload.get("version") or "")
    frame["snapshot_source_url"] = source_url
    snapshot = build_snapshot(
        frame,
        keys=["article_id"],
        namespace="monitoring_platform_latest_category",
    )
    return CategoryMappingSnapshot(
        snapshot=snapshot,
        business_date=business_date,
        generated_at=str(payload.get("generated_at") or ""),
        version=str(payload.get("version") or ""),
        source_url=source_url,
        stale=stale,
        sync_error=sync_error,
    )


def load_category_mapping_snapshot(path: Path | str) -> CategoryMappingSnapshot:
    source = Path(path).resolve()
    payload = json.loads(source.read_text(encoding="utf-8"))
    return _snapshot_from_payload(payload, source_url=f"LOCAL_JSON:{source}")


class CategoryMappingSource:
    """Fetch and freeze the monitoring platform's latest effective mapping."""

    def __init__(
        self,
        settings: Settings | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.session = session or requests.Session()
        self._cached: CategoryMappingSnapshot | None = None

    def fetch_latest_once(self) -> CategoryMappingSnapshot:
        if self._cached is not None:
            return self._cached
        response = self.session.get(self.settings.category_mapping_url, timeout=30)
        response.raise_for_status()
        self._cached = _snapshot_from_payload(
            response.json(), source_url=self.settings.category_mapping_url
        )
        return self._cached
